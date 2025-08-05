package load

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/collection"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/mathx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/timex"
)

const (
	defaultBuckets = 50
	defaultWindow  = time.Second * 5
	// using 1000m notation, 900m is like 90%, keep it as var for unit test
	defaultCpuThreshold = 900
	defaultMinRt        = float64(time.Second / time.Millisecond)
	// moving average hyperparameter beta for calculating requests on the fly
	flyingBeta               = 0.9
	coolOffDuration          = time.Second
	cpuMax                   = 1000 // millicpu
	millisecondsPerSecond    = 1000
	overloadFactorLowerBound = 0.1
)

var (
	// ErrServiceOverloaded is returned by Shedder.Allow when the service is overloaded.
	ErrServiceOverloaded = errors.New("service overloaded")

	// default to be enabled
	enabled = syncx.ForAtomicBool(true)
	// default to be enabled
	logEnabled = syncx.ForAtomicBool(true)
	// make it a variable for unit test
	// cpu 检查函数
	systemOverloadChecker = func(cpuThreshold int64) bool {
		return stat.CpuUsage() >= cpuThreshold
	}
)

type (
	// A Promise interface is returned by Shedder.Allow to let callers tell
	// whether the processing request is successful or not.
	Promise interface {
		// Pass lets the caller tell that the call is successful.
		// 请求成功时回调此函数
		Pass()
		// Fail lets the caller tell that the call is failed.
		// 请求失败时回调此函数
		Fail()
	}

	// Shedder is the interface that wraps the Allow method.
	Shedder interface {
		// Allow returns the Promise if allowed, otherwise ErrServiceOverloaded.
		// 降载检查
		// 1. 允许调用，需手动执行 Promise.accept()/reject()上报实际执行任务结构
		// 2. 拒绝调用，将会直接返回err：服务过载错误 ErrServiceOverloaded
		Allow() (Promise, error)
	}

	// ShedderOption lets caller customize the Shedder.
	// option参数模式
	ShedderOption func(opts *shedderOptions)

	// 可选配置参数
	shedderOptions struct {
		// 滑动时间窗口大小
		window time.Duration
		// 滑动时间窗口数量
		buckets int
		// cpu负载临界值
		cpuThreshold int64
	}

	// 自适应降载结构体，需实现 Shedder 接口
	adaptiveShedder struct {
		// cpu负载临界值
		// 高于临界值代表高负载需要降载保证服务
		cpuThreshold int64
		// 1s内有多少个桶
		windowScale float64
		// 并发数
		flying int64
		// 滑动平滑并发数
		avgFlying float64
		// 自旋锁，一个服务共用一个降载
		// 统计当前正在处理的请求数时必须加锁
		// 无损并发，提高性能
		avgFlyingLock syncx.SpinLock
		// 最后一次拒绝时间
		overloadTime *syncx.AtomicDuration
		// 最近是否被拒绝过
		droppedRecently *syncx.AtomicBool
		// 请求数统计，通过滑动时间窗口记录最近一段时间内指标
		passCounter *collection.RollingWindow[int64, *collection.Bucket[int64]]
		// 响应时间统计，通过滑动时间窗口记录最近一段时间内指标
		rtCounter *collection.RollingWindow[int64, *collection.Bucket[int64]]
	}
)

// Disable lets callers disable load shedding.
func Disable() {
	enabled.Set(false)
}

// DisableLog disables the stat logs for load shedding.
func DisableLog() {
	logEnabled.Set(false)
}

// NewAdaptiveShedder returns an adaptive shedder.
// opts can be used to customize the Shedder.
func NewAdaptiveShedder(opts ...ShedderOption) Shedder {
	// 为了保证代码统一
	// 当开发者关闭时返回默认的空实现，实现代码统一
	// go-zero很多地方都采用了这种设计，比如Breaker，日志组件
	if !enabled.True() {
		return newNopShedder()
	}

	// options模式设置可选配置参数
	options := shedderOptions{
		// 默认统计最近5s内数据
		window: defaultWindow,
		// 默认桶数量50个
		buckets: defaultBuckets,
		// cpu负载
		cpuThreshold: defaultCpuThreshold,
	}
	for _, opt := range opts {
		opt(&options)
	}
	// 计算每个窗口间隔时间，默认为100ms
	bucketDuration := options.window / time.Duration(options.buckets)
	newBucket := func() *collection.Bucket[int64] {
		return new(collection.Bucket[int64])
	}
	return &adaptiveShedder{
		// cpu负载
		cpuThreshold: options.cpuThreshold,
		// 1s的时间内包含多少个滑动窗口单元
		windowScale: float64(time.Second) / float64(bucketDuration) / millisecondsPerSecond,
		// 过载的时间
		overloadTime: syncx.NewAtomicDuration(),
		// 最近是否被拒绝过
		droppedRecently: syncx.NewAtomicBool(),
		// qps统计，滑动时间窗口
		// 忽略当前正在写入窗口（桶），时间周期不完整可能导致数据异常
		passCounter: collection.NewRollingWindow[int64, *collection.Bucket[int64]](newBucket, options.buckets, bucketDuration, collection.IgnoreCurrentBucket[int64, *collection.Bucket[int64]]()),
		// 响应时间统计，滑动时间窗口
		// 忽略当前正在写入窗口（桶），时间周期不完整可能导致数据异常
		rtCounter: collection.NewRollingWindow[int64, *collection.Bucket[int64]](newBucket, options.buckets, bucketDuration, collection.IgnoreCurrentBucket[int64, *collection.Bucket[int64]]()),
	}
}

// Allow implements Shedder.Allow.
func (as *adaptiveShedder) Allow() (Promise, error) {
	// 检查请求是否被丢弃
	if as.shouldDrop() {
		// 最近已被drop
		as.droppedRecently.Set(true)
		// 返回过载
		return nil, ErrServiceOverloaded
	}

	// 正在处理请求数加1
	as.addFlying(1)

	// 这里每个允许的请求都会返回一个新的promise对象
	// promise内部持有了降载指针对象
	return &promise{
		start:   timex.Now(),
		shedder: as,
	}, nil
}

func (as *adaptiveShedder) addFlying(delta int64) {
	flying := atomic.AddInt64(&as.flying, delta)
	// update avgFlying when the request is finished.
	// this strategy makes avgFlying have a little bit of lag against flying, and smoother.
	// when the flying requests increase rapidly, avgFlying increase slower, accept more requests.
	// when the flying requests drop rapidly, avgFlying drop slower, accept fewer requests.
	// it makes the service to serve as many requests as possible.
	// 请求结束后，统计当前正在处理的请求并发
	if delta < 0 {
		as.avgFlyingLock.Lock()
		// 估算当前服务近一段时间内的平均请求数
		as.avgFlying = as.avgFlying*flyingBeta + float64(flying)*(1-flyingBeta)
		as.avgFlyingLock.Unlock()
	}
}

func (as *adaptiveShedder) highThru() bool {
	// 加锁
	as.avgFlyingLock.Lock()
	// 获取滑动平均值
	// 每次请求结束后更新
	avgFlying := as.avgFlying
	// 解锁
	as.avgFlyingLock.Unlock()
	// 系统此时最大并发数
	maxFlight := as.maxFlight() * as.overloadFactor()
	// 正在处理的并发数和平均并发数是否大于系统的最大并发数
	return avgFlying > maxFlight && float64(atomic.LoadInt64(&as.flying)) > maxFlight
}

func (as *adaptiveShedder) maxFlight() float64 {
	// windows = buckets per second
	// maxQPS = maxPASS * windows
	// minRT = min average response time in milliseconds
	// allowedFlying = maxQPS * minRT / milliseconds_per_second
	maxFlight := float64(as.maxPass()) * as.minRt() * as.windowScale
	return mathx.AtLeast(maxFlight, 1)
}

func (as *adaptiveShedder) maxPass() int64 {
	var result int64 = 1

	as.passCounter.Reduce(func(b *collection.Bucket[int64]) {
		if b.Sum > result {
			result = b.Sum
		}
	})

	return result
}

func (as *adaptiveShedder) minRt() float64 {
	// if no requests in previous windows, return defaultMinRt,
	// its a reasonable large value to avoid dropping requests.
	result := defaultMinRt

	as.rtCounter.Reduce(func(b *collection.Bucket[int64]) {
		if b.Count <= 0 {
			return
		}

		avg := math.Round(float64(b.Sum) / float64(b.Count))
		if avg < result {
			result = avg
		}
	})

	return result
}

func (as *adaptiveShedder) overloadFactor() float64 {
	// as.cpuThreshold must be less than cpuMax
	factor := (cpuMax - float64(stat.CpuUsage())) / (cpuMax - float64(as.cpuThreshold))
	// at least accept 10% of acceptable requests, even cpu is highly overloaded.
	return mathx.Between(factor, overloadFactorLowerBound, 1)
}

// 请求是否应该被丢弃
func (as *adaptiveShedder) shouldDrop() bool {
	// 当前cpu负载超过阈值
	// 服务处于冷却期内应该继续检查负载并尝试丢弃请求
	if as.systemOverloaded() || as.stillHot() {
		// 检查正在处理的并发是否超出当前可承载的最大并发数
		// 超出则丢弃请求
		if as.highThru() {
			flying := atomic.LoadInt64(&as.flying)
			as.avgFlyingLock.Lock()
			avgFlying := as.avgFlying
			as.avgFlyingLock.Unlock()
			msg := fmt.Sprintf(
				"dropreq, cpu: %d, maxPass: %d, minRt: %.2f, hot: %t, flying: %d, avgFlying: %.2f",
				stat.CpuUsage(), as.maxPass(), as.minRt(), as.stillHot(), flying, avgFlying)
			logx.Error(msg)
			stat.Report(msg)
			return true
		}
	}

	return false
}

func (as *adaptiveShedder) stillHot() bool {
	// 最近没有丢弃请求
	// 说明服务正常
	if !as.droppedRecently.True() {
		return false
	}

	// 不在冷却期
	overloadTime := as.overloadTime.Load()
	if overloadTime == 0 {
		return false
	}

	// 冷却时间默认为1s
	if timex.Since(overloadTime) < coolOffDuration {
		return true
	}

	// 不在冷却期，正常处理请求中
	as.droppedRecently.Set(false)
	return false
}

// cpu 是否过载
func (as *adaptiveShedder) systemOverloaded() bool {
	if !systemOverloadChecker(as.cpuThreshold) {
		return false
	}

	as.overloadTime.Set(timex.Now())
	return true
}

// WithBuckets customizes the Shedder with the given number of buckets.
func WithBuckets(buckets int) ShedderOption {
	return func(opts *shedderOptions) {
		opts.buckets = buckets
	}
}

// WithCpuThreshold customizes the Shedder with the given cpu threshold.
func WithCpuThreshold(threshold int64) ShedderOption {
	return func(opts *shedderOptions) {
		opts.cpuThreshold = threshold
	}
}

// WithWindow customizes the Shedder with given
func WithWindow(window time.Duration) ShedderOption {
	return func(opts *shedderOptions) {
		opts.window = window
	}
}

type promise struct {
	// 请求开始时间
	start time.Duration
	// 统计请求处理耗时
	shedder *adaptiveShedder
}

func (p *promise) Fail() {
	// 请求结束，当前正在处理请求数-1
	p.shedder.addFlying(-1)
}

func (p *promise) Pass() {
	// 响应时间，单位毫秒
	rt := float64(timex.Since(p.start)) / float64(time.Millisecond)
	// 请求结束，当前正在处理请求数-1
	p.shedder.addFlying(-1)
	p.shedder.rtCounter.Add(int64(math.Ceil(rt)))
	p.shedder.passCounter.Add(1)
}
