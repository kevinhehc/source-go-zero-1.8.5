//go:build linux || darwin || freebsd

package proc

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/threading"
)

// 进程管理和优雅退出（graceful shutdown）的实现。确保服务在停止时能处理完正在进行的请求。

const (
	// defaultWrapUpTime is the default time to wait before calling wrap up listeners.
	// defaultWrapUpTime：触发优雅退出后，先给 WrapUp 监听器的缓冲时间（默认 1s）。
	defaultWrapUpTime = time.Second
	// defaultWaitTime is the default time to wait before force quitting.
	// why we use 5500 milliseconds is because most of our queues are blocking mode with 5 seconds

	// defaultWaitTime：从收到退出信号开始到“强制退出”的总等待时间（默认 5.5s）。
	// 之所以是 5500ms，是因为大多数队列是 5s 阻塞拉取，额外留 0.5s 收尾。
	defaultWaitTime = 5500 * time.Millisecond
)

var (
	// 收到退出信号后，先调用（比如：停止接入新请求、打“只读”标志、停止拉取队列等）。
	wrapUpListeners = new(listenerManager)
	// WrapUp 之后、真正退出前调用（比如：flush 日志、写回快照、关闭连接等）。
	shutdownListeners = new(listenerManager)
	// 触发 WrapUp 监听器后等待的时间（默认 1s）。
	wrapUpTime = defaultWrapUpTime
	// 从收到信号开始到强杀之间的总等待时长（默认 5.5s；因为框架里很多队列是 5s 阻塞，留 0.5s 余量）。
	waitTime = defaultWaitTime
	// 保护全局时间参数修改的锁
	shutdownLock sync.Mutex
)

// ShutdownConf defines the shutdown configuration for the process.
// ShutdownConf 用来配置优雅退出的两个窗口期。
type ShutdownConf struct {
	// WrapUpTime is the time to wait before calling shutdown listeners.
	// WrapUpTime：先调用 WrapUp 监听器后等待的时长
	WrapUpTime time.Duration `json:",default=1s"`
	// WaitTime is the time to wait before force quitting.
	// WaitTime：从收到信号起到强杀的总等待时长
	WaitTime time.Duration `json:",default=5.5s"`
}

// AddShutdownListener adds fn as a shutdown listener.
// The returned func can be used to wait for fn getting called.
// AddShutdownListener 注册一个 Shutdown 监听器。
// 返回值是一个等待函数，调用它会阻塞到“所有 Shutdown 监听器都执行完”为止（常用于测试）。
func AddShutdownListener(fn func()) (waitForCalled func()) {
	return shutdownListeners.addListener(fn)
}

// AddWrapUpListener adds fn as a wrap up listener.
// The returned func can be used to wait for fn getting called.
// AddWrapUpListener 注册一个 WrapUp 监听器。
// 返回值同上：等待“所有 WrapUp 监听器执行完”。
func AddWrapUpListener(fn func()) (waitForCalled func()) {
	return wrapUpListeners.addListener(fn)
}

// SetTimeToForceQuit sets the waiting time before force quitting.
// SetTimeToForceQuit 线程安全地设置强杀前的总等待时间（即 waitTime）。
func SetTimeToForceQuit(duration time.Duration) {
	shutdownLock.Lock()
	defer shutdownLock.Unlock()
	waitTime = duration
}

// Setup 批量设置 WrapUpTime 和 WaitTime，忽略非正值（保持默认/已有值）。
func Setup(conf ShutdownConf) {
	shutdownLock.Lock()
	defer shutdownLock.Unlock()

	if conf.WrapUpTime > 0 {
		wrapUpTime = conf.WrapUpTime
	}
	if conf.WaitTime > 0 {
		waitTime = conf.WaitTime
	}
}

// Shutdown calls the registered shutdown listeners, only for test purpose.
// Shutdown 仅用于测试：直接触发 Shutdown 监听器。
func Shutdown() {
	shutdownListeners.notifyListeners()
}

// WrapUp wraps up the process, only for test purpose.
// WrapUp 仅用于测试：直接触发 WrapUp 监听器。
func WrapUp() {
	wrapUpListeners.notifyListeners()
}

// gracefulStop 是真正的“优雅退出”执行流程：按顺序调度 WrapUp / Shutdown，并在超时后强杀。
func gracefulStop(signals chan os.Signal, sig syscall.Signal) {
	// 不再接收后续信号，防止重复触发
	signal.Stop(signals)

	logx.Infof("Got signal %d, shutting down...", sig)
	// 1) 先异步触发 WrapUp（例如：停止接入、标记只读、停止拉取队列等）
	go wrapUpListeners.notifyListeners()

	// 2) 给 WrapUp 一段缓冲时间
	time.Sleep(wrapUpTime)
	// 3) 再异步触发 Shutdown（例如：flush 日志、关闭连接、保存状态等）
	go shutdownListeners.notifyListeners()

	// 4) 计算“距离强杀”的剩余时间（waitTime 是总时间窗口）
	shutdownLock.Lock()
	remainingTime := waitTime - wrapUpTime
	shutdownLock.Unlock()

	// 5) 再等到剩余时间到了还没完全退出，就进行强杀
	time.Sleep(remainingTime)
	logx.Infof("Still alive after %v, going to force kill the process...", waitTime)
	_ = syscall.Kill(syscall.Getpid(), sig)
}

// listenerManager 负责：注册监听器、并发/安全执行、等待全部完成。
type listenerManager struct {
	lock      sync.Mutex
	waitGroup sync.WaitGroup
	listeners []func()
}

// addListener：将 fn 包装成“自动 Done 的任务”，加入列表。
// 返回的 waitForCalled() 会等待该类（WrapUp 或 Shutdown）的所有监听器都执行完毕。
func (lm *listenerManager) addListener(fn func()) (waitForCalled func()) {
	lm.waitGroup.Add(1)

	lm.lock.Lock()
	lm.listeners = append(lm.listeners, func() {
		defer lm.waitGroup.Done()
		fn()
	})
	lm.lock.Unlock()

	// we can return lm.waitGroup.Wait directly,
	// but we want to make the returned func more readable.
	// creating an extra closure would be negligible in practice.
	// 直接返回 Wait 也可以；这里包一层可读性更好
	return func() {
		lm.waitGroup.Wait()
	}
}

// notifyListeners：并发执行所有监听器，等待全部完成，最后清空列表（避免重复触发）。
func (lm *listenerManager) notifyListeners() {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	// go-zero 的并发工具，RunSafe 会 recover panic
	group := threading.NewRoutineGroup()
	for _, listener := range lm.listeners {
		group.RunSafe(listener)
	}
	group.Wait()
	// 执行完就清空，避免再次被触发，同时也有利于 GC
	lm.listeners = nil
}
