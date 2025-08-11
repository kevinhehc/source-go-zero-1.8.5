//go:build linux || darwin || freebsd

package proc

import (
	"fmt"
	"os"
	"os/signal"
	"path"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

// DefaultMemProfileRate is the default memory profiling rate.
// See also http://golang.org/pkg/runtime/#pkg-variables
// 默认的内存画像采样率：约每 4KB 分配记录一次样本（越小越重）
const DefaultMemProfileRate = 4096

// started is non zero if a profile is running.
// started：是否已开启一个全局画像会话（0=未开始，1=已开始），用原子操作保护
var started uint32

// Profile represents an active profiling session.
// Profile 表示一次“画像会话”，把所有类型的画像统一管理
type Profile struct {
	// closers holds cleanup functions that run after each profile
	// closers：每种画像对应的“停止/清理”函数，Stop 时按顺序调用
	closers []func()

	// stopped records if a call to profile.Stop has been made
	// stopped：是否已经 Stop 过，防止重复 Stop
	stopped uint32
}

func (p *Profile) close() {
	for _, closer := range p.closers {
		closer()
	}
}

// 开启阻塞画像（goroutine 在 channel/锁等处阻塞的采样）
func (p *Profile) startBlockProfile() {
	fn := createDumpFile("block")
	f, err := os.Create(fn)
	if err != nil {
		logx.Errorf("profile: could not create block profile %q: %v", fn, err)
		return
	}

	// 设置采样率为 1（每个阻塞事件都记录），Stop 时再设回 0
	runtime.SetBlockProfileRate(1)
	logx.Infof("profile: block profiling enabled, %s", fn)
	p.closers = append(p.closers, func() {
		pprof.Lookup("block").WriteTo(f, 0)
		f.Close()
		runtime.SetBlockProfileRate(0)
		logx.Infof("profile: block profiling disabled, %s", fn)
	})
}

// 开启 CPU 画像
func (p *Profile) startCpuProfile() {
	fn := createDumpFile("cpu")
	f, err := os.Create(fn)
	if err != nil {
		logx.Errorf("profile: could not create cpu profile %q: %v", fn, err)
		return
	}

	logx.Infof("profile: cpu profiling enabled, %s", fn)
	pprof.StartCPUProfile(f)
	p.closers = append(p.closers, func() {
		pprof.StopCPUProfile()
		f.Close()
		logx.Infof("profile: cpu profiling disabled, %s", fn)
	})
}

// 开启内存画像（heap）
func (p *Profile) startMemProfile() {
	fn := createDumpFile("mem")
	f, err := os.Create(fn)
	if err != nil {
		logx.Errorf("profile: could not create memory profile %q: %v", fn, err)
		return
	}

	// 记录旧的采样率，开启更细的采样
	old := runtime.MemProfileRate
	runtime.MemProfileRate = DefaultMemProfileRate
	logx.Infof("profile: memory profiling enabled (rate %d), %s", runtime.MemProfileRate, fn)
	p.closers = append(p.closers, func() {
		// 常见做法会先 runtime.GC() 让 heap 画像更及时（见下方建议）
		pprof.Lookup("heap").WriteTo(f, 0)
		f.Close()
		runtime.MemProfileRate = old
		logx.Infof("profile: memory profiling disabled, %s", fn)
	})
}

// 开启互斥锁画像（锁竞争热点）
func (p *Profile) startMutexProfile() {
	fn := createDumpFile("mutex")
	f, err := os.Create(fn)
	if err != nil {
		logx.Errorf("profile: could not create mutex profile %q: %v", fn, err)
		return
	}

	// 1 表示对阻塞在互斥锁上的 goroutine 采样
	runtime.SetMutexProfileFraction(1)
	logx.Infof("profile: mutex profiling enabled, %s", fn)
	p.closers = append(p.closers, func() {
		if mp := pprof.Lookup("mutex"); mp != nil {
			mp.WriteTo(f, 0)
		}
		f.Close()
		runtime.SetMutexProfileFraction(0)
		logx.Infof("profile: mutex profiling disabled, %s", fn)
	})
}

// 线程创建画像（goroutine/thread 创建）
func (p *Profile) startThreadCreateProfile() {
	fn := createDumpFile("threadcreate")
	f, err := os.Create(fn)
	if err != nil {
		logx.Errorf("profile: could not create threadcreate profile %q: %v", fn, err)
		return
	}

	logx.Infof("profile: threadcreate profiling enabled, %s", fn)
	p.closers = append(p.closers, func() {
		if mp := pprof.Lookup("threadcreate"); mp != nil {
			mp.WriteTo(f, 0)
		}
		f.Close()
		logx.Infof("profile: threadcreate profiling disabled, %s", fn)
	})
}

// 调度/事件 trace（与 pprof 不同，记录更细粒度的时间线）
func (p *Profile) startTraceProfile() {
	fn := createDumpFile("trace")
	f, err := os.Create(fn)
	if err != nil {
		logx.Errorf("profile: could not create trace output file %q: %v", fn, err)
		return
	}

	if err := trace.Start(f); err != nil {
		logx.Errorf("profile: could not start trace: %v", err)
		return
	}

	logx.Infof("profile: trace enabled, %s", fn)
	p.closers = append(p.closers, func() {
		trace.Stop()
		logx.Infof("profile: trace disabled, %s", fn)
	})
}

// Stop stops the profile and flushes any unwritten data.
func (p *Profile) Stop() {
	if !atomic.CompareAndSwapUint32(&p.stopped, 0, 1) {
		// someone has already called close
		return
	}
	p.close()
	atomic.StoreUint32(&started, 0)
}

// StartProfile starts a new profiling session.
// The caller should call the Stop method on the value returned
// to cleanly stop profiling.
func StartProfile() Stopper {
	if !atomic.CompareAndSwapUint32(&started, 0, 1) {
		logx.Error("profile: Start() already called")
		return noopStopper
	}

	var prof Profile
	prof.startCpuProfile()
	prof.startMemProfile()
	prof.startMutexProfile()
	prof.startBlockProfile()
	prof.startTraceProfile()
	prof.startThreadCreateProfile()

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT)
		<-c

		logx.Info("profile: caught interrupt, stopping profiles")
		prof.Stop()

		signal.Reset()
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()

	return &prof
}

func createDumpFile(kind string) string {
	command := path.Base(os.Args[0])
	pid := syscall.Getpid()
	return path.Join(os.TempDir(), fmt.Sprintf("%s-%d-%s-%s.pprof",
		command, pid, kind, time.Now().Format(timeFormat)))
}
