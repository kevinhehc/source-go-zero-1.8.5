//go:build linux || darwin || freebsd

package proc

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

const (
	// 临时性能剖析（profile）持续时长
	profileDuration = time.Minute
	// 时间格式字符串（MMDDHHMMSS），通常用来生成文件名
	timeFormat = "0102150405"
)

var done = make(chan struct{})

func init() {
	// 启动一个后台 goroutine
	go func() {
		// https://golang.org/pkg/os/signal/#Notify
		// 建一个带 1 个缓冲的 signals 通道
		signals := make(chan os.Signal, 1)
		// 用 signal.Notify 订阅：SIGUSR1, SIGUSR2, SIGTERM, SIGINT
		signal.Notify(signals, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGINT)

		for {
			v := <-signals
			switch v {
			case syscall.SIGUSR1:
				// 调用 dumpGoroutines(fileCreator{}) —— 通常把所有 goroutine 的栈信息 dump 到文件，定位死锁/卡顿。
				dumpGoroutines(fileCreator{})
			case syscall.SIGUSR2:
				// 做一次 1 分钟的性能剖析
				// （开始 pprof 等）
				profiler := StartProfile()
				// 到时自动停止。
				time.AfterFunc(profileDuration, profiler.Stop)
			case syscall.SIGTERM:
				//关闭 done
				stopOnSignal()
				// 执行框架自带的优雅退出逻辑（例如停止接入、等待在途请求完成等）。
				gracefulStop(signals, syscall.SIGTERM)
			case syscall.SIGINT:
				stopOnSignal()
				gracefulStop(signals, syscall.SIGINT)
			default:
				logx.Error("Got unregistered signal:", v)
			}
		}
	}()
}

// Done returns the channel that notifies the process quitting.
func Done() <-chan struct{} {
	return done
}

func stopOnSignal() {
	select {
	case <-done:
		// already closed
	default:
		close(done)
	}
}
