package threading

import (
	"bytes"
	"context"
	"runtime"
	"strconv"

	"github.com/zeromicro/go-zero/core/rescue"
)

// GoSafe runs the given fn using another goroutine, recovers if fn panics.
func GoSafe(fn func()) {
	go RunSafe(fn)
}

// GoSafeCtx runs the given fn using another goroutine, recovers if fn panics with ctx.
func GoSafeCtx(ctx context.Context, fn func()) {
	go RunSafeCtx(ctx, fn)
}

// RoutineId is only for debug, never use it in production.
// 获取当前 goroutine 的 ID，并以 uint64 的形式返回。
// 虽然 Go 语言官方并不鼓励通过这种方式获取 goroutine ID（因为 goroutine ID 是设计为对程序员透明的），
// 但这段代码确实可以通过解析运行时栈信息来“间接”拿到它。
func RoutineId() uint64 {
	// 创建一个长度为 64 的 byte 切片，用于接收栈信息。
	b := make([]byte, 64)
	// 调用 runtime.Stack(buf, false)，将当前 goroutine 的栈信息写入 b 中。
	// 第二个参数为 false 表示只获取当前 goroutine 的栈信息。
	// 返回值是实际写入的字节数，所以 b = b[:n] 把切片截断为实际有用的部分。
	b = b[:runtime.Stack(b, false)] // demo: goroutine 12345 [running]:
	// 去掉前缀 "goroutine "
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	// 找到第一个空格的位置，并截取前面的部分，也就是 goroutine ID 的字符串 "12345"。
	b = b[:bytes.IndexByte(b, ' ')]
	// if error, just return 0
	n, _ := strconv.ParseUint(string(b), 10, 64)

	return n
}

// RunSafe runs the given fn, recovers if fn panics.
func RunSafe(fn func()) {
	defer rescue.Recover()

	fn()
}

// RunSafeCtx runs the given fn, recovers if fn panics with ctx.
func RunSafeCtx(ctx context.Context, fn func()) {
	defer rescue.RecoverCtx(ctx)

	fn()
}
