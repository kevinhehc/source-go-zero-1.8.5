package clientinterceptors

import (
	"context"
	"path"

	"github.com/zeromicro/go-zero/core/breaker"
	"github.com/zeromicro/go-zero/zrpc/internal/codes"
	"google.golang.org/grpc"
)

// BreakerInterceptor is an interceptor that acts as a circuit breaker.
func BreakerInterceptor(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// 根据方法名获取或创建一个熔断器
	breakerName := path.Join(cc.Target(), method)
	// 执行熔断逻辑
	return breaker.DoWithAcceptableCtx(ctx, breakerName, func() error {
		// 如果熔断器允许请求通过，则调用真正的 gRPC invoker
		return invoker(ctx, method, req, reply, cc, opts...)
		// isAcceptable 是一个函数，用来判断哪些错误可以被熔断器忽略
	}, codes.Acceptable)
}
