package serverinterceptors

import (
	"context"
	"errors"
	"sync"

	"github.com/zeromicro/go-zero/core/load"
	"github.com/zeromicro/go-zero/core/stat"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const serviceType = "rpc"

var (
	sheddingStat *load.SheddingStat
	lock         sync.Mutex
)

// UnarySheddingInterceptor returns a func that does load shedding on processing unary requests.
func UnarySheddingInterceptor(shedder load.Shedder, metrics *stat.Metrics) grpc.UnaryServerInterceptor {
	ensureSheddingStat()

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (val any, err error) {
		sheddingStat.IncrementTotal()
		var promise load.Promise
		// 检查是否被降载
		promise, err = shedder.Allow()
		// 降载，记录相关日志与指标
		if err != nil {
			metrics.AddDrop()
			sheddingStat.IncrementDrop()
			err = status.Error(codes.ResourceExhausted, err.Error())
			return
		}

		// 最后回调执行结果
		defer func() {
			// 执行失败
			if errors.Is(err, context.DeadlineExceeded) {
				promise.Fail()
			} else {
				// 执行成功
				sheddingStat.IncrementPass()
				promise.Pass()
			}
		}()

		// 执行业务方法
		return handler(ctx, req)
	}
}

func ensureSheddingStat() {
	lock.Lock()
	if sheddingStat == nil {
		sheddingStat = load.NewSheddingStat(serviceType)
	}
	lock.Unlock()
}
