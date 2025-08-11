package mr

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/zeromicro/go-zero/core/errorx"
)

const (
	defaultWorkers = 16
	minWorkers     = 1
)

var (
	// ErrCancelWithNil is an error that mapreduce was cancelled with nil.
	ErrCancelWithNil = errors.New("mapreduce cancelled with nil")
	// ErrReduceNoOutput is an error that reduce did not output a value.
	ErrReduceNoOutput = errors.New("reduce not writing value")
)

type (
	// ForEachFunc is used to do element processing, but no output.
	ForEachFunc[T any] func(item T)
	// GenerateFunc is used to let callers send elements into source.
	// 数据生产func
	// source - 数据被生产后写入source
	GenerateFunc[T any] func(source chan<- T)
	// MapFunc is used to do element processing and write the output to writer.
	// 数据加工func
	// item - 生产出来的数据
	// writer - 调用writer.Write()可以将加工后的向后传递至reducer
	// cancel - 终止流程func
	MapFunc[T, U any] func(item T, writer Writer[U])
	// MapperFunc is used to do element processing and write the output to writer,
	// use cancel func to cancel the processing.
	MapperFunc[T, U any] func(item T, writer Writer[U], cancel func(error))
	// ReducerFunc is used to reduce all the mapping output and write to writer,
	// use cancel func to cancel the processing.
	// 数据聚合func
	// pipe - 加工出来的数据
	// writer - 调用writer.Write()可以将聚合后的数据返回给用户
	// cancel - 终止流程func
	ReducerFunc[U, V any] func(pipe <-chan U, writer Writer[V], cancel func(error))
	// VoidReducerFunc is used to reduce all the mapping output, but no output.
	// Use cancel func to cancel the processing.
	VoidReducerFunc[U any] func(pipe <-chan U, cancel func(error))
	// Option defines the method to customize the mapreduce.
	Option func(opts *mapReduceOptions)

	mapperContext[T, U any] struct {
		ctx       context.Context
		mapper    MapFunc[T, U]
		source    <-chan T
		panicChan *onceChan
		collector chan<- U
		doneChan  <-chan struct{}
		workers   int
	}

	mapReduceOptions struct {
		ctx     context.Context
		workers int
	}

	// Writer interface wraps Write method.
	Writer[T any] interface {
		Write(v T)
	}
)

// Finish runs fns parallelly, cancelled on any error.
// 并发执行func，发生任何错误将会立即终止流程
func Finish(fns ...func() error) error {
	if len(fns) == 0 {
		return nil
	}

	return MapReduceVoid(func(source chan<- func() error) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(fn func() error, writer Writer[any], cancel func(error)) {
		if err := fn(); err != nil {
			cancel(err)
		}
	}, func(pipe <-chan any, cancel func(error)) {
	}, WithWorkers(len(fns)))
}

// FinishVoid runs fns parallelly.
func FinishVoid(fns ...func()) {
	if len(fns) == 0 {
		return
	}

	ForEach(func(source chan<- func()) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(fn func()) {
		fn()
	}, WithWorkers(len(fns)))
}

// ForEach maps all elements from given generate but no output.
func ForEach[T any](generate GenerateFunc[T], mapper ForEachFunc[T], opts ...Option) {
	options := buildOptions(opts...)
	panicChan := &onceChan{channel: make(chan any)}
	source := buildSource(generate, panicChan)
	collector := make(chan any)
	done := make(chan struct{})

	go executeMappers(mapperContext[T, any]{
		ctx: options.ctx,
		mapper: func(item T, _ Writer[any]) {
			mapper(item)
		},
		source:    source,
		panicChan: panicChan,
		collector: collector,
		doneChan:  done,
		workers:   options.workers,
	})

	for {
		select {
		case v := <-panicChan.channel:
			// 如果 panicChan.channel 收到值（通常是某个 worker 任务 panic 后传来的），会立刻 panic 退出整个 ForEach。
			panic(v)
		case _, ok := <-collector:
			// 当所有数据处理完毕且 executeMappers 关闭 collector，ok 会变成 false，然后 ForEach 才会正常 return。
			if !ok {
				return
			}
		}
	}
}

// MapReduce maps all elements generated from given generate func,
// and reduces the output elements with given reducer.
func MapReduce[T, U, V any](generate GenerateFunc[T], mapper MapperFunc[T, U], reducer ReducerFunc[U, V],
	opts ...Option) (V, error) {
	panicChan := &onceChan{channel: make(chan any)}
	source := buildSource(generate, panicChan)
	return mapReduceWithPanicChan(source, panicChan, mapper, reducer, opts...)
}

// MapReduceChan maps all elements from source, and reduce the output elements with given reducer.
func MapReduceChan[T, U, V any](source <-chan T, mapper MapperFunc[T, U], reducer ReducerFunc[U, V],
	opts ...Option) (V, error) {
	panicChan := &onceChan{channel: make(chan any)}
	return mapReduceWithPanicChan(source, panicChan, mapper, reducer, opts...)
}

// MapReduceVoid maps all elements generated from given generate,
// and reduce the output elements with given reducer.
func MapReduceVoid[T, U any](generate GenerateFunc[T], mapper MapperFunc[T, U],
	reducer VoidReducerFunc[U], opts ...Option) error {
	_, err := MapReduce(generate, mapper, func(input <-chan U, writer Writer[any], cancel func(error)) {
		reducer(input, cancel)
	}, opts...)
	if errors.Is(err, ErrReduceNoOutput) {
		return nil
	}

	return err
}

// WithContext customizes a mapreduce processing accepts a given ctx.
func WithContext(ctx context.Context) Option {
	return func(opts *mapReduceOptions) {
		opts.ctx = ctx
	}
}

// WithWorkers customizes a mapreduce processing with given workers.
func WithWorkers(workers int) Option {
	return func(opts *mapReduceOptions) {
		if workers < minWorkers {
			opts.workers = minWorkers
		} else {
			opts.workers = workers
		}
	}
}

func buildOptions(opts ...Option) *mapReduceOptions {
	options := newOptions()
	for _, opt := range opts {
		opt(options)
	}

	return options
}

func buildSource[T any](generate GenerateFunc[T], panicChan *onceChan) chan T {
	source := make(chan T)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicChan.write(r)
			}
			close(source)
		}()

		generate(source)
	}()

	return source
}

// drain drains the channel.
func drain[T any](channel <-chan T) {
	// drain the channel
	for range channel {
	}
}

func executeMappers[T, U any](mCtx mapperContext[T, U]) {
	var wg sync.WaitGroup
	defer func() {
		// 保证所有的item都处理完成
		wg.Wait()
		close(mCtx.collector)
		drain(mCtx.source)
	}()

	var failed int32
	pool := make(chan struct{}, mCtx.workers)
	// 将mapper处理完的数据写入collector
	writer := newGuardedWriter(mCtx.ctx, mCtx.collector, mCtx.doneChan)
	for atomic.LoadInt32(&failed) == 0 {
		select {
		// 当调用了cancel会触发立即返回
		case <-mCtx.ctx.Done():
			return
		case <-mCtx.doneChan:
			return
		case pool <- struct{}{}:
			item, ok := <-mCtx.source
			if !ok {
				<-pool
				return
			}

			wg.Add(1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						atomic.AddInt32(&failed, 1)
						mCtx.panicChan.write(r)
					}
					wg.Done()
					<-pool
				}()
				// 对item进行处理，处理完调用writer.Write把结果写入collector对应的channel中
				mCtx.mapper(item, writer)
			}()
		}
	}
}

// mapReduceWithPanicChan maps all elements from source, and reduce the output elements with given reducer.
func mapReduceWithPanicChan[T, U, V any](source <-chan T, panicChan *onceChan, mapper MapperFunc[T, U],
	reducer ReducerFunc[U, V], opts ...Option) (val V, err error) {
	// 构造option
	options := buildOptions(opts...)
	// output is used to write the final result
	// 构造 output
	output := make(chan V)
	defer func() {
		// reducer can only write once, if more, panic
		// 因为output在defer之前读了一次了，如果再读一次还有数据，证明写了两次
		for range output {
			panic("more than one element written in reducer")
		}
	}()

	// collector is used to collect data from mapper, and consume in reducer
	// 从 consume 收集的结果存储
	collector := make(chan U, options.workers)
	// if done is closed, all mappers and reducer should stop processing
	// 如果 done了，那所有的 mapper和 reducer都会停止处理
	done := make(chan struct{})
	writer := newGuardedWriter(options.ctx, output, done)
	var closeOnce sync.Once
	// use atomic type to avoid data race
	var retErr errorx.AtomicError
	// 如果结束，关闭对应的通道
	finish := func() {
		closeOnce.Do(func() {
			close(done)
			close(output)
		})
	}
	// 如果取消，1、赋值错误，2、把source数据读出来，关闭相关的通道
	cancel := once(func(err error) {
		if err != nil {
			retErr.Set(err)
		} else {
			retErr.Set(ErrCancelWithNil)
		}

		drain(source)
		finish()
	})

	// 开启协程，把结果汇集到 output
	go func() {
		defer func() {
			drain(collector)
			if r := recover(); r != nil {
				panicChan.write(r)
			}
			finish()
		}()
		// reducer单goroutine对数mapper写入collector的数据进行处理，
		// 如果reducer中没有手动调用writer.Write则最终会执行finish方法对output进行close避免死锁
		reducer(collector, writer, cancel)
	}()

	// 批量处理任务
	go executeMappers(mapperContext[T, U]{
		ctx: options.ctx,
		mapper: func(item T, w Writer[U]) {
			mapper(item, w, cancel)
		},
		source:    source,
		panicChan: panicChan,
		collector: collector,
		doneChan:  done,
		workers:   options.workers,
	})

	// 结果处理
	select {
	case <-options.ctx.Done():
		// 如果 context done，那就取消all
		cancel(context.DeadlineExceeded)
		err = context.DeadlineExceeded
	case v := <-panicChan.channel:
		// drain output here, otherwise for loop panic in defer
		// 异常了，把output读出来
		drain(output)
		panic(v)
	case v, ok := <-output:
		// 如果是output有数据了
		if e := retErr.Load(); e != nil {
			// 有异常，那赋值异常
			err = e
		} else if ok {
			// 没异常，赋值数据
			val = v
		} else {
			// 没有输出
			err = ErrReduceNoOutput
		}
	}

	return
}

// 构造 option
func newOptions() *mapReduceOptions {
	return &mapReduceOptions{
		ctx:     context.Background(),
		workers: defaultWorkers,
	}
}

// 进行简单的封装
func once(fn func(error)) func(error) {
	once := new(sync.Once)
	return func(err error) {
		once.Do(func() {
			fn(err)
		})
	}
}

// 方便监控是否结束。如果有任何的结束，都不写结果了
type guardedWriter[T any] struct {
	ctx     context.Context
	channel chan<- T
	done    <-chan struct{}
}

// 方便监控是否结束。如果有任何的结束，都不写结果了
func newGuardedWriter[T any](ctx context.Context, channel chan<- T, done <-chan struct{}) guardedWriter[T] {
	return guardedWriter[T]{
		ctx:     ctx,
		channel: channel,
		done:    done,
	}
}

// 方便监控是否结束。如果有任何的结束，都不写结果了
func (gw guardedWriter[T]) Write(v T) {
	select {
	case <-gw.ctx.Done():
	case <-gw.done:
	default:
		gw.channel <- v
	}
}

type onceChan struct {
	channel chan any
	// 0 代表没写过数据进去
	// 1 代表写了数据进去，不能写了。
	wrote int32
}

// 保证异常只会被写一次
func (oc *onceChan) write(val any) {
	if atomic.CompareAndSwapInt32(&oc.wrote, 0, 1) {
		oc.channel <- val
	}
}
