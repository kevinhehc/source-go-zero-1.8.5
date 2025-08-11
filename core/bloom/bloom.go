package bloom

import (
	"context"
	_ "embed"
	"errors"
	"strconv"

	"github.com/zeromicro/go-zero/core/hash"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

// for detailed error rate table, see http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
// maps as k in the error rate table
// 表示经过多少散列函数计算
// 固定14次
const maps = 14

var (
	// ErrTooLargeOffset indicates the offset is too large in bitset.
	ErrTooLargeOffset = errors.New("too large offset")

	//go:embed setscript.lua
	setLuaScript string
	setScript    = redis.NewScript(setLuaScript)

	//go:embed testscript.lua
	testLuaScript string
	testScript    = redis.NewScript(testLuaScript)
)

type (
	// A Filter is a bloom filter.
	// 定义布隆过滤器结构体
	Filter struct {
		bits   uint
		bitSet bitSetProvider
	}

	// 位数组操作接口定义
	bitSetProvider interface {
		check(ctx context.Context, offsets []uint) (bool, error)
		set(ctx context.Context, offsets []uint) error
	}
)

// New create a Filter, store is the backed redis, key is the key for the bloom filter,
// bits is how many bits will be used, maps is how many hashes for each addition.
// best practices:
// elements - means how many actual elements
// when maps = 14, formula: 0.7*(bits/maps), bits = 20*elements, the error rate is 0.000067 < 1e-4
// for detailed error rate table, see http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
func New(store *redis.Redis, key string, bits uint) *Filter {
	return &Filter{
		bits:   bits,
		bitSet: newRedisBitSet(store, key, bits),
	}
}

// Add adds data into f.
func (f *Filter) Add(data []byte) error {
	return f.AddCtx(context.Background(), data)
}

// AddCtx adds data into f with context.
func (f *Filter) AddCtx(ctx context.Context, data []byte) error {
	locations := f.getLocations(data)
	return f.bitSet.set(ctx, locations)
}

// Exists checks if data is in f.
func (f *Filter) Exists(data []byte) (bool, error) {
	return f.ExistsCtx(context.Background(), data)
}

// ExistsCtx checks if data is in f with context.
func (f *Filter) ExistsCtx(ctx context.Context, data []byte) (bool, error) {
	locations := f.getLocations(data)
	isSet, err := f.bitSet.check(ctx, locations)
	if err != nil {
		return false, err
	}

	return isSet, nil
}

// k次散列计算出k个offset
func (f *Filter) getLocations(data []byte) []uint {
	//创建指定容量的切片
	locations := make([]uint, maps)
	//maps表示k值,作者定义为了常量:14
	for i := uint(0); i < maps; i++ {
		// 哈希计算,使用的是"MurmurHash3"算法,并每次追加一个固定的i字节进行计算
		hashValue := hash.Hash(append(data, byte(i)))
		//取下标offset
		locations[i] = uint(hashValue % uint64(f.bits))
	}

	return locations
}

// redis位数组
type redisBitSet struct {
	store *redis.Redis
	key   string
	bits  uint
}

func newRedisBitSet(store *redis.Redis, key string, bits uint) *redisBitSet {
	return &redisBitSet{
		store: store,
		key:   key,
		bits:  bits,
	}
}

// 构建偏移量offset字符串数组,因为go-redis执行lua脚本时参数定义为[]stringy
// 因此需要转换一下
func (r *redisBitSet) buildOffsetArgs(offsets []uint) ([]string, error) {
	args := make([]string, 0, len(offsets))

	for _, offset := range offsets {
		if offset >= r.bits {
			return nil, ErrTooLargeOffset
		}

		args = append(args, strconv.FormatUint(uint64(offset), 10))
	}

	return args, nil
}

// 检查偏移量offset数组是否全部为1
// 是:元素可能存在
// 否:元素一定不存在
func (r *redisBitSet) check(ctx context.Context, offsets []uint) (bool, error) {
	args, err := r.buildOffsetArgs(offsets)
	if err != nil {
		return false, err
	}

	// 执行脚本
	resp, err := r.store.ScriptRunCtx(ctx, testScript, []string{r.key}, args)

	// 这里需要注意一下,底层使用的go-redis
	// redis.Nil表示key不存在的情况需特殊判断
	if errors.Is(err, redis.Nil) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	exists, ok := resp.(int64)
	if !ok {
		return false, nil
	}

	return exists == 1, nil
}

// del only use for testing.
// 删除
func (r *redisBitSet) del() error {
	_, err := r.store.Del(r.key)
	return err
}

// expire only use for testing.
// 自动过期
func (r *redisBitSet) expire(seconds int) error {
	return r.store.Expire(r.key, seconds)
}

// 将k位点全部设置为1
func (r *redisBitSet) set(ctx context.Context, offsets []uint) error {
	args, err := r.buildOffsetArgs(offsets)
	// 底层使用的是go-redis,redis.Nil表示操作的key不存在
	// 需要针对key不存在的情况特殊判断
	if err != nil {
		return err
	}

	_, err = r.store.ScriptRunCtx(ctx, setScript, []string{r.key}, args)
	if errors.Is(err, redis.Nil) {
		return nil
	}

	return err
}
