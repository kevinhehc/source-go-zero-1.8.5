package hash

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/zeromicro/go-zero/core/lang"
)

const (
	// TopWeight is the top weight that one entry might set.
	// 权重
	TopWeight = 100

	// 最少得虚拟节点数量
	minReplicas = 100

	// 用作散列
	prime = 16777619
)

type (
	// Func defines the hash method.
	// 哈希函数
	Func func(data []byte) uint64

	// A ConsistentHash is a ring hash implementation.
	// 一致性哈希
	ConsistentHash struct {
		// 哈希函数
		hashFunc Func
		// 确定node的虚拟节点数量
		replicas int
		// 虚拟节点列表
		keys []uint64
		// 虚拟节点到物理节点的映射
		ring map[uint64][]any
		// 物理节点映射，快速判断是否存在node
		nodes map[string]lang.PlaceholderType
		// 读写锁
		lock sync.RWMutex
	}
)

// NewConsistentHash returns a ConsistentHash.
func NewConsistentHash() *ConsistentHash {
	return NewCustomConsistentHash(minReplicas, Hash)
}

// NewCustomConsistentHash returns a ConsistentHash with given replicas and hash func.
func NewCustomConsistentHash(replicas int, fn Func) *ConsistentHash {
	if replicas < minReplicas {
		replicas = minReplicas
	}

	if fn == nil {
		fn = Hash
	}

	return &ConsistentHash{
		hashFunc: fn,
		replicas: replicas,
		ring:     make(map[uint64][]any),
		nodes:    make(map[string]lang.PlaceholderType),
	}
}

// Add adds the node with the number of h.replicas,
// the later call will overwrite the replicas of the former calls.
// 扩容操作，增加物理节点
func (h *ConsistentHash) Add(node any) {
	h.AddWithReplicas(node, h.replicas)
}

// AddWithReplicas adds the node with the number of replicas,
// replicas will be truncated to h.replicas if it's larger than h.replicas,
// the later call will overwrite the replicas of the former calls.
func (h *ConsistentHash) AddWithReplicas(node any, replicas int) {
	// 支持可重复添加
	// 先执行删除操作
	h.Remove(node)

	// 不能超过放大因子上限
	if replicas > h.replicas {
		replicas = h.replicas
	}

	//node key
	nodeRepr := repr(node)
	h.lock.Lock()
	defer h.lock.Unlock()
	h.addNode(nodeRepr)

	for i := 0; i < replicas; i++ {
		hash := h.hashFunc([]byte(nodeRepr + strconv.Itoa(i)))
		h.keys = append(h.keys, hash)
		h.ring[hash] = append(h.ring[hash], node)
	}

	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

// AddWithWeight adds the node with weight, the weight can be 1 to 100, indicates the percent,
// the later call will overwrite the replicas of the former calls.
// 按权重添加节点
// 通过权重来计算方法因子，最终控制虚拟节点的数量
// 权重越高，虚拟节点数量越多
func (h *ConsistentHash) AddWithWeight(node any, weight int) {
	// don't need to make sure weight not larger than TopWeight,
	// because AddWithReplicas makes sure replicas cannot be larger than h.replicas
	replicas := h.replicas * weight / TopWeight
	h.AddWithReplicas(node, replicas)
}

// Get returns the corresponding node from h base on the given v.
// 根据v顺时针找到最近的虚拟节点
// 再通过虚拟节点映射找到真实节点
func (h *ConsistentHash) Get(v any) (any, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()

	//当前没有物理节点
	if len(h.ring) == 0 {
		return nil, false
	}

	//计算哈希值
	hash := h.hashFunc([]byte(repr(v)))

	// 二分查找
	// 因为每次添加节点后虚拟节点都会重新排序
	// 所以查询到的第一个节点就是我们的目标节点
	// 取余则可以实现环形列表效果，顺时针查找节点
	index := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	}) % len(h.keys)

	// 虚拟节点->物理节点映射
	nodes := h.ring[h.keys[index]]
	switch len(nodes) {
	case 0:
		// 不存在真实节点
		return nil, false
	case 1:
		// 只有一个真实节点，直接返回
		return nodes[0], true
	default:
		// 存在多个真实节点意味这出现哈希冲突
		// 此时我们对v重新进行哈希计算
		// 对nodes长度取余得到一个新的index
		innerIndex := h.hashFunc([]byte(innerRepr(v)))
		pos := int(innerIndex % uint64(len(nodes)))
		return nodes[pos], true
	}
}

// Remove removes the given node from h.
// 移除虚拟节点映射
func (h *ConsistentHash) Remove(node any) {
	// 节点的string
	nodeRepr := repr(node)

	// 并发安全
	h.lock.Lock()
	defer h.lock.Unlock()

	// 节点不存在
	if !h.containsNode(nodeRepr) {
		return
	}

	for i := 0; i < h.replicas; i++ {
		// 计算哈希值
		hash := h.hashFunc([]byte(nodeRepr + strconv.Itoa(i)))
		// 二分查找到第一个虚拟节点
		index := sort.Search(len(h.keys), func(i int) bool {
			return h.keys[i] >= hash
		})
		// 切片删除对应的元素
		if index < len(h.keys) && h.keys[index] == hash {
			// 定位到切片index之前的元素
			// 将index之后的元素（index+1）前移覆盖index
			h.keys = append(h.keys[:index], h.keys[index+1:]...)
		}
		// 虚拟节点删除映射
		h.removeRingNode(hash, nodeRepr)
	}
	// 删除真实节点
	h.removeNode(nodeRepr)
}

// 删除虚拟-真实节点映射关系
// hash - 虚拟节点
// nodeRepr - 真实节点
func (h *ConsistentHash) removeRingNode(hash uint64, nodeRepr string) {
	// map使用时应该校验一下
	if nodes, ok := h.ring[hash]; ok {
		// 新建一个空的切片,容量与nodes保持一致
		newNodes := nodes[:0]
		// 遍历nodes
		for _, x := range nodes {
			// 如果序列化值不相同，x是其他节点
			// 不能删除
			if repr(x) != nodeRepr {
				newNodes = append(newNodes, x)
			}
		}

		if len(newNodes) > 0 {
			// 剩余节点不为空则重新绑定映射关系
			h.ring[hash] = newNodes
		} else {
			// 否则删除即可
			delete(h.ring, hash)
		}
	}
}

func (h *ConsistentHash) addNode(nodeRepr string) {
	h.nodes[nodeRepr] = lang.Placeholder
}

func (h *ConsistentHash) containsNode(nodeRepr string) bool {
	_, ok := h.nodes[nodeRepr]
	return ok
}

func (h *ConsistentHash) removeNode(nodeRepr string) {
	delete(h.nodes, nodeRepr)
}

// 可以理解为确定node字符串值的序列化方法
// 在遇到哈希冲突时需要重新对key进行哈希计算
// 为了减少冲突的概率前面追加了一个质数 prime来减小冲突的概率
func innerRepr(node any) string {
	return fmt.Sprintf("%d:%v", prime, node)
}

// 可以理解为确定node字符串值的序列化方法
func repr(node any) string {
	return lang.Repr(node)
}
