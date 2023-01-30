package s_cache

import (
	"container/heap"
	"sync"
	"unsafe"
)

type lfuNode struct {
	n     *Node
	h     *Handle
	ban   bool
	freq  int
	index int
	count int
}

type lfu struct {
	mu       sync.Mutex
	capacity int
	used     int
	lh       lfuHeap
	counter  int
}

type lfuHeap []*lfuNode

func (lh lfuHeap) Len() int {
	return len(lh)
}

func (lh lfuHeap) Swap(i, j int) {
	lh[i], lh[j] = lh[j], lh[i]
	lh[i].index = i
	lh[j].index = j
}

func (lh lfuHeap) Less(i, j int) bool {
	if lh[i].freq == lh[j].freq {
		return lh[i].count < lh[j].count
	}
	return lh[i].freq < lh[j].freq
}

func (lh *lfuHeap) Push(x interface{}) {
	n := len(*lh)
	item := x.(*lfuNode)
	item.index = n
	*lh = append(*lh, item)
}

func (lh *lfuHeap) Pop() interface{} {
	old := *lh
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*lh = old[0 : n-1]
	return item
}

func (lh *lfuHeap) update(n *lfuNode, frequency int, count int) {
	n.count = count
	n.freq = frequency
	heap.Fix(lh, n.index)
}

func (r *lfu) Capacity() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.capacity
}

func (r *lfu) SetCapacity(capacity int) {
	var evicted []*lfuNode

	r.mu.Lock()
	r.capacity = capacity
	for r.used > r.capacity {
		rn := heap.Pop(&r.lh).(*lfuNode)
		if rn == nil {
			panic("BUG: invalid LFU used or capacity counter")
		}
		rn.n.CacheData = nil
		r.used -= rn.n.Size()
		evicted = append(evicted, rn)
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

func (r *lfu) Promote(n *Node) {
	var evicted []*lfuNode

	r.mu.Lock()
	if n.CacheData == nil {
		if n.Size() <= r.capacity {
			rn := &lfuNode{n: n, h: n.GetHandle()}
			heap.Push(&r.lh, rn)
			n.CacheData = unsafe.Pointer(rn)
			r.used += n.Size()

			for r.used > r.capacity {
				rn := heap.Pop(&r.lh).(*lfuNode)
				if rn == nil {
					panic("BUG: invalid LFU used or capacity counter")
				}
				rn.n.CacheData = nil
				r.used -= rn.n.Size()
				evicted = append(evicted, rn)
			}
		}
	} else {
		rn := (*lfuNode)(n.CacheData)
		if !rn.ban {
			r.counter++
			r.lh.update(rn, rn.freq+1, r.counter)
		}
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

func (r *lfu) Ban(n *Node) {
	r.mu.Lock()
	if n.CacheData == nil {
		n.CacheData = unsafe.Pointer(&lfuNode{n: n, ban: true})
	} else {
		rn := (*lfuNode)(n.CacheData)
		if !rn.ban {
			heap.Remove(&r.lh, rn.index)
			rn.ban = true
			r.used -= rn.n.Size()
			r.mu.Unlock()

			rn.h.Release()
			rn.h = nil
			return
		}
	}
	r.mu.Unlock()
}

func (r *lfu) Evict(n *Node) {
	r.mu.Lock()
	rn := (*lfuNode)(n.CacheData)
	if rn == nil || rn.ban {
		r.mu.Unlock()
		return
	}
	n.CacheData = nil
	r.mu.Unlock()

	rn.h.Release()
}

func (r *lfu) EvictAll() {
	r.mu.Lock()
	for i := 0; i < r.lh.Len(); i++ {
		r.lh[i].n.CacheData = nil
	}
	r.mu.Unlock()

	for i := 0; i < r.lh.Len(); i++ {
		r.lh[i].h.Release()
	}

	r.lh = lfuHeap{}
	heap.Init(&r.lh)
}

func (r *lfu) Close() error {
	return nil
}

// NewLFU create a new LFU-cache.
func NewLFU(capacity int) Cacher {
	r := &lfu{capacity: capacity, lh: lfuHeap{}}
	heap.Init(&r.lh)
	return r
}
