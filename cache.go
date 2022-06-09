package s_cache

import (
	"github.com/Gleiphir2769/s-cache/timewheel"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Node is a 'cache node'.
type Node struct {
	r *Cache

	key string

	mu    sync.Mutex
	size  int
	value Value

	ref   int32
	onDel []func()

	CacheData unsafe.Pointer
}

// Key returns this 'cache node' key.
func (n *Node) Key() string {
	return n.key
}

// Size returns this 'cache node' size.
func (n *Node) Size() int {
	return n.size
}

// Value returns this 'cache node' value.
func (n *Node) Value() Value {
	return n.value
}

// Ref returns this 'cache node' ref counter.
func (n *Node) Ref() int32 {
	return atomic.LoadInt32(&n.ref)
}

// GetHandle returns an handle for this 'cache node'.
func (n *Node) GetHandle() *Handle {
	if atomic.AddInt32(&n.ref, 1) <= 1 {
		panic("BUG: Node.GetHandle on zero ref")
	}
	return &Handle{unsafe.Pointer(n)}
}

func (n *Node) unref() {
	if atomic.AddInt32(&n.ref, -1) == 0 {
		n.r.delete(n)
	}
}

func (n *Node) unrefLocked() {
	if atomic.AddInt32(&n.ref, -1) == 0 {
		n.r.mu.RLock()
		if !n.r.closed {
			n.r.delete(n)
		}
		n.r.mu.RUnlock()
	}
}

// Cacher provides interface to implements a caching functionality.
// An implementation must be safe for concurrent use.
type Cacher interface {
	// Capacity returns cache capacity.
	Capacity() int

	// SetCapacity sets cache capacity.
	SetCapacity(capacity int)

	// Promote promotes the 'cache node'.
	Promote(n *Node)

	// Ban evicts the 'cache node' and prevent subsequent 'promote'.
	Ban(n *Node)

	// Evict evicts the 'cache node'.
	Evict(n *Node)

	// EvictAll evicts all 'cache node'.
	EvictAll()

	// Close closes the 'cache tree'
	Close() error
}

// Value is a 'cacheable object'. It may implement Releaser, if
// so the Release method will be called once object is released.
type Value interface{}

type Cache struct {
	mu                sync.RWMutex
	buckets           *shardBuckets
	size              int32
	cacher            Cacher
	closed            bool
	tw                *timewheel.TimeWheel
	defaultExpiration time.Duration
}

const (
	NoExpiration           time.Duration = -1
	DefaultExpiration      time.Duration = 0
	DefaultCleanupInterval time.Duration = time.Second
)

// NewCache creates a new 'cache map'. The cacher is optional and
// may be nil.
func NewCache(defaultExpiration, cleanupInterval time.Duration, cacher Cacher) *Cache {
	if defaultExpiration == NoExpiration {
		return newCache(cacher)
	}
	c := newCache(cacher)
	c.defaultExpiration = defaultExpiration
	tw := timewheel.New(cleanupInterval, 3600)
	tw.Start()
	c.tw = tw
	return c
}

func newCache(cacher Cacher) *Cache {
	r := &Cache{
		cacher: cacher,
	}
	r.buckets = makeShardBuckets(r, 16)
	return r
}

// djb2 with better shuffling. 5x faster than FNV with the hash.Hash overhead.
func djb33(seed uint32, k string) uint32 {
	var (
		l = uint32(len(k))
		d = 5381 + seed + l
		i = uint32(0)
	)
	// Why is all this 5x faster than a for loop?
	if l >= 4 {
		for i < l-4 {
			d = (d * 33) ^ uint32(k[i])
			d = (d * 33) ^ uint32(k[i+1])
			d = (d * 33) ^ uint32(k[i+2])
			d = (d * 33) ^ uint32(k[i+3])
			i += 4
		}
	}
	switch l - i {
	case 1:
	case 2:
		d = (d * 33) ^ uint32(k[i])
	case 3:
		d = (d * 33) ^ uint32(k[i])
		d = (d * 33) ^ uint32(k[i+1])
	case 4:
		d = (d * 33) ^ uint32(k[i])
		d = (d * 33) ^ uint32(k[i+1])
		d = (d * 33) ^ uint32(k[i+2])
	}
	return d ^ (d >> 16)
}

// Nodes returns number of 'cache node' in the map.
func (r *Cache) Nodes() int {
	return r.buckets.len()
}

// Size returns sums of 'cache node' size in the map.
func (r *Cache) Size() int {
	return int(atomic.LoadInt32(&r.size))
}

// Capacity returns cache capacity.
func (r *Cache) Capacity() int {
	if r.cacher == nil {
		return 0
	}
	return r.cacher.Capacity()
}

// SetCapacity sets cache capacity.
func (r *Cache) SetCapacity(capacity int) {
	if r.cacher != nil {
		r.cacher.SetCapacity(capacity)
	}
}

// Get gets 'cache node' with the given key.
// If cache node is not found and setFunc is not nil, Get will atomically creates
// the 'cache node' by calling setFunc. Otherwise Get will returns nil.
//
// The returned 'cache handle' should be released after use by calling Release
// method.
func (r *Cache) Get(key string, setFunc func() (size int, value Value, d time.Duration)) *Handle {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil
	}

	n, _ := r.buckets.get(r, key, setFunc == nil)
	if n != nil {
		n.mu.Lock()
		if n.value == nil {
			if setFunc == nil {
				n.mu.Unlock()
				n.unref()
				return nil
			}
			var delay time.Duration
			n.size, n.value, delay = setFunc()
			if delay == DefaultExpiration {
				delay = r.defaultExpiration
			}
			if n.value == nil {
				n.size = 0
				n.mu.Unlock()
				n.unref()
				return nil
			}
			if r.tw != nil {
				r.tw.AddJob(key, delay, func() {
					r.Evict(key)
				})
			}
			atomic.AddInt32(&r.size, int32(n.size))
		}
		n.mu.Unlock()
		if r.cacher != nil {
			r.cacher.Promote(n)
		}
		return &Handle{unsafe.Pointer(n)}
	}
	return nil
}

// Evict evicts 'cache node' with the given key. This will
// simply call Cacher.Evict.
//
// Evict return true is such 'cache node' exist.
func (r *Cache) Evict(key string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return false
	}

	n, _ := r.buckets.get(r, key, true)
	if n != nil {
		if r.cacher != nil {
			r.cacher.Evict(n)
		}
		n.unref()
		return true
	}
	return false
}

// EvictAll evicts all 'cache node'. This will simply call Cacher.EvictAll.
func (r *Cache) EvictAll() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return
	}

	if r.cacher != nil {
		r.cacher.EvictAll()
	}
}

// Delete removes and ban 'cache node' with the given key.
// A banned 'cache node' will never insert into the 'cache tree'. Ban
// only attributed to the particular 'cache node', so when a 'cache node'
// is recreated it will not be banned.
//
// If onDel is not nil, then it will be executed if such 'cache node'
// doesn't exist or once the 'cache node' is released.
//
// Delete return true is such 'cache node' exist.
func (r *Cache) Delete(key string, onDel func()) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return false
	}

	n, _ := r.buckets.get(r, key, true)
	if n != nil {
		if r.tw != nil {
			r.tw.RemoveJob(key)
		}
		if onDel != nil {
			n.mu.Lock()
			n.onDel = append(n.onDel, onDel)
			n.mu.Unlock()
		}
		if r.cacher != nil {
			r.cacher.Ban(n)
		}
		n.unref()
		return true
	}

	if onDel != nil {
		onDel()
	}

	return false
}

func (r *Cache) delete(n *Node) bool {
	n, deleted := r.buckets.remove(n.key)
	if deleted {
		// Call releaser.
		if n.value != nil {
			if r, ok := n.value.(Releaser); ok {
				r.Release()
			}
			n.value = nil
		}
		for _, f := range n.onDel {
			f()
		}
		// Update counter.
		atomic.AddInt32(&r.size, int32(n.size)*-1)
	}
	return deleted
}

// Close closes the 'cache map' and forcefully releases all 'cache node'.
func (r *Cache) Close() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		r.buckets.forEach(func(key string, n *Node) bool {
			if n.value != nil {
				if r, ok := n.value.(Releaser); ok {
					r.Release()
				}
				n.value = nil
			}
			// Call OnDel.
			for _, f := range n.onDel {
				f()
			}
			n.onDel = nil
			return false
		})
	}

	r.mu.Unlock()

	// Avoid deadlock.
	if r.cacher != nil {
		if err := r.cacher.Close(); err != nil {
			return err
		}
	}
	return nil
}

// CloseWeak closes the 'cache map' and evict all 'cache node' from cacher, but
// unlike Close it doesn't forcefully releases 'cache node'.
func (r *Cache) CloseWeak() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
	}
	r.mu.Unlock()

	// Avoid deadlock.
	if r.cacher != nil {
		r.cacher.EvictAll()
		if err := r.cacher.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Handle is a 'cache handle' of a 'cache node'.
type Handle struct {
	n unsafe.Pointer // *Node
}

// Value returns the value of the 'cache node'.
func (h *Handle) Value() Value {
	n := (*Node)(atomic.LoadPointer(&h.n))
	if n != nil {
		return n.value
	}
	return nil
}

// Release releases this 'cache handle'.
// It is safe to call release multiple times.
func (h *Handle) Release() {
	nPtr := atomic.LoadPointer(&h.n)
	if nPtr != nil && atomic.CompareAndSwapPointer(&h.n, nPtr, nil) {
		n := (*Node)(nPtr)
		n.unrefLocked()
	}
}

// Releaser is the interface that wraps the basic Release method.
type Releaser interface {
	// Release releases associated resources. Release should always success
	// and can be called multiple times without causing error.
	Release()
}
