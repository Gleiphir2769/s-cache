package s_cache

import (
	"crypto/rand"
	"math"
	"math/big"
	insecurerand "math/rand"
	"os"
	"sync"
	"sync/atomic"
)

type RecallFunc func(key string, val *Node) bool

type shardBuckets struct {
	r        *Cache
	table    []*bucket
	count    int32
	seed     uint32
	hashAlgo func(seed uint32, k string) uint32
}

type bucket struct {
	m     map[string]*Node
	mutex sync.RWMutex
}

func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	} else {
		return int(n + 1)
	}
}

func makeShardBuckets(r *Cache, shardCount int) *shardBuckets {
	shardCount = computeCapacity(shardCount)
	table := make([]*bucket, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &bucket{
			m: make(map[string]*Node),
		}
	}
	max := big.NewInt(0).SetUint64(uint64(math.MaxUint32))
	rnd, err := rand.Int(rand.Reader, max)
	var seed uint32
	if err != nil {
		os.Stderr.Write([]byte("WARNING: s-cache's makeShardBuckets failed to read from the system CSPRNG (/dev/urandom or equivalent.) Your system's security may be compromised. Continuing with an insecure seed.\n"))
		seed = insecurerand.Uint32()
	} else {
		seed = uint32(rnd.Uint64())
	}
	d := &shardBuckets{
		r:        r,
		count:    0,
		table:    table,
		seed:     seed,
		hashAlgo: djb33,
	}
	return d
}

func (s *shardBuckets) spread(hashcode uint32) uint32 {
	if s == nil {
		panic("dict is nil")
	}
	dictSize := uint32(len(s.table))
	return (dictSize - 1) & hashcode
}

func (s *shardBuckets) getShared(index uint32) *bucket {
	return s.table[index]
}

func (s *shardBuckets) addCount() {
	atomic.AddInt32(&s.count, 1)
}

func (s *shardBuckets) decreaseCount() {
	atomic.AddInt32(&s.count, -1)
}

func (s *shardBuckets) get(r *Cache, key string, noset bool) (n *Node, added bool) {
	if s == nil {
		panic("dict is nil")
	}
	hashcode := s.hashAlgo(s.seed, key)
	index := s.spread(hashcode)
	shared := s.getShared(index)
	shared.mutex.Lock()
	defer shared.mutex.Unlock()

	n, exists := shared.m[key]
	if exists {
		// node has existed, add the ref
		atomic.AddInt32(&n.ref, 1)
		return n, false
	} else if !exists && !noset {
		n = &Node{
			r:   r,
			key: key,
			ref: 1,
		}
		shared.m[key] = n
		s.addCount()
		return n, true
	} else {
		return nil, false
	}
}

func (s *shardBuckets) put(key string, val *Node) (result int) {
	if s == nil {
		panic("dict is nil")
	}
	hashcode := s.hashAlgo(s.seed, key)
	index := s.spread(hashcode)
	shared := s.getShared(index)
	shared.mutex.Lock()
	defer shared.mutex.Unlock()

	if _, ok := shared.m[key]; ok {
		shared.m[key] = val
		return 0
	} else {
		shared.m[key] = val
		s.addCount()
		return 1
	}
}

func (s *shardBuckets) len() (length int) {
	return int(atomic.LoadInt32(&s.count))
}

// PutIfAbsent if the key has existed, the value will not be replaced.
func (s *shardBuckets) putIfAbsent(key string, val *Node) (result int) {
	if s == nil {
		panic("dict is nil")
	}
	hashcode := s.hashAlgo(s.seed, key)
	index := s.spread(hashcode)
	shared := s.getShared(index)
	shared.mutex.Lock()
	defer shared.mutex.Unlock()

	if _, ok := shared.m[key]; ok {
		return 0
	} else {
		shared.m[key] = val
		s.addCount()
		return 1
	}
}

// PutIfExists the value will only be put when key has existed
func (s *shardBuckets) putIfExists(key string, val *Node) (result int) {
	if s == nil {
		panic("dict is nil")
	}
	hashcode := s.hashAlgo(s.seed, key)
	index := s.spread(hashcode)
	shared := s.getShared(index)
	shared.mutex.Lock()
	defer shared.mutex.Unlock()

	if _, ok := shared.m[key]; ok {
		shared.m[key] = val
		s.addCount()
		return 1
	} else {
		return 0
	}
}

func (s *shardBuckets) remove(key string) (val *Node, existed bool) {
	if s == nil {
		panic("dict is nil")
	}
	hashcode := s.hashAlgo(s.seed, key)
	index := s.spread(hashcode)
	shared := s.getShared(index)
	shared.mutex.Lock()
	defer shared.mutex.Unlock()

	if v, ok := shared.m[key]; ok {
		delete(shared.m, key)
		s.decreaseCount()
		return v, true
	} else {
		return nil, false
	}
}

func (s *shardBuckets) forEach(recall RecallFunc) {
	if s == nil {
		return
	}
	for _, t := range s.table {
		t.mutex.RLock()
		func() {
			defer t.mutex.RUnlock()
			for k, v := range t.m {
				if recall(k, v) {
					return
				}
			}
		}()
	}
}
