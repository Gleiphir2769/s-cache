// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package s_cache

import (
	"github.com/patrickmn/go-cache"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func BenchmarkLRUCache(b *testing.B) {
	c := NewCache(NoExpiration, DefaultCleanupInterval, NewLRU(10000000000000))

	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		for pb.Next() {
			key := strconv.Itoa(r.Intn(1000000000))
			c.Get(key, func() (int, Value, time.Duration) {
				return 1, key, c.defaultExpiration
			}).Release()
		}
	})
}

func BenchmarkGOCache(b *testing.B) {
	c := cache.New(cache.DefaultExpiration, time.Second*10)

	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		for pb.Next() {
			key := strconv.Itoa(r.Intn(1000000000))
			if _, ok := c.Get(key); !ok {
				c.Set(key, key, cache.NoExpiration)
			}
		}
	})
}
