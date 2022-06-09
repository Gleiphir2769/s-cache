# s-cache

A concurrency-safe and high performance go caching library with expiration capabilities and evict policy.

## Installation

Make sure you have a working Go environment (Go 1.17 or higher is required).

To install s-cache, simply run:

```
go get github.com/Gleiphir2769/s-cache
```

## Example

```golang
package main

import (
	"fmt"
	"github.com/Gleiphir2769/s-cache"
	"time"
)

func main() {
	// Create a cache with LRU
	c := s_cache.NewCache(s_cache.NoExpiration, time.Second, s_cache.NewLRU(100))

	// Get and Set the item with key "k1". The DefaultExpiration represents the value specified when the cache was created
	c.Get("k1", func() (size int, value s_cache.Value, d time.Duration) {
		return 1, "val", s_cache.DefaultExpiration
	}).Release()

	// Get the item with key "k1"
	h1 := c.Get("k1", nil)
	fmt.Println(h1.Value().(string))
	h1.Release()

	// Because k2 is not existed, so Get will return nil
	h2 := c.Get("k2", nil)
	if h2 == nil {
		fmt.Println("item with k2 is not existed")
	}

	// Delete cache node
	onDel := func() {
		fmt.Println("I'm deleted")
	}
	c.Delete("k1", onDel)
	h3 := c.Get("k1", nil)
	if h3 == nil {
		fmt.Println("k1 has been deleted")
	}
}
```