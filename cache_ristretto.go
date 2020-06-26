package sqlcache

import (
	"fmt"
	"time"

	"github.com/prashanthpai/sqlcache/cache"

	"github.com/dgraph-io/ristretto"
)

// Ristretto implements cache.Cacher interface to use ristretto as backend with
// go-redis as the redis client library.
type Ristretto struct {
	c *ristretto.Cache
}

// Get gets a cache item from ristretto. Returns pointer to the item, a boolean
// which represents whether key exists or not and an error.
func (r *Ristretto) Get(key string) (*cache.Item, bool, error) {
	i, ok := r.c.Get(key)
	if !ok {
		return nil, false, nil
	}

	item, ok := i.(*cache.Item)
	if !ok {
		return nil, false, fmt.Errorf("Ristretto.Get(): i.(*cache.Item) failed")
	}

	return item, ok, nil
}

// Set sets the given item into ristretto with provided TTL duration.
func (r *Ristretto) Set(key string, item *cache.Item, ttl time.Duration) error {
	// using # of rows as cost
	_ = r.c.SetWithTTL(key, item, int64(len(item.Rows)), ttl)
	return nil
}

// NewRistretto creates a new instance of ristretto backend wrapping the
// provided *ristretto.Cache instance. While creating the ristretto
// instance, please note that number of rows will be used as "cost"
// (in ristretto's terminology) for each cache item.
func NewRistretto(c *ristretto.Cache) *Ristretto {
	return &Ristretto{
		c: c,
	}
}
