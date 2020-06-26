package sqlcache

import (
	"time"

	"github.com/prashanthpai/sqlcache/cache"

	redis "github.com/go-redis/redis/v7"
	msgpack "github.com/vmihailenco/msgpack/v4"
)

// Redis implements cache.Cacher interface to use redis as backend with
// go-redis as the redis client library.
type Redis struct {
	c         redis.UniversalClient
	keyPrefix string
}

// Get gets a cache item from redis. Returns pointer to the item, a boolean
// which represents whether key exists or not and an error.
func (r *Redis) Get(key string) (*cache.Item, bool, error) {
	b, err := r.c.Get(r.keyPrefix + key).Bytes()
	switch err {
	case nil:
		var item cache.Item
		if err := msgpack.Unmarshal(b, &item); err != nil {
			return nil, true, err
		}
		return &item, true, nil
	case redis.Nil:
		return nil, false, nil
	default:
		return nil, false, err
	}
}

// Set sets the given item into redis with provided TTL duration.
func (r *Redis) Set(key string, item *cache.Item, ttl time.Duration) error {
	b, err := msgpack.Marshal(item)
	if err != nil {
		return err
	}

	_, err = r.c.Set(r.keyPrefix+key, b, ttl).Result()
	return err
}

// NewRedis creates a new instance of redis backend using go-redis client.
// All keys created in redis by sqlcache will have start with prefix.
func NewRedis(c redis.UniversalClient, keyPrefix string) *Redis {
	return &Redis{
		c:         c,
		keyPrefix: keyPrefix,
	}
}
