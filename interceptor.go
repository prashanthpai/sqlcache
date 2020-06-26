package sqlcache

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/mitchellh/hashstructure"
	"github.com/ngrok/sqlmw"
	"github.com/prashanthpai/sqlcache/cache"
)

// Config is the configuration passed to NewInterceptor for creating new
// Interceptor instances.
type Config struct {
	// Cache must be set to a type that implements the cache.Cacher interface
	// which abstracts the backend cache implementation. This is a required
	// field and cannot be nil.
	Cache cache.Cacher
	// OnError is called whenever methods of cache.Cacher interface or HashFunc
	// returns error. Since sqlcache package does not log any failures, you can
	// use this hook to log errors or even choose to disable/bypass sqlcache.
	OnError func(error)
	// HashFunc can be optionally set to provide a custom hashing function. By
	// default sqlcache uses mitchellh/hashstructure which internally uses FNV.
	HashFunc func(query string, args []driver.NamedValue) (string, error)
}

// Interceptor is a ngrok/sqlmw interceptor that caches SQL queries and
// their responses.
type Interceptor struct {
	c        cache.Cacher
	hashFunc func(query string, args []driver.NamedValue) (string, error)
	onErr    func(error)
	stats    Stats
	disabled bool
	sqlmw.NullInterceptor
}

// NewInterceptor returns a new instance of sqlcache interceptor initialised
// with the provided config.
func NewInterceptor(config *Config) (*Interceptor, error) {
	if config == nil {
		return nil, fmt.Errorf("config can't be nil")
	}

	if config.Cache == nil {
		return nil, fmt.Errorf("cache must be set in Config")
	}

	if config.HashFunc == nil {
		config.HashFunc = defaultHashFunc
	}

	return &Interceptor{
		config.Cache,
		config.HashFunc,
		config.OnError,
		Stats{},
		false,
		sqlmw.NullInterceptor{},
	}, nil
}

// Enable enables the interceptor. Interceptor instance is enabled by default
// on creation.
func (i *Interceptor) Enable() {
	i.disabled = false
}

// Disable disables the interceptor resulting in cache bypass. All queries
// would go directly to the SQL backend.
func (i *Interceptor) Disable() {
	i.disabled = true
}

// StmtQueryContext intecepts database/sql's stmt.QueryContext calls from a prepared statement.
func (i *Interceptor) StmtQueryContext(ctx context.Context, conn driver.StmtQueryContext, query string, args []driver.NamedValue) (driver.Rows, error) {

	if i.disabled {
		return conn.QueryContext(ctx, args)
	}

	attrs := getAttrs(query)
	if attrs == nil {
		return conn.QueryContext(ctx, args)
	}

	hash, err := i.hashFunc(query, args)
	if err != nil {
		atomic.AddUint64(&i.stats.Errors, 1)
		if i.onErr != nil {
			i.onErr(fmt.Errorf("HashFunc failed: %w", err))
		}
		return conn.QueryContext(ctx, args)
	}

	if cached := i.checkCache(hash); cached != nil {
		return cached, nil
	}

	rows, err := conn.QueryContext(ctx, args)
	if err != nil {
		return rows, err
	}

	cacheSetter := func(item *cache.Item) {
		err := i.c.Set(hash, item, time.Duration(attrs.ttl)*time.Second)
		if err != nil {
			atomic.AddUint64(&i.stats.Errors, 1)
			if i.onErr != nil {
				i.onErr(fmt.Errorf("Cache.Set failed: %w", err))
			}
		}
	}

	return newRowsRecorder(cacheSetter, rows, attrs.maxRows), err
}

// ConnQueryContext intecepts database/sql's DB.QueryContext Conn.QueryContext calls.
func (i *Interceptor) ConnQueryContext(ctx context.Context, conn driver.QueryerContext, query string, args []driver.NamedValue) (driver.Rows, error) {

	if i.disabled {
		return conn.QueryContext(ctx, query, args)
	}

	attrs := getAttrs(query)
	if attrs == nil {
		return conn.QueryContext(ctx, query, args)
	}

	hash, err := i.hashFunc(query, args)
	if err != nil {
		atomic.AddUint64(&i.stats.Errors, 1)
		if i.onErr != nil {
			i.onErr(fmt.Errorf("HashFunc failed: %w", err))
		}
		return conn.QueryContext(ctx, query, args)
	}

	if cached := i.checkCache(hash); cached != nil {
		return cached, nil
	}

	rows, err := conn.QueryContext(ctx, query, args)
	if err != nil {
		return rows, err
	}

	cacheSetter := func(item *cache.Item) {
		err := i.c.Set(hash, item, time.Duration(attrs.ttl)*time.Second)
		if err != nil {
			atomic.AddUint64(&i.stats.Errors, 1)
			if i.onErr != nil {
				i.onErr(fmt.Errorf("Cache.Set failed: %w", err))
			}
		}
	}

	return newRowsRecorder(cacheSetter, rows, attrs.maxRows), err
}

func (i *Interceptor) checkCache(hash string) driver.Rows {
	item, ok, err := i.c.Get(hash)
	if err != nil {
		atomic.AddUint64(&i.stats.Errors, 1)
		if i.onErr != nil {
			i.onErr(fmt.Errorf("Cache.Get failed: %w", err))
		}
		return nil
	}

	if !ok {
		atomic.AddUint64(&i.stats.Misses, 1)
		return nil
	}
	atomic.AddUint64(&i.stats.Hits, 1)

	return &rowsCached{
		item,
		0,
	}
}

func defaultHashFunc(query string, args []driver.NamedValue) (string, error) {
	u64, err := hashstructure.Hash(struct {
		Query string
		Args  []driver.NamedValue
	}{
		Query: query,
		Args:  args,
	}, nil)
	if err != nil {
		return "", err
	}

	key := fmt.Sprintf("q%da%dh%s", len(query), len(args), strconv.FormatUint(u64, 10))
	return key, nil
}

// Stats contains sqlcache statistics.
type Stats struct {
	Hits   uint64
	Misses uint64
	Errors uint64
}

// Stats returns sqlcache stats.
func (i *Interceptor) Stats() *Stats {
	return &Stats{
		Hits:   atomic.LoadUint64(&i.stats.Hits),
		Misses: atomic.LoadUint64(&i.stats.Misses),
		Errors: atomic.LoadUint64(&i.stats.Errors),
	}
}
