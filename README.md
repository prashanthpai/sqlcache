# sqlcache

[![Go.Dev reference](https://img.shields.io/badge/go.dev-reference-blue?logo=go)](https://pkg.go.dev/prashanthpai/sqlcache?tab=doc)
[![Go Report Card](https://goreportcard.com/badge/github.com/prashanthpai/sqlcache)](https://goreportcard.com/report/github.com/prashanthpai/sqlcache)
[![Test status](https://github.com/prashanthpai/sqlcache/workflows/test/badge.svg?branch=master "test status")](https://github.com/prashanthpai/sqlcache/actions)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)

sqlcache is an **experimental** caching middleware for `database/sql`. It
leverages APIs provided by the handy [sqlmw](https://github.com/ngrok/sqlmw)
project and is inspired from [slonik-interceptor-query-cache](https://github.com/gajus/slonik-interceptor-query-cache).
This liberates your Go program from maintaining imperative code that
implements the cache-aside pattern. Your program will perceive the
database client/driver as a read-through cache.

Tested with PostgreSQL database with [pgx](https://github.com/jackc/pgx/tree/master/stdlib) as the driver.

Cache backends supported:

* [ristretto](https://github.com/dgraph-io/ristretto) (in-memory)
* [redis](https://github.com/go-redis/redis)

It's easy to add other caching backends by implementing the `cache.Cacher`
interface.

## Usage

Create a backend cache instance and install the interceptor:

```go
import (
	"database/sql"

	redis "github.com/go-redis/redis/v7"
	"github.com/ngrok/sqlmw"
	"github.com/prashanthpai/sqlcache"
)

func main() {
	...
	rc := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"127.0.0.1:6379"},
	})

	// create a sqlcache.Interceptor instance with the desired backend
	interceptor, err := sqlcache.NewInterceptor(&sqlcache.Config{
		Cache: sqlcache.NewRedis(rc, "sqc"),
	})
	...

	// wrap pgx driver with the interceptor and register it
	sql.Register("pgx-with-cache", sqlmw.Driver(stdlib.GetDefaultDriver(), interceptor))

	// open the database using the wrapped driver
	db, err := sql.Open("pgx-with-cache", dsn)
	...
```

Caching is controlled using cache attributes which are SQL comments starting
with `@cache-` prefix. Only queries with cache attributes are cached.

**Cache attributes:**

|Cache attribute|Description|Required?|Default|
|---|---|---|---|
|`@cache-ttl`|Number (in seconds) to cache the query for.|Yes|N/A|
|`@cache-max-rows`|Don't cache if number of rows in query response exceeds this limit.|Yes|N/A|

Example query:

```go
rows, err := db.QueryContext(context.TODO(), `
	-- @cache-ttl 30
	-- @cache-max-rows 10
	SELECT name, pages FROM books WHERE pages > $1`, 100)
```

See [example/main.go](example/main.go) for a full working example.

## TODO

* Test against different postgres data types.
* Check if deep copy of buffers can be removed.

### References

* A declarative way to cache PostgreSQL queries using Node.js: a [blog post](https://dev.to/gajus/a-declarative-way-to-cache-postgresql-queries-using-node-js-4fbo) by the author of [Slonik](https://github.com/gajus/slonik).
* Declarative Caching with Postgres and Redis: Kyle Davis's [talk](https://youtu.be/IID2LQVztIM?t=1170) on Slonik + Redis.
