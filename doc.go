/*
Package sqlcache provides an experimental caching middleware for database/sql
users. This liberates your Go program from maintaining imperative code that
implements the cache-aside pattern. Your program will perceive the database
client/driver as a read-through cache.

Usage:

	import (
		"database/sql"

		"github.com/redis/go-redis/v9"
		"github.com/prashanthpai/sqlcache"
		"github.com/jackc/pgx/v4/stdlib"
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
		sql.Register("pgx-with-cache", interceptor.Driver(stdlib.GetDefaultDriver()))

		// open the database using the wrapped driver
		db, err := sql.Open("pgx-with-cache", dsn)
		...
	}

Caching is controlled using cache attributes which are SQL comments starting
with `@cache-` prefix. Only queries with cache attributes are cached.

Example query:

	rows, err := db.QueryContext(context.TODO(), `
		-- @cache-ttl 30
		-- @cache-max-rows 10
		SELECT name, pages FROM books WHERE pages > $1`, 100)
*/
package sqlcache
