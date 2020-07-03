package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/prashanthpai/sqlcache"
	"github.com/prashanthpai/sqlcache/cache"

	"github.com/dgraph-io/ristretto"
	redis "github.com/go-redis/redis/v7"
	"github.com/jackc/pgx/v4/stdlib"
)

const (
	defaultMaxRowsToCache = 100
)

func newRistrettoCache(maxRowsToCache int64) (cache.Cacher, error) {
	c, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10 * maxRowsToCache,
		MaxCost:     maxRowsToCache,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}

	return sqlcache.NewRistretto(c), nil
}

func newRedisCache() (cache.Cacher, error) {
	r := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"127.0.0.1:6379"},
	})

	if _, err := r.Ping().Result(); err != nil {
		return nil, err
	}

	return sqlcache.NewRedis(r, "sqc:"), nil
}

func main() {

	cache, err := newRistrettoCache(defaultMaxRowsToCache)
	if err != nil {
		log.Fatalf("newRistrettoCache() failed: %v", err)
	}

	/*
		cache, err := newRedisCache()
		if err != nil {
			log.Fatalf("newRedisCache() failed: %v", err)
		}
	*/

	interceptor, err := sqlcache.NewInterceptor(&sqlcache.Config{
		Cache: cache, // pick a Cacher interface implementation of your choice (redis or ristretto)
	})
	if err != nil {
		log.Fatalf("sqlcache.NewInterceptor() failed: %v", err)
	}

	defer func() {
		fmt.Printf("\nInterceptor metrics: %+v\n", interceptor.Stats())
	}()

	// install the wrapper which wraps pgx driver
	sql.Register("pgx-sqlcache", interceptor.Driver(stdlib.GetDefaultDriver()))

	if err := run(); err != nil {
		log.Fatalf("run() failed: %v", err)
	}
}

func run() error {

	db, err := sql.Open("pgx-sqlcache",
		"host=127.0.0.1 port=5432 user=prashanthpai dbname=postgres sslmode=disable")
	if err != nil {
		return err
	}
	defer db.Close()

	if err = db.PingContext(context.TODO()); err != nil {
		return fmt.Errorf("db.PingContext() failed: %w", err)
	}

	for i := 0; i < 15; i++ {
		start := time.Now()
		if err := doQuery(db); err != nil {
			return fmt.Errorf("doQuery() failed: %w", err)
		}
		fmt.Printf("i=%d; t=%s\n", i, time.Since(start))
		time.Sleep(1 * time.Second)
	}

	return nil
}

func doQuery(db *sql.DB) error {

	rows, err := db.QueryContext(context.TODO(), `
		-- @cache-ttl 5
		-- @cache-max-rows 10
		SELECT name, pages FROM books WHERE pages > $1`, 10)
	if err != nil {
		return fmt.Errorf("db.QueryContext() failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var pages int
		if err := rows.Scan(&name, &pages); err != nil {
			return fmt.Errorf("rows.Scan() failed: %w", err)
		}
	}

	return rows.Err()
}
