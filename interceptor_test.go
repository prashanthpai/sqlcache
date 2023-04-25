package sqlcache

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prashanthpai/sqlcache/cache"
	"github.com/prashanthpai/sqlcache/mocks"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	assert := require.New(t)

	// failure cases
	inputs := []*Config{
		nil,
		{},
	}
	for _, input := range inputs {
		i, err := NewInterceptor(input)
		assert.Nil(i)
		assert.NotNil(err)
	}

	// success
	i, err := NewInterceptor(&Config{
		Cache: new(mocks.Cacher),
	})
	assert.NotNil(i)
	assert.Nil(err)

	// stats
	s := i.Stats()
	assert.NotNil(s)
	assert.Equal(s.Hits, uint64(0))
	assert.Equal(s.Misses, uint64(0))
}

func runQuery(t *testing.T, assert *require.Assertions, qMock sqlmock.Sqlmock, db *sql.DB, query string, cacheMissExpected bool) {
	if cacheMissExpected {
		qMock.ExpectQuery(query).WithArgs(18).
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("John").AddRow("Lisa"))
	}

	rows, err := db.QueryContext(context.Background(), query, 18)
	assert.Nil(err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		assert.Nil(rows.Scan(&name))
		names = append(names, name)
	}

	assert.Equal([]string{"John", "Lisa"}, names)
	assert.Nil(qMock.ExpectationsWereMet())
}

func runQueryPrepared(t *testing.T, assert *require.Assertions, qMock sqlmock.Sqlmock, db *sql.DB, query string, cacheMissExpected bool) {
	qMock.ExpectPrepare(query)
	if cacheMissExpected {
		qMock.ExpectQuery(query).WithArgs(18).
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("John").AddRow("Lisa"))
	}

	stmt, err := db.PrepareContext(context.Background(), query)
	assert.Nil(err)

	rows, err := stmt.QueryContext(context.Background(), 18)
	assert.Nil(err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		assert.Nil(rows.Scan(&name))
		names = append(names, name)
	}

	assert.Equal([]string{"John", "Lisa"}, names)
	assert.Nil(qMock.ExpectationsWereMet())
}

func TestAttrs(t *testing.T) {
	assert := require.New(t)

	tests := map[string]struct {
		query string
	}{
		"ttl absent, max rows absent": {
			query: `SELECT name FROM users WHERE age > ?`,
		},
		"ttl present, max rows absent": {
			query: `-- @cache-ttl 30
				SELECT name FROM users WHERE age > ?`,
		},
		"ttl absent, max rows present": {
			query: `-- @cache-max-rows 10
				SELECT name FROM users WHERE age > ?`,
		},
		"ttl invalid, max rows valid": {
			query: `-- @cache-ttl -30
				-- @cache-max-rows -10
				SELECT name FROM users WHERE age > ?`,
		},
		"ttl valid, max rows invalid": {
			query: `-- @cache-max-rows -10
				-- @cache-ttl 30
				SELECT name FROM users WHERE age > ?`,
		},
	}

	dsn := fmt.Sprintf("fakeDSN:%s", t.Name())
	mockDB, qMock, err := sqlmock.NewWithDSN(dsn)
	assert.Nil(err)
	defer mockDB.Close()

	ic, _ := NewInterceptor(&Config{
		Cache: new(mocks.Cacher),
	})

	driverName := fmt.Sprintf("mockdriver:%s", t.Name())
	sql.Register(driverName, ic.Driver(mockDB.Driver()))

	db, err := sql.Open(driverName, dsn)
	assert.Nil(err)
	defer db.Close()

	cacheMissExpected := true
	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			runQuery(t, assert, qMock, db, test.query, cacheMissExpected)
			runQueryPrepared(t, assert, qMock, db, test.query, cacheMissExpected)
		})
	}
}

func TestCacheMiss(t *testing.T) {
	assert := require.New(t)

	dsn := fmt.Sprintf("fakeDSN:%s", t.Name())
	mockDB, qMock, err := sqlmock.NewWithDSN(dsn)
	assert.Nil(err)
	defer mockDB.Close()

	ic, _ := NewInterceptor(&Config{
		Cache: new(mocks.Cacher),
	})

	driverName := fmt.Sprintf("mockdriver:%s", t.Name())
	sql.Register(driverName, ic.Driver(mockDB.Driver()))

	db, err := sql.Open(driverName, dsn)
	assert.Nil(err)
	defer db.Close()

	query := `-- @cache-max-rows 10
              -- @cache-ttl 30
              SELECT name FROM users WHERE age > ?`

	tests := map[string]struct {
		err     error
		present bool
	}{
		"Get() failed; entry present": {errors.New("some error"), true},
		"Get() failed; entry absent":  {errors.New("some error"), false},
		"Get() passed: entry absent":  {nil, false},
	}

	cacheMissExpected := true
	for tcName, td := range tests {
		t.Run(tcName, func(t *testing.T) {
			mCacher := new(mocks.Cacher)
			for i := 0; i < 2; i++ { // once each for runQuery and runQueryPrepared
				mCacher.On("Get", mock.Anything, mock.Anything).Return(nil, td.present, td.err)
				mCacher.On("Set", mock.Anything, mock.Anything, mock.Anything, time.Duration(30*time.Second)).Return(nil)
			}

			ic.c = mCacher
			onErrCalled := 0
			ic.onErr = func(e error) {
				onErrCalled++
			}

			runQuery(t, assert, qMock, db, query, cacheMissExpected)
			runQueryPrepared(t, assert, qMock, db, query, cacheMissExpected)

			if td.err != nil {
				assert.Equal(2, onErrCalled)
			} else {
				assert.Equal(0, onErrCalled)
			}
			assert.True(mCacher.AssertExpectations(t))
		})
	}
}

func TestCacheHit(t *testing.T) {
	assert := require.New(t)

	dsn := fmt.Sprintf("fakeDSN:%s", t.Name())
	mockDB, qMock, err := sqlmock.NewWithDSN(dsn)
	assert.Nil(err)
	defer mockDB.Close()

	ic, _ := NewInterceptor(&Config{
		Cache: new(mocks.Cacher),
	})

	driverName := fmt.Sprintf("mockdriver:%s", t.Name())
	sql.Register(driverName, ic.Driver(mockDB.Driver()))

	db, err := sql.Open(driverName, dsn)
	assert.Nil(err)
	defer db.Close()

	query := `-- @cache-max-rows 10
              -- @cache-ttl 30
              SELECT name FROM users WHERE age > ?`

	cacheItem := &cache.Item{
		Cols: []string{"name"},
		Rows: [][]driver.Value{
			{"John"},
			{"Lisa"},
		},
	}

	mCacher := new(mocks.Cacher)
	for i := 0; i < 2; i++ { // once each for runQuery and runQueryPrepared
		mCacher.On("Get", mock.Anything, mock.Anything).Return(cacheItem, true, nil)
	}
	ic.c = mCacher

	cacheMissExpected := false
	runQuery(t, assert, qMock, db, query, cacheMissExpected)
	runQueryPrepared(t, assert, qMock, db, query, cacheMissExpected)

	assert.True(mCacher.AssertExpectations(t))
}

func TestDisabled(t *testing.T) {
	assert := require.New(t)

	dsn := fmt.Sprintf("fakeDSN:%s", t.Name())
	mockDB, qMock, err := sqlmock.NewWithDSN(dsn)
	assert.Nil(err)
	defer mockDB.Close()

	ic, _ := NewInterceptor(&Config{
		Cache: new(mocks.Cacher),
	})

	driverName := fmt.Sprintf("mockdriver:%s", t.Name())
	sql.Register(driverName, ic.Driver(mockDB.Driver()))

	db, err := sql.Open(driverName, dsn)
	assert.Nil(err)
	defer db.Close()

	query := `-- @cache-max-rows 10
              -- @cache-ttl 30
              SELECT name FROM users WHERE age > ?`

	tests := map[string]bool{
		"interceptor bypassed": false,
		"interceptor enabled":  true,
	}
	for tcName, enabled := range tests {
		t.Run(tcName, func(t *testing.T) {
			mCacher := new(mocks.Cacher)

			if enabled == true {
				ic.Enable()
				for i := 0; i < 2; i++ { // once each for runQuery and runQueryPrepared
					mCacher.On("Get", mock.Anything, mock.Anything).Return(nil, false, nil) // cache miss
					mCacher.On("Set", mock.Anything, mock.Anything, mock.Anything, time.Duration(30*time.Second)).Return(nil)
				}
			} else {
				ic.Disable()
			}
			ic.c = mCacher

			cacheMissExpected := true
			runQuery(t, assert, qMock, db, query, cacheMissExpected)
			runQueryPrepared(t, assert, qMock, db, query, cacheMissExpected)

			assert.True(mCacher.AssertExpectations(t))
		})
	}
}

func TestMaxRows(t *testing.T) {
	assert := require.New(t)

	dsn := fmt.Sprintf("fakeDSN:%s", t.Name())
	mockDB, qMock, err := sqlmock.NewWithDSN(dsn)
	assert.Nil(err)
	defer mockDB.Close()

	ic, _ := NewInterceptor(&Config{
		Cache: new(mocks.Cacher),
	})

	driverName := fmt.Sprintf("mockdriver:%s", t.Name())
	sql.Register(driverName, ic.Driver(mockDB.Driver()))

	db, err := sql.Open(driverName, dsn)
	assert.Nil(err)
	defer db.Close()

	// runQuery() and runQueryPrepared() returns 2 rows
	// setting max rows limit to 1 here
	query := `-- @cache-max-rows 1
              -- @cache-ttl 30
              SELECT name FROM users WHERE age > ?`

	mCacher := new(mocks.Cacher)
	for i := 0; i < 2; i++ { // once each for runQuery and runQueryPrepared
		mCacher.On("Get", mock.Anything, mock.Anything).Return(nil, false, nil) // cache miss
		// note that despite cache miss, no call must be made for cache.Set
		// as max rows has been exceeded
	}
	ic.c = mCacher

	cacheMissExpected := true
	runQuery(t, assert, qMock, db, query, cacheMissExpected)
	runQueryPrepared(t, assert, qMock, db, query, cacheMissExpected)
	assert.True(mCacher.AssertExpectations(t))
}

func TestHashFuncErr(t *testing.T) {
	assert := require.New(t)

	dsn := fmt.Sprintf("fakeDSN:%s", t.Name())
	mockDB, qMock, err := sqlmock.NewWithDSN(dsn)
	assert.Nil(err)
	defer mockDB.Close()

	mCacher := new(mocks.Cacher)
	hashFuncCalled := false
	onErrCalled := false
	ic, _ := NewInterceptor(&Config{
		Cache: mCacher,
		HashFunc: func(query string, args []driver.NamedValue) (string, error) {
			hashFuncCalled = true
			return "", errors.New("some error")
		},
		OnError: func(err error) {
			onErrCalled = true
		},
	})

	driverName := fmt.Sprintf("mockdriver:%s", t.Name())
	sql.Register(driverName, ic.Driver(mockDB.Driver()))

	db, err := sql.Open(driverName, dsn)
	assert.Nil(err)
	defer db.Close()

	query := `-- @cache-max-rows 10
              -- @cache-ttl 30
              SELECT name FROM users WHERE age > ?`

	cacheMissExpected := true
	runQuery(t, assert, qMock, db, query, cacheMissExpected)
	assert.True(hashFuncCalled)
	assert.True(onErrCalled)
	assert.Equal(ic.Stats().Errors, uint64(1))
	hashFuncCalled = false // reset
	onErrCalled = false    // reset

	runQueryPrepared(t, assert, qMock, db, query, cacheMissExpected)
	assert.True(hashFuncCalled)
	assert.True(onErrCalled)

	assert.True(mCacher.AssertExpectations(t))
	assert.Equal(ic.Stats().Errors, uint64(2))
}

func TestCacheSetErr(t *testing.T) {
	assert := require.New(t)

	dsn := fmt.Sprintf("fakeDSN:%s", t.Name())
	mockDB, qMock, err := sqlmock.NewWithDSN(dsn)
	assert.Nil(err)
	defer mockDB.Close()

	mCacher := new(mocks.Cacher)
	for i := 0; i < 2; i++ { // once each for runQuery and runQueryPrepared
		mCacher.On("Get", mock.Anything, mock.Anything).Return(nil, false, nil) // cache miss
		mCacher.On("Set", mock.Anything, mock.Anything, mock.Anything, time.Duration(30*time.Second)).Return(errors.New("some error"))
	}

	onErrCalled := false
	ic, _ := NewInterceptor(&Config{
		Cache: mCacher,
		OnError: func(err error) {
			onErrCalled = true
		},
	})

	driverName := fmt.Sprintf("mockdriver:%s", t.Name())
	sql.Register(driverName, ic.Driver(mockDB.Driver()))

	db, err := sql.Open(driverName, dsn)
	assert.Nil(err)
	defer db.Close()

	query := `-- @cache-max-rows 10
              -- @cache-ttl 30
              SELECT name FROM users WHERE age > ?`

	cacheMissExpected := true
	runQuery(t, assert, qMock, db, query, cacheMissExpected)
	assert.True(onErrCalled)
	onErrCalled = false // reset
	assert.Equal(ic.Stats().Errors, uint64(1))

	runQueryPrepared(t, assert, qMock, db, query, cacheMissExpected)
	assert.True(onErrCalled)

	assert.True(mCacher.AssertExpectations(t))
	assert.Equal(ic.Stats().Errors, uint64(2))
}
