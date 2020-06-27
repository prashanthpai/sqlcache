package sqlcache

import (
	"database/sql/driver"
	"io"

	"github.com/prashanthpai/sqlcache/cache"
)

// rowsCached implements driver.Rows interface
type rowsCached struct {
	*cache.Item
	ptr int
}

func (r *rowsCached) Columns() []string {
	return r.Item.Cols
}

func (r *rowsCached) Next(dest []driver.Value) error {
	if r.ptr >= len(r.Item.Rows) {
		return io.EOF
	}

	copy(dest, r.Item.Rows[r.ptr]) // copy required ?
	r.ptr++

	return nil
}

func (r *rowsCached) Close() error {
	return nil
}
