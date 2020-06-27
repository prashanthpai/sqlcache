package sqlcache

import (
	"database/sql/driver"
	"io"

	"github.com/prashanthpai/sqlcache/cache"
)

func newRowsRecorder(setter func(item *cache.Item), rows driver.Rows, maxRows int) *rowsRecorder {
	return &rowsRecorder{
		item:    new(cache.Item),
		setter:  setter,
		maxRows: maxRows,
		dr:      rows,
	}
}

type rowsRecorder struct {
	item       *cache.Item
	setter     func(item *cache.Item)
	gotErr     bool
	gotEOF     bool
	maxRowsHit bool
	maxRows    int
	dr         driver.Rows
}

func (r *rowsRecorder) Columns() []string {
	r.item.Cols = r.dr.Columns()
	return r.item.Cols
}

func (r *rowsRecorder) Close() error {
	if err := r.dr.Close(); err != nil {
		r.gotErr = true
		return err
	}

	// cache only if we've reached EOF without any errors
	// and without hitting max rows limit
	if r.gotEOF && !r.gotErr && !r.maxRowsHit {
		r.setter(r.item)
	}

	return nil
}

func (r *rowsRecorder) Next(dest []driver.Value) error {
	err := r.dr.Next(dest)
	if err != nil {
		if err == io.EOF {
			r.gotEOF = true
		} else {
			r.gotErr = true
		}
	}

	if r.gotEOF || r.gotErr || r.maxRowsHit {
		return err
	}

	if len(r.item.Rows) == r.maxRows {
		r.maxRowsHit = true
		return err
	}

	cpy := make([]driver.Value, len(dest))
	copy(cpy, dest)
	r.item.Rows = append(r.item.Rows, cpy)

	return err
}
