package dbx_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"github.com/tychoish/dbx"
	"github.com/tychoish/dbx/internal/dbxtest"
	"github.com/tychoish/fun/assert"
	"github.com/tychoish/fun/assert/check"
)

// errAfterRows is a driver.Rows that yields real rows then returns a non-EOF
// error on the next Next() call, which causes sql.Rows.Err() to be non-nil.
type errAfterRows struct {
	inner   *dbxtest.Rows
	nextErr error
}

func (r *errAfterRows) Columns() []string { return r.inner.Columns() }
func (r *errAfterRows) Close() error      { return r.inner.Close() }
func (r *errAfterRows) Next(dest []driver.Value) error {
	if err := r.inner.Next(dest); err == io.EOF && r.nextErr != nil {
		return r.nextErr
	} else {
		return err
	}
}

type intRow struct {
	ID   int    `sql:"id"`
	Name string `sql:"name"`
}

func newQueryDB(tb testing.TB, rows *dbxtest.Rows, qErr error) *sql.DB {
	return dbxtest.NewDB(tb, dbxtest.Driver{
		QueryContext: func(_ testing.TB, _ string, _ []any) (driver.Rows, error) {
			return rows, qErr
		},
	})
}

func TestQuery(t *testing.T) {
	ctx := context.Background()

	t.Run("returns rows", func(t *testing.T) {
		db := newQueryDB(t, dbxtest.NewRows("id", "name").Add(1, "Alice").Add(2, "Bob"), nil)
		var got []intRow
		for r, err := range dbx.Query[intRow](ctx, db.QueryContext, "SELECT id, name FROM tbl") {
			assert.NotError(t, err)
			got = append(got, r)
		}
		assert.Equal(t, len(got), 2)
		check.Equal(t, got[0].ID, 1)
		check.Equal(t, got[0].Name, "Alice")
		check.Equal(t, got[1].ID, 2)
		check.Equal(t, got[1].Name, "Bob")
	})

	t.Run("empty result", func(t *testing.T) {
		db := newQueryDB(t, dbxtest.NewRows("id", "name"), nil)
		var got []intRow
		for r, err := range dbx.Query[intRow](ctx, db.QueryContext, "SELECT id, name FROM tbl") {
			assert.NotError(t, err)
			got = append(got, r)
		}
		assert.Equal(t, len(got), 0)
	})

	t.Run("query error", func(t *testing.T) {
		queryErr := errors.New("db error")
		db := newQueryDB(t, nil, queryErr)
		var gotErr error
		for _, err := range dbx.Query[intRow](ctx, db.QueryContext, "SELECT id, name FROM tbl") {
			gotErr = err
			break
		}
		check.True(t, gotErr != nil)
	})

	t.Run("scan error", func(t *testing.T) {
		// struct{} has no sql-tagged fields → scan fails after rows.Next() succeeds
		db := newQueryDB(t, dbxtest.NewRows("id", "name").Add(1, "Alice"), nil)
		var gotErr error
		for _, err := range dbx.Query[struct{}](ctx, db.QueryContext, "SELECT id, name FROM tbl") {
			gotErr = err
			break
		}
		check.True(t, gotErr != nil)
	})

	t.Run("early stop", func(t *testing.T) {
		db := newQueryDB(t, dbxtest.NewRows("id", "name").Add(1, "Alice").Add(2, "Bob").Add(3, "Carol"), nil)
		var got []intRow
		for r, err := range dbx.Query[intRow](ctx, db.QueryContext, "SELECT id, name FROM tbl") {
			assert.NotError(t, err)
			got = append(got, r)
			if len(got) == 1 {
				break
			}
		}
		assert.Equal(t, len(got), 1)
	})
}

func TestQueryRow(t *testing.T) {
	ctx := context.Background()

	t.Run("returns row", func(t *testing.T) {
		db := newQueryDB(t, dbxtest.NewRows("id", "name").Add(1, "Alice"), nil)
		r, err := dbx.QueryRow[intRow](ctx, db.QueryContext, "SELECT id, name FROM tbl WHERE id = 1")
		assert.NotError(t, err)
		check.Equal(t, r.ID, 1)
		check.Equal(t, r.Name, "Alice")
	})

	t.Run("no rows", func(t *testing.T) {
		db := newQueryDB(t, dbxtest.NewRows("id", "name"), nil)
		_, err := dbx.QueryRow[intRow](ctx, db.QueryContext, "SELECT id, name FROM tbl WHERE id = 999")
		assert.ErrorIs(t, err, sql.ErrNoRows)
	})

	t.Run("query error", func(t *testing.T) {
		queryErr := errors.New("db error")
		db := newQueryDB(t, nil, queryErr)
		_, err := dbx.QueryRow[intRow](ctx, db.QueryContext, "SELECT id, name FROM tbl")
		check.True(t, err != nil)
	})

	t.Run("scan error", func(t *testing.T) {
		// struct{} has no sql-tagged fields → scan fails after rows.Next() succeeds
		db := newQueryDB(t, dbxtest.NewRows("id", "name").Add(1, "Alice"), nil)
		_, err := dbx.QueryRow[struct{}](ctx, db.QueryContext, "SELECT id, name FROM tbl")
		check.True(t, err != nil)
	})

	t.Run("columns error on closed rows", func(t *testing.T) {
		// Obtain real *sql.Rows, close them, then hand them back via a custom
		// QueryFunc so that findOne's rows.Columns() call returns an error.
		db := newQueryDB(t, dbxtest.NewRows("id", "name").Add(1, "Alice"), nil)
		closedRows, err := db.QueryContext(ctx, "SELECT id, name FROM tbl")
		assert.NotError(t, err)
		closedRows.Close()

		qf := func(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
			return closedRows, nil
		}
		_, err = dbx.QueryRow[intRow](ctx, qf, "SELECT id, name FROM tbl")
		check.True(t, err != nil)
	})
}

func TestCursor(t *testing.T) {
	ctx := context.Background()

	t.Run("returns rows", func(t *testing.T) {
		db := newQueryDB(t, dbxtest.NewRows("id", "name").Add(1, "Alice").Add(2, "Bob"), nil)
		sqlRows, err := db.QueryContext(ctx, "SELECT id, name FROM tbl")
		assert.NotError(t, err)
		var got []intRow
		for r, err := range dbx.Cursor[intRow](sqlRows) {
			assert.NotError(t, err)
			got = append(got, r)
		}
		assert.Equal(t, len(got), 2)
		check.Equal(t, got[0].ID, 1)
		check.Equal(t, got[0].Name, "Alice")
	})

	t.Run("empty result", func(t *testing.T) {
		db := newQueryDB(t, dbxtest.NewRows("id", "name"), nil)
		sqlRows, err := db.QueryContext(ctx, "SELECT id, name FROM tbl")
		assert.NotError(t, err)
		var got []intRow
		for r, err := range dbx.Cursor[intRow](sqlRows) {
			assert.NotError(t, err)
			got = append(got, r)
		}
		assert.Equal(t, len(got), 0)
	})

	t.Run("columns error on closed rows", func(t *testing.T) {
		db := newQueryDB(t, dbxtest.NewRows("id", "name"), nil)
		sqlRows, err := db.QueryContext(ctx, "SELECT id, name FROM tbl")
		assert.NotError(t, err)
		sqlRows.Close() // pre-close so Columns() returns an error
		var gotErr error
		for _, err := range dbx.Cursor[intRow](sqlRows) {
			gotErr = err
			break
		}
		check.True(t, gotErr != nil)
	})

	t.Run("rows.Err after iteration", func(t *testing.T) {
		iterErr := errors.New("iteration error")
		db := dbxtest.NewDB(t, dbxtest.Driver{
			QueryContext: func(_ testing.TB, _ string, _ []any) (driver.Rows, error) {
				return &errAfterRows{
					inner:   dbxtest.NewRows("id", "name").Add(1, "Alice"),
					nextErr: iterErr,
				}, nil
			},
		})
		sqlRows, err := db.QueryContext(ctx, "SELECT id, name FROM tbl")
		assert.NotError(t, err)
		var gotErr error
		for _, err := range dbx.Cursor[intRow](sqlRows) {
			if err != nil {
				gotErr = err
			}
		}
		check.True(t, gotErr != nil)
	})

	t.Run("early stop closes rows", func(t *testing.T) {
		db := newQueryDB(t, dbxtest.NewRows("id", "name").Add(1, "Alice").Add(2, "Bob"), nil)
		sqlRows, err := db.QueryContext(ctx, "SELECT id, name FROM tbl")
		assert.NotError(t, err)
		for _, err := range dbx.Cursor[intRow](sqlRows) {
			assert.NotError(t, err)
			break
		}
		// rows.Close() is idempotent; calling it again should not error
		check.NotError(t, sqlRows.Close())
	})
}
