// Package queriestest implements utilities for testing SQL queries.
package dbxtest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"slices"
	"testing"
)

// Driver is a convenience helper to easily create an implementation of [driver.Driver] for use in tests.
type Driver struct {
	// ExecContext is a test implementation of [driver.ExecerContext].
	// If the code being tested uses [sql.Result],
	// ExecContext should return a [driver.Result] created with [NewResult].
	ExecContext func(tb testing.TB, query string, args []any) (driver.Result, error)

	// QueryContext is a test implementation of [driver.QueryerContext].
	// If the code being tested uses [sql.Rows],
	// QueryContext should return [Rows] created with [NewRows].
	QueryContext func(tb testing.TB, query string, args []any) (driver.Rows, error)
}

// NewDB creates a test [sql.DB] backed by the given [Driver].
func NewDB(tb testing.TB, d Driver) *sql.DB {
	name := tb.Name()
	sql.Register(name, testDriver{tb, d})
	db, _ := sql.Open(name, "")
	return db
}

var (
	_ driver.Driver         = testDriver{}
	_ driver.Conn           = testDriver{}
	_ driver.ConnBeginTx    = testDriver{}
	_ driver.Tx             = testDriver{}
	_ driver.ExecerContext  = testDriver{}
	_ driver.QueryerContext = testDriver{}
)

type testDriver struct {
	tb testing.TB
	Driver
}

// Open implements [driver.Driver].
func (d testDriver) Open(name string) (driver.Conn, error) { return d, nil }

// Prepare implements [driver.Conn].
func (d testDriver) Prepare(query string) (driver.Stmt, error) { panic("use ExecContext") }

// Close implements [driver.Conn].
func (d testDriver) Close() error { return nil }

// Begin implements [driver.Conn].
func (d testDriver) Begin() (driver.Tx, error) { panic("use BeginTx") }

// BeginTx implements [driver.ConnBeginTx].
func (d testDriver) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return d, nil
}

// Commit implements [driver.Tx].
func (d testDriver) Commit() error { return nil }

// Rollback implements [driver.Tx].
func (d testDriver) Rollback() error { return nil }

// ExecContext implements [driver.ExecerContext].
func (d testDriver) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if d.Driver.ExecContext == nil {
		panic("ExecContext must be set")
	}
	return d.Driver.ExecContext(d.tb, query, namedToAny(args))
}

// QueryContext implements [driver.QueryerContext].
func (d testDriver) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if d.Driver.QueryContext == nil {
		panic("QueryContext must be set")
	}
	return d.Driver.QueryContext(d.tb, query, namedToAny(args))
}

func namedToAny(named []driver.NamedValue) []any {
	args := make([]any, len(named))
	for i, arg := range named {
		args[i] = arg.Value
	}
	return args
}

type result struct {
	lastInsertId int64
	rowsAffected int64
}

// NewResult creates a [driver.Result] for use in tests.
func NewResult(lastInsertId, rowsAffected int64) driver.Result {
	return result{lastInsertId, rowsAffected}
}

// LastInsertId implements [driver.Result].
func (r result) LastInsertId() (int64, error) { return r.lastInsertId, nil }

// RowsAffected implements [driver.Result].
func (r result) RowsAffected() (int64, error) { return r.rowsAffected, nil }

// Rows is an implementation of [driver.Rows] for use in tests.
type Rows struct {
	columns []string
	values  [][]driver.Value
}

// NewRows creates a [Rows] with the given column names.
func NewRows(columns ...string) *Rows {
	return &Rows{columns: columns}
}

// Add appends a row of values to the [Rows].
func (r *Rows) Add(values ...driver.Value) *Rows {
	r.values = append(r.values, values)
	return r
}

// Columns implements [driver.Rows].
func (r *Rows) Columns() []string { return r.columns }

// Close implements [driver.Rows].
func (r *Rows) Close() error { return nil }

// Next implements [driver.Rows].
func (r *Rows) Next(values []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	for i := range min(len(values), len(r.values[0])) {
		values[i] = r.values[0][i]
	}
	r.values = slices.Delete(r.values, 0, 1)
	return nil
}
