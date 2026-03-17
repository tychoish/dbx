package dbx

import (
	"context"
	"database/sql"
	"iter"

	"github.com/tychoish/fun/irt"
)

// Queryer is an interface implemented by [sql.DB] and [sql.Tx].
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Query executes a query that returns rows, scans each row into a T, and returns an iterator over the Ts.
// If an error occurs, the iterator yields it as the second value, and the caller should then stop the iteration.
// [Queryer] can be either [sql.DB] or [sql.Tx], the rest of the arguments are passed directly to [Queryer.QueryContext].
// Query fully manages the lifecycle of the [sql.Rows] returned by [Queryer.QueryContext], so the caller does not have to.
//
// The following Ts are supported:
//   - int (any kind)
//   - uint (any kind)
//   - float (any kind)
//   - bool
//   - string
//   - time.Time
//   - [sql.Scanner] (implemented by [sql.Null] types)
//   - any struct
//
// See the [sql.Rows.Scan] documentation for the scanning rules.
// If the query has multiple columns, T must be a struct, other types can only be used for single-column queries.
// The fields of a struct T must have the `sql:"COLUMN"` tag, where COLUMN is the name of the corresponding column in the query.
// Untagged and unexported and fields are ignored.
//
// If the caller prefers the result to be a slice rather than an iterator, Query can be combined with [Collect].
func Query[T any](ctx context.Context, q Queryer, query string, args ...any) iter.Seq2[T, error] {
	var cc cursor[T]
	return cc.findMany(ctx, q, query, args)
}

// QueryRow is a [Query] variant for queries that are expected to return at most one row,
// so instead of an iterator, it returns a single T.
// Like [sql.DB.QueryRow], QueryRow returns [sql.ErrNoRows] if the query selects no rows,
// otherwise it scans the first row and discards the rest.
// See the [Query] documentation for details on supported Ts.
func QueryRow[T any](ctx context.Context, q Queryer, query string, args ...any) (T, error) {
	var cc cursor[T]
	return cc.findOne(ctx, q, query, args)
}

func Cursor[T any](rows *sql.Rows) iter.Seq2[T, error] {
	var cc cursor[T]
	return irt.KVsplit(irt.WithHooks(irt.KVjoin(cc.rows(rows)), nil, func() { _ = rows.Close() }))
}
