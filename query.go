package dbx

import (
	"context"
	"database/sql"
	"iter"

	"github.com/tychoish/fun/irt"
)

// QueryFunc describes the operation that produces a cursor. 'QueryContext', provided by the
// database and transaction types, is the likely provider of this function.
type QueryFunc func(ctx context.Context, query string, args ...any) (*sql.Rows, error)

// Query executes a query that returns rows, scans each row into a T,
// and returns an iterator over the Ts. If an error occurs, the
// iterator yields it as the second value, and the caller should then
// stop the iteration. [QueryFunc] is typically provided by passing
// 'db.QueryContext' or 'tx.QueryContext' from a [sql.DB] or [sql.Tx],
// with the rest of the arguments passed directly to it. Query fully
// manages the lifecycle of the [sql.Rows] returned, so the caller does
// not have to.
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
//   - pointers to any supported type (e.g. *int, *struct, etc.)
//   - []any, map[string]any, []irt.KV[string, any], and iter.Seq2[string, any]
//   - typed slices ([]V), typed maps (map[string]V), typed KV slices ([]irt.KV[string, V]),
//     and typed iterators (iter.Seq2[string, V]) where V is directly scannable
//
// See the [sql.Rows.Scan] documentation for the scanning rules. If
// the query has multiple columns, T must be a struct or one of the
// supported collection types (slice, map, KV slice, seq2); scalar
// types can only be used for single-column queries. For struct Ts, the
// column name for each field is resolved in priority order: the `dbx` tag,
// then the `sql` tag, then the `db` tag, and finally the lowercase
// field name as a fallback. Unexported fields are always ignored.
func Query[T any](ctx context.Context, q QueryFunc, query string, args ...any) iter.Seq2[T, error] {
	var cc cursor[T]
	return cc.findMany(ctx, q, query, args)
}

// QueryRow is a [Query] variant for queries that are expected to
// return at most one row, so instead of an iterator, it returns a
// single T.
//
// Like [sql.DB.QueryRow], QueryRow returns [sql.ErrNoRows] if the
// query selects no rows, otherwise it scans the first row and
// discards the rest. See the [Query] documentation for details on
// supported Ts.
func QueryRow[T any](ctx context.Context, q QueryFunc, query string, args ...any) (T, error) {
	var cc cursor[T]
	return cc.findOne(ctx, q, query, args)
}

// Cursor takes an '*sql.Rows' object and processes it as an
// iterator. The 'sql.Rows' is closed when the iterator is exhausted
// or iteration ends (e.g. early return.)
func Cursor[T any](rows *sql.Rows) iter.Seq2[T, error] {
	var cc cursor[T]
	return irt.KVsplit(irt.WithHooks(irt.KVjoin(cc.rows(rows)), nil, func() { _ = rows.Close() }))
}
