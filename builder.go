// Package dbx implements convenience helpers for working with SQL queries.
package dbx

import (
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/tychoish/fun/dt"
	"github.com/tychoish/fun/irt"
	"github.com/tychoish/fun/strut"
)

// Builder is a string-oriented query builder to produce well formed
// parameterized queries without a new DSL, or mess string building by
// hand. Use either directly, using he With, WithSQL, and WithParams, or
// with the standalone SQL() helper.
//
// dbx.SQL() and Builder.With() work like a SQL-printf, where
// Builder.WithSQL and Builder.WithParams allow you to add SQL clauses
// and query parameters separately. In general, use dbx.SQL() for
// smaller queries, and use Builder.With() for longer or more
// complicated queries that you want to break up, using WithSQL and
// WithParams for specific cases as needed.
//
// The builder formats according to the given format and appends the
// result to the query. It works like fmt.Appendf meaning all the rules
// from the [fmt] package are applied. In addition, SQL supports special
// verbs that automatically expand to database placeholders.
//
//	-----------------------------------------------
//	| Database               | Verb | Placeholder |
//	|------------------------|------|-------------|
//	| MySQL, MariaDB, SQLite | %?   | ?           |
//	| PostgreSQL             | %$   | $N          |
//	| Microsoft SQL Server   | %@   | @pN         |
//	| Oracle Database        | %:   | :N          |
//	-----------------------------------------------
//
// Here, N is an auto-incrementing counter. For example, "%$, %$, %$"
// expands to "$1, $2, $3".
//
// If a special verb includes the "+" flag, it automatically expands
// to multiple placeholders.  For example, given the verb "%+?" and
// the argument []int{1, 2, 3}, the builder will produce"?, ?, ?" to the query
// and appends 1, 2, and 3 to the arguments.  You may want to use this
// flag to build "WHERE IN (...)" clauses.
//
// Make sure to always pass arguments from user input with placeholder
// verbs to avoid SQL injections.
type Builder struct {
	qb   queryBuilder
	sql  strut.Buffer
	args dt.List[formatter]
}

// SQL exposes a Printf style query builder that wraps 'dbx.Builder'
func SQL(tpl string, a ...any) (string, []any)           { return new(Builder).With(tpl, a...).Build() }
func (b *Builder) WithSQL(statement ...string) *Builder  { b.sql.Concat(statement...); return b }
func (b *Builder) WithParams(args ...any) *Builder       { b.args.Extend(b.pseq(args)); return b }
func (b *Builder) With(tpl string, args ...any) *Builder { return b.pushStmt(tpl).pushArgs(args) }
func (b *Builder) as(arg any) formatter                  { return formatter{arg: arg, builder: &b.qb} }
func (b *Builder) pseq(args []any) iter.Seq[formatter]   { return irt.Convert(irt.Slice(args), b.as) }
func (b *Builder) pushArgs(args []any) *Builder          { b.args.Extend(b.pseq(args)); return b }
func (b *Builder) pushStmt(s string) *Builder            { b.sql.WriteString(s); return b }

func (b *Builder) Build() (string, []any) {
	mut := strut.MakeMutable(b.sql.Len() + 4*b.args.Len())
	defer mut.Release()

	fmt.Fprintf(mut, b.sql.String(), irt.Collect(irt.Any(b.args.IteratorFront()), b.args.Len())...)
	return mut.String(), b.qb.args
}

// queryBuilder is a raw SQL query builder.
// The zero value is ready to use.
// Do not copy a non-zero queryBuilder.
type queryBuilder struct {
	args        []any
	counter     int
	placeholder rune
}

type formatter struct {
	arg     any
	builder *queryBuilder
}

// Format implements [fmt.Formatter].
func (f formatter) Format(s fmt.State, verb rune) {
	switch verb {
	case '?', '$', '@', ':':
		if f.builder.placeholder == 0 {
			f.builder.placeholder = verb
		} else if f.builder.placeholder != verb {
			panic("unexpected placeholder")
		}

		if s.Flag('+') {
			f.builder.appendAll(s, f.arg)
		} else {
			f.builder.appendOne(s, f.arg)
		}
	default:
		fmt.Fprintf(s, fmt.FormatString(s, verb), f.arg)
	}
}

var (
	builderSqlitePlaceholder = []byte{'?'}
	builderConjunction       = []byte{',', ' '}
)

func (b *queryBuilder) appendOne(w io.Writer, arg any) {
	b.counter++
	b.args = append(b.args, arg)

	switch b.placeholder {
	case '?':
		_, _ = w.Write(builderSqlitePlaceholder)
	case '$':
		fmt.Fprintf(w, "$%d", b.counter)
	case '@':
		fmt.Fprintf(w, "@p%d", b.counter)
	case ':':
		fmt.Fprintf(w, ":%d", b.counter)
	}
}

func (b *queryBuilder) appendAll(w io.Writer, arg any) {
	seq := getSlice(arg)
	if seq == nil {
		panic("non-slice argument")
	}
	var ct int
	for val := range seq {
		if ct > 0 {
			_, _ = w.Write(builderConjunction)
		}
		ct++
		b.appendOne(w, val)
	}
	if ct == 0 {
		panic("zero-length slice argument")
	}
}

func getSlice(arg any) iter.Seq[any] {
	switch data := arg.(type) {
	case []any:
		return irt.Slice(data)
	case []string:
		return irt.Any(irt.Slice(data))
	case []bool:
		return irt.Any(irt.Slice(data))
	case []int:
		return irt.Any(irt.Slice(data))
	case []int8:
		return irt.Any(irt.Slice(data))
	case []int16:
		return irt.Any(irt.Slice(data))
	case []int32:
		return irt.Any(irt.Slice(data))
	case []int64:
		return irt.Any(irt.Slice(data))
	case []uint:
		return irt.Any(irt.Slice(data))
	case []uint8:
		return irt.Any(irt.Slice(data))
	case []uint16:
		return irt.Any(irt.Slice(data))
	case []uint32:
		return irt.Any(irt.Slice(data))
	case []uint64:
		return irt.Any(irt.Slice(data))
	case []time.Time:
		return irt.Any(irt.Slice(data))
	case []float64:
		return irt.Any(irt.Slice(data))
	case []float32:
		return irt.Any(irt.Slice(data))
	case [][]byte:
		return irt.Any(irt.Slice(data))
	case []*any:
		return irt.Any(irt.Slice(data))
	case []*string:
		return irt.Any(irt.Slice(data))
	case []*bool:
		return irt.Any(irt.Slice(data))
	case []*int:
		return irt.Any(irt.Slice(data))
	case []*int8:
		return irt.Any(irt.Slice(data))
	case []*int16:
		return irt.Any(irt.Slice(data))
	case []*int32:
		return irt.Any(irt.Slice(data))
	case []*int64:
		return irt.Any(irt.Slice(data))
	case []*uint:
		return irt.Any(irt.Slice(data))
	case []*uint8:
		return irt.Any(irt.Slice(data))
	case []*uint16:
		return irt.Any(irt.Slice(data))
	case []*uint32:
		return irt.Any(irt.Slice(data))
	case []*uint64:
		return irt.Any(irt.Slice(data))
	case []*time.Time:
		return irt.Any(irt.Slice(data))
	case []*float64:
		return irt.Any(irt.Slice(data))
	case []*float32:
		return irt.Any(irt.Slice(data))
	case []*[]byte:
		return irt.Any(irt.Slice(data))
	default:
		return nil
	}
}
