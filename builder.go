// Package dbx implements convenience helpers for working with SQL queries.
package dbx

import (
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/tychoish/fun/irt"
	"github.com/tychoish/fun/strut"
)

// queryBuilder is a raw SQL query builder.
// The zero value is ready to use.
// Do not copy a non-zero queryBuilder.
type queryBuilder struct {
	args        []any
	counter     int
	placeholder rune
}

// SQL exposes a Printf style query builder.
//
// SQL formats according to the given format and appends the result
// to the query.  It works like fmt.Appendf meaning all the rules from
// the [fmt] package are applied.  In addition, SQL supports
// special verbs that automatically expand to database placeholders.
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
// Here, N is an auto-incrementing counter.
// For example, "%$, %$, %$" expands to "$1, $2, $3".
//
// If a special verb includes the "+" flag, it automatically expands
// to multiple placeholders.  For example, given the verb "%+?" and
// the argument []int{1, 2, 3}, SQL writes "?, ?, ?" to the query
// and appends 1, 2, and 3 to the arguments.  You may want to use this
// flag to build "WHERE IN (...)" clauses.
//
// Make sure to always pass arguments from user input with placeholder
// verbs to avoid SQL injections.
func SQL(tpl string, a ...any) (string, []any) {
	var b queryBuilder

	fs := make([]any, len(a))
	for i := range a {
		fs[i] = formatter{arg: a[i], builder: &b}
	}

	mut := strut.MakeMutable(len(tpl) + (len(a) * 4))
	defer mut.Release()

	fmt.Fprintf(mut, tpl, fs...)

	return mut.String(), b.args
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
