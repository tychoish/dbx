package dbx

import (
	"database/sql"
	"database/sql/driver"
	"iter"
	"testing"

	queriestest "github.com/tychoish/dbx/internal/dbxtest"
	"github.com/tychoish/fun/irt"
)

func BenchmarkQuery_withScanner(b *testing.B) {
	db := newDB(b)
	b.ReportAllocs()
	for b.Loop() {
		for range Query[mediumRow](b.Context(), db, "") {
		}
	}
}

func BenchmarkQuery_withoutScanner(b *testing.B) {
	db := newDB(b)
	b.ReportAllocs()
	for b.Loop() {
		rows, _ := db.QueryContext(b.Context(), "")
		for rows.Next() {
			var row mediumRow
			_ = rows.Scan(&row.A, &row.B, &row.C, &row.D, &row.E, &row.F, &row.G, &row.H)
		}
		_ = rows.Close()
	}
}

func BenchmarkQueryRow_withScanner(b *testing.B) {
	db := newDB(b)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = QueryRow[mediumRow](b.Context(), db, "")
	}
}

func BenchmarkQueryRow_withoutScanner(b *testing.B) {
	db := newDB(b)
	b.ReportAllocs()
	for b.Loop() {
		var row mediumRow
		_ = db.QueryRowContext(b.Context(), "").
			Scan(&row.A, &row.B, &row.C, &row.D, &row.E, &row.F, &row.G, &row.H)
	}
}

func newDB(tb testing.TB) *sql.DB {
	return queriestest.NewDB(tb, queriestest.Driver{
		QueryContext: func(testing.TB, string, []any) (driver.Rows, error) {
			return queriestest.NewRows("a", "b", "c", "d", "e", "f", "g", "h").
				Add(1, 2, 3, 4, 5, 6, 7, 8).
				Add(1, 2, 3, 4, 5, 6, 7, 8).
				Add(1, 2, 3, 4, 5, 6, 7, 8).
				Add(1, 2, 3, 4, 5, 6, 7, 8).
				Add(1, 2, 3, 4, 5, 6, 7, 8).
				Add(1, 2, 3, 4, 5, 6, 7, 8).
				Add(1, 2, 3, 4, 5, 6, 7, 8).
				Add(1, 2, 3, 4, 5, 6, 7, 8), nil
		},
	})
}

func Benchmark_scan_smallRowWithCache(b *testing.B)     { benchmarkScan[smallRow](b, true) }
func Benchmark_scan_smallRowWithoutCache(b *testing.B)  { benchmarkScan[smallRow](b, false) }
func Benchmark_scan_mediumRowWithCache(b *testing.B)    { benchmarkScan[mediumRow](b, true) }
func Benchmark_scan_mediumRowWithoutCache(b *testing.B) { benchmarkScan[mediumRow](b, false) }
func Benchmark_scan_largeRowWithCache(b *testing.B)     { benchmarkScan[largeRow](b, true) }
func Benchmark_scan_largeRowWithoutCache(b *testing.B)  { benchmarkScan[largeRow](b, false) }

func benchmarkScan[T dst](b *testing.B, withCache bool) {
	var t T
	columns := t.columns()
	s := mockScanner{values: t.values()}

	b.ReportAllocs()
	for b.Loop() {
		cc := cursor[T]{noCache: !withCache}
		_, _ = cc.scan(&s, columns)
	}
}

type dst interface {
	columns() []string
	values() []any
}

type smallRow struct {
	A int `sql:"a"`
	B int `sql:"b"`
	C int `sql:"c"`
	D int `sql:"d"`
}

func (smallRow) columns() []string { return []string{"a", "b", "c", "d"} }
func (smallRow) values() []any     { return []any{1, 2, 3, 4} }

type mediumRow struct {
	A int `sql:"a"`
	B int `sql:"b"`
	C int `sql:"c"`
	D int `sql:"d"`
	E int `sql:"e"`
	F int `sql:"f"`
	G int `sql:"g"`
	H int `sql:"h"`
}

func (mediumRow) columns() []string { return []string{"a", "b", "c", "d", "e", "f", "g", "h"} }
func (mediumRow) values() []any     { return []any{1, 2, 3, 4, 5, 6, 7, 8} }

type largeRow struct {
	A int `sql:"a"`
	B int `sql:"b"`
	C int `sql:"c"`
	D int `sql:"d"`
	E int `sql:"e"`
	F int `sql:"f"`
	G int `sql:"g"`
	H int `sql:"h"`
	I int `sql:"i"`
	J int `sql:"j"`
	K int `sql:"k"`
	L int `sql:"l"`
	M int `sql:"m"`
	N int `sql:"n"`
	O int `sql:"o"`
	P int `sql:"p"`
}

func (largeRow) columns() []string {
	return []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"}
}

func (largeRow) values() []any {
	return []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
}

// --- cursor-vs-direct scan benchmarks (4 columns, plan pre-warmed) -----------
//
// Each Cursor/Direct pair measures the same logical operation so the numbers
// are directly comparable. "Direct" is hand-written code with no dispatch
// overhead; "Cursor" goes through cursor.scan with a pre-built plan.

var (
	bench4cols = []string{"a", "b", "c", "d"}
	bench4vals = []any{1, 2, 3, 4}
	bench1cols = []string{"v"}
	bench1vals = []any{42}
)

// benchmarkCursorWarm pre-builds the scan plan, then measures per-row cost.
func benchmarkCursorWarm[T any](b *testing.B, columns []string, values []any) {
	b.Helper()
	s := mockScanner{values: values}
	cc := cursor[T]{}
	_, _ = cc.scan(&s, columns) // build plan once
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = cc.scan(&s, columns)
	}
}

// scalar -----------------------------------------------------------------------

func BenchmarkCursorScan_scalar(b *testing.B) {
	benchmarkCursorWarm[int](b, bench1cols, bench1vals)
}
func BenchmarkDirectScan_scalar(b *testing.B) {
	s := mockScanner{values: bench1vals}
	var v int
	b.ReportAllocs()
	for b.Loop() {
		_ = s.Scan(&v)
	}
}

// struct (4 columns) -----------------------------------------------------------

func BenchmarkCursorScan_struct4(b *testing.B) {
	benchmarkCursorWarm[smallRow](b, bench4cols, bench4vals)
}
func BenchmarkDirectScan_struct4(b *testing.B) {
	s := mockScanner{values: bench4vals}
	var row smallRow
	b.ReportAllocs()
	for b.Loop() {
		_ = s.Scan(&row.A, &row.B, &row.C, &row.D)
	}
}

// []int typed slice ------------------------------------------------------------

func BenchmarkCursorScan_typedSlice(b *testing.B) {
	benchmarkCursorWarm[[]int](b, bench4cols, bench4vals)
}
func BenchmarkDirectScan_typedSlice(b *testing.B) {
	s := mockScanner{values: bench4vals}
	var a, b2, c, d int
	b.ReportAllocs()
	for b.Loop() {
		_ = s.Scan(&a, &b2, &c, &d)
		_ = []int{a, b2, c, d}
	}
}

// []irt.KV[string, int] typed KV -----------------------------------------------

func BenchmarkCursorScan_typedKV(b *testing.B) {
	benchmarkCursorWarm[[]irt.KV[string, int]](b, bench4cols, bench4vals)
}
func BenchmarkDirectScan_typedKV(b *testing.B) {
	s := mockScanner{values: bench4vals}
	var a, b2, c, d int
	b.ReportAllocs()
	for b.Loop() {
		_ = s.Scan(&a, &b2, &c, &d)
		_ = []irt.KV[string, int]{
			irt.MakeKV(bench4cols[0], a), irt.MakeKV(bench4cols[1], b2),
			irt.MakeKV(bench4cols[2], c), irt.MakeKV(bench4cols[3], d),
		}
	}
}

// iter.Seq2[string, int] typed Seq2 --------------------------------------------
//
// typedSeq2 is the most expensive cursor type. Per-row cost breaks down as:
//   - N allocs for ptrs ([]reflect.New): required because the iterator is lazy
//     and captures those pointers directly; if rows share ptrs, consecutive
//     iterators alias each other.
//   - 1 alloc for args slice
//   - 1 alloc for reflect.MakeFunc (wraps a reflect-dispatched closure)
//   - reflect closure alloc per MakeFunc call
//
// Three optimisation options were considered:
//
//   Option 1 — Borrow semantics: pre-allocate ptrs at build time (static plan),
//   document that the returned Seq2 is valid only until the next row is scanned.
//   Eliminates the per-row ptr allocs. Changes the caller contract; suitable
//   only if the iterator is consumed immediately inside a range loop.
//
//   Option 2 — Eager evaluation: pre-allocate ptrs (static plan), scan into them,
//   then in postScan snapshot the values into a fresh []V and set v to a Seq2 that
//   iterates the snapshot. Alloc count matches typedSlice (3). reflect.MakeFunc can
//   be avoided entirely: construct a concrete func literal and pass it to
//   reflect.ValueOf — the same technique mappingSeq2StringAny uses for the
//   iter.Seq2[string,any] case. This only works if V is known at compile time, which
//   requires a separate generic entry point (see Option 3).
//
//   Option 3 — Separate generic entry point (e.g. Seq2Query[V any]): exposes V as
//   a type parameter, allowing a concrete func(yield func(string,V) bool) closure
//   to be built without any reflection, matching the DirectScan_typedSeq2 baseline.
//   Requires a distinct API surface alongside the general Query[T] path.
//
// Current implementation uses reflect.MakeFunc (no option applied).

func BenchmarkCursorScan_typedSeq2(b *testing.B) {
	benchmarkCursorWarm[iter.Seq2[string, int]](b, bench4cols, bench4vals)
}
func BenchmarkDirectScan_typedSeq2(b *testing.B) {
	s := mockScanner{values: bench4vals}
	var a, b2, c, d int
	b.ReportAllocs()
	for b.Loop() {
		_ = s.Scan(&a, &b2, &c, &d)
		vals := [4]int{a, b2, c, d}
		_ = iter.Seq2[string, int](func(yield func(string, int) bool) {
			for i, k := range bench4cols {
				if !yield(k, vals[i]) {
					break
				}
			}
		})
	}
}

// []irt.KV[string, any] any KV -------------------------------------------------

func BenchmarkCursorScan_anyKV(b *testing.B) {
	benchmarkCursorWarm[[]irt.KV[string, any]](b, bench4cols, bench4vals)
}
func BenchmarkDirectScan_anyKV(b *testing.B) {
	s := mockScanner{values: bench4vals}
	var vals [4]any
	ptrs := []any{&vals[0], &vals[1], &vals[2], &vals[3]}
	b.ReportAllocs()
	for b.Loop() {
		_ = s.Scan(ptrs...)
		_ = []irt.KV[string, any]{
			irt.MakeKV(bench4cols[0], vals[0]), irt.MakeKV(bench4cols[1], vals[1]),
			irt.MakeKV(bench4cols[2], vals[2]), irt.MakeKV(bench4cols[3], vals[3]),
		}
	}
}

// iter.Seq2[string, any] any Seq2 ----------------------------------------------

func BenchmarkCursorScan_anySeq2(b *testing.B) {
	benchmarkCursorWarm[iter.Seq2[string, any]](b, bench4cols, bench4vals)
}
func BenchmarkDirectScan_anySeq2(b *testing.B) {
	s := mockScanner{values: bench4vals}
	var vals [4]any
	ptrs := []any{&vals[0], &vals[1], &vals[2], &vals[3]}
	b.ReportAllocs()
	for b.Loop() {
		_ = s.Scan(ptrs...)
		snapshot := vals // copy array to capture per-iteration values
		_ = iter.Seq2[string, any](func(yield func(string, any) bool) {
			for i, k := range bench4cols {
				if !yield(k, snapshot[i]) {
					break
				}
			}
		})
	}
}
