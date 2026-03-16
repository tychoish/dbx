package dbx

import (
	"database/sql"
	"errors"
	"iter"
	"reflect"
	"testing"
	"time"

	"github.com/tychoish/fun/assert"
	"github.com/tychoish/fun/assert/check"
	"github.com/tychoish/fun/irt"
)

// ---- helpers ----------------------------------------------------------------

// scanWith creates a fresh cursor[T] with optional noCache and calls scan once.
func scanWith[T any](s scanner, columns []string, noCache bool) (T, error) {
	cc := cursor[T]{noCache: noCache}
	return cc.scan(s, columns)
}

// scanOnce is shorthand for scanWith with caching enabled.
func scanOnce[T any](s scanner, columns []string) (T, error) {
	return scanWith[T](s, columns, false)
}

// ---- error sentinel tests ---------------------------------------------------

func TestScan_errors(t *testing.T) {
	t.Run("no columns", func(t *testing.T) {
		_, err := scanOnce[int](nil, nil)
		check.ErrorIs(t, err, errNoColumns)
	})
	t.Run("no columns empty slice", func(t *testing.T) {
		_, err := scanOnce[int](nil, []string{})
		check.ErrorIs(t, err, errNoColumns)
	})
	t.Run("scalar with multiple columns", func(t *testing.T) {
		_, err := scanOnce[int](nil, []string{"a", "b"})
		check.ErrorIs(t, err, errNonStructT)
	})
	t.Run("string with multiple columns", func(t *testing.T) {
		_, err := scanOnce[string](nil, []string{"a", "b"})
		check.ErrorIs(t, err, errNonStructT)
	})
	t.Run("bool with multiple columns", func(t *testing.T) {
		_, err := scanOnce[bool](nil, []string{"a", "b"})
		check.ErrorIs(t, err, errNonStructT)
	})
	t.Run("sql.Null with multiple columns", func(t *testing.T) {
		_, err := scanOnce[sql.Null[string]](nil, []string{"a", "b"})
		check.ErrorIs(t, err, errNonStructT)
	})
	t.Run("struct missing column tag", func(t *testing.T) {
		_, err := scanOnce[struct{ Foo int }](nil, []string{"foo"})
		check.ErrorIs(t, err, errNoStructField)
	})
	t.Run("struct empty tags missing column", func(t *testing.T) {
		type row struct {
			A int `sql:"a"`
		}
		_, err := scanOnce[row](nil, []string{"a", "b"})
		check.ErrorIs(t, err, errNoStructField)
	})
	t.Run("unsupported: complex64", func(t *testing.T) {
		_, err := scanOnce[complex64](nil, []string{"x"})
		check.ErrorIs(t, err, errUnsupportedT)
	})
	t.Run("unsupported: complex128", func(t *testing.T) {
		_, err := scanOnce[complex128](nil, []string{"x"})
		check.ErrorIs(t, err, errUnsupportedT)
	})
	t.Run("unsupported: pointer to complex64", func(t *testing.T) {
		_, err := scanOnce[*complex64](nil, []string{"x"})
		check.ErrorIs(t, err, errUnsupportedT)
	})
	t.Run("unsupported: chan", func(t *testing.T) {
		_, err := scanOnce[chan int](nil, []string{"x"})
		check.ErrorIs(t, err, errUnsupportedT)
	})
	t.Run("pointer to int with multiple columns", func(t *testing.T) {
		_, err := scanOnce[*int](nil, []string{"a", "b"})
		check.ErrorIs(t, err, errNonStructT)
	})
	t.Run("scan error propagated", func(t *testing.T) {
		want := errors.New("scan failed")
		type row struct {
			A int `sql:"a"`
		}
		s := mockScanner{err: want}
		_, err := scanOnce[row](&s, []string{"a"})
		check.ErrorIs(t, err, want)
	})
	t.Run("scan error propagated for scalar", func(t *testing.T) {
		want := errors.New("scan failed")
		s := mockScanner{err: want}
		_, err := scanOnce[int](&s, []string{"a"})
		check.ErrorIs(t, err, want)
	})
	t.Run("slice of non-struct elem (reaches scannableKVStringSliceElem)", func(t *testing.T) {
		// []complex64: scannableSliceElem returns false (complex64 not scannable),
		// scannableKVStringSliceElem hits the elem.Kind() != Struct branch.
		_, err := scanOnce[[]complex64](nil, []string{"x"})
		check.ErrorIs(t, err, errUnsupportedT)
	})
	t.Run("slice of 2-field struct with wrong key field name", func(t *testing.T) {
		// scannableKVStringSliceElem: keyField.Name != "Key" branch.
		type wrongKey struct {
			X     string
			Value int
		}
		_, err := scanOnce[[]wrongKey](nil, []string{"x"})
		check.ErrorIs(t, err, errUnsupportedT)
	})
	t.Run("slice of 2-field struct with wrong value field name", func(t *testing.T) {
		// scannableKVStringSliceElem: valField.Name != "Value" branch.
		type wrongVal struct {
			Key  string
			Data int
		}
		_, err := scanOnce[[]wrongVal](nil, []string{"x"})
		check.ErrorIs(t, err, errUnsupportedT)
	})
	t.Run("seq iterator (yield has 1 arg, not 2)", func(t *testing.T) {
		// iter.Seq[string] = func(func(string) bool); yield.NumIn()==1 ≠ 2
		// hits the seq2StringScannableValueType inner-yield-shape branch.
		_, err := scanOnce[iter.Seq[string]](nil, []string{"x"})
		check.ErrorIs(t, err, errUnsupportedT)
	})
	t.Run("seq2 with non-string key type", func(t *testing.T) {
		// iter.Seq2[int, string]: yield.In(0)==int ≠ string
		// hits seq2StringScannableValueType yield.In(0) != reflectTypeString branch.
		_, err := scanOnce[iter.Seq2[int, string]](nil, []string{"x"})
		check.ErrorIs(t, err, errUnsupportedT)
	})
}

// ---- scalar type tests -------------------------------------------------------

func TestScan_scalars(t *testing.T) {
	col := []string{"v"}

	cases := []struct {
		name string
		fn   func(*testing.T)
	}{
		{"bool true", func(t *testing.T) {
			v, err := scanOnce[bool](&mockScanner{values: []any{true}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, true)
		}},
		{"bool false", func(t *testing.T) {
			v, err := scanOnce[bool](&mockScanner{values: []any{false}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, false)
		}},
		{"int", func(t *testing.T) {
			v, err := scanOnce[int](&mockScanner{values: []any{42}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, 42)
		}},
		{"int8", func(t *testing.T) {
			v, err := scanOnce[int8](&mockScanner{values: []any{int8(-8)}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, int8(-8))
		}},
		{"int16", func(t *testing.T) {
			v, err := scanOnce[int16](&mockScanner{values: []any{int16(-16)}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, int16(-16))
		}},
		{"int32", func(t *testing.T) {
			v, err := scanOnce[int32](&mockScanner{values: []any{int32(-32)}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, int32(-32))
		}},
		{"int64", func(t *testing.T) {
			v, err := scanOnce[int64](&mockScanner{values: []any{int64(-64)}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, int64(-64))
		}},
		{"uint", func(t *testing.T) {
			v, err := scanOnce[uint](&mockScanner{values: []any{uint(1)}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, uint(1))
		}},
		{"uint8", func(t *testing.T) {
			v, err := scanOnce[uint8](&mockScanner{values: []any{uint8(8)}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, uint8(8))
		}},
		{"uint16", func(t *testing.T) {
			v, err := scanOnce[uint16](&mockScanner{values: []any{uint16(16)}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, uint16(16))
		}},
		{"uint32", func(t *testing.T) {
			v, err := scanOnce[uint32](&mockScanner{values: []any{uint32(32)}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, uint32(32))
		}},
		{"uint64", func(t *testing.T) {
			v, err := scanOnce[uint64](&mockScanner{values: []any{uint64(64)}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, uint64(64))
		}},
		{"float32", func(t *testing.T) {
			v, err := scanOnce[float32](&mockScanner{values: []any{float32(1.5)}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, float32(1.5))
		}},
		{"float64", func(t *testing.T) {
			v, err := scanOnce[float64](&mockScanner{values: []any{float64(2.5)}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, float64(2.5))
		}},
		{"string", func(t *testing.T) {
			v, err := scanOnce[string](&mockScanner{values: []any{"hello"}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, "hello")
		}},
		{"time.Time", func(t *testing.T) {
			now := time.Now().Truncate(time.Second)
			v, err := scanOnce[time.Time](&mockScanner{values: []any{now}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, now)
		}},
		{"sql.Null[string] valid", func(t *testing.T) {
			v, err := scanOnce[sql.Null[string]](&mockScanner{values: []any{"test"}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, sql.Null[string]{V: "test", Valid: true})
		}},
		{"sql.Null[int64] valid", func(t *testing.T) {
			v, err := scanOnce[sql.Null[int64]](&mockScanner{values: []any{int64(7)}}, col)
			assert.NotError(t, err)
			check.Equal(t, v, sql.Null[int64]{V: 7, Valid: true})
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, tc.fn)
	}
}

// ---- struct tests ------------------------------------------------------------

func TestScan_struct(t *testing.T) {
	t.Run("basic tagged fields", func(t *testing.T) {
		type row struct {
			Name string `sql:"name"`
			Age  int    `sql:"age"`
		}
		s := mockScanner{values: []any{"alice", 30}}
		v, err := scanOnce[row](&s, []string{"name", "age"})
		assert.NotError(t, err)
		check.Equal(t, v.Name, "alice")
		check.Equal(t, v.Age, 30)
	})

	t.Run("db tag", func(t *testing.T) {
		type row struct {
			Name string `db:"name"`
			Age  int    `db:"age"`
		}
		s := mockScanner{values: []any{"bob", 25}}
		v, err := scanOnce[row](&s, []string{"name", "age"})
		assert.NotError(t, err)
		check.Equal(t, v.Name, "bob")
		check.Equal(t, v.Age, 25)
	})

	t.Run("sql tag takes precedence over db tag", func(t *testing.T) {
		type row struct {
			X int `sql:"x" db:"y"`
		}
		s := mockScanner{values: []any{99}}
		v, err := scanOnce[row](&s, []string{"x"})
		assert.NotError(t, err)
		check.Equal(t, v.X, 99)
	})

	t.Run("subset of columns", func(t *testing.T) {
		type row struct {
			A int `sql:"a"`
			B int `sql:"b"`
			C int `sql:"c"`
		}
		// only scan columns a and c; b stays zero
		s := mockScanner{values: []any{1, 3}}
		v, err := scanOnce[row](&s, []string{"a", "c"})
		assert.NotError(t, err)
		check.Equal(t, v.A, 1)
		check.Equal(t, v.B, 0)
		check.Equal(t, v.C, 3)
	})

	t.Run("embedded struct promoted fields", func(t *testing.T) {
		type embedded struct {
			Baz bool `sql:"baz"`
		}
		type row struct {
			embedded
			Foo int    `sql:"foo"`
			Bar string `sql:"bar"`
		}
		s := mockScanner{values: []any{1, "test", true}}
		v, err := scanOnce[row](&s, []string{"foo", "bar", "baz"})
		assert.NotError(t, err)
		check.Equal(t, v.Foo, 1)
		check.Equal(t, v.Bar, "test")
		check.Equal(t, v.Baz, true)
	})

	t.Run("untagged fields ignored", func(t *testing.T) {
		type row struct {
			Tagged   int `sql:"x"`
			Untagged int
		}
		s := mockScanner{values: []any{5}}
		v, err := scanOnce[row](&s, []string{"x"})
		assert.NotError(t, err)
		check.Equal(t, v.Tagged, 5)
		check.Equal(t, v.Untagged, 0)
	})

	t.Run("empty tag ignored", func(t *testing.T) {
		type row struct {
			A int `sql:"a"`
			B int `sql:""`
		}
		s := mockScanner{values: []any{7}}
		v, err := scanOnce[row](&s, []string{"a"})
		assert.NotError(t, err)
		check.Equal(t, v.A, 7)
		check.Equal(t, v.B, 0)
	})

	t.Run("unexported fields ignored", func(t *testing.T) {
		type row struct {
			Pub int `sql:"pub"`
			pri int //nolint
		}
		s := mockScanner{values: []any{3}}
		v, err := scanOnce[row](&s, []string{"pub"})
		assert.NotError(t, err)
		check.Equal(t, v.Pub, 3)
		check.Equal(t, v.pri, 0)
	})

	t.Run("column order independent of struct order", func(t *testing.T) {
		type row struct {
			A int `sql:"a"`
			B int `sql:"b"`
			C int `sql:"c"`
		}
		s := mockScanner{values: []any{3, 1, 2}}
		v, err := scanOnce[row](&s, []string{"c", "a", "b"})
		assert.NotError(t, err)
		check.Equal(t, v.A, 1)
		check.Equal(t, v.B, 2)
		check.Equal(t, v.C, 3)
	})
}

// ---- slice/map/kv/seq2 tests ------------------------------------------------

func TestScan_sliceOfAny(t *testing.T) {
	t.Run("values preserved in order", func(t *testing.T) {
		s := mockScanner{values: []any{1, "two", true}}
		v, err := scanOnce[[]any](&s, []string{"a", "b", "c"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 3)
			check.Equal(t, v[0], 1)
			check.Equal(t, v[1], "two")
			check.Equal(t, v[2], true)
	})
	t.Run("independent slices per row", func(t *testing.T) {
		cc := cursor[[]any]{}
		cols := []string{"x", "y"}

		s1 := mockScanner{values: []any{1, 2}}
		row1, err := cc.scan(&s1, cols)
		assert.NotError(t, err)

		s2 := mockScanner{values: []any{3, 4}}
		row2, err := cc.scan(&s2, cols)
		assert.NotError(t, err)

		// row1 must not be aliased by row2
		check.Equal(t, row1[0], 1)
		check.Equal(t, row2[0], 3)
	})
}

func TestScan_mapStringAny(t *testing.T) {
	t.Run("keys are column names", func(t *testing.T) {
		s := mockScanner{values: []any{42, "world"}}
		v, err := scanOnce[map[string]any](&s, []string{"id", "name"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 2)
			check.Equal(t, v["id"], 42)
			check.Equal(t, v["name"], "world")
	})
	t.Run("independent maps per row", func(t *testing.T) {
		cc := cursor[map[string]any]{}
		cols := []string{"k"}

		s1 := mockScanner{values: []any{"first"}}
		row1, err := cc.scan(&s1, cols)
		assert.NotError(t, err)

		s2 := mockScanner{values: []any{"second"}}
		row2, err := cc.scan(&s2, cols)
		assert.NotError(t, err)

		check.Equal(t, row1["k"], "first")
		check.Equal(t, row2["k"], "second")
	})
}

func TestScan_kvStringAny(t *testing.T) {
	s := mockScanner{values: []any{42, "world"}}
	v, err := scanOnce[[]irt.KV[string, any]](&s, []string{"id", "name"})
	assert.NotError(t, err)
	check.Equal(t, len(v), 2)
		check.Equal(t, v[0].Key, "id")
		check.Equal(t, v[0].Value, 42)
		check.Equal(t, v[1].Key, "name")
		check.Equal(t, v[1].Value, "world")
}

func TestScan_seq2StringAny(t *testing.T) {
	s := mockScanner{values: []any{42, "world"}}
	v, err := scanOnce[iter.Seq2[string, any]](&s, []string{"id", "name"})
	assert.NotError(t, err)
	m := irt.Collect2(v)
	check.Equal(t, len(m), 2)
		check.Equal(t, m["id"], 42)
		check.Equal(t, m["name"], "world")
}

func TestScan_typedSlice(t *testing.T) {
	t.Run("[]string", func(t *testing.T) {
		s := mockScanner{values: []any{"a", "b", "c"}}
		v, err := scanOnce[[]string](&s, []string{"x", "y", "z"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 3)
			check.Equal(t, v[0], "a")
			check.Equal(t, v[1], "b")
			check.Equal(t, v[2], "c")
	})
	t.Run("[]int", func(t *testing.T) {
		s := mockScanner{values: []any{10, 20}}
		v, err := scanOnce[[]int](&s, []string{"p", "q"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 2)
			check.Equal(t, v[0], 10)
			check.Equal(t, v[1], 20)
	})
	t.Run("independent slices per row", func(t *testing.T) {
		cc := cursor[[]string]{}
		cols := []string{"v"}

		s1 := mockScanner{values: []any{"first"}}
		row1, err := cc.scan(&s1, cols)
		assert.NotError(t, err)

		s2 := mockScanner{values: []any{"second"}}
		row2, err := cc.scan(&s2, cols)
		assert.NotError(t, err)

		check.Equal(t, row1[0], "first")
		check.Equal(t, row2[0], "second")
	})
}

func TestScan_typedMap(t *testing.T) {
	t.Run("map[string]string", func(t *testing.T) {
		s := mockScanner{values: []any{"alice", "smith"}}
		v, err := scanOnce[map[string]string](&s, []string{"first", "last"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 2)
			check.Equal(t, v["first"], "alice")
			check.Equal(t, v["last"], "smith")
	})
	t.Run("map[string]int", func(t *testing.T) {
		s := mockScanner{values: []any{10, 20}}
		v, err := scanOnce[map[string]int](&s, []string{"a", "b"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 2)
			check.Equal(t, v["a"], 10)
			check.Equal(t, v["b"], 20)
	})
	t.Run("independent maps per row", func(t *testing.T) {
		cc := cursor[map[string]string]{}
		cols := []string{"k"}

		s1 := mockScanner{values: []any{"v1"}}
		row1, err := cc.scan(&s1, cols)
		assert.NotError(t, err)

		s2 := mockScanner{values: []any{"v2"}}
		row2, err := cc.scan(&s2, cols)
		assert.NotError(t, err)

		check.Equal(t, row1["k"], "v1")
		check.Equal(t, row2["k"], "v2")
	})
}

func TestScan_typedKVSlice(t *testing.T) {
	t.Run("[]irt.KV[string,string]", func(t *testing.T) {
		s := mockScanner{values: []any{"alice", "bob"}}
		v, err := scanOnce[[]irt.KV[string, string]](&s, []string{"first", "last"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 2)
			check.Equal(t, v[0].Key, "first")
			check.Equal(t, v[0].Value, "alice")
			check.Equal(t, v[1].Key, "last")
			check.Equal(t, v[1].Value, "bob")
	})
	t.Run("[]irt.KV[string,int]", func(t *testing.T) {
		s := mockScanner{values: []any{1, 2}}
		v, err := scanOnce[[]irt.KV[string, int]](&s, []string{"a", "b"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 2)
			check.Equal(t, v[0].Key, "a")
			check.Equal(t, v[0].Value, 1)
			check.Equal(t, v[1].Key, "b")
			check.Equal(t, v[1].Value, 2)
	})
	t.Run("independent slices per row", func(t *testing.T) {
		cc := cursor[[]irt.KV[string, int]]{}
		cols := []string{"n"}

		s1 := mockScanner{values: []any{100}}
		row1, err := cc.scan(&s1, cols)
		assert.NotError(t, err)

		s2 := mockScanner{values: []any{200}}
		row2, err := cc.scan(&s2, cols)
		assert.NotError(t, err)

		check.Equal(t, row1[0].Value, 100)
		check.Equal(t, row2[0].Value, 200)
	})
}

func TestScan_typedSeq2(t *testing.T) {
	t.Run("iter.Seq2[string,string]", func(t *testing.T) {
		s := mockScanner{values: []any{"alice", "bob"}}
		v, err := scanOnce[iter.Seq2[string, string]](&s, []string{"first", "last"})
		assert.NotError(t, err)
		m := irt.Collect2(v)
		check.Equal(t, len(m), 2)
			check.Equal(t, m["first"], "alice")
			check.Equal(t, m["last"], "bob")
	})
	t.Run("iter.Seq2[string,int]", func(t *testing.T) {
		s := mockScanner{values: []any{10, 20}}
		v, err := scanOnce[iter.Seq2[string, int]](&s, []string{"x", "y"})
		assert.NotError(t, err)
		m := irt.Collect2(v)
		check.Equal(t, len(m), 2)
			check.Equal(t, m["x"], 10)
			check.Equal(t, m["y"], 20)
	})
	t.Run("early break stops iteration", func(t *testing.T) {
		// Exercises the `break` in mappingTypedSeq2 when the consumer returns false.
		s := mockScanner{values: []any{"a", "b", "c"}}
		v, err := scanOnce[iter.Seq2[string, string]](&s, []string{"x", "y", "z"})
		assert.NotError(t, err)
		count := 0
		v(func(_, _ string) bool {
			count++
			return false // stop after first pair
		})
		check.Equal(t, count, 1)
	})
	t.Run("iterators from different rows are independent", func(t *testing.T) {
		cc := cursor[iter.Seq2[string, string]]{}
		cols := []string{"k"}

		s1 := mockScanner{values: []any{"v1"}}
		seq1, err := cc.scan(&s1, cols)
		assert.NotError(t, err)

		s2 := mockScanner{values: []any{"v2"}}
		seq2, err := cc.scan(&s2, cols)
		assert.NotError(t, err)

		// consume both after both rows scanned
		m1 := irt.Collect2(seq1)
		m2 := irt.Collect2(seq2)
		check.Equal(t, m1["k"], "v1")
		check.Equal(t, m2["k"], "v2")
	})
}

// ---- plan reuse tests -------------------------------------------------------

func TestScan_planReuse(t *testing.T) {
	t.Run("plan cached after first scan", func(t *testing.T) {
		type row struct{ A int `sql:"a"` }
		cc := cursor[row]{}
		check.Equal(t, cc.plan == nil, true)

		s := mockScanner{values: []any{1}}
		_, err := cc.scan(&s, []string{"a"})
		assert.NotError(t, err)
		check.Equal(t, cc.plan != nil, true)
	})

	t.Run("same plan used for all rows", func(t *testing.T) {
		type row struct {
			A int    `sql:"a"`
			B string `sql:"b"`
		}
		cc := cursor[row]{}
		cols := []string{"a", "b"}

		s1 := mockScanner{values: []any{1, "first"}}
		r1, err := cc.scan(&s1, cols)
		assert.NotError(t, err)
		plan1 := cc.plan

		s2 := mockScanner{values: []any{2, "second"}}
		r2, err := cc.scan(&s2, cols)
		assert.NotError(t, err)

		// pointer equality: same plan object
		check.Equal(t, cc.plan == plan1, true)

		check.Equal(t, r1.A, 1)
		check.Equal(t, r1.B, "first")
		check.Equal(t, r2.A, 2)
		check.Equal(t, r2.B, "second")
	})
}

// ---- struct field cache tests -----------------------------------------------

func TestParseStruct_cache(t *testing.T) {
	type myRow struct {
		Name string `sql:"name"`
		Age  int    `sql:"age"`
	}
	typ := reflect.TypeFor[myRow]()

	// Remove from cache to ensure a clean test
	cache.Delete(typ)

	// First call: populates cache
	m1 := parseStruct(typ)
	check.Equal(t, len(m1), 2)

	// Second call: must return from cache (same map pointer)
	m2 := parseStruct(typ)
	// maps are not comparable directly, but cache should hold an entry
	_, inCache := cache.Load(typ)
	check.Equal(t, inCache, true)
	check.Equal(t, len(m2), 2)
}

func TestParseStructFields_noCache(t *testing.T) {
	type myRow struct {
		X int `sql:"x"`
		Y int `sql:"y"`
	}
	typ := reflect.TypeFor[myRow]()
	cache.Delete(typ)

	// parseStructFields never reads/writes cache
	m := parseStructFields(typ)
	check.Equal(t, len(m), 2)

	_, inCache := cache.Load(typ)
	check.Equal(t, inCache, false)
}

func TestCursor_noCache(t *testing.T) {
	type cachedRow struct {
		V int `sql:"v"`
	}
	typ := reflect.TypeFor[cachedRow]()
	cache.Delete(typ)

	// noCache cursor: does NOT populate global cache
	cc := cursor[cachedRow]{noCache: true}
	s := mockScanner{values: []any{1}}
	_, err := cc.scan(&s, []string{"v"})
	assert.NotError(t, err)

	_, inCache := cache.Load(typ)
	check.Equal(t, inCache, false)
}

func TestCursor_withCache(t *testing.T) {
	type cachedRow2 struct {
		V int `sql:"v"`
	}
	typ := reflect.TypeFor[cachedRow2]()
	cache.Delete(typ)

	// default cursor (noCache=false): populates global cache
	cc := cursor[cachedRow2]{}
	s := mockScanner{values: []any{1}}
	_, err := cc.scan(&s, []string{"v"})
	assert.NotError(t, err)

	_, inCache := cache.Load(typ)
	check.Equal(t, inCache, true)
}

// ---- parseStructFields correctness ------------------------------------------

func TestParseStructFields(t *testing.T) {
	t.Run("sql tag wins over db tag", func(t *testing.T) {
		type row struct {
			F int `sql:"sql_col" db:"db_col"`
		}
		m := parseStructFields(reflect.TypeFor[row]())
		_, hasSql := m["sql_col"]
		_, hasDb := m["db_col"]
		check.Equal(t, hasSql, true)
		check.Equal(t, hasDb, false)
	})

	t.Run("embedded fields included", func(t *testing.T) {
		type inner struct {
			B int `sql:"b"`
		}
		type outer struct {
			inner
			A int `sql:"a"`
		}
		m := parseStructFields(reflect.TypeFor[outer]())
		_, hasA := m["a"]
		_, hasB := m["b"]
		check.Equal(t, hasA, true)
		check.Equal(t, hasB, true)
	})

	t.Run("unexported fields excluded", func(t *testing.T) {
		type row struct {
			Pub int `sql:"pub"`
			pri int //nolint
		}
		m := parseStructFields(reflect.TypeFor[row]())
		check.Equal(t, len(m), 1)
		_, ok := m["pub"]
		check.Equal(t, ok, true)
	})

	t.Run("empty sql tag excluded", func(t *testing.T) {
		type row struct {
			A int `sql:"a"`
			B int `sql:""`
		}
		m := parseStructFields(reflect.TypeFor[row]())
		check.Equal(t, len(m), 1)
	})
}

// ---- pointer / indirect type tests ------------------------------------------

func TestScan_pointerToScalar(t *testing.T) {
	col := []string{"v"}
	t.Run("*int", func(t *testing.T) {
		s := mockScanner{values: []any{42}}
		v, err := scanOnce[*int](&s, col)
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, *v, 42)
	})
	t.Run("*string", func(t *testing.T) {
		s := mockScanner{values: []any{"hello"}}
		v, err := scanOnce[*string](&s, col)
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, *v, "hello")
	})
	t.Run("*bool", func(t *testing.T) {
		s := mockScanner{values: []any{true}}
		v, err := scanOnce[*bool](&s, col)
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, *v, true)
	})
	t.Run("*int64", func(t *testing.T) {
		s := mockScanner{values: []any{int64(99)}}
		v, err := scanOnce[*int64](&s, col)
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, *v, int64(99))
	})
	t.Run("*time.Time", func(t *testing.T) {
		now := time.Now().Truncate(time.Second)
		s := mockScanner{values: []any{now}}
		v, err := scanOnce[*time.Time](&s, col)
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, *v, now)
	})
	t.Run("**int", func(t *testing.T) {
		s := mockScanner{values: []any{7}}
		v, err := scanOnce[**int](&s, col)
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, *v != nil, true)
				check.Equal(t, **v, 7)
	})
	t.Run("rows are independent", func(t *testing.T) {
		cc := cursor[*int]{}
		s1 := mockScanner{values: []any{1}}
		r1, err := cc.scan(&s1, col)
		assert.NotError(t, err)
		s2 := mockScanner{values: []any{2}}
		r2, err := cc.scan(&s2, col)
		assert.NotError(t, err)
		check.Equal(t, *r1, 1)
		check.Equal(t, *r2, 2)
	})
}

func TestScan_pointerToStruct(t *testing.T) {
	type row struct {
		A int    `sql:"a"`
		B string `sql:"b"`
	}
	t.Run("basic", func(t *testing.T) {
		s := mockScanner{values: []any{5, "five"}}
		v, err := scanOnce[*row](&s, []string{"a", "b"})
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, v.A, 5)
			check.Equal(t, v.B, "five")
	})
	t.Run("rows are independent", func(t *testing.T) {
		cc := cursor[*row]{}
		cols := []string{"a", "b"}
		s1 := mockScanner{values: []any{1, "one"}}
		r1, err := cc.scan(&s1, cols)
		assert.NotError(t, err)
		s2 := mockScanner{values: []any{2, "two"}}
		r2, err := cc.scan(&s2, cols)
		assert.NotError(t, err)
		check.Equal(t, r1.A, 1)
		check.Equal(t, r2.A, 2)
	})
	t.Run("embedded struct promoted fields", func(t *testing.T) {
		type inner struct{ C bool `sql:"c"` }
		type outer struct {
			inner
			A int `sql:"a"`
		}
		s := mockScanner{values: []any{3, true}}
		v, err := scanOnce[*outer](&s, []string{"a", "c"})
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, v.A, 3)
			check.Equal(t, v.C, true)
	})
}

func TestScan_pointerToSlice(t *testing.T) {
	t.Run("*[]string", func(t *testing.T) {
		s := mockScanner{values: []any{"x", "y"}}
		v, err := scanOnce[*[]string](&s, []string{"a", "b"})
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, len(*v), 2)
			check.Equal(t, (*v)[0], "x")
			check.Equal(t, (*v)[1], "y")
	})
	t.Run("*[]any", func(t *testing.T) {
		s := mockScanner{values: []any{1, "two"}}
		v, err := scanOnce[*[]any](&s, []string{"a", "b"})
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, len(*v), 2)
	})
	t.Run("rows are independent", func(t *testing.T) {
		cc := cursor[*[]string]{}
		cols := []string{"v"}
		s1 := mockScanner{values: []any{"first"}}
		r1, err := cc.scan(&s1, cols)
		assert.NotError(t, err)
		s2 := mockScanner{values: []any{"second"}}
		r2, err := cc.scan(&s2, cols)
		assert.NotError(t, err)
		check.Equal(t, (*r1)[0], "first")
		check.Equal(t, (*r2)[0], "second")
	})
}

func TestScan_pointerToMap(t *testing.T) {
	t.Run("*map[string]string", func(t *testing.T) {
		s := mockScanner{values: []any{"alice", "bob"}}
		v, err := scanOnce[*map[string]string](&s, []string{"first", "last"})
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, (*v)["first"], "alice")
			check.Equal(t, (*v)["last"], "bob")
	})
	t.Run("*map[string]any", func(t *testing.T) {
		s := mockScanner{values: []any{42, "world"}}
		v, err := scanOnce[*map[string]any](&s, []string{"id", "name"})
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, (*v)["id"], 42)
	})
}

// ---- Scanner-implementing type tests ----------------------------------------

func TestScan_scannerInterface(t *testing.T) {
	t.Run("sql.Null[string] single column", func(t *testing.T) {
		s := mockScanner{values: []any{"hello"}}
		v, err := scanOnce[sql.Null[string]](&s, []string{"v"})
		assert.NotError(t, err)
		check.Equal(t, v, sql.Null[string]{V: "hello", Valid: true})
	})
	t.Run("sql.Null[string] multiple columns → errNonStructT", func(t *testing.T) {
		_, err := scanOnce[sql.Null[string]](nil, []string{"a", "b"})
		check.ErrorIs(t, err, errNonStructT)
	})
	t.Run("sql.Null[int64] single column", func(t *testing.T) {
		s := mockScanner{values: []any{int64(7)}}
		v, err := scanOnce[sql.Null[int64]](&s, []string{"v"})
		assert.NotError(t, err)
		check.Equal(t, v, sql.Null[int64]{V: 7, Valid: true})
	})
	t.Run("sql.Null[int64] multiple columns → errNonStructT", func(t *testing.T) {
		_, err := scanOnce[sql.Null[int64]](nil, []string{"a", "b"})
		check.ErrorIs(t, err, errNonStructT)
	})
	t.Run("struct with Scanner-implementing field", func(t *testing.T) {
		// Struct fields that implement Scanner are scanned correctly because
		// *FieldType implements Scanner and is passed directly to sql.Rows.Scan.
		type row struct {
			ID   int              `sql:"id"`
			Name sql.Null[string] `sql:"name"`
		}
		s := mockScanner{values: []any{1, "alice"}}
		v, err := scanOnce[row](&s, []string{"id", "name"})
		assert.NotError(t, err)
		check.Equal(t, v.ID, 1)
		check.Equal(t, v.Name, sql.Null[string]{V: "alice", Valid: true})
	})
	t.Run("*sql.Null[string] single column", func(t *testing.T) {
		// Pointer to a Scanner type: value is allocated and Scanner called on it.
		s := mockScanner{values: []any{"world"}}
		v, err := scanOnce[*sql.Null[string]](&s, []string{"v"})
		assert.NotError(t, err)
		check.Equal(t, v != nil, true)
			check.Equal(t, *v, sql.Null[string]{V: "world", Valid: true})
	})
}
