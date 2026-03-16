package dbx

import (
	"database/sql"
	"errors"
	"iter"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/tychoish/fun/assert"
	"github.com/tychoish/fun/assert/check"
	"github.com/tychoish/fun/erc"
	"github.com/tychoish/fun/irt"
)

func scan[T any](s scanner, columns []string) (T, error) {
	var cc cursor[T]
	return cc.scan(s, columns)
}

func TestCollect(t *testing.T) {
	anErr := errors.New("an error")

	tests := map[string]struct {
		seq     iter.Seq2[int, error]
		want    []int
		wantErr error
	}{
		"no error": {slices.All([]error{nil, nil}), []int{0, 1}, nil},
		"an error": {slices.All([]error{nil, anErr}), nil, anErr},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := erc.FromIteratorUntil(tt.seq)
			assert.ErrorIs(t, err, tt.wantErr)
			if err == nil {
				assert.EqualItems(t, got, tt.want)
			}
		})
	}
}

func Test_scan(t *testing.T) {
	t.Run("no columns", func(t *testing.T) {
		_, err := scan[int](nil, []string{})
		check.ErrorIs(t, err, errNoColumns)
	})

	t.Run("non-struct T with len(columns) > 1", func(t *testing.T) {
		_, err := scan[int](nil, []string{"foo", "bar"})
		check.ErrorIs(t, err, errNonStructT)
	})

	t.Run("no struct field", func(t *testing.T) {
		_, err := scan[struct{}](nil, []string{"foo", "bar"})
		check.ErrorIs(t, err, errNoStructField)
	})

	t.Run("unsupported T", func(t *testing.T) {
		_, err := scan[complex64](nil, []string{"foo", "bar"})
		check.ErrorIs(t, err, errUnsupportedT)
	})

	t.Run("scan error", func(t *testing.T) {
		s := mockScanner{err: errors.New("an error")}

		type row struct {
			Foo int    `sql:"foo"`
			Bar string `sql:"bar"`
		}
		_, err := scan[row](&s, []string{"foo", "bar"})
		check.ErrorIs(t, err, s.err)
	})

	t.Run("struct T", func(t *testing.T) {
		s := mockScanner{values: []any{1, "test", true}}

		type embedded struct {
			Baz bool `sql:"baz"`
		}
		type row struct {
			embedded
			Foo        int    `sql:"foo"`
			Bar        string `sql:"bar"`
			EmptyTag   string `sql:""`
			Untagged   string
			unexported string
		}
		v, err := scan[row](&s, []string{"foo", "bar", "baz"})
		assert.NotError(t, err)
		check.Equal(t, v.Foo, 1)
		check.Equal(t, v.Bar, "test")
		check.Equal(t, v.Baz, true)
		check.Equal(t, v.EmptyTag, "")
		check.Equal(t, v.Untagged, "")
		check.Equal(t, v.unexported, "")
	})

	t.Run("non-struct T", func(t *testing.T) {
		columns := []string{"foo"}

		tests := []struct {
			scan  func(scanner) (any, error)
			value any
		}{
			{func(s scanner) (any, error) { return scan[bool](s, columns) }, true},
			{func(s scanner) (any, error) { return scan[int](s, columns) }, int(-1)},
			{func(s scanner) (any, error) { return scan[int8](s, columns) }, int8(-8)},
			{func(s scanner) (any, error) { return scan[int16](s, columns) }, int16(-16)},
			{func(s scanner) (any, error) { return scan[int32](s, columns) }, int32(-32)},
			{func(s scanner) (any, error) { return scan[int64](s, columns) }, int64(-64)},
			{func(s scanner) (any, error) { return scan[uint](s, columns) }, uint(1)},
			{func(s scanner) (any, error) { return scan[uint8](s, columns) }, uint8(8)},
			{func(s scanner) (any, error) { return scan[uint16](s, columns) }, uint16(16)},
			{func(s scanner) (any, error) { return scan[uint32](s, columns) }, uint32(32)},
			{func(s scanner) (any, error) { return scan[uint64](s, columns) }, uint64(64)},
			{func(s scanner) (any, error) { return scan[float32](s, columns) }, float32(0.32)},
			{func(s scanner) (any, error) { return scan[float64](s, columns) }, float64(0.64)},
			{func(s scanner) (any, error) { return scan[string](s, columns) }, "test"},
			{func(s scanner) (any, error) { return scan[time.Time](s, columns) }, time.Now()},
		}
		for _, tt := range tests {
			s := mockScanner{values: []any{tt.value}}
			v, err := tt.scan(&s)
			assert.NotError(t, err)
			check.Equal(t, v, tt.value)
		}

		s := mockScanner{values: []any{"test"}}
		v, err := scan[sql.Null[string]](&s, columns)
		assert.NotError(t, err)
		check.Equal(t, v, sql.Null[string]{V: "test", Valid: true})
	})

	t.Run("slice of any", func(t *testing.T) {
		s := mockScanner{values: []any{1, "hello", true}}
		v, err := scan[[]any](&s, []string{"a", "b", "c"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 3)
		check.Equal(t, v[0], 1)
		check.Equal(t, v[1], "hello")
		check.Equal(t, v[2], true)
	})

	t.Run("kv string any slice", func(t *testing.T) {
		s := mockScanner{values: []any{42, "world"}}
		v, err := scan[[]irt.KV[string, any]](&s, []string{"id", "name"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 2)
		check.Equal(t, v[0].Key, "id")
		check.Equal(t, v[0].Value, 42)
		check.Equal(t, v[1].Key, "name")
		check.Equal(t, v[1].Value, "world")
	})

	t.Run("kv string string slice", func(t *testing.T) {
		s := mockScanner{values: []any{"alice", "bob"}}
		v, err := scan[[]irt.KV[string, string]](&s, []string{"first", "last"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 2)
		check.Equal(t, v[0].Key, "first")
		check.Equal(t, v[0].Value, "alice")
		check.Equal(t, v[1].Key, "last")
		check.Equal(t, v[1].Value, "bob")
	})

	t.Run("seq2 string any", func(t *testing.T) {
		s := mockScanner{values: []any{42, "world"}}
		v, err := scan[iter.Seq2[string, any]](&s, []string{"id", "name"})
		assert.NotError(t, err)
		m := irt.Collect2(v)
		check.Equal(t, len(m), 2)
		check.Equal(t, m["id"], 42)
		check.Equal(t, m["name"], "world")
	})

	t.Run("seq2 string string", func(t *testing.T) {
		s := mockScanner{values: []any{"alice", "bob"}}
		v, err := scan[iter.Seq2[string, string]](&s, []string{"first", "last"})
		assert.NotError(t, err)
		m := irt.Collect2(v)
		check.Equal(t, len(m), 2)
		check.Equal(t, m["first"], "alice")
		check.Equal(t, m["last"], "bob")
	})

	t.Run("map string any", func(t *testing.T) {
		s := mockScanner{values: []any{42, "world"}}
		v, err := scan[map[string]any](&s, []string{"id", "name"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 2)
		check.Equal(t, v["id"], 42)
		check.Equal(t, v["name"], "world")
	})

	t.Run("map string string", func(t *testing.T) {
		s := mockScanner{values: []any{"alice", "bob"}}
		v, err := scan[map[string]string](&s, []string{"first", "last"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 2)
		check.Equal(t, v["first"], "alice")
		check.Equal(t, v["last"], "bob")
	})

	t.Run("map string int", func(t *testing.T) {
		s := mockScanner{values: []any{10, 20}}
		v, err := scan[map[string]int](&s, []string{"a", "b"})
		assert.NotError(t, err)
		check.Equal(t, len(v), 2)
		check.Equal(t, v["a"], 10)
		check.Equal(t, v["b"], 20)
	})
}

type mockScanner struct {
	values []any
	err    error
}

// Scan implements [sql.Scanner].
func (s *mockScanner) Scan(dst ...any) error {
	if s.err != nil {
		return s.err
	}
	for i := range dst {
		if sc, ok := dst[i].(sql.Scanner); ok {
			if err := sc.Scan(s.values[i]); err != nil {
				return err
			}
		} else {
			v := reflect.ValueOf(s.values[i])
			reflect.ValueOf(dst[i]).Elem().Set(v)
		}
	}
	return nil
}
