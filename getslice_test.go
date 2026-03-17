package dbx

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/tychoish/fun/assert"
)

func ptr[T any](v T) *T { return &v }

func collectGetSlice(arg any) ([]any, bool) {
	seq := getSlice(arg)
	if seq == nil {
		return nil, false
	}
	return slices.Collect(seq), true
}

func TestGetSlice(t *testing.T) {
	now := time.Now()
	byt1 := []byte("hello")
	byt2 := []byte("world")

	t.Run("SupportedTypes", func(t *testing.T) {
		// Pre-create pointers so input and expected reference the same values.
		pStr1, pStr2 := ptr("a"), ptr("b")
		pBool := ptr(true)
		pInt1, pInt2 := ptr(1), ptr(2)
		pInt8 := ptr(int8(1))
		pInt16 := ptr(int16(1))
		pInt32 := ptr(int32(1))
		pInt64 := ptr(int64(1))
		pUint := ptr(uint(1))
		pUint8 := ptr(uint8(1))
		pUint16 := ptr(uint16(1))
		pUint32 := ptr(uint32(1))
		pUint64 := ptr(uint64(1))
		pF32 := ptr(float32(1.5))
		pF64 := ptr(1.5)
		pTime := ptr(now)
		pByt := ptr(byt1)
		pAny1 := ptr[any](1)

		tests := []struct {
			name string
			arg  any
			want []any
		}{
			{"[]any", []any{1, "two", true}, []any{1, "two", true}},
			{"[]string", []string{"a", "b"}, []any{"a", "b"}},
			{"[]bool", []bool{true, false}, []any{true, false}},
			{"[]int", []int{1, 2, 3}, []any{1, 2, 3}},
			{"[]int8", []int8{1, 2}, []any{int8(1), int8(2)}},
			{"[]int16", []int16{1, 2}, []any{int16(1), int16(2)}},
			{"[]int32", []int32{1, 2}, []any{int32(1), int32(2)}},
			{"[]int64", []int64{1, 2}, []any{int64(1), int64(2)}},
			{"[]uint", []uint{1, 2}, []any{uint(1), uint(2)}},
			{"[]uint8", []uint8{1, 2}, []any{uint8(1), uint8(2)}},
			{"[]uint16", []uint16{1, 2}, []any{uint16(1), uint16(2)}},
			{"[]uint32", []uint32{1, 2}, []any{uint32(1), uint32(2)}},
			{"[]uint64", []uint64{1, 2}, []any{uint64(1), uint64(2)}},
			{"[]float32", []float32{1.5, 2.5}, []any{float32(1.5), float32(2.5)}},
			{"[]float64", []float64{1.5, 2.5}, []any{1.5, 2.5}},
			{"[]time.Time", []time.Time{now}, []any{now}},
			// pointer variants
			{"[]*any", []*any{pAny1}, []any{pAny1}},
			{"[]*string", []*string{pStr1, pStr2}, []any{pStr1, pStr2}},
			{"[]*bool", []*bool{pBool}, []any{pBool}},
			{"[]*int", []*int{pInt1, pInt2}, []any{pInt1, pInt2}},
			{"[]*int8", []*int8{pInt8}, []any{pInt8}},
			{"[]*int16", []*int16{pInt16}, []any{pInt16}},
			{"[]*int32", []*int32{pInt32}, []any{pInt32}},
			{"[]*int64", []*int64{pInt64}, []any{pInt64}},
			{"[]*uint", []*uint{pUint}, []any{pUint}},
			{"[]*uint8", []*uint8{pUint8}, []any{pUint8}},
			{"[]*uint16", []*uint16{pUint16}, []any{pUint16}},
			{"[]*uint32", []*uint32{pUint32}, []any{pUint32}},
			{"[]*uint64", []*uint64{pUint64}, []any{pUint64}},
			{"[]*float32", []*float32{pF32}, []any{pF32}},
			{"[]*float64", []*float64{pF64}, []any{pF64}},
			{"[]*time.Time", []*time.Time{pTime}, []any{pTime}},
			{"[]*[]byte", []*[]byte{pByt}, []any{pByt}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, ok := collectGetSlice(tt.arg)
				assert.True(t, ok)
				assert.EqualItems(t, got, tt.want)
			})
		}

		// [][]byte handled separately: []byte is not comparable via EqualItems.
		t.Run("[][]byte", func(t *testing.T) {
			got, ok := collectGetSlice([][]byte{byt1, byt2})
			assert.True(t, ok)
			assert.Equal(t, len(got), 2)
			assert.True(t, reflect.DeepEqual(got[0], byt1))
			assert.True(t, reflect.DeepEqual(got[1], byt2))
		})
	})

	t.Run("EmptySlices", func(t *testing.T) {
		empties := []any{
			[]any{},
			[]string{},
			[]int{},
			[]int64{},
			[]float64{},
			[]time.Time{},
			[]*string{},
			[][]byte{},
		}
		for _, arg := range empties {
			got, ok := collectGetSlice(arg)
			assert.True(t, ok)
			assert.Equal(t, len(got), 0)
		}
	})

	t.Run("UnsupportedTypes", func(t *testing.T) {
		unsupported := []any{
			"hello",
			42,
			3.14,
			true,
			struct{ x int }{1},
			now,
			map[string]int{"a": 1},
			nil,
		}
		for _, arg := range unsupported {
			_, ok := collectGetSlice(arg)
			assert.True(t, !ok)
		}
	})
}
