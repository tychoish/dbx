package dbx

import (
	"database/sql"
	"iter"
	"reflect"
	"time"

	"github.com/tychoish/fun/adt"
	"github.com/tychoish/fun/ers"
	"github.com/tychoish/fun/irt"
)

func scan[T any](s scanner, columns []string) (T, error) {
	var cc cursor[T]
	return cc.scan(s, columns)
}

type scanner interface {
	Scan(...any) error
}

const (
	errNoColumns     ers.Error = "queries: no columns in the query"
	errNonStructT    ers.Error = "queries: T must be a struct if len(columns) > 1"
	errNoStructField ers.Error = "queries: no struct field for the column"
	errUnsupportedT  ers.Error = "queries: unsupported T"
)

var (
	reflectTypeTime    = reflect.TypeFor[time.Time]()
	reflectTypeScanner = reflect.TypeFor[sql.Scanner]()
	reflectTypeString  = reflect.TypeFor[string]()
)

var cache = &adt.SyncMap[reflect.Type, map[string][]int]{}

// rowPlan carries the per-row scan targets and an optional post-scan step.
// resolve returns nil on error; postScan may be nil when no post-processing is needed.
type rowPlan struct {
	args     []any
	postScan func()
}

// scanPlan holds the resolved scan strategy for a given cursor[T].
// It is computed once on the first scan() call — including the columns for that
// query — and reused for all subsequent rows, since every row has the same columns.
type scanPlan[T any] struct {
	resolve func(t *T) *rowPlan
}

// wrapReflectMapping adapts a reflect.Value-based mapping function to the *T
// signature required by scanPlan[T].
func wrapReflectMapping[T any](inner func(reflect.Value) *rowPlan) func(*T) *rowPlan {
	return func(t *T) *rowPlan {
		return inner(reflect.ValueOf(t).Elem())
	}
}

func mappingDirect[T any](t *T) *rowPlan {
	return &rowPlan{args: []any{t}}
}

// mappingSliceOfAny allocates fresh scan targets per row; the result slice shares
// the backing array so it cannot be pre-allocated without aliasing the caller's value.
func mappingSliceOfAny(columns []string) func(reflect.Value) *rowPlan {
	return func(v reflect.Value) *rowPlan {
		vals := make([]any, len(columns))
		args := make([]any, len(columns))
		for i := range vals {
			args[i] = &vals[i]
		}
		v.Set(reflect.ValueOf(vals))
		return &rowPlan{args: args}
	}
}

// mappingMapStringAny pre-allocates scan targets; vals are copied into a fresh map
// in postScan so reuse across rows is safe.
func mappingMapStringAny(columns []string) func(reflect.Value) *rowPlan {
	vals := make([]any, len(columns))
	args := make([]any, len(columns))
	for i := range vals {
		args[i] = &vals[i]
	}
	return func(v reflect.Value) *rowPlan {
		return &rowPlan{args: args, postScan: func() {
			target := make(map[string]any, len(columns))
			for i, name := range columns {
				target[name] = vals[i]
			}
			v.Set(reflect.ValueOf(target))
		}}
	}
}

// mappingKVStringAny pre-allocates scan targets; vals are copied into a fresh slice
// in postScan so reuse across rows is safe.
func mappingKVStringAny(columns []string) func(reflect.Value) *rowPlan {
	vals := make([]any, len(columns))
	args := make([]any, len(columns))
	for i := range vals {
		args[i] = &vals[i]
	}
	return func(v reflect.Value) *rowPlan {
		return &rowPlan{args: args, postScan: func() {
			target := make([]irt.KV[string, any], len(columns))
			for i, name := range columns {
				target[i] = irt.MakeKV(name, vals[i])
			}
			v.Set(reflect.ValueOf(target))
		}}
	}
}

// mappingSeq2StringAny allocates fresh scan targets per row because the returned
// iterator is lazy and may be consumed after the next row is scanned.
func mappingSeq2StringAny(columns []string) func(reflect.Value) *rowPlan {
	return func(v reflect.Value) *rowPlan {
		vals := make([]any, len(columns))
		args := make([]any, len(columns))
		for i := range vals {
			args[i] = &vals[i]
		}
		return &rowPlan{args: args, postScan: func() {
			v.Set(reflect.ValueOf(irt.Zip(irt.Slice(columns), irt.Slice(vals))))
		}}
	}
}

// mappingTypedSlice pre-allocates typed scan pointers; values are copied into a
// fresh slice in postScan so reuse across rows is safe.
func mappingTypedSlice(typ reflect.Type, columns []string) func(reflect.Value) *rowPlan {
	elemType := typ.Elem()
	ptrs := make([]reflect.Value, len(columns))
	args := make([]any, len(columns))
	for i := range ptrs {
		ptrs[i] = reflect.New(elemType)
		args[i] = ptrs[i].Interface()
	}
	return func(v reflect.Value) *rowPlan {
		return &rowPlan{args: args, postScan: func() {
			target := reflect.MakeSlice(typ, len(ptrs), len(ptrs))
			for i := range ptrs {
				target.Index(i).Set(ptrs[i].Elem())
			}
			v.Set(target)
		}}
	}
}

// mappingTypedStringMap pre-allocates typed scan pointers; values are copied into a
// fresh map in postScan so reuse across rows is safe.
func mappingTypedStringMap(typ reflect.Type, columns []string) func(reflect.Value) *rowPlan {
	elemType := typ.Elem()
	ptrs := make([]reflect.Value, len(columns))
	args := make([]any, len(columns))
	for i := range ptrs {
		ptrs[i] = reflect.New(elemType)
		args[i] = ptrs[i].Interface()
	}
	return func(v reflect.Value) *rowPlan {
		return &rowPlan{args: args, postScan: func() {
			target := reflect.MakeMap(typ)
			for i, name := range columns {
				target.SetMapIndex(reflect.ValueOf(name), ptrs[i].Elem())
			}
			v.Set(target)
		}}
	}
}

// mappingTypedKVSlice pre-allocates typed scan pointers; values are copied into a
// fresh KV slice in postScan so reuse across rows is safe.
// The value type V is derived from the KV[string, V].Value field of typ's element.
func mappingTypedKVSlice(typ reflect.Type, columns []string) func(reflect.Value) *rowPlan {
	valType := typ.Elem().Field(1).Type // irt.KV[K,V].Value
	ptrs := make([]reflect.Value, len(columns))
	args := make([]any, len(columns))
	for i := range ptrs {
		ptrs[i] = reflect.New(valType)
		args[i] = ptrs[i].Interface()
	}
	return func(v reflect.Value) *rowPlan {
		return &rowPlan{args: args, postScan: func() {
			target := reflect.MakeSlice(typ, len(columns), len(columns))
			for i, name := range columns {
				kv := target.Index(i)
				kv.Field(0).Set(reflect.ValueOf(name))
				kv.Field(1).Set(ptrs[i].Elem())
			}
			v.Set(target)
		}}
	}
}

// mappingTypedSeq2 allocates fresh typed scan pointers per row because the returned
// iterator is lazy and captures the pointers directly.
// The value type V is derived from the yield function's second parameter.
func mappingTypedSeq2(typ reflect.Type, columns []string) func(reflect.Value) *rowPlan {
	valType := typ.In(0).In(1) // Seq2[K,V] = func(func(K,V) bool); yield.In(1) = V
	return func(v reflect.Value) *rowPlan {
		ptrs := make([]reflect.Value, len(columns))
		args := make([]any, len(columns))
		for i := range ptrs {
			ptrs[i] = reflect.New(valType)
			args[i] = ptrs[i].Interface()
		}
		return &rowPlan{args: args, postScan: func() {
			vals := make([]reflect.Value, len(ptrs))
			for i := range ptrs {
				vals[i] = ptrs[i].Elem()
			}
			v.Set(reflect.MakeFunc(typ, func(fnArgs []reflect.Value) []reflect.Value {
				yield := fnArgs[0]
				for i, name := range columns {
					if !yield.Call([]reflect.Value{reflect.ValueOf(name), vals[i]})[0].Bool() {
						break
					}
				}
				return nil
			}))
		}}
	}
}

var (
	reflectTypeSliceOfAny       = reflect.TypeFor[[]any]()
	reflectTypeMapStringAny     = reflect.TypeFor[map[string]any]()
	reflectTypeKVStringAnySlice = reflect.TypeFor[[]irt.KV[string, any]]()
	reflectTypeSeq2StringAny    = reflect.TypeFor[iter.Seq2[string, any]]()
)

func isSliceOfAny(t reflect.Type) bool       { return t == reflectTypeSliceOfAny }
func isStringToAnyMap(t reflect.Type) bool   { return t == reflectTypeMapStringAny }
func isKVStringAnySlice(t reflect.Type) bool { return t == reflectTypeKVStringAnySlice }
func isSeq2StringAny(t reflect.Type) bool    { return t == reflectTypeSeq2StringAny }

func isScannableType(t reflect.Type, k reflect.Kind) bool {
	return (k >= reflect.Bool && k <= reflect.Float64) ||
		k == reflect.String ||
		t == reflectTypeTime ||
		reflect.PointerTo(t).Implements(reflectTypeScanner)
}

// scannableStringMapElem reports whether t is a map[string]V where V is directly scannable.
func scannableStringMapElem(t reflect.Type, k reflect.Kind) bool {
	if k != reflect.Map || t.Key() != reflectTypeString {
		return false
	}
	elem := t.Elem()
	return isScannableType(elem, elem.Kind())
}

// scannableSliceElem reports whether t is []V where V is directly scannable.
func scannableSliceElem(t reflect.Type, k reflect.Kind) bool {
	if k != reflect.Slice {
		return false
	}
	elem := t.Elem()
	return isScannableType(elem, elem.Kind())
}

// scannableKVStringSliceElem reports whether t is []irt.KV[string, V] where V is
// directly scannable (non-interface).
// The []irt.KV[string, any] case is handled separately by isKVStringAnySlice.
func scannableKVStringSliceElem(t reflect.Type, k reflect.Kind) bool {
	if k != reflect.Slice {
		return false
	}
	elem := t.Elem()
	if elem.Kind() != reflect.Struct || elem.NumField() != 2 {
		return false
	}
	keyField := elem.Field(0)
	valField := elem.Field(1)
	if keyField.Name != "Key" || keyField.Type != reflectTypeString {
		return false
	}
	if valField.Name != "Value" {
		return false
	}
	valType := valField.Type
	return isScannableType(valType, valType.Kind())
}

// seq2StringScannableValueType reports whether t is iter.Seq2[string, V] where V is
// directly scannable (non-interface).
// The iter.Seq2[string, any] case is handled separately by isSeq2StringAny.
func seq2StringScannableValueType(t reflect.Type, k reflect.Kind) bool {
	if k != reflect.Func || t.NumIn() != 1 || t.NumOut() != 0 {
		return false
	}
	yield := t.In(0)
	if yield.Kind() != reflect.Func || yield.NumIn() != 2 || yield.NumOut() != 1 {
		return false
	}
	if yield.In(0) != reflectTypeString || yield.Out(0).Kind() != reflect.Bool {
		return false
	}
	valType := yield.In(1)
	return isScannableType(valType, valType.Kind())
}

// parseStructFields parses the struct type t and returns a map of column tag names to
// field index paths. Embedded struct fields are included via reflect.VisibleFields.
func parseStructFields(t reflect.Type) map[string][]int {
	fields := reflect.VisibleFields(t)
	indexes := make(map[string][]int, len(fields))
	for _, field := range fields {
		if !field.IsExported() {
			continue
		}
		if column, ok := field.Tag.Lookup("sql"); ok && column != "" {
			indexes[column] = field.Index
		} else if column, ok = field.Tag.Lookup("db"); ok && column != "" {
			indexes[column] = field.Index
		}
	}
	return indexes
}

// parseStruct is like parseStructFields but caches results in the global cache.
func parseStruct(t reflect.Type) map[string][]int {
	if m, ok := cache.Load(t); ok {
		return m
	}
	m := parseStructFields(t)
	cache.Store(t, m)
	return m
}
