package dbx

import (
	"database/sql"
	"fmt"
	"iter"
	"reflect"
	"time"

	"github.com/tychoish/fun/adt"
	"github.com/tychoish/fun/ers"
	"github.com/tychoish/fun/irt"
)

type scanner interface {
	Scan(...any) error
}

const (
	errNoColumns     ers.Error = "queries: no columns in the query"
	errNonStructT    ers.Error = "queries: T must be a struct if len(columns) > 1"
	errNoStructField ers.Error = "queries: no struct field for the column"
	errUnsupportedT  ers.Error = "queries: unsupported T"
)

func scan[T any](s scanner, columns []string) (T, error) {
	var cc cursor[T]
	return cc.scan(s, columns)
}

var (
	reflectTypeTime    = reflect.TypeFor[time.Time]()
	reflectTypeScanner = reflect.TypeFor[sql.Scanner]()
	reflectTypeString  = reflect.TypeFor[string]()
)

var (
	useCache = true
	cache    = &adt.SyncMap[reflect.Type, map[string][]int]{}
)

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
	resolve func(t *T) (*rowPlan, error)
}

// wrapReflectMapping adapts a reflect.Value-based mapping function to the *T
// signature required by scanPlan[T].
func wrapReflectMapping[T any](inner func(reflect.Value) (*rowPlan, error)) func(*T) (*rowPlan, error) {
	return func(t *T) (*rowPlan, error) {
		return inner(reflect.ValueOf(t).Elem())
	}
}

func buildPlan[T any](columns []string) (*scanPlan[T], error) {
	typ := reflect.TypeFor[T]()
	k := typ.Kind()

	switch {
	case isScannableType(typ, k):
		if len(columns) != 1 {
			var zero T
			return nil, fmt.Errorf("%w %T", errNonStructT, zero)
		}
		return &scanPlan[T]{resolve: mappingDirect[T]}, nil
	case k == reflect.Struct:
		return buildStructPlan[T](typ, columns)
	case typ.Implements(reflectTypeScanner):
		if len(columns) != 1 {
			return nil, fmt.Errorf("%w %T", errNonStructT, *new(T))
		}
		return &scanPlan[T]{resolve: mappingImplements[T]}, nil
	case k == reflect.Slice && isSliceOfAny(typ):
		return &scanPlan[T]{resolve: wrapReflectMapping[T](mappingSliceOfAny(columns))}, nil
	case k == reflect.Map && isStringToAnyMap(typ):
		return &scanPlan[T]{resolve: wrapReflectMapping[T](mappingMapStringAny(columns))}, nil
	case k == reflect.Slice && isKVStringAnySlice(typ):
		return &scanPlan[T]{resolve: wrapReflectMapping[T](mappingKVStringAny(columns))}, nil
	case k == reflect.Func && isSeq2StringAny(typ):
		return &scanPlan[T]{resolve: wrapReflectMapping[T](mappingSeq2StringAny(columns))}, nil
	}

	if elemType, ok := scannableSliceElem(typ, k); ok {
		return &scanPlan[T]{resolve: wrapReflectMapping[T](mappingTypedSlice(typ, elemType, columns))}, nil
	}
	if elemType, ok := scannableStringMapElem(typ, k); ok {
		return &scanPlan[T]{resolve: wrapReflectMapping[T](mappingTypedStringMap(typ, elemType, columns))}, nil
	}
	if valType, ok := scannableKVStringSliceElem(typ, k); ok {
		return &scanPlan[T]{resolve: wrapReflectMapping[T](mappingTypedKVSlice(typ, valType, columns))}, nil
	}
	if valType, ok := seq2StringScannableValueType(typ, k); ok {
		return &scanPlan[T]{resolve: wrapReflectMapping[T](mappingTypedSeq2(typ, valType, columns))}, nil
	}

	return nil, fmt.Errorf("%w %T", errUnsupportedT, typ)
}

// buildStructPlan resolves the column→field-index mapping for struct type typ and
// returns a scanPlan[T] that scans rows directly into struct fields.
// Embedded struct fields are supported via multi-element index paths from reflect.VisibleFields.
func buildStructPlan[T any](typ reflect.Type, columns []string) (*scanPlan[T], error) {
	m := parseStruct(typ)
	indices := make([][]int, len(columns))
	for i, col := range columns {
		idx, ok := m[col]
		if !ok {
			return nil, fmt.Errorf("%w %q", errNoStructField, col)
		}
		indices[i] = idx
	}
	args := make([]any, len(indices))
	return &scanPlan[T]{resolve: wrapReflectMapping[T](func(v reflect.Value) (*rowPlan, error) {
		for i, idx := range indices {
			args[i] = v.FieldByIndex(idx).Addr().Interface()
		}
		return &rowPlan{args: args}, nil
	})}, nil
}

func mappingDirect[T any](t *T) (*rowPlan, error) {
	return &rowPlan{args: []any{t}}, nil
}

func mappingImplements[T any](t *T) (*rowPlan, error) {
	return &rowPlan{args: []any{any(*t)}}, nil
}

// mappingSliceOfAny allocates fresh scan targets per row; the result slice shares
// the backing array so it cannot be pre-allocated without aliasing the caller's value.
func mappingSliceOfAny(columns []string) func(reflect.Value) (*rowPlan, error) {
	return func(v reflect.Value) (*rowPlan, error) {
		vals := make([]any, len(columns))
		args := make([]any, len(columns))
		for i := range vals {
			args[i] = &vals[i]
		}
		v.Set(reflect.ValueOf(vals))
		return &rowPlan{args: args}, nil
	}
}

// mappingMapStringAny pre-allocates scan targets; vals are copied into a fresh map
// in postScan so reuse across rows is safe.
func mappingMapStringAny(columns []string) func(reflect.Value) (*rowPlan, error) {
	vals := make([]any, len(columns))
	args := make([]any, len(columns))
	for i := range vals {
		args[i] = &vals[i]
	}
	return func(v reflect.Value) (*rowPlan, error) {
		return &rowPlan{args: args, postScan: func() {
			target := make(map[string]any, len(columns))
			for i, name := range columns {
				target[name] = vals[i]
			}
			v.Set(reflect.ValueOf(target))
		}}, nil
	}
}

// mappingKVStringAny pre-allocates scan targets; vals are copied into a fresh slice
// in postScan so reuse across rows is safe.
func mappingKVStringAny(columns []string) func(reflect.Value) (*rowPlan, error) {
	vals := make([]any, len(columns))
	args := make([]any, len(columns))
	for i := range vals {
		args[i] = &vals[i]
	}
	return func(v reflect.Value) (*rowPlan, error) {
		return &rowPlan{args: args, postScan: func() {
			target := make([]irt.KV[string, any], len(columns))
			for i, name := range columns {
				target[i] = irt.MakeKV(name, vals[i])
			}
			v.Set(reflect.ValueOf(target))
		}}, nil
	}
}

// mappingSeq2StringAny allocates fresh scan targets per row because the returned
// iterator is lazy and may be consumed after the next row is scanned.
func mappingSeq2StringAny(columns []string) func(reflect.Value) (*rowPlan, error) {
	return func(v reflect.Value) (*rowPlan, error) {
		vals := make([]any, len(columns))
		args := make([]any, len(columns))
		for i := range vals {
			args[i] = &vals[i]
		}
		return &rowPlan{args: args, postScan: func() {
			v.Set(reflect.ValueOf(irt.Zip(irt.Slice(columns), irt.Slice(vals))))
		}}, nil
	}
}

// mappingTypedSlice pre-allocates typed scan pointers; values are copied into a
// fresh slice in postScan so reuse across rows is safe.
func mappingTypedSlice(typ, elemType reflect.Type, columns []string) func(reflect.Value) (*rowPlan, error) {
	ptrs := make([]reflect.Value, len(columns))
	args := make([]any, len(columns))
	for i := range ptrs {
		ptrs[i] = reflect.New(elemType)
		args[i] = ptrs[i].Interface()
	}
	return func(v reflect.Value) (*rowPlan, error) {
		return &rowPlan{args: args, postScan: func() {
			target := reflect.MakeSlice(typ, len(ptrs), len(ptrs))
			for i := range ptrs {
				target.Index(i).Set(ptrs[i].Elem())
			}
			v.Set(target)
		}}, nil
	}
}

// mappingTypedStringMap pre-allocates typed scan pointers; values are copied into a
// fresh map in postScan so reuse across rows is safe.
func mappingTypedStringMap(typ, elemType reflect.Type, columns []string) func(reflect.Value) (*rowPlan, error) {
	ptrs := make([]reflect.Value, len(columns))
	args := make([]any, len(columns))
	for i := range ptrs {
		ptrs[i] = reflect.New(elemType)
		args[i] = ptrs[i].Interface()
	}
	return func(v reflect.Value) (*rowPlan, error) {
		return &rowPlan{args: args, postScan: func() {
			target := reflect.MakeMap(typ)
			for i, name := range columns {
				target.SetMapIndex(reflect.ValueOf(name), ptrs[i].Elem())
			}
			v.Set(target)
		}}, nil
	}
}

// mappingTypedKVSlice pre-allocates typed scan pointers; values are copied into a
// fresh KV slice in postScan so reuse across rows is safe.
func mappingTypedKVSlice(typ, valType reflect.Type, columns []string) func(reflect.Value) (*rowPlan, error) {
	ptrs := make([]reflect.Value, len(columns))
	args := make([]any, len(columns))
	for i := range ptrs {
		ptrs[i] = reflect.New(valType)
		args[i] = ptrs[i].Interface()
	}
	return func(v reflect.Value) (*rowPlan, error) {
		return &rowPlan{args: args, postScan: func() {
			target := reflect.MakeSlice(typ, len(columns), len(columns))
			for i, name := range columns {
				kv := target.Index(i)
				kv.Field(0).Set(reflect.ValueOf(name))
				kv.Field(1).Set(ptrs[i].Elem())
			}
			v.Set(target)
		}}, nil
	}
}

// mappingTypedSeq2 allocates fresh typed scan pointers per row because the returned
// iterator is lazy and captures the pointers directly.
func mappingTypedSeq2(typ, valType reflect.Type, columns []string) func(reflect.Value) (*rowPlan, error) {
	return func(v reflect.Value) (*rowPlan, error) {
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
		}}, nil
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

// scannableStringMapElem checks whether t is a map[string]V where V is directly
// scannable. Returns the element type and true if so.
func scannableStringMapElem(t reflect.Type, k reflect.Kind) (reflect.Type, bool) {
	if k != reflect.Map || t.Key() != reflectTypeString {
		return nil, false
	}
	elem := t.Elem()
	return elem, isScannableType(elem, elem.Kind())
}

func scannableSliceElem(t reflect.Type, k reflect.Kind) (reflect.Type, bool) {
	if k != reflect.Slice {
		return nil, false
	}
	elem := t.Elem()
	return elem, isScannableType(elem, elem.Kind())
}

// scannableKVStringSliceElem checks whether t is []irt.KV[string, V] where V is
// directly scannable (non-interface). Returns the value element type and true if so.
// The []irt.KV[string, any] case is handled separately by isKVStringAnySlice.
func scannableKVStringSliceElem(t reflect.Type, k reflect.Kind) (reflect.Type, bool) {
	if k != reflect.Slice {
		return nil, false
	}
	elem := t.Elem()
	if elem.Kind() != reflect.Struct || elem.NumField() != 2 {
		return nil, false
	}
	keyField := elem.Field(0)
	valField := elem.Field(1)
	if keyField.Name != "Key" || keyField.Type != reflectTypeString {
		return nil, false
	}
	if valField.Name != "Value" {
		return nil, false
	}
	valType := valField.Type
	return valType, isScannableType(valType, valType.Kind())
}

// seq2StringScannableValueType checks whether t is iter.Seq2[string, V] where V is
// directly scannable (non-interface). Returns the value type V and true if so.
// The iter.Seq2[string, any] case is handled separately by isSeq2StringAny.
func seq2StringScannableValueType(t reflect.Type, k reflect.Kind) (reflect.Type, bool) {
	if k != reflect.Func || t.NumIn() != 1 || t.NumOut() != 0 {
		return nil, false
	}
	yield := t.In(0)
	if yield.Kind() != reflect.Func || yield.NumIn() != 2 || yield.NumOut() != 1 {
		return nil, false
	}
	if yield.In(0) != reflectTypeString || yield.Out(0).Kind() != reflect.Bool {
		return nil, false
	}
	valType := yield.In(1)
	return valType, isScannableType(valType, valType.Kind())
}

// parseStruct parses the given struct type and returns a map of column names to field indexes.
// The result is cached, so each struct type is parsed only once.
func parseStruct(t reflect.Type) map[string][]int {
	var indexes map[string][]int

	if useCache {
		if m, ok := cache.Load(t); ok {
			return m
		}
		defer func() { cache.Store(t, indexes) }()
	}

	fields := reflect.VisibleFields(t)
	indexes = make(map[string][]int, len(fields))

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
