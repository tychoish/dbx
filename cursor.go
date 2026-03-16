package dbx

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"reflect"
)

type cursor[T any] struct {
	plan    *scanPlan[T]
	noCache bool // when true, bypasses the global struct field-map cache
}

func (c *cursor[T]) findMany(ctx context.Context, q Queryer, query string, args []any) iter.Seq2[T, error] {
	var zero T
	return func(yield func(T, error) bool) {
		rows, err := q.QueryContext(ctx, query, args...)
		if err != nil {
			yield(zero, err)
			return
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			yield(zero, err)
			return
		}

		for rows.Next() {
			t, err := c.scan(rows, columns)
			if err != nil {
				yield(zero, err)
				return
			}
			if !yield(t, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(zero, err)
			return
		}
	}
}

func (c *cursor[T]) findOne(ctx context.Context, q Queryer, query string, args []any) (T, error) {
	var zero T
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return zero, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return zero, err
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return zero, err
		}
		return zero, sql.ErrNoRows
	}

	t, err := c.scan(rows, columns)
	if err != nil {
		return zero, err
	}
	if err := rows.Err(); err != nil {
		return zero, err
	}

	return t, nil
}

func (c *cursor[T]) scan(s scanner, columns []string) (zero T, _ error) {
	if len(columns) == 0 {
		return zero, errNoColumns
	}

	var t T

	if c.plan == nil {
		p, err := c.buildPlan(columns)
		if err != nil {
			return zero, err
		}
		c.plan = p
	}

	rp := c.plan.resolve(&t)

	if err := s.Scan(rp.args...); err != nil {
		return zero, err
	}

	if rp.postScan != nil {
		rp.postScan()
	}

	return t, nil
}

// buildPlan constructs the scan plan for T. For directly-scannable scalars (bool, int
// kinds, float kinds, string, time.Time, sql.Scanner implementors) it uses the
// allocation-free mappingDirect; for all other types it delegates to buildReflectMapping.
func (c cursor[T]) buildPlan(columns []string) (*scanPlan[T], error) {
	typ := reflect.TypeFor[T]()
	k := typ.Kind()

	// mappingDirect avoids reflect overhead for the common scalar case.
	if isScannableType(typ, k) {
		if len(columns) != 1 {
			var zero T
			return nil, fmt.Errorf("%w %T", errNonStructT, zero)
		}
		return &scanPlan[T]{resolve: mappingDirect[T]}, nil
	}

	inner, err := c.buildReflectMapping(typ, k, columns)
	if err != nil {
		return nil, err
	}
	return &scanPlan[T]{resolve: wrapReflectMapping[T](inner)}, nil
}

// buildReflectMapping returns a reflect.Value-based mapping function for typ.
// It handles structs, all collection types, and pointer types (recursively).
func (c cursor[T]) buildReflectMapping(typ reflect.Type, k reflect.Kind, columns []string) (func(reflect.Value) *rowPlan, error) {
	switch {
	case isScannableType(typ, k):
		// Reached when peeling a pointer reveals a scalar (e.g. *int, *time.Time).
		if len(columns) != 1 {
			return nil, fmt.Errorf("%w %s", errNonStructT, typ)
		}
		return func(v reflect.Value) *rowPlan {
			return &rowPlan{args: []any{v.Addr().Interface()}}
		}, nil
	case k == reflect.Struct:
		return c.buildStructMapping(typ, columns)
	case k == reflect.Slice && isSliceOfAny(typ):
		return mappingSliceOfAny(columns), nil
	case k == reflect.Map && isStringToAnyMap(typ):
		return mappingMapStringAny(columns), nil
	case k == reflect.Slice && isKVStringAnySlice(typ):
		return mappingKVStringAny(columns), nil
	case k == reflect.Func && isSeq2StringAny(typ):
		return mappingSeq2StringAny(columns), nil
	case scannableSliceElem(typ, k):
		return mappingTypedSlice(typ, columns), nil
	case scannableStringMapElem(typ, k):
		return mappingTypedStringMap(typ, columns), nil
	case scannableKVStringSliceElem(typ, k):
		return mappingTypedKVSlice(typ, columns), nil
	case seq2StringScannableValueType(typ, k):
		return mappingTypedSeq2(typ, columns), nil
	case k == reflect.Pointer:
		return c.buildPtrMapping(typ, columns)
	}
	return nil, fmt.Errorf("%w %s", errUnsupportedT, typ)
}

// buildPtrMapping peels one level of pointer and wraps the inner mapping so that
// a fresh element is allocated per row and the pointer field is set after scanning.
// Supports arbitrary pointer depth: *int, **int, *struct{}, *[]T, etc.
func (c cursor[T]) buildPtrMapping(typ reflect.Type, columns []string) (func(reflect.Value) *rowPlan, error) {
	elem := typ.Elem()
	inner, err := c.buildReflectMapping(elem, elem.Kind(), columns)
	if err != nil {
		return nil, err
	}
	return func(v reflect.Value) *rowPlan {
		newElem := reflect.New(elem).Elem() // fresh allocation per row
		rp := inner(newElem)
		orig := rp.postScan
		rp.postScan = func() {
			if orig != nil {
				orig()
			}
			v.Set(newElem.Addr()) // set the pointer field to the newly allocated value
		}
		return rp
	}, nil
}

// buildStructMapping resolves column→field-index paths for a struct type and returns
// a reflect.Value-based row-mapping function. Embedded fields are supported via
// multi-element index paths from reflect.VisibleFields.
// When c.noCache is true the global struct field-map cache is bypassed.
func (c cursor[T]) buildStructMapping(typ reflect.Type, columns []string) (func(reflect.Value) *rowPlan, error) {
	var m map[string][]int
	if c.noCache {
		m = parseStructFields(typ)
	} else {
		m = parseStruct(typ)
	}
	indices := make([][]int, len(columns))
	for i, col := range columns {
		idx, ok := m[col]
		if !ok {
			return nil, fmt.Errorf("%w %q", errNoStructField, col)
		}
		indices[i] = idx
	}
	args := make([]any, len(indices))
	return func(v reflect.Value) *rowPlan {
		for i, idx := range indices {
			args[i] = v.FieldByIndex(idx).Addr().Interface()
		}
		return &rowPlan{args: args}
	}, nil
}
