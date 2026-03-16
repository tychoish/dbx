package dbx

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"reflect"
)

type cursor[T any] struct{}

func (cursor[T]) zero() (z T) { return z }

func (c cursor[T]) findMany(ctx context.Context, q Queryer, query string, args []any) iter.Seq2[T, error] {
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

func (c cursor[T]) findOne(ctx context.Context, q Queryer, query string, args []any) (T, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return c.zero(), err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return c.zero(), err
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return c.zero(), err
		}
		return c.zero(), sql.ErrNoRows
	}

	t, err := c.scan(rows, columns)
	if err != nil {
		return c.zero(), err
	}
	if err := rows.Err(); err != nil {
		return c.zero(), err
	}

	return t, nil
}

func (cursor[T]) resolveReflection(t *T) (reflect.Value, reflect.Kind) {
	value := reflect.ValueOf(t).Elem()
	kind := value.Kind()
	return value, kind
}

func (c cursor[T]) scan(s scanner, columns []string) (zero T, err error) {
	if len(columns) == 0 {
		return zero, errNoColumns
	}

	var t T
	var args []any
	var postScan func()

	if v, k := c.resolveReflection(&t); isScannable(v, k) && len(columns) == 1 {
		args = []any{v.Addr().Interface()}
	} else if typ := v.Type(); isCached(typ) {
		args, err = resolveMapping(v, columns, getCachedMapping(typ))
	} else if typ.Implements(reflectTypeScanner) && len(columns) == 1 {
		args = []any{v.Interface()}
	} else if k == reflect.Struct {
		args, err = resolveMapping(v, columns, parseStruct(typ))
	} else if k == reflect.Slice && isSliceOfAny(typ) {
		vals := make([]any, len(columns))
		args = make([]any, len(columns))
		for i := range vals {
			args[i] = &vals[i]
		}
		v.Set(reflect.ValueOf(vals))
	} else if k == reflect.Map && isStringToAnyMap(typ) {
		vals := make([]any, len(columns))
		args = make([]any, len(columns))
		for i := range vals {
			args[i] = &vals[i]
		}
		postScan = func() {
			target := make(map[string]any, len(columns))
			for idx, name := range columns {
				target[name] = vals[idx]
			}
			v.Set(reflect.ValueOf(target))
		}
	} else if elemType, ok := scannableSliceElem(typ, k); ok {
		ptrs := make([]reflect.Value, len(columns))
		args = make([]any, len(columns))
		for i := range ptrs {
			ptrs[i] = reflect.New(elemType)
			args[i] = ptrs[i].Interface()
		}
		postScan = func() {
			target := reflect.MakeSlice(typ, len(columns), len(columns))
			for i := range ptrs {
				target.Index(i).Set(ptrs[i].Elem())
			}
			v.Set(target)
		}
	} else if elemType, ok := scannableStringMapElem(typ, k); ok {
		ptrs := make([]reflect.Value, len(columns))
		args = make([]any, len(columns))
		for i := range ptrs {
			ptrs[i] = reflect.New(elemType)
			args[i] = ptrs[i].Interface()
		}
		postScan = func() {
			target := reflect.MakeMap(typ)
			for i, name := range columns {
				target.SetMapIndex(reflect.ValueOf(name), ptrs[i].Elem())
			}
			v.Set(target)
		}
	} else if len(columns) > 1 && isScannable(v, k) {
		err = fmt.Errorf("%w %T", errNonStructT, typ)
	} else {
		err = fmt.Errorf("%w %T", errUnsupportedT, typ)
	}
	// TODO implement as a special case of a slice irt.KV[string, any] type from the fun package

	if err != nil {
		return zero, err
	}

	if err := s.Scan(args...); err != nil {
		return zero, err
	}

	if postScan != nil {
		postScan()
	}

	return t, nil
}
