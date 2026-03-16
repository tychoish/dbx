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

	if v, k := c.resolveReflection(&t); isScannable(v, k) {
		if len(columns) > 1 {
			return zero, errNonStructT
		}
		args = []any{v.Addr().Interface()}
	} else if t := v.Type(); isCached(t) {
		args, err = resolveMapping(v, columns, getCachedMapping(t))
	} else if v.Type().Implements(reflectTypeScanner) {
		// TODO create implementation; may need to change scanner to make this possible
		panic("not implemented (yet)")
	} else if k == reflect.Struct {
		args, err = resolveMapping(v, columns, parseStruct(t))
	} else if k == reflect.Slice && isSliceOfAny(t) {
		args = make([]any, len(columns))
		v.Addr().Set(reflect.ValueOf(args))
	} else if k == reflect.Map && isStringToAnyMap(t) {
		// TODO determine if it's possible to support other kinds of maps (with any kind of
		//      theoretically scannable value, not just the trivally scanable ones)

		// TODO write tests to ensure that this handle interior pointers correctly.
		args = make([]any, len(columns))
		target := make(map[string]any, len(columns))
		for idx, name := range columns {
			target[name] = args[idx]
		}
		args = make([]any, len(columns))
		v.Addr().Set(reflect.ValueOf(target))
	} else {
		err = fmt.Errorf("%w %T", errUnsupportedT, t)
	}

	if err != nil {
		return zero, err
	}

	if err := s.Scan(args...); err != nil {
		return zero, err
	}

	return t, nil
}
