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

func (cursor[T]) scan(s scanner, columns []string) (zero T, _ error) {
	if len(columns) == 0 {
		return zero, errNoColumns
	}

	var t T
	v := reflect.ValueOf(&t).Elem()
	args := make([]any, len(columns))

	switch {
	case scannable(v):
		if len(columns) > 1 {
			return zero, errNonStructT
		}
		args[0] = v.Addr().Interface()
	case v.Kind() == reflect.Struct:
		v.Kind()
		indexes := parseStruct(v.Type())
		for i, column := range columns {
			idx, ok := indexes[column]
			if !ok {
				return zero, fmt.Errorf("%w %q", errNoStructField, column)
			}
			args[i] = v.FieldByIndex(idx).Addr().Interface()
		}
	default:
		return zero, fmt.Errorf("%w %T", errUnsupportedT, t)
	}

	if err := s.Scan(args...); err != nil {
		return zero, err
	}

	return t, nil
}
