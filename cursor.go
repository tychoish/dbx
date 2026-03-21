package dbx

import (
	"context"
	"database/sql"
	"errors"
	"iter"
	"reflect"
)

type cursor[T any] struct {
	plan    *scanPlan[T]
	noCache bool // when true, bypasses the global struct field-map cache
}

func (c *cursor[T]) findMany(ctx context.Context, q QueryFunc, query string, args []any) iter.Seq2[T, error] {
	var zero T
	return func(yield func(T, error) bool) {
		rows, err := q(ctx, query, args...)
		if err != nil {
			yield(zero, err)
			return
		}
		defer rows.Close()

		flush2(c.rows(rows), yield)
	}
}

func (c *cursor[T]) rows(rows *sql.Rows) iter.Seq2[T, error] {
	var zero T
	return func(yield func(T, error) bool) {
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

func (c *cursor[T]) findOne(ctx context.Context, q QueryFunc, query string, args []any) (T, error) {
	var zero T
	rows, err := q(ctx, query, args...)
	if err != nil {
		return zero, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return zero, err
	}

	if !rows.Next() {
		return zero, errors.Join(sql.ErrNoRows, rows.Err())
	}

	t, err := c.scan(rows, columns)
	if err != nil {
		return zero, err
	}

	return t, nil
}

func (c *cursor[T]) scan(s scanner, columns []string) (zero T, _ error) {
	if len(columns) == 0 {
		return zero, errNoColumns
	}

	if err := c.initPlan(columns); err != nil {
		return zero, err
	}

	var t T

	rp := c.resolveRowPlan(&t)

	if err := s.Scan(rp.args...); err != nil {
		return zero, err
	}

	if rp.postScan != nil {
		rp.postScan(reflect.ValueOf(&t).Elem())
	}

	return t, nil
}

func (c *cursor[T]) resolveRowPlan(t *T) *rowPlan {
	if c.plan.rp == nil {
		return c.plan.resolve(t)
	}

	if c.plan.rp.build != nil {
		return c.plan.rp.build(reflect.ValueOf(t).Elem())
	}

	return c.plan.rp
}

func (c *cursor[T]) initPlan(cols []string) error {
	if c.plan != nil {
		return nil
	}

	var err error
	c.plan, err = c.buildPlan(cols)
	if err != nil {
		return err
	}

	return nil
}

func flush2[A, B any](seq iter.Seq2[A, B], yield func(A, B) bool) {
	for k, v := range seq {
		if !yield(k, v) {
			return
		}
	}
}
