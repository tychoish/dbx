package dbx

import (
	"context"
	"database/sql"
	"iter"
)

type cursor[T any] struct {
	plan *scanPlan[T]
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

func (c *cursor[T]) scan(s scanner, columns []string) (zero T, err error) {
	if len(columns) == 0 {
		return zero, errNoColumns
	}

	var t T

	if c.plan == nil {
		c.plan, err buildPlan[T](columns)
		if err != nil {
			return zero, err
		}
	}

	rp, err := c.plan.resolve(&t)
	if err != nil {
		return zero, err
	}

	if err := s.Scan(rp.args...); err != nil {
		return zero, err
	}

	if rp.postScan != nil {
		rp.postScan()
	}

	return t, nil
}
