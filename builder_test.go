package dbx_test

import (
	"testing"

	queries "github.com/tychoish/dbx"
	"github.com/tychoish/fun/assert"
)

func TestBuilder(t *testing.T) {
	var qb queries.Builder

	qb.With("SELECT %s FROM tbl WHERE 1=1", "*")
	qb.With(" AND foo = %$", 42)
	qb.With(" AND bar = %$", "test")
	qb.With(" AND baz = %$", false)

	query, args := qb.Build()
	assert.Equal(t, query, "SELECT * FROM tbl WHERE 1=1 AND foo = $1 AND bar = $2 AND baz = $3")
	assert.EqualItems(t, args, []any{42, "test", false})
}

func TestBuilder_dialects(t *testing.T) {
	tests := map[string]struct {
		format string
		query  string
	}{
		"?": {
			format: "SELECT * FROM tbl WHERE foo = %? AND bar = %? AND baz = %?",
			query:  "SELECT * FROM tbl WHERE foo = ? AND bar = ? AND baz = ?",
		},
		"$": {
			format: "SELECT * FROM tbl WHERE foo = %$ AND bar = %$ AND baz = %$",
			query:  "SELECT * FROM tbl WHERE foo = $1 AND bar = $2 AND baz = $3",
		},
		"@": {
			format: "SELECT * FROM tbl WHERE foo = %@ AND bar = %@ AND baz = %@",
			query:  "SELECT * FROM tbl WHERE foo = @p1 AND bar = @p2 AND baz = @p3",
		},
		":": {
			format: "SELECT * FROM tbl WHERE foo = %: AND bar = %: AND baz = %:",
			query:  "SELECT * FROM tbl WHERE foo = :1 AND bar = :2 AND baz = :3",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			query, args := queries.SQL(test.format, 1, 2, 3)
			assert.Equal(t, query, test.query)
			assert.EqualItems(t, args, []any{1, 2, 3})
		})
	}
}

func TestBuilder_sliceArgument(t *testing.T) {
	format := "SELECT * FROM tbl WHERE foo IN (%+$)"
	query, args := queries.SQL(format, []int{1, 2, 3})
	assert.Equal(t, query, "SELECT * FROM tbl WHERE foo IN ($1, $2, $3)")
	assert.EqualItems(t, args, []any{1, 2, 3})
}

func TestBuilder_WithSQL(t *testing.T) {
	var qb queries.Builder
	qb.WithSQL("SELECT * FROM tbl", " ORDER BY id")
	query, args := qb.Build()
	assert.Equal(t, query, "SELECT * FROM tbl ORDER BY id")
	assert.Equal(t, len(args), 0)
}

func TestBuilder_WithParams(t *testing.T) {
	var qb queries.Builder
	qb.WithSQL("SELECT * FROM tbl WHERE foo = %$ AND bar = %$")
	qb.WithParams(42, "test")
	query, args := qb.Build()
	assert.Equal(t, query, "SELECT * FROM tbl WHERE foo = $1 AND bar = $2")
	assert.EqualItems(t, args, []any{42, "test"})
}

func TestBuilder_When(t *testing.T) {
	t.Run("true: sql and args appended", func(t *testing.T) {
		var qb queries.Builder
		qb.WithSQL("SELECT * FROM tbl WHERE 1=1")
		qb.When(true, " AND foo = %$", 42)
		query, args := qb.Build()
		assert.Equal(t, query, "SELECT * FROM tbl WHERE 1=1 AND foo = $1")
		assert.EqualItems(t, args, []any{42})
	})
	t.Run("false: nothing appended", func(t *testing.T) {
		var qb queries.Builder
		qb.WithSQL("SELECT * FROM tbl WHERE 1=1")
		qb.When(false, " AND foo = %$", 42)
		query, args := qb.Build()
		assert.Equal(t, query, "SELECT * FROM tbl WHERE 1=1")
		assert.Equal(t, len(args), 0)
	})
	t.Run("chaining: true then false", func(t *testing.T) {
		var qb queries.Builder
		qb.WithSQL("SELECT * FROM tbl WHERE 1=1")
		qb.When(true, " AND a = %$", 1).When(false, " AND b = %$", 2).When(true, " AND c = %$", 3)
		query, args := qb.Build()
		assert.Equal(t, query, "SELECT * FROM tbl WHERE 1=1 AND a = $1 AND c = $2")
		assert.EqualItems(t, args, []any{1, 3})
	})
	t.Run("true with multiple args", func(t *testing.T) {
		var qb queries.Builder
		qb.WithSQL("SELECT * FROM tbl WHERE 1=1")
		qb.When(true, " AND baz = %$ AND qux = %$", "x", "y")
		query, args := qb.Build()
		assert.Equal(t, query, "SELECT * FROM tbl WHERE 1=1 AND baz = $1 AND qux = $2")
		assert.EqualItems(t, args, []any{"x", "y"})
	})
	t.Run("returns builder for chaining", func(t *testing.T) {
		var qb queries.Builder
		result := qb.When(true, "SELECT 1")
		assert.Equal(t, result == &qb, true)
	})
}

func TestBuilder_WhenSQL(t *testing.T) {
	t.Run("true: statements appended", func(t *testing.T) {
		var qb queries.Builder
		qb.WithSQL("SELECT * FROM tbl")
		qb.WhenSQL(true, " ORDER BY id", " DESC")
		query, args := qb.Build()
		assert.Equal(t, query, "SELECT * FROM tbl ORDER BY id DESC")
		assert.Equal(t, len(args), 0)
	})
	t.Run("false: nothing appended", func(t *testing.T) {
		var qb queries.Builder
		qb.WithSQL("SELECT * FROM tbl")
		qb.WhenSQL(false, " ORDER BY id")
		query, args := qb.Build()
		assert.Equal(t, query, "SELECT * FROM tbl")
		assert.Equal(t, len(args), 0)
	})
	t.Run("chaining: mixed conditions", func(t *testing.T) {
		var qb queries.Builder
		qb.WithSQL("SELECT * FROM tbl WHERE 1=1")
		qb.WhenSQL(true, " AND active = true").WhenSQL(false, " AND deleted = false").WhenSQL(true, " LIMIT 10")
		query, _ := qb.Build()
		assert.Equal(t, query, "SELECT * FROM tbl WHERE 1=1 AND active = true LIMIT 10")
	})
	t.Run("does not affect args", func(t *testing.T) {
		var qb queries.Builder
		qb.WithSQL("SELECT * FROM tbl WHERE foo = %$")
		qb.WithParams(99)
		qb.WhenSQL(true, " ORDER BY id")
		_, args := qb.Build()
		assert.EqualItems(t, args, []any{99})
	})
	t.Run("returns builder for chaining", func(t *testing.T) {
		var qb queries.Builder
		result := qb.WhenSQL(true, "SELECT 1")
		assert.Equal(t, result == &qb, true)
	})
}

func TestBuilder_WhenParams(t *testing.T) {
	t.Run("true: params appended", func(t *testing.T) {
		var qb queries.Builder
		qb.WithSQL("SELECT * FROM tbl WHERE foo = %$ AND bar = %$")
		qb.WhenParams(true, 1, 2)
		query, args := qb.Build()
		assert.Equal(t, query, "SELECT * FROM tbl WHERE foo = $1 AND bar = $2")
		assert.EqualItems(t, args, []any{1, 2})
	})
	t.Run("false: params not appended", func(t *testing.T) {
		var qb queries.Builder
		qb.WithSQL("SELECT * FROM tbl WHERE foo = %$")
		qb.WithParams(1)
		qb.WhenParams(false, 99)
		_, args := qb.Build()
		assert.EqualItems(t, args, []any{1})
	})
	t.Run("chaining: conditional params compose with unconditional", func(t *testing.T) {
		var qb queries.Builder
		qb.WithSQL("SELECT * FROM tbl WHERE a = %$ AND b = %$ AND c = %$")
		qb.WithParams(1)
		qb.WhenParams(false, 2)
		qb.WithParams(3)
		_, args := qb.Build()
		assert.EqualItems(t, args, []any{1, 3})
	})
	t.Run("does not affect sql", func(t *testing.T) {
		var qb queries.Builder
		qb.With("SELECT * FROM tbl WHERE a = %$ AND b = %$")
		qb.WhenParams(true, 1, 2)
		query, _ := qb.Build()
		assert.Equal(t, query, "SELECT * FROM tbl WHERE a = $1 AND b = $2")
	})
	t.Run("returns builder for chaining", func(t *testing.T) {
		var qb queries.Builder
		result := qb.WhenParams(true, 1)
		assert.Equal(t, result == &qb, true)
	})
}

func TestBuilder_badQuery(t *testing.T) {
	tests := map[string]struct {
		format string
		args   []any
		query  string
	}{
		"wrong verb": {
			format: "SELECT %d FROM tbl",
			args:   []any{"foo"},
			query:  "SELECT %!d(string=foo) FROM tbl",
		},
		"too few arguments": {
			format: "SELECT %s FROM tbl",
			args:   []any{},
			query:  "SELECT %!s(MISSING) FROM tbl",
		},
		"too many arguments": {
			format: "SELECT %s FROM tbl",
			args:   []any{"foo", "bar"},
			query:  "SELECT foo FROM tbl%!(EXTRA dbx.formatter=bar)",
		},
		"unexpected placeholder": {
			format: "SELECT * FROM tbl WHERE foo = %? AND bar = %$",
			args:   []any{1, 2},
			query:  "SELECT * FROM tbl WHERE foo = ? AND bar = %!$(PANIC=Format method: unexpected placeholder)",
		},
		"non-slice argument": {
			format: "SELECT * FROM tbl WHERE foo IN (%+$)",
			args:   []any{1},
			query:  "SELECT * FROM tbl WHERE foo IN (%!$(PANIC=Format method: non-slice argument))",
		},
		"zero-length slice argument": {
			format: "SELECT * FROM tbl WHERE foo IN (%+$)",
			args:   []any{[]int{}},
			query:  "SELECT * FROM tbl WHERE foo IN (%!$(PANIC=Format method: zero-length slice argument))",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			query, _ := queries.SQL(test.format, test.args...)
			assert.Equal(t, query, test.query)
		})
	}
}
