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
