package dbx

import (
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"time"
)

type scanner interface {
	Scan(...any) error
}

var (
	errNoColumns     = errors.New("queries: no columns in the query")
	errNonStructT    = errors.New("queries: T must be a struct if len(columns) > 1")
	errNoStructField = errors.New("queries: no struct field for the column")
	errUnsupportedT  = errors.New("queries: unsupported T")
)

func scan[T any](s scanner, columns []string) (T, error) {
	var cc cursor[T]
	return cc.scan(s, columns)
}

func scannable(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return true
	}
	if v.Type() == reflect.TypeFor[time.Time]() {
		return true
	}
	if v.Addr().Type().Implements(reflect.TypeFor[sql.Scanner]()) {
		return true
	}
	return false
}

var (
	useCache = true
	cache    sync.Map // map[reflect.Type]map[string]int
)

// parseStruct parses the given struct type and returns a map of column names to field indexes.
// The result is cached, so each struct type is parsed only once.
func parseStruct(t reflect.Type) map[string][]int {
	if useCache {
		if m, ok := cache.Load(t); ok {
			return m.(map[string][]int)
		}
	}

	fields := reflect.VisibleFields(t)
	indexes := make(map[string][]int, len(fields))

	for _, field := range fields {
		if !field.IsExported() {
			continue
		}
		column, ok := field.Tag.Lookup("sql")
		if !ok || column == "" {
			continue
		}
		indexes[column] = field.Index
	}

	if useCache {
		cache.Store(t, indexes)
	}
	return indexes
}
