package dbx

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/tychoish/fun/adt"
	"github.com/tychoish/fun/ers"
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
)

func isScannable(v reflect.Value, kind reflect.Kind) bool {
	switch {
	case kind >= reflect.Bool && kind <= reflect.Float64:
		return true
	case kind == reflect.String:
		return true
	case v.Type() == reflectTypeTime:
		return true
	case v.Addr().Type().Implements(reflectTypeScanner):
		return true
	default:
		return false
	}
}

var (
	useCache = true
	cache    = &adt.SyncMap[reflect.Type, map[string][]int]{} // map[reflect.Type]
)

func isCached(t reflect.Type) bool                     { return useCache && cache.Check(t) }
func getCachedMapping(t reflect.Type) map[string][]int { return cache.Get(t) }

func isSliceOfAny(v any) bool     { _, ok := v.([]any); return ok }
func isStringToAnyMap(v any) bool { _, ok := v.(map[string]any); return ok }

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

func resolveMapping(v reflect.Value, columns []string, mapping map[string][]int) ([]any, error) {
	args := make([]any, len(columns))
	for i, column := range columns {
		idx, ok := mapping[column]
		if !ok {
			return nil, fmt.Errorf("%w %q", errNoStructField, column)
		}
		args[i] = v.FieldByIndex(idx).Addr().Interface()
	}

	return args, nil
}
