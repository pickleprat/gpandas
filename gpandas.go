package gpandas

import (
	"errors"
	"gpandas/dataframe"
)

// FloatColumn represents a slice of float64 values.
type FloatColumn []float64

// StringColumn represents a slice of string values.
type StringColumn []string

// IntColumn represents a slice of int64 values.
type IntColumn []int64

// BoolColumn represents a slice of bool values.
type BoolColumn []bool

// Column represents a slice of any type.
type Column []any

// TypeColumn represents a slice of a comparable type T.
type TypeColumn[T comparable] []T

func (gpandas) DataFrame(columns []string, data map[string][]any, columns_types map[string]any) (*dataframe.DataFrame, error) {
	if columns_types == nil {
		return nil, errors.New("provide columns_type map to assert types to the columns")
	}

	if columns != nil && data == nil {

	} else if columns == nil && data != nil {

	}
	cols, err := dataframe.GetMapKeys(data)
	if err != nil {
		return nil, err
	}

	df := &dataframe.DataFrame{data: data}
	return df, nil
}

type gpandas struct{}
