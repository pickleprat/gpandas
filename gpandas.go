package gpandas

import (
	"errors"
	"fmt"
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

func (gpandas) DataFrame(columns []string, data []Column, columns_types map[string]any) (*dataframe.DataFrame, error) {
	// Validate inputs
	if columns_types == nil {
		return nil, errors.New("columns_types map is required to assert column types")
	}

	if len(columns) == 0 {
		return nil, errors.New("at least one column name is required")
	}

	if len(data) == 0 {
		return nil, errors.New("data cannot be empty")
	}

	if len(columns) != len(data) {
		return nil, errors.New("number of columns must match number of data columns")
	}

	// Validate all columns have same length
	rowCount := len(data[0])
	for i, col := range data {
		if len(col) != rowCount {
			return nil, fmt.Errorf("inconsistent row count in column %s: expected %d, got %d", columns[i], rowCount, len(col))
		}
	}

	// Validate column types
	for _, colName := range columns {
		if _, exists := columns_types[colName]; !exists {
			return nil, fmt.Errorf("missing type definition for column: %s", colName)
		}
	}

	// Create DataFrame
	df := &dataframe.DataFrame{
		Columns: columns,
		Data:    make([][]any, len(columns)),
	}

	// Convert data to internal format
	for i, col := range data {
		df.Data[i] = make([]any, rowCount)
		for j, val := range col {
			// Type assertion based on columns_types using defined types
			switch columns_types[columns[i]].(type) {
			case FloatColumn:
				if v, ok := val.(float64); ok {
					df.Data[i][j] = FloatColumn{v}
				} else {
					return nil, fmt.Errorf("type mismatch for column %s: expected FloatColumn, got %T", columns[i], val)
				}
			case IntColumn:
				if v, ok := val.(int64); ok {
					df.Data[i][j] = IntColumn{v}
				} else {
					return nil, fmt.Errorf("type mismatch for column %s: expected IntColumn, got %T", columns[i], val)
				}
			case StringColumn:
				if v, ok := val.(string); ok {
					df.Data[i][j] = StringColumn{v}
				} else {
					return nil, fmt.Errorf("type mismatch for column %s: expected StringColumn, got %T", columns[i], val)
				}
			case BoolColumn:
				if v, ok := val.(bool); ok {
					df.Data[i][j] = BoolColumn{v}
				} else {
					return nil, fmt.Errorf("type mismatch for column %s: expected BoolColumn, got %T", columns[i], val)
				}
			default:
				df.Data[i][j] = val // Fallback for any other type
			}
		}
	}

	return df, nil
}

type gpandas struct{}
