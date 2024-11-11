package dataframe

import (
	"bytes"
	"errors"
	"fmt"
	"gpandas/utils/collection"
	"sync"

	"github.com/olekukonko/tablewriter"
)

type GoPandas struct{}

// FloatColumn represents a slice of float64 values.
type FloatCol []float64

// StringColumn represents a slice of string values.
type StringCol []string

// IntColumn represents a slice of int64 values.
type IntCol []int64

// BoolColumn represents a slice of bool values.
type BoolCol []bool

// Column represents a slice of any type.
type Column []any

// TypeColumn represents a slice of a comparable type T.
type TypeColumn[T comparable] []T

func GetMapKeys[K comparable, V any](input_map map[K]V) (collection.Set[K], error) {
	keys, err := collection.NewSet[K]()
	if err != nil {
		return nil, err
	}
	for k := range input_map {
		keys.Add(k)
	}
	return keys, nil
}

type DataFrame struct {
	sync.Mutex
	Columns []string
	Data    [][]any
}

func (df *DataFrame) Rename(columns map[string]string) error {
	if len(columns) == 0 {
		return errors.New("'columns' slice is empty. Slice of Maps to declare columns to rename is required")
	}
	if df == nil {
		return errors.New("'df *DataFrame' param is nil. Supply a dataframe to rename columns")
	}

	keys, err := GetMapKeys[string, string](columns)
	if err != nil {
		return err
	}

	// locking df and unlocking if facing error or after finished processing
	df.Lock()
	defer df.Unlock()

	dfcols, err2 := collection.ToSet(df.Columns)
	if err2 != nil {
		return err2
	}

	keys_dfcols_set_intersect, err3 := keys.Intersect(dfcols)
	if err3 != nil {
		return err3
	}

	is_equal_cols, false_val := keys.Compare(keys_dfcols_set_intersect)
	if !is_equal_cols && false_val != nil {
		return errors.New("the column '" + false_val.(string) + "' is not present in DataFrame. Specify correct values as keys in columns map")
	} else if !is_equal_cols && false_val == nil {
		return errors.New("the columns specified in 'columns' parameter is not present in the the DataFrame")
	}

	// all conditions met till this point
	for original_column_name, new_column_name := range columns {
		for df_column_idx := range df.Columns {
			if df.Columns[df_column_idx] == original_column_name {
				df.Columns[df_column_idx] = new_column_name
			}
		}
	}
	return nil
}

func (df *DataFrame) String() string {
	var buf bytes.Buffer
	table := tablewriter.NewWriter(&buf)

	// Set table properties
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("+")
	table.SetColumnSeparator("|")
	table.SetRowSeparator("-")
	table.SetHeaderLine(true)
	table.SetBorder(true)

	// Set headers
	table.SetHeader(df.Columns)

	// Convert data to strings and add to table
	for _, row := range df.Data {
		stringRow := make([]string, len(row))
		for i, val := range row {
			stringRow[i] = fmt.Sprintf("%v", val)
		}
		table.Append(stringRow)
	}

	// Add row count information
	numRows := len(df.Data)
	shape := fmt.Sprintf("[%d rows x %d columns]", numRows, len(df.Columns))

	// Render the table
	table.Render()
	return buf.String() + shape + "\n"
}
