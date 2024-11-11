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

// MergeHow represents the type of merge operation
type MergeHow string

const (
	LeftMerge  MergeHow = "left"
	RightMerge MergeHow = "right"
	InnerMerge MergeHow = "inner"
	FullMerge  MergeHow = "full"
)

// Merge combines two DataFrames based on a specified column and merge type.
//
// Parameters:
//
// other: The DataFrame to merge with the current DataFrame.
//
// on: The name of the column to merge on. This column must exist in both DataFrames.
//
// how: The type of merge to perform. It can be one of the following:
//   - LeftMerge: Keep all rows from the left DataFrame and match rows from the right DataFrame.
//   - RightMerge: Keep all rows from the right DataFrame and match rows from the left DataFrame.
//   - InnerMerge: Keep only rows that have matching values in both DataFrames.
//   - FullMerge: Keep all rows from both DataFrames, filling in missing values with nil.
//
// Returns:
//   - A new DataFrame containing the merged data.
//   - An error if the merge operation fails, such as if the specified column does not exist in one or both DataFrames.
//
// Examples:
//
//	// Create two sample DataFrames
//	df1 := &DataFrame{
//		Columns: []string{"ID", "Name"},
//		Data: [][]any{
//			{1, "Alice"},
//			{2, "Bob"},
//			{3, "Charlie"},
//		},
//	}
//
//	df2 := &DataFrame{
//		Columns: []string{"ID", "Age"},
//		Data: [][]any{
//			{1, 25},
//			{2, 30},
//			{4, 35},
//		},
//	}
//
//	// Inner merge example (only matching IDs)
//	result, err := df1.Merge(df2, "ID", InnerMerge)
//	// Result:
//	// ID | Name    | Age
//	// 1  | Alice   | 25
//	// 2  | Bob     | 30
//
//	// Left merge example (all rows from df1)
//	result, err := df1.Merge(df2, "ID", LeftMerge)
//	// Result:
//	// ID | Name    | Age
//	// 1  | Alice   | 25
//	// 2  | Bob     | 30
//	// 3  | Charlie | nil
//
//	// Full merge example (all rows from both)
//	result, err := df1.Merge(df2, "ID", FullMerge)
//	// Result:
//	// ID | Name    | Age
//	// 1  | Alice   | 25
//	// 2  | Bob     | 30
//	// 3  | Charlie | nil
//	// 4  | nil     | 35
func (df *DataFrame) Merge(other *DataFrame, on string, how MergeHow) (*DataFrame, error) {
	if df == nil || other == nil {
		return nil, errors.New("both DataFrames must be non-nil")
	}

	// Validate 'on' column exists in both DataFrames
	df1ColIdx := -1
	df2ColIdx := -1
	for i, col := range df.Columns {
		if col == on {
			df1ColIdx = i
			break
		}
	}
	for i, col := range other.Columns {
		if col == on {
			df2ColIdx = i
			break
		}
	}
	if df1ColIdx == -1 || df2ColIdx == -1 {
		return nil, fmt.Errorf("column '%s' not found in both DataFrames", on)
	}

	// Create maps for faster lookups
	df2Map := make(map[any][]int)
	for i, row := range other.Data {
		key := row[df2ColIdx]
		df2Map[key] = append(df2Map[key], i)
	}

	// Prepare result columns
	resultColumns := make([]string, 0)
	resultColumns = append(resultColumns, df.Columns...)
	for _, col := range other.Columns {
		if col != on {
			resultColumns = append(resultColumns, col)
		}
	}

	// Prepare result data based on merge type
	var resultData [][]any
	switch how {
	case InnerMerge:
		resultData = performInnerMerge(df, other, df1ColIdx, df2ColIdx, df2Map)
	case LeftMerge:
		resultData = performLeftMerge(df, other, df1ColIdx, df2ColIdx, df2Map)
	case RightMerge:
		resultData = performRightMerge(df, other, df1ColIdx, df2ColIdx, df2Map)
	case FullMerge:
		resultData = performFullMerge(df, other, df1ColIdx, df2ColIdx, df2Map)
	default:
		return nil, fmt.Errorf("invalid merge type: %s", how)
	}

	return &DataFrame{
		Columns: resultColumns,
		Data:    resultData,
	}, nil
}

func performInnerMerge(df1, df2 *DataFrame, df1ColIdx, df2ColIdx int, df2Map map[any][]int) [][]any {
	var result [][]any
	for _, row1 := range df1.Data {
		key := row1[df1ColIdx]
		if matches, ok := df2Map[key]; ok {
			for _, matchIdx := range matches {
				row2 := df2.Data[matchIdx]
				newRow := make([]any, 0)
				newRow = append(newRow, row1...)
				for j, val := range row2 {
					if j != df2ColIdx {
						newRow = append(newRow, val)
					}
				}
				result = append(result, newRow)
			}
		}
	}
	return result
}

func performLeftMerge(df1, df2 *DataFrame, df1ColIdx, df2ColIdx int, df2Map map[any][]int) [][]any {
	var result [][]any
	nullRow := make([]any, len(df2.Columns)-1)

	for _, row1 := range df1.Data {
		key := row1[df1ColIdx]
		if matches, ok := df2Map[key]; ok {
			for _, matchIdx := range matches {
				row2 := df2.Data[matchIdx]
				newRow := make([]any, 0)
				newRow = append(newRow, row1...)
				for j, val := range row2 {
					if j != df2ColIdx {
						newRow = append(newRow, val)
					}
				}
				result = append(result, newRow)
			}
		} else {
			newRow := make([]any, 0)
			newRow = append(newRow, row1...)
			newRow = append(newRow, nullRow...)
			result = append(result, newRow)
		}
	}
	return result
}

func performRightMerge(df1, df2 *DataFrame, df1ColIdx, df2ColIdx int, _ map[any][]int) [][]any {
	// Create reverse mapping for df1
	df1Map := make(map[any][]int)
	for i, row := range df1.Data {
		key := row[df1ColIdx]
		df1Map[key] = append(df1Map[key], i)
	}

	var result [][]any
	nullRow := make([]any, len(df1.Columns))

	for _, row2 := range df2.Data {
		key := row2[df2ColIdx]
		if matches, ok := df1Map[key]; ok {
			for _, matchIdx := range matches {
				row1 := df1.Data[matchIdx]
				newRow := make([]any, 0)
				newRow = append(newRow, row1...)
				for j, val := range row2 {
					if j != df2ColIdx {
						newRow = append(newRow, val)
					}
				}
				result = append(result, newRow)
			}
		} else {
			newRow := make([]any, 0)
			newRow = append(newRow, nullRow...)
			for j, val := range row2 {
				if j != df2ColIdx {
					newRow = append(newRow, val)
				}
			}
			result = append(result, newRow)
		}
	}
	return result
}

func performFullMerge(df1, df2 *DataFrame, df1ColIdx, df2ColIdx int, df2Map map[any][]int) [][]any {
	// Get all rows from left merge
	result := performLeftMerge(df1, df2, df1ColIdx, df2ColIdx, df2Map)

	// Create set of keys already processed
	processedKeys := make(map[any]bool)
	for _, row := range df1.Data {
		processedKeys[row[df1ColIdx]] = true
	}

	// Add remaining rows from right DataFrame
	nullRow := make([]any, len(df1.Columns))
	for _, row2 := range df2.Data {
		key := row2[df2ColIdx]
		if !processedKeys[key] {
			newRow := make([]any, 0)
			newRow = append(newRow, nullRow...)
			for j, val := range row2 {
				if j != df2ColIdx {
					newRow = append(newRow, val)
				}
			}
			result = append(result, newRow)
		}
	}
	return result
}
