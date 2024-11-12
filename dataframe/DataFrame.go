package dataframe

import (
	"bytes"
	"errors"
	"fmt"
	"gpandas/utils/collection"
	"os"
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

// Rename changes the names of specified columns in the DataFrame.
//
// The method allows renaming multiple columns at once by providing a map where:
//   - Keys are the current/original column names
//   - Values are the new column names to replace them with
//
// The operation is thread-safe as it uses mutex locking to prevent concurrent modifications.
//
// Parameters:
//   - columns: map[string]string where keys are original column names and values are new names
//
// Returns:
//   - error: nil if successful, otherwise an error describing what went wrong
//
// Possible errors:
//   - If the columns map is empty
//   - If the DataFrame is nil
//   - If any specified original column name doesn't exist in the DataFrame
//   - If there are any issues with internal set operations
//
// Example:
//
//	df := &DataFrame{
//	    Columns: []string{"A", "B", "C"},
//	    Data:    [][]any{{1, 2, 3}, {4, 5, 6}},
//	}
//
//	// Rename columns "A" to "X" and "B" to "Y"
//	err := df.Rename(map[string]string{
//	    "A": "X",
//	    "B": "Y",
//	})
//
//	// Result:
//	// Columns will be ["X", "Y", "C"]
//
// Thread Safety:
//
// The method uses sync.Mutex to ensure thread-safe operation when modifying column names.
// The lock is automatically released using defer when the function returns.
//
// Note:
//   - All specified original column names must exist in the DataFrame
//   - The operation modifies the DataFrame in place
//   - Column order remains unchanged
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

// String returns a string representation of the DataFrame in a formatted table.
//
// The method creates a visually appealing ASCII table representation of the DataFrame
// with the following features:
//   - Column headers are displayed in the first row
//   - Data is aligned to the left within columns
//   - Table borders and separators use ASCII characters
//   - Each cell's content is automatically converted to string representation
//   - A summary line showing dimensions ([rows x columns]) is appended
//
// The table format follows this pattern:
//
//	+-----+-----+-----+
//	| Col1| Col2| Col3|
//	+-----+-----+-----+
//	| val1| val2| val3|
//	| val4| val5| val6|
//	+-----+-----+-----+
//	[2 rows x 3 columns]
//
// Parameters:
//   - None (receiver method on DataFrame)
//
// Returns:
//   - string: The formatted table representation of the DataFrame
//
// Example:
//
//	df := &DataFrame{
//	    Columns: []string{"A", "B"},
//	    Data:    [][]any{{1, 2}, {3, 4}},
//	}
//	fmt.Println(df.String())
//
// Note:
//   - All values are converted to strings using fmt.Sprintf("%v", val)
//   - The table is rendered using the github.com/olekukonko/tablewriter package
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

// ToCSV converts the DataFrame to a CSV string representation or writes it to a file.
//
// Parameters:
//   - sep: string representing the separator character (defaults to comma if empty)
//   - filepath: optional file path to write the CSV to
//
// Returns:
//   - string: CSV representation of the DataFrame if filepath is empty
//   - error: nil if successful, otherwise an error describing what went wrong
//
// Note: If filepath is provided, the method returns ("", nil) on success
//
// Example:
//
//	// Get CSV as string with comma separator
//	csv, err := df.ToCSV(",", "")
//
//	// Write to file with semicolon separator
//	_, err := df.ToCSV(";", "path/to/output.csv")
//
//	// Write to file with default comma separator
//	_, err := df.ToCSV("", "path/to/output.csv")
func (df *DataFrame) ToCSV(sep string, filepath string) (string, error) {
	if df == nil {
		return "", errors.New("DataFrame is nil")
	}

	// Use comma as default separator if none provided
	if sep == "" {
		sep = ","
	}

	var buf bytes.Buffer

	// Write headers
	for i, col := range df.Columns {
		if i > 0 {
			buf.WriteString(sep)
		}
		buf.WriteString(col)
	}
	buf.WriteString("\n")

	// Write data rows
	for _, row := range df.Data {
		for i, val := range row {
			if i > 0 {
				buf.WriteString(sep)
			}
			buf.WriteString(fmt.Sprintf("%v", val))
		}
		buf.WriteString("\n")
	}

	// If filepath is provided, write to file and return nil
	if filepath != "" {
		err := os.WriteFile(filepath, buf.Bytes(), 0644)
		if err != nil {
			return "", fmt.Errorf("failed to write CSV to file: %w", err)
		}
		return "", nil
	}

	// If no filepath, return the CSV string
	return buf.String(), nil
}
