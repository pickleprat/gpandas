package gpandas

import (
	"encoding/csv"
	"errors"
	"fmt"
	"gpandas/dataframe"
	"os"
	"runtime"
	"sync"
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

func FloatColumn(col []any) ([]float64, error) {
	floatCol := make(FloatCol, len(col))
	for i, v := range col {
		if val, ok := v.(float64); ok {
			floatCol[i] = val
		} else {
			return nil, fmt.Errorf("invalid type for column %d: expected float64, got %T", i, v)
		}
	}
	return floatCol, nil
}

// DataFrame creates a new DataFrame from the provided columns, data, and column types.
//
// It validates the input parameters to ensure data consistency and proper type definitions.
//
// The function performs several validation checks:
// - Ensures column_types map is provided
// - Verifies at least one column name is present
// - Checks that data is not empty
// - Confirms the number of columns matches the data columns
// - Validates all columns have the same length
// - Ensures type definitions exist for all columns
//
// The data is then converted to the internal DataFrame format, performing type assertions
// based on the specified column types (FloatCol, IntCol, StringCol, BoolCol).
//
// Parameters:
//
//	columns: A slice of strings representing column names
//	data: A slice of Columns containing the actual data
//	columns_types: A map defining the expected type for each column
//
// Returns:
//
//	A pointer to a DataFrame containing the processed data, or an error if validation fails
func (GoPandas) DataFrame(columns []string, data []Column, columns_types map[string]any) (*dataframe.DataFrame, error) {
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
			case FloatCol:
				if v, ok := val.(float64); ok {
					df.Data[i][j] = FloatCol{v}
				} else {
					return nil, fmt.Errorf("type mismatch for column %s: expected FloatColumn, got %T", columns[i], val)
				}
			case IntCol:
				if v, ok := val.(int64); ok {
					df.Data[i][j] = IntCol{v}
				} else {
					return nil, fmt.Errorf("type mismatch for column %s: expected IntColumn, got %T", columns[i], val)
				}
			case StringCol:
				if v, ok := val.(string); ok {
					df.Data[i][j] = StringCol{v}
				} else {
					return nil, fmt.Errorf("type mismatch for column %s: expected StringColumn, got %T", columns[i], val)
				}
			case BoolCol:
				if v, ok := val.(bool); ok {
					df.Data[i][j] = BoolCol{v}
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

// Read_csv reads a CSV file from the specified filepath and converts it into a DataFrame.
//
// It opens the CSV file, reads the header to determine the column names, and then reads all the records.
//
// The function checks for errors during file operations and ensures that the CSV file is not empty.
//
// It initializes data columns based on the number of headers and populates them with the corresponding values from the records.
//
// If the number of columns in any row is inconsistent with the header, an error is returned.
//
// The function also creates a map of column types, defaulting to StringCol for all columns.
//
// Finally, it calls the DataFrame constructor to create and return a DataFrame containing the data from the CSV file.
//
// Parameters:
//
//	filepath: A string representing the path to the CSV file to be read.
//
// Returns:
//
//	A pointer to a DataFrame containing the data from the CSV file, or an error if the operation fails.
func (GoPandas) Read_csv(filepath string) (*dataframe.DataFrame, error) {
	// Open the CSV file
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)

	// Read header
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading headers: %w", err)
	}

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading records: %w", err)
	}

	if len(records) == 0 {
		return nil, errors.New("CSV file is empty")
	}

	// Initialize data columns
	columnCount := len(headers)
	data := make([]Column, columnCount)
	for i := range data {
		data[i] = make(Column, len(records)) // Preallocate memory for each column
	}

	// Use a WaitGroup to synchronize goroutines
	var wg sync.WaitGroup
	chunkSize := len(records) / runtime.NumCPU() // Determine chunk size based on available CPUs

	// Populate data columns in parallel
	for i := 0; i < len(records); i += chunkSize {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			end := start + chunkSize
			if end > len(records) {
				end = len(records)
			}
			for j := start; j < end; j++ {
				row := records[j]
				if len(row) != columnCount {
					// Handle inconsistent column count
					return
				}
				for k, val := range row {
					data[k][j] = val // Direct assignment
				}
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Create columns_types map (default to string type)
	columns_types := make(map[string]any, columnCount) // Preallocate map size
	for _, header := range headers {
		columns_types[header] = StringCol{}
	}

	// Create DataFrame using existing DataFrame function
	return GoPandas{}.DataFrame(headers, data, columns_types)
}
