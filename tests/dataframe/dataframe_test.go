package dataframe_test

import (
	"gpandas/dataframe"
	"testing"
)

// Helper function to compare slices
func sliceEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Helper function to compare string slices
func strSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDataFrameRename tests the DataFrame.Rename method which allows renaming columns in a DataFrame.
//
// The test covers several scenarios:
//
// 1. Successful rename:
//   - Tests renaming multiple existing columns ("A" to "X" and "B" to "Y")
//   - Verifies no error is returned when renaming valid columns
//
// 2. Renaming non-existent column:
//   - Attempts to rename column "D" which doesn't exist
//   - Verifies an error is returned for invalid column name
//
// 3. Nil DataFrame:
//   - Tests behavior when DataFrame is nil
//   - Verifies appropriate error handling for nil DataFrame
//
// 4. Empty columns map:
//   - Tests behavior when an empty rename map is provided
//   - Verifies error is returned for empty rename request
//
// Each test case validates:
//   - Error behavior matches expectations (error/no error)
//   - Error conditions are properly handled
//   - Method behaves correctly for valid and invalid inputs
func TestDataFrameRename(t *testing.T) {
	tests := []struct {
		name        string
		df          *dataframe.DataFrame
		columns     map[string]string
		expectError bool
	}{
		{
			name: "successful rename",
			df: &dataframe.DataFrame{
				Columns: []string{"A", "B", "C"},
				Data:    [][]any{{1, 2, 3}, {4, 5, 6}},
			},
			columns:     map[string]string{"A": "X", "B": "Y"},
			expectError: false,
		},
		{
			name: "rename non-existent column",
			df: &dataframe.DataFrame{
				Columns: []string{"A", "B", "C"},
				Data:    [][]any{{1, 2, 3}, {4, 5, 6}},
			},
			columns:     map[string]string{"D": "X"},
			expectError: true,
		},
		{
			name:        "nil dataframe",
			df:          nil,
			columns:     map[string]string{"A": "X"},
			expectError: true,
		},
		{
			name: "empty columns map",
			df: &dataframe.DataFrame{
				Columns: []string{"A", "B", "C"},
				Data:    [][]any{{1, 2, 3}, {4, 5, 6}},
			},
			columns:     map[string]string{},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.df.Rename(test.columns)
			if (err != nil) != test.expectError {
				t.Errorf("expected error: %v, got: %v", test.expectError, err)
			}
		})
	}
}

// TestDataFrameString tests the String() method of the DataFrame struct, which converts
// a DataFrame into a formatted string representation.
//
// The test suite covers three main scenarios:
//
// 1. Basic DataFrame ("basic dataframe"):
//   - Tests a simple numeric DataFrame with:
//   - 3 columns (A, B, C)
//   - 2 rows of integer data
//   - Verifies correct table formatting with headers, borders, and row count
//
// 2. Empty DataFrame ("empty dataframe"):
//   - Tests DataFrame with:
//   - 2 columns (A, B)
//   - No data rows
//   - Verifies proper handling of empty data while maintaining structure
//   - Confirms correct row count display ([0 rows x 2 columns])
//
// 3. Mixed Data Types ("mixed data types"):
//   - Tests DataFrame with different data types:
//   - String column (Name)
//   - Integer column (Age)
//   - Boolean column (Active)
//   - Verifies proper string conversion of different data types
//   - Confirms alignment and spacing with varying content lengths
//
// Test Structure:
//   - Uses table-driven tests for multiple scenarios
//   - Each test case includes:
//   - name: descriptive test case name
//   - df: input DataFrame
//   - expected: exact expected string output
//
// Verification:
//   - Compares exact string output including:
//   - Table borders and separators
//   - Column headers
//   - Data alignment
//   - Row count summary
//   - Uses exact string matching to ensure precise formatting
//
// Example test case:
//
//	{
//	    name: "basic dataframe",
//	    df: &dataframe.DataFrame{
//	        Columns: []string{"A", "B", "C"},
//	        Data:    [][]any{{1, 2, 3}, {4, 5, 6}},
//	    },
//	    expected: `+---+---+---+
//	               | A | B | C |
//	               +---+---+---+
//	               | 1 | 2 | 3 |
//	               | 4 | 5 | 6 |
//	               +---+---+---+
//	               [2 rows x 3 columns]
//	               `,
//	}
func TestDataFrameString(t *testing.T) {
	tests := []struct {
		name     string
		df       *dataframe.DataFrame
		expected string
	}{
		{
			name: "basic dataframe",
			df: &dataframe.DataFrame{
				Columns: []string{"A", "B", "C"},
				Data:    [][]any{{1, 2, 3}, {4, 5, 6}},
			},
			expected: `+---+---+---+
| A | B | C |
+---+---+---+
| 1 | 2 | 3 |
| 4 | 5 | 6 |
+---+---+---+
[2 rows x 3 columns]
`,
		},
		{
			name: "empty dataframe",
			df: &dataframe.DataFrame{
				Columns: []string{"A", "B"},
				Data:    [][]any{},
			},
			expected: `+---+---+
| A | B |
+---+---+
+---+---+
[0 rows x 2 columns]
`,
		},
		{
			name: "mixed data types",
			df: &dataframe.DataFrame{
				Columns: []string{"Name", "Age", "Active"},
				Data:    [][]any{{"John", 30, true}, {"Jane", 25, false}},
			},
			expected: `+------+-----+--------+
| Name | Age | Active |
+------+-----+--------+
| John | 30  | true   |
| Jane | 25  | false  |
+------+-----+--------+
[2 rows x 3 columns]
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.df.String()
			if result != test.expected {
				t.Errorf("expected:\n%s\ngot:\n%s", test.expected, result)
			}
		})
	}
}

// TestDataFrameMerge tests the DataFrame.Merge method which combines two DataFrames
// based on a common column and specified merge strategy.
//
// The test suite covers seven main scenarios:
//
// 1. Inner Merge ("inner merge - basic case"):
//   - Tests basic inner join functionality
//   - Input:
//   - df1: ID-Name pairs (3 rows)
//   - df2: ID-Age pairs (3 rows)
//   - Verifies only matching rows are kept (2 rows)
//   - Checks correct column combination
//
// 2. Left Merge ("left merge - keep all left rows"):
//   - Tests left join functionality
//   - Input:
//   - df1: ID-Name pairs (3 rows)
//   - df2: ID-Age pairs (2 rows)
//   - Verifies all left rows are kept
//   - Confirms nil values for non-matching right rows
//
// 3. Right Merge ("right merge - keep all right rows"):
//   - Tests right join functionality
//   - Input:
//   - df1: ID-Name pairs (2 rows)
//   - df2: ID-Age pairs (3 rows)
//   - Verifies all right rows are kept
//   - Confirms nil values for non-matching left rows
//
// 4. Full Merge ("full merge - keep all rows"):
//
//   - Tests full outer join functionality
//
//   - Input:
//
//   - df1: ID-Name pairs (3 rows)
//
//   - df2: ID-Age pairs (3 rows)
//
//   - Verifies all rows from both DataFrames are kept
//
//   - Confirms nil values for non-matching rows
//
//     5. Error Cases:
//     a. Nil DataFrame ("nil dataframe error"):
//
//   - Tests behavior with nil DataFrame input
//
//   - Verifies appropriate error handling
//
//     b. Missing Column ("column not found error"):
//
//   - Tests behavior when merge column doesn't exist
//
//   - Verifies appropriate error detection
//
//     c. Invalid Merge Type ("invalid merge type error"):
//
//   - Tests behavior with invalid merge strategy
//
//   - Verifies appropriate error handling
//
// Test Structure:
//   - Uses table-driven tests for multiple scenarios
//   - Each test case includes:
//   - name: descriptive test case name
//   - df1: first input DataFrame
//   - df2: second input DataFrame
//   - on: column to merge on
//   - how: merge strategy
//   - expected: expected result DataFrame
//   - expectError: whether an error is expected
//
// Verification Steps:
// 1. Error handling:
//   - Checks if errors occur as expected
//   - Verifies error cases return appropriate errors
//
// 2. Success cases:
//   - Verifies column names match expected output
//   - Checks number of rows matches expected output
//   - Validates each row's data matches expected values
//
// Example test case:
//
//	{
//	    name: "inner merge - basic case",
//	    df1: &dataframe.DataFrame{
//	        Columns: []string{"ID", "Name"},
//	        Data:    [][]any{{1, "Alice"}, {2, "Bob"}, {3, "Charlie"}},
//	    },
//	    df2: &dataframe.DataFrame{
//	        Columns: []string{"ID", "Age"},
//	        Data:    [][]any{{1, 25}, {2, 30}, {4, 35}},
//	    },
//	    on:  "ID",
//	    how: dataframe.InnerMerge,
//	    expected: &dataframe.DataFrame{
//	        Columns: []string{"ID", "Name", "Age"},
//	        Data:    [][]any{{1, "Alice", 25}, {2, "Bob", 30}},
//	    },
//	    expectError: false,
//	}
func TestDataFrameMerge(t *testing.T) {
	tests := []struct {
		name        string
		df1         *dataframe.DataFrame
		df2         *dataframe.DataFrame
		on          string
		how         dataframe.MergeHow
		expected    *dataframe.DataFrame
		expectError bool
	}{
		{
			name: "inner merge - basic case",
			df1: &dataframe.DataFrame{
				Columns: []string{"ID", "Name"},
				Data:    [][]any{{1, "Alice"}, {2, "Bob"}, {3, "Charlie"}},
			},
			df2: &dataframe.DataFrame{
				Columns: []string{"ID", "Age"},
				Data:    [][]any{{1, 25}, {2, 30}, {4, 35}},
			},
			on:  "ID",
			how: dataframe.InnerMerge,
			expected: &dataframe.DataFrame{
				Columns: []string{"ID", "Name", "Age"},
				Data:    [][]any{{1, "Alice", 25}, {2, "Bob", 30}},
			},
			expectError: false,
		},
		{
			name: "left merge - keep all left rows",
			df1: &dataframe.DataFrame{
				Columns: []string{"ID", "Name"},
				Data:    [][]any{{1, "Alice"}, {2, "Bob"}, {3, "Charlie"}},
			},
			df2: &dataframe.DataFrame{
				Columns: []string{"ID", "Age"},
				Data:    [][]any{{1, 25}, {2, 30}},
			},
			on:  "ID",
			how: dataframe.LeftMerge,
			expected: &dataframe.DataFrame{
				Columns: []string{"ID", "Name", "Age"},
				Data:    [][]any{{1, "Alice", 25}, {2, "Bob", 30}, {3, "Charlie", nil}},
			},
			expectError: false,
		},
		{
			name: "right merge - keep all right rows",
			df1: &dataframe.DataFrame{
				Columns: []string{"ID", "Name"},
				Data:    [][]any{{1, "Alice"}, {2, "Bob"}},
			},
			df2: &dataframe.DataFrame{
				Columns: []string{"ID", "Age"},
				Data:    [][]any{{1, 25}, {2, 30}, {3, 35}},
			},
			on:  "ID",
			how: dataframe.RightMerge,
			expected: &dataframe.DataFrame{
				Columns: []string{"ID", "Name", "Age"},
				Data:    [][]any{{1, "Alice", 25}, {2, "Bob", 30}, {3, nil, 35}},
			},
			expectError: false,
		},
		{
			name: "full merge - keep all rows",
			df1: &dataframe.DataFrame{
				Columns: []string{"ID", "Name"},
				Data:    [][]any{{1, "Alice"}, {2, "Bob"}, {3, "Charlie"}},
			},
			df2: &dataframe.DataFrame{
				Columns: []string{"ID", "Age"},
				Data:    [][]any{{1, 25}, {2, 30}, {4, 35}},
			},
			on:  "ID",
			how: dataframe.FullMerge,
			expected: &dataframe.DataFrame{
				Columns: []string{"ID", "Name", "Age"},
				Data:    [][]any{{1, "Alice", 25}, {2, "Bob", 30}, {3, "Charlie", nil}, {4, nil, 35}},
			},
			expectError: false,
		},
		{
			name:        "nil dataframe error",
			df1:         nil,
			df2:         &dataframe.DataFrame{},
			on:          "ID",
			how:         dataframe.InnerMerge,
			expectError: true,
		},
		{
			name: "column not found error",
			df1: &dataframe.DataFrame{
				Columns: []string{"ID", "Name"},
				Data:    [][]any{{1, "Alice"}},
			},
			df2: &dataframe.DataFrame{
				Columns: []string{"UserID", "Age"},
				Data:    [][]any{{1, 25}},
			},
			on:          "ID",
			how:         dataframe.InnerMerge,
			expectError: true,
		},
		{
			name: "invalid merge type error",
			df1: &dataframe.DataFrame{
				Columns: []string{"ID", "Name"},
				Data:    [][]any{{1, "Alice"}},
			},
			df2: &dataframe.DataFrame{
				Columns: []string{"ID", "Age"},
				Data:    [][]any{{1, 25}},
			},
			on:          "ID",
			how:         "invalid",
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.df1.Merge(test.df2, test.on, test.how)

			// Check error cases
			if test.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Check columns match
			if !strSliceEqual(result.Columns, test.expected.Columns) {
				t.Errorf("columns mismatch\nexpected: %v\ngot: %v", test.expected.Columns, result.Columns)
			}

			// Check data matches
			if len(result.Data) != len(test.expected.Data) {
				t.Errorf("data length mismatch\nexpected: %d\ngot: %d", len(test.expected.Data), len(result.Data))
				return
			}

			for i, row := range result.Data {
				if !sliceEqual(row, test.expected.Data[i]) {
					t.Errorf("row %d mismatch\nexpected: %v\ngot: %v", i, test.expected.Data[i], row)
				}
			}
		})
	}
}
