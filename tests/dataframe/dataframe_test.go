package dataframe_test

import (
	"gpandas/dataframe"
	"testing"
)

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
