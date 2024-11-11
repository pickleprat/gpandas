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
