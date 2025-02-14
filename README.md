<p align="center">
  <img src="https://github.com/user-attachments/assets/2a0d2716-33ec-449d-a5fc-a9f95b8df9d9" />
</p>

# GPandas

GPandas is a high-performance data manipulation and analysis library written in Go, drawing inspiration from Python's popular pandas library. It provides efficient and easy-to-use data structures, primarily the DataFrame, to handle structured data in Go applications.

## Project Structure

The project is organized into the following directories and files:

```
├── .gitignore
├── README.md
├── benchmark
│   ├── read_csv.go
│   ├── read_csv.py
│   ├── read_gbq.go
│   ├── read_gbq.py
│   └── sql_commands.go
├── dataframe
│   ├── DataFrame.go
│   └── merge.go
├── go.mod
├── go.sum
├── gpandas.go
├── gpandas_sql.go
├── tests
│   ├── dataframe
│   │   └── dataframe_test.go
│   ├── gpandas_sql_test.go
│   ├── gpandas_test.go
│   └── utils
│       └── collection
│           └── set_test.go
└── utils
    └── collection
        └── set.go
```

- **`.gitignore`**: Specifies intentionally untracked files that Git should ignore. Currently ignores CSV files, executables, and environment files (`.env`).
- **`README.md`**: The current file, providing an overview of the GPandas library, its features, project structure, and usage instructions.
- **`benchmark/`**: Contains benchmark scripts for performance evaluation against Python's pandas:
    - **`read_csv.go` & `read_csv.py`**: Benchmark Go GPandas and Python Pandas CSV reading performance.
    - **`read_gbq.go` & `read_gbq.py`**: Benchmark Go GPandas and Python Pandas-GBQ reading from Google BigQuery.
    - **`sql_commands.go`**: Example Go script demonstrating SQL query execution against BigQuery using GPandas.
- **`dataframe/`**:  Houses the core DataFrame implementation:
    - **`DataFrame.go`**: Defines the `DataFrame` struct, column types (`FloatCol`, `StringCol`, `IntCol`, `BoolCol`, `Column`, `TypeColumn`), and fundamental DataFrame operations such as:
        - `Rename()`: For renaming columns.
        - `String()`: For pretty printing DataFrame content as a formatted table in string format.
        - `ToCSV()`: For exporting DataFrame content to CSV format, either as a string or to a file.
    - **`merge.go`**: Implements DataFrame merging capabilities, supporting various join types:
        - `Merge()`:  Main function to merge two DataFrames based on a common column and specified merge type (inner, left, right, full outer).
        - `performInnerMerge()`, `performLeftMerge()`, `performRightMerge()`, `performFullMerge()`: Internal functions implementing the different merge algorithms.
- **`go.mod` & `go.sum`**: Go module files that manage project dependencies and their checksums for reproducible builds.
- **`gpandas.go`**: Serves as the primary entry point for the GPandas library. It provides high-level API functions for DataFrame creation and data loading:
    - `DataFrame()`: Constructor to create a new DataFrame from columns, data, and column type definitions.
    - `Read_csv()`: Functionality to read data from a CSV file and create a DataFrame. It uses concurrent processing for efficient CSV parsing.
- **`gpandas_sql.go`**:  Extends GPandas to interact with SQL databases and Google BigQuery:
    - `Read_sql()`: Enables reading data from relational databases (like SQL Server, PostgreSQL) by executing a SQL query and returning the result as a DataFrame.
    - `From_gbq()`: Provides functionality to query Google BigQuery and load the results into a DataFrame.
- **`tests/`**: Contains unit tests to ensure the correctness and robustness of GPandas:
    - **`dataframe/dataframe_test.go`**: Tests for core DataFrame operations defined in `dataframe/DataFrame.go` and `dataframe/merge.go` (e.g., `Rename`, `String`, `Merge`, `ToCSV`).
    - **`gpandas_sql_test.go`**: Tests for SQL related functionalities in `gpandas_sql.go` (`Read_sql`, `From_gbq`).
    - **`gpandas_test.go`**: Tests for general GPandas functionalities in `gpandas.go` (e.g., `Read_csv`).
    - **`utils/collection/set_test.go`**: Unit tests for the generic `Set` data structure implemented in `utils/collection/set.go`.
- **`utils/collection/`**: Contains generic collection utilities:
    - **`set.go`**: Implements a generic `Set` data structure in Go, providing common set operations like `Add`, `Has`, `Union`, `Intersect`, `Difference`, and `Compare`. This `Set` is used internally within GPandas for efficient data handling.

## Code Functionality

GPandas is designed to provide a familiar and efficient way to work with tabular data in Go. Key functionalities include:

### Core DataFrame Operations

- **DataFrame Creation**: Construct DataFrames from in-memory data using `gpandas.DataFrame()`, or load from external sources like CSV files using `gpandas.Read_csv()`.
- **Column Manipulation**:
    - **Renaming**: Easily rename columns using `DataFrame.Rename()`.
- **Data Merging**: Combine DataFrames based on common columns with `DataFrame.Merge()`, supporting:
    - **Inner Join (`InnerMerge`)**: Keep only matching rows from both DataFrames.
    - **Left Join (`LeftMerge`)**: Keep all rows from the left DataFrame, and matching rows from the right.
    - **Right Join (`RightMerge`)**: Keep all rows from the right DataFrame, and matching rows from the left.
    - **Full Outer Join (`FullMerge`)**: Keep all rows from both DataFrames, filling in missing values with `nil`.
- **Data Export**:
    - **CSV Export**:  Export DataFrames to CSV format using `DataFrame.ToCSV()`, with options for:
        - Custom separators.
        - Writing to a file path or returning a CSV string.
- **Data Display**:
    - **Pretty Printing**:  Generate formatted, human-readable table representations of DataFrames using `DataFrame.String()`.

### Data Loading from External Sources

- **CSV Reading**: Efficiently read CSV files into DataFrames with `gpandas.Read_csv()`, leveraging concurrent processing for performance.
- **SQL Database Integration**:
    - **`Read_sql()`**: Query and load data from SQL databases (SQL Server, PostgreSQL, and others supported by Go database/sql package) into DataFrames.
- **Google BigQuery Support**:
    - **`From_gbq()`**: Query and load data from Google BigQuery tables into DataFrames, enabling analysis of large datasets stored in BigQuery.

### Data Types

GPandas provides strong type support for common data types within DataFrames:

- **`FloatCol`**: For `float64` columns.
- **`StringCol`**: For `string` columns.
- **`IntCol`**: For `int64` columns.
- **`BoolCol`**: For `bool` columns.
- **`Column`**: Generic column type to hold `any` type values when specific type constraints are not needed.
- **`TypeColumn[T comparable]`**: Generic column type for columns of any comparable type `T`.

GPandas aims for type safety in its operations, ensuring data integrity and preventing unexpected behavior.

### Performance Features

GPandas is built with performance in mind, incorporating several features for efficiency:

- **Concurrent CSV Reading**: Utilizes worker pools and buffered channels for parallel CSV parsing, significantly speeding up CSV loading, especially for large files.
- **Efficient Data Structures**:  Uses Go's native data structures and generics to minimize overhead and maximize performance.
- **Mutex-based Thread Safety**:  Provides thread-safe operations for DataFrame manipulations using mutex locks, ensuring data consistency in concurrent environments.
- **Optimized Memory Management**: Designed for efficient memory usage to handle large datasets effectively.
- **Buffered Channels**: Employs buffered channels for data processing pipelines to improve throughput and reduce blocking.

## Getting Started

### Prerequisites

GPandas requires **Go version 1.18 or above** due to its use of generics.

### Installation

Install GPandas using `go get`:

```bash
go get github.com/apoplexi24/gpandas
```

## Core Components

### DataFrame

The central data structure in GPandas, the `DataFrame`, is designed for handling two-dimensional, labeled data. It provides methods for data manipulation, analysis, and I/O operations, similar to pandas DataFrames in Python.

### Set

The `utils/collection/set.go` provides a generic `Set` implementation, useful for various set operations. While not directly exposed as a primary user-facing component, it's an important utility within GPandas for efficient data management and algorithm implementations.

## Performance

GPandas is engineered for performance through:

- **Generics**: Leveraging Go generics to avoid runtime type assertions and interface overhead, leading to faster execution.
- **Efficient Memory Usage**:  Designed to minimize memory allocations and copies for better performance when dealing with large datasets.
- **Concurrency**: Utilizing Go's concurrency features, such as goroutines and channels, to parallelize operations like CSV reading and potentially other data processing tasks in the future.
- **Zero-copy Operations**:  Aiming for zero-copy operations wherever feasible to reduce overhead and improve speed.

### Development Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/apoplexi24/gpandas.git
   cd gpandas
   ```
2. **Install dependencies**:
   ```bash
   go mod download
   ```

## Acknowledgments

- Inspired by Python's pandas library, aiming to bring similar data manipulation capabilities to the Go ecosystem.
- Built using Go's powerful generic system for type safety and performance.
- Thanks to the Go community for valuable feedback and contributions.

## Status

GPandas is under active development and is suitable for production use. However, it's still evolving, with ongoing efforts to add more features, enhance performance, and improve API ergonomics. Expect continued updates and improvements.