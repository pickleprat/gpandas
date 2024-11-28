<p align="center">
  <img src="https://github.com/user-attachments/assets/2a0d2716-33ec-449d-a5fc-a9f95b8df9d9" />
</p>

# GPandas

GPandas is a data manipulation and analysis library written in Go. It provides high-performance, easy-to-use data structures and tools for working with structured data, inspired by Python's pandas library.

## Features

### Core DataFrame Operations
* Column renaming with `Rename()`
* Data merging with `Merge()` supporting:
  - Inner joins
  - Left joins 
  - Right joins
  - Full outer joins
* CSV import/export with:
  - Concurrent CSV reading for high performance
  - Custom separators
  - File or string output options
* Pretty printing with `String()` for formatted table output

### Data Types
* Strong type support for common data types:
  - FloatCol (float64)
  - StringCol (string) 
  - IntCol (int64)
  - BoolCol (bool)
* Generic TypeColumn for custom comparable types
* Type-safe operations

### Performance Features
* Concurrent CSV reading with worker pools
* Zero-copy operations where possible
* Mutex-based thread safety
* Efficient memory management
* Buffered channels for data processing

## Getting Started

### Prerequisites

GPandas requires Go version 1.18 or above (for generics support).

### Installation

```bash
go get github.com/apoplexi24/gpandas
```

## Core Components

### DataFrame
The primary data structure for handling 2-dimensional data with labeled axes.

### Set
Generic implementation of set operations supporting any comparable type.

## Performance

GPandas is designed with performance in mind, utilizing:
- Generic implementations to avoid interface overhead
- Efficient memory management
- Concurrent operations where applicable
- Zero-copy operations when possible


### Development Setup

1. Clone the repository
2. Install dependencies

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by Python's pandas library
- Built with Go's powerful generic system
- Contributions from the Go community

## Status

GPandas is under active development. While it's suitable for production use, some features are still being added and enhanced.
