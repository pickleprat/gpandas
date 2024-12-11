package gpandas

import (
	"database/sql"
	"fmt"
	"gpandas/dataframe"

	_ "github.com/denisenkom/go-mssqldb" // SQL Server driver
)

type DbConfig struct {
	database_server string
	driver          string
	server          string
	port            string
	database        string
	username        string
	password        string
}

func connect_to_db(db_config *DbConfig) (*sql.DB, error) {
	var connString string
	if db_config.database_server == "sqlserver" {
		connString = fmt.Sprintf(
			"server=%s;user id=%s;password=%s;port=%s;database=%s",
			db_config.server, db_config.username, db_config.password, db_config.port, db_config.database,
		)
	} else {
		connString = fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			db_config.server, db_config.port, db_config.username, db_config.password, db_config.database,
		)
	}
	DB, err := sql.Open(db_config.database_server, connString)
	if err != nil {
		fmt.Printf("%s", err)
		return nil, err
	}
	defer DB.Close()
	return DB, err
}

func (GoPandas) Read_sql(query string, db_config DbConfig) (*dataframe.DataFrame, error) {
	DB, err := connect_to_db(&db_config)
	if err != nil {
		return nil, fmt.Errorf("database connection error: %w", err)
	}
	defer DB.Close()

	results, err := DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query execution error: %w", err)
	}
	defer results.Close()

	// Get column names
	columns, err := results.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting columns: %w", err)
	}

	// Create slices to store the data
	columnCount := len(columns)
	data := make([][]any, columnCount)
	for i := range data {
		data[i] = make([]any, 0)
	}

	// Create a slice of interfaces to scan into
	values := make([]any, columnCount)
	valuePtrs := make([]any, columnCount)
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for results.Next() {
		err := results.Scan(valuePtrs...)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		// Add values to respective columns
		for i := range values {
			data[i] = append(data[i], values[i])
		}
	}

	// Check for errors from iterating over rows
	if err := results.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	// Create and return DataFrame
	return &dataframe.DataFrame{
		Columns: columns,
		Data:    data,
	}, nil
}
