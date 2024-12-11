package gpandas

import (
	"context"
	"database/sql"
	"fmt"
	"gpandas/dataframe"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	_ "github.com/denisenkom/go-mssqldb" // SQL Server driver
)

// struct to store db config.
//
// NOTE: Prefer using env vars instead of hardcoding values
type DbConfig struct {
	database_server string
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

// Read_sql executes a SQL query against a database and returns the results as a DataFrame.
//
// Parameters:
//
//	query: The SQL query string to execute.
//	db_config: A DbConfig struct containing database connection parameters:
//	  - database_server: Type of database ("sqlserver" or other)
//	  - server: Database server hostname or IP
//	  - port: Database server port
//	  - database: Database name
//	  - username: Database user
//	  - password: Database password
//
// Returns:
//   - A pointer to a DataFrame containing the query results.
//   - An error if the database connection, query execution, or data processing fails.
//
// The DataFrame's structure will match the query results:
//   - Columns will be named according to the SELECT statement
//   - Data types will be preserved from the database types
//
// Examples:
//
//	gp := gpandas.GoPandas{}
//	config := DbConfig{
//	    database_server: "sqlserver",
//	    server: "localhost",
//	    port: "1433",
//	    database: "mydb",
//	    username: "user",
//	    password: "pass",
//	}
//	query := `SELECT employee_id, name, department
//	          FROM employees
//	          WHERE department = 'Sales'`
//	df, err := gp.Read_sql(query, config)
//	// Result DataFrame:
//	// employee_id | name  | department
//	// 1          | John  | Sales
//	// 2          | Alice | Sales
//	// 3          | Bob   | Sales
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

	if err := results.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	return &dataframe.DataFrame{
		Columns: columns,
		Data:    data,
	}, nil
}

// QueryBigQuery executes a BigQuery SQL query and returns the results as a DataFrame.
//
// Parameters:
//
//	query: The BigQuery SQL query string to execute.
//	projectID: The Google Cloud Project ID where the BigQuery dataset resides.
//
// Returns:
//   - A pointer to a DataFrame containing the query results.
//   - An error if the query execution fails or if there are issues with the BigQuery client.
//
// The DataFrame's structure will match the query results:
//   - Columns will be named according to the SELECT statement
//   - Data types will be converted from BigQuery types to Go types
//
// Examples:
//
//	gp := gpandas.GoPandas{}
//	query := `SELECT name, age, city
//	          FROM dataset.users
//	          WHERE age > 25`
//	df, err := gp.QueryBigQuery(query, "my-project-id")
//	// Result DataFrame:
//	// name    | age | city
//	// Alice   | 30  | New York
//	// Bob     | 35  | Chicago
//	// Charlie | 28  | Boston
//
// Note: Requires appropriate Google Cloud credentials to be configured in the environment.
func (GoPandas) From_gbq(query string, projectID string) (*dataframe.DataFrame, error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	q := client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("query.Read: %v", err)
	}

	// Get column names
	schema := it.Schema
	columns := make([]string, len(schema))
	for i, field := range schema {
		columns[i] = field.Name
	}

	var data [][]any

	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("iterator.Next: %v", err)
		}

		// Convert bigquery.Value to interface{}
		interfaceRow := make([]any, len(columns))
		for i, col := range columns {
			interfaceRow[i] = row[col]
		}

		data = append(data, interfaceRow)
	}

	return &dataframe.DataFrame{
		Columns: columns,
		Data:    data,
	}, nil
}
