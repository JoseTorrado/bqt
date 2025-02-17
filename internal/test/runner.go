package test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"os"

	"cloud.google.com/go/bigquery"
	"github.com/fatih/color"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// COnverts each row of the csv into a sql statement
func mockInputToSql(columnName string, value string, columnType string) string {

	if value == "" {
		value = "null"
	} else {
		value = fmt.Sprintf("\"%s\"", value)
	}
	if columnType != "" {
		return fmt.Sprintf("CAST(%s AS %s) AS %s", value, columnType, columnName)
	}

	return fmt.Sprintf("%s AS %s", value, columnName)

}

// Converts the mocked input into a sql query that can be injected into the sql being testes as source table
func mockToSql(m Mock) (SQLMock, error) {

	allColumns := []string{}
	file, err := os.Open(m.Filepath)
	if err != nil {
		return SQLMock{}, err

	}
	data := CSVToMap(file)
	var sqlStatements []string
	for _, row := range data {

		columnsValues := []string{}
		columns := make([]string, 0)
		// ordering columns so we can test
		for k := range row {
			columns = append(columns, k)
		}
		sort.Strings(columns)
		if len(allColumns) == 0 {
			allColumns = columns
		}
		for _, column := range columns {
			value := row[column]
			columnType := m.Types[column]
			entry := mockInputToSql(column, value, columnType)
			columnsValues = append(columnsValues, entry)

		}
		statement := fmt.Sprintf("\n SELECT %s", strings.Join(columnsValues, ", "))
		sqlStatements = append(sqlStatements, statement)
	}
	return SQLMock{Sql: strings.Join(sqlStatements, "\n UNION ALL \n"), Columns: allColumns}, nil

}

func RunQueryMinusExpectation(ctx context.Context, client *bigquery.Client, query string) error {
	q := client.Query((query))
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}
	for {

		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}

		fmt.Println(yellow("\t------Unexpected Data-------"))
		for i, field := range it.Schema {
			record := fmt.Sprintf("\t%s : %v", field.Name, row[i])
			color.Green(record)

		}
		fmt.Println(yellow("\t-------------"))
		err = errors.New("Query returned extra data compared to expectation..")
	}

	return err
}

func RunExpectationMinusQuery(ctx context.Context, client *bigquery.Client, query string) error {
	it, err := client.Query((query)).Read(ctx)
	if err != nil {
		return err
	}
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}
		fmt.Println(yellow("\t------Missing Data----------"))
		for i, field := range it.Schema {
			record := fmt.Sprintf("\t%s : %v", field.Name, row[i])
			color.Red(record)

		}
		fmt.Println(yellow("\t-------------"))
		err = errors.New("Expected data is missing..")

	}
	return err
}

func RunTests(mode string, tests []Test) error {
	ctx := context.Background()
	const (
		projectID = "dummybqproject"
		datasetID = "dataset1"
		routineID = "routine1"
	)
	bqServer, err := server.New(server.TempStorage)
	if err != nil {
		return err
	}
	if err := bqServer.Load(
		server.StructSource(
			types.NewProject(
				projectID,
				types.NewDataset(
					datasetID,
				),
			),
		),
	); err != nil {
		return err
	}
	if err := bqServer.SetProject(projectID); err != nil {
		return err
	}
	testServer := bqServer.TestServer()
	defer testServer.Close()

	var client *bigquery.Client
	if mode == "local" {
		client, err = bigquery.NewClient(
			ctx,
			projectID,
			option.WithEndpoint(testServer.URL),
			option.WithoutAuthentication(),
		)
	} else {
		client, err = bigquery.NewClient(
			ctx,
			projectID,
		)
	}

	if err != nil {
		return err
	}
	defer client.Close()

	var lastErr error = nil

	for _, t := range tests {
		fmt.Println("")
		fmt.Println(fmt.Sprintf("Running Test: %+v : %+v", t.Name, t.SourceFile))
		sqlQueries, err := GenerateTestSQL(t)

		if err != nil {
			return err
		}
		// Checking for unexpected data
		unexpectedDataErr := RunQueryMinusExpectation(ctx, client, sqlQueries.QueryMinusExpected)

		// Check for missing data
		missingDataErr := RunExpectationMinusQuery(ctx, client, sqlQueries.ExpectedMinusQuery)

		// Combine the errors
		testErr := errors.Join(unexpectedDataErr, missingDataErr)

		if testErr == nil {
			fmt.Println(green(fmt.Sprintf("Test Success: %+v : %+v\n", t.Name, t.SourceFile)))
		} else {
			if unexpectedDataErr != nil {
				fmt.Println(red(fmt.Sprintf("Unexpected Data Error: %+v", unexpectedDataErr)))
			}
			if missingDataErr != nil {
				fmt.Println(red(fmt.Sprintf("Missing Data Error: %+v", missingDataErr)))
			}
			fmt.Println(red(fmt.Sprintf("Test Failed: %+v : %+v\n", t.Name, t.SourceFile)))
			lastErr = err
		}
	}
	if lastErr != nil {
		lastErr = errors.New("Some tests failed")
	}
	return lastErr
}
