package test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"os"

	"cloud.google.com/go/bigquery"
	"github.com/alexeyco/simpletable"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"google.golang.org/api/googleapi"
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

func getDetailedBigQueryError(err error) string {
	if err == nil {
		return ""
	}

	var gErr *googleapi.Error
	if errors.As(err, &gErr) {
		if len(gErr.Errors) > 0 {
			detailedMsg := gErr.Errors[0].Message
			return fmt.Sprintf("BigQuery Syntax Error: %s", detailedMsg)
		}
	}

	return fmt.Sprintf("Query execution failed: %v", err)
}

func RunQueryMinusExpectation(ctx context.Context, client *bigquery.Client, query string) error {
	q := client.Query((query)) // I should make this more concise
	it, err := q.Read(ctx)
	if err != nil {
		fmt.Println(red(fmt.Sprintf("ERROR - %s\n", getDetailedBigQueryError(err))))
		return err
	}

	table := simpletable.New()
	table.Header = &simpletable.Header{}
	var cells [][]*simpletable.Cell
	var columnNames []string

	// Read and structure the data
	firstRow := true
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}

		// Set column headers once
		if firstRow {
			for _, field := range it.Schema {
				columnNames = append(columnNames, field.Name)
				table.Header.Cells = append(table.Header.Cells, &simpletable.Cell{
					Align: simpletable.AlignCenter, Text: field.Name,
				})
			}
			firstRow = false
		}

		var rowCells []*simpletable.Cell
		for _, value := range row {
			rowCells = append(rowCells, &simpletable.Cell{
				Text: fmt.Sprintf("%v", value),
			})
		}
		cells = append(cells, rowCells)
	}

	if len(cells) == 0 {
		return nil
	}

	table.Body = &simpletable.Body{Cells: cells}

	table.Footer = &simpletable.Footer{
		Cells: []*simpletable.Cell{
			{Align: simpletable.AlignCenter, Span: len(columnNames), Text: yellow("Additional Records")},
		},
	}

	table.SetStyle(simpletable.StyleDefault)
	table.Println()

	// Print the error message
	errorMsg := "Query output has records not in expectation"
	fmt.Println(red(fmt.Sprintf("ERROR - %s\n", errorMsg)))
	return errors.New(errorMsg)
}

func RunExpectationMinusQuery(ctx context.Context, client *bigquery.Client, query string) error {
	it, err := client.Query(query).Read(ctx)
	if err != nil {
		fmt.Println(red(fmt.Sprintf("ERROR - %s\n", getDetailedBigQueryError(err))))
		return err
	}

	table := simpletable.New()
	table.Header = &simpletable.Header{}
	var cells [][]*simpletable.Cell
	var columnNames []string

	// Read and structure the data
	firstRow := true
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}

		// Set column headers once
		if firstRow {
			for _, field := range it.Schema {
				columnNames = append(columnNames, field.Name)
				table.Header.Cells = append(table.Header.Cells, &simpletable.Cell{
					Align: simpletable.AlignCenter, Text: field.Name,
				})
			}
			firstRow = false
		}

		// Populate the table with row values
		var rowCells []*simpletable.Cell
		for _, value := range row {
			rowCells = append(rowCells, &simpletable.Cell{
				Text: fmt.Sprintf("%v", value),
			})
		}
		cells = append(cells, rowCells)
	}

	// If no missing data, exit early
	if len(cells) == 0 {
		return nil
	}

	table.Body = &simpletable.Body{Cells: cells}

	// Footer indicating issue
	table.Footer = &simpletable.Footer{
		Cells: []*simpletable.Cell{
			{Align: simpletable.AlignCenter, Span: len(columnNames), Text: yellow("Missing Records")},
		},
	}

	table.SetStyle(simpletable.StyleDefault)
	table.Println()

	// Print the error message immediately after the table
	errorMsg := "Query output is missing expected records"
	fmt.Println(red(fmt.Sprintf("ERROR - %s\n", errorMsg)))
	return errors.New(errorMsg)
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
			fmt.Println(red(fmt.Sprintf("Test Failed: %+v : %+v\n", t.Name, t.SourceFile)))
			lastErr = err
		}
	}
	if lastErr != nil {
		lastErr = errors.New("Some tests failed")
	}
	return lastErr
}
