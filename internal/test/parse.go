package test

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/goccy/go-yaml"
)

// Returns a Test structure given a filepath
func ParseTest(path string) (Test, error) {

	yamlFile, err := os.Open(path)
	if err != nil {
		return Test{}, err
	}
	bytes, err := io.ReadAll(yamlFile)
	if err != nil {
		return Test{}, err
	}
	test := Test{}
	if err := yaml.Unmarshal(bytes, &test); err != nil {
		return Test{}, err
	}
	sqlQuery, err := ReadContents(test.File)
	test.FileContent = sqlQuery
	test.SourceFile = path
	if err != nil {
		return Test{}, err
	}
	return test, nil
}

// Returns Test structs found in a given folder
// TODO: Consider a Walk approahc instead to find yamls in nested directories
func ParseFolder(path string) ([]Test, error) {

	files, err := os.ReadDir(path)
	tests := []Test{}
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		if strings.HasSuffix(strings.ToLower(f.Name()), ".yaml") { // checks for the test configs, identifying them by the json suffix
			fullPath := filepath.Join(path, f.Name())
			test, err := ParseTest(fullPath) // we parse the tests now...
			if err != nil {
				return nil, err
			}
			fmt.Println(fmt.Sprintf("Detected test: %v", fullPath))
			tests = append(tests, test)
		}

	}
	return tests, nil
}

/*
Utility Function, converts a CSV file into a List of dictionaries.
Each row is converted into a dictionary where the keys are columns.
*/
func CSVToMap(reader io.Reader) []map[string]string {

	r := csv.NewReader(reader)
	rows := []map[string]string{}
	var header []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if header == nil {
			header = record
		} else {
			dict := map[string]string{}
			for i := range header {
				dict[header[i]] = record[i]
			}
			rows = append(rows, dict)
		}
	}
	return rows
}

func SaveSQL(path string, sql string) error {
	err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		return err
	}
	data := []byte(sql)
	err = os.WriteFile(path, data, 0644)
	if err != nil {
		return err
	}
	return nil
}
