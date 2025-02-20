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
func ParseFolder(rootPath string) ([]Test, error) {
	tests := []Test{}

	// Walk through all files and directories recursively
	err := filepath.WalkDir(rootPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Check if the file has a .yaml extension
		if !d.IsDir() && strings.HasSuffix(strings.ToLower(d.Name()), ".yaml") {
			fmt.Println(fmt.Sprintf("Detected test: %v", path))

			test, err := ParseTest(path)
			if err != nil {
				return fmt.Errorf("failed to parse test %v: %w", path, err)
			}
			tests = append(tests, test)
		}

		return nil
	})

	if err != nil {
		return nil, err
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
