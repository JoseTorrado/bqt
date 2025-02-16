package test

type Mock struct {
	Filepath string            `yaml:"filepath"`
	Types    map[string]string `yaml:"types"`
}

type Output struct {
	Name string `yaml:"name"`
}

type Test struct {
	SourceFile  string
	Name        string          `yaml:"name"`
	File        string          `yaml:"file"`
	Mocks       map[string]Mock `yaml:"mocks"`
	Output      Mock            `yaml:"output"`
	FileContent string
}

type SQLMock struct {
	Sql     string
	Columns []string
}

type Replacement struct {
	TableFullName  string
	ReplaceSql     string
	TableShortName string
}

type SQLTestQuery struct {
	ExpectedMinusQuery  string
	QueryMinusExpected  string
	QueryWithMockedData string
}
