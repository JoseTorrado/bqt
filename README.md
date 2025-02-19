# bqt

**bqt** is a CLI tool designed to help you run BigQuery jobs completely locally without the need to connect to the cloud. It is built to bridge the gap for unit testing when developing in BigQuery, ensuring your queries behave as expected before deployment.

## Features

- **Fully Local Execution**: Run and test BigQuery jobs locally using an emulator.
- **Unit Testing for BigQuery**: Define and execute tests with mock inputs and expected outputs.
- **Seamless Integration**: Works with YAML-based test definitions and CSV-based data mocking.
- **Automated Validation**: Compares query outputs against expected results to determine test pass/fail status.

## Installation

To install **bqt**, ensure you have Go installed and run the following command:

```bash
 go install github.com/JoseTorrado/bqt/cmd/bqt@latest
```

This will fetch and install the latest version of **bqt** from the repository.

## Usage

**bqt** tests are defined using `YAML` files. Inputs and outputs are passed as CSV files, which the tool automatically mocks into your queries.

To run tests, use:

```bash
bqt t --tests tests_folder
```

Where `tests_folder` contains the YAML files with test definitions.

## Test Definitions

Tests should be defined in `YAML` format as follows:

```yaml
name: simple_test
file: tests_data/test1/test1.sql
mocks:
  "`dataset`.`table`":
    filepath: tests_data/test1/test1_in1.csv
    types:
      c1: int64
output:
  filepath: tests_data/test1/out.csv
  types:
    column1: string
```

### Explanation
- **`mocks`**: Defines the source tables your query pulls from and the sample data to be mocked as input.
- **`output`**: Specifies the expected results for comparison. If a schema is not provided, it defaults to `STRING`.

## How It Works

**bqt** uses a BigQuery emulator to create an on-demand server powered by **zetasql**. This allows it to:

- Parse and execute queries with behavior matching BigQuery’s functionality.
- Automatically mock data inputs into the queries under test.
- Compare query results with expected outputs to determine test success.

## References
- [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator)
- [goccy/go-zetasql](https://github.com/goccy/go-zetasql)
- [dav009/bqt](https://github.com/dav009/bqt)

## License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/JoseTorrado/bqt/blob/main/LICENSE) file for more details.

