# Running Unit Tests

This guide explains how to run the unit tests for Petaly.

## Prerequisites

1. **Python 3.10 - 3.12** (as required by Petaly)
2. **pytest** - Test framework (should be installed with dependencies)
3. **Virtual environment** (recommended)

## Setup

### 1. Install Dependencies

If you haven't already, install Petaly and its dependencies:

```bash
# From project root
pip install -e .

# Or install with all optional dependencies
pip install -e ".[all]"
```

### 2. Install Test Dependencies

pytest should be installed, but if not:

```bash
pip install pytest
```

## Running Tests

### Run All Tests

From the project root directory:

```bash
# Run all tests
pytest tests/unittest/

# Or with verbose output
pytest tests/unittest/ -v

# Or with even more detail
pytest tests/unittest/ -vv
```

### Run Specific Test Files

```bash
# Run tests for a specific connector
pytest tests/unittest/connectors/postgres/test_postgres_connector.py

# Run tests for MySQL
pytest tests/unittest/connectors/mysql/test_mysql_connector.py

# Run tests for BigQuery
pytest tests/unittest/connectors/bigquery/test_bigquery_connector.py
```

### Run Specific Test Classes

```bash
# Run only TestPsqlConnector class
pytest tests/unittest/connectors/postgres/test_postgres_connector.py::TestPsqlConnector

# Run only TestMysqlExtractor class
pytest tests/unittest/connectors/mysql/test_mysql_connector.py::TestMysqlExtractor
```

### Run Specific Test Methods

```bash
# Run a specific test method
pytest tests/unittest/connectors/postgres/test_postgres_connector.py::TestPsqlConnector::test_connector_initialization
```

### Run Tests with Coverage

```bash
# Install coverage tool
pip install pytest-cov

# Run tests with coverage report
pytest tests/unittest/ --cov=src/petaly --cov-report=html --cov-report=term

# View HTML coverage report
open htmlcov/index.html  # macOS
# or
xdg-open htmlcov/index.html  # Linux
```

## Test Output Options

### Verbose Output

```bash
# Show test names and results
pytest tests/unittest/ -v

# Show even more detail (including print statements)
pytest tests/unittest/ -vv -s
```

### Stop on First Failure

```bash
pytest tests/unittest/ -x
```

### Show Local Variables on Failure

```bash
pytest tests/unittest/ -l
```

### Run Tests in Parallel (faster)

```bash
# Install pytest-xdist
pip install pytest-xdist

# Run tests in parallel (4 workers)
pytest tests/unittest/ -n 4
```

## Common Test Patterns

### Run Tests Matching a Pattern

```bash
# Run all tests with "connector" in the name
pytest tests/unittest/ -k connector

# Run all tests with "extract" in the name
pytest tests/unittest/ -k extract

# Run all tests except those with "loader" in the name
pytest tests/unittest/ -k "not loader"
```

### Run Tests by Marker

```bash
# If you add markers to tests (e.g., @pytest.mark.slow)
pytest tests/unittest/ -m slow
```

## Test Structure

```
tests/unittest/
├── connectors/
│   ├── bigquery/
│   │   └── test_bigquery_connector.py
│   ├── csv/
│   │   └── test_csv_connector.py
│   ├── gs/
│   │   └── test_gs_connector.py
│   ├── mysql/
│   │   └── test_mysql_connector.py
│   ├── postgres/
│   │   └── test_postgres_connector.py
│   ├── redshift/
│   │   └── test_redshift_connector.py
│   └── s3/
│       └── test_s3_connector.py
└── README.md
```

## Troubleshooting

### Import Errors

If you get import errors, make sure you're running from the project root and that Petaly is installed:

```bash
# Install in editable mode
pip install -e .

# Or add src to PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$(pwd)/src
```

### Missing Dependencies

Some tests require optional dependencies:

```bash
# Install all optional dependencies
pip install -e ".[all]"

# Or install specific ones
pip install -e ".[gcp]"  # For BigQuery/GS tests
pip install -e ".[aws]"  # For Redshift/S3 tests
```

### Mock-Related Issues

The tests use `unittest.mock` extensively. If you see mock-related errors, ensure you're using Python 3.10+ where `unittest.mock` is part of the standard library.

## Continuous Integration

For CI/CD pipelines, you might want to:

```bash
# Run tests with JUnit XML output
pytest tests/unittest/ --junitxml=test-results.xml

# Run with coverage and fail if coverage is below threshold
pytest tests/unittest/ --cov=src/petaly --cov-fail-under=80
```

## Example Commands

```bash
# Quick test run (all tests)
pytest tests/unittest/ -v

# Run only PostgreSQL tests
pytest tests/unittest/connectors/postgres/ -v

# Run with coverage
pytest tests/unittest/ --cov=src/petaly --cov-report=term-missing

# Run specific test
pytest tests/unittest/connectors/postgres/test_postgres_connector.py::TestPsqlConnector::test_connector_initialization -v
```

## See Also

- [Test Review Document](REVIEW.md) - Comprehensive review of test coverage and issues
- [Pytest Documentation](https://docs.pytest.org/) - Official pytest documentation

