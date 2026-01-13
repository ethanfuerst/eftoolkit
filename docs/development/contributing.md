# Contributing

Thank you for your interest in contributing to eftoolkit!

## Development Setup

1. Clone the repository:

    ```bash
    git clone https://github.com/ethanfuerst/eftoolkit.git
    cd eftoolkit
    ```

2. Install dependencies with uv:

    ```bash
    uv sync
    ```

3. Install pre-commit hooks:

    ```bash
    uv run pre-commit install
    ```

## Development Workflow

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=eftoolkit --cov-report=term-missing

# Run specific test file
uv run pytest tests/sql/test_duckdb.py
```

### Linting and Formatting

```bash
# Run all pre-commit checks
uv run pre-commit run --all-files

# Run ruff directly
uv run ruff check eftoolkit tests
uv run ruff format eftoolkit tests
```

### Building Documentation

```bash
# Serve docs locally
uv run mkdocs serve

# Build docs
uv run mkdocs build
```

## Code Style

- Follow [PEP 8](https://pep8.org/)
- Use single quotes for strings
- Use type hints for function signatures
- Write docstrings in Google style

### Example

```python
def process_data(
    df: pd.DataFrame,
    *,
    filter_empty: bool = True,
) -> pd.DataFrame:
    """Process the input DataFrame.

    Args:
        df: Input DataFrame to process.
        filter_empty: If True, remove empty rows.

    Returns:
        Processed DataFrame.

    Raises:
        ValueError: If DataFrame is empty.
    """
    if df.empty:
        raise ValueError('DataFrame cannot be empty')

    if filter_empty:
        df = df.dropna()

    return df
```

## Testing Guidelines

- Write tests for all new functionality
- Use pytest fixtures for setup
- Keep tests focused and independent
- Use moto for mocking S3 operations
- Use `local_preview=True` for Google Sheets tests

### Test Structure

```
tests/
├── conftest.py          # Shared fixtures
├── sql/
│   ├── test_duckdb.py
│   └── ...
├── s3/
│   ├── test_read.py
│   └── ...
└── gsheets/
    ├── test_spreadsheet.py
    └── ...
```

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes
3. Run tests and linting
4. Submit a pull request
5. Wait for CI to pass and review

### PR Checklist

- [ ] Tests pass locally (`uv run pytest`)
- [ ] Linting passes (`uv run pre-commit run --all-files`)
- [ ] Documentation updated if needed
- [ ] Commit messages are clear

## Questions?

Open an issue on [GitHub](https://github.com/ethanfuerst/eftoolkit/issues).
