# API Reference

Complete API documentation for all eftoolkit modules.

## Modules

| Module | Description |
|--------|-------------|
| [`eftoolkit.sql`](sql.md) | DuckDB wrapper with S3 integration |
| [`eftoolkit.s3`](s3.md) | S3FileSystem for parquet operations |
| [`eftoolkit.gsheets`](gsheets.md) | Spreadsheet and Worksheet classes |
| [`eftoolkit.config`](config.md) | Configuration utilities |

## Top-Level Exports

The following are available directly from `eftoolkit`:

```python
from eftoolkit import (
    DuckDB,
    S3FileSystem,
    Spreadsheet,
    Worksheet,
    load_json_config,
    setup_logging,
)
```

## Package Info

```python
import eftoolkit
print(eftoolkit.__version__)  # e.g., '0.1.0'
```
