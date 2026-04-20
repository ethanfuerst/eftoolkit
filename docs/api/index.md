# API Reference

Complete API documentation for all eftoolkit modules.

## Modules

| Module | Description |
|--------|-------------|
| [`eftoolkit.sql`](sql.md) | DuckDB wrapper with S3 integration |
| [`eftoolkit.s3`](s3.md) | S3FileSystem for parquet operations |
| [`eftoolkit.gsheets`](gsheets.md) | Spreadsheet and Worksheet classes |
| [`eftoolkit.config`](config.md) | Configuration utilities |

## Imports

`eftoolkit/__init__.py` exposes `__version__` only. Import classes and helpers from their submodules:

```python
from eftoolkit import __version__
from eftoolkit.sql import DuckDB
from eftoolkit.s3 import S3FileSystem
from eftoolkit.gsheets import Spreadsheet, Worksheet
from eftoolkit.gsheets.utils import load_json_config
from eftoolkit.utils import setup_logging
```

## Package Info

```python
import eftoolkit
print(eftoolkit.__version__)  # e.g., '2.6.1'
```
