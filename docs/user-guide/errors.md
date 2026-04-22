# Errors and Retry Policy

`eftoolkit` does not define its own exception hierarchy. It raises stdlib
exceptions (`ValueError`, `FileNotFoundError`, …) and propagates third-party
ones (`gspread.exceptions.APIError`, `botocore.exceptions.ClientError`)
unchanged. Write your own retry / logging policies against those types.

## Transient vs. permanent

### Transient (safe to retry)

| Exception | Raised by | Notes |
|-----------|-----------|-------|
| `gspread.exceptions.APIError` (status `429`, `500`, `502`, `503`, `504`) | Google Sheets write operations via `Spreadsheet._execute_with_retry` | **Automatically retried** with exponential backoff. See below. |
| `botocore.exceptions.ClientError` (e.g., `SlowDown`, `ServiceUnavailable`, `ThrottlingException`) | Any `S3FileSystem` operation | **Not retried by eftoolkit.** Propagated unchanged — wrap in your own retry loop if needed. |

### Permanent (fix the input)

| Exception | Raised by | Meaning |
|-----------|-----------|---------|
| `ValueError` | `S3FileSystem` (bad S3 URI, missing `.parquet`, missing credentials); `DuckDB` (S3 not configured); `Worksheet.read(header_row=...)` < 1; unknown batch request type; duplicate/unknown reorder name; config-loader missing keys; `load_json_config` invalid arg combo | Caller-side error. Fix and call again. |
| `FileNotFoundError` | `S3FileSystem.get_object`, `.cp` (source missing), `.read_df_from_parquet` (prefix missing or prefix has no `.parquet` files) | Object or prefix doesn't exist. |
| `NotImplementedError` | `Worksheet.read` / `.read_cell` / `.read_range` in `local_preview=True` mode | Preview mode has no backing API. |
| `RuntimeError` | `Spreadsheet.open_all_previews` / `Worksheet.open_preview` outside preview mode | Mode mismatch. |
| `gspread.exceptions.WorksheetNotFound` | `Spreadsheet.delete_worksheet` with `ignore_missing=False` | Worksheet doesn't exist. |
| `gspread.exceptions.APIError` (status `400`, `401`, `403`, `404`, …) | Google Sheets operations | Non-retryable status — propagated immediately without retry. |
| `json.JSONDecodeError` | `load_json_config` / `load_service_account_credentials` | Config file / env var content isn't valid JSON. |

## Google Sheets retry policy

The retry loop lives in `Spreadsheet._execute_with_retry`. Configure via the
constructor:

```python
from eftoolkit import Spreadsheet

ss = Spreadsheet(
    credentials=creds,
    spreadsheet_name='My Sheet',
    max_retries=5,     # total attempts = max_retries + 1
    base_delay=2.0,    # seconds, exponential backoff
)
```

Backoff: `delay = base_delay * (2 ** attempt) + uniform(0, 1)` seconds.
Default: up to 6 attempts across roughly 2 + 4 + 8 + 16 + 32 = 62 seconds of
sleep. After the last attempt, the underlying `APIError` re-raises.

### Writes retry — reads do not

Every `Worksheet` write, format, and layout handler funnels through
`_execute_with_retry`. Reads do **not**:

- `Worksheet.read()` — calls `gspread.Worksheet.get_all_values()` directly.
- `Worksheet.read_cell()` — calls `gspread.Worksheet.acell()` directly.
- `Worksheet.read_range()` — calls `gspread.Worksheet.get()` directly.

A 429 or 5xx on a read surfaces immediately. Wrap read paths in your own
retry if you need parity with the write side.

## S3 error handling

`S3FileSystem` translates a narrow set of `botocore.exceptions.ClientError`
codes into `FileNotFoundError`:

- `NoSuchKey` on `get_object`, `cp` (source), `read_df_from_parquet`
  (single-file mode).
- A missing prefix, or a prefix with no `.parquet` files, on
  `read_df_from_parquet` (directory mode).

All other `ClientError` codes (`AccessDenied`, `SlowDown`,
`InvalidBucketName`, …) propagate unchanged. No retry is attempted.

### Writing your own retry for S3

```python
import time

from botocore.exceptions import ClientError

from eftoolkit import S3FileSystem

s3 = S3FileSystem()
retryable = {'SlowDown', 'ServiceUnavailable', 'ThrottlingException'}

for attempt in range(3):
    try:
        s3.write_df_to_parquet(df, 's3://bucket/out.parquet')
        break
    except ClientError as e:
        if e.response['Error']['Code'] in retryable:
            time.sleep(2 ** attempt)
            continue
        raise
```

## See also

- [Google Sheets](gsheets.md) — batching and flush semantics.
- [S3 Operations](s3.md) — `FileNotFoundError` vs raw `ClientError`.
