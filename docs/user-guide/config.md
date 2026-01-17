# Utilities

General utilities for common tasks.

## Logging Setup

### Basic Setup

```python
from eftoolkit.utils import setup_logging

setup_logging()
```

This configures the root logger with:

- Level: `INFO`
- Format: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`

### Custom Configuration

```python
import logging
from eftoolkit.utils import setup_logging

# Debug level
setup_logging(level=logging.DEBUG)

# Custom format
setup_logging(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s',
)
```

### Using with Other Loggers

```python
from eftoolkit.utils import setup_logging
import logging

# Configure root logger
setup_logging(level=logging.INFO)

# All module loggers inherit this configuration
logger = logging.getLogger(__name__)
logger.info("Application started")

# Third-party library loggers also work
logging.getLogger('boto3').setLevel(logging.WARNING)
```

## JSON Config Loading

For loading JSONC files with comment support, use the gsheets utilities:

```python
from eftoolkit.gsheets.utils import load_json_config, remove_comments

config = load_json_config('config.jsonc')
```

See the [gsheets documentation](gsheets.md) for more details.

## See Also

- [API Reference](../api/config.md) - Full API documentation
