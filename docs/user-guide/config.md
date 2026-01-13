# Configuration Utilities

The `config` module provides utilities for JSON loading and logging setup.

## JSON Config Loading

### Basic Usage

```python
from eftoolkit import load_json_config

config = load_json_config('config.json')
```

### JSONC Support

Load JSON files with comments (JSONC):

```python
config = load_json_config('config.jsonc')
```

Example `config.jsonc`:

```jsonc
{
  // Database configuration
  "database": {
    "host": "localhost",
    "port": 5432,
    "name": "myapp"
  },

  /*
   * Feature flags
   * These can be toggled for different environments
   */
  "features": {
    "debug": true,
    "cache_enabled": false
  },

  "api_key": "your-key-here"  // Replace in production
}
```

Supported comment styles:

- Single-line: `// comment`
- Block: `/* comment */`

### Error Handling

```python
from eftoolkit import load_json_config
import json

try:
    config = load_json_config('config.json')
except FileNotFoundError:
    print("Config file not found")
except json.JSONDecodeError as e:
    print(f"Invalid JSON: {e}")
```

## Logging Setup

### Basic Setup

```python
from eftoolkit import setup_logging

setup_logging()
```

This configures the root logger with:

- Level: `INFO`
- Format: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`

### Custom Configuration

```python
import logging
from eftoolkit import setup_logging

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
from eftoolkit import setup_logging
import logging

# Configure root logger
setup_logging(level=logging.INFO)

# All module loggers inherit this configuration
logger = logging.getLogger(__name__)
logger.info("Application started")

# Third-party library loggers also work
logging.getLogger('boto3').setLevel(logging.WARNING)
```

## See Also

- [API Reference](../api/config.md) - Full API documentation
