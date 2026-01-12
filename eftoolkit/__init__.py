"""eftoolkit - A streamlined Python toolkit for everyday programming tasks."""

from eftoolkit.config import load_json_config, setup_logging
from eftoolkit.gsheets import Spreadsheet, Worksheet
from eftoolkit.s3 import S3FileSystem
from eftoolkit.sql import DuckDB

__version__ = '0.0.1'
__all__ = [
    'DuckDB',
    'S3FileSystem',
    'Spreadsheet',
    'Worksheet',
    'load_json_config',
    'setup_logging',
]
