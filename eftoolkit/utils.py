"""General utilities for eftoolkit."""

import logging


def setup_logging(
    level: int = logging.INFO,
    format: str | None = None,
    date_format: str | None = None,
) -> None:
    """Configure the root logger with the specified level and format.

    Args:
        level: Logging level (default: logging.INFO)
        format: Log format string (default: '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        date_format: Date/time format string for %(asctime)s (default: None, uses
            logging module default of '%Y-%m-%d %H:%M:%S,uuu')
    """
    if format is None:
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    logging.basicConfig(
        level=level,
        format=format,
        datefmt=date_format,
        force=True,
    )
