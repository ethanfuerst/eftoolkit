"""Tests for logging utilities."""

import logging

from eftoolkit.utils import setup_logging


def test_setup_logging():
    """setup_logging configures root logger with correct level."""
    setup_logging(level=logging.WARNING)

    logger = logging.getLogger()

    assert logger.level == logging.WARNING


def test_setup_logging_default_level():
    """setup_logging uses INFO level by default."""
    setup_logging()

    logger = logging.getLogger()

    assert logger.level == logging.INFO


def test_setup_logging_custom_format():
    """setup_logging accepts custom format string."""
    custom_format = '%(levelname)s: %(message)s'

    setup_logging(format=custom_format)

    # Verify no error occurred
    logger = logging.getLogger()

    assert logger.level == logging.INFO


def test_setup_logging_custom_date_format():
    """setup_logging accepts custom date_format string."""
    custom_date_format = '%Y-%m-%d'

    setup_logging(date_format=custom_date_format)

    logger = logging.getLogger()

    assert logger.level == logging.INFO


def test_setup_logging_date_format_none_by_default():
    """setup_logging uses None date_format by default."""
    setup_logging()

    # Verify no error occurred - date_format=None is valid
    logger = logging.getLogger()

    assert logger.level == logging.INFO
