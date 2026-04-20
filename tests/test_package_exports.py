"""Smoke tests for the top-level eftoolkit package."""

import eftoolkit


def test_version_is_non_empty_string():
    assert isinstance(eftoolkit.__version__, str)
    assert eftoolkit.__version__
