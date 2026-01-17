"""Tests for CellLocation dataclass."""

import pytest

from eftoolkit.gsheets.types import CellLocation


def test_create_with_cell():
    """CellLocation with cell address."""
    location = CellLocation(cell='B4')

    assert location.cell == 'B4'


def test_frozen_immutable():
    """CellLocation is immutable (frozen=True)."""
    location = CellLocation(cell='B4')

    with pytest.raises(AttributeError):
        location.cell = 'C5'


def test_equality():
    """CellLocation instances with same values are equal."""
    loc1 = CellLocation(cell='B4')
    loc2 = CellLocation(cell='B4')

    assert loc1 == loc2


def test_inequality():
    """CellLocation instances with different cells are not equal."""
    loc1 = CellLocation(cell='B4')
    loc2 = CellLocation(cell='C5')

    assert loc1 != loc2


def test_hashable():
    """CellLocation is hashable (can be used in sets/dicts)."""
    location = CellLocation(cell='B4')

    locations_set = {location}

    assert location in locations_set
