"""Tests for WorksheetAsset dataclass."""

import pandas as pd

from eftoolkit.gsheets.runner import CellLocation, CellRange, WorksheetAsset


def test_create_minimal():
    """WorksheetAsset with only required fields."""
    df = pd.DataFrame({'a': [1, 2, 3]})
    location = CellLocation(cell='B4')

    asset = WorksheetAsset(df=df, location=location)

    assert asset.df is df
    assert asset.location == location
    assert asset.post_write_hooks == []


def test_create_with_post_write_hooks():
    """WorksheetAsset with post_write_hooks."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='A1')
    hooks = [lambda ws: None, lambda ws: None]

    asset = WorksheetAsset(df=df, location=location, post_write_hooks=hooks)

    assert len(asset.post_write_hooks) == 2


def test_post_write_hooks_default_empty_list():
    """Each WorksheetAsset gets its own empty list for post_write_hooks."""
    df = pd.DataFrame({'a': [1]})
    location = CellLocation(cell='A1')

    asset1 = WorksheetAsset(df=df, location=location)
    asset2 = WorksheetAsset(df=df, location=location)

    # Modify one, shouldn't affect the other
    asset1.post_write_hooks.append(lambda ws: None)

    assert len(asset1.post_write_hooks) == 1
    assert len(asset2.post_write_hooks) == 0


# Computed range property tests


def test_num_rows_and_num_cols():
    """num_rows and num_cols reflect DataFrame dimensions."""
    df = pd.DataFrame({'Name': ['Alice', 'Bob', 'Charlie'], 'Score': [95, 87, 92]})
    asset = WorksheetAsset(df=df, location=CellLocation(cell='A1'))

    assert asset.num_rows == 3
    assert asset.num_cols == 2


def test_start_and_end_row():
    """start_row and end_row are 1-based row indices."""
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5]})
    asset = WorksheetAsset(df=df, location=CellLocation(cell='B4'))

    assert asset.start_row == 4  # Header at row 4
    assert asset.end_row == 9  # 5 data rows: rows 5-9


def test_start_and_end_col():
    """start_col and end_col are column letters."""
    df = pd.DataFrame({'A': [1], 'B': [2], 'C': [3], 'D': [4]})
    asset = WorksheetAsset(df=df, location=CellLocation(cell='B4'))

    assert asset.start_col == 'B'
    assert asset.end_col == 'E'  # 4 columns: B, C, D, E


def test_header_range():
    """header_range returns a CellRange for the header row."""
    df = pd.DataFrame({'Name': ['Alice'], 'Score': [95], 'Grade': ['A']})
    asset = WorksheetAsset(df=df, location=CellLocation(cell='B4'))

    assert isinstance(asset.header_range, CellRange)
    assert asset.header_range.value == 'B4:D4'


def test_data_range():
    """data_range returns a CellRange for data rows (excluding header)."""
    df = pd.DataFrame({'Name': ['Alice', 'Bob'], 'Score': [95, 87]})
    asset = WorksheetAsset(df=df, location=CellLocation(cell='B4'))

    # Header at B4, data at B5:C6
    assert isinstance(asset.data_range, CellRange)
    assert asset.data_range.value == 'B5:C6'


def test_full_range():
    """full_range returns a CellRange for header + data."""
    df = pd.DataFrame({'Name': ['Alice', 'Bob'], 'Score': [95, 87]})
    asset = WorksheetAsset(df=df, location=CellLocation(cell='B4'))

    # Header at B4, data at B5:C6, full range B4:C6
    assert isinstance(asset.full_range, CellRange)
    assert asset.full_range.value == 'B4:C6'


def test_column_ranges():
    """column_ranges maps column names to CellRange objects."""
    df = pd.DataFrame({'Name': ['Alice', 'Bob'], 'Score': [95, 87]})
    asset = WorksheetAsset(df=df, location=CellLocation(cell='B4'))

    assert isinstance(asset.column_ranges['Name'], CellRange)
    assert isinstance(asset.column_ranges['Score'], CellRange)
    assert asset.column_ranges['Name'].value == 'B4:B6'  # Header + 2 data rows
    assert asset.column_ranges['Score'].value == 'C4:C6'


def test_data_column_ranges():
    """data_column_ranges maps column names to data-only CellRange objects."""
    df = pd.DataFrame({'Name': ['Alice', 'Bob'], 'Score': [95, 87]})
    asset = WorksheetAsset(df=df, location=CellLocation(cell='B4'))

    assert isinstance(asset.data_column_ranges['Name'], CellRange)
    assert isinstance(asset.data_column_ranges['Score'], CellRange)
    assert asset.data_column_ranges['Name'].value == 'B5:B6'  # Data only, no header
    assert asset.data_column_ranges['Score'].value == 'C5:C6'


def test_ranges_with_a1_location():
    """Computed ranges work correctly when starting at A1."""
    df = pd.DataFrame({'X': [1, 2, 3]})
    asset = WorksheetAsset(df=df, location=CellLocation(cell='A1'))

    # Single-column header is a single cell, so CellRange returns 'A1' not 'A1:A1'
    assert asset.header_range.value == 'A1'
    assert asset.header_range.is_single_cell
    assert asset.data_range.value == 'A2:A4'
    assert asset.full_range.value == 'A1:A4'
    assert asset.column_ranges['X'].value == 'A1:A4'
    assert asset.data_column_ranges['X'].value == 'A2:A4'


def test_ranges_with_double_letter_columns():
    """Computed ranges handle column letters beyond Z (AA, AB, etc.)."""
    # Create a DataFrame with 30 columns to go past Z
    cols = {f'Col{i}': [i] for i in range(30)}
    df = pd.DataFrame(cols)
    asset = WorksheetAsset(df=df, location=CellLocation(cell='A1'))

    # 30 columns starting at A goes to AD (A-Z is 26, then AA, AB, AC, AD)
    assert asset.start_col == 'A'
    assert asset.end_col == 'AD'
    assert asset.header_range.value == 'A1:AD1'


def test_ranges_with_empty_dataframe():
    """Computed ranges handle empty DataFrames gracefully."""
    df = pd.DataFrame({'A': [], 'B': []})
    asset = WorksheetAsset(df=df, location=CellLocation(cell='B4'))

    assert asset.num_rows == 0
    assert asset.num_cols == 2
    assert asset.start_row == 4
    assert asset.end_row == 4  # Header only, no data
    assert asset.header_range.value == 'B4:C4'
    assert asset.full_range.value == 'B4:C4'


def test_range_properties_provide_computed_values():
    """Range properties provide access to CellRange computed values."""
    df = pd.DataFrame({'Name': ['Alice', 'Bob'], 'Score': [95, 87]})
    asset = WorksheetAsset(df=df, location=CellLocation(cell='B4'))

    # Test that CellRange properties are accessible
    assert asset.full_range.start_row == 3  # 0-indexed
    assert asset.full_range.end_row == 5  # 0-indexed
    assert asset.full_range.start_col == 1  # 0-indexed (B=1)
    assert asset.full_range.end_col == 2  # 0-indexed (C=2)
    assert asset.full_range.num_rows == 3  # Header + 2 data rows
    assert asset.full_range.num_cols == 2


def test_hash_based_on_location():
    """WorksheetAsset is hashable based on location."""
    df = pd.DataFrame({'A': [1, 2, 3]})
    asset1 = WorksheetAsset(df=df, location=CellLocation(cell='A1'))
    asset2 = WorksheetAsset(df=df, location=CellLocation(cell='A1'))
    asset3 = WorksheetAsset(df=df, location=CellLocation(cell='B2'))

    # Same location should have same hash
    assert hash(asset1) == hash(asset2)
    # Different location should have different hash
    assert hash(asset1) != hash(asset3)
