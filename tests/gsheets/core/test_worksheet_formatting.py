"""Tests for WorksheetFormatting dataclass."""

from pathlib import Path

from eftoolkit.gsheets.runner import WorksheetFormatting


def test_create_empty():
    """WorksheetFormatting with all defaults."""
    formatting = WorksheetFormatting()

    assert formatting.merge_ranges == []
    assert formatting.conditional_formats == []
    assert formatting.notes == {}
    assert formatting.column_widths == {}
    assert formatting.borders == {}
    assert formatting.data_validations == []
    assert formatting.freeze_rows is None
    assert formatting.freeze_columns is None
    assert formatting.auto_resize_columns is None
    assert formatting.format_config_path is None
    assert formatting.format_dict is None


def test_create_with_freeze_rows():
    """WorksheetFormatting with freeze_rows."""
    formatting = WorksheetFormatting(freeze_rows=1)

    assert formatting.freeze_rows == 1


def test_create_with_freeze_columns():
    """WorksheetFormatting with freeze_columns."""
    formatting = WorksheetFormatting(freeze_columns=2)

    assert formatting.freeze_columns == 2


def test_create_with_auto_resize_columns():
    """WorksheetFormatting with auto_resize_columns."""
    formatting = WorksheetFormatting(auto_resize_columns=(0, 5))

    assert formatting.auto_resize_columns == (0, 5)


def test_create_with_format_config_path():
    """WorksheetFormatting with format_config_path."""
    config_path = Path('formats/summary.json')

    formatting = WorksheetFormatting(format_config_path=config_path)

    assert formatting.format_config_path == config_path


def test_create_with_format_dict():
    """WorksheetFormatting with inline format_dict."""
    format_dict = {'header_color': '#4a86e8', 'bold': True}

    formatting = WorksheetFormatting(format_dict=format_dict)

    assert formatting.format_dict == format_dict


def test_create_with_merge_ranges():
    """WorksheetFormatting with merge_ranges."""
    formatting = WorksheetFormatting(merge_ranges=['A1:C1', 'B5:D5'])

    assert formatting.merge_ranges == ['A1:C1', 'B5:D5']


def test_create_with_notes():
    """WorksheetFormatting with notes."""
    notes = {'A1': 'Header note', 'B2': 'Data note'}

    formatting = WorksheetFormatting(notes=notes)

    assert formatting.notes == notes


def test_create_with_column_widths():
    """WorksheetFormatting with column_widths."""
    column_widths = {'A': 100, 'B': 150, 0: 200}

    formatting = WorksheetFormatting(column_widths=column_widths)

    assert formatting.column_widths == column_widths


def test_create_with_all_options():
    """WorksheetFormatting with all options set."""
    formatting = WorksheetFormatting(
        merge_ranges=['A1:C1'],
        conditional_formats=[{'range': 'B2:B10', 'rule': 'positive'}],
        notes={'A1': 'Note'},
        column_widths={'A': 100},
        borders={'A1:C10': {'style': 'solid'}},
        data_validations=[{'range': 'D1:D10', 'type': 'list'}],
        freeze_rows=1,
        freeze_columns=1,
        auto_resize_columns=(0, 5),
        format_config_path=Path('formats/base.json'),
        format_dict={'header_color': '#4a86e8'},
    )

    assert formatting.merge_ranges == ['A1:C1']
    assert formatting.conditional_formats == [{'range': 'B2:B10', 'rule': 'positive'}]
    assert formatting.notes == {'A1': 'Note'}
    assert formatting.column_widths == {'A': 100}
    assert formatting.borders == {'A1:C10': {'style': 'solid'}}
    assert formatting.data_validations == [{'range': 'D1:D10', 'type': 'list'}]
    assert formatting.freeze_rows == 1
    assert formatting.freeze_columns == 1
    assert formatting.auto_resize_columns == (0, 5)
    assert formatting.format_config_path == Path('formats/base.json')
    assert formatting.format_dict == {'header_color': '#4a86e8'}


def test_mutable_defaults_isolated():
    """Each WorksheetFormatting gets its own mutable default lists/dicts."""
    f1 = WorksheetFormatting()
    f2 = WorksheetFormatting()

    # Modify one, shouldn't affect the other
    f1.merge_ranges.append('A1:B1')
    f1.notes['A1'] = 'Test'

    assert f1.merge_ranges == ['A1:B1']
    assert f2.merge_ranges == []
    assert f1.notes == {'A1': 'Test'}
    assert f2.notes == {}
