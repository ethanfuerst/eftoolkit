"""Tests for Google Sheets local preview functionality."""

from eftoolkit.gsheets import Spreadsheet


class TestPreviewColumnWidths:
    """Tests for column width handling in preview mode."""

    def test_column_width_with_string_letter(self, tmp_path):
        """Column width with string letter (e.g., 'A') is applied in preview."""
        ss = Spreadsheet(
            local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
        )
        ws = ss.worksheet('Sheet1')

        ws.write_values('A1', [['Value']])
        ws.set_column_width('A', 150)
        ws.flush()

        html_file = list(tmp_path.glob('*.html'))[0]
        content = html_file.read_text()

        assert 'width: 150px' in content

    def test_column_width_with_int_index(self, tmp_path):
        """Column width with int index (1-based) is applied in preview."""
        ss = Spreadsheet(
            local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
        )
        ws = ss.worksheet('Sheet1')

        ws.write_values('A1', [['Value']])
        ws.set_column_width(1, 200)  # Column 1 = A
        ws.flush()

        html_file = list(tmp_path.glob('*.html'))[0]
        content = html_file.read_text()

        assert 'width: 200px' in content

    def test_column_width_with_multi_letter_column(self, tmp_path):
        """Column width with multi-letter column (e.g., 'AA') works."""
        ss = Spreadsheet(
            local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
        )
        ws = ss.worksheet('Sheet1')

        # Write enough columns to reach AA
        row = [f'Col{i}' for i in range(27)]  # A through AA
        ws.write_values('A1', [row])
        ws.set_column_width('AA', 250)
        ws.flush()

        html_file = list(tmp_path.glob('*.html'))[0]
        content = html_file.read_text()

        assert 'width: 250px' in content


class TestPreviewNotes:
    """Tests for cell notes in preview mode."""

    def test_notes_appear_as_tooltips(self, tmp_path):
        """Notes appear as title attributes (tooltips) in preview."""
        ss = Spreadsheet(
            local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
        )
        ws = ss.worksheet('Sheet1')

        ws.write_values('A1', [['Data']])
        ws.set_notes({'A1': 'This is a note'})
        ws.flush()

        html_file = list(tmp_path.glob('*.html'))[0]
        content = html_file.read_text()

        assert 'title="This is a note"' in content
        assert 'has-note' in content

    def test_notes_html_escape_ampersand(self, tmp_path):
        """Notes with ampersand are HTML escaped."""
        ss = Spreadsheet(
            local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
        )
        ws = ss.worksheet('Sheet1')

        ws.write_values('A1', [['Data']])
        ws.set_notes({'A1': 'Tom & Jerry'})
        ws.flush()

        html_file = list(tmp_path.glob('*.html'))[0]
        content = html_file.read_text()

        assert 'Tom &amp; Jerry' in content

    def test_notes_html_escape_quotes(self, tmp_path):
        """Notes with quotes are HTML escaped."""
        ss = Spreadsheet(
            local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
        )
        ws = ss.worksheet('Sheet1')

        ws.write_values('A1', [['Data']])
        ws.set_notes({'A1': 'Say "Hello"'})
        ws.flush()

        html_file = list(tmp_path.glob('*.html'))[0]
        content = html_file.read_text()

        assert 'Say &quot;Hello&quot;' in content

    def test_notes_html_escape_angle_brackets(self, tmp_path):
        """Notes with angle brackets are HTML escaped."""
        ss = Spreadsheet(
            local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
        )
        ws = ss.worksheet('Sheet1')

        ws.write_values('A1', [['Data']])
        ws.set_notes({'A1': '<script>alert("xss")</script>'})
        ws.flush()

        html_file = list(tmp_path.glob('*.html'))[0]
        content = html_file.read_text()

        assert '&lt;script&gt;' in content
        assert '&lt;/script&gt;' in content
        # Original not present
        assert '<script>' not in content.replace('&lt;script&gt;', '')

    def test_notes_on_non_a1_cell(self, tmp_path):
        """Notes on cells other than A1 work correctly."""
        ss = Spreadsheet(
            local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
        )
        ws = ss.worksheet('Sheet1')

        ws.write_values('A1', [['A', 'B'], ['1', '2']])
        ws.set_notes({'B2': 'Note on B2'})
        ws.flush()

        html_file = list(tmp_path.glob('*.html'))[0]
        content = html_file.read_text()

        assert 'Note on B2' in content


class TestPreviewAccumulation:
    """Tests for preview history accumulation across multiple flushes."""

    def test_multiple_flushes_accumulate(self, tmp_path):
        """Multiple flush calls accumulate in preview history."""
        ss = Spreadsheet(
            local_preview=True, spreadsheet_name='Test', preview_dir=str(tmp_path)
        )
        ws = ss.worksheet('Sheet1')

        ws.write_values('A1', [['First']])
        ws.flush()

        ws.write_values('B1', [['Second']])
        ws.flush()

        html_file = list(tmp_path.glob('*.html'))[0]
        content = html_file.read_text()

        assert 'First' in content
        assert 'Second' in content
