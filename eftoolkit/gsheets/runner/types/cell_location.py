"""CellLocation type for specifying DataFrame locations within worksheets."""

from dataclasses import dataclass


@dataclass(frozen=True)
class CellLocation:
    """Where a DataFrame should be written within a worksheet.

    The worksheet name comes from the WorksheetDefinition.name property,
    so CellLocation only needs the cell address within that worksheet.

    Attributes:
        cell: The cell address where the DataFrame starts (e.g., 'B4', 'A1').

    Example:
        >>> location = CellLocation(cell='B4')
        >>> location.cell
        'B4'
    """

    cell: str
