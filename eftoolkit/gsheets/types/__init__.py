"""Type definitions for Google Sheets operations.

This module provides cell types and type aliases for worksheet operations.
These types are intentionally separate from the runner module to avoid
circular import issues.
"""

from eftoolkit.gsheets.types.cell_location import CellLocation
from eftoolkit.gsheets.types.cell_range import CellRange

# Type alias for single cell parameters that accept CellLocation or string
CellType = CellLocation | str

# Type alias for range parameters that accept CellLocation, CellRange, or string
RangeType = CellLocation | CellRange | str

__all__ = ['CellLocation', 'CellRange', 'CellType', 'RangeType']
