"""Registry for worksheet definitions.

This module provides a centralized registry for managing worksheet definitions,
maintaining registration order and supporting ordered retrieval.

Example usage:
    from eftoolkit.gsheets import WorksheetRegistry
    from eftoolkit.gsheets.types import WorksheetDefinition

    # Register multiple worksheets at once (simplest approach)
    WorksheetRegistry.register([
        SummaryWorksheet(),
        RevenueWorksheet(),
        ExpensesWorksheet(),
    ])

    # Or register one at a time
    WorksheetRegistry.register(SummaryWorksheet())

    # Retrieve in registration order
    worksheets = WorksheetRegistry.get_ordered_worksheets()

    # Reorder worksheets at any time
    WorksheetRegistry.reorder(['Expenses', 'Summary', 'Revenue'])

    # Get a specific worksheet
    revenue = WorksheetRegistry.get_worksheet('Revenue')

    # Clear for testing
    WorksheetRegistry.clear()
"""

import threading

from eftoolkit.gsheets.types import WorksheetDefinition


class WorksheetRegistry:
    """Registry of worksheet definitions.

    A thread-safe registry that maintains worksheet definitions in registration order.
    Use `reorder()` to change the order after registration.

    This class uses class-level state, so all operations are performed via classmethods.
    Use `clear()` in tests to reset state between test cases.

    Example:
        >>> WorksheetRegistry.register([Summary(), Revenue(), Expenses()])
        >>> worksheets = WorksheetRegistry.get_ordered_worksheets()
        >>> [ws.name for ws in worksheets]
        ['Summary', 'Revenue', 'Expenses']

        # Or register one at a time:
        >>> WorksheetRegistry.register(SummaryWorksheet())
    """

    _worksheets: dict[str, WorksheetDefinition] = {}
    _order: list[str] = []
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def register(
        cls,
        worksheets: WorksheetDefinition | list[WorksheetDefinition],
    ) -> None:
        """Register one or more worksheet definitions.

        Args:
            worksheets: A single worksheet definition or a list of definitions.
                Must implement the WorksheetDefinition protocol. Worksheets are
                appended in the order provided.

        Raises:
            ValueError: If a worksheet with the same name is already registered.

        Example:
            >>> WorksheetRegistry.register(SummaryWorksheet())
            >>> WorksheetRegistry.register([Revenue(), Expenses()])
        """
        worksheet_list = worksheets if isinstance(worksheets, list) else [worksheets]
        with cls._lock:
            for worksheet in worksheet_list:
                if worksheet.name in cls._worksheets:
                    raise ValueError(
                        f"Worksheet '{worksheet.name}' is already registered"
                    )

                cls._worksheets[worksheet.name] = worksheet
                cls._order.append(worksheet.name)

    @classmethod
    def get_ordered_worksheets(cls) -> list[WorksheetDefinition]:
        """Return worksheets in registration order.

        Returns:
            List of worksheet definitions in the order they were registered,
            respecting any reordering done via `reorder()`.

        Example:
            >>> worksheets = WorksheetRegistry.get_ordered_worksheets()
            >>> for ws in worksheets:
            ...     print(ws.name)
        """
        with cls._lock:
            return [cls._worksheets[name] for name in cls._order]

    @classmethod
    def get_worksheet(cls, name: str) -> WorksheetDefinition | None:
        """Get a worksheet by name.

        Args:
            name: The name of the worksheet to retrieve.

        Returns:
            The worksheet definition if found, None otherwise.

        Example:
            >>> revenue = WorksheetRegistry.get_worksheet('Revenue')
            >>> if revenue:
            ...     assets = revenue.generate(config, context)
        """
        with cls._lock:
            return cls._worksheets.get(name)

    @classmethod
    def reorder(cls, names: list[str]) -> None:
        """Reorder worksheets to match the specified order.

        Sets the worksheet order to match the provided list of names.
        All registered worksheet names must be included exactly once.

        Args:
            names: List of worksheet names in the desired order.

        Raises:
            ValueError: If names don't match registered worksheets exactly
                (missing names, extra names, or duplicates).

        Example:
            >>> WorksheetRegistry.register([Summary(), Revenue(), Expenses()])
            >>> WorksheetRegistry.reorder(['Expenses', 'Summary', 'Revenue'])
            >>> [ws.name for ws in WorksheetRegistry.get_ordered_worksheets()]
            ['Expenses', 'Summary', 'Revenue']
        """
        with cls._lock:
            registered = set(cls._worksheets.keys())
            provided = set(names)

            if len(names) != len(provided):
                raise ValueError('Duplicate names in reorder list')

            missing = registered - provided
            if missing:
                raise ValueError(f'Missing worksheets in reorder: {missing}')

            extra = provided - registered
            if extra:
                raise ValueError(f'Unknown worksheets in reorder: {extra}')

            cls._order = list(names)

    @classmethod
    def clear(cls) -> None:
        """Clear all registered worksheets.

        This method is primarily intended for use in tests to reset
        the registry state between test cases.

        Example:
            >>> WorksheetRegistry.clear()
            >>> len(WorksheetRegistry.get_ordered_worksheets())
            0
        """
        with cls._lock:
            cls._worksheets = {}
            cls._order = []
