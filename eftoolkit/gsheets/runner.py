"""DashboardRunner: 6-phase workflow orchestrator.

Orchestrates the complete workflow for updating Google Sheets dashboards:

1. Validate structure (worksheets exist, permissions)
2. Generate all DataFrames (pure data phase, no API calls)
3. Write all DataFrames to worksheets (I/O phase)
4. Apply formatting (formatting phase)
5. Run post-write hooks
6. Log summary

Example usage:
    from eftoolkit.gsheets import DashboardRunner, WorksheetRegistry

    # Define and register worksheets
    WorksheetRegistry.register([
        RevenueWorksheet(),
        ExpensesWorksheet(),
        SummaryWorksheet(),
    ])

    # Run the dashboard
    runner = DashboardRunner(
        config={'db': conn, 'sheet_name': 'Q1 Report'},
        credentials=load_json_config('credentials.json'),
    )
    runner.run()
"""

import logging
from typing import Any

from eftoolkit.config.utils import load_json_config
from eftoolkit.gsheets.registry import WorksheetRegistry
from eftoolkit.gsheets.sheet import Spreadsheet
from eftoolkit.gsheets.types import WorksheetAsset, WorksheetDefinition

logger = logging.getLogger(__name__)


class DashboardRunner:
    """Orchestrates the 6-phase sheet update workflow.

    Executes worksheet generation, writing, and formatting in a structured
    sequence with logging and error handling.

    Attributes:
        config: Configuration dictionary passed to worksheet generate() methods.
        credentials: Google service account credentials dictionary.
        context: Shared state dictionary populated during generation and available
            to subsequent worksheets.
        results: Dictionary mapping worksheet names to their generated assets.

    Example:
        >>> runner = DashboardRunner(
        ...     config={'db': conn, 'sheet_name': 'Q1 Report'},
        ...     credentials=credentials,
        ...     worksheets=[RevenueWorksheet(), ExpensesWorksheet()],
        ... )
        >>> runner.run()
    """

    def __init__(
        self,
        config: dict[str, Any],
        credentials: dict[str, Any],
        worksheets: list[WorksheetDefinition] | None = None,
        *,
        local_preview: bool = False,
    ) -> None:
        """Initialize DashboardRunner.

        Args:
            config: Configuration dictionary. Must include 'sheet_name' key.
                Passed to each worksheet's generate() method.
            credentials: Google service account credentials dictionary.
            worksheets: List of worksheet definitions to process. If None,
                uses worksheets from WorksheetRegistry.
            local_preview: If True, render to local HTML instead of Google Sheets.

        Raises:
            ValueError: If 'sheet_name' not in config.
            ValueError: If no worksheets provided and registry is empty.
        """
        if 'sheet_name' not in config:
            raise ValueError("config must include 'sheet_name' key")

        self.config = config
        self.credentials = credentials
        self.local_preview = local_preview

        if worksheets is not None:
            self.worksheets = worksheets
        else:
            self.worksheets = WorksheetRegistry.get_ordered_worksheets()

        if not self.worksheets:
            raise ValueError(
                'No worksheets provided. Pass worksheets parameter or register with WorksheetRegistry.'
            )

        self.context: dict[str, Any] = {}
        self.results: dict[str, list[WorksheetAsset]] = {}

    def run(self) -> None:
        """Execute the full 6-phase workflow.

        Phases:
            1. Validate structure
            2. Generate data
            3. Write data
            4. Apply formatting
            5. Run hooks
            6. Log summary

        Raises:
            Exception: Re-raises any exception from individual phases.
        """
        logger.info('Starting dashboard run for: %s', self.config['sheet_name'])

        self._phase_1_validate_structure()
        self._phase_2_generate_data()
        self._phase_3_write_data()
        self._phase_4_apply_formatting()
        self._phase_5_run_hooks()
        self._phase_6_log_summary()

        logger.info('Dashboard run complete')

    def _phase_1_validate_structure(self) -> None:
        """Phase 1: Validate sheet structure.

        Verifies:
        - Spreadsheet is accessible
        - Credentials are valid

        In local_preview mode, this phase is skipped.
        """
        logger.info('Phase 1: Validating structure')

        if self.local_preview:
            logger.info('  Skipping validation (local_preview mode)')
            return

        # Basic validation by opening the spreadsheet
        with Spreadsheet(
            credentials=self.credentials,
            spreadsheet_name=self.config['sheet_name'],
        ):
            logger.info('  Spreadsheet accessible: %s', self.config['sheet_name'])

    def _phase_2_generate_data(self) -> None:
        """Phase 2: Generate all DataFrames (no API calls).

        Calls generate() on each worksheet definition, storing results
        and populating context for dependent worksheets.
        """
        logger.info('Phase 2: Generating data')

        for worksheet_def in self.worksheets:
            logger.info('  Generating: %s', worksheet_def.name)
            assets = worksheet_def.generate(self.config, self.context)
            self.results[worksheet_def.name] = assets

            # Store in context for dependent worksheets
            self.context[worksheet_def.name] = {
                'assets': assets,
                'total_rows': sum(len(a.df) for a in assets),
                'asset_count': len(assets),
            }

        logger.info('  Generated %d worksheets', len(self.results))

    def _phase_3_write_data(self) -> None:
        """Phase 3: Write all DataFrames to worksheets.

        Opens the spreadsheet and writes each asset to its worksheet.
        Creates worksheets if they don't exist.
        """
        logger.info('Phase 3: Writing data')

        with Spreadsheet(
            credentials=self.credentials if not self.local_preview else None,
            spreadsheet_name=self.config['sheet_name'],
            local_preview=self.local_preview,
        ) as ss:
            for worksheet_def in self.worksheets:
                ws = ss.create_worksheet(worksheet_def.name, replace=True)

                for asset in self.results[worksheet_def.name]:
                    # Build format dict from file and/or inline config
                    format_dict = None
                    if asset.format_config_path:
                        format_dict = load_json_config(asset.format_config_path)
                    if asset.format_dict:
                        format_dict = {**(format_dict or {}), **asset.format_dict}

                    ws.write_dataframe(
                        df=asset.df,
                        location=asset.location.cell,
                        format_dict=format_dict,
                    )
                    logger.info(
                        '  Wrote %d rows to %s!%s',
                        len(asset.df),
                        worksheet_def.name,
                        asset.location.cell,
                    )

    def _phase_4_apply_formatting(self) -> None:
        """Phase 4: Apply worksheet-level formatting overrides.

        Calls get_format_overrides() on each worksheet definition.
        Currently logs the overrides; actual application is worksheet-specific.
        """
        logger.info('Phase 4: Applying formatting')

        for worksheet_def in self.worksheets:
            overrides = worksheet_def.get_format_overrides(self.context)
            if overrides:
                logger.info(
                    '  Format overrides for %s: %s', worksheet_def.name, overrides
                )

    def _phase_5_run_hooks(self) -> None:
        """Phase 5: Run post-write hooks.

        Executes post_write_hooks from each WorksheetAsset.
        """
        logger.info('Phase 5: Running hooks')

        hook_count = 0
        for worksheet_def in self.worksheets:
            for asset in self.results[worksheet_def.name]:
                for hook in asset.post_write_hooks:
                    logger.info(
                        '  Running hook for %s!%s',
                        worksheet_def.name,
                        asset.location.cell,
                    )
                    hook()
                    hook_count += 1

        logger.info('  Executed %d hooks', hook_count)

    def _phase_6_log_summary(self) -> None:
        """Phase 6: Log run summary."""
        logger.info('Phase 6: Summary')

        total_rows = 0
        total_assets = 0

        for worksheet_def in self.worksheets:
            assets = self.results[worksheet_def.name]
            rows = sum(len(a.df) for a in assets)
            total_rows += rows
            total_assets += len(assets)
            logger.info(
                '  %s: %d assets, %d total rows',
                worksheet_def.name,
                len(assets),
                rows,
            )

        logger.info(
            'Total: %d worksheets, %d assets, %d rows',
            len(self.worksheets),
            total_assets,
            total_rows,
        )
