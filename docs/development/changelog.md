# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Core Types: CellLocation, WorksheetAsset, WorksheetDefinition (#23)
- WorksheetRegistry: Registry for worksheet definitions (#24)

## [0.3.9] - 2026-01-17

### Added

- Deploy documentation only on releases (#22)

## [0.3.8] - 2026-01-17

### Added

- Add cell value read methods to Worksheet (#21)

## [0.3.7] - 2026-01-17

### Added

- Open-ended range support for conditional formatting (#20)

## [0.3.6] - 2026-01-17

### Added

- Add custom timestamp for logging module (#19)

## [0.3.5] - 2026-01-17

### Added

- Add strip_comment_keys parameter to load_json_config (#18)

## [0.3.4] - 2026-01-17

### Added

- Add remove_comments utility function (#17)

## [0.3.3] - 2026-01-17

### Added

- Add reorder_worksheets method to Spreadsheet class (#16)

## [0.3.2] - 2026-01-16

### Fixed

- Fixing Formatting Bug (#15)

## [0.3.1] - 2026-01-15

### Fixed

- Resolving import (#14)

## [0.3.0] - 2026-01-14

### Added

- Adding contextmanager support for GoogleSheets (#13)

## [0.2.0] - 2026-01-14

### Added

- Adding contextmanager support (#12)

## [0.1.2] - 2026-01-13

### Changed

- Updating docs to remove status warning (#11)

## [0.1.1] - 2026-01-13

### Changed

- Updating docs (#10)

## [0.1.0] - 2026-01-13

### Added

- Implement S3FileSystem with moto tests (#2)
- Implement DuckDB wrapper with S3 integration and tests (#3)
- Add config utilities (load_json_config, setup_logging) with tests (#4)
- Implement Spreadsheet with batching + retries (no preview) and tests (#5)
- Add gsheets preview, docs/examples, finalize packaging, and decide on example_usage cleanup (#6)

### Changed

- Adjusting S3Object Class (#8)
- Prep package for publishing (#7)
- Updating readme for prod (#9)
