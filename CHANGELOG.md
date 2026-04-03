# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.7] - 2026-04-03

### Fixed
- Kafka source crashing with `UnicodeDecodeError` on messages containing non-UTF-8 bytes

## [0.3.6] - 2026-04-03

### Fixed
- BigQuery streaming v2 silently dropping large rows when all rows in a batch exceed `MAX_ROW_SIZE_BYTES`

## [0.3.5] - 2026-04-03

### Fixed
- Kafka source crashing on messages with unescaped control characters in JSON strings
- BigQuery streaming v2 crashing with `max_workers must be greater than 0` when batch list is empty

## [0.3.4] - 2026-04-02

### Fixed
- BigQuery streaming v2 crashing on unnested rows with dict/list values (e.g. `__schema` from Debezium CDC)

## [0.3.2] - 2026-03-04

### Fixed
- BigQuery incremental sync failing on first run when main table doesn't exist yet

## [0.3.1] - 2026-02-06

### Added
- Auto-load `.env` file when running `bizon run`
- `--env-file` CLI option to specify a custom env file path

## [0.3.0] - 2026-01-06

### Added
- Incremental sync support for Notion source
- BigQuery streaming destinations improvements
- Automated GitHub releases on tag push
- CHANGELOG.md for tracking changes

### Changed
- Removed `safe_cast_record_values` from BigQuery streaming destinations

## [0.2.0]

### Fixed
- Resolved merge conflicts with gorgias branch

## [0.1.0] - Initial Release

### Added
- Core EL framework with producer-consumer pattern
- Source abstraction with auto-discovery
- Destination abstraction with buffering
- Queue implementations: python_queue, kafka, rabbitmq
- Backend implementations: sqlite, postgres, bigquery
- Runner implementations: thread, process, stream
- CLI commands: `bizon run`, `bizon source list`, `bizon stream list`
- Built-in source connectors
- Built-in destination connectors
- Transform system for data transformations
- Cursor-based checkpointing for fault tolerance

[Unreleased]: https://github.com/bizon-data/bizon-core/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/bizon-data/bizon-core/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/bizon-data/bizon-core/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/bizon-data/bizon-core/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/bizon-data/bizon-core/releases/tag/v0.1.0
