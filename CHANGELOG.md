# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- BigQuery batch destination (`bigquery`) `finalize()` now publishes the temp table to the main table with a **copy job** (`copy_table`) instead of query DML. Full refresh uses `WRITE_TRUNCATE` and incremental uses `WRITE_APPEND`. Copy jobs are free (metadata-only) and near-instant, whereas the previous `CREATE OR REPLACE TABLE ... AS SELECT *` / `INSERT INTO ... SELECT *` scanned and rewrote the whole temp table and were billed on bytes processed. The incremental first run now also inherits the temp table's `_bizon_loaded_at` partitioning (previously lost by `CREATE TABLE AS SELECT`).
- BigQuery destinations (`bigquery`, `bigquery_streaming`, `bigquery_streaming_v2`) now prefix auto-generated table names with `_bizon_` so bizon-managed tables are clearly namespaced in shared datasets. This is **backwards compatible**: when no explicit `destination_id` is set, the destination first checks whether the legacy unprefixed table (`{source}_{stream}`) already exists — if it does, it keeps writing to it untouched, so existing pipelines are never disrupted; only brand-new tables get the `_bizon_` prefix. The lookup result is cached (one `get_table` call per run) and any non-`NotFound` error falls back to the legacy name. An explicit `destination_id` is never prefixed. Temp/staging tables (`_temp` / `_incremental`) inherit the resolved name. The prefix is configurable per destination via the new `table_prefix` field (default `_bizon_`; set to `""` to disable).

### Added
- BigQuery batch destination: `create_dataset` config flag (default `false`). When enabled, the dataset is created if it does not exist before the first write; when disabled, a missing dataset raises a clear error up front instead of failing load jobs with retried 404s mid-run. (The destination's `check_connection` was never invoked during a run, so datasets were not auto-created previously.)
- BigQuery batch destination: optional asynchronous, batched load jobs to raise throughput and cut the per-table load-job quota. Enable with `async_load: true`; tune with `load_files_per_job` (GCS files batched into one load job, default 10) and `load_max_in_flight_jobs` (back-pressure bound, default 3). Buffer flushes upload to GCS and submit load jobs without blocking, batching multiple files into fewer jobs and overlapping uploads with loads. Destination cursors are created only once a load lands and strictly in submission order, and a failed load aborts the run before any later cursor is written — so successful cursors always stay a contiguous prefix and recovery (`get_last_cursor_by_job_id`) resumes from the last good point and re-fetches the failed range, preserving the at-least-once contract with no lost records. Disabled by default (`async_load: false`) — no behavior change for existing pipelines.

### Fixed
- BigQuery streaming destinations (`bigquery_streaming`, `bigquery_streaming_v2`) now pass `exists_ok=True` when creating the dataset in `check_connection`, avoiding a 409 `Conflict` when multiple workers race to create the same missing dataset.

## [0.3.16] - 2026-04-20

### Fixed
- Cursor resume crashed with `ValueError: malformed node or string on line 1: <ast.Name object>` whenever the stored pagination dict contained a boolean or `None`. Pagination is written to the backend via `json.dumps`, but the producer was reading it back with `ast.literal_eval`, which can't parse JSON's `true` / `false` / `null` (they become `ast.Name` nodes). Connectors like Notion (`has_more`, `data_sources_loaded`) and Cycle (GraphQL `pageInfo.hasNextPage`) could never resume — the first retry would hit this error and the pod would exit with `BACKEND_ERROR`. Switched the producer to `json.loads`, matching the write path.

## [0.3.15] - 2026-04-17

### Fixed
- Kafka rebalance storm, take 2: 0.3.14's "log-and-continue on ILLEGAL_GENERATION" was incorrect — librdkafka did not auto-rejoin after a commit-time generation failure, so the consumer stayed permanently in a stale generation and every subsequent commit failed indefinitely. Reverted to the 0.3.13 recreate-in-place behavior AND added **static membership** (KIP-345): `group.instance.id` is now derived from the `HOSTNAME` env var (auto-populated by Kubernetes to the pod name). With a stable instance ID, the close+recreate on `ILLEGAL_GENERATION` reconnects as the same member instead of joining as a new one, so the broker skips the full-group rebalance. This breaks the cascade that made one pod's eviction invalidate all 15 others. Users can override by setting `group.instance.id` explicitly in `consumer_config`.

## [0.3.14] - 2026-04-17

### Fixed
- Kafka consumer rebalance storm: on `ILLEGAL_GENERATION` / `UNKNOWN_MEMBER_ID` during commit, the source no longer closes and recreates the Consumer. Closing sends `LeaveGroup`, which triggers a group-wide rebalance that invalidates every other consumer's generation, causing them to recreate too — a self-sustaining cascade observed in production with 16 replicas evicting in perfect millisecond-synchrony every ~35s. librdkafka's group state machine already handles the rejoin automatically on the next `consume()` call, preserving `member.id` and keeping the rest of the group undisturbed. Commit errors are now log-and-continue for this error class.

## [0.3.13] - 2026-04-17

### Changed
- Kafka source no longer crashes the streaming pipeline when commit fails with `ILLEGAL_GENERATION` / `UNKNOWN_MEMBER_ID` (consumer evicted from the group). Previously the source closed the consumer and re-raised, causing the runner to exit with `SOURCE_ERROR` and the pod to be restarted by Kubernetes. The source now closes the evicted consumer, recreates a fresh one in place, and returns — the next iteration's `subscribe()` / `assign()` rejoins the group cleanly. Uncommitted records from the failed batch may be reprocessed by the new partition owner, which is consistent with Bizon's at-least-once delivery contract.

## [0.3.12] - 2026-04-17

### Fixed
- Streaming runner crashed with a secondary `AttributeError: ERROR` when `source.commit()` raised, because `PipelineReturnStatus.ERROR` does not exist on the enum. The error branch now uses `PipelineReturnStatus.SOURCE_ERROR`, matching the convention used in `producer.py`, so the runner reports a clean Failure status instead of hiding the real commit error behind an enum lookup.

## [0.3.11] - 2026-04-17

### Fixed
- Kafka source now closes the consumer and exits when commit fails with `ILLEGAL_GENERATION` or `UNKNOWN_MEMBER_ID` (consumer evicted from the group). Previously these errors were silently swallowed, causing the evicted consumer to keep processing and duplicating writes with the new partition owner until the next rebalance. The pod now exits cleanly and Kubernetes restarts it with a fresh consumer that rejoins the group.

## [0.3.9] - 2026-04-07

### Fixed
- Reverted the streaming runner ThreadPoolExecutor changes from 0.3.8: the intended `consume(N+1)`/`write(N)` overlap was not actually achieved (the loop waited for the previous write before consuming the next batch), adding thread overhead and a one-iteration commit delay without throughput gain. Restored the simple sequential loop. The `max.poll.interval.ms=600000` Kafka consumer default introduced in 0.3.8 is retained as a safety net.

## [0.3.8] - 2026-04-03

### Fixed
- Kafka consumer stalling due to rebalance by overlapping consume and BigQuery writes in separate threads
- Added `max.poll.interval.ms` default (10 min) to Kafka consumer config

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
