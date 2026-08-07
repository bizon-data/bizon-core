# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.2] - 2026-08-07

### Fixed

- **A `full_refresh` run resumed a job left `running` by a killed process, so the destination table was never republished.** An external kill (Kubernetes `activeDeadlineSeconds`, OOM, preemption) never gets to mark its job `failed`, so it always leaves a `running` row behind. Resuming is right for `incremental`, but a full refresh has nothing worth resuming: the run continued a stale item list, never reached the last iteration, and so never called `finalize()` — and because the job stayed `running`, the next run resumed it too. The table was served indefinitely at its last successfully published contents while every run reported success. Observed in production as a table stuck at week-old data through seven consecutive "successful" runs. `get_or_create_job()` now cancels a `running` job and starts a fresh one when the sync mode is `full_refresh`. Stream resets are unaffected: they are incremental jobs and keep their own `stream_resets` recovery contract.

- **A `full_refresh` run did not clear its staging table, so a crashed attempt's rows were published alongside the next attempt's.** Loads always `WRITE_APPEND` into `{table}_temp` and `finalize()` publishes it with a `WRITE_TRUNCATE` copy, but `_ensure_clean_temp_table()` only dropped a stale temp table for *reset* runs — even though a plain full refresh stages into the same table and publishes the same way. Seen alongside the issue above: 132k rows accumulated in `_temp` across seven killed attempts, with overlapping and missing cursor ranges. The guard now covers every run that publishes with `WRITE_TRUNCATE`. A plain incremental stages into `{table}_incremental` and still appends across runs, unchanged.

- **Concurrent pipelines sharing a schema crashed creating the state tables.** `create_all_tables()` inspects before creating, but inspect-then-create is not atomic: pipelines sharing a source (and so a dataset/schema) and starting on the same cron all saw a table missing, all issued `CREATE TABLE`, and every process but one died with `409 Already Exists` on BigQuery (`DuplicateTable` on Postgres). Losing that race is not an error, so it is now tolerated when the tables do exist afterwards, and still raised otherwise. This race was always present for every state table; it only became reachable when a table was missing, which is why it surfaced on the first run after 0.5.0 added `stream_resets`.

- **Resuming a job whose last cursor had no pagination failed with an unattributable error.** `create_destination_cursor()` stores a falsy pagination as SQL `NULL` rather than `"{}"`, while the reader called `json.loads()` on it unconditionally, producing `the JSON object must be str, bytes or bytearray, not NoneType` — naming neither the job, the stream, nor a remedy, and sending at least one investigation down an API/auth dead end. Such a job genuinely cannot be resumed (an empty pagination means the source was already exhausted, and handing it back to the source would restart it from the first page and re-fetch the whole stream), so it now fails with a message naming the job id, the stream, and how to recover.

## [0.5.1] - 2026-08-06

### Fixed

- **`pip install bizon` with no extras produced a CLI that could not start.** Every command, `bizon --help` included, failed with `ImportError: cannot import name 'bigquery' from 'google.cloud'`. `bizon.common.models` imports every destination config unconditionally (pydantic needs the classes to build the discriminated union), and the BigQuery config pulled in `table_naming` purely for a string constant — but that module imported `google.cloud.bigquery` at module level, so the whole CLI depended on the `bigquery` extra. This affected anyone using only the `file` or `logger` destination. The import is now made inside `resolve_default_table_id()`, which is the only thing that needs it and only runs when a BigQuery destination is actually writing. Present since well before 0.5.0.

## [0.5.0] - 2026-08-06

### Added

- **Stream reset for incremental syncs** — one run that re-fetches an incremental stream in full and *replaces* the destination table, after which incremental resumes from that run. Previously the only way to rebuild a drifted table was to delete backend rows by hand: the watermark (`last_run`, taken from the last succeeded job) had no escape hatch.

  Three ways to ask for one, all equivalent:

  ```bash
  bizon run config.yml --reset                    # one-shot, manual
  bizon stream reset config.yml                   # queued, consumed by the next run
  bizon stream reset config.yml --cancel          # withdraw it
  bizon stream reset config.yml --stream deals    # pick the stream, for templated configs
  ```

  ...or `source.reset: true` in the config. `bizon stream reset` records the request in the backend rather than running anything, so a pipeline whose command line is fixed by a scheduler picks it up with no change to its cron/Airflow job.

- Requests are **scoped to a single stream**, keyed on `(name, source_name, stream_name)` — the same triple as the watermark they override — so resetting one stream never affects another under the same pipeline name.

- During a reset the producer skips the watermark and calls `get()` instead of `get_records_after()`, and the run reaches destinations as `sync_mode: full_refresh` so they replace their table through their existing full-refresh path (for `bigquery`: staging into `{table}_temp`, then a `WRITE_TRUNCATE` copy job). The job row stays `incremental`, so the reset run becomes the next run's watermark. The request stays bound to the job running it, so a crashed reset is retried as a reset rather than silently degrading into an append.

- Only meaningful for `sync_mode: incremental` (ignored with a warning otherwise), and supported by every destination with a working full-refresh path. The exception is `bigquery_streaming`, which has no staging table and appends even on a full refresh, so a reset there is rejected at config validation instead of duplicating data.

### Changed

- `AbstractBackend` gains five `stream_resets` methods. Backends bundled with bizon implement them; an out-of-tree `AbstractBackend` subclass will need them added. Adds a `stream_resets` table, created automatically alongside the existing ones — no migration needed.

## [0.4.1] - 2026-06-29

### Fixed
- BigQuery batch destination (`bigquery`): metadata columns (`_source_record_id`, `_source_timestamp`, `_bizon_extracted_at`, `_bizon_loaded_at`, `_bizon_id`) were declared `REQUIRED` in the load-job schema, but BigQuery forbids promoting an existing `NULLABLE` column to `REQUIRED`. Load jobs against any table whose metadata columns were already `NULLABLE` failed with `400 Provided Schema does not match Table ... Field _source_record_id has changed mode from NULLABLE to REQUIRED`, breaking every affected pipeline (most visibly in `stream` mode, which loads directly into the final table). These columns are now `NULLABLE` (bizon always populates them, so only the BQ-level NOT NULL constraint is dropped, not data). Load jobs additionally set `ALLOW_FIELD_RELAXATION` and `ALLOW_FIELD_ADDITION` so any table created `REQUIRED` by the previous behavior self-heals on its next load without a manual migration.

## [0.4.0] - 2026-06-26

### Added
- **Secret manager references** for config: any string field can now reference a managed secret with a URI scheme instead of plaintext or an env var. Google Secret Manager is the first provider: `token: gsm://notion-api-token` resolves to the secret's latest version, `gsm://my-secret/versions/3` pins a version, and a full `gsm://projects/<p>/secrets/<id>/versions/<n>` resource path is used as-is. References also work **inline** inside a larger string, e.g. `dsn: "postgres://u:${gsm://db-pw}@host/db"`, and multiple `${...}` tokens per value are allowed. A built-in `env://VAR` scheme exposes environment variables the same way (and, unlike the legacy whole-value `BIZON_ENV_` prefix, works inline too) — `BIZON_ENV_` keeps working unchanged. Resolution happens once over the raw config before validation, so **no connector changes are needed** — sources/destinations keep reading plain strings. GSM uses Application Default Credentials (workload identity / ambient creds); set provider defaults under an optional top-level `secrets:` block (e.g. `secrets.gsm.project_id`). New optional dependency: `pip install 'bizon[secretmanager]'`. Validate references before a run with `bizon secrets check <config>`, which dry-runs every reference and reports resolved/failed with masked output.
- **GBIF occurrence source connector**: a no-auth, high-volume public API (occurrence search exposes ~3.9B rich ~2KB records via offset/limit pagination, capped at offset 100,000). Auto-discovered like other sources; `full_refresh` only. Useful as a load generator for benchmarking destination throughput where small sources like pokeapi can't fill a size-based buffer. Ships with a to-logger example config.
- BigQuery batch destination: `create_dataset` config flag (default `false`). When enabled, the dataset is created if it does not exist before the first write; when disabled, a missing dataset raises a clear error up front instead of failing load jobs with retried 404s mid-run. (The destination's `check_connection` was never invoked during a run, so datasets were not auto-created previously.)
- BigQuery batch destination: optional asynchronous, batched load jobs to raise throughput and cut the per-table load-job quota. Enable with `async_load: true`; tune with `load_files_per_job` (GCS files batched into one load job, default 10) and `load_max_in_flight_jobs` (back-pressure bound, default 3). Buffer flushes upload to GCS and submit load jobs without blocking, batching multiple files into fewer jobs and overlapping uploads with loads. Destination cursors are created only once a load lands and strictly in submission order, and a failed load aborts the run before any later cursor is written — so successful cursors always stay a contiguous prefix and recovery (`get_last_cursor_by_job_id`) resumes from the last good point and re-fetches the failed range, preserving the at-least-once contract with no lost records. Disabled by default (`async_load: false`) — no behavior change for existing pipelines.

### Changed
- BigQuery batch destination (`bigquery`) `finalize()` now publishes the temp table to the main table with a **copy job** (`copy_table`) instead of query DML. Full refresh uses `WRITE_TRUNCATE` and incremental uses `WRITE_APPEND`. Copy jobs are free (metadata-only) and near-instant, whereas the previous `CREATE OR REPLACE TABLE ... AS SELECT *` / `INSERT INTO ... SELECT *` scanned and rewrote the whole temp table and were billed on bytes processed. The incremental first run now also inherits the temp table's `_bizon_loaded_at` partitioning (previously lost by `CREATE TABLE AS SELECT`).
- BigQuery destinations (`bigquery`, `bigquery_streaming`, `bigquery_streaming_v2`) now prefix auto-generated table names with `_bizon_` so bizon-managed tables are clearly namespaced in shared datasets. This is **backwards compatible**: when no explicit `destination_id` is set, the destination first checks whether the legacy unprefixed table (`{source}_{stream}`) already exists — if it does, it keeps writing to it untouched, so existing pipelines are never disrupted; only brand-new tables get the `_bizon_` prefix. The lookup result is cached (one `get_table` call per run) and any non-`NotFound` error falls back to the legacy name. An explicit `destination_id` is never prefixed. Temp/staging tables (`_temp` / `_incremental`) inherit the resolved name. The prefix is configurable per destination via the new `table_prefix` field (default `_bizon_`; set to `""` to disable).

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

[Unreleased]: https://github.com/bizon-data/bizon-core/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/bizon-data/bizon-core/compare/v0.3.16...v0.4.0
[0.3.1]: https://github.com/bizon-data/bizon-core/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/bizon-data/bizon-core/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/bizon-data/bizon-core/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/bizon-data/bizon-core/releases/tag/v0.1.0
