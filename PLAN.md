# ccusage-rs Project Plan

## Vision & Scope
- Rust rewrite of `ccusage`: analyze Claude Code JSONL logs locally to show token usage and estimated costs.
- Target parity with the original TypeScript CLI (daily/monthly/weekly/session/block reports, JSON + table output, live monitoring/statusline).
- Zero network requirement for normal operation; pricing data should be cached/bundled.

## Data & Requirements
- Inputs: Claude Code JSONL logs (new path `~/.config/claude/projects/`, legacy `~/.claude/projects/`). Support multiple projects/instances.
- Outputs: JSON and human-friendly tables; compact mode for narrow terminals.
- Reports: daily, weekly (configurable start day), monthly, sessions, 5-hour billing blocks; statusline and live modes.
- Costing: per-model pricing, including cache create/read tokens; offline pricing with optional refresh from bundled data.
- Config: CLI flags > local config file > user config > defaults (see `ccusage.example.json` in upstream repo for shape). Provide JSON schema for autocomplete.
- UX: responsive tables, timezone + locale aware, filters (`--since/--until`, project, instances).
- Non-goals (for now): GUI/daemon; modifying Claude data; remote telemetry.

## Architecture Outline
- `cli`: argument parsing, config merging, command dispatch (`clap`/`clap_complete`).
- `config`: load/merge config files (per-project/user), env vars, defaults; validate against schema.
- `pricing`: embedded pricing table + optional refresh; versioned data for reproducibility.
- `ingest`: discover Claude data dirs, read JSONL files (streaming), parse records into typed structs; handle legacy/new paths.
- `models`: domain types for events, sessions, aggregates, costs, time ranges.
- `aggregate`: reducers for daily/weekly/monthly/session/block; timezone-aware bucketing; cache token handling.
- `output`: table rendering (colors + compact layout) and JSON serializers.
- `live`: tail files + periodic recompute for blocks/statusline; clean cancellation.
- `telemetry`: none; ensure all processing is local/read-only.

## Proposed CLI Surface
- Default: `ccusage-rs` -> daily report.
- Subcommands: `daily`, `weekly`, `monthly`, `session`, `blocks`, `statusline`, `live` (or `blocks --live`).
- Common flags: `--json`, `--compact`, `--since/--until`, `--project`, `--instances`, `--timezone`, `--locale`, `--offline`, `--config <path>`.
- Blocks options: `--token-limit`, `--session-length`, `--active` (live mode).

## Milestones
1) **Scaffolding & Data Model**
   - Choose deps (`clap`, `serde/json`, `time`/`chrono`, `walkdir`, `rayon` optional, `tabled`/`comfy-table`, `notify` for live).
   - Define core types (usage event, model pricing, aggregates, time buckets).
   - Implement config loader/merger (config file + env + CLI).
   - Bundle pricing data (JSON asset) with versioning.

2) **MVP Reports**
   - File discovery for legacy/new Claude paths; allow override.
   - Streaming JSONL parse into typed events.
   - Daily/weekly/monthly aggregations with costs; `--json` and table output; compact mode.
   - Basic tests with fixture JSONL data + golden outputs.

3) **Sessions & Blocks**
   - Session grouping heuristics (per conversation ID) and summaries.
   - 5-hour billing blocks with active/inactive detection; projections.
   - Instance/project grouping and filtering.
   - Pricing breakdowns per model; cache create/read fields.

4) **Live & UX Polish**
   - Live monitoring (tail files, periodic recompute); statusline formatter.
   - JSON schema generation for config; completions (`clap_complete`).
   - Performance tuning (parallel IO, memory caps); large-file benchmarks.
   - Packaging: release builds, cross-compilation targets; checksum artifacts.

5) **Docs & Release**
   - CLI help + README usage examples mirroring upstream.
   - Troubleshooting guide (missing logs, timezone quirks).
   - Prepare v0.1 release notes and changelog.

## Testing Strategy
- Fixture JSONL datasets (small/medium/large) covering model mix, cache tokens, multiple projects, timezone edges.
- Golden tests for each report (JSON and table snapshots).
- Property tests for time bucketing and cost calculations.
- Integration tests for config precedence and CLI flag parsing.
- Benchmark harness for large logs and live mode tailing.

## Open Questions / To Clarify
- Exact Claude log schema (fields, session IDs); capture samples for fixtures.
- Pricing source of truth and update cadence.
- Session heuristics (timeout vs explicit IDs) and block activity rules.
- Minimum supported Rust version (MSRV) and release cadence.

## Repo Tasks (initial)
- [x] Initialize cargo binary crate and git repo.
- [x] Add pricing + log schema stubs and basic aggregations (daily/monthly/sessions).
- [x] Start config loader (JSON) and CLI flags for breakdown/blocks/statusline.
- [ ] Add lint/format configs (rustfmt, clippy).
- [ ] Add sample fixtures and tests.
