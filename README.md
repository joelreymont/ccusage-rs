# ccusage-rs

Rust reimplementation of [ccusage](https://github.com/ryoppippi/ccusage), the Claude Code usage analyzer. The goal is a fast, fully offline CLI that reads local Claude Code JSONL logs, calculates token usage/costs, and presents daily/monthly/session/block reports (plus live monitoring) with JSON or table output.

## Status

- Planning phase; no functionality yet.
- See `PLAN.md` for milestones and architecture notes.

## Local Setup

```bash
rustup default stable           # ensure a stable toolchain
cargo run                       # placeholder message until CLI is built
```

## Creating the GitHub Repo

This directory is already `git init`-ed. To publish it, either:

- `gh repo create joel/ccusage-rs --public --source=. --remote=origin --push`
- or create an empty repo on GitHub, then:
  ```bash
  git remote add origin git@github.com:<your-username>/ccusage-rs.git
  git push -u origin main
  ```

## High-Level Goals

- Parse Claude Code JSONL logs from both legacy (`~/.claude/projects/`) and new (`~/.config/claude/projects/`) paths.
- Aggregate usage by day, month, session, and 5-hour billing blocks.
- Estimate costs per model (incl. cache create/read tokens) with offline pricing data.
- Provide JSON output plus human-friendly tables; compact mode for narrow terminals.
- Live monitoring/dashboard for active sessions and burn-rate projections.
- Config via CLI flags, env vars, and JSON config files with schema validation.

## References

- Original TypeScript repo: https://github.com/ryoppippi/ccusage
- Project plan: `PLAN.md`
