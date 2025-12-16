# ccusage-rs Agent Notes

This repo uses the `bd` CLI (prefix `ccusage`) for lightweight issue tracking.

Quick commands:
- `bd list` – show issues
- `bd ready` – ready-to-work items
- `bd create "title" --description "details"` – new bead
- `bd close <id>` – close an issue
- `bd show <id>` – details

Repo setup:
- Initialized with `bd init --prefix ccusage`; database lives under `.beads/`.
- Sync branch: `master`.

If bd version complains, upgrade: `brew upgrade bd` or `bd upgrade`.
