# Runes

Runes is a CLI-first, markdown-first, VCS-backed work management system.

- Canonical records are markdown files with KDL frontmatter.
- SQLite is a rebuildable query cache, never source of truth.
- Cache files are stored at `~/.runes/cache/<store>.sqlite`.
- Store refs use `<store>:<project-id>` (example: `proj:runes-cx3`).

See:

- `docs/framing.md`
- `docs/schema.md`
- `docs/milestones.md`
- `docs/backend-sdk-plan.md`

## Build

```bash
cargo build
```

## Test

```bash
cargo test
```

- Integration coverage lives in `cli/tests/workflows.rs` and validates:
- `jj` issue lifecycle (create/edit/list/log).
- milestone hierarchy/progress behavior.
- `pijul` issue lifecycle plus SDK-backed backend observability commands.
- `pijul` cross-store move behavior (source removal + target add).

## CLI

```bash
cargo run -p runes -- store list
```

- Core commands:

- `store init`, `store list`, `store info`, `store remove`, `store doctor`
- `project create`
- `issue new`, `issue new-milestone`, `issue show`, `issue list`, `issue edit`, `issue move`
- `issue archive`, `issue log --section`
- `issue check`, `milestone progress`
- `list` (`runes list --project <store:project> [--type <issues|milestones>] [--status <status>]`)
- `backend status`, `backend adapter`, `backend capabilities`, `backend log`, `backend sync`

`store doctor <store>` rebuilds the sqlite query cache and can be used anytime you suspect the index needs refreshing.

`runes new` now accepts `--project <store:project>` and, when omitted, follows the default project inference described in `docs/configuration.md`.

Use `just cli ...` for quick exploration; `just cli --help` now prints the same usage as `runes --help`.

## Current Bootstrap State

- `~/.runes/workspaces/proj` is configured as a `pijul` store.
- `~/.runes/workspaces/how` is configured as a `jj` colocated store.
