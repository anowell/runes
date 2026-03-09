# Runes CLI Agent Guide

This repo models the entire rune lifecycle. Always drive changes through the `runes` CLI and let the tooling manage canonical files inside the configured stores. Use this guide to stay aligned with the expected flow and the documentation maintained under `docs/`.

## Key references (read before acting)
- `README.md`: quick overview of the runnable commands, store topology, and developer expectations.
- `docs/framing.md`: explains the vision and guiding principles that the CLI and workflows should honor.
- `docs/schema.md`: defines the KDL frontmatter structure, ID naming rules, and directory layout that every rune must obey.
- `docs/milestones.md`: describes the milestone-driven roadmap (e.g., CLI core, dual-backend, migration discipline) so you can place runes correctly.
- `docs/backend-sdk-plan.md`: tracks which backend operations already run via `libpijul`/`jj-lib` and which ones are still CLI-backed; reference it when touching backend behavior.

## Standard operating environment
- Default store: `proj` located at `~/.runes/stores/proj` (a `pijul` repo).
- Target project: `runes` inside that store (path `~/.runes/stores/proj/runes`).
- Always build the CLI before invoking commands, for example `cargo build` or `cargo run -p runes -- help` to ensure CLI behavior matches the code you edit.

## CLI-first rune lifecycle (follow this order)
1. Run `runes list --store proj --project runes` to see existing runes and avoid duplication. If you need backend context, run `runes backend status proj` or `runes backend capabilities proj` first.
2. Create new runes exclusively through the CLI: `runes new --project runes "Title" --store proj [--status <state>] [--label <label>] [--type issue|milestone]`. If `--project` is omitted the CLI uses `RUNES_PROJECT`, `default_project`, the current directory name, or the repo root name (in that order) to infer the target project, then lets `runes new` pick the canonical filename and metadata.
3. After `runes new` returns, rerun `runes list...` to confirm the ID and path generated inside `~/.runes/stores/proj/runes`. Capture the file path reported so future edits happen on that doc.
4. Use CLI helpers to change metadata when possible (`runes issue edit`, `runes issue move`, etc.). Only open the generated markdown if you must add or remove sections that the CLI does not cover yet.
5. When a change requires editing the underlying backend behavior, coordinate with `runes backend ...` commands (`status`, `log`, `sync`, `probe-sdk`), and mention in the rune body whether the feature remains CLI-backed (see `docs/backend-sdk-plan.md`).

## Documentation discipline
- Never author or modify rune files directly under `docs/runes/` (those are stores' mirrors, not canonical). All canonical rune files live in `~/.runes/stores/proj/runes/...` and are managed via the CLI.
- Refer to `docs/schema.md` when editing runes to ensure KDL frontmatter fields (id, status, labels, relations, links) remain consistent.
- If you need to describe backend capabilities or migration gaps, mention them in the rune body and cite `docs/backend-sdk-plan.md` so future readers know why the CLI flow differs from SDK-backed code.

## Backend context
- Keep `docs/backend-sdk-plan.md`, `core/src/backend.rs`, and `core/src/backend/*.rs` aligned: explain in runes when you add SDK-backed functionality and when you fall back to CLI operations.
- Run `runes backend status proj` and `runes backend capabilities proj` after backend code changes to confirm capability tokens shift from `cli` to `sdk` where expected.

## Tips & reminders
- Always connect new functionality to the milestone roadmap in `docs/milestones.md`. When crafting a rune, mention which milestone it belongs to (e.g., M04 Dual Backend) so reviewers can assess scope in context.
- Keep updates focused: avoid large manual patches in `core/` that reimplement behavior already covered by Pijul or Jujutsu SDK crates; prefer wiring existing abstractions where the docs already call them out.
