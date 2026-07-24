# Runes CLI Agent Guide

This repo *is* the `runes` CLI. It also tracks its own work with runes, in the
`rn` project (see `runes.kdl`).

## Using runes

Run `runes quickstart` — it is generated from the live build, so it always
describes the current commands, states, and views, plus this machine's stores
and paths. `runes init` installs the same guide as an agent skill
(`~/.claude/skills/runes/SKILL.md`, `~/.agents/skills/runes/SKILL.md`), minus
anything machine-specific: those files are global, so their text depends only
on the binary. Every `runes init` refreshes them; a hand-edited skill is left
alone until `runes init --force-skill`.

Rune docs are markdown files with KDL frontmatter, and editing them directly is
expected: change the file, `runes diff <id>` to review, `runes commit <id>` to
record just that rune.

## Working on the CLI

- Build and test with `cargo build` / `cargo test`. `just fix` runs
  `cargo clippy --fix` then `cargo fmt`.
- `cli/src/main.rs` holds the commands; `core/` holds the model, schema, cache,
  and the `jj` and `pijul` backends.
- Quickstart text is the installed skill, so keep it accurate and terse, and
  keep environment-dependent lines behind `QuickstartMode::Live`.

## Docs

- `README.md`: overview and install.
- `docs/framing.md`: vision and guiding principles.
- `docs/schema.md`: KDL frontmatter, ID rules, directory layout.
- `docs/configuration.md`: global and local config keys.
