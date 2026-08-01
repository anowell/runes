# Runes CLI Agent Guide

This repo *is* the `runes` CLI. It also tracks its own work with runes, in the
`rn` project (see `runes.kdl`).

## Using runes

Run `runes quickstart` — it is generated from the live build, so it always
describes the current commands, states, and views, plus this machine's stores
and paths. It comes in two variants: the agent guide (path + `commit <id>`
flow, `--json` contracts) and the human guide (`$EDITOR`-driven). A detected
agent gets the agent one, so you will; `--human` / `--agent` force it either
way. `runes init` installs the agent guide as a skill
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
- Vocabulary: a *status* is a *state* (`todo`, `wip`, `closed`) plus an optional
  *substate*, written `state:substate`. Keep docs, help text, errors and
  comments to that split.
- Quickstart text is the installed skill, so keep it accurate and terse. Keep
  environment-dependent lines behind `QuickstartMode.live`, and audience-
  specific ones behind its `audience` (editor-driven advice is human-only,
  `--json` and doc paths lead in the agent variant).

## Docs

- `README.md`: overview and install.
- `docs/framing.md`: vision and guiding principles.
- `docs/schema.md`: KDL frontmatter, ID rules, directory layout.
- `docs/configuration.md`: global and local config keys.
