# Runes

A local-first, CLI-driven issue tracker. Runes stores issues as markdown files with KDL frontmatter, backed by version control. No server, no web UI — just files, your editor, and your VCS.

A rune is just a file in a repo: `myapp/a3x--add-soft-deletes-to-billing.md`

```markdown
---
task "myapp-a3x" {
  status "wip"
  assignee "anthony"
  labels "api" "billing"
  dep "myapp-q7m"
}
---

# Add soft deletes to the billing models

## Summary

Hard-deleting subscriptions and invoices breaks audit trails and makes
support debugging impossible after cancellation. Add a deleted_at
timestamp and filter them from default queries so the app behaves the
same but the data survives.

## Acceptance

- [ ] Deleted records excluded from all default query scopes
- [ ] Admin API can still retrieve soft-deleted records
- [ ] Existing DELETE endpoints set deleted_at instead of removing rows
```

That's the entire issue — metadata, context, and acceptance criteria in one portable, version-controlled, greppable file. No database, no API, no sync lag.

## Install

```bash
cargo install --git https://github.com/anowell/runes runes
```

## Getting Started

Initialize runes (first run creates a global config and default store):

```bash
runes init --stealth
```

The `--stealth` flag keeps `runes.kdl` out of your repo's tracked files by adding it to `.git/info/exclude`.

Create your first rune, writing the description in your editor:

```bash
runes new "My first issue" -e
```

You get back an ID like `myproject-a3x`, and saving records the rune. Or pipe the
content in:

```bash
echo "Some details" | runes new "My first issue" -f -
```

Without `-e`/`-f`/`--commit`, `runes new` prints the ID and the absolute path of
the markdown file it created and leaves it a draft — the flow AI agents use, since
they can write the file directly:

```bash
runes new "My first issue"
$EDITOR /path/from/the/output.md
runes diff myproject-a3x     # what is pending
runes commit myproject-a3x   # record just this rune
```

A rune you never committed is only a file, so `runes delete <id>` discards it
outright — no `--force`, no trace in the log.

`runes quickstart` prints a guide to everything below, generated from the build
you have and describing this machine's stores and schema. It comes in a human
variant and an agent variant (`--human` / `--agent`; an agent in the environment
selects the agent one) — `runes init` installs the agent variant as a skill for
Claude Code and friends.

Scripts can skip the text output entirely — `runes new "..." --json` prints
`{"id": ..., "path": <absolute>, "committed": <bool>}`.

List your runes:

```bash
runes list
```

## Usage Guide

### Creating and editing runes

```bash
# Create an issue: prints the id and the doc path (--json for {id, path, committed})
runes new "Fix the login bug"
runes commit myproject-a3x            # after editing the printed file

# Create and open in $EDITOR (-e and -f commit on their own; --no-commit opts out)
runes new "Design the API" -e

# Create with metadata, committing immediately
runes new "Refactor auth" --status wip --label backend --assignee self --commit

# Create a milestone
runes new "v1 Release" --kind milestone

# Edit metadata
runes edit myproject-a3x --status closed
runes edit myproject-a3x --label urgent --assignee alice

# Edit body in $EDITOR
runes edit myproject-a3x -e

# Replace body from file or stdin
runes edit myproject-a3x -f notes.md
cat updated.md | runes edit myproject-a3x -f -

# Edit a whole doc: input starting with a `---` frontmatter block replaces both
# metadata and body (the id must match; `runes new -f` gets a fresh id instead).
# `show` output round-trips as-is — the fields, dep statuses, edit annotations and
# comment attributions it adds for display are dropped on the way back in.
runes show myproject-a3x > doc.md
runes edit myproject-a3x -f doc.md

# Field flags combine with -f and override whatever the file says
runes edit myproject-a3x -f doc.md --status closed
```

### Browsing and filtering

```bash
# List open runes (the default view)
runes list

# Filter by state, assignee, kind
runes list --status wip --assignee self       # matches wip:review too
runes list --status closed:canceled           # or an exact substate
runes list --kind milestones

# Built-in views: open, mine, all, closed
runes list mine
runes list closed
runes list --all      # same as `runes list all`

# Full-text search titles and bodies (all states, including closed)
runes search login
runes search "auth flow" --project '' --with-archived

# Show a specific rune
runes show myproject-a3x

# View change history
runes log myproject-a3x
```

### States

Every rune is in one of three core states, optionally refined by a substate
written as `state:substate`:

| State | Meaning |
|-------|---------|
| `todo` | ready work |
| `wip` | in progress — `wip:design`, `wip:impl`, `wip:review` |
| `closed` | terminal; bare `closed` and `closed:done` mean completed, `closed:canceled` and `closed:duplicate` are the exceptions |

Filtering by a core state includes its substates (`--status closed` matches
`closed:canceled`); filtering by `state:substate` is exact. Core states are
fixed; the allowed substates are configurable:

```bash
runes config set state.wip.substate "design,impl,review,qa"
```

`done` and `in-progress` are still accepted wherever a status is taken, and are
rewritten to `closed` and `wip`. `runes store doctor <store>` migrates a store
written with the old vocabulary.

### Other operations

```bash
# Move a rune to another project
runes move myproject-a3x --project otherproject

# Archive a rune
runes archive myproject-a3x

# Sync store with remote
runes sync
```

### Configuration

Runes uses KDL config files (`runes.kdl`) at two levels:

- **Global** (`~/.runes/config.kdl`) — user identity, stores, default queries
- **Local** (per-repo `runes.kdl`) — project defaults, path bindings

`runes init` creates both. The local config sets `defaults.project` so commands like `runes new` know which project to target.

Read and write config values with `runes config`:

```bash
runes config get defaults.project
runes config set new.task.assignee self
runes config list
runes config list --global
```

See [docs/configuration.md](docs/configuration.md) for the full configuration reference.

### Stores

A store is a VCS-backed repository that holds your runes. Runes supports `jj` (Jujutsu) and `pijul` backends, and the backend's binary has to be installed.

`runes init` creates the first store for you, so `store init` is only needed for a second one.

```bash
# List configured stores (* marks the default)
runes store list

# Add a new store (--backend defaults to jj, --path to ~/.runes/stores/<name>)
runes store init mystore --backend pijul

# Rebuild the query cache and migrate legacy statuses
runes store doctor mystore
```

## Document Format

Rune docs are markdown files with KDL frontmatter:

```markdown
---
task "myproject-a3x" {
  status "todo"
  assignee "alice"
  labels "backend" "urgent"
  dep "myproject-b2f"
}
---

# Fix the login bug

## Summary

The login page throws a 500 when...
```

Files are named `<id>--<slug>.md` (e.g. `a3x--fix-the-login-bug.md`). The ID is canonical; the slug is for readability and updates automatically on title changes.

## License

MIT
