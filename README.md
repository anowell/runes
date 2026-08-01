# Runes

A local-first, CLI-driven issue tracker. Runes stores issues as markdown files with KDL frontmatter, backed by version control. No server, no web UI — just files, your editor, and your VCS.

A rune is just a file in a repo: `myapp/a3x--add-soft-deletes-to-billing.md`

```markdown
---
task myapp-a3x {
  status wip
  assignee anthony
  labels api billing
  dep myapp-q7m
}
---

# Add soft deletes to the billing models

## Description

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

Initialize runes:
- Creates global config and default store (first run only)
- Installs agent skill (~/.claude and/or ~/.agents)
- Creates project config

```bash
runes init --stealth
```

The `--stealth` flag keeps `runes.kdl` out of your repo's tracked files by adding it to `.git/info/exclude`.

Create your first rune, writing the description in your editor:

```bash
runes new "My first issue" -e
```

You get back an ID like `proj-a3x`, and saving records the rune. Or pipe the
content in:

```bash
echo "Some details" | runes new "My first issue" -f -
```

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
# Create an issue
runes new "Fix the login bug"

# Create and open in $EDITOR
runes new "Design the API" -e

# Create with metadata
runes new "Refactor auth" --status wip --label backend --assignee self --commit

# Create a milestone
runes new "v1 Release" --kind milestone

# Edit metadata
runes edit proj-a3x --status closed
runes edit proj-a3x --label urgent --assignee alice

# Edit body in $EDITOR
runes edit proj-a3x -e

# Replace body from file (- for stdin)
runes edit proj-a3x -f notes.md
```

### Browsing and filtering

```bash
# List open runes (the default view)
runes list

# Filter by status, assignee, kind
runes list --status wip --assignee self       # a state matches its substates too
runes list --status closed:canceled           # or filter on an exact substate
runes list --kind milestones

# Built-in views: open, mine, all, closed
runes list mine
runes list closed

# Full-text search titles and bodies (every status, including closed)
runes search login
runes search "auth flow" --with-archived

# Show a specific rune
runes show proj-a3x

# View change history
runes log proj-a3x
```

### Status

Every rune has a `status`: one of three core states, optionally refined by a
substate and written `state:substate`.

| State | Default substates |
|-------|-------------------|
| `todo` (not started) | any |
| `wip` (in progress) | `design`, `impl`, `review` |
| `closed` (terminal) | `done`, `canceled`, `duplicate` |

Filtering by a bare state includes its substates (`--status closed` matches
`closed:canceled`); filtering by `state:substate` is exact. The core states are
fixed; their substates are configurable:

```bash
runes config set state.wip.substate "design,impl,review,qa"
```

### Other operations

```bash
# Move a rune to another project
runes move proj-a3x --project otherproject

# Archive a rune
runes archive proj-a3x

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

