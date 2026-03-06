# Runes

A local-first, CLI-driven issue tracker. Runes stores issues as markdown files with KDL frontmatter, backed by version control. No server, no web UI — just files, your editor, and your VCS.

A rune is just a file in a repo: `myapp/a3x--add-soft-deletes-to-billing.md`

```markdown
---
task "myapp-a3x" {
  status "in-progress"
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

Create your first rune:

```bash
runes new "My first issue"
```

That's it. A markdown file is created, committed to your store, and you get back an ID like `myproject-a3x`.

Want to write a description right away? Open it in your editor:

```bash
runes new "My first issue" -e
```

Or pipe content in:

```bash
echo "Some details" | runes new "My first issue" -f -
```

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
runes new "Refactor auth" --status in-progress --label backend --assignee self

# Create a milestone
runes new "v1 Release" --type milestone

# Edit metadata
runes edit myproject-a3x --status done
runes edit myproject-a3x --label urgent --assignee alice

# Edit body in $EDITOR
runes edit myproject-a3x -e

# Replace body from file or stdin
runes edit myproject-a3x -f notes.md
cat updated.md | runes edit myproject-a3x -f -
```

### Browsing and filtering

```bash
# List all runes (uses default query if configured)
runes list

# Filter by status, assignee, type
runes list --status todo --assignee self
runes list --type milestones

# Use a saved query
runes list mine

# Show a specific rune
runes show myproject-a3x

# View change history
runes log myproject-a3x
```

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

A store is a VCS-backed repository that holds your runes. Runes supports `jj` (Jujutsu) and `pijul` backends.

```bash
# List configured stores
runes store list

# Add a new store
runes store init mystore --backend jj

# Rebuild the query cache
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
