# Runes Schema

## Core Concepts

- **Store**: an independent VCS-backed repository (e.g. `proj`, `work`). Backend can be `jj` or `pijul`.
- **Project**: a directory inside a store. Creating a directory creates a project.
- **Rune doc**: a markdown file with KDL frontmatter. Can be a task or milestone.
- **Ref**: canonical identifier using `<store>:<project-id>` syntax (e.g. `proj:runes-cx3`).

## Store Layout

```text
~/.runes/stores/
  proj/                               # store repo
    myproject/                        # project directory
      _archive/                       # archived runes
      a3x--fix-login-bug.md          # task (id: myproject-a3x)
      m01--v1-release/                # milestone directory
        _milestone.md                 # milestone doc (id: myproject-m01)
        b2f--design-api.md           # child task
```

Cache (not canonical, rebuildable):

```text
~/.runes/cache/
  <store>.sqlite
```

## File Naming

Task files:

```text
<short-id>--<slug>.md
```

- The ID is canonical; the slug is for readability.
- The CLI auto-renames the slug on title changes.
- Resolution works even if the slug is stale.

Milestone containers:

```text
<short-id>--<slug>/
  _milestone.md
```

Child tasks live alongside `_milestone.md` in the milestone directory.

## ID Generation

IDs are project-scoped: `<project>-<short>` (e.g. `myproject-a3x`).

Default strategy: 3-character base32 random suffix. Configurable to sequential numeric (e.g. `myproject-104`).

## Document Format

All rune docs are markdown with KDL frontmatter in a node block:

```markdown
---
task "myproject-a3x" {
  status "todo"
  assignee "alice"
  labels "backend" "urgent"
  milestone "myproject-m01"
  relations {
    blocks "myproject-b2f"
  }
  dep "myproject-c1z"
}
---

# Fix the login bug

## Summary

Description of the issue...

## Design

Technical approach...

## Acceptance

- [ ] Criteria...

## Comments

Discussion and notes...
```

### Frontmatter fields

The top-level node declares the doc type and ID:

- `task "<id>" { ... }` — a task/issue
- `milestone "<id>" { ... }` — a milestone

Inside the block:

| Field | Required | Description |
|-------|----------|-------------|
| `status` | yes | Current state, `state` or `state:substate` (e.g. `"todo"`, `"wip:review"`, `"closed:canceled"`) |
| `assignee` | no | Assigned user, by email (see below) |
| `labels` | no | Space-separated quoted strings |
| `milestone` | no | Parent milestone ID |
| `relations` | no | Block of typed relations (e.g. `blocks`, `related`) |
| `dep` | no | Dependency ID (repeatable) |

A user field holds the canonical identity — an email address, or a bare handle
when there is no email. Passing `--assignee "Ana Ruiz <ana@example.com>"` writes
`assignee "ana@example.com"` and records the name in the store's `.mailmap`;
`runes show` renders it back per `user.format`. See
[configuration.md](configuration.md#names-and-emails).

### Body conventions

The `# Title` heading is the source of truth for the document title.

The default body template for all kinds is:
- `## Description`
- `## Acceptance`

Custom kind templates can be placed at `<store>/<project>/.kinds/<kind>.md`
or `<store>/.kinds/<kind>.md` to override the default.

### States

A status is one of three fixed core states with an optional substate:

| State | Terminal | Default substates |
|-------|----------|-------------------|
| `todo` | no | any |
| `wip` | no | `design`, `impl`, `review` |
| `closed` | yes | `done`, `canceled`, `duplicate` (bare `closed` also means completed) |

Core states are not configurable and are not part of `.kinds/schema.kdl` — a
`status` or `terminal` declaration there is ignored. Allowed substates come from
`runes.kdl` (see [configuration.md](configuration.md)). `done` and `in-progress`
are accepted on input and rewritten to `closed` and `wip`; `runes store doctor`
migrates stores written with the old vocabulary.

## Hierarchy

- Milestone containers are directories with a `_milestone.md` control doc.
- Child rune docs live in the same directory.
- Parent completion can be inferred from child status (policy configurable).
- Archive moves the directory to `<project>/_archive/`.
