# Runes Schema

## Core Concepts

- **Store**: an independent VCS-backed repository (e.g. `proj`, `work`). Backend can be `jj` or `pijul`.
- **Project**: a directory inside a store. Creating a directory creates a project.
- **Rune doc**: a markdown file with KDL frontmatter. Can be a task or milestone.
- **Ref**: canonical identifier using `<store>:<project-id>` syntax (e.g. `proj:runes-cx3`).

## Store Layout

```text
~/.runes/workspaces/
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
| `status` | yes | Current status (e.g. `"todo"`, `"in-progress"`, `"done"`) |
| `assignee` | no | Assigned user |
| `labels` | no | Space-separated quoted strings |
| `milestone` | no | Parent milestone ID |
| `relations` | no | Block of typed relations (e.g. `blocks`, `related`) |
| `dep` | no | Dependency ID (repeatable) |

### Body conventions

The `# Title` heading is the source of truth for the document title.

Recommended sections for tasks:
- `## Summary`
- `## Design`
- `## Acceptance`
- `## Comments`

Recommended sections for milestones:
- `## Goal`
- `## Exit Criteria`
- `## Scope`
- `## Risks`
- `## Tracking`

## Hierarchy

- Milestone containers are directories with a `_milestone.md` control doc.
- Child rune docs live in the same directory.
- Parent completion can be inferred from child status (policy configurable).
- Archive moves the directory to `<project>/_archive/`.
