# Runes Schema (v0 Draft)

This document defines the pre-bootstrap storage schema for Runes.

## Core Concepts

- `store`: an independent issue repository (for example `me`, `team`).
- `project`: a directory inside a store. Creating a directory creates a project.
- `issue`: a markdown document with KDL frontmatter.
- `ref`: canonical identifier: `<store>:<project-id>`

Examples:

- `me:runes-cx3`
- `team:api-102`

## Store Layout

Each store is an independent VCS repo (backend: `jj` or `pijul`).

```text
~/.runes/workspaces/
  proj/                                # a store repo (personal, in this bootstrap)
    runes/                             # project root
      _archive/                        # optional archive area
      cx3--lock-schema.md              # issue (id is runes-cx3)
      m01--principles-schema-bootstrap/
        _milestone.md                  # container doc
        cx3--lock-runes-v1.md          # nested issues
        f4q--bootstrap-cli.md
```

Local cache layout:

```text
~/.runes/cache/
  <store>.sqlite
```

Cache files are not canonical and should not live inside store repos.

## File Naming

Issue filename format:

```text
<id>--<slug>.md
```

- `id` is canonical.
- `slug` is convenience-only for readability/search.
- CLI should auto-rename slug on title change.
- CLI must still resolve files if slug is stale or missing.

Milestone container format:

```text
<id>--<slug>/
  _milestone.md
```

- milestone/epic/feature containers are directories with an underscored control doc.
- nested issue files under a container are normal issue docs (no underscore).
- this keeps hierarchy explicit and avoids massive flat directories.

## ID Generation

Project-unique IDs with configurable strategy:

- default: `<project>-<rand3_base32>` (example `runes-cx3`)
- configurable:
  - random: charset + length
  - sequential: numeric (example `runes-104`)

## Document Format

All documents are markdown files with KDL frontmatter enclosed by `---`.

```markdown
---
doc kind="issue" id="runes-cx3" status="todo" priority=2
labels "bootstrap" "cli" "core"
relations {
  blocks "runes-cx9"
}
links {
  repo "anowell/spice" branch="main"
}
---

# Define Runes v1 CLI scope

## Summary
...

## Design
...

## Acceptance
...

## Comments
...
```

Notes:

- `# <title>` is the title source of truth.
- canonical identity is the `doc id`; path can move and nest freely.
- temporal/author context should come from VCS history by default.
- structured metadata belongs in KDL frontmatter.
- longform context and iterative notes belong in markdown sections.

## Hierarchy and Completion Semantics

- container docs (`_milestone.md`, future `_epic.md`, `_feature.md`) represent parent runes.
- child rune docs live in the same directory.
- parent completion can be inferred from child completion state (policy configurable).
- archive workflows should be directory moves (for example `runes/m01--...` to `runes/_archive/...`).

## Section Conventions

Recommended sections for issues:

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

Future CLI feature: filter file history by heading-scoped edits (for example `--section Design`).
