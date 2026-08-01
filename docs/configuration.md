# Configuration Reference

Runes follows the git-config model: two personal KDL config files, neither ever
committed, sharing one vocabulary.

| File | Tracked | Typical contents |
|---|---|---|
| `~/.runes/config.kdl` | never | identity, `defaults.*`, `store` definitions, `new` defaults, `attribution` |
| `<repo>/.runes/config.kdl` | never | any of the above, plus the store/project pointer for that repo |

Process config — kinds, statuses, fields, templates — lives in the *store* and
is committed there (see [schema.md](schema.md)).

The local file is found by walking ancestors of the current directory to the
repo root (marked by `.runes/`, `.git`, `.jj`, or `.pijul`). Local values
override global ones. Nothing about runes lands in the code repo's history: the
`.runes/` directory ignores itself via a `.runes/.gitignore` (honored by git
and jj) and a `.runes/.ignore` (pijul), both written when the directory is
created.

`runes init` creates both global and local configs interactively, and
initializes a store when the machine has none. Non-interactively it needs
`--project` and an identity it can read: an existing `user.email` in the global
config, or `RUNES_USER`. A repo whose local config lives at the pre-`.runes/`
location (`<repo>/runes.kdl`) is migrated automatically by `runes init`.

`runes init --help` lists the stores on this machine and marks the default.

## Reading and writing config

```bash
runes config list              # effective config, each value naming its source file
runes config list --global     # show global config
runes config get <key>         # read a value
runes config set <key> <value> # write a value (to the repo-local config)
runes config set <key> <value> --global  # write to global config
runes config unset <key>       # remove a value
```

## Global config (`~/.runes/config.kdl`)

Created by `runes init` on first run. Typical contents:

```kdl
user {
  email "you@example.com"
}

defaults {
  store "proj"
  query "open"
}

store "proj" {
  backend "jj"
  path "/Users/you/.runes/stores/proj"
}

new {
  task {
    assignee "self"
  }
}
```

## Local config (`<repo>/.runes/config.kdl`)

Created by `runes init`. Accepts the full global vocabulary; most repos only
need the project pointer:

```kdl
defaults {
  project "myproject"
}
```

The file is personal, like `.git/config`: it never travels with the repo, so
`runes init` is per-repo per-machine. Most repos need no local file at all — a
global `defaults.store` plus a directory name matching the project is enough.

## Config blocks

### `user`

- `email` — your identity for VCS operations
- `name` — your display name, recorded alongside the email
- `format` — how users render in human-facing output (default `name`)

`user.format` accepts `name` (full name, falling back to the email),
`email`, `name-email` (`Ana Ruiz <ana@example.com>`), `username` (the email's
local part), `first-name`, and `last-name`. Underscores and spaces work
wherever a dash does, so `email_username` and `firstName` are accepted too.

Only display changes: rune docs and commits always record the canonical email,
and `--json` output stays canonical. See [Names and emails](#names-and-emails).

### `attribution`

- `detect` — automatic AI agent detection for commit authorship (default `true`)

When no `--author` flag and no `RUNES_USER` are given, runes checks well-known
agent environment markers, first match wins:

1. `RUNES_AGENT` — explicit escape hatch, its value is the agent name
2. purpose-built markers — `CLAUDECODE` → `claude`, `GEMINI_CLI` → `gemini`,
   `CODEX_*` → `codex`, `CURSOR_*` → `cursor`, then `AUGMENT_AGENT`,
   `OPENCODE*`, `JUNIE_*`, `CLINE_ACTIVE`
3. generic `AI_AGENT`, then `AGENT` — their value names the agent

Purpose-built markers beat `AI_AGENT`/`AGENT` because they map to an exact
name, while the generic vars carry whatever the agent stamped there — Claude
Code sets `AI_AGENT=claude-code_2-1-218_agent` alongside `CLAUDECODE=1`, and
attributing to a version-stamped name would fragment history. Generic values
are cut at the first `_` (`claude-code_2-1-218_agent` → `claude-code`). Names
are lowercased and must match `[a-z0-9][a-z0-9._-]*`; anything else is ignored
and the next signal is tried.

A detected agent commits as `<agent>@agents.localhost`, named
`<agent> (on behalf of <your email>)` when `user.email` is configured. Set
`attribution.detect false` to always commit as `user.email`.

## Names and emails

Every user — an assignee, a commit author — is stored by email, the value that
does not change when someone is renamed. The name lives once per store, in a
`.mailmap` at the store root, and reading commands join the two back together.

### Who you commit as

Authorship is resolved in this order, first hit wins:

1. `--author` (`email` or `"Name <email>"`)
2. `RUNES_USER` (same two forms)
3. a detected AI agent, unless `attribution.detect false`
4. `user.email` / `user.name` from runes config
5. `git config user.name` / `user.email`, read from the repo holding the
   nearest runes config (so the machine-global `~/.gitconfig` counts too)

A detected agent still acts on behalf of whichever human identity 4 or 5 finds.
`runes init` writes the same fallback chain into the global config on a
non-interactive first run.

### The store's `.mailmap`

Whenever runes sees a name *and* an email together — a commit author, an
assignee passed as `"Ana Ruiz <ana@example.com>"` — it records
`Ana Ruiz <ana@example.com>` in `<store>/.mailmap` and commits the file
alongside the change. A value that carries only an email, or only a handle,
records nothing. Both `.mailmap` forms git supports are read, including
`Proper Name <new@email> <old@email>` for someone whose address changed.

The rune doc itself keeps only the email:

```kdl
assignee "ana@example.com"     # on disk
assignee "Ana Ruiz"            # what `runes show` prints
```

Filters follow the same mapping — `runes list --assignee "Ana Ruiz"`,
`--assignee Ana`, and `--assignee ana@example.com` all select the same runes,
and `runes log --changed-by` matches either half of an identity.

Pijul stores keep their local identities as a name source on read; the
`.mailmap` is what runes writes, for both backends.

### `defaults`

- `store` — default store when no `--store` flag or store prefix is given
- `project` — default project for `runes new` when no `--project` flag is given (accepts `store:project` syntax)
- `query` — view applied by default to `runes list` (a built-in view name; defaults to `open`)

### `store "<name>"`

Defines a named store:

- `backend` — VCS backend: `"jj"` or `"pijul"`
- `path` — absolute path to the store repository

### `new`

Creation defaults applied during `runes new`:

- `new.task.assignee` — default assignee for tasks (use `"self"` to expand to your configured email)
- `new.task.status` — default status
- `new.task.labels` — default labels
- `new.milestone.status` — default status for milestones

### `state "<core>"`

Allowed substates for a core state. The core states (`todo`, `wip`, `closed`) are
fixed; only their substates are configurable, and configuring one replaces the
defaults for that state:

```kdl
state "wip" {
    substate "design" "impl" "review"
}
```

Set from the CLI with a comma-separated list:

```bash
runes config set state.closed.substate "done,canceled,duplicate,wontfix"
```

A core state with no `state` node accepts any substate (the default for `todo`).

### `path`

Bind directories to a store or a default view:

```kdl
path "/Users/you/work" {
  store "work"
  query "mine"
}
```

When your working directory is under a bound path, the associated store and view are used as defaults. `query` names a built-in view (`open`, `mine`, `all`, `closed`).

## Store selection order

1. Explicit prefix in the ref (`store:id` or `store:project`)
2. `--store` flag
3. Nearest `path` entry matching the current working directory
4. `defaults.store` from config
5. The single configured store (if only one exists)

## Project selection for `runes new`

When `--project` is omitted, the CLI checks in order:

1. `RUNES_PROJECT` environment variable (accepts `store:project` syntax)
2. `defaults.project` from config (the repo-local file, then global)
3. Whether the current directory name matches a project in the resolved store
4. Whether the repo root name matches a project in the resolved store
5. Fails with a prompt to pass `--project` or configure a default
