# Configuration Reference

Runes uses KDL config files named `runes.kdl`. Configuration is loaded by searching the current directory, then walking ancestors up to `~`, and finally checking `~/.runes/config.kdl`. Values from closer files override those from further ones.

`runes init` creates both global and local configs interactively.

## Reading and writing config

```bash
runes config list              # show effective local config
runes config list --global     # show global config
runes config get <key>         # read a value
runes config set <key> <value> # write a value (to local config)
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

## Local config (per-repo `runes.kdl`)

Created by `runes init` when run inside a git/jj/pijul repo. Sets the default project for that directory:

```kdl
defaults {
  project "myproject"
}
```

Use `--stealth` with `runes init` to add `runes.kdl` to `.git/info/exclude` so it stays untracked.

## Config blocks

### `user`

- `email` — your identity for VCS operations

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

### `query "<name>"` (deprecated)

Custom views are deprecated while the built-in views stabilize. `runes list` ships
with `open`, `mine`, `all`, and `closed`, which need no config — see
`runes list --help`. Config-defined queries still work (and still shadow a
built-in of the same name), but using one prints a deprecation warning, and a
reimagined customization story may replace them.

Saved filter sets for `runes list`. Use with `runes list <name>` or `runes list --query <name>`.

Supported filters:

- `project` — restrict to a project
- `status` — one or more statuses (multiple values are OR'd)
- `kind` — doc type (`task`, `milestone`)
- `assignee` — assignee (use `"self"` to match your configured email)
- `archived` — boolean, include archived docs

Multiple filters of the same key are OR'd; different keys are AND'd. Explicit CLI flags (`--status`, `--assignee`, etc.) override stored query values.

### `path`

Bind directories to stores or queries:

```kdl
path "/Users/you/work" {
  store "work"
  query "mine"
}
```

When your working directory is under a bound path, the associated store and query are used as defaults.

## Store selection order

1. Explicit prefix in the ref (`store:id` or `store:project`)
2. `--store` flag
3. Nearest `path` entry matching the current working directory
4. `defaults.store` from config
5. The single configured store (if only one exists)

## Project selection for `runes new`

When `--project` is omitted, the CLI checks in order:

1. `RUNES_PROJECT` environment variable (accepts `store:project` syntax)
2. `defaults.project` from the nearest `runes.kdl`
3. Whether the current directory name matches a project in the resolved store
4. Whether the repo root name matches a project in the resolved store
5. Fails with a prompt to pass `--project` or configure a default
