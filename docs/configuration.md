# Configuration

Runes is configured with **KDL** documents named `runes.kdl`. The loader searches in the current directory, then walks ancestors up to `~`, and finally checks `~/.runes/config.kdl`. Each document can add or override fields.

A sample configuration:

```kdl
identity {
  user "anthony"
  default_store "work"
}

creation {
  type "task"
  assignee "self"
  labels
}

default_query "mine"

queries.mine {
  assignee "self"
  status "todo" "in-progress"
}

queries.open {
  status "todo" "in-progress"
}

tools {
  # placeholder for future CLI/tool integration flags
}
```

## Blocks

- `identity`: optional defaults such as `user` (string) and `default_store` (string).
- `creation`: defaults applied during `runes new` (`type`, `status`, `assignee`, and repeatable `labels`).
- `default_query`: name of a saved query (see below) applied when `runes list`/`runes show` are invoked without filters.
- `default_project`: project (optionally prefixed with `<store>:`) used by `runes new` when no `--project` flag is supplied. See below for the selection order.
- `queries.<name>`: stores filters for `runes list`. Supported child nodes include `project`, repeatable `status`, `kind`, `archived`, and `assignee` (the latter can be set to `self` to reuse the configured identity user). Values with the same key are OR’ed, while different keys are AND’ed, and explicit flags (`--status`, `--assignee`, `--query`, ...) override the stored set.
- `path`: optional entries that bind directories to stores or queries (`store` and `query` properties on the node).

## Store selection

Runes resolves stores in this order:
1. explicit prefix (`store:id` or `store/project`).
2. `--store` flag (legacy compatibility).
3. the nearest `path` entry for the current working directory.
4. the configured `identity.default_store`.
5. the global default store from `~/.runes/config.txt`.

## Default project selection for `runes new`

`runes new` now accepts `--project <store:project>` but will infer a target project when that flag is omitted. The CLI evaluates the following in order:

1. The `RUNES_PROJECT` environment variable (which can include a `<store>:` prefix).
2. The nearest `runes.kdl` document's `default_project` value (also accepts `<store>:` syntax).
3. Whether the basename of the current working directory matches a project in the resolved store.
4. Whether the repository root's basename (detected by finding `.git`, `.jj`, `.pjul`, or `.pj`) matches a project in the resolved store.
5. Otherwise the command fails and asks you to pass `--project` or configure one of the defaults above.

## Queries

Queries let you save filter sets for `runes list`. Use `runes list --query mine` or simply `runes list mine` when you want to apply a stored query manually, and the defaults above apply when no overriding flags are supplied. For example, `queries.mine` can automatically focus on issues assigned to you (`assignee "self"` expands to the configured identity user) and restrict results to whatever statuses you list: `status "todo" "in-progress"` is interpreted as `(status == "todo" OR status == "in-progress")`, and combining that with an `assignee` or `project` node adds an `AND`. Explicit flags like `--status` or `--assignee` still override the stored values when you need a different slice of work.
