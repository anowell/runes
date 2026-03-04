# Runes CLI

Runes currently provides a pragmatic bootstrap CLI for self-hosting project management in file-backed stores.

## Key Concepts

- `store`: named workspace with backend (`pijul` or `jj`)
- `project`: folder inside a store
- `rune`: markdown doc with KDL frontmatter
- canonical id: `<project>-<short>` (example `runes-cx3`)
- canonical ref: `<store>:<project-id>` (example `proj:runes-cx3`)

## Command Surface

```bash
runes store init <name> --backend <jj|pijul> [--path <path>] [--default]
runes store list
runes store info <name>
runes store remove <name>
runes store doctor <store>

runes project create <store> <project>

runes issue new <store> <project> <title> [--in <container-id>] [--status <status>]
runes issue new-milestone <store> <project> <title> [--id <m01>]
runes issue show <store> <project-id> | <store:project-id> | <project-id>
runes issue list <store> [--project <project>] [--status <status>]
runes issue edit <store> <project-id> | <store:project-id> | <project-id> [--title <title>] [--status <status>] [--add-label <l>] [--remove-label <l>] [--milestone <id|none>] [--add-rel <kind:id>] [--remove-rel <kind:id>]
runes issue move <from-store> <project-id> <to-store> <to-project> [--to-container <id>]
runes issue move <store:project-id> <to-store> <to-project> [--to-container <id>]
runes issue archive <store> <project-id> | <store:project-id> | <project-id>
runes issue log <store> <project-id> | <store:project-id> | <project-id> [--limit <n>] [--section <Heading>]
runes issue check <store> <project-id> | <store:project-id> | <project-id>
runes milestone progress <store> <milestone-id> | <store:milestone-id> | <milestone-id>

* `store doctor <store>` rebuilds the sqlite query cache and can be run whenever the cache needs reindexing.

runes backend status <store>
runes backend adapter <store>
runes backend capabilities <store>
runes backend probe-sdk <store>
runes backend log <store> [--limit <n>]
runes backend sync <store>
```

## Notes

- Markdown files are canonical; sqlite cache can be rebuilt from docs.
- Slug in filename is convenience-only.
- Title edits auto-rename slug.
- Canonical refs like `proj:runes-cx3` are accepted across issue commands.
- If store is omitted, the configured default store is used.
- Issue archive moves docs into `<project>/_archive`.
- `issue log --section` uses heading-match heuristics from backend change output.
- `issue edit` supports metadata updates for labels, milestone link, and relations.
- `issue check` validates milestone/relation references exist.
- `milestone progress` reports child rune status rollups from the container directory.
- Backend integration currently shells out to `jj`/`pijul` CLI and is tracked for migration to SDK/library adapters.
- `backend adapter <store>` prints active adapter implementation.
- `backend capabilities <store>` prints a capability matrix for the active adapter.
- `backend probe-sdk <store>` runs a backend SDK probe (currently `jj-lib` workspace load for jj stores).
