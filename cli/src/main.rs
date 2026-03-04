mod user_config;
use atty::Stream;
use pijul_interaction::{set_context, InteractiveContext};
use runes_core::backend;
use runes_core::cache;
use runes_core::config::{ensure_dir, BackendKind, Config, Store};
use runes_core::model::{
    discover_project_docs, new_issue_doc, new_milestone_doc, next_short_id, parse_doc,
    parse_full_id, render_doc, replace_title, resolve_issue_path, slugify, RuneDoc,
};
use runes_core::{Error, Result};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::Command;
use user_config::UserConfig;

fn usage() {
    eprintln!(
        "Runes CLI

Usage:
  runes new <project> <title> [--store <store>] [--type <issue|milestone>] [--status <status>] [--assignee <assignee>] [--parent <parent-id>] [--milestone <milestone-id>] [--label <label>] [--relation <kind:id>] [--id <custom-short>] [--no-commit]
  runes list [<view>] [--store <store>] [--project <project>] [--query <name>] [--type <issues|milestones>] [--status <status>] [--assignee <assignee>] [--archived] [--with-archived]
  runes show <id>
  runes edit <id> [--title <title>] [--status <status>] [--assignee <assignee>] [--label <label>] [--remove-label <label>] [--milestone <id|none>] [--relation <kind:id>] [--remove-relation <kind:id>] [-f <file>|--file <file>] [--no-commit]
  runes commit [<store> | <store>:<id>]
  runes move <id> [--project <project>] [--parent <parent-id>]
  runes archive <id>
  runes delete <id> [--force]
  runes log <id> [--limit <n>] [--section <heading>]
  runes sync [--store <store>] [--all]
  runes store init <name> --backend <jj|pijul> [--path <path>] [--default]
  runes store list
  runes store info <name>
  runes store remove <name>
  runes cache rebuild <store>
  runes cache query <store> <where-clause>"
    );
}

fn main() {
    set_context(InteractiveContext::Terminal);
    if let Err(err) = dispatch(std::env::args().collect()) {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn dispatch(args: Vec<String>) -> Result<()> {
    if args.len() < 2 {
        usage();
        return Err(Error::new("Missing command"));
    }
    if args.len() == 2 && (args[1] == "--help" || args[1] == "-h") {
        usage();
        return Ok(());
    }
    let tail = if args.len() > 2 {
        args[2..].to_vec()
    } else {
        Vec::new()
    };
    match args[1].as_str() {
        "new" => run_new(tail),
        "list" => run_list(tail),
        "show" => run_show(tail),
        "edit" => run_edit(tail),
        "commit" => run_commit(tail),
        "move" => run_move(tail),
        "archive" => run_archive(tail),
        "delete" => run_delete(tail),
        "log" => run_log(tail),
        "sync" => run_sync(tail),
        "store" => run_store(tail),
        "cache" => run_cache(tail),
        _ => Err(Error::new(format!("Unknown command: {}", args[1]))),
    }
}
fn home_dir() -> Result<PathBuf> {
    Ok(PathBuf::from(
        std::env::var("HOME").map_err(|_| Error::new("HOME not set"))?,
    ))
}

fn default_store_path(name: &str) -> Result<PathBuf> {
    Ok(home_dir()?.join(".runes").join("workspaces").join(name))
}

fn pop_arg(args: &mut Vec<String>, name: &str) -> Option<String> {
    if let Some(idx) = args.iter().position(|a| a == name) {
        args.remove(idx);
        return if idx >= args.len() {
            None
        } else {
            Some(args.remove(idx))
        };
    }
    None
}

fn pop_multi(args: &mut Vec<String>, name: &str) -> Vec<String> {
    let mut values = Vec::new();
    while let Some(v) = pop_arg(args, name) {
        values.push(v);
    }
    values
}

fn has_flag(args: &mut Vec<String>, name: &str) -> bool {
    if let Some(idx) = args.iter().position(|a| a == name) {
        args.remove(idx);
        true
    } else {
        false
    }
}

fn require_len(args: &[String], n: usize, usage_hint: &str) -> Result<()> {
    if args.len() < n {
        return Err(Error::new(format!(
            "Expected at least {n} args: {usage_hint}"
        )));
    }
    Ok(())
}

fn load_context() -> Result<(Config, UserConfig, PathBuf)> {
    let config = Config::load()?;
    let cwd = std::env::current_dir().map_err(|e| Error::new(e.to_string()))?;
    let user_cfg = UserConfig::load_from_dir(&cwd)?;
    Ok((config, user_cfg, cwd))
}
fn split_store_prefix(spec: &str) -> (Option<String>, &str) {
    if let Some((store, rest)) = spec.split_once(':') {
        return (Some(store.to_string()), rest);
    }
    if let Some((store, rest)) = spec.split_once('/') {
        return (Some(store.to_string()), rest);
    }
    (None, spec)
}

fn resolve_store_with_context(
    config: &Config,
    user_config: &UserConfig,
    cwd: &Path,
    store_hint: Option<&str>,
) -> Result<Store> {
    if let Some(name) = store_hint {
        return config.get_store(name);
    }
    if let Some(name) = user_config.store_for_path(cwd) {
        return config.get_store(&name);
    }
    if let Some(name) = user_config.default_store.as_deref() {
        return config.get_store(name);
    }
    config.default_store()
}

fn resolve_store_and_project(
    config: &Config,
    user_config: &UserConfig,
    cwd: &Path,
    store_hint: Option<&str>,
    project_spec: Option<&String>,
) -> Result<(Store, Option<String>)> {
    if let Some(spec) = project_spec {
        let (project_store_hint, project) = split_store_prefix(spec);
        if project.is_empty() {
            return Err(Error::new("Project name may not be empty"));
        }
        let hint = project_store_hint.as_deref().or(store_hint);
        let store = resolve_store_with_context(config, user_config, cwd, hint)?;
        return Ok((store, Some(project.to_string())));
    }
    let store = resolve_store_with_context(config, user_config, cwd, store_hint)?;
    Ok((store, None))
}

fn resolve_store_and_project_required(
    config: &Config,
    user_config: &UserConfig,
    cwd: &Path,
    store_hint: Option<&str>,
    project_spec: &str,
) -> Result<(Store, String)> {
    let (project_store_hint, project) = split_store_prefix(project_spec);
    if project.is_empty() {
        return Err(Error::new("Project name may not be empty"));
    }
    let hint = project_store_hint.as_deref().or(store_hint);
    let store = resolve_store_with_context(config, user_config, cwd, hint)?;
    Ok((store, project.to_string()))
}

fn resolve_store_and_id(
    config: &Config,
    user_config: &UserConfig,
    cwd: &Path,
    store_hint: Option<&str>,
    id_spec: &str,
) -> Result<(Store, String)> {
    let (project_store_hint, id_part) = split_store_prefix(id_spec);
    if id_part.is_empty() {
        return Err(Error::new("ID may not be empty"));
    }
    let hint = project_store_hint.as_deref().or(store_hint);
    let store = resolve_store_with_context(config, user_config, cwd, hint)?;
    Ok((store, id_part.to_string()))
}

fn locate_doc(store: &Store, id: &str) -> Result<PathBuf> {
    if parse_full_id(id).is_ok() {
        return resolve_issue_path(&store.path, id);
    }
    find_short_id(&store.path, id)
}

fn find_short_id(store_path: &Path, short: &str) -> Result<PathBuf> {
    let mut matches = Vec::new();
    for entry in fs::read_dir(store_path)? {
        let entry = entry?;
        let project_root = entry.path();
        if !project_root.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue;
        }
        let docs = discover_project_docs(&project_root)?;
        for path in docs {
            let doc = match parse_doc(&path) {
                Ok(d) => d,
                Err(_) => continue,
            };
            if let Some((_, candidate)) = doc.id.split_once('-') {
                if candidate == short {
                    matches.push(path.clone());
                }
            }
        }
    }
    match matches.len() {
        0 => Err(Error::new(format!("No file found for id '{short}'"))),
        1 => Ok(matches.remove(0)),
        _ => Err(Error::new(format!(
            "Multiple files matched id '{short}'. Narrow your query next time."
        ))),
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListKind {
    Issues,
    Milestones,
}

impl ListKind {
    fn parse(value: &str) -> ListKind {
        match value.to_lowercase().as_str() {
            "milestones" | "milestone" => ListKind::Milestones,
            _ => ListKind::Issues,
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            ListKind::Issues => "issue",
            ListKind::Milestones => "milestone",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArchivedMode {
    Exclude,
    Only,
    Include,
}

impl ArchivedMode {
    fn from_keyword(value: &str) -> Option<Self> {
        match value.to_lowercase().as_str() {
            "only" | "archived-only" => Some(ArchivedMode::Only),
            "archived" | "include" | "with-archived" => Some(ArchivedMode::Include),
            "exclude" | "open" | "active" => Some(ArchivedMode::Exclude),
            _ => None,
        }
    }
}
struct IssueFilters {
    project: Option<String>,
    statuses: Vec<String>,
    kind: Option<String>,
    assignee: Option<String>,
    archived: ArchivedMode,
}
fn sql_escape(val: &str) -> String {
    val.replace('\'', "''")
}

fn query_issues(store: &Store, filters: IssueFilters) -> Result<String> {
    let mut where_parts = vec!["1=1".to_string()];
    if let Some(project) = filters.project {
        where_parts.push(format!("project='{}'", sql_escape(&project)));
    }
    if !filters.statuses.is_empty() {
        let quoted: Vec<String> = filters
            .statuses
            .iter()
            .map(|status| format!("'{}'", sql_escape(status)))
            .collect();
        where_parts.push(format!("status IN ({})", quoted.join(",")));
    }
    if let Some(kind) = filters.kind {
        where_parts.push(format!("kind='{}'", sql_escape(&kind)));
    }
    if let Some(assignee) = filters.assignee {
        where_parts.push(format!("assignee='{}'", sql_escape(&assignee)));
    }
    match filters.archived {
        ArchivedMode::Exclude => {
            where_parts.push("path NOT LIKE '%/_archive/%'".to_string());
        }
        ArchivedMode::Only => {
            where_parts.push("path LIKE '%/_archive/%'".to_string());
        }
        ArchivedMode::Include => {}
    }
    let where_clause = where_parts.join(" AND ");
    cache::query_cache(store, &where_clause)
}
fn parse_relations(relations: &[String]) -> Result<Vec<(String, String)>> {
    let mut parsed = Vec::new();
    for rel in relations {
        if let Some((kind, id)) = rel.split_once(':') {
            if kind.is_empty() || id.is_empty() {
                return Err(Error::new(format!(
                    "Invalid relation '{rel}', expected kind:id"
                )));
            }
            parsed.push((kind.to_string(), id.to_string()));
        } else {
            return Err(Error::new(format!(
                "Invalid relation '{rel}', expected kind:id"
            )));
        }
    }
    Ok(parsed)
}

fn id_exists(project_root: &Path, id: &str) -> Result<bool> {
    if !project_root.exists() {
        return Ok(false);
    }
    for path in discover_project_docs(project_root)? {
        if let Ok(doc) = parse_doc(&path) {
            if doc.id == id {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn commit_paths(store: &Store, paths: &[PathBuf], message: &str) -> Result<()> {
    let mut rels = Vec::new();
    for path in paths {
        let rel = path
            .strip_prefix(&store.path)
            .map_err(|e| Error::new(e.to_string()))?
            .to_path_buf();
        rels.push(rel);
    }
    backend::commit_paths(store, &rels, message)?;
    cache::rebuild_cache(store)?;
    Ok(())
}

fn stdin_is_tty() -> bool {
    atty::is(Stream::Stdin)
}

fn editor_available() -> bool {
    atty::is(Stream::Stdout) && stdin_is_tty()
}

fn open_editor(path: &Path) -> Result<()> {
    let editor = std::env::var("VISUAL")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or_else(|_| "vi".to_string());
    let status = Command::new(editor)
        .arg(path)
        .status()
        .map_err(|e| Error::new(format!("Editor launch failed: {e}")))?;
    if !status.success() {
        return Err(Error::new(format!("Editor exited with status: {status}")));
    }
    Ok(())
}

fn read_from_stdin() -> Result<String> {
    let mut buffer = String::new();
    io::stdin()
        .read_to_string(&mut buffer)
        .map_err(|e| Error::new(e.to_string()))?;
    Ok(buffer)
}

fn print_uncommitted_hint(store: &Store, id: &str, path: &Path) {
    println!(
        "Changes for {id} are staged at {path}. Run `runes commit {store_name}:{id}` when ready.",
        id = id,
        path = path.display(),
        store_name = store.name
    );
}
fn create_issue(
    store: &Store,
    project: &str,
    title: &str,
    status: &str,
    parent: Option<&str>,
    milestone: Option<&str>,
    labels: &[String],
    relations: &[(String, String)],
    assignee: Option<&str>,
    short_override: Option<&str>,
) -> Result<(String, PathBuf)> {
    let project_root = store.path.join(project);
    ensure_dir(&project_root)?;
    let short = if let Some(override_id) = short_override {
        if override_id.contains('-') || override_id.contains('/') {
            return Err(Error::new("Custom short ids may not contain '-' or '/'"));
        }
        override_id.to_string()
    } else {
        next_short_id(project, &project_root, 3)?
    };
    let full_id = format!("{project}-{short}");
    if id_exists(&project_root, &full_id)? {
        return Err(Error::new(format!("ID '{full_id}' already exists")));
    }
    let slug = slugify(title);
    let parent_dir = if let Some(parent_id) = parent {
        find_container_dir(&project_root, parent_id)?
    } else {
        project_root.clone()
    };
    ensure_dir(&parent_dir)?;
    let file_name = format!("{short}--{slug}.md");
    let path = parent_dir.join(&file_name);
    let mut doc = new_issue_doc(&full_id, title, milestone);
    doc.status = status.to_string();
    doc.labels = labels.to_vec();
    doc.relations = relations.to_vec();
    if let Some(assignee_value) = assignee {
        doc.assignee = Some(assignee_value.to_string());
    }
    fs::write(&path, render_doc(&doc))?;
    Ok((full_id, path))
}

fn create_milestone(
    store: &Store,
    project: &str,
    title: &str,
    status: &str,
    labels: &[String],
    short_override: Option<&str>,
) -> Result<(String, PathBuf)> {
    let project_root = store.path.join(project);
    ensure_dir(&project_root)?;
    let short = if let Some(override_id) = short_override {
        override_id.to_string()
    } else {
        let generated = next_short_id(project, &project_root, 2)?;
        format!("m{generated}")
    };
    let full_id = format!("{project}-{short}");
    let slug = slugify(title);
    let container_dir = project_root.join(format!("{short}--{slug}"));
    ensure_dir(&container_dir)?;
    let path = container_dir.join("_milestone.md");
    let mut doc = new_milestone_doc(&full_id, title);
    doc.status = status.to_string();
    if !labels.is_empty() {
        doc.labels = labels.to_vec();
    }
    fs::write(&path, render_doc(&doc))?;
    Ok((full_id, path))
}
fn run_new(mut args: Vec<String>) -> Result<()> {
    require_len(&args, 2, "new <project> <title>")?;
    let project_spec = args.remove(0);
    let title = args.remove(0);
    let no_commit = has_flag(&mut args, "--no-commit");
    let store_flag = pop_arg(&mut args, "--store");
    let kind_flag = pop_arg(&mut args, "--type");
    let status_flag = pop_arg(&mut args, "--status");
    let assignee_flag = pop_arg(&mut args, "--assignee");
    let parent = pop_arg(&mut args, "--parent");
    let milestone = pop_arg(&mut args, "--milestone");
    let id_override = pop_arg(&mut args, "--id");
    let label_overrides = pop_multi(&mut args, "--label");
    let relation_inputs = pop_multi(&mut args, "--relation");
    let relations = parse_relations(&relation_inputs)?;
    let (cfg, user_cfg, cwd) = load_context()?;
    let creation_defaults = user_cfg.creation_defaults();
    let kind_value = kind_flag
        .clone()
        .or_else(|| creation_defaults.kind.clone())
        .unwrap_or_else(|| "issue".to_string());
    let is_milestone = kind_value.eq_ignore_ascii_case("milestone");
    let kind = if is_milestone { "milestone" } else { "issue" };
    let mut status_value = status_flag
        .clone()
        .or_else(|| creation_defaults.status.clone());
    if status_value.is_none() {
        status_value = Some(if kind == "milestone" {
            "active".to_string()
        } else {
            "todo".to_string()
        });
    }
    let status = status_value.unwrap();
    let mut labels = creation_defaults.labels.clone();
    labels.extend(label_overrides);
    let assignee_value = assignee_flag
        .clone()
        .or_else(|| creation_defaults.assignee.clone());
    let resolved_assignee = assignee_value
        .as_deref()
        .and_then(|value| user_cfg.resolve_user_alias(value));
    let (store, project) = resolve_store_and_project_required(
        &cfg,
        &user_cfg,
        &cwd,
        store_flag.as_deref(),
        &project_spec,
    )?;
    let (identifier, doc_path) = if kind == "milestone" {
        create_milestone(
            &store,
            &project,
            &title,
            &status,
            &labels,
            id_override.as_deref(),
        )?
    } else {
        create_issue(
            &store,
            &project,
            &title,
            &status,
            parent.as_deref(),
            milestone.as_deref(),
            &labels,
            &relations,
            resolved_assignee.as_deref(),
            id_override.as_deref(),
        )?
    };
    let interactive = editor_available();
    if interactive {
        open_editor(&doc_path)?;
    }
    if interactive && !no_commit {
        commit_paths(&store, &[doc_path.clone()], &format!("Add {identifier}"))?;
    } else {
        print_uncommitted_hint(&store, &identifier, &doc_path);
    }
    println!("{identifier}");
    Ok(())
}
fn run_list(mut args: Vec<String>) -> Result<()> {
    let query_flag = pop_arg(&mut args, "--query");
    let store_flag = pop_arg(&mut args, "--store");
    let project_spec = pop_arg(&mut args, "--project");
    let type_flag = pop_arg(&mut args, "--type");
    let status_flag = pop_arg(&mut args, "--status");
    let assignee_flag = pop_arg(&mut args, "--assignee");
    let archived_only = has_flag(&mut args, "--archived");
    let include_archived = has_flag(&mut args, "--with-archived");
    if archived_only && include_archived {
        return Err(Error::new(
            "Cannot use --archived and --with-archived together",
        ));
    }
    let view_arg = if !args.is_empty() {
        let value = args.remove(0);
        if !args.is_empty() {
            return Err(Error::new(format!(
                "Unexpected arguments after view name: {}",
                args.join(" ")
            )));
        }
        Some(value)
    } else {
        None
    };
    let mut archived_mode = if archived_only {
        ArchivedMode::Only
    } else if include_archived {
        ArchivedMode::Include
    } else {
        ArchivedMode::Exclude
    };
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, project) = resolve_store_and_project(
        &cfg,
        &user_cfg,
        &cwd,
        store_flag.as_deref(),
        project_spec.as_ref(),
    )?;
    let project_flag_present = project_spec.is_some();
    let status_flag_present = status_flag.is_some();
    let type_flag_present = type_flag.is_some();
    let assignee_filter = assignee_flag
        .as_deref()
        .and_then(|value| user_cfg.resolve_user_alias(value));
    let mut list_kind = type_flag
        .as_deref()
        .map(ListKind::parse)
        .unwrap_or(ListKind::Issues);
    let mut filters = IssueFilters {
        project,
        statuses: status_flag
            .as_ref()
            .map(|value| vec![value.clone()])
            .unwrap_or_else(Vec::new),
        kind: None,
        assignee: assignee_filter,
        archived: archived_mode,
    };
    let query_name = query_flag
        .or(view_arg)
        .or_else(|| user_cfg.query_for_path(&cwd))
        .or_else(|| user_cfg.default_query.clone());
    if let Some(query_key) = query_name {
        if let Some(query) = user_cfg.query(&query_key) {
            if !project_flag_present {
                filters.project = query.project.clone();
            }
            if !status_flag_present {
                filters.statuses = query.statuses.clone();
            }
            if !type_flag_present {
                if let Some(kind_value) = &query.kind {
                    list_kind = ListKind::parse(kind_value);
                }
            }
            if !archived_only && !include_archived {
                if let Some(archived_value) = &query.archived {
                    if let Some(parsed) = ArchivedMode::from_keyword(archived_value) {
                        archived_mode = parsed;
                    }
                }
            }
            if filters.assignee.is_none() {
                if let Some(query_assignee) = &query.assignee {
                    filters.assignee = user_cfg.resolve_user_alias(query_assignee);
                }
            }
        }
    }
    filters.archived = archived_mode;
    filters.kind = Some(list_kind.kind_name().to_string());
    match list_kind {
        ListKind::Issues => {
            let output = query_issues(&store, filters)?;
            print!("{output}");
            Ok(())
        }
        ListKind::Milestones => {
            let mut rows = Vec::new();
            if let Some(project_name) = filters.project {
                rows = list_project_milestones(&store, &project_name, archived_mode)?;
            } else {
                let projects = all_projects(&store)?;
                for project_name in projects {
                    let mut project_rows =
                        list_project_milestones(&store, &project_name, archived_mode)?;
                    rows.append(&mut project_rows);
                }
            }
            if rows.is_empty() {
                return Err(Error::new("No milestones found"));
            }
            for row in rows {
                println!("{row}");
            }
            Ok(())
        }
    }
}
fn all_projects(store: &Store) -> Result<Vec<String>> {
    let mut projects = Vec::new();
    for entry in fs::read_dir(&store.path)? {
        let entry = entry?;
        if !entry.path().is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') || name == "_archive" {
            continue;
        }
        projects.push(name);
    }
    Ok(projects)
}

fn list_project_milestones(
    store: &Store,
    project: &str,
    archived_mode: ArchivedMode,
) -> Result<Vec<String>> {
    let mut rows = Vec::new();
    if archived_mode != ArchivedMode::Only {
        rows.append(&mut list_milestones_in_scope(store, project, false)?);
    }
    if archived_mode != ArchivedMode::Exclude {
        rows.append(&mut list_milestones_in_scope(store, project, true)?);
    }
    Ok(rows)
}

fn list_milestones_in_scope(store: &Store, project: &str, archived: bool) -> Result<Vec<String>> {
    let project_root = store.path.join(project);
    let container_root = if archived {
        project_root.join("_archive")
    } else {
        project_root.clone()
    };
    if !container_root.exists() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    for entry in fs::read_dir(&container_root)? {
        let entry = entry?;
        if !entry.path().is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if !archived && name == "_archive" {
            continue;
        }
        let milestone_file = entry.path().join("_milestone.md");
        if !milestone_file.exists() {
            continue;
        }
        let doc = parse_doc(&milestone_file)?;
        if doc.kind != "milestone" {
            continue;
        }
        let (total, done, in_progress, todo) = count_milestone_children(&entry.path())?;
        let pct = if total == 0 {
            100.0
        } else {
            (done as f64 / total as f64) * 100.0
        };
        let archived_flag = if archived { " archived=true" } else { "" };
        rows.push(format!(
            "milestone={} status={} total={} done={} in_progress={} todo={} complete_pct={:.1}{} title={}",
            doc.id,
            doc.status,
            total,
            done,
            in_progress,
            todo,
            pct,
            archived_flag,
            doc.title
        ));
    }
    Ok(rows)
}

fn count_milestone_children(container: &Path) -> Result<(usize, usize, usize, usize)> {
    let mut total = 0;
    let mut done = 0;
    let mut in_progress = 0;
    let mut todo = 0;
    for entry in fs::read_dir(container)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("md") {
            continue;
        }
        if path.file_name().and_then(|s| s.to_str()) == Some("_milestone.md") {
            continue;
        }
        let child = parse_doc(&path)?;
        total += 1;
        match child.status.as_str() {
            "done" => done += 1,
            "in-progress" => in_progress += 1,
            _ => todo += 1,
        }
    }
    Ok((total, done, in_progress, todo))
}
fn run_show(mut args: Vec<String>) -> Result<()> {
    require_len(&args, 1, "show <id>")?;
    let id_spec = args.remove(0);
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id_spec)?;
    let path = locate_doc(&store, &id)?;
    let doc = parse_doc(&path)?;
    print_doc_summary(&path, &doc)
}

fn print_doc_summary(path: &Path, doc: &RuneDoc) -> Result<()> {
    println!("path={}", path.display());
    println!("kind={}", doc.kind);
    println!("id={}", doc.id);
    println!("status={}", doc.status);
    if let Some(assignee) = &doc.assignee {
        println!("assignee={}", assignee);
    }
    if !doc.labels.is_empty() {
        println!("labels={}", doc.labels.join(","));
    }
    if let Some(milestone) = &doc.milestone {
        println!("milestone={}", milestone);
    }
    if !doc.relations.is_empty() {
        let rels: Vec<String> = doc
            .relations
            .iter()
            .map(|(kind, id)| format!("{kind}:{id}"))
            .collect();
        println!("relations={}", rels.join(","));
    }
    if doc.kind == "milestone" {
        if let Some(container) = path.parent() {
            if container.exists() {
                let (total, done, in_progress, todo) = count_milestone_children(container)?;
                let pct = if total == 0 {
                    100.0
                } else {
                    (done as f64 / total as f64) * 100.0
                };
                println!("child_total={total} child_done={done} child_in_progress={in_progress} child_todo={todo} complete_pct={pct:.1}");
                let children = list_container_children(container)?;
                if !children.is_empty() {
                    println!("children:");
                    for child in children {
                        println!("  {child}");
                    }
                }
            }
        }
    }
    println!();
    print!("{}", fs::read_to_string(path)?);
    Ok(())
}

fn list_container_children(container: &Path) -> Result<Vec<String>> {
    let mut rows = Vec::new();
    for entry in fs::read_dir(container)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("md") {
            continue;
        }
        if path.file_name().and_then(|s| s.to_str()) == Some("_milestone.md") {
            continue;
        }
        if let Ok(child) = parse_doc(&path) {
            rows.push(format!("{} ({})", child.id, child.status));
        }
    }
    Ok(rows)
}
fn run_edit(mut args: Vec<String>) -> Result<()> {
    require_len(&args, 1, "edit <id>")?;
    let id_spec = args.remove(0);
    let file_arg = pop_arg(&mut args, "-f").or_else(|| pop_arg(&mut args, "--file"));
    let no_commit = has_flag(&mut args, "--no-commit");
    let new_title = pop_arg(&mut args, "--title");
    let new_status = pop_arg(&mut args, "--status");
    let new_assignee = pop_arg(&mut args, "--assignee");
    let add_labels = pop_multi(&mut args, "--label");
    let remove_labels = pop_multi(&mut args, "--remove-label");
    let milestone = pop_arg(&mut args, "--milestone");
    let add_relation_inputs = pop_multi(&mut args, "--relation");
    let remove_relation_inputs = pop_multi(&mut args, "--remove-relation");
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id_spec)?;
    let path = locate_doc(&store, &id)?;
    let mut doc = parse_doc(&path)?;
    let mut final_path = path.clone();
    let has_field_edits = new_title.is_some()
        || new_status.is_some()
        || new_assignee.is_some()
        || !add_labels.is_empty()
        || !remove_labels.is_empty()
        || milestone.is_some()
        || !add_relation_inputs.is_empty()
        || !remove_relation_inputs.is_empty();
    if file_arg.is_some() && has_field_edits {
        return Err(Error::new(
            "Cannot mix field edits with --file/STDIN content updates",
        ));
    }
    if has_field_edits {
        if let Some(status) = new_status {
            doc.status = status;
        }
        if let Some(assignee_value) = new_assignee {
            if assignee_value.eq_ignore_ascii_case("none") {
                doc.assignee = None;
            } else if let Some(resolved) = user_cfg.resolve_user_alias(&assignee_value) {
                doc.assignee = Some(resolved);
            } else {
                doc.assignee = Some(assignee_value);
            }
        }
        for label in add_labels {
            if !doc.labels.iter().any(|l| l == &label) {
                doc.labels.push(label);
            }
        }
        for label in remove_labels {
            doc.labels.retain(|l| l != &label);
        }
        if let Some(milestone_value) = milestone {
            if milestone_value == "none" {
                doc.milestone = None;
            } else {
                doc.milestone = Some(milestone_value);
            }
        }
        let add_relations = parse_relations(&add_relation_inputs)?;
        for (kind, target) in add_relations {
            if !doc.relations.iter().any(|(existing_kind, existing_id)| {
                existing_kind == &kind && existing_id == &target
            }) {
                doc.relations.push((kind, target));
            }
        }
        let remove_relations = parse_relations(&remove_relation_inputs)?;
        for (kind, target) in remove_relations {
            doc.relations.retain(|(existing_kind, existing_id)| {
                existing_kind != &kind || existing_id != &target
            });
        }
        if let Some(title) = new_title {
            doc.title = title.clone();
            doc.body = replace_title(&doc.body, &title);
            let parsed = parse_full_id(&doc.id)?;
            let new_name = format!("{}--{}.md", parsed.short, slugify(&title));
            final_path = path
                .parent()
                .ok_or_else(|| Error::new("Invalid issue path"))?
                .join(new_name);
        }
        fs::write(&path, render_doc(&doc))?;
        if final_path != path {
            fs::rename(&path, &final_path)?;
        }
    } else {
        if let Some(file_path) = file_arg {
            let contents = fs::read_to_string(&file_path)?;
            fs::write(&path, contents)?;
        } else if !stdin_is_tty() {
            let contents = read_from_stdin()?;
            fs::write(&path, contents)?;
        } else if editor_available() {
            open_editor(&path)?;
        } else {
            return Err(Error::new("No edits specified and no editor available"));
        }
        doc = parse_doc(&path)?;
        final_path = path.clone();
    }
    let commit_message = format!("Update {}", doc.id);
    if !no_commit {
        commit_paths(&store, &[final_path.clone()], &commit_message)?;
    } else {
        print_uncommitted_hint(&store, &doc.id, &final_path);
    }
    Ok(())
}

fn store_exists(config: &Config, name: &str) -> bool {
    config.stores.iter().any(|store| store.name == name)
}

fn commit_store(store: &Store) -> Result<()> {
    let message = format!("Record staged changes in {}", store.name);
    commit_paths(store, &[], &message)?;
    println!("Committed staged changes in {}", store.name);
    Ok(())
}

fn commit_rune(store: &Store, id_spec: &str) -> Result<()> {
    let path = locate_doc(store, id_spec)?;
    let doc = parse_doc(&path)?;
    let message = format!("Record {}", doc.id);
    commit_paths(store, &[path.clone()], &message)?;
    println!("Committed {}", doc.id);
    Ok(())
}

fn run_commit(mut args: Vec<String>) -> Result<()> {
    if args.len() > 1 {
        return Err(Error::new("Unexpected arguments after commit target"));
    }
    let target = if args.is_empty() {
        None
    } else {
        Some(args.remove(0))
    };
    let (cfg, user_cfg, cwd) = load_context()?;
    match target {
        Some(spec) if spec.contains(':') || spec.contains('/') => {
            let (store_hint, id_part) = split_store_prefix(&spec);
            if let Some(store_name) = store_hint.as_deref() {
                if store_name.is_empty() {
                    return Err(Error::new("Store name may not be empty"));
                }
            }
            let store = resolve_store_with_context(&cfg, &user_cfg, &cwd, store_hint.as_deref())?;
            if id_part.is_empty() {
                commit_store(&store)?;
            } else {
                commit_rune(&store, id_part)?;
            }
        }
        Some(spec) => {
            if store_exists(&cfg, &spec) {
                let store = resolve_store_with_context(&cfg, &user_cfg, &cwd, Some(&spec))?;
                commit_store(&store)?;
            } else {
                let store = resolve_store_with_context(&cfg, &user_cfg, &cwd, None)?;
                commit_rune(&store, &spec)?;
            }
        }
        None => {
            let store = resolve_store_with_context(&cfg, &user_cfg, &cwd, None)?;
            commit_store(&store)?;
        }
    }
    Ok(())
}
fn find_container_dir(project_root: &Path, full_id: &str) -> Result<PathBuf> {
    let parsed = parse_full_id(full_id)?;
    let docs = discover_project_docs(project_root)?;
    let needle = format!("{}--", parsed.short);
    for path in docs {
        if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
            if name == "_milestone.md" {
                let doc = parse_doc(&path)?;
                if doc.id == full_id {
                    return path
                        .parent()
                        .map(|p| p.to_path_buf())
                        .ok_or_else(|| Error::new("Invalid container path"));
                }
            } else if name.starts_with(&needle) {
                return path
                    .parent()
                    .map(|p| p.to_path_buf())
                    .ok_or_else(|| Error::new("Invalid file path"));
            }
        }
    }
    Err(Error::new(format!("Container '{full_id}' not found")))
}
fn move_rune(
    from_store: &Store,
    to_store: &Store,
    full_id: &str,
    to_project: &str,
    to_parent: Option<&str>,
) -> Result<()> {
    let source_path = locate_doc(from_store, full_id)?;
    let source_doc = parse_doc(&source_path)?;
    let parsed = parse_full_id(&source_doc.id)?;
    let file_name = source_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| Error::new("Invalid source filename"))?
        .to_string();
    let to_project_root = to_store.path.join(to_project);
    ensure_dir(&to_project_root)?;
    let target_parent = if let Some(container_id) = to_parent {
        find_container_dir(&to_project_root, container_id)?
    } else {
        to_project_root
    };
    let target_path = target_parent.join(&file_name);
    let mut target_doc = source_doc.clone();
    if parsed.project != to_project {
        target_doc.id = format!("{to_project}-{}", parsed.short);
    }

    if from_store.name == to_store.name {
        fs::write(&target_path, render_doc(&target_doc))?;
        if source_path != target_path {
            fs::remove_file(&source_path)?;
        }
        commit_paths(
            from_store,
            &[target_path.clone()],
            &format!("Move {} within {}", target_doc.id, from_store.name),
        )?;
        println!("Moved {}", target_doc.id);
        return Ok(());
    }

    fs::write(&target_path, render_doc(&target_doc))?;
    commit_paths(
        to_store,
        &[target_path.clone()],
        &format!("Move in {}", target_doc.id),
    )?;
    let from_rel = source_path
        .strip_prefix(&from_store.path)
        .map_err(|e| Error::new(e.to_string()))?
        .to_path_buf();
    backend::remove_path(from_store, &from_rel)?;
    fs::remove_file(&source_path)?;
    if let Err(err) = backend::commit_paths(from_store, &[], &format!("Move out {}", source_doc.id))
    {
        fs::write(&source_path, render_doc(&source_doc))?;
        return Err(Error::new(format!(
            "Move-in committed to target, but source commit failed: {err}"
        )));
    }
    cache::rebuild_cache(from_store)?;
    println!(
        "Moved {} from {} to {}",
        parsed.full, from_store.name, to_store.name
    );
    Ok(())
}
fn run_move(mut args: Vec<String>) -> Result<()> {
    require_len(&args, 1, "move <id>")?;
    let id_spec = args.remove(0);
    let target_project = pop_arg(&mut args, "--project")
        .ok_or_else(|| Error::new("Missing --project <target-project> for move command"))?;
    let parent = pop_arg(&mut args, "--parent");
    let (cfg, user_cfg, cwd) = load_context()?;
    let (from_store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id_spec)?;
    let (to_store, project) =
        resolve_store_and_project_required(&cfg, &user_cfg, &cwd, None, &target_project)?;
    move_rune(&from_store, &to_store, &id, &project, parent.as_deref())
}
fn run_archive(mut args: Vec<String>) -> Result<()> {
    require_len(&args, 1, "archive <id>")?;
    let id_spec = args.remove(0);
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id_spec)?;
    archive_rune(&store, &id)
}

fn archive_rune(store: &Store, id: &str) -> Result<()> {
    let source_path = locate_doc(store, id)?;
    let doc = parse_doc(&source_path)?;
    let parsed = parse_full_id(&doc.id)?;
    let project_root = store.path.join(&parsed.project);
    let archive_dir = project_root.join("_archive");
    ensure_dir(&archive_dir)?;
    let target_path = if source_path.file_name().and_then(|s| s.to_str()) == Some("_milestone.md") {
        let container = source_path
            .parent()
            .ok_or_else(|| Error::new("Invalid milestone container path"))?;
        let container_name = container
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| Error::new("Invalid milestone container name"))?;
        archive_dir.join(container_name)
    } else {
        let fname = source_path
            .file_name()
            .ok_or_else(|| Error::new("Invalid source file"))?;
        archive_dir.join(fname)
    };
    if source_path.file_name().and_then(|s| s.to_str()) == Some("_milestone.md") {
        let source_container = source_path
            .parent()
            .ok_or_else(|| Error::new("Invalid container path"))?;
        fs::rename(source_container, &target_path)?;
    } else {
        fs::rename(&source_path, &target_path)?;
    }
    commit_paths(store, &[target_path], &format!("Archive {}", doc.id))?;
    Ok(())
}
fn run_delete(mut args: Vec<String>) -> Result<()> {
    require_len(&args, 1, "delete <id>")?;
    let id_spec = args.remove(0);
    let force = has_flag(&mut args, "--force");
    if !force {
        return Err(Error::new("Use --force to delete runes"));
    }
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id_spec)?;
    delete_rune(&store, &id)
}

fn delete_rune(store: &Store, id: &str) -> Result<()> {
    let source_path = locate_doc(store, id)?;
    let doc = parse_doc(&source_path)?;
    if doc.kind == "milestone" {
        let container = source_path
            .parent()
            .ok_or_else(|| Error::new("Invalid container path"))?;
        fs::remove_dir_all(container)?;
    } else {
        fs::remove_file(&source_path)?;
    }
    let rel_path = source_path
        .strip_prefix(&store.path)
        .map_err(|e| Error::new(e.to_string()))?
        .to_path_buf();
    backend::remove_path(store, &rel_path)?;
    backend::commit_paths(store, &[], &format!("Delete {}", doc.id))?;
    println!("Deleted {}", doc.id);
    Ok(())
}
fn run_log(mut args: Vec<String>) -> Result<()> {
    require_len(&args, 1, "log <id>")?;
    let id_spec = args.remove(0);
    let limit = pop_arg(&mut args, "--limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(20);
    let section = pop_arg(&mut args, "--section");
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id_spec)?;
    let path = locate_doc(&store, &id)?;
    let rel_path = path
        .strip_prefix(&store.path)
        .map_err(|e| Error::new(e.to_string()))?;
    if section.is_none() {
        print!("{}", backend::file_log(&store, &rel_path, limit)?);
        return Ok(());
    }
    let section_raw = section.unwrap();
    let marker = if section_raw.starts_with('#') {
        section_raw
    } else {
        format!("## {section_raw}")
    };
    let change_ids = backend::file_change_ids(&store, &rel_path, limit)?;
    let mut printed = 0usize;
    for id in change_ids {
        let details = backend::show_change(&store, &id, &rel_path)?;
        let section_hit = details.lines().any(|line| {
            line.contains(&marker)
                && (line.starts_with('+') || line.starts_with('-') || line.contains("Hunks"))
        });
        if section_hit {
            println!("Change {id}");
            for line in details.lines().take(30) {
                println!("{line}");
            }
            println!();
            printed += 1;
        }
    }
    if printed == 0 {
        println!("No matching section edits found for '{marker}'");
    }
    Ok(())
}
fn run_sync(mut args: Vec<String>) -> Result<()> {
    let store_flag = pop_arg(&mut args, "--store");
    let all = has_flag(&mut args, "--all");
    let (cfg, user_cfg, cwd) = load_context()?;
    if all {
        for store in cfg.stores {
            backend::sync(&store)?;
            println!("Synced {}", store.name);
        }
        return Ok(());
    }
    let store = resolve_store_with_context(&cfg, &user_cfg, &cwd, store_flag.as_deref())?;
    backend::sync(&store)?;
    println!("Synced {}", store.name);
    Ok(())
}
fn run_store(mut args: Vec<String>) -> Result<()> {
    if args.is_empty() {
        return Err(Error::new("Missing store subcommand"));
    }
    let sub = args.remove(0);
    match sub.as_str() {
        "init" => store_init(args),
        "list" => store_list(),
        "info" => store_info(args),
        "remove" => store_remove(args),
        _ => Err(Error::new(format!("Unknown store command: {sub}"))),
    }
}

fn store_init(mut args: Vec<String>) -> Result<()> {
    require_len(&args, 1, "store init <name>")?;
    let name = args.remove(0);
    let backend_s = pop_arg(&mut args, "--backend")
        .ok_or_else(|| Error::new("Missing --backend <jj|pijul> for store init"))?;
    let path = if let Some(path_arg) = pop_arg(&mut args, "--path") {
        PathBuf::from(path_arg)
    } else {
        default_store_path(&name)?
    };
    let backend_kind = BackendKind::parse(&backend_s)?;
    let set_default = has_flag(&mut args, "--default");
    backend::init_store(&path, backend_kind.clone())?;
    let mut cfg = Config::load()?;
    cfg.upsert_store(Store {
        name: name.clone(),
        backend: backend_kind,
        path: path.clone(),
    });
    if set_default || cfg.default_store.is_none() {
        cfg.default_store = Some(name.clone());
    }
    cfg.save()?;
    println!("Initialized store {name}");
    Ok(())
}

fn store_list() -> Result<()> {
    let cfg = Config::load()?;
    for store in cfg.stores {
        let marker = if cfg.default_store.as_deref() == Some(store.name.as_str()) {
            "*"
        } else {
            " "
        };
        println!(
            "{} {} {} {}",
            marker,
            store.name,
            store.backend.as_str(),
            store.path.display()
        );
    }
    Ok(())
}

fn store_info(mut args: Vec<String>) -> Result<()> {
    require_len(&args, 1, "store info <name>")?;
    let name = args.remove(0);
    let cfg = Config::load()?;
    let store = cfg.get_store(&name)?;
    println!("name={}", store.name);
    println!("backend={}", backend::adapter_name(&store));
    println!("path={}", store.path.display());
    println!("status:");
    print!("{}", backend::status(&store)?);
    let caps = backend::adapter_capabilities(&store);
    println!("capabilities:");
    println!("  cli_backed={}", caps.cli_backed);
    println!("  sdk_probe={}", caps.sdk_probe);
    println!("  file_scoped_log={}", caps.file_scoped_log);
    println!("  file_change_inspection={}", caps.file_change_inspection);
    println!("  sync_supported={}", caps.sync_supported);
    println!("  remove_path_supported={}", caps.remove_path_supported);
    Ok(())
}

fn store_remove(mut args: Vec<String>) -> Result<()> {
    require_len(&args, 1, "store remove <name>")?;
    let name = args.remove(0);
    let mut cfg = Config::load()?;
    let index = cfg
        .stores
        .iter()
        .position(|s| s.name == name)
        .ok_or_else(|| Error::new(format!("Unknown store '{name}'")))?;
    cfg.stores.remove(index);
    if cfg.default_store.as_deref() == Some(name.as_str()) {
        cfg.default_store = None;
    }
    cfg.save()?;
    println!("Removed store {name}");
    Ok(())
}
fn run_cache(mut args: Vec<String>) -> Result<()> {
    if args.is_empty() {
        return Err(Error::new("Missing cache subcommand"));
    }
    let sub = args.remove(0);
    match sub.as_str() {
        "rebuild" => cache_rebuild(args),
        "query" => cache_query(args),
        _ => Err(Error::new(format!("Unknown cache command: {sub}"))),
    }
}

fn cache_rebuild(mut args: Vec<String>) -> Result<()> {
    require_len(&args, 1, "cache rebuild <store>")?;
    let store_name = args.remove(0);
    let store = load_store(&store_name)?;
    cache::rebuild_cache(&store)?;
    println!("Cache rebuilt for {}", store.name);
    Ok(())
}

fn cache_query(mut args: Vec<String>) -> Result<()> {
    require_len(&args, 2, "cache query <store> <where-clause>")?;
    let store_name = args.remove(0);
    let where_clause = args.remove(0);
    let store = load_store(&store_name)?;
    let output = cache::query_cache(&store, &where_clause)?;
    print!("{output}");
    Ok(())
}

fn load_store(name: &str) -> Result<Store> {
    Config::load()?.get_store(name)
}
