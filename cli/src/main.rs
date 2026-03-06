mod user_config;
use atty::Stream;
use clap::{Parser, Subcommand};
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

#[derive(Debug, Parser)]
#[command(name = "runes", version, about = "A local-first issue tracker stored as markdown rune docs", propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Create a new rune doc (issue or milestone)
    New(NewArgs),
    /// List rune docs with optional filters
    List(ListArgs),
    /// Show a rune doc by ID
    Show(ShowArgs),
    /// Edit metadata on an existing rune doc
    Edit(EditArgs),
    /// Commit pending rune doc changes to the store backend
    Commit(CommitArgs),
    /// Move a rune doc to a different project
    Move(MoveArgs),
    /// Archive a rune doc
    Archive(ArchiveArgs),
    /// Delete a rune doc
    Delete(DeleteArgs),
    /// Show change log for a rune doc
    Log(LogArgs),
    /// Sync store with its backend
    Sync(SyncArgs),
    /// Manage stores
    #[command(subcommand)]
    Store(StoreCommand),
    /// Read and write config values
    #[command(subcommand)]
    Config(ConfigCommand),
    /// Initialize runes for a repo or globally
    Init(InitArgs),
}

#[derive(Debug, Subcommand)]
enum StoreCommand {
    /// Initialize a new store
    Init {
        /// Store name
        name: String,
        /// Backend type (e.g. pijul, jj)
        #[arg(long)]
        backend: String,
        /// Path to the store directory
        #[arg(long)]
        path: Option<PathBuf>,
        /// Set as the default store
        #[arg(long)]
        default: bool,
    },
    /// List configured stores
    List,
    /// Show store details
    Info {
        /// Store name
        name: String,
    },
    /// Remove a store from config
    Remove {
        /// Store name
        name: String,
    },
    /// Check store health and fix issues
    Doctor {
        /// Store name
        store: String,
    },
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// List all config values
    List {
        /// Show global config only
        #[arg(short, long)]
        global: bool,
    },
    /// Get a config value by key
    Get {
        /// Config key (e.g. user.email, defaults.store)
        key: String,
        /// Read from global config
        #[arg(short, long)]
        global: bool,
    },
    /// Set a config value
    Set {
        /// Config key (e.g. user.email, defaults.store)
        key: String,
        /// Value to set
        value: String,
        /// Write to global config
        #[arg(short, long)]
        global: bool,
    },
    /// Remove a config value
    Unset {
        /// Config key to remove
        key: String,
        /// Remove from global config
        #[arg(short, long)]
        global: bool,
    },
}

#[derive(Debug, Parser)]
struct InitArgs {
    /// Project prefix (optionally store:project)
    #[arg(long)]
    project: Option<String>,
    /// Add runes.kdl to .git/info/exclude instead of committing it
    #[arg(long)]
    stealth: bool,
}

#[derive(Debug, Parser)]
struct NewArgs {
    /// Title for the new rune doc
    title: String,
    /// Target project (or store:project)
    #[arg(long)]
    project: Option<String>,
    /// Store to create the doc in
    #[arg(long)]
    store: Option<String>,
    /// Doc type (e.g. issue, milestone)
    #[arg(long = "type")]
    command_type: Option<String>,
    /// Initial status
    #[arg(long)]
    status: Option<String>,
    /// Assignee
    #[arg(long)]
    assignee: Option<String>,
    /// Parent rune ID
    #[arg(long)]
    parent: Option<String>,
    /// Milestone ID to associate with
    #[arg(long)]
    milestone: Option<String>,
    /// Override the generated ID
    #[arg(long = "id")]
    id_override: Option<String>,
    /// Add a label (repeatable)
    #[arg(long = "label")]
    labels: Vec<String>,
    /// Add a relation e.g. "blocks:runes-x1" (repeatable)
    #[arg(long = "relation")]
    relations: Vec<String>,
    /// Replace body from file (use - for stdin)
    #[arg(short = 'f', long = "file")]
    file: Option<PathBuf>,
    /// Open editor after creation
    #[arg(short = 'e', long = "edit")]
    edit: bool,
    /// Skip auto-commit after creation
    #[arg(long = "no-commit")]
    no_commit: bool,
}

#[derive(Debug, Parser)]
struct ListArgs {
    /// Named query/view to apply
    #[arg(value_name = "view")]
    view: Option<String>,
    /// Store to list from
    #[arg(long)]
    store: Option<String>,
    /// Filter by project (or store:project; empty string for all)
    #[arg(long)]
    project: Option<String>,
    /// Named query from runes.kdl
    #[arg(long)]
    query: Option<String>,
    /// Filter by type (e.g. issues, milestones)
    #[arg(long = "type")]
    kind: Option<String>,
    /// Filter by status
    #[arg(long)]
    status: Option<String>,
    /// Filter by assignee
    #[arg(long)]
    assignee: Option<String>,
    /// Show only archived docs
    #[arg(long, conflicts_with = "with_archived")]
    archived: bool,
    /// Include archived docs in results
    #[arg(long = "with-archived", conflicts_with = "archived")]
    with_archived: bool,
}

#[derive(Debug, Parser)]
struct ShowArgs {
    /// Rune doc ID (or store:id)
    id: String,
}

#[derive(Debug, Parser)]
struct EditArgs {
    /// Rune doc ID (or store:id)
    id: String,
    /// Set the title
    #[arg(long)]
    title: Option<String>,
    /// Set the status
    #[arg(long)]
    status: Option<String>,
    /// Set the assignee (use "none" to clear)
    #[arg(long)]
    assignee: Option<String>,
    /// Add a label (repeatable)
    #[arg(long = "label")]
    add_labels: Vec<String>,
    /// Remove a label (repeatable)
    #[arg(long = "remove-label")]
    remove_labels: Vec<String>,
    /// Set the milestone
    #[arg(long)]
    milestone: Option<String>,
    /// Add a relation e.g. "blocks:runes-x1" (repeatable)
    #[arg(long = "relation")]
    add_relations: Vec<String>,
    /// Remove a relation (repeatable)
    #[arg(long = "remove-relation")]
    remove_relations: Vec<String>,
    /// Replace body from file (use - for stdin)
    #[arg(short = 'f', long = "file")]
    file: Option<PathBuf>,
    /// Open editor
    #[arg(short = 'e', long = "edit")]
    edit: bool,
    /// Skip auto-commit after edit
    #[arg(long = "no-commit")]
    no_commit: bool,
}

#[derive(Debug, Parser)]
struct CommitArgs {
    /// Store or store:project to commit (defaults to all pending)
    target: Option<String>,
}

#[derive(Debug, Parser)]
struct MoveArgs {
    /// Rune doc ID to move
    id: String,
    /// Destination project
    #[arg(long = "project")]
    target_project: String,
    /// New parent rune ID in the destination project
    #[arg(long)]
    parent: Option<String>,
}

#[derive(Debug, Parser)]
struct ArchiveArgs {
    /// Rune doc ID to archive
    id: String,
}

#[derive(Debug, Parser)]
struct DeleteArgs {
    /// Rune doc ID to delete
    id: String,
    /// Skip confirmation prompt
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Parser)]
struct LogArgs {
    /// Rune doc ID
    id: String,
    /// Max number of entries to show
    #[arg(long)]
    limit: Option<usize>,
    /// Filter to a specific section
    #[arg(long)]
    section: Option<String>,
}

#[derive(Debug, Parser)]
struct SyncArgs {
    /// Store to sync
    #[arg(long)]
    store: Option<String>,
    /// Sync all configured stores
    #[arg(long)]
    all: bool,
}

fn main() {
    set_context(InteractiveContext::Terminal);
    let cli = Cli::parse();
    if let Err(err) = handle_command(cli.command) {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn handle_command(command: CliCommand) -> Result<()> {
    match command {
        CliCommand::New(args) => run_new(args),
        CliCommand::List(args) => run_list(args),
        CliCommand::Show(args) => run_show(args),
        CliCommand::Edit(args) => run_edit(args),
        CliCommand::Commit(args) => run_commit(args),
        CliCommand::Move(args) => run_move(args),
        CliCommand::Archive(args) => run_archive(args),
        CliCommand::Delete(args) => run_delete(args),
        CliCommand::Log(args) => run_log(args),
        CliCommand::Sync(args) => run_sync(args),
        CliCommand::Store(store_cmd) => run_store(store_cmd),
        CliCommand::Config(config_cmd) => run_config(config_cmd),
        CliCommand::Init(args) => run_init(args),
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

fn load_context() -> Result<(Config, UserConfig, PathBuf)> {
    let mut config = Config::load()?;
    let cwd = std::env::current_dir().map_err(|e| Error::new(e.to_string()))?;
    let user_cfg = UserConfig::load_from_dir(&cwd)?;
    // Merge store definitions from KDL config into Config
    for store_def in &user_cfg.stores {
        if !store_def.backend.is_empty() && !store_def.path.is_empty() {
            if let Ok(backend) = BackendKind::parse(&store_def.backend) {
                let store = Store {
                    name: store_def.name.clone(),
                    backend,
                    path: PathBuf::from(&store_def.path),
                };
                config.upsert_store(store);
            }
        }
    }
    // Also use default_store from user config if config.txt doesn't have one
    if config.default_store.is_none() {
        if let Some(ds) = &user_cfg.default_store {
            config.default_store = Some(ds.clone());
        }
    }
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
            ListKind::Issues => "task",
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
fn run_new(args: NewArgs) -> Result<()> {
    let NewArgs {
        title,
        project: project_arg,
        store: store_hint,
        command_type,
        status: status_flag,
        assignee,
        parent,
        milestone,
        id_override,
        labels,
        relations,
        file,
        edit,
        no_commit,
    } = args;
    let relation_inputs = relations;
    let (cfg, user_cfg, cwd) = load_context()?;
    let creation_defaults = user_cfg.creation_defaults();
    let kind_value = command_type
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
    let mut combined_labels = creation_defaults.labels.clone();
    combined_labels.extend(labels);
    let assignee_value = assignee
        .as_deref()
        .map(|s| s.to_string())
        .or_else(|| creation_defaults.assignee.clone());
    let resolved_assignee = assignee_value
        .as_deref()
        .and_then(|value| user_cfg.resolve_user_alias(value));
    let (store, project_name) = resolve_project_for_new(
        &cfg,
        &user_cfg,
        &cwd,
        store_hint.as_deref(),
        project_arg.as_ref(),
    )?;
    let relations = parse_relations(&relation_inputs)?;
    if file.is_some() && edit {
        return Err(Error::new("Cannot use both --file and --edit"));
    }
    let (identifier, doc_path) = if kind == "milestone" {
        create_milestone(
            &store,
            &project_name,
            &title,
            &status,
            &combined_labels,
            id_override.as_deref(),
        )?
    } else {
        create_issue(
            &store,
            &project_name,
            &title,
            &status,
            parent.as_deref(),
            milestone.as_deref(),
            &combined_labels,
            &relations,
            resolved_assignee.as_deref(),
            id_override.as_deref(),
        )?
    };
    if let Some(file_path) = file {
        let contents = if file_path == Path::new("-") {
            read_from_stdin()?
        } else {
            fs::read_to_string(&file_path)?
        };
        let mut doc = parse_doc(&doc_path)?;
        doc.body = contents;
        fs::write(&doc_path, render_doc(&doc))?;
    } else if edit {
        open_editor(&doc_path)?;
    }
    if !no_commit {
        commit_paths(&store, &[doc_path.clone()], &format!("Add {identifier}"))?;
    } else {
        print_uncommitted_hint(&store, &identifier, &doc_path);
    }
    println!("{identifier}");
    Ok(())
}

fn resolve_store_and_project_from_spec(
    config: &Config,
    user_config: &UserConfig,
    cwd: &Path,
    store_hint: Option<&str>,
    spec: &str,
) -> Result<(Store, String)> {
    let trimmed_spec = spec.trim();
    if trimmed_spec.is_empty() {
        return Err(Error::new("Project name may not be empty"));
    }
    let (project_store_hint, project_value) = split_store_prefix(trimmed_spec);
    let project_trimmed = project_value.trim();
    if project_trimmed.is_empty() {
        return Err(Error::new("Project name may not be empty"));
    }
    let override_hint = project_store_hint
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let hint = override_hint.or(store_hint);
    let store = resolve_store_with_context(config, user_config, cwd, hint)?;
    Ok((store, project_trimmed.to_string()))
}

fn resolve_project_for_new(
    config: &Config,
    user_config: &UserConfig,
    cwd: &Path,
    store_hint: Option<&str>,
    project_arg: Option<&String>,
) -> Result<(Store, String)> {
    if let Some(spec) = project_arg {
        return resolve_store_and_project_from_spec(config, user_config, cwd, store_hint, spec);
    }
    if let Ok(env_value) = std::env::var("RUNES_PROJECT") {
        let trimmed = env_value.trim();
        if !trimmed.is_empty() {
            return resolve_store_and_project_from_spec(
                config,
                user_config,
                cwd,
                store_hint,
                trimmed,
            );
        }
    }
    if let Some(default_spec) = user_config.default_project.as_deref() {
        let trimmed = default_spec.trim();
        if !trimmed.is_empty() {
            return resolve_store_and_project_from_spec(
                config,
                user_config,
                cwd,
                store_hint,
                trimmed,
            );
        }
    }
    let store = resolve_store_with_context(config, user_config, cwd, store_hint)?;
    let projects = all_projects(&store)?;
    if let Some(name) = cwd.file_name().and_then(|n| n.to_str()) {
        if projects.iter().any(|proj| proj == name) {
            return Ok((store, name.to_string()));
        }
    }
    if let Some(repo_name) = repo_root_basename(cwd) {
        if projects.iter().any(|proj| proj == &repo_name) {
            return Ok((store, repo_name));
        }
    }
    Err(Error::new(
        "Project not specified; provide --project, set RUNES_PROJECT/default_project, \
        or run from a directory whose name matches a project.",
    ))
}

fn repo_root_basename(start: &Path) -> Option<String> {
    find_repo_root(start).and_then(|root| {
        root.file_name()
            .and_then(|name| name.to_str())
            .map(|value| value.to_string())
    })
}

fn find_repo_root(start: &Path) -> Option<PathBuf> {
    let mut cursor = start.to_path_buf();
    loop {
        if has_vcs_marker(&cursor) {
            return Some(cursor);
        }
        if !cursor.pop() {
            return None;
        }
    }
}

fn has_vcs_marker(path: &Path) -> bool {
    path.join(".git").exists()
        || path.join(".jj").exists()
        || path.join(".pjul").exists()
        || path.join(".pj").exists()
}

fn run_list(args: ListArgs) -> Result<()> {
    let ListArgs {
        view,
        store,
        project,
        query,
        kind,
        status,
        assignee,
        archived,
        with_archived,
    } = args;
    let mut archived_mode = if archived {
        ArchivedMode::Only
    } else if with_archived {
        ArchivedMode::Include
    } else {
        ArchivedMode::Exclude
    };
    let (cfg, user_cfg, cwd) = load_context()?;
    let project_flag_present = project.is_some();
    let effective_project = project.filter(|p| !p.is_empty());
    let (store, project_proj) =
        resolve_store_and_project(&cfg, &user_cfg, &cwd, store.as_deref(), effective_project.as_ref())?;
    let status_flag_present = status.is_some();
    let type_flag_present = kind.is_some();
    let assignee_filter = assignee
        .as_deref()
        .and_then(|value| user_cfg.resolve_user_alias(value));
    let mut list_kind = kind
        .as_deref()
        .map(ListKind::parse)
        .unwrap_or(ListKind::Issues);
    let mut filters = IssueFilters {
        project: project_proj,
        statuses: status
            .as_ref()
            .map(|value| vec![value.clone()])
            .unwrap_or_else(Vec::new),
        kind: None,
        assignee: assignee_filter,
        archived: archived_mode,
    };
    let query_name = view
        .or(query)
        .or_else(|| user_cfg.query_for_path(&cwd))
        .or_else(|| user_cfg.default_query.clone());
    let mut query_set_project = false;
    if let Some(query_key) = query_name {
        if let Some(query_cfg) = user_cfg.query(&query_key) {
            if !project_flag_present {
                if query_cfg.project.is_some() {
                    query_set_project = true;
                }
                filters.project = query_cfg.project.clone();
            }
            if !status_flag_present {
                filters.statuses = query_cfg.statuses.clone();
            }
            if !type_flag_present {
                if let Some(kind_value) = &query_cfg.kind {
                    list_kind = ListKind::parse(kind_value);
                }
            }
            if !archived && !with_archived {
                if let Some(archived_value) = &query_cfg.archived {
                    if let Some(parsed) = ArchivedMode::from_keyword(archived_value) {
                        archived_mode = parsed;
                    }
                }
            }
            if filters.assignee.is_none() {
                if let Some(query_assignee) = &query_cfg.assignee {
                    filters.assignee = user_cfg.resolve_user_alias(query_assignee);
                }
            }
        }
    }
    // Empty project means "any project" (overrides default_project)
    if filters.project.as_deref() == Some("") {
        filters.project = None;
    } else if filters.project.is_none() && !project_flag_present && !query_set_project {
        if let Some(default_spec) = user_cfg.default_project.as_deref() {
            let (_, proj_name) = split_store_prefix(default_spec);
            if !proj_name.is_empty() {
                filters.project = Some(proj_name.to_string());
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
fn run_show(args: ShowArgs) -> Result<()> {
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &args.id)?;
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
fn run_edit(args: EditArgs) -> Result<()> {
    let EditArgs {
        id,
        title,
        status,
        assignee,
        add_labels,
        remove_labels,
        milestone,
        add_relations,
        remove_relations,
        file,
        edit,
        no_commit,
    } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id)?;
    let path = locate_doc(&store, &id)?;
    let mut doc = parse_doc(&path)?;
    let mut final_path = path.clone();
    let has_field_edits = title.is_some()
        || status.is_some()
        || assignee.is_some()
        || !add_labels.is_empty()
        || !remove_labels.is_empty()
        || milestone.is_some()
        || !add_relations.is_empty()
        || !remove_relations.is_empty();
    if file.is_some() && edit {
        return Err(Error::new("Cannot use both --file and --edit"));
    }
    if (file.is_some() || edit) && has_field_edits {
        return Err(Error::new(
            "Cannot mix field edits with --file or --edit",
        ));
    }
    if has_field_edits {
        if let Some(status_value) = status {
            doc.status = status_value;
        }
        if let Some(assignee_value) = assignee {
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
        let added = parse_relations(&add_relations)?;
        for (kind, target) in added {
            if !doc.relations.iter().any(|(existing_kind, existing_id)| {
                existing_kind == &kind && existing_id == &target
            }) {
                doc.relations.push((kind, target));
            }
        }
        let removed = parse_relations(&remove_relations)?;
        for (kind, target) in removed {
            doc.relations.retain(|(existing_kind, existing_id)| {
                existing_kind != &kind || existing_id != &target
            });
        }
        if let Some(title_value) = title {
            doc.title = title_value.clone();
            doc.body = replace_title(&doc.body, &title_value);
            let parsed = parse_full_id(&doc.id)?;
            let new_name = format!("{}--{}.md", parsed.short, slugify(&title_value));
            final_path = path
                .parent()
                .ok_or_else(|| Error::new("Invalid issue path"))?
                .join(new_name);
        }
        fs::write(&path, render_doc(&doc))?;
        if final_path != path {
            fs::rename(&path, &final_path)?;
        }
    } else if let Some(file_path) = file {
        let contents = if file_path == Path::new("-") {
            read_from_stdin()?
        } else {
            fs::read_to_string(&file_path)?
        };
        fs::write(&path, contents)?;
        doc = parse_doc(&path)?;
        final_path = path.clone();
    } else if edit || editor_available() {
        open_editor(&path)?;
        doc = parse_doc(&path)?;
        final_path = path.clone();
    } else {
        return Err(Error::new("No edits specified and no editor available"));
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

fn run_commit(args: CommitArgs) -> Result<()> {
    let target = args.target;
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
fn run_move(args: MoveArgs) -> Result<()> {
    let MoveArgs {
        id,
        target_project,
        parent,
    } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let (from_store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id)?;
    let (to_store, project) =
        resolve_store_and_project_required(&cfg, &user_cfg, &cwd, None, &target_project)?;
    move_rune(&from_store, &to_store, &id, &project, parent.as_deref())
}
fn run_archive(args: ArchiveArgs) -> Result<()> {
    let ArchiveArgs { id } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id)?;
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
fn run_delete(args: DeleteArgs) -> Result<()> {
    let DeleteArgs { id, force } = args;
    if !force {
        return Err(Error::new("Use --force to delete runes"));
    }
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id)?;
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
fn run_log(args: LogArgs) -> Result<()> {
    let LogArgs { id, limit, section } = args;
    let limit = limit.unwrap_or(20);
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id)?;
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
    for change_id in change_ids {
        let details = backend::show_change(&store, &change_id, &rel_path)?;
        let section_hit = details.lines().any(|line| {
            line.contains(&marker)
                && (line.starts_with('+') || line.starts_with('-') || line.contains("Hunks"))
        });
        if section_hit {
            println!("Change {change_id}");
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
fn run_sync(args: SyncArgs) -> Result<()> {
    let SyncArgs { store, all } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    if all {
        for store in cfg.stores {
            backend::sync(&store)?;
            println!("Synced {}", store.name);
        }
        return Ok(());
    }
    let store = resolve_store_with_context(&cfg, &user_cfg, &cwd, store.as_deref())?;
    backend::sync(&store)?;
    println!("Synced {}", store.name);
    Ok(())
}

fn run_store(command: StoreCommand) -> Result<()> {
    match command {
        StoreCommand::Init {
            name,
            backend,
            path,
            default,
        } => store_init(name, backend, path, default),
        StoreCommand::List => store_list(),
        StoreCommand::Info { name } => store_info(name),
        StoreCommand::Remove { name } => store_remove(name),
        StoreCommand::Doctor { store } => cache_rebuild(store),
    }
}
fn store_init(
    name: String,
    backend_s: String,
    path: Option<PathBuf>,
    set_default: bool,
) -> Result<()> {
    let path = if let Some(path_arg) = path {
        path_arg
    } else {
        default_store_path(&name)?
    };
    let backend_kind = BackendKind::parse(&backend_s)?;
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

fn store_info(name: String) -> Result<()> {
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

fn store_remove(name: String) -> Result<()> {
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
fn cache_rebuild(store_name: String) -> Result<()> {
    let store = load_store(&store_name)?;
    cache::rebuild_cache(&store)?;
    println!("Cache rebuilt for {}", store.name);
    Ok(())
}

fn load_store(name: &str) -> Result<Store> {
    Config::load()?.get_store(name)
}

fn run_config(cmd: ConfigCommand) -> Result<()> {
    let cwd = std::env::current_dir().map_err(|e| Error::new(e.to_string()))?;
    match cmd {
        ConfigCommand::List { global } => {
            if global {
                let path = user_config::global_config_path()?;
                let pairs = user_config::config_list(&path)?;
                for (k, v) in pairs {
                    println!("{k}={v}");
                }
            } else {
                // Show merged: global then local
                let global_path = user_config::global_config_path()?;
                let local_path = user_config::local_config_path(&cwd);
                let mut pairs = user_config::config_list(&global_path)?;
                if let Some(lp) = local_path {
                    let local_pairs = user_config::config_list(&lp)?;
                    for (k, v) in local_pairs {
                        if let Some(existing) = pairs.iter_mut().find(|(ek, _)| ek == &k) {
                            existing.1 = v;
                        } else {
                            pairs.push((k, v));
                        }
                    }
                }
                for (k, v) in pairs {
                    println!("{k}={v}");
                }
            }
            Ok(())
        }
        ConfigCommand::Get { key, global } => {
            let path = if global {
                user_config::global_config_path()?
            } else {
                // Check local first, then global
                let local = user_config::local_config_path(&cwd);
                if let Some(lp) = &local {
                    if let Some(val) = user_config::config_get(lp, &key)? {
                        println!("{val}");
                        return Ok(());
                    }
                }
                user_config::global_config_path()?
            };
            match user_config::config_get(&path, &key)? {
                Some(val) => println!("{val}"),
                None => return Err(Error::new(format!("Key '{key}' not found"))),
            }
            Ok(())
        }
        ConfigCommand::Set { key, value, global } => {
            let path = if global {
                user_config::global_config_path()?
            } else {
                user_config::local_config_path(&cwd)
                    .ok_or_else(|| Error::new("Not in a repo. Use --global or run from a repo root."))?
            };
            user_config::config_set(&path, &key, &value)?;
            Ok(())
        }
        ConfigCommand::Unset { key, global } => {
            let path = if global {
                user_config::global_config_path()?
            } else {
                user_config::local_config_path(&cwd)
                    .ok_or_else(|| Error::new("Not in a repo. Use --global or run from a repo root."))?
            };
            user_config::config_unset(&path, &key)?;
            Ok(())
        }
    }
}

fn run_init(args: InitArgs) -> Result<()> {
    let cwd = std::env::current_dir().map_err(|e| Error::new(e.to_string()))?;
    let global_path = user_config::global_config_path()?;

    // Ensure global config exists
    if !global_path.exists() {
        if !stdin_is_tty() {
            return Err(Error::new(
                "Global config not found. Run `runes init` interactively to create it.",
            ));
        }
        println!("Creating global config at {}", global_path.display());

        // Prompt for default store name
        eprint!("Default store name [proj]: ");
        let mut store_name = String::new();
        io::stdin().read_line(&mut store_name).map_err(|e| Error::new(e.to_string()))?;
        let store_name = store_name.trim();
        let store_name = if store_name.is_empty() { "proj" } else { store_name };

        // Prompt for backend
        eprint!("Backend (jj or pijul) [jj]: ");
        let mut backend_input = String::new();
        io::stdin().read_line(&mut backend_input).map_err(|e| Error::new(e.to_string()))?;
        let backend_input = backend_input.trim();
        let backend = if backend_input.is_empty() { "jj" } else { backend_input };
        BackendKind::parse(backend)?;

        // Prompt for user email
        eprint!("User email: ");
        let mut email = String::new();
        io::stdin().read_line(&mut email).map_err(|e| Error::new(e.to_string()))?;
        let email = email.trim().to_string();

        // Create global config
        user_config::config_set(&global_path, "user.email", &email)?;
        user_config::config_set(&global_path, "defaults.store", store_name)?;

        let store_path = default_store_path(store_name)?;
        user_config::config_set(&global_path, &format!("store.{store_name}.backend"), backend)?;
        user_config::config_set(
            &global_path,
            &format!("store.{store_name}.path"),
            &store_path.display().to_string(),
        )?;

        // Create default new and query nodes
        user_config::config_set(&global_path, "new.task.assignee", "self")?;
        user_config::config_set(&global_path, "query.open.status", "todo")?;
        user_config::config_set(&global_path, "query.mine.assignee", "self")?;
        user_config::config_set(&global_path, "query.mine.status", "todo")?;
        user_config::config_set(&global_path, "defaults.query", "open")?;

        // Initialize the store
        let backend_kind = BackendKind::parse(backend)?;
        backend::init_store(&store_path, backend_kind.clone())?;
        let mut cfg = Config::load()?;
        cfg.upsert_store(Store {
            name: store_name.to_string(),
            backend: backend_kind,
            path: store_path,
        });
        if cfg.default_store.is_none() {
            cfg.default_store = Some(store_name.to_string());
        }
        cfg.save()?;
        println!("Global config created.");
    } else {
        println!("Global config already exists at {}", global_path.display());
    }

    // Create local config if in a repo
    let repo_root = find_repo_root(&cwd);
    if let Some(root) = repo_root {
        let local_path = root.join("runes.kdl");
        if !local_path.exists() {
            let project = if let Some(spec) = &args.project {
                let (_store_hint, proj) = split_store_prefix(spec);
                proj.to_string()
            } else if stdin_is_tty() {
                let default_name = root
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("myproject");
                eprint!("Project prefix [{}]: ", default_name);
                let mut input = String::new();
                io::stdin().read_line(&mut input).map_err(|e| Error::new(e.to_string()))?;
                let input = input.trim();
                if input.is_empty() {
                    default_name.to_string()
                } else {
                    input.to_string()
                }
            } else {
                return Err(Error::new(
                    "Use --project to specify the project prefix non-interactively.",
                ));
            };

            // If project spec included a store, set that too
            if let Some(spec) = &args.project {
                let (store_hint, _) = split_store_prefix(spec);
                if let Some(store) = store_hint {
                    user_config::config_set(&local_path, "defaults.store", &store)?;
                }
            }
            user_config::config_set(&local_path, "defaults.project", &project)?;

            if args.stealth {
                let exclude_path = root.join(".git").join("info").join("exclude");
                if exclude_path.parent().map_or(false, |p| p.exists()) {
                    let existing = fs::read_to_string(&exclude_path).unwrap_or_default();
                    if !existing.lines().any(|l| l.trim() == "runes.kdl") {
                        let mut content = existing;
                        if !content.ends_with('\n') && !content.is_empty() {
                            content.push('\n');
                        }
                        content.push_str("runes.kdl\n");
                        fs::write(&exclude_path, content)?;
                        println!("Added runes.kdl to .git/info/exclude");
                    }
                }
            }
            println!("Local config created at {}", local_path.display());
        } else {
            println!("Local config already exists at {}", local_path.display());
        }
    } else {
        println!("Not in a repo; skipping local config.");
    }

    Ok(())
}
