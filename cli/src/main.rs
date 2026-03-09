mod color;
mod user_config;
use atty::Stream;
use clap::{Parser, Subcommand};
use pijul_interaction::{set_context, InteractiveContext};
use runes_core::backend::{self, LogEntry};
use runes_core::cache;
use runes_core::config::{ensure_dir, BackendKind, Config, Store};
use runes_core::model::{
    discover_project_docs, ensure_title, new_issue_doc, new_milestone_doc, next_short_id, parse_doc,
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
    command: Option<CliCommand>,
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
    /// Show change log for store or a specific rune doc
    Log(LogArgs),
    /// Show diff for a rune doc at a revision or between revisions
    Diff(DiffArgs),
    /// Restore a rune doc to a previous revision
    Restore(RestoreArgs),
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
    /// Add a comment to a rune doc
    Comment(CommentArgs),
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
    /// Commit message (implies commit)
    #[arg(short = 'm', long = "message")]
    message: Option<String>,
}

#[derive(Debug, Default, Parser)]
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
    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Parser)]
struct ShowArgs {
    /// Rune doc ID (or store:id)
    id: String,
    /// Show rune at a specific revision
    #[arg(long)]
    revision: Option<String>,
    /// Output as JSON
    #[arg(long)]
    json: bool,
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
    /// Commit message (implies commit)
    #[arg(short = 'm', long = "message")]
    message: Option<String>,
}

#[derive(Debug, Parser)]
struct CommitArgs {
    /// Rune ID to commit (commits just that rune)
    target: Option<String>,
    /// Commit all runes in a specific store
    #[arg(long = "store")]
    store: Option<String>,
    /// Commit all runes in a specific project (within the default store)
    #[arg(long = "project")]
    project: Option<String>,
    /// Commit message
    #[arg(short = 'm', long = "message")]
    message: Option<String>,
    /// Override commit author (email or "Name <email>")
    #[arg(long)]
    author: Option<String>,
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
    /// Skip auto-commit after move
    #[arg(long = "no-commit")]
    no_commit: bool,
    /// Commit message (implies commit)
    #[arg(short = 'm', long = "message")]
    message: Option<String>,
}

#[derive(Debug, Parser)]
struct ArchiveArgs {
    /// Rune doc ID to archive
    id: String,
    /// Skip auto-commit after archive
    #[arg(long = "no-commit")]
    no_commit: bool,
    /// Commit message (implies commit)
    #[arg(short = 'm', long = "message")]
    message: Option<String>,
}

#[derive(Debug, Parser)]
struct DeleteArgs {
    /// Rune doc ID to delete
    id: String,
    /// Skip confirmation prompt
    #[arg(long)]
    force: bool,
    /// Skip auto-commit after delete
    #[arg(long = "no-commit")]
    no_commit: bool,
    /// Commit message (implies commit)
    #[arg(short = 'm', long = "message")]
    message: Option<String>,
}

#[derive(Debug, Parser)]
struct LogArgs {
    /// Project name or rune ID (project:shortid); omit for default project log
    id: Option<String>,
    /// Max number of entries to show
    #[arg(long)]
    limit: Option<usize>,
    /// Filter to a specific section (requires rune ID)
    #[arg(long)]
    section: Option<String>,
    /// Filter by change author
    #[arg(long = "changed-by")]
    changed_by: Option<String>,
    /// Output as JSON
    #[arg(long)]
    json: bool,
    /// Disable pager
    #[arg(long)]
    no_pager: bool,
    /// Show all projects (ignore default project)
    #[arg(long)]
    all: bool,
}

#[derive(Debug, Parser)]
struct DiffArgs {
    /// Rune doc ID
    id: String,
    /// Show what changed in this specific revision
    #[arg(short = 'r', long = "revision", conflicts_with_all = ["from", "to"])]
    revision: Option<String>,
    /// Diff from this revision (to working copy, or to --to revision)
    #[arg(long)]
    from: Option<String>,
    /// Diff to this revision (requires --from)
    #[arg(long, requires = "from")]
    to: Option<String>,
}

#[derive(Debug, Parser)]
struct RestoreArgs {
    /// Rune doc ID to restore
    id: String,
    /// Revision to restore from
    #[arg(long)]
    revision: String,
    /// Skip auto-commit after restore
    #[arg(long = "no-commit")]
    no_commit: bool,
    /// Commit message (implies commit)
    #[arg(short = 'm', long = "message")]
    message: Option<String>,
}

#[derive(Debug, Parser)]
struct CommentArgs {
    /// Rune doc ID (or store:id)
    id: String,
    /// Comment text
    #[arg(short = 'm', long = "message")]
    message: Option<String>,
    /// Read comment from file (use - for stdin)
    #[arg(short = 'f', long = "file", conflicts_with = "message")]
    file: Option<PathBuf>,
    /// Skip auto-commit after commenting
    #[arg(long = "no-commit")]
    no_commit: bool,
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
    let command = cli.command.unwrap_or(CliCommand::List(ListArgs::default()));
    if let Err(err) = handle_command(command) {
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
        CliCommand::Diff(args) => run_diff(args),
        CliCommand::Restore(args) => run_restore(args),
        CliCommand::Sync(args) => run_sync(args),
        CliCommand::Store(store_cmd) => run_store(store_cmd),
        CliCommand::Config(config_cmd) => run_config(config_cmd),
        CliCommand::Init(args) => run_init(args),
        CliCommand::Comment(args) => run_comment(args),
    }
}
fn home_dir() -> Result<PathBuf> {
    Ok(PathBuf::from(
        std::env::var("HOME").map_err(|_| Error::new("HOME not set"))?,
    ))
}

fn default_store_path(name: &str) -> Result<PathBuf> {
    Ok(home_dir()?.join(".runes").join("stores").join(name))
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

fn query_issues(store: &Store, filters: IssueFilters) -> Result<Vec<cache::CacheRow>> {
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

/// Parse an author string: "Name <email>" or just "email"
fn parse_author_string(s: &str) -> (String, String) {
    let s = s.trim();
    if let Some(start) = s.find('<') {
        if let Some(end) = s.find('>') {
            let name = s[..start].trim().to_string();
            let email = s[start + 1..end].trim().to_string();
            return (name, email);
        }
    }
    // Treat entire string as email, use email as name fallback
    (s.to_string(), s.to_string())
}

/// Resolve commit author from: override flag > RUNES_USER env > config
fn resolve_commit_author(user_cfg: &UserConfig, author_override: Option<&str>) -> Result<(String, String)> {
    if let Some(author_str) = author_override {
        return Ok(parse_author_string(author_str));
    }
    if let Ok(env_val) = std::env::var("RUNES_USER") {
        return Ok(parse_author_string(&env_val));
    }
    if let Some(email) = &user_cfg.identity_email {
        let name = user_cfg.identity_name.as_deref().unwrap_or(email);
        return Ok((name.to_string(), email.clone()));
    }
    Err(Error::new(
        "No author configured. Set user.email in runes config, RUNES_USER env var, or use --author flag."
    ))
}

fn commit_store_changes(store: &Store, paths: &[PathBuf], message: &str, author_name: &str, author_email: &str) -> Result<()> {
    backend::commit_paths(store, paths, message, author_name, author_email)?;
    cache::rebuild_cache(store)?;
    Ok(())
}

/// Build a compact change description from an old and new RuneDoc.
/// Returns snippets like "in-progress", "assign to alice", "description", "comments", etc.
fn edit_change_snippets(old: &RuneDoc, new: &RuneDoc) -> Vec<String> {
    let mut snippets = Vec::new();
    // Status change (highest priority)
    if old.status != new.status {
        snippets.push(new.status.clone());
    }
    // Assignee change
    if old.assignee != new.assignee {
        match &new.assignee {
            Some(a) => snippets.push(format!("assign to {a}")),
            None => snippets.push("unassign".to_string()),
        }
    }
    // Label changes
    let added_labels: Vec<_> = new.labels.iter().filter(|l| !old.labels.contains(l)).collect();
    let removed_labels: Vec<_> = old.labels.iter().filter(|l| !new.labels.contains(l)).collect();
    if !added_labels.is_empty() || !removed_labels.is_empty() {
        snippets.push("labels".to_string());
    }
    // Milestone change
    if old.milestone != new.milestone {
        snippets.push("milestone".to_string());
    }
    // Relation changes
    if old.relations != new.relations {
        snippets.push("relations".to_string());
    }
    // Body/section changes — detect which sections changed
    let old_sections = body_section_names(&old.body);
    let new_sections = body_section_names(&new.body);
    // Check for changed section content
    for section in &new_sections {
        let old_content = extract_section_content(&old.body, section);
        let new_content = extract_section_content(&new.body, section);
        if old_content != new_content {
            snippets.push(section.to_lowercase());
        }
    }
    // Check for new sections
    for section in &new_sections {
        if !old_sections.contains(section) && !snippets.iter().any(|s| s == &section.to_lowercase()) {
            snippets.push(section.to_lowercase());
        }
    }
    // If body changed but no section-level diff caught it, say "description"
    if old.body != new.body && snippets.iter().all(|s| {
        !["description", "design", "comments", "notes", "acceptance criteria"].contains(&s.as_str())
    }) {
        // Check if the non-section body content changed
        let old_main = extract_section_content(&old.body, "");
        let new_main = extract_section_content(&new.body, "");
        if old_main != new_main {
            snippets.push("description".to_string());
        }
    }
    // Extra frontmatter
    if old.frontmatter_extra != new.frontmatter_extra {
        snippets.push("meta".to_string());
    }
    snippets
}

/// Extract `## Section` names from a body.
fn body_section_names(body: &str) -> Vec<String> {
    body.lines()
        .filter_map(|line| {
            line.strip_prefix("## ").map(|rest| rest.trim().to_string())
        })
        .collect()
}

/// Extract content of a named section (or main body if name is empty).
fn extract_section_content<'a>(body: &'a str, section_name: &str) -> String {
    let mut collecting = section_name.is_empty();
    let mut content = String::new();
    for line in body.lines() {
        if let Some(heading) = line.strip_prefix("## ") {
            if section_name.is_empty() {
                // Stop at first ## heading for main body
                break;
            }
            if heading.trim() == section_name {
                collecting = true;
                continue;
            } else if collecting {
                break;
            }
        }
        if collecting {
            content.push_str(line);
            content.push('\n');
        }
    }
    content
}

/// Build a commit message with verb, id, and optional change snippets, capped at ~100 chars.
fn build_commit_message(verb: &str, id: &str, snippets: &[String]) -> String {
    let prefix = format!("{verb} {id}");
    if snippets.is_empty() {
        return prefix;
    }
    let joined = snippets.join(", ");
    let full = format!("{prefix}: {joined}");
    if full.len() <= 100 {
        return full;
    }
    // Truncate by including snippets until we'd exceed the limit
    let mut msg = prefix.clone();
    msg.push_str(": ");
    let budget = 100 - msg.len();
    let mut remaining = budget;
    let mut included = 0;
    for (i, snippet) in snippets.iter().enumerate() {
        let sep_len = if i > 0 { 2 } else { 0 }; // ", "
        let needed = sep_len + snippet.len();
        if needed > remaining {
            break;
        }
        if i > 0 {
            msg.push_str(", ");
        }
        msg.push_str(snippet);
        remaining -= needed;
        included += 1;
    }
    if included == 0 {
        // First snippet itself is too long, truncate it
        let mut truncated = snippets[0].clone();
        truncated.truncate(budget.saturating_sub(3));
        msg.push_str(&truncated);
        msg.push_str("...");
    }
    msg
}

fn reconcile_filename(path: &Path, full_id: &str) -> Result<PathBuf> {
    let doc = parse_doc(path)?;
    let parsed = parse_full_id(full_id)?;
    let expected_name = format!("{}--{}.md", parsed.short, slugify(&doc.title));
    let current_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    if current_name == "_milestone.md" || current_name == expected_name {
        return Ok(path.to_path_buf());
    }
    let new_path = path
        .parent()
        .ok_or_else(|| Error::new("Invalid issue path"))?
        .join(&expected_name);
    fs::rename(path, &new_path)?;
    Ok(new_path)
}

fn maybe_commit(
    store: &Store,
    no_commit: bool,
    user_message: Option<&str>,
    default_message: &str,
    user_cfg: &UserConfig,
    rune_path: Option<&Path>,
) -> Result<()> {
    if no_commit && user_message.is_none() {
        eprintln!("hint: uncommitted changes pending. Will be included in next commit or `runes commit`.");
        return Ok(());
    }
    let msg = user_message.unwrap_or(default_message);
    let (author_name, author_email) = resolve_commit_author(user_cfg, None)?;
    let paths: Vec<PathBuf> = match rune_path {
        Some(p) => {
            let rel = p.strip_prefix(&store.path).unwrap_or(p);
            vec![rel.to_path_buf()]
        }
        None => vec![],
    };
    commit_store_changes(store, &paths, msg, &author_name, &author_email)
}

fn warn_if_uncommitted(store: &Store) {
    if let Ok(true) = backend::has_uncommitted_changes(store) {
        eprintln!("hint: store has uncommitted changes. Run `runes commit` to commit them.");
    }
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
        message,
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
        let (body, effective_title) = ensure_title(&doc.body, &title);
        doc.body = body;
        doc.title = effective_title;
        fs::write(&doc_path, render_doc(&doc))?;
    } else if edit {
        open_editor(&doc_path)?;
        let mut doc = parse_doc(&doc_path)?;
        let (body, effective_title) = ensure_title(&doc.body, &title);
        doc.body = body;
        doc.title = effective_title;
        fs::write(&doc_path, render_doc(&doc))?;
    }
    let final_path = reconcile_filename(&doc_path, &identifier)?;
    let default_msg = build_commit_message("Add", &identifier, &[status.clone()]);
    maybe_commit(&store, no_commit, message.as_deref(), &default_msg, &user_cfg, Some(&final_path))?;
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
        json,
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
    let result = match list_kind {
        ListKind::Issues => {
            let rows = query_issues(&store, filters)?;
            if json {
                let json_rows: Vec<serde_json::Value> = rows.iter().map(|row| {
                    serde_json::json!({
                        "kind": row.kind,
                        "id": row.id,
                        "title": row.title,
                        "store": store.name,
                        "project": row.project,
                        "path": row.path,
                        "status": row.status,
                        "assignee": if row.assignee.is_empty() { None } else { Some(&row.assignee) },
                    })
                }).collect();
                println!("{}", serde_json::to_string_pretty(&json_rows).unwrap());
            } else {
                print_issue_table(&rows);
            }
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
    };
    if !json {
        warn_if_uncommitted(&store);
    }
    result
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
fn print_issue_table(rows: &[cache::CacheRow]) {
    if rows.is_empty() {
        return;
    }
    // Calculate column widths
    let mut w_id = "id".len();
    let mut w_kind = "kind".len();
    let mut w_status = "status".len();
    let mut w_assignee = "assignee".len();
    let mut w_title = "title".len();
    for row in rows {
        w_id = w_id.max(row.id.len());
        w_kind = w_kind.max(row.kind.len());
        w_status = w_status.max(row.status.len());
        w_assignee = w_assignee.max(row.assignee.len());
        w_title = w_title.max(row.title.len());
    }
    // Header
    println!(
        "{:<w_id$}  {:<w_kind$}  {:<w_status$}  {:<w_assignee$}  {:<w_title$}",
        "id", "kind", "status", "assignee", "title"
    );
    println!(
        "{:-<w_id$}  {:-<w_kind$}  {:-<w_status$}  {:-<w_assignee$}  {:-<w_title$}",
        "", "", "", "", ""
    );
    for row in rows {
        let id = color::colored_id(&row.id);
        let status = color::status_color(&row.status);
        // Pad based on raw (uncolored) lengths
        let id_pad = w_id.saturating_sub(row.id.len());
        let status_pad = w_status.saturating_sub(row.status.len());
        println!(
            "{}{:id_pad$}  {:<w_kind$}  {}{:status_pad$}  {:<w_assignee$}  {:<w_title$}",
            id, "", row.kind, status, "", row.assignee, row.title
        );
    }
}

fn run_show(args: ShowArgs) -> Result<()> {
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &args.id)?;
    let path = locate_doc(&store, &id)?;
    let content = if let Some(revision) = &args.revision {
        let rel_path = path
            .strip_prefix(&store.path)
            .map_err(|e| Error::new(e.to_string()))?;
        let contents = backend::file_at_revision(&store, rel_path, revision)?;
        if !args.json {
            println!("revision={}", &revision[..revision.len().min(12)]);
        }
        contents
    } else {
        fs::read_to_string(&path)?
    };

    if args.json {
        let doc = parse_doc(&path)?;
        let rel_path = path
            .strip_prefix(&store.path)
            .map_err(|e| Error::new(e.to_string()))?
            .display()
            .to_string();
        let project = doc.id.split('-').next().unwrap_or("").to_string();
        let meta = content
            .split("---")
            .nth(1)
            .map(|s| s.trim())
            .unwrap_or("");
        let json = serde_json::json!({
            "kind": doc.kind,
            "id": doc.id,
            "title": doc.title,
            "store": store.name,
            "project": project,
            "path": rel_path,
            "status": doc.status,
            "assignee": doc.assignee,
            "deps": doc.deps,
            "labels": doc.labels,
            "meta": meta,
            "description": doc.body.trim(),
        });
        println!("{}", serde_json::to_string_pretty(&json).unwrap());
        return Ok(());
    }

    let rel_path = path
        .strip_prefix(&store.path)
        .map_err(|e| Error::new(e.to_string()))?;
    let history = backend::file_rich_log(&store, rel_path, 50).unwrap_or_default();
    print_annotated_rune_doc(&content, &history, &store, rel_path);
    let doc = parse_doc(&path)?;
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
    warn_if_uncommitted(&store);
    Ok(())
}

fn split_rune_doc(content: &str) -> (String, String) {
    let mut lines = content.lines();
    let mut frontmatter = String::new();
    let mut body = String::new();
    let mut in_fm = false;
    let mut fm_done = false;
    for line in &mut lines {
        if !fm_done && line.trim() == "---" {
            frontmatter.push_str(line);
            frontmatter.push('\n');
            if in_fm {
                fm_done = true;
            } else {
                in_fm = true;
            }
            continue;
        }
        if !fm_done && in_fm {
            frontmatter.push_str(line);
            frontmatter.push('\n');
        } else if fm_done {
            body.push_str(line);
            body.push('\n');
            break;
        }
    }
    for line in lines {
        body.push_str(line);
        body.push('\n');
    }
    // Normalize body: collapse leading blank lines to a single newline
    let body_trimmed = body.trim_start_matches('\n');
    let body_normalized = format!("\n{body_trimmed}");
    (frontmatter, body_normalized)
}

fn format_timestamp_local(epoch_secs: i64) -> String {
    use jiff::Timestamp;
    let Ok(ts) = Timestamp::from_second(epoch_secs) else {
        return String::new();
    };
    let zdt = ts.to_zoned(jiff::tz::TimeZone::system());
    zdt.strftime("%b %-d at %-I:%M%P").to_string()
}

fn print_annotated_rune_doc(
    content: &str,
    history: &[LogEntry],
    store: &Store,
    rel_path: &Path,
) {
    let (frontmatter, body) = split_rune_doc(content);
    let is_uncommitted = history.is_empty();

    if is_uncommitted {
        // Never-committed rune: show frontmatter with red "<not committed>" marker
        inject_frontmatter_metadata(&frontmatter, "  created_at \"<not committed>\"\n", true);
        print_annotated_body(&body, &[], &[], "");
        return;
    }

    // Oldest entry = created, newest = last update
    let created = history.last().unwrap();
    let updated = history.first().unwrap();

    let mut injected = String::new();
    if !created.author.is_empty() {
        injected.push_str(&format!("  created_by \"{}\"\n", created.author));
    }
    if created.timestamp > 0 {
        injected.push_str(&format!("  created_at \"{}\"\n", format_timestamp_local(created.timestamp)));
    }
    if updated.revision != created.revision {
        if !updated.author.is_empty() && updated.author != created.author {
            injected.push_str(&format!("  updated_by \"{}\"\n", updated.author));
        }
        if updated.timestamp > 0 {
            injected.push_str(&format!("  updated_at \"{}\"\n", format_timestamp_local(updated.timestamp)));
        }
    }

    // Check if current disk content differs from latest committed version
    let has_pending = has_pending_changes(store, rel_path, &updated.revision);
    if has_pending {
        injected.push_str("  pending_changes true\n");
    }

    inject_frontmatter_metadata(&frontmatter, &injected, false);

    // Build section-level and comment attribution by diffing consecutive revisions
    let (section_annotations, comment_attributions) =
        build_annotations(history, store, rel_path, &body, created, has_pending);

    // Print body with section and comment annotations
    print_annotated_body(&body, &section_annotations, &comment_attributions, &created.revision);
}

/// Check if the current disk content of a rune file differs from the latest committed version.
fn has_pending_changes(store: &Store, rel_path: &Path, latest_revision: &str) -> bool {
    let disk_content = match fs::read_to_string(store.path.join(rel_path)) {
        Ok(c) => c,
        Err(_) => return false,
    };
    let committed_content = match backend::file_at_revision(store, rel_path, latest_revision) {
        Ok(c) => c,
        Err(_) => return true,
    };
    disk_content != committed_content
}

/// Print KDL frontmatter with injected metadata lines before the closing `}`.
fn inject_frontmatter_metadata(frontmatter: &str, injected: &str, use_red: bool) {
    let fm_lines: Vec<&str> = frontmatter.trim_end().lines().collect();
    if let Some(close_idx) = fm_lines.iter().rposition(|l| l.trim() == "---") {
        if let Some(brace_idx) = fm_lines[..close_idx].iter().rposition(|l| l.trim() == "}") {
            let before = &fm_lines[..brace_idx];
            let after = &fm_lines[brace_idx..];
            let mut annotated_fm = String::new();
            for line in before {
                annotated_fm.push_str(line);
                annotated_fm.push('\n');
            }
            if use_red {
                // Print what we have so far with KDL highlighting, then the red part, then rest
                color::highlight_kdl(&annotated_fm);
                println!("{}", color::red(injected.trim_end()));
                let mut rest = String::new();
                for line in after {
                    rest.push_str(line);
                    rest.push('\n');
                }
                color::highlight_kdl(&rest);
                return;
            }
            annotated_fm.push_str(injected);
            for line in after {
                annotated_fm.push_str(line);
                annotated_fm.push('\n');
            }
            color::highlight_kdl(&annotated_fm);
            return;
        }
    }
    color::highlight_kdl(frontmatter);
}

/// A section heading annotation
struct SectionAnnotation {
    /// The heading line text (e.g. "## Design")
    heading: String,
    /// Last editor of this section
    last_editor: String,
    /// Timestamp of last edit
    last_edited_at: i64,
    /// Revision of last edit (for comparing against created revision)
    last_edit_revision: String,
    /// Whether this section has uncommitted changes
    uncommitted: bool,
}

/// Attribution for a single comment block
struct CommentAttribution {
    /// The comment text (lines between --- separators), used for matching
    #[allow(dead_code)]
    text: String,
    /// Author who added this comment
    author: String,
    /// Timestamp when this comment was added
    timestamp: i64,
    /// Whether this comment has not yet been committed
    uncommitted: bool,
}

fn build_annotations(
    history: &[LogEntry],
    store: &Store,
    rel_path: &Path,
    current_body: &str,
    created: &LogEntry,
    has_pending: bool,
) -> (Vec<SectionAnnotation>, Vec<CommentAttribution>) {
    let current_sections = parse_sections(current_body);
    if current_sections.is_empty() {
        return (Vec::new(), Vec::new());
    }

    // Get file content at each revision, oldest-to-newest for attribution
    let mut revisions_content: Vec<(&LogEntry, String)> = Vec::new();
    for entry in history.iter().rev() {
        if let Ok(content) = backend::file_at_revision(store, rel_path, &entry.revision) {
            let (_, body) = split_rune_doc(&content);
            revisions_content.push((entry, body));
        }
    }

    // Get the last committed body for uncommitted change detection
    let last_committed_body = revisions_content.last().map(|(_, b)| b.clone());
    let last_committed_sections = last_committed_body
        .as_deref()
        .map(parse_sections)
        .unwrap_or_default();

    // Section annotations: find the last revision that changed each section
    let mut section_annotations = Vec::new();
    for (heading, current_text) in &current_sections {
        if heading == "Comments" || heading.is_empty() {
            continue;
        }
        let mut last_editor = created.author.clone();
        let mut last_edited_at = created.timestamp;
        let mut last_edit_revision = created.revision.clone();
        let mut prev_section_text: Option<String> = None;

        for (entry, body) in &revisions_content {
            let sections = parse_sections(body);
            let section_text = sections
                .iter()
                .find(|(h, _)| h == heading)
                .map(|(_, t)| t.clone());

            if let Some(ref text) = section_text {
                if prev_section_text.as_ref() != Some(text) {
                    last_editor = entry.author.clone();
                    last_edited_at = entry.timestamp;
                    last_edit_revision = entry.revision.clone();
                }
            }
            prev_section_text = section_text;
        }

        // Check if this section has uncommitted changes
        let section_uncommitted = has_pending && {
            let committed_text = last_committed_sections
                .iter()
                .find(|(h, _)| h == heading)
                .map(|(_, t)| t.as_str());
            committed_text != Some(current_text.as_str())
        };

        section_annotations.push(SectionAnnotation {
            heading: heading.clone(),
            last_editor,
            last_edited_at,
            last_edit_revision,
            uncommitted: section_uncommitted,
        });
    }

    // Comment attributions
    let mut comment_attributions = Vec::new();
    let comments_text = current_sections
        .iter()
        .find(|(h, _)| h == "Comments")
        .map(|(_, t)| t.clone())
        .unwrap_or_default();
    let current_comments = split_comments(&comments_text);

    // Get last committed comments for uncommitted detection
    let committed_comments_text = last_committed_sections
        .iter()
        .find(|(h, _)| h == "Comments")
        .map(|(_, t)| t.clone())
        .unwrap_or_default();
    let committed_comments = split_comments(&committed_comments_text);

    if !current_comments.is_empty() {
        for (ci, comment) in current_comments.iter().enumerate() {
            let mut author = created.author.clone();
            let mut timestamp = created.timestamp;
            let mut prev_text: Option<String> = None;

            for (entry, body) in &revisions_content {
                let sections = parse_sections(body);
                let rev_comments_text = sections
                    .iter()
                    .find(|(h, _)| h == "Comments")
                    .map(|(_, t)| t.clone())
                    .unwrap_or_default();
                let rev_comments = split_comments(&rev_comments_text);
                let rev_text = rev_comments.get(ci).map(|c| c.trim().to_string());

                if let Some(ref text) = rev_text {
                    if prev_text.as_ref() != Some(text) {
                        author = entry.author.clone();
                        timestamp = entry.timestamp;
                    }
                }
                prev_text = rev_text;
            }

            // A comment is uncommitted if it doesn't exist in committed version
            // or its content differs
            let comment_uncommitted = has_pending && {
                let committed_text = committed_comments.get(ci).map(|c| c.trim());
                committed_text != Some(comment.trim())
            };

            comment_attributions.push(CommentAttribution {
                text: comment.clone(),
                author,
                timestamp,
                uncommitted: comment_uncommitted,
            });
        }
    }

    (section_annotations, comment_attributions)
}

/// Split comment section text into individual comment blocks separated by ---
fn split_comments(text: &str) -> Vec<String> {
    let mut comments = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        if line.trim() == "---" {
            let trimmed = current.trim().to_string();
            if !trimmed.is_empty() {
                comments.push(trimmed);
            }
            current = String::new();
        } else {
            current.push_str(line);
            current.push('\n');
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        comments.push(trimmed);
    }
    comments
}

/// Parse markdown body into sections keyed by heading text.
/// Returns vec of (heading_text, section_content) pairs.
/// Headings inside fenced code blocks (``` or ~~~) are ignored.
fn parse_sections(body: &str) -> Vec<(String, String)> {
    let mut sections: Vec<(String, String)> = Vec::new();
    let mut current_heading = String::new();
    let mut current_content = String::new();
    let mut in_code_fence = false;

    for line in body.lines() {
        if line.trim_start().starts_with("```") || line.trim_start().starts_with("~~~") {
            in_code_fence = !in_code_fence;
            current_content.push_str(line);
            current_content.push('\n');
        } else if !in_code_fence && line.starts_with('#') {
            if !current_heading.is_empty() || !current_content.trim().is_empty() {
                sections.push((current_heading.clone(), current_content.clone()));
            }
            current_heading = line.trim_start_matches('#').trim().to_string();
            current_content = String::new();
        } else {
            current_content.push_str(line);
            current_content.push('\n');
        }
    }
    if !current_heading.is_empty() || !current_content.trim().is_empty() {
        sections.push((current_heading, current_content));
    }
    sections
}

fn print_annotated_body(
    body: &str,
    annotations: &[SectionAnnotation],
    comment_attrs: &[CommentAttribution],
    created_revision: &str,
) {
    let lines: Vec<&str> = body.lines().collect();
    let mut i = 0;
    let mut in_comments = false;
    let mut in_code_fence = false;
    let mut comment_idx = 0;
    let mut comment_buf: Vec<&str> = Vec::new();
    let mut comment_header_printed = false;

    while i < lines.len() {
        let line = lines[i];

        // Track code fences to avoid treating headings inside them as real headings
        if line.trim_start().starts_with("```") || line.trim_start().starts_with("~~~") {
            in_code_fence = !in_code_fence;
        }

        // Check if this is a heading (outside code fences)
        if !in_code_fence && line.starts_with('#') {
            let heading_text = line.trim_start_matches('#').trim();

            // Check for Comments section
            if heading_text == "Comments" {
                in_comments = true;
                color::highlight_markdown(&format!("{line}\n"));
                i += 1;
                continue;
            }

            // Find annotation for this heading
            if let Some(ann) = annotations.iter().find(|a| a.heading == heading_text) {
                color::highlight_markdown(&format!("{line}\n"));
                if ann.uncommitted {
                    println!("{}", color::red("pending uncommitted changes"));
                } else if ann.last_edit_revision != created_revision && ann.last_edited_at != 0 {
                    let ts = format_timestamp_local(ann.last_edited_at);
                    if ann.last_editor.is_empty() {
                        println!("{}", color::gray(&format!("Edited on {ts}")));
                    } else {
                        println!("{}", color::gray(&format!("Edited by {} on {}", ann.last_editor, ts)));
                    }
                }
                i += 1;
                continue;
            }
        }

        if in_comments {
            if line.trim() == "---" {
                // Flush buffered comment with attribution
                flush_comment_buf(&mut comment_buf, comment_attrs, &mut comment_idx, &mut comment_header_printed);
                // Print separator
                println!("{}", color::gray("---"));
            } else {
                comment_buf.push(line);
            }
        } else {
            color::highlight_markdown(&format!("{line}\n"));
        }
        i += 1;
    }

    // Flush remaining comment buffer
    flush_comment_buf(&mut comment_buf, comment_attrs, &mut comment_idx, &mut comment_header_printed);
}

fn flush_comment_buf(
    buf: &mut Vec<&str>,
    comment_attrs: &[CommentAttribution],
    comment_idx: &mut usize,
    header_printed: &mut bool,
) {
    if buf.is_empty() {
        return;
    }
    let text = buf.join("\n");
    let trimmed = text.trim();
    if trimmed.is_empty() {
        buf.clear();
        return;
    }

    // Print attribution header
    if let Some(attr) = comment_attrs.get(*comment_idx) {
        if attr.uncommitted {
            println!("{}", color::red("<not committed>"));
        } else {
            let ts = format_timestamp_local(attr.timestamp);
            if attr.author.is_empty() {
                println!("{}", color::gray(&format!("On {ts}")));
            } else {
                println!(
                    "{}{}",
                    color::gray(&format!("On {ts} by ")),
                    color::yellow(&attr.author),
                );
            }
        }
        println!();
        *header_printed = true;
        *comment_idx += 1;
    }

    color::highlight_markdown(&format!("{trimmed}\n"));
    buf.clear();
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
        message,
    } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id)?;
    let path = locate_doc(&store, &id)?;
    let mut doc = parse_doc(&path)?;
    let original_doc = doc.clone();
    let original_title = doc.title.clone();
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
        if let Some(title_value) = &title {
            if title_value.is_empty() {
                // Empty --title means keep the original title
            } else {
                doc.title = title_value.clone();
                doc.body = replace_title(&doc.body, title_value);
            }
        }
        fs::write(&path, render_doc(&doc))?;
    } else if let Some(file_path) = file {
        let contents = if file_path == Path::new("-") {
            read_from_stdin()?
        } else {
            fs::read_to_string(&file_path)?
        };
        doc.body = contents;
        let (body, effective_title) = ensure_title(&doc.body, &original_title);
        doc.body = body;
        doc.title = effective_title;
        fs::write(&path, render_doc(&doc))?;
    } else if edit || editor_available() {
        open_editor(&path)?;
        doc = parse_doc(&path)?;
        let (body, effective_title) = ensure_title(&doc.body, &original_title);
        doc.body = body;
        doc.title = effective_title;
        fs::write(&path, render_doc(&doc))?;
    } else {
        return Err(Error::new("No edits specified and no editor available"));
    }
    let final_path = reconcile_filename(&path, &doc.id)?;
    let snippets = edit_change_snippets(&original_doc, &doc);
    let default_msg = build_commit_message("Update", &doc.id, &snippets);
    maybe_commit(&store, no_commit, message.as_deref(), &default_msg, &user_cfg, Some(&final_path))?;
    Ok(())
}

fn run_comment(args: CommentArgs) -> Result<()> {
    let CommentArgs {
        id,
        message,
        file,
        no_commit,
    } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id)?;
    let path = locate_doc(&store, &id)?;
    let mut doc = parse_doc(&path)?;

    // Get comment text from -m, -f, or editor
    let comment_text = if let Some(msg) = message {
        msg
    } else if let Some(file_path) = file {
        if file_path == Path::new("-") {
            read_from_stdin()?
        } else {
            fs::read_to_string(&file_path)?
        }
    } else if editor_available() {
        let tmp_dir = std::env::temp_dir();
        let tmp_path = tmp_dir.join(format!("runes-comment-{}.md", &id));
        fs::write(&tmp_path, "")?;
        open_editor(&tmp_path)?;
        let text = fs::read_to_string(&tmp_path)?;
        let _ = fs::remove_file(&tmp_path);
        if text.trim().is_empty() {
            return Err(Error::new("Empty comment, aborting"));
        }
        text
    } else {
        return Err(Error::new(
            "No comment provided. Use -m <message> or -f <file>, or run from a terminal.",
        ));
    };

    let comment_text = comment_text.trim_end().to_string();

    // Find or create the Comments section in the body
    let body = &doc.body;
    let mut comments_heading_pos = None;
    let mut comments_heading_level = None;
    for (i, line) in body.lines().enumerate() {
        // Match any heading level where the text lowercases to "comments"
        if let Some(rest) = line.strip_prefix('#') {
            let mut hashes = 1;
            let mut rest = rest;
            while let Some(r) = rest.strip_prefix('#') {
                hashes += 1;
                rest = r;
            }
            if rest.trim().to_lowercase() == "comments" {
                // Use the highest heading level (lowest number) that matches
                if comments_heading_level.is_none() || hashes < comments_heading_level.unwrap() {
                    comments_heading_pos = Some(i);
                    comments_heading_level = Some(hashes);
                }
            }
        }
    }

    let lines: Vec<&str> = body.lines().collect();
    let mut new_body = String::new();

    if let Some(pos) = comments_heading_pos {
        // Find the end of the comments section content (next heading of same or higher level, or EOF)
        let level = comments_heading_level.unwrap();
        let mut section_end = lines.len();
        for i in (pos + 1)..lines.len() {
            if let Some(rest) = lines[i].strip_prefix('#') {
                let mut h = 1;
                let mut r = rest;
                while let Some(next) = r.strip_prefix('#') {
                    h += 1;
                    r = next;
                }
                if h <= level && !r.is_empty() && r.starts_with(' ') {
                    section_end = i;
                    break;
                }
            }
        }

        // Build new body: lines before section_end, then append comment, then rest
        for line in &lines[..section_end] {
            new_body.push_str(line);
            new_body.push('\n');
        }

        // Check if there's existing content in the section (non-empty lines after heading)
        let has_existing_content = lines[(pos + 1)..section_end]
            .iter()
            .any(|l| !l.trim().is_empty());

        if has_existing_content {
            // Separate from previous comment with horizontal rule
            new_body.push_str("\n---\n\n");
        } else {
            new_body.push('\n');
        }
        new_body.push_str(&comment_text);
        new_body.push('\n');

        // Append remaining lines after the section
        if section_end < lines.len() {
            new_body.push('\n');
            for line in &lines[section_end..] {
                new_body.push_str(line);
                new_body.push('\n');
            }
        }
    } else {
        // No Comments heading found — append one at the end
        new_body.push_str(body.trim_end());
        new_body.push_str("\n\n## Comments\n\n");
        new_body.push_str(&comment_text);
        new_body.push('\n');
    }

    doc.body = new_body;
    fs::write(&path, render_doc(&doc))?;
    let default_msg = build_commit_message("Comment on", &doc.id, &[]);
    maybe_commit(&store, no_commit, None, &default_msg, &user_cfg, Some(&path))?;
    Ok(())
}

fn run_commit(args: CommitArgs) -> Result<()> {
    let CommitArgs { target, store: store_flag, project: project_flag, message, author } = args;
    let (cfg, user_cfg, cwd) = load_context()?;

    // Determine scope: specific rune, project directory, or entire store
    let (store, paths, scope_label) = if let Some(rune_id) = &target {
        // `runes commit <rune_id>` → commit a specific rune file
        let (store_hint, id_part) = split_store_prefix(rune_id);
        let s = resolve_store_with_context(&cfg, &user_cfg, &cwd, store_hint.as_deref())?;
        let doc_path = locate_doc(&s, &id_part)?;
        let rel = doc_path.strip_prefix(&s.path)
            .map_err(|e| Error::new(e.to_string()))?;
        (s, vec![rel.to_path_buf()], id_part.to_string())
    } else if let Some(store_name) = &store_flag {
        // `runes commit --store <name>` → commit all files in the entire store
        let s = resolve_store_with_context(&cfg, &user_cfg, &cwd, Some(store_name))?;
        let paths = discover_store_paths(&s)?;
        let label = s.name.clone();
        (s, paths, label)
    } else if let Some(proj) = &project_flag {
        // `runes commit --project <name>` → commit all runes in default_store/project
        let s = resolve_store_with_context(&cfg, &user_cfg, &cwd, None)?;
        let project_root = s.path.join(proj);
        let paths = discover_dir_paths(&s.path, &project_root)?;
        (s, paths, proj.clone())
    } else {
        // `runes commit` (no args) → commit default store's default project
        let s = resolve_store_with_context(&cfg, &user_cfg, &cwd, None)?;
        if let Some(default_spec) = user_cfg.default_project.as_deref() {
            // default_project may be "store:project" or just "project"
            let project_name = if default_spec.contains(':') {
                default_spec.split(':').nth(1).unwrap_or(default_spec)
            } else {
                default_spec
            };
            let project_root = s.path.join(project_name);
            let paths = discover_dir_paths(&s.path, &project_root)?;
            (s, paths, project_name.to_string())
        } else {
            // No default project — commit entire store
            let paths = discover_store_paths(&s)?;
            let label = s.name.clone();
            (s, paths, label)
        }
    };

    let msg = message.unwrap_or_else(|| format!("Record changes for {scope_label}"));
    let (author_name, author_email) = resolve_commit_author(&user_cfg, author.as_deref())?;
    commit_store_changes(&store, &paths, &msg, &author_name, &author_email)?;
    println!("Committed changes in {}", store.name);
    Ok(())
}

/// Discover all markdown files in a store, returning paths relative to the store root.
fn discover_store_paths(store: &Store) -> Result<Vec<PathBuf>> {
    discover_dir_paths(&store.path, &store.path)
}

/// Discover all markdown files under `dir`, returning paths relative to `base`.
fn discover_dir_paths(base: &Path, dir: &Path) -> Result<Vec<PathBuf>> {
    let docs = discover_project_docs(dir)?;
    let mut rel_paths = Vec::new();
    for doc in docs {
        if let Ok(rel) = doc.strip_prefix(base) {
            rel_paths.push(rel.to_path_buf());
        }
    }
    Ok(rel_paths)
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

    fs::write(&target_path, render_doc(&target_doc))?;
    if source_path != target_path {
        if from_store.name == to_store.name {
            fs::remove_file(&source_path)?;
        } else {
            let from_rel = source_path
                .strip_prefix(&from_store.path)
                .map_err(|e| Error::new(e.to_string()))?
                .to_path_buf();
            backend::remove_path(from_store, &from_rel)?;
            fs::remove_file(&source_path)?;
        }
    }
    println!("Moved {}", target_doc.id);
    Ok(())
}
fn run_move(args: MoveArgs) -> Result<()> {
    let MoveArgs {
        id,
        target_project,
        parent,
        no_commit,
        message,
    } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let (from_store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id)?;
    let (to_store, project) =
        resolve_store_and_project_required(&cfg, &user_cfg, &cwd, None, &target_project)?;
    move_rune(&from_store, &to_store, &id, &project, parent.as_deref())?;
    let move_msg = format!("Move {id} to {project}");
    if from_store.name == to_store.name {
        maybe_commit(&from_store, no_commit, message.as_deref(), &move_msg, &user_cfg, None)?;
    } else {
        let move_in_msg = format!("Move in {id} from {}", from_store.name);
        maybe_commit(&to_store, no_commit, message.as_deref(), &move_in_msg, &user_cfg, None)?;
        // Commit the removal from the source store
        if !no_commit || message.is_some() {
            let default_from_msg = format!("Move out {id} to {}", to_store.name);
            let from_msg = message.as_deref().unwrap_or(&default_from_msg);
            let (author_name, author_email) = resolve_commit_author(&user_cfg, None)?;
            commit_store_changes(&from_store, &[], from_msg, &author_name, &author_email)?;
        }
    }
    Ok(())
}
fn run_archive(args: ArchiveArgs) -> Result<()> {
    let ArchiveArgs { id, no_commit, message } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id)?;
    archive_rune(&store, &id)?;
    let default_msg = format!("Archive {id}");
    maybe_commit(&store, no_commit, message.as_deref(), &default_msg, &user_cfg, None)?;
    Ok(())
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
    Ok(())
}
fn run_delete(args: DeleteArgs) -> Result<()> {
    let DeleteArgs { id, force, no_commit, message } = args;
    if !force {
        return Err(Error::new("Use --force to delete runes"));
    }
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id)?;
    delete_rune(&store, &id)?;
    let default_msg = format!("Delete {id}");
    maybe_commit(&store, no_commit, message.as_deref(), &default_msg, &user_cfg, None)?;
    Ok(())
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
    println!("Deleted {}", doc.id);
    Ok(())
}
fn format_log_timestamp(epoch_secs: i64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let dt = UNIX_EPOCH + Duration::from_secs(epoch_secs as u64);
    let elapsed = dt
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    // Simple UTC formatting: YYYY-MM-DD HH:MM
    let secs_per_day = 86400u64;
    let days = elapsed / secs_per_day;
    let day_secs = elapsed % secs_per_day;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    // Days since epoch to date (simplified)
    let mut y = 1970i64;
    let mut remaining = days as i64;
    loop {
        let year_days = if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) { 366 } else { 365 };
        if remaining < year_days {
            break;
        }
        remaining -= year_days;
        y += 1;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let month_days = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut m = 0usize;
    for &md in &month_days {
        if remaining < md as i64 {
            break;
        }
        remaining -= md as i64;
        m += 1;
    }
    format!("{y:04}-{:02}-{:02} {hours:02}:{minutes:02}", m + 1, remaining + 1)
}

fn rune_id_from_path(file_path: &str) -> Option<String> {
    let name = file_path.rsplit('/').next().unwrap_or(file_path);
    let stem = name.strip_suffix(".md")?;
    if stem == "_milestone" {
        // For milestones, the rune ID is derived from the parent dir
        return None;
    }
    let short = stem.split("--").next()?;
    // We need the project prefix from the path
    let parts: Vec<&str> = file_path.split('/').collect();
    if parts.len() >= 2 {
        let project = parts[0];
        Some(format!("{project}-{short}"))
    } else {
        None
    }
}

fn description_line_for_id<'a>(description: &'a str, id: &str) -> &'a str {
    for line in description.lines() {
        if line.contains(id) {
            return line.trim();
        }
    }
    description.lines().next().unwrap_or("").trim()
}


fn print_log_entries_json(
    entries: &[LogEntry],
    rune_filter: Option<&str>,
    project_filter: Option<&str>,
    author_filter: Option<&str>,
) {
    let mut json_entries = Vec::new();
    for entry in entries {
        if let Some(author) = author_filter {
            if !entry.author.eq_ignore_ascii_case(author) {
                continue;
            }
        }
        let rune_ids: Vec<String> = entry
            .changed_files
            .iter()
            .filter_map(|f| rune_id_from_path(f))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        if let Some(filter_id) = rune_filter {
            if !rune_ids.iter().any(|rid| rid == filter_id) {
                continue;
            }
        } else if let Some(proj) = project_filter {
            let prefix = format!("{proj}-");
            if !rune_ids.iter().any(|rid| rid.starts_with(&prefix)) {
                continue;
            }
        }
        let comment = entry.description.lines().next().unwrap_or("").trim();
        json_entries.push(serde_json::json!({
            "revision": entry.revision,
            "committed_at": entry.timestamp,
            "runes": rune_ids,
            "comment": comment,
        }));
    }
    println!("{}", serde_json::to_string_pretty(&json_entries).unwrap());
}

fn format_log_entries(
    entries: &[LogEntry],
    rune_filter: Option<&str>,
    project_filter: Option<&str>,
    author_filter: Option<&str>,
) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let project_prefix = project_filter.map(|p| format!("{p}-"));
    for entry in entries {
        if let Some(author) = author_filter {
            if !entry.author.eq_ignore_ascii_case(author) {
                continue;
            }
        }
        let short_rev = &entry.revision[..entry.revision.len().min(12)];
        let ts = format_log_timestamp(entry.timestamp);
        let rev_colored = color::gray(short_rev);
        let ts_colored = color::teal(&ts);
        let author_colored = color::yellow(&entry.author);

        // Derive rune IDs from changed files, falling back to description parsing
        let rune_ids: Vec<String> = entry
            .changed_files
            .iter()
            .filter_map(|f| rune_id_from_path(f))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        if rune_ids.is_empty() {
            if rune_filter.is_some() || project_prefix.is_some() {
                continue;
            }
            let desc = entry.description.lines().next().unwrap_or("").trim();
            let _ = writeln!(out, "{rev_colored}  {ts_colored}  {author_colored}  {desc}");
            continue;
        }

        if let Some(filter_id) = rune_filter {
            if !rune_ids.iter().any(|rid| rid == filter_id) {
                continue;
            }
        } else if let Some(ref prefix) = project_prefix {
            if !rune_ids.iter().any(|rid| rid.starts_with(prefix.as_str())) {
                continue;
            }
        }
        for rune_id in &rune_ids {
            if let Some(filter_id) = rune_filter {
                if rune_id != filter_id {
                    continue;
                }
            } else if let Some(ref prefix) = project_prefix {
                if !rune_id.starts_with(prefix.as_str()) {
                    continue;
                }
            }
            let desc = description_line_for_id(&entry.description, rune_id);
            let id_colored = color::colored_id(rune_id);
            let _ = writeln!(out, "{rev_colored}  {ts_colored}  {author_colored}  {id_colored}  {desc}");
        }
    }
    out
}

fn run_log(args: LogArgs) -> Result<()> {
    let LogArgs {
        id,
        limit,
        section,
        changed_by,
        json,
        no_pager,
        all,
    } = args;
    let limit = limit.unwrap_or(50);
    let (cfg, user_cfg, cwd) = load_context()?;

    // Parse the positional arg into project filter vs rune filter:
    //   <project>-<shortid> → rune filter (rune_id contains hyphen)
    //   <store>:<project>-<shortid> → rune filter with store hint
    //   <project> (no hyphen) → project filter
    //   None → default project (unless --all)
    let (rune_filter, project_filter) = match &id {
        Some(spec) if split_store_prefix(spec).1.contains('-') => {
            // rune_id (with optional store prefix)
            let (_, resolved) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, spec)?;
            let store = resolve_store_with_context(&cfg, &user_cfg, &cwd, split_store_prefix(spec).0.as_deref())?;
            let _ = locate_doc(&store, &resolved)?;
            (Some(resolved), None)
        }
        Some(proj) => {
            // Project-level filter (no hyphen, so it's a project name)
            let (_, proj_name) = split_store_prefix(proj);
            (None, Some(proj_name.to_string()))
        }
        None if all => {
            // --all: no filtering
            (None, None)
        }
        None => {
            // Use default project if configured
            let proj = user_cfg.default_project.as_ref().map(|spec| {
                let (_, proj_name) = split_store_prefix(spec);
                proj_name.to_string()
            }).filter(|p| !p.is_empty());
            (None, proj)
        }
    };

    // Section filter requires a rune ID
    if section.is_some() && rune_filter.is_none() {
        return Err(Error::new("--section requires a rune ID (e.g. proj:shortid)"));
    }

    // If section is specified, use the section-diff logic
    if let (Some(rune_id), Some(section_raw)) = (&rune_filter, section) {
        let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, rune_id)?;
        let path = locate_doc(&store, &id)?;
        let rel_path = path
            .strip_prefix(&store.path)
            .map_err(|e| Error::new(e.to_string()))?;
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
        return Ok(());
    }

    // Rich log: filtered by project, rune, or all
    let store = resolve_store_with_context(&cfg, &user_cfg, &cwd, None)?;
    let entries = backend::rich_log(&store, limit)?;
    if json {
        print_log_entries_json(&entries, rune_filter.as_deref(), project_filter.as_deref(), changed_by.as_deref());
    } else {
        let output = format_log_entries(&entries, rune_filter.as_deref(), project_filter.as_deref(), changed_by.as_deref());
        color::print_with_pager(&output, no_pager);
    }
    Ok(())
}
fn run_diff(args: DiffArgs) -> Result<()> {
    let DiffArgs {
        id,
        revision,
        from,
        to,
    } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id)?;
    let path = locate_doc(&store, &id)?;
    let rel_path = path
        .strip_prefix(&store.path)
        .map_err(|e| Error::new(e.to_string()))?;

    match store.backend {
        BackendKind::Jj => run_diff_jj(&store, rel_path, revision, from, to),
        BackendKind::Pijul => run_diff_sdk(&store, &path, rel_path, revision, from, to),
    }
}

fn run_diff_jj(
    store: &Store,
    rel_path: &Path,
    revision: Option<String>,
    from: Option<String>,
    to: Option<String>,
) -> Result<()> {
    let mut cmd = Command::new("jj");
    cmd.arg("diff").current_dir(&store.path);
    if let Some(rev) = revision {
        cmd.arg("-r").arg(&rev);
    } else if let Some(from_rev) = from {
        cmd.arg("--from").arg(&from_rev);
        if let Some(to_rev) = to {
            cmd.arg("--to").arg(&to_rev);
        }
    }
    cmd.arg("--").arg(rel_path);
    let output = cmd.output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::new(format!("jj diff failed: {}", stderr.trim())));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    print!("{stdout}");
    Ok(())
}

fn run_diff_sdk(
    store: &Store,
    abs_path: &Path,
    rel_path: &Path,
    revision: Option<String>,
    from: Option<String>,
    to: Option<String>,
) -> Result<()> {
    if let Some(rev) = revision {
        // Single revision diff: state before vs state after
        let before = backend::file_before_revision(store, rel_path, &rev)?;
        let after = backend::file_at_revision(store, rel_path, &rev)?;
        print_unified_diff(rel_path, &before, &after);
    } else if let Some(from_rev) = from {
        let before = backend::file_at_revision(store, rel_path, &from_rev)?;
        let after = if let Some(to_rev) = to {
            backend::file_at_revision(store, rel_path, &to_rev)?
        } else {
            fs::read_to_string(abs_path)?
        };
        print_unified_diff(rel_path, &before, &after);
    } else {
        // No revision specified — show uncommitted changes via backend CLI
        let cmd_name = store.backend.as_str();
        let mut cmd = Command::new(cmd_name);
        cmd.arg("diff")
            .arg("--")
            .arg(rel_path)
            .current_dir(&store.path);
        let output = cmd.output()?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        print!("{stdout}");
    }
    Ok(())
}

fn print_unified_diff(rel_path: &Path, before: &str, after: &str) {
    let diff = similar::TextDiff::from_lines(before, after);
    let path_str = rel_path.display();
    let mut has_changes = false;
    for hunk in diff.unified_diff().context_radius(3).iter_hunks() {
        if !has_changes {
            println!("{}", color::diff_file_header(&format!("--- a/{path_str}")));
            println!("{}", color::diff_file_header(&format!("+++ b/{path_str}")));
            has_changes = true;
        }
        let hunk_str = hunk.to_string();
        for line in hunk_str.lines() {
            if line.starts_with("@@") {
                println!("{}", color::diff_hunk_header(line));
            } else if line.starts_with('+') {
                println!("{}", color::diff_added(line));
            } else if line.starts_with('-') {
                println!("{}", color::diff_removed(line));
            } else {
                println!("{line}");
            }
        }
    }
    if !has_changes {
        println!("(no changes)");
    }
}

fn run_restore(args: RestoreArgs) -> Result<()> {
    let RestoreArgs {
        id,
        revision,
        no_commit,
        message,
    } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, id) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, &id)?;
    let path = locate_doc(&store, &id)?;
    let rel_path = path
        .strip_prefix(&store.path)
        .map_err(|e| Error::new(e.to_string()))?;
    let contents = backend::file_at_revision(&store, rel_path, &revision)?;
    fs::write(&path, &contents)?;
    let doc = parse_doc(&path)?;
    let final_path = reconcile_filename(&path, &doc.id)?;
    let short_rev = &revision[..revision.len().min(12)];
    println!("Restored {} to revision {short_rev}", doc.id);
    let default_msg = format!("Restore {} to revision {short_rev}", doc.id);
    maybe_commit(&store, no_commit, message.as_deref(), &default_msg, &user_cfg, Some(&final_path))?;
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
