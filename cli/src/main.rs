mod color;
mod user_config;
use atty::Stream;
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use pijul_interaction::{set_context, InteractiveContext};
use runes_core::backend::{self, LogEntry};
use runes_core::cache;
use runes_core::config::{discover_stores, ensure_dir, get_store, BackendKind, Store};
use runes_core::model::{
    discover_project_docs, ensure_title, has_frontmatter, new_milestone_doc, new_rune_doc,
    next_short_id, parse_doc, parse_doc_text, parse_full_id, render_doc, replace_title,
    resolve_issue_path, slugify, RuneDoc,
};
use runes_core::schema::{find_kind_template_path, load_kind_template, load_schema};
use runes_core::state::{self, StateConfig};
use runes_core::{Error, Result};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::Command;
use user_config::UserConfig;

#[derive(Debug, Parser)]
#[command(
    name = "runes",
    version,
    about = "A local-first issue tracker stored as markdown rune docs",
    propagate_version = true
)]
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
    /// Full-text search rune titles and bodies
    Search(SearchArgs),
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
    /// Show a quickstart guide for using runes
    Quickstart(QuickstartArgs),
}

#[derive(Debug, Parser)]
struct QuickstartArgs {
    /// Write the guide for an AI agent (the default when one is detected)
    #[arg(long)]
    agent: bool,
    /// Write the guide for a person at a terminal (the default otherwise)
    #[arg(long, conflicts_with = "agent")]
    human: bool,
}

#[derive(Debug, Subcommand)]
enum StoreCommand {
    /// Initialize a new store
    Init {
        /// Store name
        name: String,
        /// Backend type: jj or pijul (default jj)
        #[arg(long)]
        backend: Option<String>,
        /// Path to the store directory (default ~/.runes/stores/<name>)
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
        /// Store name (uses default store if omitted)
        name: Option<String>,
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
    /// Skip installing the agent skill (init refreshes it on every run otherwise)
    #[arg(long = "no-skill", conflicts_with = "force_skill")]
    no_skill: bool,
    /// Overwrite a hand-edited agent skill (unedited ones refresh silently)
    #[arg(long = "force-skill")]
    force_skill: bool,
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
    /// Doc kind (e.g. issue, milestone)
    #[arg(short = 'k', long = "kind")]
    command_kind: Option<String>,
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
    #[arg(short = 'l', long = "label")]
    labels: Vec<String>,
    /// Add a relation e.g. "blocks:runes-x1" (repeatable)
    #[arg(long = "relation")]
    relations: Vec<String>,
    /// Add a dependency (repeatable)
    #[arg(long = "dep")]
    deps: Vec<String>,
    /// Body, or full doc whose frontmatter seeds metadata (use - for stdin)
    #[arg(short = 'f', long = "file")]
    file: Option<PathBuf>,
    /// Open editor after creation
    #[arg(short = 'e', long = "edit")]
    edit: bool,
    /// Commit immediately instead of leaving the doc for in-place editing
    #[arg(long = "commit", conflicts_with = "no_commit")]
    commit: bool,
    /// Leave the new doc uncommitted even when -e/-f/-m provided content
    #[arg(long = "no-commit")]
    no_commit: bool,
    /// Commit message (implies commit)
    #[arg(short = 'm', long = "message")]
    message: Option<String>,
    /// Emit {id, path, committed} as JSON instead of the human-readable summary
    #[arg(long)]
    json: bool,
}

/// Built-in views, in the order they are listed to users.
const BUILTIN_VIEWS: &[(&str, &str)] = &[
    ("open", "runes that aren't closed yet (the default)"),
    ("mine", "open runes assigned to you"),
    ("all", "every rune, whatever its status"),
    ("closed", "runes in a closed status"),
];

const VIEW_OPEN: &str = "open";
const VIEW_MINE: &str = "mine";
const VIEW_ALL: &str = "all";
const VIEW_CLOSED: &str = "closed";

fn builtin_views_help() -> String {
    let mut help = String::from("Built-in views (runes list <view>):\n");
    for (name, description) in BUILTIN_VIEWS {
        help.push_str(&format!("  {name:<8}{description}\n"));
    }
    help
}

#[derive(Debug, Default, Parser)]
#[command(after_help = builtin_views_help())]
struct ListArgs {
    /// Built-in view to apply (open, mine, all, closed)
    #[arg(value_name = "view")]
    view: Option<String>,
    /// Store to list from
    #[arg(long)]
    store: Option<String>,
    /// Filter by project (or store:project; empty string for all)
    #[arg(long)]
    project: Option<String>,
    /// Named query from runes.kdl (deprecated; prefer a built-in view)
    #[arg(long)]
    query: Option<String>,
    /// Show runes in any status (alias for the `all` view)
    #[arg(long = "all", conflicts_with_all = ["view", "query"])]
    all: bool,
    /// Filter by kind (e.g. issues, milestones)
    #[arg(short = 'k', long = "kind")]
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
    /// Filter by label (repeatable)
    #[arg(short = 'l', long = "label")]
    labels: Vec<String>,
    /// Show only blocked runes (have unresolved deps)
    #[arg(long, conflicts_with = "ready")]
    blocked: bool,
    /// Show only ready runes (no unresolved deps)
    #[arg(long, conflicts_with = "blocked")]
    ready: bool,
    /// Show runes blocked by a specific rune ID
    #[arg(long = "blocked-by")]
    blocked_by: Option<String>,
    /// Show runes that block a specific rune ID
    #[arg(long)]
    blocks: Option<String>,
    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Parser)]
struct SearchArgs {
    /// Text to look for in rune titles and bodies (including comments)
    term: String,
    /// Store to search
    #[arg(long)]
    store: Option<String>,
    /// Filter by project (or store:project; empty string for all)
    #[arg(long)]
    project: Option<String>,
    /// Filter by status (all statuses are searched by default)
    #[arg(long)]
    status: Option<String>,
    /// Filter by label (repeatable)
    #[arg(short = 'l', long = "label")]
    labels: Vec<String>,
    /// Search only archived docs
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
    /// Add a dependency (repeatable)
    #[arg(long = "dep")]
    add_deps: Vec<String>,
    /// Remove a dependency (repeatable)
    #[arg(long = "remove-dep")]
    remove_deps: Vec<String>,
    /// Replace body, or full doc with frontmatter (use - for stdin)
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
    /// Rune ID: <project>-<shortid>, <store>:<project>-<shortid>, or bare <shortid>
    /// (bare shortid requires the rune to still exist on disk)
    id: Option<String>,
    /// Filter by project (or store:project)
    #[arg(long, conflicts_with_all = ["id", "all"])]
    project: Option<String>,
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
    #[arg(long, conflicts_with = "id")]
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
    restore_default_sigpipe();
    set_context(InteractiveContext::Terminal);
    let cli = parse_cli();
    let command = cli.command.unwrap_or(CliCommand::List(ListArgs::default()));
    if let Err(err) = handle_command(command) {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

/// Parse argv, teaching `runes init --help` about this machine's stores.
/// The listing costs a directory scan, so only a help run pays for it.
fn parse_cli() -> Cli {
    let mut command = Cli::command();
    let wants_help = std::env::args().any(|arg| arg == "-h" || arg == "--help" || arg == "help");
    if wants_help {
        if let Some(text) = stores_help_text() {
            command = command.mut_subcommand("init", |init| init.after_help(text));
        }
    }
    let matches = command.get_matches();
    Cli::from_arg_matches(&matches).unwrap_or_else(|err| err.exit())
}

/// Rust ignores SIGPIPE, so writing past a closed pipe (`runes show <id> | head -3`)
/// panics instead of ending quietly the way other unix CLIs do.
fn restore_default_sigpipe() {
    #[cfg(unix)]
    // SAFETY: installing a signal disposition before any threads are spawned.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}

fn handle_command(command: CliCommand) -> Result<()> {
    match command {
        CliCommand::New(args) => run_new(args),
        CliCommand::List(args) => run_list(args),
        CliCommand::Search(args) => run_search(args),
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
        CliCommand::Quickstart(args) => run_quickstart(args),
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

const DEFAULT_BACKEND: &str = "jj";
const DEFAULT_STORE_NAME: &str = "proj";

const DRAFT_MAX_AGE_DAYS: u64 = 30;

fn drafts_root(store_name: &str) -> Result<PathBuf> {
    Ok(home_dir()?.join(".runes").join("drafts").join(store_name))
}

/// Build a draft file path under `~/.runes/drafts/<store>/<proj>/` for editor-based edits.
///
/// Format: `<rune_id>--<content_hash>--<title_slug>.md`
/// where `content_hash` is a short hash of the original content for uniqueness.
fn draft_path(store_name: &str, rune_id: &str, title: &str, content: &str) -> Result<PathBuf> {
    use std::hash::{Hash, Hasher};
    let parsed = parse_full_id(rune_id)?;
    let drafts_dir = drafts_root(store_name)?.join(&parsed.project);
    ensure_dir(&drafts_dir)?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    content.hash(&mut hasher);
    let content_hash = format!("{:07x}", hasher.finish() & 0x0FFFFFFF);
    let slug: String = slugify(title).chars().take(60).collect();
    let filename = format!("{rune_id}--{content_hash}--{slug}.md");
    Ok(drafts_dir.join(filename))
}

/// Drop drafts for a rune whose edits just landed: what is left over comes from an
/// aborted or failed editor session and is no longer recoverable state.
fn prune_drafts_for_rune(store_name: &str, rune_id: &str) {
    let (Ok(parsed), Ok(root)) = (parse_full_id(rune_id), drafts_root(store_name)) else {
        return;
    };
    let Ok(entries) = fs::read_dir(root.join(&parsed.project)) else {
        return;
    };
    let prefix = format!("{rune_id}--");
    for entry in entries.flatten() {
        if entry.file_name().to_string_lossy().starts_with(&prefix) {
            let _ = fs::remove_file(entry.path());
        }
    }
}

/// Delete drafts untouched for `max_age`, returning how many were removed; younger
/// drafts stay recoverable.
fn prune_aged_drafts(store_name: &str, max_age: std::time::Duration) -> Result<usize> {
    let Ok(projects) = fs::read_dir(drafts_root(store_name)?) else {
        return Ok(0);
    };
    let now = std::time::SystemTime::now();
    let mut removed = 0;
    for project in projects.flatten() {
        let Ok(entries) = fs::read_dir(project.path()) else {
            continue;
        };
        for entry in entries.flatten() {
            let aged = entry
                .metadata()
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|modified| now.duration_since(modified).ok())
                .is_some_and(|age| age >= max_age);
            if aged && fs::remove_file(entry.path()).is_ok() {
                removed += 1;
            }
        }
    }
    Ok(removed)
}

fn load_context() -> Result<(Vec<Store>, UserConfig, PathBuf)> {
    let mut stores = discover_stores()?;
    let cwd = std::env::current_dir().map_err(|e| Error::new(e.to_string()))?;
    let user_cfg = UserConfig::load_from_dir(&cwd)?;
    // Merge store definitions from KDL config (overrides discovered stores)
    for store_def in &user_cfg.stores {
        if !store_def.backend.is_empty() && !store_def.path.is_empty() {
            if let Ok(backend) = BackendKind::parse(&store_def.backend) {
                let store = Store {
                    name: store_def.name.clone(),
                    backend,
                    path: PathBuf::from(&store_def.path),
                };
                if let Some(existing) = stores.iter_mut().find(|s| s.name == store.name) {
                    *existing = store;
                } else {
                    stores.push(store);
                }
            }
        }
    }
    Ok((stores, user_cfg, cwd))
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
    stores: &[Store],
    user_config: &UserConfig,
    cwd: &Path,
    store_hint: Option<&str>,
) -> Result<Store> {
    if let Some(name) = store_hint {
        return get_store(stores, name);
    }
    if let Some(name) = user_config.store_for_path(cwd) {
        return get_store(stores, &name);
    }
    if let Some(name) = user_config.default_store.as_deref() {
        return get_store(stores, name);
    }
    if stores.len() == 1 {
        return Ok(stores[0].clone());
    }
    Err(Error::new(
        "No default store configured. Set defaults.store in runes.kdl or ~/.runes/config.kdl",
    ))
}

fn resolve_store_and_project(
    stores: &[Store],
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
        let store = resolve_store_with_context(stores, user_config, cwd, hint)?;
        return Ok((store, Some(project.to_string())));
    }
    let store = resolve_store_with_context(stores, user_config, cwd, store_hint)?;
    Ok((store, None))
}

fn resolve_store_and_project_required(
    stores: &[Store],
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
    let store = resolve_store_with_context(stores, user_config, cwd, hint)?;
    Ok((store, project.to_string()))
}

fn resolve_store_and_id(
    stores: &[Store],
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
    let store = resolve_store_with_context(stores, user_config, cwd, hint)?;
    Ok((store, id_part.to_string()))
}

/// Resolve a rune ID spec to its store and file path.
///
/// Accepts all three forms:
/// - `store:project-short` (fully qualified)
/// - `project-short` (uses default store)
/// - `short` (uses default store, tries default project first, then scans all projects)
fn resolve_rune_id(
    stores: &[Store],
    user_config: &UserConfig,
    cwd: &Path,
    id_spec: &str,
) -> Result<(Store, PathBuf)> {
    let (store_hint, id_part) = split_store_prefix(id_spec);
    if id_part.is_empty() {
        return Err(Error::new("ID may not be empty"));
    }
    let store = resolve_store_with_context(stores, user_config, cwd, store_hint.as_deref())?;
    let path = if parse_full_id(id_part).is_ok() {
        resolve_issue_path(&store.path, id_part)?
    } else {
        // Bare short ID: try default project first to avoid ambiguity
        let mut found = None;
        if let Some(default_spec) = user_config.default_project.as_deref() {
            let project = split_store_prefix(default_spec).1;
            if !project.is_empty() {
                let full_id = format!("{project}-{id_part}");
                if let Ok(path) = resolve_issue_path(&store.path, &full_id) {
                    found = Some(path);
                }
            }
        }
        match found {
            Some(path) => path,
            None => find_short_id(&store.path, id_part)?,
        }
    };
    Ok((store, path))
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

use runes_core::cache::{ArchivedMode, CacheFilter};

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

/// Environment lookup, injected so agent detection stays unit-testable.
type EnvLookup<'a> = &'a dyn Fn(&str) -> Option<String>;

/// Read an env var, trimmed, treating blank values as unset.
fn system_env(key: &str) -> Option<String> {
    std::env::var(key).ok().and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

/// Explicit, caller-chosen agent slug. Outranks every other signal.
const AGENT_OVERRIDE_VAR: &str = "RUNES_AGENT";

/// Generic conventions whose value names the agent. Lowest confidence: the
/// value is free-form and often version-stamped (Claude Code exports
/// `AI_AGENT=claude-code_2-1-218_agent`), so it is normalized to its first
/// `_`-separated token and only used when no canonical marker matched.
const AGENT_SLUG_VARS: &[&str] = &["AI_AGENT", "AGENT"];

/// Env markers implying a fixed agent slug: (var, required value, slug).
/// Only markers an agent sets for its own subprocesses — terminal/IDE hints
/// like TERM_PROGRAM describe the terminal, not the agent.
const AGENT_MARKER_VARS: &[(&str, Option<&str>, &str)] = &[
    ("CLAUDECODE", None, "claude"),
    ("GEMINI_CLI", None, "gemini"),
    ("CODEX_SANDBOX", None, "codex"),
    ("CODEX_THREAD_ID", None, "codex"),
    ("CURSOR_AGENT", None, "cursor"),
    ("CURSOR_EXTENSION_HOST_ROLE", Some("agent-exec"), "cursor"),
    ("AUGMENT_AGENT", None, "augment"),
    ("OPENCODE", None, "opencode"),
    ("OPENCODE_CLIENT", None, "opencode"),
    ("JUNIE_DATA", None, "junie"),
    ("JUNIE_SHIM_PATH", None, "junie"),
    ("CLINE_ACTIVE", None, "cline"),
];

/// Normalize an env value into an agent slug, or `None` if it can't be one.
/// The slug lands in the author name and in `<slug>@agents.localhost`, so it
/// must be lowercase and match `[a-z0-9][a-z0-9._-]*`; anything else (spaces,
/// `@`, quotes) is rejected rather than injected into an identity.
fn agent_slug(value: &str) -> Option<String> {
    let slug = value.trim().to_lowercase();
    let mut chars = slug.chars();
    if !chars.next()?.is_ascii_alphanumeric() {
        return None;
    }
    chars
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        .then_some(slug)
}

/// Detect the AI agent running this command, first match wins:
/// `RUNES_AGENT` > canonical markers > generic `AI_AGENT`/`AGENT`. Canonical
/// markers outrank the generic vars because their slug is exact, while a
/// generic value is whatever the agent chose to stamp there.
fn detect_agent(env: EnvLookup) -> Option<String> {
    if let Some(slug) = env(AGENT_OVERRIDE_VAR).as_deref().and_then(agent_slug) {
        return Some(slug);
    }
    for (var, required, slug) in AGENT_MARKER_VARS {
        let Some(value) = env(var) else { continue };
        let value = value.trim();
        if !value.is_empty() && required.is_none_or(|expected| value == expected) {
            return Some((*slug).to_string());
        }
    }
    AGENT_SLUG_VARS
        .iter()
        .filter_map(|var| env(var))
        .find_map(|value| agent_slug(value.split('_').next().unwrap_or_default()))
}

/// Identity for a detected agent, recording the configured human (if any)
/// as on-behalf-of unless that identity is the agent itself.
fn agent_identity(slug: &str, on_behalf_of: Option<&str>) -> (String, String) {
    let email = format!("{slug}@agents.localhost");
    match on_behalf_of {
        Some(human) if !human.eq_ignore_ascii_case(&email) && !human.eq_ignore_ascii_case(slug) => {
            (format!("{slug} (on behalf of {human})"), email)
        }
        _ => (slug.to_string(), email),
    }
}

/// Resolve commit author from: override flag > RUNES_USER env > detected agent > config
fn resolve_commit_author(
    user_cfg: &UserConfig,
    author_override: Option<&str>,
) -> Result<(String, String)> {
    resolve_commit_author_env(user_cfg, author_override, &system_env)
}

fn resolve_commit_author_env(
    user_cfg: &UserConfig,
    author_override: Option<&str>,
    env: EnvLookup,
) -> Result<(String, String)> {
    if let Some(author_str) = author_override {
        return Ok(parse_author_string(author_str));
    }
    if let Some(env_val) = env("RUNES_USER") {
        return Ok(parse_author_string(&env_val));
    }
    if user_cfg.attribution_detect() {
        if let Some(slug) = detect_agent(env) {
            return Ok(agent_identity(&slug, user_cfg.identity_email.as_deref()));
        }
    }
    if let Some(email) = &user_cfg.identity_email {
        let name = user_cfg.identity_name.as_deref().unwrap_or(email);
        return Ok((name.to_string(), email.clone()));
    }
    Err(Error::new(
        "No author configured. Set user.email in runes config, RUNES_USER env var, or use --author flag."
    ))
}

fn commit_store_changes(
    store: &Store,
    paths: &[PathBuf],
    message: &str,
    author_name: &str,
    author_email: &str,
) -> Result<()> {
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
    let added_labels: Vec<_> = new
        .labels
        .iter()
        .filter(|l| !old.labels.contains(l))
        .collect();
    let removed_labels: Vec<_> = old
        .labels
        .iter()
        .filter(|l| !new.labels.contains(l))
        .collect();
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
    // Dep changes
    if old.deps != new.deps {
        snippets.push("deps".to_string());
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
        if !old_sections.contains(section) && !snippets.iter().any(|s| s == &section.to_lowercase())
        {
            snippets.push(section.to_lowercase());
        }
    }
    // If body changed but no section-level diff caught it, say "description"
    if old.body != new.body
        && snippets.iter().all(|s| {
            ![
                "description",
                "design",
                "comments",
                "notes",
                "acceptance criteria",
            ]
            .contains(&s.as_str())
        })
    {
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
        .filter_map(|line| line.strip_prefix("## ").map(|rest| rest.trim().to_string()))
        .collect()
}

/// Extract content of a named section (or main body if name is empty).
fn extract_section_content(body: &str, section_name: &str) -> String {
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
    let current_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
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
    rune_paths: &[PathBuf],
) -> Result<()> {
    if no_commit && user_message.is_none() {
        // list/search read the cache, not the store files, so it tracks drafts too.
        cache::rebuild_cache(store)?;
        eprintln!(
            "hint: uncommitted changes pending. Will be included in next commit or `runes commit`."
        );
        return Ok(());
    }
    let msg = user_message.unwrap_or(default_message);
    let (author_name, author_email) = resolve_commit_author(user_cfg, None)?;
    let paths: Vec<PathBuf> = rune_paths
        .iter()
        .map(|p| p.strip_prefix(&store.path).unwrap_or(p).to_path_buf())
        .collect();
    commit_store_changes(store, &paths, msg, &author_name, &author_email)
}

/// Accept legacy status names on any input path, then validate.
fn normalize_status(doc: &mut RuneDoc, states: &StateConfig) -> Result<()> {
    doc.status = state::normalize(&doc.status);
    states.validate(&doc.status)
}

/// Whether the file on disk differs from what `revision` recorded.
/// Unreadable either way counts as unchanged: a hint is not worth an error.
fn has_drifted_from(store: &Store, rel_path: &Path, revision: &str) -> bool {
    let Ok(recorded) = backend::file_at_revision(store, rel_path, revision) else {
        return false;
    };
    let Ok(current) = fs::read_to_string(store.path.join(rel_path)) else {
        return false;
    };
    current != recorded
}

/// Whether the file was touched at or after `epoch_secs`.
fn touched_since(store: &Store, rel_path: &Path, epoch_secs: i64) -> bool {
    let Ok(mtime) = fs::metadata(store.path.join(rel_path)).and_then(|m| m.modified()) else {
        return false;
    };
    let Ok(since_epoch) = mtime.duration_since(std::time::UNIX_EPOCH) else {
        return false;
    };
    since_epoch.as_secs() as i64 >= epoch_secs
}

/// Nudge only about runes that are already in history but have drifted on disk.
/// A never-committed draft is what `runes new` leaves behind by design, so
/// warning about one would nag on every single list.
fn warn_modified(modified: &[&str]) {
    let Some(first) = modified.first() else {
        return;
    };
    if modified.len() == 1 {
        eprintln!(
            "hint: {first} has edits not yet in history. Record them with `runes commit {first}`."
        );
    } else {
        eprintln!(
            "hint: {} runes have edits not yet in history, e.g. `runes commit {first}`.",
            modified.len()
        );
    }
}

/// A rune with no timestamp never reached history, so it is a draft, not a
/// modification — only the latter is worth a nudge.
fn warn_if_modified(rows: &[cache::CacheRow], uncommitted_ids: &std::collections::HashSet<String>) {
    let modified: Vec<&str> = rows
        .iter()
        .filter(|row| row.updated.is_some() && uncommitted_ids.contains(&row.id))
        .map(|row| row.id.as_str())
        .collect();
    warn_modified(&modified);
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

/// Fields `runes show` injects for display; they are never stored, so full-doc
/// input must not carry them back in.
const DERIVED_FRONTMATTER_FIELDS: [&str; 5] = [
    "created_by",
    "created_at",
    "updated_by",
    "updated_at",
    "pending_changes",
];

/// Read `--file` input, where `-` means stdin.
fn read_file_input(path: &Path) -> Result<String> {
    if path == Path::new("-") {
        read_from_stdin()
    } else {
        Ok(fs::read_to_string(path)?)
    }
}

/// Label for `--file` input in error messages.
fn input_source(path: &Path) -> &Path {
    if path == Path::new("-") {
        Path::new("<stdin>")
    } else {
        path
    }
}

/// Parse `--file` input that carries a leading frontmatter fence as a full doc.
fn parse_input_doc(contents: &str, path: &Path) -> Result<RuneDoc> {
    let mut doc = parse_doc_text(contents, input_source(path))?;
    doc.frontmatter_extra.retain(|line| {
        let field = line.split_whitespace().next().unwrap_or("");
        !DERIVED_FRONTMATTER_FIELDS.contains(&field)
    });
    doc.body = strip_show_injections(&doc.body);
    Ok(doc)
}

/// `runes show` decorates the body as well as the frontmatter: annotation lines under
/// headings, attributions above comments, and a trailing block of resolved deps and
/// milestone counts. Full-doc input is usually a copy of that output, so the
/// decorations are stripped back out before they get stored and re-accumulate on the
/// next round-trip. Matching is by exact shape and position, so body text that merely
/// resembles a decoration survives.
fn strip_show_injections(body: &str) -> String {
    let mut lines: Vec<&str> = body.lines().collect();
    truncate_show_trailers(&mut lines);

    let mut kept: Vec<&str> = Vec::with_capacity(lines.len());
    let mut in_code_fence = false;
    let mut in_comments = false;
    let mut after_heading = false;
    let mut at_comment_start = false;
    for (idx, line) in lines.iter().enumerate() {
        let is_fence = line.trim_start().starts_with("```") || line.trim_start().starts_with("~~~");
        if !in_code_fence && !is_fence {
            if after_heading && is_heading_annotation(line) {
                after_heading = false;
                continue;
            }
            // The blank line below an attribution is `show`'s too, but the stored
            // comment already starts with one, so only the attribution goes.
            if at_comment_start
                && is_comment_attribution(line)
                && lines
                    .get(idx + 1)
                    .is_some_and(|next| next.trim().is_empty())
            {
                at_comment_start = false;
                continue;
            }
            // `show` trims the blank line each comment ends with; put it back so the
            // section matches the shape `runes comment` writes.
            if in_comments
                && line.trim() == "---"
                && kept.last().is_some_and(|prev| !prev.trim().is_empty())
            {
                kept.push("");
            }
        }
        kept.push(line);
        if is_fence {
            in_code_fence = !in_code_fence;
        }
        (after_heading, at_comment_start) = if in_code_fence || is_fence {
            (false, false)
        } else if let Some(heading) = line.strip_prefix('#') {
            in_comments = heading.trim_start_matches('#').trim() == "Comments";
            (true, in_comments)
        } else {
            // Comments are separated by a `---` rule, so each one starts a new block
            (false, in_comments && line.trim() == "---")
        };
    }

    let mut out = kept.join("\n");
    if body.ends_with('\n') && !out.is_empty() {
        out.push('\n');
    }
    out
}

/// Drop the resolved-deps and milestone-progress blocks `show` appends after the body.
/// Only a trailing run of them is removed, so a body that ends in similar-looking text
/// is left alone.
fn truncate_show_trailers(lines: &mut Vec<&str>) {
    loop {
        let Some(end) = lines.iter().rposition(|line| !line.trim().is_empty()) else {
            return;
        };
        let end = end + 1;
        let start = if is_milestone_counts_line(lines[end - 1]) {
            end - 1
        } else {
            // A `deps:`/`children:` header followed by nothing but its entries
            let mut idx = end;
            while idx > 0 && is_dep_list_entry(lines[idx - 1]) {
                idx -= 1;
            }
            if idx == end || idx == 0 || !matches!(lines[idx - 1], "deps:" | "children:") {
                return;
            }
            idx - 1
        };
        lines.truncate(start);
    }
}

/// `Jul 24 at 3:40pm` — the timestamp shape `show` renders (`%b %-d at %-I:%M%P`).
fn is_show_timestamp(tokens: &[&str]) -> bool {
    let [month, day, "at", time] = tokens else {
        return false;
    };
    let digits =
        |s: &str, max: usize| (1..=max).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit());
    let clock = time
        .strip_suffix("am")
        .or_else(|| time.strip_suffix("pm"))
        .and_then(|clock| clock.split_once(':'));
    month.len() == 3
        && month.bytes().all(|b| b.is_ascii_alphabetic())
        && digits(day, 2)
        && clock.is_some_and(|(hour, min)| digits(hour, 2) && min.len() == 2 && digits(min, 2))
}

/// `Edited by <who> on <ts>`, `Edited on <ts>`, or the uncommitted marker — what `show`
/// prints directly under an edited heading.
fn is_heading_annotation(line: &str) -> bool {
    if line == "pending uncommitted changes" {
        return true;
    }
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.len() < 6 || !is_show_timestamp(&tokens[tokens.len() - 4..]) {
        return false;
    }
    matches!(
        &tokens[..tokens.len() - 4],
        ["Edited", "on"] | ["Edited", "by", .., "on"]
    )
}

/// `On <ts> by <author>`, `On <ts>`, or the uncommitted marker — what `show` prints
/// above each comment.
fn is_comment_attribution(line: &str) -> bool {
    if line == "<not committed>" {
        return true;
    }
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.len() < 5 || tokens[0] != "On" || !is_show_timestamp(&tokens[1..5]) {
        return false;
    }
    tokens.len() == 5 || (tokens.len() > 6 && tokens[5] == "by")
}

/// `  proj-abc (todo)` — one entry of a `deps:` or `children:` list.
fn is_dep_list_entry(line: &str) -> bool {
    let Some((id, status)) = line
        .strip_prefix("  ")
        .and_then(|rest| rest.split_once(" ("))
        .and_then(|(id, status)| Some((id, status.strip_suffix(')')?)))
    else {
        return false;
    };
    !id.is_empty()
        && !status.is_empty()
        && [id, status]
            .iter()
            .all(|s| !s.contains(|c: char| c.is_whitespace() || c == '(' || c == ')'))
}

/// `child_total=3 child_done=1 child_in_progress=1 child_todo=1 complete_pct=33.3`
fn is_milestone_counts_line(line: &str) -> bool {
    const FIELDS: [&str; 5] = [
        "child_total=",
        "child_done=",
        "child_in_progress=",
        "child_todo=",
        "complete_pct=",
    ];
    let tokens: Vec<&str> = line.split_whitespace().collect();
    tokens.len() == FIELDS.len()
        && tokens
            .iter()
            .zip(FIELDS)
            .all(|(token, field)| token.strip_prefix(field).is_some_and(|v| !v.is_empty()))
}

fn extend_unique<T: PartialEq>(dst: &mut Vec<T>, items: impl IntoIterator<Item = T>) {
    for item in items {
        if !dst.contains(&item) {
            dst.push(item);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn create_rune(
    store: &Store,
    project: &str,
    kind: &str,
    title: &str,
    body_template: &str,
    status: &str,
    parent: Option<&str>,
    milestone: Option<&str>,
    labels: &[String],
    relations: &[(String, String)],
    deps: &[String],
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
    let mut doc = new_rune_doc(&full_id, kind, title, body_template, milestone);
    doc.status = status.to_string();
    doc.labels = labels.to_vec();
    doc.relations = relations.to_vec();
    doc.deps = deps.to_vec();
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
    let body_template = load_kind_template(&store.path, Some(project), "milestone");
    let mut doc = new_milestone_doc(&full_id, title, &body_template);
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
        command_kind,
        status: status_flag,
        assignee,
        parent,
        mut milestone,
        id_override,
        labels,
        relations,
        mut deps,
        file,
        edit,
        commit,
        no_commit,
        message,
        json,
    } = args;
    let relation_inputs = relations;
    let (cfg, user_cfg, cwd) = load_context()?;
    if file.is_some() && edit {
        return Err(Error::new("Cannot use both --file and --edit"));
    }
    // A full-doc --file seeds metadata: explicit flags win, and the id stays ours
    let file_input = file.as_deref().map(read_file_input).transpose()?;
    let incoming = match (&file_input, file.as_deref()) {
        (Some(contents), Some(path)) if has_frontmatter(contents) => {
            Some(parse_input_doc(contents, path)?)
        }
        _ => None,
    };
    let creation_defaults = user_cfg.creation_defaults();
    let kind_value = command_kind
        .clone()
        .or_else(|| incoming.as_ref().map(|doc| doc.kind.clone()))
        .or_else(|| creation_defaults.kind.clone())
        .unwrap_or_else(|| "issue".to_string());
    let is_milestone = kind_value.eq_ignore_ascii_case("milestone");
    // Normalize "issue" to "task" for the kind field
    let kind = if is_milestone {
        "milestone".to_string()
    } else if kind_value == "issue" {
        "task".to_string()
    } else {
        kind_value.clone()
    };
    let status = state::normalize(
        &status_flag
            .clone()
            .or_else(|| incoming.as_ref().map(|doc| doc.status.clone()))
            .or_else(|| creation_defaults.status.clone())
            .unwrap_or_else(|| state::TODO.to_string()),
    );
    let mut combined_labels = creation_defaults.labels.clone();
    combined_labels.extend(labels);
    let assignee_value = assignee
        .as_deref()
        .map(|s| s.to_string())
        .or_else(|| incoming.as_ref().and_then(|doc| doc.assignee.clone()))
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
    let mut relations = parse_relations(&relation_inputs)?;
    if let Some(doc) = &incoming {
        extend_unique(&mut combined_labels, doc.labels.iter().cloned());
        extend_unique(&mut deps, doc.deps.iter().cloned());
        extend_unique(&mut relations, doc.relations.iter().cloned());
        milestone = milestone.or_else(|| doc.milestone.clone());
    }

    // Load schema and validate kind/status
    let schema = load_schema(&store.path, Some(&project_name))?;
    let states = user_cfg.state_config()?;
    schema.validate_kind(&kind)?;
    states.validate(&status)?;
    if let Some(doc) = &incoming {
        schema.validate_custom_fields(&kind, &doc.frontmatter_extra)?;
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
        // Use kind template for body
        let body_template = load_kind_template(&store.path, Some(&project_name), &kind);
        create_rune(
            &store,
            &project_name,
            &kind,
            &title,
            &body_template,
            &status,
            parent.as_deref(),
            milestone.as_deref(),
            &combined_labels,
            &relations,
            &deps,
            resolved_assignee.as_deref(),
            id_override.as_deref(),
        )?
    };
    if let Some(contents) = file_input {
        let mut doc = parse_doc(&doc_path)?;
        match incoming {
            Some(input_doc) => {
                doc.body = input_doc.body;
                doc.frontmatter_extra = input_doc.frontmatter_extra;
            }
            None => doc.body = contents,
        }
        let (body, effective_title) = ensure_title(&doc.body, &title);
        doc.body = body;
        doc.title = effective_title;
        fs::write(&doc_path, render_doc(&doc))?;
    } else if edit {
        // Use a draft file so the original stays clean until validation passes
        let original_content = fs::read_to_string(&doc_path)?;
        let tmp_path = draft_path(&store.name, &identifier, &title, &original_content)?;
        fs::copy(&doc_path, &tmp_path)?;
        open_editor(&tmp_path)?;
        let mut edited_doc = parse_doc(&tmp_path)?;
        // Validate after editor changes
        if let Err(e) = normalize_status(&mut edited_doc, &states) {
            eprintln!("error: {e}");
            eprintln!("Your edits are saved in: {}", tmp_path.display());
            eprintln!(
                "Fix and apply with: runes edit {identifier} -f {}",
                tmp_path.display()
            );
            // Clean up the newly created doc since it was never valid
            let _ = fs::remove_file(&doc_path);
            return Err(Error::new("Validation failed after editor edit"));
        }
        if let Err(e) =
            schema.validate_custom_fields(&edited_doc.kind, &edited_doc.frontmatter_extra)
        {
            eprintln!("error: {e}");
            eprintln!("Your edits are saved in: {}", tmp_path.display());
            eprintln!(
                "Fix and apply with: runes edit {identifier} -f {}",
                tmp_path.display()
            );
            let _ = fs::remove_file(&doc_path);
            return Err(Error::new("Validation failed after editor edit"));
        }
        let mut doc = edited_doc;
        let (body, effective_title) = ensure_title(&doc.body, &title);
        doc.body = body;
        doc.title = effective_title;
        fs::write(&doc_path, render_doc(&doc))?;
        let _ = fs::remove_file(&tmp_path);
    }
    let final_path = reconcile_filename(&doc_path, &identifier)?;
    // Content supplied up front (-e/-f/-m) is finished work; a bare `new` leaves a draft.
    let should_commit = !no_commit && (commit || edit || file.is_some() || message.is_some());
    if should_commit {
        let default_msg = build_commit_message("Add", &identifier, std::slice::from_ref(&status));
        maybe_commit(
            &store,
            false,
            message.as_deref(),
            &default_msg,
            &user_cfg,
            std::slice::from_ref(&final_path),
        )?;
    } else {
        cache::rebuild_cache(&store)?;
    }
    let abs_path = absolute_path(&final_path, &cwd);
    if json {
        let out = serde_json::json!({
            "id": identifier,
            "path": abs_path.display().to_string(),
            "committed": should_commit,
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
        return Ok(());
    }
    if should_commit {
        println!("Created {identifier}");
    } else {
        println!("Created {identifier} (uncommitted)");
    }
    println!("{}", abs_path.display());
    Ok(())
}

/// Absolute form of `path`, resolved against `cwd` when the store root is relative.
fn absolute_path(path: &Path, cwd: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

fn resolve_store_and_project_from_spec(
    stores: &[Store],
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
    let store = resolve_store_with_context(stores, user_config, cwd, hint)?;
    Ok((store, project_trimmed.to_string()))
}

fn resolve_project_for_new(
    stores: &[Store],
    user_config: &UserConfig,
    cwd: &Path,
    store_hint: Option<&str>,
    project_arg: Option<&String>,
) -> Result<(Store, String)> {
    if let Some(spec) = project_arg {
        return resolve_store_and_project_from_spec(stores, user_config, cwd, store_hint, spec);
    }
    if let Ok(env_value) = std::env::var("RUNES_PROJECT") {
        let trimmed = env_value.trim();
        if !trimmed.is_empty() {
            return resolve_store_and_project_from_spec(
                stores,
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
                stores,
                user_config,
                cwd,
                store_hint,
                trimmed,
            );
        }
    }
    let store = resolve_store_with_context(stores, user_config, cwd, store_hint)?;
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
        if cursor.join("runes.kdl").exists() || has_vcs_marker(&cursor) {
            return Some(cursor);
        }
        if !cursor.pop() {
            return None;
        }
    }
}

fn has_vcs_marker(path: &Path) -> bool {
    path.join(".git").exists() || path.join(".jj").exists() || path.join(".pijul").exists()
}

/// Print a notice that the current repo has no runes project configured,
/// followed by `runes init` usage help. Shown when a list command would
/// otherwise spill runes from every project in the store.
fn print_uninitialized_notice() {
    println!("This repo has not been initialized for runes.");
    println!();
    println!("Run `runes init` to configure a project for this repo:");
    println!();
    let mut cmd = Cli::command();
    if let Some(init) = cmd.find_subcommand_mut("init") {
        let mut init = init.clone().bin_name("runes init");
        let _ = init.print_help();
        println!();
    }
}

fn run_list(args: ListArgs) -> Result<()> {
    let ListArgs {
        view,
        store,
        project,
        query,
        all,
        kind,
        status,
        assignee,
        archived,
        with_archived,
        labels,
        blocked,
        ready,
        blocked_by,
        blocks,
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
    let (store, project_proj) = resolve_store_and_project(
        &cfg,
        &user_cfg,
        &cwd,
        store.as_deref(),
        effective_project.as_ref(),
    )?;
    let status_flag_present = status.is_some();
    let kind_flag_present = kind.is_some();
    let assignee_filter = assignee
        .as_deref()
        .and_then(|value| user_cfg.resolve_user_alias(value));
    let mut list_kind = kind
        .as_deref()
        .map(ListKind::parse)
        .unwrap_or(ListKind::Issues);
    let mut kind_explicitly_set = kind_flag_present;
    let label_flag_present = !labels.is_empty();
    let blocked_filter = if blocked {
        Some(true)
    } else if ready {
        Some(false)
    } else {
        None
    };
    let states = user_cfg.state_config()?;
    let status = match status {
        Some(value) => {
            let normalized = state::normalize(&value);
            states.validate(&normalized)?;
            Some(normalized)
        }
        None => None,
    };
    let mut filters = CacheFilter {
        project: project_proj,
        statuses: status.map(|value| vec![value]).unwrap_or_default(),
        kind: None,
        assignee: assignee_filter,
        labels,
        archived: Some(archived_mode),
        blocked: blocked_filter,
        blocked_by,
        blocks,
    };
    let view_name = view
        .or(query)
        .or_else(|| all.then(|| VIEW_ALL.to_string()))
        .or_else(|| user_cfg.query_for_path(&cwd))
        .or_else(|| user_cfg.default_query.clone())
        .unwrap_or_else(|| VIEW_OPEN.to_string());
    let mut query_set_project = false;
    // Config-defined views shadow built-ins for back-compat, with a nudge to drop them.
    let builtin_view = if let Some(query_cfg) = user_cfg.query(&view_name) {
        eprintln!(
            "warning: custom views are deprecated while built-in views stabilize \
            (view \"{view_name}\" comes from your config)"
        );
        if !project_flag_present {
            if query_cfg.project.is_some() {
                query_set_project = true;
            }
            filters.project = query_cfg.project.clone();
        }
        if !status_flag_present {
            filters.statuses = query_cfg
                .statuses
                .iter()
                .map(|value| state::normalize(value))
                .collect();
        }
        if !kind_flag_present {
            if let Some(kind_value) = &query_cfg.kind {
                list_kind = ListKind::parse(kind_value);
                kind_explicitly_set = true;
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
        if !label_flag_present && !query_cfg.labels.is_empty() {
            filters.labels = query_cfg.labels.clone();
        }
        // Apply blocked/blocks/blocked-by from query if not set by CLI flags
        if filters.blocked.is_none() {
            filters.blocked = query_cfg.blocked;
        }
        if filters.blocks.is_none() {
            filters.blocks = query_cfg.blocks.clone();
        }
        if filters.blocked_by.is_none() {
            filters.blocked_by = query_cfg.blocked_by.clone();
        }
        None
    } else {
        BUILTIN_VIEWS
            .iter()
            .find(|(name, _)| *name == view_name)
            .map(|(name, _)| *name)
    };
    if let Some(view) = builtin_view {
        if view == VIEW_MINE && filters.assignee.is_none() {
            filters.assignee = user_cfg.resolve_user_alias("self");
        }
        if !status_flag_present {
            match view {
                VIEW_OPEN | VIEW_MINE => filters.statuses = open_statuses(),
                VIEW_CLOSED => filters.statuses = vec![state::CLOSED.to_string()],
                _ => {}
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
    filters.archived = Some(archived_mode);
    if kind_explicitly_set {
        filters.kind = Some(list_kind.kind_name().to_string());
    }
    // When no project context could be determined (no --project flag, no repo-local
    // runes.kdl, no default project), the repo likely hasn't been initialized for
    // runes. Listing every rune across all projects is confusing in a fresh repo, so
    // show an init hint instead. Skipped for --json to keep programmatic output stable.
    if !json && !project_flag_present && !query_set_project && filters.project.is_none() {
        print_uninitialized_notice();
        return Ok(());
    }
    // For --ready, add non-terminal status filter if no explicit statuses set
    if filters.blocked == Some(false) && filters.statuses.is_empty() {
        filters.statuses = open_statuses();
    }
    match list_kind {
        ListKind::Issues => {
            let mut rows = cache::query_cache(&store, &filters)?;
            let uncommitted_ids = uncommitted_rune_ids(&store, &rows);
            // Sort: uncommitted first, then by updated DESC (nulls last)
            rows.sort_by(|a, b| {
                let a_uncommitted = uncommitted_ids.contains(&a.id);
                let b_uncommitted = uncommitted_ids.contains(&b.id);
                match (a_uncommitted, b_uncommitted) {
                    (true, false) => std::cmp::Ordering::Less,
                    (false, true) => std::cmp::Ordering::Greater,
                    _ => match (b.updated, a.updated) {
                        (Some(b_ts), Some(a_ts)) => b_ts.cmp(&a_ts),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => a.id.cmp(&b.id),
                    },
                }
            });
            output_issue_rows(&store, &rows, &uncommitted_ids, json);
            if !json {
                warn_if_modified(&rows, &uncommitted_ids);
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
            rows.sort_by(|a, b| (&a.project, &a.id).cmp(&(&b.project, &b.id)));
            if json {
                let json_rows: Vec<serde_json::Value> =
                    rows.iter().map(|row| row.to_json(&store.name)).collect();
                println!("{}", serde_json::to_string_pretty(&json_rows).unwrap());
            } else {
                if rows.is_empty() {
                    return Err(Error::new("No milestones found"));
                }
                for row in &rows {
                    println!("{}", row.to_text());
                }
            }
            Ok(())
        }
    }
}

/// The non-terminal core states, which match their substates too.
fn open_statuses() -> Vec<String> {
    state::OPEN_STATES.iter().map(|s| s.to_string()).collect()
}

fn uncommitted_rune_ids(
    store: &Store,
    rows: &[cache::CacheRow],
) -> std::collections::HashSet<String> {
    // No timestamp means it never reached history: a draft. Anything else has
    // to be compared against what history recorded.
    rows.iter()
        .filter(|row| row.updated.is_none() || has_unrecorded_edits(store, row))
        .map(|row| row.id.clone())
        .collect()
}

/// Whether a rune that reached history has since drifted on disk. The mtime
/// gate keeps the content comparison off the common path: a commit never
/// rewrites the file it records, so an older mtime rules drift out.
fn has_unrecorded_edits(store: &Store, row: &cache::CacheRow) -> bool {
    let rel_path = Path::new(&row.path);
    row.updated
        .is_some_and(|ts| touched_since(store, rel_path, ts))
        && backend::file_rich_log(store, rel_path, 1)
            .ok()
            .and_then(|log| log.first().map(|entry| entry.revision.clone()))
            .is_some_and(|revision| has_drifted_from(store, rel_path, &revision))
}

/// Render rune rows in the shared list/search shape, in the order given.
fn output_issue_rows(
    store: &Store,
    rows: &[cache::CacheRow],
    uncommitted_ids: &std::collections::HashSet<String>,
    json: bool,
) {
    if json {
        let json_rows: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "kind": row.kind,
                    "id": row.id,
                    "title": row.title,
                    "store": store.name,
                    "project": row.project,
                    "path": row.path,
                    "status": row.status,
                    "assignee": if row.assignee.is_empty() { None } else { Some(&row.assignee) },
                    "labels": row.labels,
                    "updated": row.updated,
                    "uncommitted": uncommitted_ids.contains(&row.id),
                    "blocked": row.blocked,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&json_rows).unwrap());
    } else {
        print_issue_table(rows, uncommitted_ids);
    }
}

fn run_search(args: SearchArgs) -> Result<()> {
    let SearchArgs {
        term,
        store,
        project,
        status,
        labels,
        archived,
        with_archived,
        json,
    } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let project_flag_present = project.is_some();
    let effective_project = project.filter(|p| !p.is_empty());
    let (store, mut project_proj) = resolve_store_and_project(
        &cfg,
        &user_cfg,
        &cwd,
        store.as_deref(),
        effective_project.as_ref(),
    )?;
    // `--project ''` searches every project; no flag falls back to the default.
    if project_proj.is_none() && !project_flag_present {
        if let Some(default_spec) = user_cfg.default_project.as_deref() {
            let (_, proj_name) = split_store_prefix(default_spec);
            if !proj_name.is_empty() {
                project_proj = Some(proj_name.to_string());
            }
        }
    }
    let states = user_cfg.state_config()?;
    let status = match status {
        Some(value) => {
            let normalized = state::normalize(&value);
            states.validate(&normalized)?;
            Some(normalized)
        }
        None => None,
    };
    let filters = CacheFilter {
        project: project_proj,
        // No status filter by default: finding closed runes is the point.
        statuses: status.map(|value| vec![value]).unwrap_or_default(),
        labels,
        archived: Some(if archived {
            ArchivedMode::Only
        } else if with_archived {
            ArchivedMode::Include
        } else {
            ArchivedMode::Exclude
        }),
        ..CacheFilter::default()
    };
    let rows = cache::search_cache(&store, &term, &filters)?;
    let uncommitted_ids = uncommitted_rune_ids(&store, &rows);
    output_issue_rows(&store, &rows, &uncommitted_ids, json);
    if !json && rows.is_empty() {
        println!("No runes match '{term}'.");
    }
    Ok(())
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

struct MilestoneRow {
    id: String,
    title: String,
    project: String,
    path: String,
    status: String,
    assignee: Option<String>,
    labels: Vec<String>,
    archived: bool,
    total: usize,
    closed: usize,
    wip: usize,
    todo: usize,
}

impl MilestoneRow {
    fn complete_pct(&self) -> f64 {
        if self.total == 0 {
            100.0
        } else {
            (self.closed as f64 / self.total as f64) * 100.0
        }
    }

    fn to_text(&self) -> String {
        format!(
            "milestone={} status={} total={} closed={} wip={} todo={} complete_pct={:.1}{} title={}",
            self.id,
            self.status,
            self.total,
            self.closed,
            self.wip,
            self.todo,
            self.complete_pct(),
            if self.archived { " archived=true" } else { "" },
            self.title
        )
    }

    fn to_json(&self, store_name: &str) -> serde_json::Value {
        serde_json::json!({
            "kind": "milestone",
            "id": self.id,
            "title": self.title,
            "store": store_name,
            "project": self.project,
            "path": self.path,
            "status": self.status,
            "assignee": self.assignee,
            "labels": self.labels,
            "archived": self.archived,
            "child_total": self.total,
            "child_closed": self.closed,
            "child_wip": self.wip,
            "child_todo": self.todo,
            "complete_pct": (self.complete_pct() * 10.0).round() / 10.0,
        })
    }
}

fn list_project_milestones(
    store: &Store,
    project: &str,
    archived_mode: ArchivedMode,
) -> Result<Vec<MilestoneRow>> {
    let mut rows = Vec::new();
    if archived_mode != ArchivedMode::Only {
        rows.append(&mut list_milestones_in_scope(store, project, false)?);
    }
    if archived_mode != ArchivedMode::Exclude {
        rows.append(&mut list_milestones_in_scope(store, project, true)?);
    }
    Ok(rows)
}

fn list_milestones_in_scope(
    store: &Store,
    project: &str,
    archived: bool,
) -> Result<Vec<MilestoneRow>> {
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
        let (total, closed, wip, todo) = count_milestone_children(&entry.path())?;
        let rel_path = milestone_file
            .strip_prefix(&store.path)
            .unwrap_or(&milestone_file);
        rows.push(MilestoneRow {
            id: doc.id,
            title: doc.title,
            project: project.to_string(),
            path: rel_path.display().to_string(),
            status: doc.status,
            assignee: doc.assignee,
            labels: doc.labels,
            archived,
            total,
            closed,
            wip,
            todo,
        });
    }
    Ok(rows)
}

/// Count a milestone's children as (total, closed, wip, todo), keyed by core state.
fn count_milestone_children(container: &Path) -> Result<(usize, usize, usize, usize)> {
    let mut total = 0;
    let mut closed = 0;
    let mut wip = 0;
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
        match state::core_of(&child.status) {
            state::CLOSED => closed += 1,
            state::WIP => wip += 1,
            _ => todo += 1,
        }
    }
    Ok((total, closed, wip, todo))
}
fn format_labels(labels: &[String], max: usize) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let shown: Vec<&str> = labels.iter().take(max).map(|s| s.as_str()).collect();
    let mut result = shown.join(",");
    if labels.len() > max {
        result.push_str(",...");
    }
    result
}

fn format_updated(updated: Option<i64>, is_uncommitted: bool) -> String {
    if is_uncommitted && updated.is_none() {
        return "(draft)".to_string();
    }
    match updated {
        Some(epoch_secs) => {
            use jiff::Timestamp;
            let Ok(ts) = Timestamp::from_second(epoch_secs) else {
                return String::new();
            };
            let zdt = ts.to_zoned(jiff::tz::TimeZone::system());
            zdt.strftime("%b %d %H:%M").to_string()
        }
        None => String::new(),
    }
}

fn terminal_width() -> Option<usize> {
    terminal_size::terminal_size().map(|(w, _)| w.0 as usize)
}

fn truncate_to_width(s: &str, max: usize) -> String {
    use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};
    if s.width() <= max {
        return s.to_string();
    }
    let budget = max.saturating_sub(3);
    let mut out = String::new();
    let mut width = 0;
    for c in s.chars() {
        let cw = c.width().unwrap_or(0);
        if width + cw > budget {
            break;
        }
        out.push(c);
        width += cw;
    }
    out.push_str("...");
    out
}

fn print_issue_table(
    rows: &[cache::CacheRow],
    uncommitted_ids: &std::collections::HashSet<String>,
) {
    use unicode_width::UnicodeWidthStr;
    if rows.is_empty() {
        return;
    }
    let has_labels = rows.iter().any(|r| !r.labels.is_empty());
    let _has_blocked = rows.iter().any(|r| r.blocked);
    // Build display strings
    let updated_strs: Vec<String> = rows
        .iter()
        .map(|r| format_updated(r.updated, uncommitted_ids.contains(&r.id)))
        .collect();
    let id_strs: Vec<String> = rows
        .iter()
        .map(|r| {
            if uncommitted_ids.contains(&r.id) {
                format!("{} *", r.id)
            } else {
                r.id.clone()
            }
        })
        .collect();
    let status_strs: Vec<String> = rows
        .iter()
        .map(|r| {
            if r.blocked {
                format!("{} [blocked]", r.status)
            } else {
                r.status.clone()
            }
        })
        .collect();
    // Calculate column widths
    let mut w_updated = "updated".len();
    let mut w_id = "id".len();
    let mut w_kind = "kind".len();
    let mut w_status = "status".len();
    let mut w_assignee = "assignee".len();
    let mut w_labels = if has_labels { "labels".len() } else { 0 };
    let mut w_title = "title".len();
    let label_strs: Vec<String> = rows.iter().map(|r| format_labels(&r.labels, 3)).collect();
    for (i, row) in rows.iter().enumerate() {
        w_updated = w_updated.max(updated_strs[i].len());
        w_id = w_id.max(id_strs[i].len());
        w_kind = w_kind.max(row.kind.len());
        w_status = w_status.max(status_strs[i].len());
        w_assignee = w_assignee.max(row.assignee.len());
        if has_labels {
            w_labels = w_labels.max(label_strs[i].len());
        }
        w_title = w_title.max(row.title.width());
    }
    // Cap the title column so rows fit the viewport; wider titles get truncated.
    if let Some(term_width) = terminal_width() {
        let fixed = w_updated
            + w_id
            + w_kind
            + w_status
            + w_assignee
            + if has_labels { w_labels + 2 } else { 0 }
            + 5 * 2; // two-space separators between columns
        w_title = w_title.min(term_width.saturating_sub(fixed).max("title".len()));
    }
    let title_strs: Vec<String> = rows
        .iter()
        .map(|r| truncate_to_width(&r.title, w_title))
        .collect();
    // Header
    if has_labels {
        println!(
            "{:<w_updated$}  {:<w_id$}  {:<w_kind$}  {:<w_status$}  {:<w_assignee$}  {:<w_labels$}  title",
            "updated", "id", "kind", "status", "assignee", "labels"
        );
        println!(
            "{:-<w_updated$}  {:-<w_id$}  {:-<w_kind$}  {:-<w_status$}  {:-<w_assignee$}  {:-<w_labels$}  {:-<w_title$}",
            "", "", "", "", "", "", ""
        );
    } else {
        println!(
            "{:<w_updated$}  {:<w_id$}  {:<w_kind$}  {:<w_status$}  {:<w_assignee$}  title",
            "updated", "id", "kind", "status", "assignee"
        );
        println!(
            "{:-<w_updated$}  {:-<w_id$}  {:-<w_kind$}  {:-<w_status$}  {:-<w_assignee$}  {:-<w_title$}",
            "", "", "", "", "", ""
        );
    }
    for (i, row) in rows.iter().enumerate() {
        let updated_display = color::gray(&format!("{:<w_updated$}", updated_strs[i]));
        let id_display = if uncommitted_ids.contains(&row.id) {
            let colored = color::colored_id(&row.id);
            format!("{} {}", colored, color::yellow("*"))
        } else {
            color::colored_id(&row.id)
        };
        let status_display = if row.blocked {
            let base = color::status_color(&row.status);
            format!("{} {}", base, color::yellow("[blocked]"))
        } else {
            color::status_color(&row.status)
        };
        // Pad based on raw (uncolored) lengths
        let id_pad = w_id.saturating_sub(id_strs[i].len());
        let status_pad = w_status.saturating_sub(status_strs[i].len());
        if has_labels {
            println!(
                "{}  {}{:id_pad$}  {:<w_kind$}  {}{:status_pad$}  {:<w_assignee$}  {:<w_labels$}  {}",
                updated_display, id_display, "", row.kind, status_display, "", row.assignee, label_strs[i], title_strs[i]
            );
        } else {
            println!(
                "{}  {}{:id_pad$}  {:<w_kind$}  {}{:status_pad$}  {:<w_assignee$}  {}",
                updated_display,
                id_display,
                "",
                row.kind,
                status_display,
                "",
                row.assignee,
                title_strs[i]
            );
        }
    }
}

fn run_show(args: ShowArgs) -> Result<()> {
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, path) = resolve_rune_id(&cfg, &user_cfg, &cwd, &args.id)?;
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
        let meta = content.split("---").nth(1).map(|s| s.trim()).unwrap_or("");
        // Resolve dep statuses
        let deps_resolved: Vec<serde_json::Value> = doc
            .deps
            .iter()
            .map(|dep_id| {
                let dep_status = cache::lookup_status(&store, dep_id).ok().flatten();
                serde_json::json!({
                    "id": dep_id,
                    "status": dep_status,
                })
            })
            .collect();
        let json = serde_json::json!({
            "kind": doc.kind,
            "id": doc.id,
            "title": doc.title,
            "store": store.name,
            "project": project,
            "path": rel_path,
            "status": doc.status,
            "assignee": doc.assignee,
            "deps": deps_resolved,
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
    // Display dep status inline
    if !doc.deps.is_empty() {
        println!("deps:");
        for dep_id in &doc.deps {
            let dep_status = cache::lookup_status(&store, dep_id).ok().flatten();
            match dep_status {
                Some(status) => println!("  {} ({})", dep_id, status),
                None => println!("  {} (unknown)", dep_id),
            }
        }
    }
    if doc.kind == "milestone" {
        if let Some(container) = path.parent() {
            if container.exists() {
                let (total, closed, wip, todo) = count_milestone_children(container)?;
                let pct = if total == 0 {
                    100.0
                } else {
                    (closed as f64 / total as f64) * 100.0
                };
                println!("child_total={total} child_closed={closed} child_wip={wip} child_todo={todo} complete_pct={pct:.1}");
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
    // Only a rune that reached history can carry unrecorded modifications;
    // `show` already marks a draft as `<not committed>` in its frontmatter.
    if history
        .first()
        .is_some_and(|entry| has_drifted_from(&store, rel_path, &entry.revision))
    {
        warn_modified(&[&doc.id]);
    }
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

fn print_annotated_rune_doc(content: &str, history: &[LogEntry], store: &Store, rel_path: &Path) {
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
        injected.push_str(&format!(
            "  created_at \"{}\"\n",
            format_timestamp_local(created.timestamp)
        ));
    }
    if updated.revision != created.revision {
        if !updated.author.is_empty() && updated.author != created.author {
            injected.push_str(&format!("  updated_by \"{}\"\n", updated.author));
        }
        if updated.timestamp > 0 {
            injected.push_str(&format!(
                "  updated_at \"{}\"\n",
                format_timestamp_local(updated.timestamp)
            ));
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
    print_annotated_body(
        &body,
        &section_annotations,
        &comment_attributions,
        &created.revision,
    );
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
                        println!(
                            "{}",
                            color::gray(&format!("Edited by {} on {}", ann.last_editor, ts))
                        );
                    }
                }
                i += 1;
                continue;
            }
        }

        if in_comments {
            if line.trim() == "---" {
                // Flush buffered comment with attribution
                flush_comment_buf(
                    &mut comment_buf,
                    comment_attrs,
                    &mut comment_idx,
                    &mut comment_header_printed,
                );
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
    flush_comment_buf(
        &mut comment_buf,
        comment_attrs,
        &mut comment_idx,
        &mut comment_header_printed,
    );
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
        add_deps,
        remove_deps,
        file,
        edit,
        no_commit,
        message,
    } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, path) = resolve_rune_id(&cfg, &user_cfg, &cwd, &id)?;
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
        || !remove_relations.is_empty()
        || !add_deps.is_empty()
        || !remove_deps.is_empty();
    if file.is_some() && edit {
        return Err(Error::new("Cannot use both --file and --edit"));
    }
    if edit && has_field_edits {
        return Err(Error::new("Cannot mix field edits with --edit"));
    }
    // Load schema for validation
    let parsed_id = parse_full_id(&doc.id)?;
    let schema = load_schema(&store.path, Some(&parsed_id.project))?;
    let states = user_cfg.state_config()?;

    // --file first, then field flags on top: explicit flags win over the file's values
    let has_file = file.is_some();
    if let Some(file_path) = file {
        let contents = read_file_input(&file_path)?;
        if has_frontmatter(&contents) {
            let mut input_doc = parse_input_doc(&contents, &file_path)?;
            if input_doc.id != doc.id {
                return Err(Error::new(format!(
                    "Frontmatter id '{}' does not match rune '{}'",
                    input_doc.id, doc.id
                )));
            }
            schema.validate_kind(&input_doc.kind)?;
            normalize_status(&mut input_doc, &states)?;
            schema.validate_custom_fields(&input_doc.kind, &input_doc.frontmatter_extra)?;
            doc = RuneDoc {
                path: doc.path,
                ..input_doc
            };
        } else {
            doc.body = contents;
        }
        let (body, effective_title) = ensure_title(&doc.body, &original_title);
        doc.body = body;
        doc.title = effective_title;
    }

    if has_field_edits {
        if let Some(status_value) = status {
            doc.status = status_value;
            normalize_status(&mut doc, &states)?;
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
        for dep in add_deps {
            if !doc.deps.iter().any(|d| d == &dep) {
                doc.deps.push(dep);
            }
        }
        for dep in remove_deps {
            doc.deps.retain(|d| d != &dep);
        }
        if let Some(title_value) = &title {
            if title_value.is_empty() {
                // Empty --title means keep the original title
            } else {
                doc.title = title_value.clone();
                doc.body = replace_title(&doc.body, title_value);
            }
        }
    }

    if has_field_edits || has_file {
        fs::write(&path, render_doc(&doc))?;
    } else if edit || editor_available() {
        // Use a draft file for editor-based edits so we can validate before writing
        let original_content = render_doc(&original_doc);
        let tmp_path = draft_path(&store.name, &doc.id, &original_title, &original_content)?;
        fs::copy(&path, &tmp_path)?;
        open_editor(&tmp_path)?;
        let mut edited_doc = parse_doc(&tmp_path)?;
        // Validate status after editor changes
        if let Err(e) = normalize_status(&mut edited_doc, &states) {
            eprintln!("error: {e}");
            eprintln!("Your edits are saved in: {}", tmp_path.display());
            eprintln!(
                "Fix and apply with: runes edit {} -f {}",
                id,
                tmp_path.display()
            );
            return Err(Error::new("Validation failed after editor edit"));
        }
        // Validate custom fields
        if let Err(e) =
            schema.validate_custom_fields(&edited_doc.kind, &edited_doc.frontmatter_extra)
        {
            eprintln!("error: {e}");
            eprintln!("Your edits are saved in: {}", tmp_path.display());
            eprintln!(
                "Fix and apply with: runes edit {} -f {}",
                id,
                tmp_path.display()
            );
            return Err(Error::new("Validation failed after editor edit"));
        }
        doc = edited_doc;
        let (body, effective_title) = ensure_title(&doc.body, &original_title);
        doc.body = body;
        doc.title = effective_title;
        fs::write(&path, render_doc(&doc))?;
        // Clean up tmp file on success
        let _ = fs::remove_file(&tmp_path);
    } else {
        return Err(Error::new("No edits specified and no editor available"));
    }
    let final_path = reconcile_filename(&path, &doc.id)?;
    prune_drafts_for_rune(&store.name, &doc.id);
    let snippets = edit_change_snippets(&original_doc, &doc);
    let default_msg = build_commit_message("Update", &doc.id, &snippets);
    maybe_commit(
        &store,
        no_commit,
        message.as_deref(),
        &default_msg,
        &user_cfg,
        std::slice::from_ref(&final_path),
    )?;
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
    let (store, path) = resolve_rune_id(&cfg, &user_cfg, &cwd, &id)?;
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
        let tmp_path = draft_path(&store.name, &doc.id, &format!("comment {}", doc.title), "")?;
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
        for (i, line) in lines.iter().enumerate().skip(pos + 1) {
            if let Some(rest) = line.strip_prefix('#') {
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
    prune_drafts_for_rune(&store.name, &doc.id);
    let default_msg = build_commit_message("Comment on", &doc.id, &[]);
    maybe_commit(
        &store,
        no_commit,
        None,
        &default_msg,
        &user_cfg,
        std::slice::from_ref(&path),
    )?;
    Ok(())
}

fn run_commit(args: CommitArgs) -> Result<()> {
    let CommitArgs {
        target,
        store: store_flag,
        project: project_flag,
        message,
        author,
    } = args;
    let (cfg, user_cfg, cwd) = load_context()?;

    // Determine scope: specific rune, project directory, or entire store
    let (store, paths, scope_label) = if let Some(rune_id) = &target {
        // `runes commit <rune_id>` → commit a specific rune file
        let (s, doc_path) = resolve_rune_id(&cfg, &user_cfg, &cwd, rune_id)?;
        let doc = parse_doc(&doc_path)?;
        let rel = doc_path
            .strip_prefix(&s.path)
            .map_err(|e| Error::new(e.to_string()))?;
        (s, vec![rel.to_path_buf()], doc.id)
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
    source_path: &Path,
    to_project: &str,
    to_parent: Option<&str>,
) -> Result<()> {
    let source_doc = parse_doc(source_path)?;
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
            fs::remove_file(source_path)?;
        } else {
            let from_rel = source_path
                .strip_prefix(&from_store.path)
                .map_err(|e| Error::new(e.to_string()))?
                .to_path_buf();
            backend::remove_path(from_store, &from_rel)?;
            fs::remove_file(source_path)?;
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
    let (from_store, source_path) = resolve_rune_id(&cfg, &user_cfg, &cwd, &id)?;
    let source_doc = parse_doc(&source_path)?;
    let (to_store, project) =
        resolve_store_and_project_required(&cfg, &user_cfg, &cwd, None, &target_project)?;
    move_rune(
        &from_store,
        &to_store,
        &source_path,
        &project,
        parent.as_deref(),
    )?;
    let move_msg = format!("Move {} to {project}", source_doc.id);
    if from_store.name == to_store.name {
        maybe_commit(
            &from_store,
            no_commit,
            message.as_deref(),
            &move_msg,
            &user_cfg,
            &[],
        )?;
    } else {
        let move_in_msg = format!("Move in {} from {}", source_doc.id, from_store.name);
        maybe_commit(
            &to_store,
            no_commit,
            message.as_deref(),
            &move_in_msg,
            &user_cfg,
            &[],
        )?;
        // Commit the removal from the source store
        if !no_commit || message.is_some() {
            let default_from_msg = format!("Move out {} to {}", source_doc.id, to_store.name);
            let from_msg = message.as_deref().unwrap_or(&default_from_msg);
            let (author_name, author_email) = resolve_commit_author(&user_cfg, None)?;
            commit_store_changes(&from_store, &[], from_msg, &author_name, &author_email)?;
        }
    }
    Ok(())
}
fn run_archive(args: ArchiveArgs) -> Result<()> {
    let ArchiveArgs {
        id,
        no_commit,
        message,
    } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, path) = resolve_rune_id(&cfg, &user_cfg, &cwd, &id)?;
    let doc = archive_rune(&store, &path)?;
    let default_msg = format!("Archive {}", doc.id);
    maybe_commit(
        &store,
        no_commit,
        message.as_deref(),
        &default_msg,
        &user_cfg,
        &[],
    )?;
    Ok(())
}

fn archive_rune(store: &Store, source_path: &Path) -> Result<RuneDoc> {
    let doc = parse_doc(source_path)?;
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
        fs::rename(source_path, &target_path)?;
    }
    Ok(doc)
}
fn run_delete(args: DeleteArgs) -> Result<()> {
    let DeleteArgs {
        id,
        force,
        no_commit,
        message,
    } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    let (store, path) = resolve_rune_id(&cfg, &user_cfg, &cwd, &id)?;
    let rel_path = path
        .strip_prefix(&store.path)
        .map_err(|e| Error::new(e.to_string()))?;
    // A never-committed rune is just a draft file: discarding it destroys no
    // history, so it needs neither --force nor a commit to record the removal.
    let committed = backend::file_rich_log(&store, rel_path, 1).map_or(true, |log| !log.is_empty());
    if committed && !force {
        return Err(Error::new("Use --force to delete runes"));
    }
    let (doc, removed) = delete_rune(&store, &path)?;
    if !committed {
        return cache::rebuild_cache(&store);
    }
    let default_msg = format!("Delete {}", doc.id);
    // Scoped to what this delete removed: other pending drafts stay pending
    // instead of riding along in the "Delete <id>" commit.
    maybe_commit(
        &store,
        no_commit,
        message.as_deref(),
        &default_msg,
        &user_cfg,
        &removed,
    )?;
    Ok(())
}

/// Delete a rune, returning its doc and every path removed — a milestone takes
/// its whole container with it.
fn delete_rune(store: &Store, source_path: &Path) -> Result<(RuneDoc, Vec<PathBuf>)> {
    let doc = parse_doc(source_path)?;
    let mut removed = Vec::new();
    if doc.kind == "milestone" {
        let container = source_path
            .parent()
            .ok_or_else(|| Error::new("Invalid container path"))?;
        collect_files(container, &mut removed)?;
        fs::remove_dir_all(container)?;
    } else {
        removed.push(source_path.to_path_buf());
        fs::remove_file(source_path)?;
    }
    let rel_path = source_path
        .strip_prefix(&store.path)
        .map_err(|e| Error::new(e.to_string()))?
        .to_path_buf();
    backend::remove_path(store, &rel_path)?;
    println!("Deleted {}", doc.id);
    Ok((doc, removed))
}

/// Every file under `dir`, recursively.
fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(&path, out)?;
        } else {
            out.push(path);
        }
    }
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
        let year_days = if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
            366
        } else {
            365
        };
        if remaining < year_days {
            break;
        }
        remaining -= year_days;
        y += 1;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let month_days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut m = 0usize;
    for &md in &month_days {
        if remaining < md as i64 {
            break;
        }
        remaining -= md as i64;
        m += 1;
    }
    format!(
        "{y:04}-{:02}-{:02} {hours:02}:{minutes:02}",
        m + 1,
        remaining + 1
    )
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

/// A commit that passed the log filters, plus every rune it touched.
/// `--limit` counts these commits, so text and JSON agree even when one commit
/// expands into several rune rows.
struct MatchedEntry {
    revision: String,
    timestamp: i64,
    author: String,
    description: String,
    rune_ids: Vec<String>,
}

/// Filters `entries` and stops at the newest `limit` matching commits.
fn match_log_entries(
    entries: &[LogEntry],
    rune_filter: Option<&str>,
    project_filter: Option<&str>,
    author_filter: Option<&str>,
    limit: usize,
) -> Vec<MatchedEntry> {
    let project_prefix = project_filter.map(|p| format!("{p}-"));
    let mut matched = Vec::new();
    for entry in entries {
        if matched.len() >= limit {
            break;
        }
        if let Some(author) = author_filter {
            if !entry.author.eq_ignore_ascii_case(author) {
                continue;
            }
        }
        // Derive rune IDs from changed files; sorted so rows are stable across runs.
        let mut rune_ids: Vec<String> = entry
            .changed_files
            .iter()
            .filter_map(|f| rune_id_from_path(f))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        rune_ids.sort();

        let scoped = rune_filter.is_some() || project_prefix.is_some();
        if scoped {
            let hit = rune_ids.iter().any(|rid| match rune_filter {
                Some(filter_id) => rid == filter_id,
                None => rid.starts_with(project_prefix.as_deref().unwrap_or_default()),
            });
            if !hit {
                continue;
            }
        }
        matched.push(MatchedEntry {
            revision: entry.revision.clone(),
            timestamp: entry.timestamp,
            author: entry.author.clone(),
            description: entry.description.clone(),
            rune_ids,
        });
    }
    matched
}

/// Walks history in batches that double until `limit` commits match or the walk
/// hits the root, so a filtered log never materializes all of history up front.
fn collect_matching_entries<F>(
    mut walk: F,
    rune_filter: Option<&str>,
    project_filter: Option<&str>,
    author_filter: Option<&str>,
    limit: usize,
) -> Result<Vec<MatchedEntry>>
where
    F: FnMut(usize) -> Result<Vec<LogEntry>>,
{
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut walk_limit = limit;
    loop {
        let entries = walk(walk_limit)?;
        // A short batch means the walk hit the root: growing it further finds nothing.
        let exhausted = entries.len() < walk_limit;
        let matched =
            match_log_entries(&entries, rune_filter, project_filter, author_filter, limit);
        if exhausted || matched.len() >= limit {
            return Ok(matched);
        }
        walk_limit = walk_limit.saturating_mul(2);
    }
}

fn collect_log_entries(
    store: &Store,
    rune_filter: Option<&str>,
    project_filter: Option<&str>,
    author_filter: Option<&str>,
    limit: usize,
) -> Result<Vec<MatchedEntry>> {
    collect_matching_entries(
        |walk_limit| backend::rich_log(store, walk_limit),
        rune_filter,
        project_filter,
        author_filter,
        limit,
    )
}

fn print_log_entries_json(entries: &[MatchedEntry]) {
    let json_entries: Vec<_> = entries
        .iter()
        .map(|entry| {
            let comment = entry.description.lines().next().unwrap_or("").trim();
            serde_json::json!({
                "revision": entry.revision,
                "committed_at": entry.timestamp,
                "runes": entry.rune_ids,
                "comment": comment,
            })
        })
        .collect();
    println!("{}", serde_json::to_string_pretty(&json_entries).unwrap());
}

fn format_log_entries(
    entries: &[MatchedEntry],
    rune_filter: Option<&str>,
    project_filter: Option<&str>,
) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let project_prefix = project_filter.map(|p| format!("{p}-"));
    for entry in entries {
        let short_rev = &entry.revision[..entry.revision.len().min(12)];
        let ts = format_log_timestamp(entry.timestamp);
        let rev_colored = color::gray(short_rev);
        let ts_colored = color::teal(&ts);
        let author_colored = color::yellow(&entry.author);

        if entry.rune_ids.is_empty() {
            let desc = entry.description.lines().next().unwrap_or("").trim();
            let _ = writeln!(out, "{rev_colored}  {ts_colored}  {author_colored}  {desc}");
            continue;
        }

        // Every matching rune of a counted commit gets a row.
        for rune_id in &entry.rune_ids {
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
            let _ = writeln!(
                out,
                "{rev_colored}  {ts_colored}  {author_colored}  {id_colored}  {desc}"
            );
        }
    }
    out
}

fn run_log(args: LogArgs) -> Result<()> {
    let LogArgs {
        id,
        project,
        limit,
        section,
        changed_by,
        json,
        no_pager,
        all,
    } = args;
    let limit = limit.unwrap_or(50);
    let (cfg, user_cfg, cwd) = load_context()?;

    // Resolve scope: a specific rune (positional), a project (--project), all (--all),
    // or the default project (no args). Clap enforces mutual exclusion.
    let (rune_filter, project_filter) = match (&id, &project) {
        (Some(spec), _) => {
            let (_, id_part) = split_store_prefix(spec);
            if id_part.is_empty() {
                return Err(Error::new("ID may not be empty"));
            }
            let resolved = if id_part.contains('-') {
                // Full form (project-shortid): no FS lookup, so deleted runes still work.
                let (_, full) = resolve_store_and_id(&cfg, &user_cfg, &cwd, None, spec)?;
                full
            } else {
                // Bare shortid: scan filesystem to recover the project.
                let (_, path) = resolve_rune_id(&cfg, &user_cfg, &cwd, spec)?;
                parse_doc(&path)?.id
            };
            (Some(resolved), None)
        }
        (None, Some(proj)) => {
            let (_, proj_name) = split_store_prefix(proj);
            (None, Some(proj_name.to_string()))
        }
        (None, None) if all => (None, None),
        (None, None) => {
            // Use default project if configured
            let proj = user_cfg
                .default_project
                .as_ref()
                .map(|spec| {
                    let (_, proj_name) = split_store_prefix(spec);
                    proj_name.to_string()
                })
                .filter(|p| !p.is_empty());
            (None, proj)
        }
    };

    // Section filter requires a rune ID
    if section.is_some() && rune_filter.is_none() {
        return Err(Error::new(
            "--section requires a rune ID (e.g. proj:shortid)",
        ));
    }

    // If section is specified, use the section-diff logic
    if let (Some(rune_id), Some(section_raw)) = (&rune_filter, section) {
        let (store, path) = resolve_rune_id(&cfg, &user_cfg, &cwd, rune_id)?;
        let rel_path = path
            .strip_prefix(&store.path)
            .map_err(|e| Error::new(e.to_string()))?;
        let marker = if section_raw.starts_with('#') {
            section_raw
        } else {
            format!("## {section_raw}")
        };
        let change_ids = backend::file_change_ids(&store, rel_path, limit)?;
        let mut printed = 0usize;
        for change_id in change_ids {
            let details = backend::show_change(&store, &change_id, rel_path)?;
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
    // Filter, then limit: capping the walk at `limit` raw commits would hide runes
    // whose changes are older than that, so the walk grows until `limit` commits match.
    let entries = collect_log_entries(
        &store,
        rune_filter.as_deref(),
        project_filter.as_deref(),
        changed_by.as_deref(),
        limit,
    )?;
    if json {
        print_log_entries_json(&entries);
    } else {
        let output =
            format_log_entries(&entries, rune_filter.as_deref(), project_filter.as_deref());
        if output.is_empty() {
            println!("No matching changes found.");
        } else {
            color::print_with_pager(&output, no_pager);
        }
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
    let (store, path) = resolve_rune_id(&cfg, &user_cfg, &cwd, &id)?;
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
    let (store, path) = resolve_rune_id(&cfg, &user_cfg, &cwd, &id)?;
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
    maybe_commit(
        &store,
        no_commit,
        message.as_deref(),
        &default_msg,
        &user_cfg,
        std::slice::from_ref(&final_path),
    )?;
    Ok(())
}

fn run_sync(args: SyncArgs) -> Result<()> {
    let SyncArgs { store, all } = args;
    let (cfg, user_cfg, cwd) = load_context()?;
    if all {
        for store in &cfg {
            backend::sync(store)?;
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
        StoreCommand::Doctor { store } => store_doctor(store),
    }
}
fn store_init(
    name: String,
    backend_s: Option<String>,
    path: Option<PathBuf>,
    set_default: bool,
) -> Result<()> {
    let path = if let Some(path_arg) = path {
        path_arg
    } else {
        default_store_path(&name)?
    };
    let backend_kind = BackendKind::parse(backend_s.as_deref().unwrap_or(DEFAULT_BACKEND))?;
    create_store(&name, &backend_kind, &path, set_default)
}

/// Config records the store because discovery only scans `~/.runes/stores`,
/// and `--path` can put one anywhere.
fn create_store(name: &str, backend: &BackendKind, path: &Path, set_default: bool) -> Result<()> {
    backend::init_store(path, backend.clone())?;
    let global_path = user_config::global_config_path()?;
    user_config::config_set(
        &global_path,
        &format!("store.{name}.backend"),
        backend.as_str(),
    )?;
    user_config::config_set(
        &global_path,
        &format!("store.{name}.path"),
        &path.display().to_string(),
    )?;
    let has_default = user_config::config_get(&global_path, "defaults.store")?.is_some();
    if set_default || !has_default {
        user_config::config_set(&global_path, "defaults.store", name)?;
    }
    println!(
        "Initialized {} store '{name}' at {}",
        backend.as_str(),
        path.display()
    );
    Ok(())
}

fn store_list() -> Result<()> {
    let (stores, user_cfg, _) = load_context()?;
    let default_store = user_cfg.default_store.as_deref();
    for store in &stores {
        let marker = if default_store == Some(store.name.as_str()) {
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

fn store_info(name: Option<String>) -> Result<()> {
    let (stores, user_cfg, cwd) = load_context()?;
    let store = if let Some(name) = name {
        get_store(&stores, &name)?
    } else {
        resolve_store_with_context(&stores, &user_cfg, &cwd, None)?
    };
    println!("store \"{}\" {{", store.name);
    println!("  backend=\"{}\"", backend::adapter_name(&store));
    println!("  path=\"{}\"", store.path.display());
    // Status
    let status = backend::status(&store)?;
    println!("  status {{");
    for line in status.trim().lines() {
        println!("    {line}");
    }
    // Uncommitted runes
    match backend::uncommitted_rune_paths(&store) {
        Ok(paths) if !paths.is_empty() => {
            for p in &paths {
                let rune_id = rune_id_from_store_path(p);
                if let Some(id) = &rune_id {
                    println!("    uncommitted \"{id}\" path=\"{}\"", p.display());
                } else {
                    println!("    uncommitted path=\"{}\"", p.display());
                }
            }
        }
        _ => {}
    }
    println!("  }}");
    println!("}}");
    Ok(())
}

/// Extract a rune ID from a store-relative path like `project/short--slug.md` → `project-short`
fn rune_id_from_store_path(rel_path: &Path) -> Option<String> {
    let project = rel_path.parent()?.file_name()?.to_str()?;
    let filename = rel_path.file_name()?.to_str()?;
    let stem = filename.strip_suffix(".md")?;
    let short = stem.split("--").next()?;
    Some(format!("{project}-{short}"))
}

fn store_remove(name: String) -> Result<()> {
    let (stores, _, _) = load_context()?;
    let store = get_store(&stores, &name)?;
    eprintln!("To remove store '{name}', delete its directory:");
    eprintln!("  rm -rf {}", store.path.display());
    Ok(())
}
/// Rewrite a legacy status onto its core state, leaving the rest of the file alone.
/// Returns `None` when nothing needs migrating.
fn migrate_status_line(text: &str) -> Option<String> {
    let mut out = String::with_capacity(text.len());
    let mut delimiters = 0;
    let mut migrated = false;
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim();
        if trimmed == "---" {
            delimiters += 1;
        }
        // Only the first status, and only inside the frontmatter block.
        if !migrated && delimiters == 1 {
            if let Some(value) = trimmed
                .strip_prefix("status \"")
                .and_then(|rest| rest.split('"').next())
            {
                let normalized = state::normalize(value);
                if normalized != value {
                    out.push_str(
                        &line.replace(&format!("\"{value}\""), &format!("\"{normalized}\"")),
                    );
                    migrated = true;
                    continue;
                }
            }
        }
        out.push_str(line);
    }
    migrated.then_some(out)
}

/// Migrate every rune in the store off the pre-substate statuses.
/// Returns the store-relative paths that changed.
fn migrate_store_statuses(store: &Store) -> Result<Vec<PathBuf>> {
    let mut migrated = Vec::new();
    for project in all_projects(store)? {
        for path in discover_project_docs(&store.path.join(&project))? {
            let Some(updated) = migrate_status_line(&fs::read_to_string(&path)?) else {
                continue;
            };
            fs::write(&path, updated)?;
            let rel = path.strip_prefix(&store.path).unwrap_or(&path);
            migrated.push(rel.to_path_buf());
        }
    }
    Ok(migrated)
}

fn store_doctor(store_name: String) -> Result<()> {
    let store = load_store(&store_name)?;
    let migrated = migrate_store_statuses(&store)?;
    if !migrated.is_empty() {
        let (_, user_cfg, _) = load_context()?;
        let (author_name, author_email) = resolve_commit_author(&user_cfg, None)?;
        commit_store_changes(
            &store,
            &migrated,
            "Migrate statuses to todo/wip/closed",
            &author_name,
            &author_email,
        )?;
        println!(
            "Migrated {} rune(s) to the todo/wip/closed states",
            migrated.len()
        );
    }
    cache::rebuild_cache(&store)?;
    println!("Cache rebuilt for {}", store.name);
    let max_age = std::time::Duration::from_secs(DRAFT_MAX_AGE_DAYS * 24 * 60 * 60);
    let pruned = prune_aged_drafts(&store.name, max_age)?;
    if pruned > 0 {
        println!("Pruned {pruned} draft(s) older than {DRAFT_MAX_AGE_DAYS} days");
    }
    Ok(())
}

fn load_store(name: &str) -> Result<Store> {
    let (stores, _, _) = load_context()?;
    get_store(&stores, name)
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
                user_config::local_config_path(&cwd).ok_or_else(|| {
                    Error::new("Not in a repo. Use --global or run from a repo root.")
                })?
            };
            user_config::config_set(&path, &key, &value)?;
            Ok(())
        }
        ConfigCommand::Unset { key, global } => {
            let path = if global {
                user_config::global_config_path()?
            } else {
                user_config::local_config_path(&cwd).ok_or_else(|| {
                    Error::new("Not in a repo. Use --global or run from a repo root.")
                })?
            };
            user_config::config_unset(&path, &key)?;
            Ok(())
        }
    }
}

fn run_init(args: InitArgs) -> Result<()> {
    let cwd = std::env::current_dir().map_err(|e| Error::new(e.to_string()))?;
    let global_path = user_config::global_config_path()?;

    // Local config lands at the repo/config root if there is one, else cwd.
    let local_path = user_config::local_config_path(&cwd).unwrap_or_else(|| cwd.join("runes.kdl"));
    let root = local_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| cwd.clone());
    if args.stealth && !root.join(".git").exists() {
        return Err(Error::new("--stealth only works in a git repo"));
    }

    // Identity only; the store is settled below, so an existing config with no
    // store still gets one.
    if !global_path.exists() {
        if stdin_is_tty() {
            println!("Creating global config at {}", global_path.display());
        }
        let email = initial_identity_email()?;
        user_config::config_set(&global_path, "user.email", &email)?;
        user_config::config_set(&global_path, "new.task.assignee", "self")?;
        println!("Global config created at {}", global_path.display());
    }

    let (stores, user_cfg, _) = load_context()?;

    let spec = if local_path.exists() {
        None
    } else if let Some(spec) = args.project.clone() {
        Some(spec)
    } else if stdin_is_tty() {
        let dir_name = root
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("myproject");
        // Only offer the store prefix once there is a choice to make.
        let default_spec = match user_cfg.default_store.as_deref() {
            Some(store) if stores.len() > 1 => format!("{store}:{dir_name}"),
            _ => dir_name.to_string(),
        };
        Some(prompt_line("Project prefix", &default_spec)?)
    } else {
        return Err(Error::new(
            "Use --project to specify the project prefix non-interactively.",
        ));
    };
    let (store_hint, project) = match &spec {
        Some(spec) => {
            let (store, project) = split_store_prefix(spec);
            (store, Some(project.to_string()))
        }
        None => (None, None),
    };

    let store_name = ensure_store(&stores, &user_cfg, store_hint.as_deref())?;

    if let Some(project) = project {
        if project.is_empty() {
            return Err(Error::new("Project prefix cannot be empty"));
        }
        // Unpinned repos follow the global default store as it changes.
        if store_hint.is_some() {
            user_config::config_set(&local_path, "defaults.store", &store_name)?;
        }
        user_config::config_set(&local_path, "defaults.project", &project)?;

        if args.stealth {
            let info_dir = root.join(".git").join("info");
            fs::create_dir_all(&info_dir)?;
            let exclude_path = info_dir.join("exclude");
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
        println!(
            "Local config created at {} (store '{store_name}', project '{project}')",
            local_path.display()
        );
    } else {
        println!("Local config already exists at {}", local_path.display());
    }

    if !args.no_skill {
        install_skill(args.force_skill)?;
    }

    Ok(())
}

/// Falls back to the environment so a non-interactive first run is not a dead end.
fn initial_identity_email() -> Result<String> {
    if stdin_is_tty() {
        return prompt_required("User email");
    }
    match std::env::var("RUNES_USER") {
        Ok(value) if !value.trim().is_empty() => Ok(parse_author_string(&value).1),
        _ => Err(Error::new(
            "No global config yet. Run `runes init` interactively, or set an identity first \
             with `runes config set user.email <you@example.com> --global`.",
        )),
    }
}

/// The store `runes init` wires the repo to, created when this machine has none.
/// A name matching no existing store is read as a typo: quietly making a second
/// store is the worse guess.
fn ensure_store(stores: &[Store], user_cfg: &UserConfig, hint: Option<&str>) -> Result<String> {
    let wanted = hint.or(user_cfg.default_store.as_deref());
    if let Some(name) = wanted {
        if stores.iter().any(|s| s.name == name) {
            return Ok(name.to_string());
        }
        if !stores.is_empty() {
            let fallback = user_cfg
                .default_store
                .as_deref()
                .filter(|d| stores.iter().any(|s| s.name == *d))
                .unwrap_or(stores[0].name.as_str());
            return Err(Error::new(if hint.is_some() {
                format!(
                    "Unknown store '{name}'. Run `runes store init {name}` to create it, \
                     or use the default store '{fallback}'."
                )
            } else {
                format!(
                    "The default store '{name}' does not exist. Run `runes store init {name}` \
                     to create it, or point at another with \
                     `runes config set defaults.store <name> --global`."
                )
            }));
        }
    }

    let interactive = stdin_is_tty();
    if interactive {
        println!("No store found; creating one.");
    }
    let name = match wanted {
        Some(name) => name.to_string(),
        None if interactive => prompt_line("Store name", DEFAULT_STORE_NAME)?,
        None => DEFAULT_STORE_NAME.to_string(),
    };
    let backend = if interactive {
        prompt_line("Backend (jj or pijul)", DEFAULT_BACKEND)?
    } else {
        DEFAULT_BACKEND.to_string()
    };
    create_store(
        &name,
        &BackendKind::parse(&backend)?,
        &default_store_path(&name)?,
        true,
    )?;
    Ok(name)
}

/// Ask on stderr and read one line, falling back to `default` on an empty answer.
fn prompt_line(label: &str, default: &str) -> Result<String> {
    eprint!("{label} [{default}]: ");
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| Error::new(e.to_string()))?;
    let input = input.trim();
    if input.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(input.to_string())
    }
}

/// Keep asking until non-empty: a blank would be stored as an identity nothing
/// can commit under.
fn prompt_required(label: &str) -> Result<String> {
    loop {
        eprint!("{label}: ");
        let mut input = String::new();
        let read = io::stdin()
            .read_line(&mut input)
            .map_err(|e| Error::new(e.to_string()))?;
        let trimmed = input.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
        if read == 0 {
            return Err(Error::new(format!("{label} is required")));
        }
    }
}

/// This machine's stores, for `runes init --help`.
fn stores_help_text() -> Option<String> {
    let (stores, user_cfg, _) = load_context().ok()?;
    if stores.is_empty() {
        return Some("Stores:\n  none yet - init creates one".to_string());
    }
    let default = user_cfg.default_store.as_deref();
    let width = stores.iter().map(|s| s.name.len()).max().unwrap_or(0);
    let mut out = String::from("Stores (* = default):\n");
    for store in &stores {
        let marker = if default == Some(store.name.as_str()) {
            '*'
        } else {
            ' '
        };
        out.push_str(&format!(
            "  {marker} {:width$}  {:5}  {}\n",
            store.name,
            store.backend.as_str(),
            store.path.display()
        ));
    }
    Some(out.trim_end().to_string())
}

/// Skill locations relative to $HOME. Codex, Gemini and Cursor are left out:
/// they have no skill directory, only a global instructions file the user owns.
const SKILL_PATHS: &[&str] = &[
    ".claude/skills/runes/SKILL.md",
    ".agents/skills/runes/SKILL.md",
];

const SKILL_DESCRIPTION: &str = "Track tasks and issues as VCS-backed markdown docs via the runes CLI. Use when creating, finding, updating, commenting on, or committing runes (tasks/issues) in a project that uses runes.";

fn skill_document() -> Result<String> {
    let mut body = Vec::new();
    write_quickstart(
        &mut body,
        QuickstartMode {
            audience: Audience::Agent,
            live: false,
        },
    )?;
    let body = String::from_utf8(body).map_err(|e| Error::new(e.to_string()))?;
    Ok(format!(
        "---\nname: \"Runes task tracking\"\ndescription: \"{SKILL_DESCRIPTION}\"\n---\n\n{body}"
    ))
}

fn install_skill(force: bool) -> Result<()> {
    let home = home_dir()?;
    let document = skill_document()?;
    let mut installed = false;
    let mut failure = None;
    for relative in SKILL_PATHS {
        let path = home.join(relative);
        match write_skill(&path, &document, force) {
            Ok(()) => installed = true,
            Err(e) => {
                // One unwritable agent directory should not fail the whole init.
                eprintln!(
                    "warning: could not install the agent skill at {}: {e}",
                    path.display()
                );
                failure = Some(e);
            }
        }
    }
    match failure {
        Some(e) if !installed => Err(e),
        _ => Ok(()),
    }
}

/// Writes the skill unless the file on disk was hand-edited. Identical content
/// is rewritten silently, so re-running `runes init` keeps the skill in step
/// with the binary without any output.
fn write_skill(path: &Path, document: &str, force: bool) -> Result<()> {
    if !force && path.exists() {
        if fs::read_to_string(path).unwrap_or_default() != document {
            println!(
                "Agent skill at {} differs; leaving it alone (use --force-skill to overwrite)",
                path.display()
            );
        }
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, document)?;
    println!("Agent skill written to {}", path.display());
    Ok(())
}

/// Who the guide is for: humans work through `$EDITOR`, agents patch the doc
/// file they were given and commit it by id.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Audience {
    Human,
    Agent,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct QuickstartMode {
    audience: Audience,
    /// Live text may describe this machine. The installed skill is one
    /// machine-global file, so it must depend on nothing but the binary -
    /// otherwise a later init reports a hand edit that never happened.
    live: bool,
}

fn run_quickstart(args: QuickstartArgs) -> Result<()> {
    let stdout = io::stdout();
    let mode = QuickstartMode {
        audience: quickstart_audience(&args, &system_env),
        live: true,
    };
    write_quickstart(&mut stdout.lock(), mode)
}

/// Flag, then a detected agent, then human. Deliberately not isatty: agents
/// often run under a PTY and a human piping to a pager has none, so it
/// misclassifies both ways.
fn quickstart_audience(args: &QuickstartArgs, env: EnvLookup) -> Audience {
    if args.agent {
        return Audience::Agent;
    }
    if args.human {
        return Audience::Human;
    }
    if detect_agent(env).is_some() {
        Audience::Agent
    } else {
        Audience::Human
    }
}

fn backend_label(kind: &BackendKind) -> &'static str {
    match kind {
        BackendKind::Pijul => "pijul",
        BackendKind::Jj => "jj",
    }
}

/// The human guide opens with what is set up and what is not, rather than a
/// wall of paths: global config, then the store this repo writes to.
fn write_init_status(
    out: &mut impl io::Write,
    has_local: bool,
    active_store: Option<&Store>,
) -> Result<()> {
    const OK: &str = "\u{2713}";
    const NO: &str = "\u{2717}";

    writeln!(out, "  {OK} runes is initialized globally")?;
    match (has_local, active_store) {
        (true, Some(store)) => {
            writeln!(
                out,
                "  {OK} repo is configured to use store \"{}\" (backend: {})",
                store.name,
                backend_label(&store.backend)
            )?;
            writeln!(out, "      {OK} local:  {}", store.path.display())?;
            // Remote listing is best-effort: a store that cannot be opened is
            // still worth reporting as "no remote" rather than failing the guide.
            let remotes = backend::remotes(store).unwrap_or_default();
            if remotes.is_empty() {
                writeln!(out, "      {NO} remote: not configured")?;
            } else {
                writeln!(out, "      {OK} remote: {}", remotes.join(", "))?;
            }
        }
        (true, None) => {
            writeln!(
                out,
                "  {NO} repo names a store that is not set up - run `runes init` here"
            )?;
        }
        (false, _) => {
            writeln!(
                out,
                "  {NO} repo is not configured - run `runes init` here to create a local runes.kdl"
            )?;
        }
    }
    writeln!(out)?;
    Ok(())
}

/// Shared by `runes quickstart` and the skill `runes init` installs, so the
/// installed skill cannot drift from the live guide.
fn write_quickstart(out: &mut impl io::Write, mode: QuickstartMode) -> Result<()> {
    let live = mode.live;
    let for_agent = mode.audience == Audience::Agent;

    let (all_stores, user_cfg) = if live {
        load_context()
            .map(|(stores, cfg, _)| (stores, cfg))
            .unwrap_or_default()
    } else {
        Default::default()
    };

    let default_store = user_cfg.default_store.as_deref();
    let default_project = user_cfg.default_project.as_deref();

    // Resolve the active store
    let active_store = default_store.and_then(|name| all_stores.iter().find(|s| s.name == name));

    // Load schema if we have a store
    let schema = active_store.and_then(|store| load_schema(&store.path, default_project).ok());

    // -- Header --
    writeln!(
        out,
        "runes - A local-first issue tracker stored as markdown rune docs"
    )?;
    writeln!(out)?;

    // -- Initialization -- (live only: it reports this machine's setup)
    if live {
        let cwd = std::env::current_dir().map_err(|e| Error::new(e.to_string()))?;
        let global_exists = user_config::global_config_path()?.exists();
        let has_local = find_repo_root(&cwd)
            .map(|root| root.join("runes.kdl").exists())
            .unwrap_or(false);

        if !global_exists {
            writeln!(out, "GETTING STARTED")?;
            writeln!(out, "===============")?;
            writeln!(out)?;
            writeln!(out, "  Run `runes init` to set up runes. This will:")?;
            writeln!(out, "  - Create a global config at ~/.runes/config.kdl")?;
            writeln!(out, "  - Initialize a store backed by jj or pijul")?;
            writeln!(
                out,
                "  - Optionally create a local runes.kdl config for the current repo"
            )?;
            writeln!(out)?;
            writeln!(out, "  Example:")?;
            writeln!(
                out,
                "    runes init                     # interactive setup"
            )?;
            writeln!(
                out,
                "    runes init --project myapp     # non-interactive (needs RUNES_USER set)"
            )?;
            writeln!(out)?;
        } else {
            write_init_status(out, has_local, active_store)?;
        }
    }

    // -- Rune docs -- (neutral only: with a live config the status above already
    // names the store, and the commands below never need a path spelled out.)
    if !live {
        writeln!(out, "RUNE DOCS")?;
        writeln!(out, "=========")?;
        writeln!(out)?;
        writeln!(
            out,
            "  Rune docs are markdown files kept in a store outside the repo."
        )?;
        writeln!(
            out,
            "  `runes quickstart` reports this machine's store and schema, and"
        )?;
        writeln!(out, "  `runes show <id> --json` gives the path to one doc.")?;
        writeln!(out)?;
    }

    // -- Creating runes --
    writeln!(out, "CREATING RUNES")?;
    writeln!(out, "==============")?;
    writeln!(out)?;
    if for_agent {
        writeln!(
            out,
            "  Use `runes new` - it allocates the id and applies the project's current"
        )?;
        writeln!(out, "  template. Never create the doc file yourself.")?;
        writeln!(out)?;
        writeln!(
            out,
            "  Always pass `--json`: it answers with {{id, path, committed}}."
        )?;
        writeln!(out)?;
        writeln!(out, "  1) Create a draft, fill it in, commit it:")?;
        writeln!(out)?;
        writeln!(out, "       runes new \"Fix login bug\" --kind bug --json")?;
        writeln!(
            out,
            "       # edit the file at the path `runes new` printed"
        )?;
        writeln!(out, "       runes commit <id>")?;
        writeln!(out)?;
        writeln!(out, "  2) Create and commit a rune with no description:")?;
        writeln!(out)?;
        writeln!(
            out,
            "       runes new \"v2.0 release\" --kind milestone --commit --json"
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "  3) Create a rune from an existing markdown file (auto-commits):"
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "       runes new \"Fix flake\" -f notes.md --json   # --no-commit leaves it uncommitted"
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "  Also takes --status, --assignee, --label, --dep, --milestone, --parent."
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "  Without it, stdout is `Created <id>` - `Created <id> (uncommitted)` for a"
        )?;
        writeln!(out, "  draft - then the absolute path.")?;
    } else {
        writeln!(
            out,
            "  runes new \"Add auth\"                       # create an empty rune"
        )?;
        writeln!(
            out,
            "  runes new \"Add auth\" -e                    # draft it in $EDITOR"
        )?;
        writeln!(out, "  runes new \"Fix login bug\" --kind bug")?;
        writeln!(out, "  runes new \"v2.0 release\" --kind milestone")?;
        writeln!(
            out,
            "  runes new \"Fix flake\" -f notes.md          # create it from another markdown file"
        )?;
    }
    writeln!(out)?;

    // -- Viewing runes --
    writeln!(out, "VIEWING RUNES")?;
    writeln!(out, "=============")?;
    writeln!(out)?;
    writeln!(
        out,
        "  runes                         # list open runes (default view)"
    )?;
    writeln!(out, "  runes list                    # same as above")?;
    writeln!(
        out,
        "  runes list --status wip       # filter by status (matches wip:review too)"
    )?;
    writeln!(out, "  runes list --kind bug         # filter by kind")?;
    writeln!(out, "  runes show <id>               # show full rune doc")?;
    writeln!(
        out,
        "  runes search login            # full-text search, any status"
    )?;
    writeln!(out)?;
    writeln!(out, "  Built-in views:")?;
    for (name, description) in BUILTIN_VIEWS {
        writeln!(out, "    runes list {:<10}{}", name, description)?;
    }
    writeln!(
        out,
        "    runes list {:<10}same as `runes list all`",
        "--all"
    )?;
    writeln!(out)?;
    writeln!(out, "  Dependencies:")?;
    writeln!(
        out,
        "    runes list --ready            # no unresolved deps left"
    )?;
    writeln!(
        out,
        "    runes list --blocked          # waiting on an unresolved dep"
    )?;
    if for_agent {
        writeln!(
            out,
            "    runes list --blocked-by <id>  # runes waiting on <id>"
        )?;
        writeln!(
            out,
            "    runes list --blocks <id>      # runes <id> is waiting on"
        )?;
        writeln!(out)?;
        writeln!(out, "  Parse the output with --json:")?;
        writeln!(
            out,
            "    runes list --json             # array of rune summaries"
        )?;
        writeln!(
            out,
            "    runes search <term> --json    # array of matching runes"
        )?;
        writeln!(
            out,
            "    runes show <id> --json        # full rune, incl. store and doc path"
        )?;
        writeln!(
            out,
            "    runes log --json              # array of log entries"
        )?;
    }
    if !user_cfg.queries.is_empty() {
        let mut query_names: Vec<&str> = user_cfg.queries.keys().map(String::as_str).collect();
        query_names.sort();
        writeln!(out)?;
        writeln!(
            out,
            "  Custom views are deprecated while built-in views stabilize."
        )?;
        writeln!(
            out,
            "  Still defined in your config: {}",
            query_names.join(", ")
        )?;
    }
    writeln!(out)?;

    // -- Updating runes --
    writeln!(out, "UPDATING RUNES")?;
    writeln!(out, "==============")?;
    writeln!(out)?;
    if !for_agent {
        writeln!(
            out,
            "  runes edit <id>                            # open the rune in $EDITOR"
        )?;
        writeln!(
            out,
            "  runes comment <id>                         # write a comment in $EDITOR"
        )?;
        writeln!(out)?;
        writeln!(out, "  Or change one field without leaving the shell:")?;
        writeln!(out)?;
    }
    writeln!(
        out,
        "  runes edit <id> --status wip:review        # change status"
    )?;
    writeln!(
        out,
        "  runes edit <id> --status closed            # close it"
    )?;
    writeln!(
        out,
        "  runes edit <id> --title \"New title\"        # rename (updates the h1 line)"
    )?;
    writeln!(
        out,
        "  runes edit <id> --assignee alice           # reassign"
    )?;
    writeln!(
        out,
        "  runes edit <id> --label urgent             # add a label"
    )?;
    writeln!(
        out,
        "  runes edit <id> --remove-label urgent      # remove a label"
    )?;
    writeln!(
        out,
        "  runes edit <id> --milestone <mid>          # link to milestone"
    )?;
    writeln!(
        out,
        "  runes edit <id> --dep <id2>                # <id> waits on <id2>"
    )?;
    writeln!(
        out,
        "  runes edit <id> --remove-dep <id2>         # drop that dependency"
    )?;
    if for_agent {
        writeln!(
            out,
            "  runes comment <id> -m \"Looks good\"         # append under ## Comments"
        )?;
        writeln!(
            out,
            "  runes edit <id> -f doc.md                  # body (or full doc) from a file"
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "  Each of these commits on its own; --no-commit defers to `runes commit <id>`."
        )?;
        writeln!(
            out,
            "  Patching the file directly is equivalent: `runes diff <id>` to review,"
        )?;
        writeln!(out, "  `runes commit <id>` to record just that rune.")?;
    } else {
        writeln!(
            out,
            "  runes comment <id> -m \"Looks good\"         # comment without the editor"
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "  Each of these records the change as you go. If you edit a rune in the"
        )?;
        writeln!(
            out,
            "  store outside of runes, run `runes commit <id>` to record that change."
        )?;
    }
    writeln!(out)?;

    // -- Retiring runes -- (the agent guide covers both under OTHER COMMANDS)
    if !for_agent {
        writeln!(out, "DELETE / ARCHIVE")?;
        writeln!(out, "================")?;
        writeln!(out)?;
        writeln!(
            out,
            "  runes archive <id>                         # keep it, out of the way"
        )?;
        writeln!(
            out,
            "  runes delete <id>                          # remove it"
        )?;
        writeln!(out)?;
    }

    // -- Schema info --
    {
        writeln!(out, "SCHEMA")?;
        writeln!(out, "======")?;
        writeln!(out)?;

        // Neutral mode has no config to read, so this is the built-in default.
        let states = user_cfg.state_config().unwrap_or_default();
        writeln!(out, "  Status:")?;
        for core in state::CORE_STATES {
            writeln!(out, "    {}", states.allowed_display(core))?;
        }
        writeln!(out)?;

        if !live {
            writeln!(
                out,
                "  Statuses, kinds and custom fields are per-project: run `runes quickstart`"
            )?;
            writeln!(out, "  for the schema in effect here.")?;
            writeln!(out)?;
        }

        if let Some(ref schema) = schema {
            let kinds = schema.available_kinds();
            if !kinds.is_empty() {
                writeln!(out, "  Kinds: {}", kinds.join(", "))?;
            }
            // Show custom fields
            if !schema.fields.is_empty() {
                writeln!(out)?;
                writeln!(out, "  Custom fields:")?;
                let mut field_names: Vec<&String> = schema.fields.keys().collect();
                field_names.sort();
                for name in field_names {
                    let field = &schema.fields[name];
                    let mut desc = String::new();
                    if !field.values.is_empty() {
                        desc.push_str(&format!(" ({})", field.values.join(", ")));
                    }
                    if field.optional {
                        desc.push_str(" [optional]");
                    }
                    writeln!(out, "    {}{}", name, desc)?;
                }
            }
            writeln!(out)?;

            // Show kind templates with custom paths if any exist
            if let Some(store) = active_store {
                let has_custom_templates = kinds.iter().any(|kind| {
                    find_kind_template_path(&store.path, default_project, kind).is_some()
                });

                if has_custom_templates {
                    writeln!(out, "  Kind templates:")?;
                    for kind in &kinds {
                        if let Some(path) =
                            find_kind_template_path(&store.path, default_project, kind)
                        {
                            writeln!(out, "    {}: {}", kind, path.display())?;
                        } else {
                            writeln!(out, "    {}: (builtin default)", kind)?;
                        }
                    }
                    writeln!(out)?;
                }
            }
        }

        // Where to change what a new rune starts out as, rather than what the
        // built-in template happens to contain today.
        match active_store {
            Some(store) => {
                let kinds_dir = match default_project {
                    Some(proj) => store.path.join(proj).join(".kinds"),
                    None => store.path.join(".kinds"),
                };
                writeln!(out, "  Rune templates: {}/<kind>.md", kinds_dir.display())?;
            }
            None => {
                writeln!(
                    out,
                    "  Rune templates: <project>/.kinds/<kind>.md in the store"
                )?;
            }
        }
        writeln!(out)?;
    }

    // -- Other commands -- (agents only: the human guide stops at getting
    // going, and `runes --help` lists the rest)
    if for_agent {
        writeln!(out, "OTHER COMMANDS")?;
        writeln!(out, "==============")?;
        writeln!(out)?;
        writeln!(
            out,
            "  runes log                     # change history for the project"
        )?;
        writeln!(
            out,
            "  runes log <id>                # change history for a specific rune"
        )?;
        writeln!(
            out,
            "  runes diff <id>               # show uncommitted changes to a rune"
        )?;
        writeln!(
            out,
            "  runes commit [-m <msg>]       # commit everything uncommitted"
        )?;
        writeln!(out, "  runes archive <id>            # archive a rune")?;
        writeln!(out, "  runes delete <id>             # delete a rune")?;
        writeln!(
            out,
            "  runes move <id> --project p   # move rune to another project"
        )?;
        writeln!(
            out,
            "  runes restore <id> --revision r  # restore to a previous revision"
        )?;
        writeln!(
            out,
            "  runes sync                    # sync store with backend"
        )?;
        writeln!(out)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        collect_matching_entries, detect_agent, format_log_entries, match_log_entries,
        migrate_status_line, quickstart_audience, resolve_commit_author_env, strip_show_injections,
        truncate_to_width, Audience, LogEntry, QuickstartArgs, UserConfig,
    };
    use unicode_width::UnicodeWidthStr;

    /// Env lookup over a fixed list, returning values verbatim — detection has
    /// to cope with padding and blanks on its own, not lean on `system_env`.
    fn env_of<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            vars.iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    }

    const NO_FLAG: QuickstartArgs = QuickstartArgs {
        agent: false,
        human: false,
    };
    const AGENT_FLAG: QuickstartArgs = QuickstartArgs {
        agent: true,
        human: false,
    };
    const HUMAN_FLAG: QuickstartArgs = QuickstartArgs {
        agent: false,
        human: true,
    };

    #[test]
    fn quickstart_audience_follows_the_flag_then_the_environment() {
        let agent_env = env_of(&[("CLAUDECODE", "1")]);
        let plain_env = env_of(&[("TERM", "xterm-256color")]);

        assert_eq!(quickstart_audience(&NO_FLAG, &agent_env), Audience::Agent);
        assert_eq!(quickstart_audience(&NO_FLAG, &plain_env), Audience::Human);
        assert_eq!(
            quickstart_audience(&HUMAN_FLAG, &agent_env),
            Audience::Human
        );
        assert_eq!(
            quickstart_audience(&AGENT_FLAG, &plain_env),
            Audience::Agent
        );
    }

    fn cfg_with_email(email: Option<&str>) -> UserConfig {
        UserConfig {
            identity_email: email.map(str::to_string),
            ..UserConfig::default()
        }
    }

    #[test]
    fn migrate_status_line_touches_only_the_frontmatter_status() {
        let doc = "---\ntask \"proj-abc\" {\n  status \"in-progress\"\n}\n---\n\n# Done and done\n\nstatus \"done\" in the body\n";
        let migrated = migrate_status_line(doc).expect("migrated");
        assert_eq!(
            migrated,
            "---\ntask \"proj-abc\" {\n  status \"wip\"\n}\n---\n\n# Done and done\n\nstatus \"done\" in the body\n"
        );
    }

    #[test]
    fn migrate_status_line_leaves_core_states_alone() {
        let doc = "---\ntask \"proj-abc\" {\n  status \"closed:canceled\"\n}\n---\n\n# Title\n";
        assert_eq!(migrate_status_line(doc), None);
    }

    #[test]
    fn detect_agent_marker_vars() {
        let cases = [
            (vec![("CLAUDECODE", "1")], "claude"),
            (vec![("GEMINI_CLI", "1")], "gemini"),
            (vec![("CODEX_SANDBOX", "seatbelt")], "codex"),
            (vec![("CODEX_THREAD_ID", "abc123")], "codex"),
            (vec![("CURSOR_AGENT", "1")], "cursor"),
            (vec![("CURSOR_EXTENSION_HOST_ROLE", "agent-exec")], "cursor"),
            (vec![("AUGMENT_AGENT", "1")], "augment"),
            (vec![("OPENCODE", "1")], "opencode"),
            (vec![("OPENCODE_CLIENT", "cli")], "opencode"),
            (vec![("JUNIE_DATA", "/tmp/junie")], "junie"),
            (vec![("JUNIE_SHIM_PATH", "/tmp/shim")], "junie"),
            (vec![("CLINE_ACTIVE", "1")], "cline"),
        ];
        for (vars, expected) in cases {
            assert_eq!(
                detect_agent(&env_of(&vars)).as_deref(),
                Some(expected),
                "unexpected detection for {vars:?}"
            );
        }
    }

    #[test]
    fn detect_agent_marker_values_are_trimmed() {
        assert_eq!(
            detect_agent(&env_of(&[("CLAUDECODE", " 1\n")])).as_deref(),
            Some("claude")
        );
        assert_eq!(
            detect_agent(&env_of(&[("CURSOR_EXTENSION_HOST_ROLE", " agent-exec ")])).as_deref(),
            Some("cursor")
        );
    }

    #[test]
    fn detect_agent_generic_vars_use_their_value() {
        assert_eq!(
            detect_agent(&env_of(&[("AGENT", "goose")])).as_deref(),
            Some("goose")
        );
        assert_eq!(
            detect_agent(&env_of(&[("AI_AGENT", "amp")])).as_deref(),
            Some("amp")
        );
        assert_eq!(
            detect_agent(&env_of(&[("AGENT", "goose"), ("AI_AGENT", "amp")])).as_deref(),
            Some("amp")
        );
    }

    #[test]
    fn detect_agent_generic_values_drop_version_stamp() {
        // Claude Code exports AI_AGENT=claude-code_2-1-218_agent; keeping the
        // whole value would fragment history across releases.
        assert_eq!(
            detect_agent(&env_of(&[("AI_AGENT", "claude-code_2-1-218_agent")])).as_deref(),
            Some("claude-code")
        );
        assert_eq!(
            detect_agent(&env_of(&[("AGENT", " Goose_1-2-3 ")])).as_deref(),
            Some("goose")
        );
    }

    #[test]
    fn detect_agent_canonical_markers_beat_generic_vars() {
        // The live Claude Code env exports both; the exact marker must win so
        // commits land on claude@agents.localhost, not claude-code@...
        let env = env_of(&[
            ("AI_AGENT", "claude-code_2-1-218_agent"),
            ("CLAUDECODE", "1"),
        ]);
        assert_eq!(detect_agent(&env).as_deref(), Some("claude"));
    }

    #[test]
    fn detect_agent_runes_agent_overrides_all() {
        let env = env_of(&[
            ("RUNES_AGENT", " Scribe "),
            ("AGENT", "goose"),
            ("CLAUDECODE", "1"),
        ]);
        assert_eq!(detect_agent(&env).as_deref(), Some("scribe"));
    }

    #[test]
    fn detect_agent_rejects_unusable_slugs() {
        // Values that can't be a slug are ignored, never injected verbatim.
        for value in ["My Tool v2", "-leading", "a@b", "\"quoted\"", "  ", "_"] {
            assert_eq!(
                detect_agent(&env_of(&[("RUNES_AGENT", value)])),
                None,
                "RUNES_AGENT={value:?} should not detect"
            );
            assert_eq!(
                detect_agent(&env_of(&[("AI_AGENT", value)])),
                None,
                "AI_AGENT={value:?} should not detect"
            );
        }
        // ...and detection falls through to the next precedence level.
        let env = env_of(&[("RUNES_AGENT", "My Tool v2"), ("CLAUDECODE", "1")]);
        assert_eq!(detect_agent(&env).as_deref(), Some("claude"));
        let env = env_of(&[("AI_AGENT", "My Tool v2"), ("AGENT", "goose")]);
        assert_eq!(detect_agent(&env).as_deref(), Some("goose"));
    }

    #[test]
    fn detect_agent_ignores_terminal_and_blank_markers() {
        let env = env_of(&[
            ("TERM_PROGRAM", "vscode"),
            ("GIT_EDITOR", "code --wait"),
            ("COPILOT_MODEL", "gpt-5"),
            ("CLAUDECODE", ""),
            ("CURSOR_EXTENSION_HOST_ROLE", "ui"),
        ]);
        assert_eq!(detect_agent(&env), None);
    }

    #[test]
    fn author_explicit_signals_win_without_decoration() {
        let cfg = cfg_with_email(Some("anowell@gmail.com"));
        let env = env_of(&[("CLAUDECODE", "1"), ("RUNES_USER", "Bot <bot@example.com>")]);

        let (name, email) =
            resolve_commit_author_env(&cfg, Some("Ann <ann@example.com>"), &env).unwrap();
        assert_eq!((name.as_str(), email.as_str()), ("Ann", "ann@example.com"));

        let (name, email) = resolve_commit_author_env(&cfg, None, &env).unwrap();
        assert_eq!((name.as_str(), email.as_str()), ("Bot", "bot@example.com"));
    }

    #[test]
    fn author_detected_agent_acts_on_behalf_of_human() {
        let cfg = cfg_with_email(Some("anowell@gmail.com"));
        let (name, email) =
            resolve_commit_author_env(&cfg, None, &env_of(&[("CLAUDECODE", "1")])).unwrap();
        assert_eq!(name, "claude (on behalf of anowell@gmail.com)");
        assert_eq!(email, "claude@agents.localhost");
    }

    #[test]
    fn author_detected_agent_without_human_identity() {
        let (name, email) =
            resolve_commit_author_env(&cfg_with_email(None), None, &env_of(&[("CLAUDECODE", "1")]))
                .unwrap();
        assert_eq!(
            (name.as_str(), email.as_str()),
            ("claude", "claude@agents.localhost")
        );
    }

    #[test]
    fn author_skips_on_behalf_of_when_identity_is_the_agent() {
        let cfg = cfg_with_email(Some("claude@agents.localhost"));
        let (name, email) =
            resolve_commit_author_env(&cfg, None, &env_of(&[("CLAUDECODE", "1")])).unwrap();
        assert_eq!(
            (name.as_str(), email.as_str()),
            ("claude", "claude@agents.localhost")
        );
    }

    #[test]
    fn author_unusable_agent_slug_falls_back_to_config() {
        let cfg = cfg_with_email(Some("anowell@gmail.com"));
        let (name, email) =
            resolve_commit_author_env(&cfg, None, &env_of(&[("RUNES_AGENT", "My Tool v2")]))
                .unwrap();
        assert_eq!(
            (name.as_str(), email.as_str()),
            ("anowell@gmail.com", "anowell@gmail.com")
        );
    }

    #[test]
    fn author_detection_disabled_falls_back_to_config() {
        let cfg = UserConfig {
            identity_email: Some("anowell@gmail.com".to_string()),
            identity_name: Some("Anthony".to_string()),
            attribution_detect: Some(false),
            ..UserConfig::default()
        };
        let (name, email) =
            resolve_commit_author_env(&cfg, None, &env_of(&[("CLAUDECODE", "1")])).unwrap();
        assert_eq!(
            (name.as_str(), email.as_str()),
            ("Anthony", "anowell@gmail.com")
        );
    }

    #[test]
    fn author_errors_without_any_identity() {
        let err = resolve_commit_author_env(&cfg_with_email(None), None, &env_of(&[])).unwrap_err();
        assert!(
            err.to_string().contains("No author configured"),
            "unexpected error: {err}"
        );
    }

    fn log_entry(revision: &str, files: &[&str]) -> LogEntry {
        LogEntry {
            revision: revision.to_string(),
            timestamp: 1_700_000_000,
            author: "test@runes.dev".to_string(),
            description: "some change".to_string(),
            changed_files: files.iter().map(|f| f.to_string()).collect(),
        }
    }

    #[test]
    fn log_limit_counts_commits_not_rows() {
        // One commit touching 3 runes counts once against --limit, and still
        // prints all 3 rows - the same commit JSON would emit as one entry.
        let entries = vec![
            log_entry(
                "aaa",
                &["proj/aa--one.md", "proj/bb--two.md", "proj/cc--three.md"],
            ),
            log_entry("bbb", &["proj/dd--four.md"]),
        ];
        let matched = match_log_entries(&entries, None, Some("proj"), None, 2);
        assert_eq!(matched.len(), 2, "expected 2 commits");
        assert_eq!(matched[0].rune_ids.len(), 3, "expected 3 runes on commit 1");
        let out = format_log_entries(&matched, None, Some("proj"));
        assert_eq!(out.lines().count(), 4, "expected 4 rows: {out}");
    }

    #[test]
    fn log_limit_truncates_at_commit_boundary() {
        let entries = vec![
            log_entry("aaa", &["proj/aa--one.md", "proj/bb--two.md"]),
            log_entry("bbb", &["proj/cc--three.md"]),
        ];
        let matched = match_log_entries(&entries, None, Some("proj"), None, 1);
        assert_eq!(matched.len(), 1, "limit 1 keeps only the newest commit");
        let out = format_log_entries(&matched, None, Some("proj"));
        assert_eq!(out.lines().count(), 2, "both rune rows survive: {out}");
        assert!(!out.contains("proj-cc"), "second commit leaked: {out}");
    }

    #[test]
    fn log_filter_reaches_entries_beyond_limit() {
        let mut entries: Vec<LogEntry> = (0..10)
            .map(|i| log_entry(&format!("rev{i}"), &["other/zz--noise.md"]))
            .collect();
        entries.push(log_entry("target", &["proj/aa--one.md"]));
        let matched = match_log_entries(&entries, Some("proj-aa"), None, None, 5);
        let out = format_log_entries(&matched, Some("proj-aa"), None);
        assert_eq!(out.lines().count(), 1, "expected 1 row: {out}");
        assert!(out.contains("proj-aa"), "missing matched rune: {out}");
    }

    #[test]
    fn log_rune_filter_hides_sibling_rune_rows() {
        // A commit touching two runes shows only the filtered one in text mode.
        let entries = vec![log_entry("aaa", &["proj/aa--one.md", "proj/bb--two.md"])];
        let matched = match_log_entries(&entries, Some("proj-aa"), None, None, 5);
        assert_eq!(matched.len(), 1);
        let out = format_log_entries(&matched, Some("proj-aa"), None);
        assert_eq!(out.lines().count(), 1, "expected 1 row: {out}");
        assert!(!out.contains("proj-bb"), "sibling rune leaked: {out}");
    }

    /// Fake history: `total` commits, every `nth` of them touching proj-aa.
    fn fake_history(total: usize, nth: usize) -> Vec<LogEntry> {
        (0..total)
            .map(|i| {
                let files: &[&str] = if i % nth == 0 {
                    &["proj/aa--one.md"]
                } else {
                    &["other/zz--noise.md"]
                };
                log_entry(&format!("rev{i}"), files)
            })
            .collect()
    }

    #[test]
    fn walk_stops_early_when_matches_are_dense() {
        let history = fake_history(10_000, 1);
        let mut walked = Vec::new();
        let matched = collect_matching_entries(
            |n| {
                walked.push(n);
                Ok(history.iter().take(n).cloned().collect())
            },
            Some("proj-aa"),
            None,
            None,
            5,
        )
        .expect("collect");
        assert_eq!(matched.len(), 5);
        assert_eq!(walked, vec![5], "should not walk past the first batch");
    }

    #[test]
    fn walk_grows_until_enough_matches() {
        // Only every 10th commit matches, so 5 matches need ~50 commits walked.
        let history = fake_history(10_000, 10);
        let mut walked = Vec::new();
        let matched = collect_matching_entries(
            |n| {
                walked.push(n);
                Ok(history.iter().take(n).cloned().collect())
            },
            Some("proj-aa"),
            None,
            None,
            5,
        )
        .expect("collect");
        assert_eq!(matched.len(), 5);
        let deepest = *walked.last().expect("at least one walk");
        assert!(
            deepest >= 41,
            "walk too shallow to find 5 matches: {walked:?}"
        );
        assert!(
            deepest < 10_000,
            "walk should stop well short of full history: {walked:?}"
        );
    }

    #[test]
    fn walk_stops_at_history_root_when_matches_are_scarce() {
        // Nothing matches: the walk must terminate at the root, not loop forever.
        let history = fake_history(100, 1000);
        let mut walked = Vec::new();
        let matched = collect_matching_entries(
            |n| {
                walked.push(n);
                Ok(history.iter().take(n).cloned().collect())
            },
            Some("proj-nope"),
            None,
            None,
            5,
        )
        .expect("collect");
        assert!(matched.is_empty(), "nothing should match");
        assert!(
            *walked.last().expect("at least one walk") >= 100,
            "should have reached the root: {walked:?}"
        );
    }

    #[test]
    fn truncate_noop_when_within_width() {
        assert_eq!(truncate_to_width("short title", 20), "short title");
        assert_eq!(truncate_to_width("exact", 5), "exact");
    }

    #[test]
    fn truncate_long_ascii() {
        let out = truncate_to_width("a very long title that overflows", 12);
        assert_eq!(out, "a very lo...");
        assert_eq!(out.width(), 12);
    }

    #[test]
    fn truncate_wide_chars_stays_within_width() {
        // CJK chars are 2 columns wide; truncation must count display width,
        // not chars or bytes, and must not split a wide char.
        let out = truncate_to_width("日本語のタイトルです", 10);
        assert!(out.ends_with("..."));
        assert!(out.width() <= 10);
    }

    #[test]
    fn strip_show_injections_removes_decorations() {
        let shown = concat!(
            "# Rune\n\n",
            "## Description\n",
            "Edited by Test User on Jul 24 at 3:40pm\n\n",
            "Body.\n\n",
            "## Notes\n",
            "pending uncommitted changes\n\n",
            "## Comments\n",
            "On Jul 24 at 3:40pm by Test User\n\n",
            "first\n",
            "---\n",
            "<not committed>\n\n",
            "second\n",
            "deps:\n",
            "  proj-abc (todo)\n",
            "child_total=1 child_done=0 child_in_progress=0 child_todo=1 complete_pct=0.0\n",
            "children:\n",
            "  proj-abc (todo)\n",
        );
        assert_eq!(
            strip_show_injections(shown),
            concat!(
                "# Rune\n\n",
                "## Description\n\n",
                "Body.\n\n",
                "## Notes\n\n",
                "## Comments\n\n",
                "first\n\n",
                "---\n\n",
                "second\n",
            )
        );
    }

    #[test]
    fn strip_show_injections_keeps_lookalike_body_text() {
        // Same words, wrong shape or wrong position — all of it is real body text
        let body = concat!(
            "# Rune\n\n",
            "## Description\n\n",
            "Edited by Test User on Tuesday\n",
            "Edited by Test User on Jul 24 at 3:40pm\n\n",
            "On Jul 24 at 3:40pm by Test User\n\n",
            "```\n",
            "## Fenced\n",
            "Edited by Test User on Jul 24 at 3:40pm\n",
            "```\n\n",
            "deps:\n",
            "  proj-abc (todo)\n\n",
            "Trailing prose keeps the deps list in the body.\n",
        );
        assert_eq!(strip_show_injections(body), body);
    }
}
