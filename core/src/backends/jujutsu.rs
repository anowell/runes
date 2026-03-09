use crate::config::Store;
use crate::{Error, Result};
use jj_lib::backend::{Signature, Timestamp as JjTimestamp};
use jj_lib::config::StackedConfig;
use jj_lib::conflicts::{materialize_tree_value, MaterializedTreeValue};
use jj_lib::git::{
    expand_fetch_refspecs, get_all_remote_names, get_git_repo, load_default_fetch_bookmarks,
    push_branches, GitBranchPushTargets, GitFetch, GitFetchRefExpression, GitImportOptions,
    GitSettings, GitSidebandLineTerminator, GitSubprocessCallback,
};
use jj_lib::gitignore::GitIgnoreFile;
use jj_lib::matchers::EverythingMatcher;
use jj_lib::merged_tree::{MergedTree, TreeDiffIterator};
use jj_lib::object_id::{HexPrefix, ObjectId, PrefixResolution};
use jj_lib::refs::{classify_bookmark_push_action, BookmarkPushAction, LocalAndRemoteRef};
use jj_lib::repo::Repo as _;
use jj_lib::repo::StoreFactories;
use jj_lib::repo_path::RepoPathBuf;
use jj_lib::settings::UserSettings;
use jj_lib::str_util::{StringExpression, StringMatcher};
use jj_lib::working_copy::SnapshotOptions;
use jj_lib::workspace::default_working_copy_factories;
use jj_lib::workspace::Workspace;
use pollster::FutureExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};

pub(super) fn probe_jj_workspace(store: &Store) -> Result<(PathBuf, PathBuf)> {
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let workspace = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?;
    Ok((
        workspace.workspace_root().to_path_buf(),
        workspace.repo_path().to_path_buf(),
    ))
}

pub(super) fn jj_sdk_has_uncommitted_changes(store: &Store) -> Result<bool> {
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let workspace = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo load failed: {e}")))?;
    if let Some(wc_commit_id) = repo.view().get_wc_commit_id(workspace.workspace_name()) {
        let commit = repo
            .store()
            .get_commit(wc_commit_id)
            .map_err(|e| Error::new(format!("jj-lib wc commit load failed: {e}")))?;
        let wc_tree = workspace
            .working_copy()
            .tree()
            .map_err(|e| Error::new(format!("jj-lib working copy state failed: {e}")))?;
        Ok(wc_tree.tree_ids_and_labels() != commit.tree().tree_ids_and_labels())
    } else {
        Ok(false)
    }
}

pub(super) fn jj_sdk_uncommitted_rune_paths(store: &Store) -> Result<Vec<PathBuf>> {
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let workspace = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo load failed: {e}")))?;
    let wc_commit_id = match repo.view().get_wc_commit_id(workspace.workspace_name()) {
        Some(id) => id.clone(),
        None => return Ok(Vec::new()),
    };
    let commit = repo
        .store()
        .get_commit(&wc_commit_id)
        .map_err(|e| Error::new(format!("jj-lib wc commit load failed: {e}")))?;
    let wc_tree = workspace
        .working_copy()
        .tree()
        .map_err(|e| Error::new(format!("jj-lib working copy state failed: {e}")))?;
    let commit_tree = commit.tree();

    if wc_tree.tree_ids_and_labels() == commit_tree.tree_ids_and_labels() {
        return Ok(Vec::new());
    }

    let mut paths: Vec<PathBuf> = TreeDiffIterator::new(&commit_tree, &wc_tree, &EverythingMatcher)
        .filter_map(|entry| {
            let path_str = entry.path.as_internal_file_string();
            if path_str.ends_with(".md") {
                Some(PathBuf::from(path_str))
            } else {
                None
            }
        })
        .collect();
    paths.sort();
    Ok(paths)
}

pub(super) fn jj_sdk_status(store: &Store) -> Result<String> {
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let workspace = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo load failed: {e}")))?;
    let workspace_name = workspace.workspace_name().as_str();
    let mut lines = vec![format!("workspace = \"{workspace_name}\"")];

    // Count total changes by walking the commit graph
    let mut queue: VecDeque<_> = repo.view().heads().iter().cloned().collect();
    let mut seen = HashSet::new();
    let mut latest_non_empty: Option<jj_lib::commit::Commit> = None;
    while let Some(commit_id) = queue.pop_front() {
        if !seen.insert(commit_id.clone()) {
            continue;
        }
        let commit = repo
            .store()
            .get_commit(&commit_id)
            .map_err(|e| Error::new(format!("jj-lib commit load failed: {e}")))?;
        // Track the first non-empty commit as latest
        if latest_non_empty.is_none() && !commit.description().trim().is_empty() {
            latest_non_empty = Some(commit.clone());
        }
        for parent_id in commit.parent_ids() {
            if !seen.contains(parent_id) {
                queue.push_back(parent_id.clone());
            }
        }
    }
    // Subtract 1 for the root commit
    let changes = seen.len().saturating_sub(1);
    lines.push(format!("changes = {changes}"));

    if let Some(latest) = &latest_non_empty {
        lines.push(format!("latest_change = \"{}\"", latest.change_id().reverse_hex()));
        lines.push(format!("latest_commit = \"{}\"", latest.id().hex()));
    }

    // List remotes
    let remotes = get_all_remote_names(repo.store())
        .unwrap_or_default();
    if !remotes.is_empty() {
        let remote_strs: Vec<&str> = remotes.iter().map(|r| r.as_ref()).collect();
        lines.push(format!("remotes = [{}]", remote_strs.iter().map(|r| format!("\"{r}\"")).collect::<Vec<_>>().join(", ")));
    }
    Ok(lines.join("\n") + "\n")
}

pub(super) fn jj_sdk_log(store: &Store, limit: usize) -> Result<String> {
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let workspace = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo load failed: {e}")))?;
    let mut queue: VecDeque<_> = repo.view().heads().iter().cloned().collect();
    let mut seen = HashSet::new();
    let mut lines = Vec::new();
    while let Some(commit_id) = queue.pop_front() {
        if lines.len() >= limit {
            break;
        }
        if !seen.insert(commit_id.clone()) {
            continue;
        }
        let commit = repo
            .store()
            .get_commit(&commit_id)
            .map_err(|e| Error::new(format!("jj-lib commit load failed: {e}")))?;
        let desc = commit
            .description()
            .lines()
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        let summary = if desc.is_empty() {
            "(no description)"
        } else {
            &desc
        };
        lines.push(format!("{} {}", commit_id.hex(), summary));
        for parent_id in commit.parent_ids() {
            if !seen.contains(parent_id) {
                queue.push_back(parent_id.clone());
            }
        }
    }
    Ok(lines.join("\n") + "\n")
}

pub(super) fn jj_sdk_rich_log(store: &Store, limit: usize) -> Result<Vec<super::LogEntry>> {
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let workspace = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo load failed: {e}")))?;
    let mut queue: VecDeque<_> = repo.view().heads().iter().cloned().collect();
    let mut seen = HashSet::new();
    let mut entries = Vec::new();
    while let Some(commit_id) = queue.pop_front() {
        if entries.len() >= limit {
            break;
        }
        if !seen.insert(commit_id.clone()) {
            continue;
        }
        let commit = repo
            .store()
            .get_commit(&commit_id)
            .map_err(|e| Error::new(format!("jj-lib commit load failed: {e}")))?;
        let desc = commit.description().trim().to_string();
        if desc.is_empty() && commit.parent_ids().len() <= 1 {
            // Skip empty working copy changes
            for parent_id in commit.parent_ids() {
                if !seen.contains(parent_id) {
                    queue.push_back(parent_id.clone());
                }
            }
            continue;
        }
        let author = commit.author();
        let author_name = if author.email.is_empty() {
            author.name.clone()
        } else {
            author.email.clone()
        };
        let timestamp = author.timestamp.timestamp.0 / 1000;
        let changed_files = match repo
            .index()
            .changed_paths_in_commit(&commit_id)
            .map_err(|e| Error::new(format!("jj-lib changed-path query failed: {e}")))?
        {
            Some(paths) => paths.map(|p| p.as_internal_file_string().to_string()).collect(),
            None => {
                // Fallback: diff commit tree against parent tree
                let parent_tree = commit.parent_tree(repo.as_ref())
                    .map_err(|e| Error::new(format!("jj-lib parent tree failed: {e}")))?;
                let commit_tree = commit.tree();
                TreeDiffIterator::new(&parent_tree, &commit_tree, &EverythingMatcher)
                    .filter_map(|entry| {
                        let path_str = entry.path.as_internal_file_string().to_string();
                        if path_str.ends_with(".md") { Some(path_str) } else { None }
                    })
                    .collect()
            }
        };
        entries.push(super::LogEntry {
            revision: commit_id.hex(),
            timestamp,
            author: author_name,
            description: desc,
            changed_files,
        });
        for parent_id in commit.parent_ids() {
            if !seen.contains(parent_id) {
                queue.push_back(parent_id.clone());
            }
        }
    }
    Ok(entries)
}

pub(super) fn jj_sdk_file_at_revision(
    store: &Store,
    rel_path: &Path,
    revision: &str,
) -> Result<String> {
    let path_raw = rel_path.to_string_lossy().replace('\\', "/");
    let repo_path = RepoPathBuf::from_internal_string(path_raw.clone())
        .map_err(|_| Error::new(format!("invalid repo-relative path for jj: {path_raw}")))?;
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let workspace = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo load failed: {e}")))?;
    let resolved = jj_resolve_commit_id(repo.as_ref(), revision)?;
    let commit = repo
        .store()
        .get_commit(&resolved)
        .map_err(|e| Error::new(format!("jj-lib commit load failed: {e}")))?;
    let tree = commit.tree();
    match jj_materialize_file(repo.store().as_ref(), &tree, repo_path.as_ref())? {
        Some(contents) => Ok(contents),
        None => Err(Error::new(format!(
            "file '{}' not found at revision {}",
            path_raw,
            &revision[..revision.len().min(12)]
        ))),
    }
}

pub(super) fn jj_sdk_file_before_revision(
    store: &Store,
    rel_path: &Path,
    revision: &str,
) -> Result<String> {
    let path_raw = rel_path.to_string_lossy().replace('\\', "/");
    let repo_path = RepoPathBuf::from_internal_string(path_raw.clone())
        .map_err(|_| Error::new(format!("invalid repo-relative path for jj: {path_raw}")))?;
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let workspace = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo load failed: {e}")))?;
    let resolved = jj_resolve_commit_id(repo.as_ref(), revision)?;
    let commit = repo
        .store()
        .get_commit(&resolved)
        .map_err(|e| Error::new(format!("jj-lib commit load failed: {e}")))?;
    let parent_tree = commit
        .parent_tree(repo.as_ref())
        .map_err(|e| Error::new(format!("jj-lib parent tree failed: {e}")))?;
    match jj_materialize_file(repo.store().as_ref(), &parent_tree, repo_path.as_ref())? {
        Some(contents) => Ok(contents),
        None => Ok(String::new()), // File didn't exist before this revision
    }
}

fn jj_collect_commits_for_path(
    store: &Store,
    rel_path: &Path,
    limit: usize,
) -> Result<Vec<(String, String)>> {
    let path_raw = rel_path.to_string_lossy().replace('\\', "/");
    let target = RepoPathBuf::from_internal_string(path_raw.clone())
        .map_err(|_| Error::new(format!("invalid repo-relative path for jj: {path_raw}")))?;
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let workspace = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo load failed: {e}")))?;
    let mut queue: VecDeque<_> = repo.view().heads().iter().cloned().collect();
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    while let Some(commit_id) = queue.pop_front() {
        if out.len() >= limit {
            break;
        }
        if !seen.insert(commit_id.clone()) {
            continue;
        }
        let mut changed = false;
        if let Some(paths) = repo
            .index()
            .changed_paths_in_commit(&commit_id)
            .map_err(|e| Error::new(format!("jj-lib changed-path query failed: {e}")))?
        {
            for p in paths {
                if p == target || p.starts_with(target.as_ref()) || target.starts_with(p.as_ref()) {
                    changed = true;
                    break;
                }
            }
        }
        let commit = repo
            .store()
            .get_commit(&commit_id)
            .map_err(|e| Error::new(format!("jj-lib commit load failed: {e}")))?;
        if changed {
            let desc = commit
                .description()
                .lines()
                .next()
                .unwrap_or("")
                .trim()
                .to_string();
            out.push((commit_id.hex(), desc));
        }
        for parent_id in commit.parent_ids() {
            if !seen.contains(parent_id) {
                queue.push_back(parent_id.clone());
            }
        }
    }
    Ok(out)
}

pub(super) fn jj_sdk_file_log(store: &Store, rel_path: &Path, limit: usize) -> Result<String> {
    let rows = jj_collect_commits_for_path(store, rel_path, limit)?;
    let mut lines = Vec::with_capacity(rows.len());
    for (id, desc) in rows {
        let summary = if desc.is_empty() {
            "(no description)"
        } else {
            desc.as_str()
        };
        lines.push(format!("{id} {summary}"));
    }
    Ok(lines.join("\n") + "\n")
}

pub(super) fn jj_sdk_file_change_ids(
    store: &Store,
    rel_path: &Path,
    limit: usize,
) -> Result<Vec<String>> {
    let rows = jj_collect_commits_for_path(store, rel_path, limit)?;
    Ok(rows.into_iter().map(|(id, _)| id).collect())
}

pub(super) fn jj_sdk_file_rich_log(
    store: &Store,
    rel_path: &Path,
    limit: usize,
) -> Result<Vec<super::LogEntry>> {
    let path_raw = rel_path.to_string_lossy().replace('\\', "/");
    let target = RepoPathBuf::from_internal_string(path_raw.clone())
        .map_err(|_| Error::new(format!("invalid repo-relative path for jj: {path_raw}")))?;
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let workspace = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo load failed: {e}")))?;
    let mut queue: VecDeque<_> = repo.view().heads().iter().cloned().collect();
    let mut seen = HashSet::new();
    let mut entries = Vec::new();
    while let Some(commit_id) = queue.pop_front() {
        if entries.len() >= limit {
            break;
        }
        if !seen.insert(commit_id.clone()) {
            continue;
        }
        let commit = repo
            .store()
            .get_commit(&commit_id)
            .map_err(|e| Error::new(format!("jj-lib commit load failed: {e}")))?;
        let (changed, all_changed_files) = match repo
            .index()
            .changed_paths_in_commit(&commit_id)
            .map_err(|e| Error::new(format!("jj-lib changed-path query failed: {e}")))?
        {
            Some(paths) => {
                let mut found = false;
                let mut files = Vec::new();
                for p in paths {
                    let s = p.as_internal_file_string().to_string();
                    if p == target || p.starts_with(target.as_ref()) || target.starts_with(p.as_ref()) {
                        found = true;
                    }
                    if s.ends_with(".md") {
                        files.push(s);
                    }
                }
                (found, files)
            }
            None => {
                // Fallback: diff commit tree against parent tree
                let parent_tree = commit.parent_tree(repo.as_ref())
                    .map_err(|e| Error::new(format!("jj-lib parent tree failed: {e}")))?;
                let commit_tree = commit.tree();
                let mut found = false;
                let mut files = Vec::new();
                for entry in TreeDiffIterator::new(&parent_tree, &commit_tree, &EverythingMatcher) {
                    let s = entry.path.as_internal_file_string().to_string();
                    if s == path_raw || s.starts_with(&format!("{path_raw}/")) {
                        found = true;
                    }
                    if s.ends_with(".md") {
                        files.push(s);
                    }
                }
                (found, files)
            }
        };
        if changed {
            let desc = commit.description().trim().to_string();
            let author = commit.author();
            let author_name = if author.name.is_empty() {
                author.email.clone()
            } else {
                author.name.clone()
            };
            let timestamp = author.timestamp.timestamp.0 / 1000;
            entries.push(super::LogEntry {
                revision: commit_id.hex(),
                timestamp,
                author: author_name,
                description: desc,
                changed_files: all_changed_files,
            });
        }
        for parent_id in commit.parent_ids() {
            if !seen.contains(parent_id) {
                queue.push_back(parent_id.clone());
            }
        }
    }
    Ok(entries)
}

pub(super) fn jj_sdk_commit_paths(store: &Store, message: &str, author_name: &str, author_email: &str) -> Result<()> {
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let mut workspace = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo load failed: {e}")))?;

    let wc_commit_id = repo
        .view()
        .get_wc_commit_id(workspace.workspace_name())
        .ok_or_else(|| Error::new("jj-lib workspace has no working-copy commit"))?
        .clone();
    let wc_commit = repo
        .store()
        .get_commit(&wc_commit_id)
        .map_err(|e| Error::new(format!("jj-lib wc commit load failed: {e}")))?;
    let workspace_name = workspace.workspace_name().to_owned();

    let mut locked_ws = workspace
        .start_working_copy_mutation()
        .map_err(|e| Error::new(format!("jj-lib working-copy lock failed: {e}")))?;
    let everything = EverythingMatcher;
    let snapshot_options = SnapshotOptions {
        base_ignores: GitIgnoreFile::empty(),
        progress: None,
        start_tracking_matcher: &everything,
        force_tracking_matcher: &everything,
        max_new_file_size: u64::MAX,
    };
    let (new_tree, _stats): (MergedTree, _) = locked_ws
        .locked_wc()
        .snapshot(&snapshot_options)
        .block_on()
        .map_err(|e| Error::new(format!("jj-lib snapshot failed: {e}")))?;

    if new_tree.tree_ids_and_labels() == wc_commit.tree().tree_ids_and_labels() {
        locked_ws
            .finish(repo.op_id().clone())
            .map_err(|e| Error::new(format!("jj-lib working-copy finalize failed: {e}")))?;
        return Ok(());
    }

    let mut tx = repo.start_transaction();
    let mut_repo = tx.repo_mut();
    let author_sig = Signature {
        name: author_name.to_string(),
        email: author_email.to_string(),
        timestamp: JjTimestamp::now(),
    };
    let committed = mut_repo
        .rewrite_commit(&wc_commit)
        .set_tree(new_tree)
        .set_description(message)
        .set_author(author_sig)
        .write()
        .map_err(|e| Error::new(format!("jj-lib rewrite commit failed: {e}")))?;
    mut_repo
        .rebase_descendants()
        .map_err(|e| Error::new(format!("jj-lib rebase descendants failed: {e}")))?;
    let new_wc_commit = mut_repo
        .check_out(workspace_name, &committed)
        .map_err(|e| Error::new(format!("jj-lib checkout commit failed: {e}")))?;
    let repo = tx
        .commit(message)
        .map_err(|e| Error::new(format!("jj-lib transaction commit failed: {e}")))?;

    locked_ws
        .locked_wc()
        .check_out(&new_wc_commit)
        .block_on()
        .map_err(|e| Error::new(format!("jj-lib working-copy checkout failed: {e}")))?;
    locked_ws
        .finish(repo.op_id().clone())
        .map_err(|e| Error::new(format!("jj-lib working-copy finalize failed: {e}")))?;
    Ok(())
}

#[derive(Debug, Default)]
struct SilentGitCallback;

impl GitSubprocessCallback for SilentGitCallback {
    fn needs_progress(&self) -> bool {
        false
    }

    fn progress(&mut self, _progress: &jj_lib::git::GitProgress) -> io::Result<()> {
        Ok(())
    }

    fn local_sideband(
        &mut self,
        _message: &[u8],
        _term: Option<GitSidebandLineTerminator>,
    ) -> io::Result<()> {
        Ok(())
    }

    fn remote_sideband(
        &mut self,
        _message: &[u8],
        _term: Option<GitSidebandLineTerminator>,
    ) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn jj_sdk_sync(store: &Store) -> Result<()> {
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let repo = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo load failed: {e}")))?;

    let remotes = get_all_remote_names(repo.store())
        .map_err(|e| Error::new(format!("jj-lib remote list failed: {e}")))?;
    if remotes.is_empty() {
        return Ok(());
    }

    let git_repo = get_git_repo(repo.store())
        .map_err(|e| Error::new(format!("jj-lib git repo failed: {e}")))?;
    let git_settings = GitSettings::from_settings(&settings)
        .map_err(|e| Error::new(format!("git settings failed: {e}")))?;
    let import_options = GitImportOptions {
        auto_local_bookmark: git_settings.auto_local_bookmark,
        abandon_unreachable_commits: git_settings.abandon_unreachable_commits,
        remote_auto_track_bookmarks: HashMap::<_, StringMatcher>::new(),
    };

    {
        let mut tx = repo.start_transaction();
        let mut callback = SilentGitCallback;
        let mut fetch = GitFetch::new(
            tx.repo_mut(),
            git_settings.to_subprocess_options(),
            &import_options,
        )
        .map_err(|e| Error::new(format!("jj-lib git fetch init failed: {e}")))?;
        for remote in &remotes {
            let (_ignored, bookmark_expr) =
                load_default_fetch_bookmarks(remote.as_ref(), &git_repo)
                    .map_err(|e| Error::new(format!("jj-lib load fetch refs failed: {e}")))?;
            let fetch_expr = GitFetchRefExpression {
                bookmark: bookmark_expr,
                tag: StringExpression::none(),
            };
            let expanded = expand_fetch_refspecs(remote.as_ref(), fetch_expr)
                .map_err(|e| Error::new(format!("jj-lib expand fetch refs failed: {e}")))?;
            fetch
                .fetch(remote.as_ref(), expanded, &mut callback, None, None)
                .map_err(|e| Error::new(format!("jj-lib fetch failed: {e}")))?;
        }
        fetch
            .import_refs()
            .map_err(|e| Error::new(format!("jj-lib import refs failed: {e}")))?;
        let _ = tx
            .commit("runes sync fetch")
            .map_err(|e| Error::new(format!("jj-lib fetch transaction failed: {e}")))?;
    }

    let repo = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace reload failed: {e}")))?
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo reload failed: {e}")))?;
    let mut tx = repo.start_transaction();
    let mut callback = SilentGitCallback;
    for remote in &remotes {
        let updates = {
            let view = tx.repo().view();
            view.local_bookmarks()
                .filter_map(|(name, local_target)| {
                    let remote_ref =
                        view.get_remote_bookmark(name.to_remote_symbol(remote.as_ref()));
                    match classify_bookmark_push_action(LocalAndRemoteRef {
                        local_target,
                        remote_ref,
                    }) {
                        BookmarkPushAction::Update(update) => Some((name.to_owned(), update)),
                        _ => None,
                    }
                })
                .collect::<Vec<_>>()
        };
        if updates.is_empty() {
            continue;
        }
        let targets = GitBranchPushTargets {
            branch_updates: updates,
        };
        let _stats = push_branches(
            tx.repo_mut(),
            git_settings.to_subprocess_options(),
            remote.as_ref(),
            &targets,
            &mut callback,
        )
        .map_err(|e| Error::new(format!("jj-lib push failed: {e}")))?;
    }
    let _ = tx
        .commit("runes sync push")
        .map_err(|e| Error::new(format!("jj-lib push transaction failed: {e}")))?;
    Ok(())
}

fn jj_resolve_commit_id(
    repo: &dyn jj_lib::repo::Repo,
    change_id: &str,
) -> Result<jj_lib::backend::CommitId> {
    let prefix = HexPrefix::try_from_hex(change_id)
        .ok_or_else(|| Error::new(format!("invalid jj change id: {change_id}")))?;
    match repo
        .index()
        .resolve_commit_id_prefix(&prefix)
        .map_err(|e| Error::new(format!("jj-lib resolve commit id failed: {e}")))?
    {
        PrefixResolution::NoMatch => Err(Error::new(format!("commit not found: {change_id}"))),
        PrefixResolution::AmbiguousMatch => Err(Error::new(format!(
            "ambiguous commit id prefix: {change_id}"
        ))),
        PrefixResolution::SingleMatch(id) => Ok(id),
    }
}

fn jj_materialize_file(
    store: &jj_lib::store::Store,
    tree: &jj_lib::merged_tree::MergedTree,
    repo_path: &jj_lib::repo_path::RepoPath,
) -> Result<Option<String>> {
    let value = tree
        .path_value(repo_path)
        .map_err(|e| Error::new(format!("jj-lib path value failed: {e}")))?;
    let materialized = materialize_tree_value(store, repo_path, value, tree.labels())
        .block_on()
        .map_err(|e| Error::new(format!("jj-lib materialize tree value failed: {e}")))?;
    match materialized {
        MaterializedTreeValue::Absent => Ok(None),
        MaterializedTreeValue::File(mut f) => {
            let bytes = f
                .read_all(repo_path)
                .block_on()
                .map_err(|e| Error::new(format!("jj-lib read file failed: {e}")))?;
            Ok(Some(String::from_utf8_lossy(&bytes).to_string()))
        }
        MaterializedTreeValue::Symlink { target, .. } => Ok(Some(target)),
        MaterializedTreeValue::FileConflict(file) => {
            let first = file.contents.into_iter().next().unwrap_or_default();
            Ok(Some(String::from_utf8_lossy(first.as_ref()).to_string()))
        }
        MaterializedTreeValue::OtherConflict { .. } => Ok(Some("(conflict)".to_string())),
        MaterializedTreeValue::GitSubmodule(id) => {
            Ok(Some(format!("(git-submodule:{})", id.hex())))
        }
        MaterializedTreeValue::Tree(_) => Ok(Some("(tree)".to_string())),
        MaterializedTreeValue::AccessDenied(_) => Ok(Some("(access-denied)".to_string())),
    }
}

fn jj_simple_unified_diff(path: &str, before: Option<&str>, after: Option<&str>) -> String {
    let mut out = String::new();
    out.push_str(&format!("diff --git a/{0} b/{0}\n", path));
    out.push_str(&format!("--- a/{path}\n"));
    out.push_str(&format!("+++ b/{path}\n"));
    if before == after {
        return out;
    }
    if let Some(b) = before {
        for line in b.lines() {
            out.push('-');
            out.push_str(line);
            out.push('\n');
        }
    }
    if let Some(a) = after {
        for line in a.lines() {
            out.push('+');
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

pub(super) fn jj_sdk_show_change(
    store: &Store,
    change_id: &str,
    rel_path: &Path,
) -> Result<String> {
    let path_raw = rel_path.to_string_lossy().replace('\\', "/");
    let repo_path = RepoPathBuf::from_internal_string(path_raw.clone())
        .map_err(|_| Error::new(format!("invalid repo-relative path for jj: {path_raw}")))?;
    let config = StackedConfig::with_defaults();
    let settings = UserSettings::from_config(config).map_err(|e| Error::new(e.to_string()))?;
    let store_factories = StoreFactories::default();
    let wc_factories = default_working_copy_factories();
    let workspace = Workspace::load(&settings, &store.path, &store_factories, &wc_factories)
        .map_err(|e| Error::new(format!("jj-lib workspace load failed: {e}")))?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .map_err(|e| Error::new(format!("jj-lib repo load failed: {e}")))?;
    let resolved = jj_resolve_commit_id(repo.as_ref(), change_id)?;
    let commit = repo
        .store()
        .get_commit(&resolved)
        .map_err(|e| Error::new(format!("jj-lib commit load failed: {e}")))?;
    let before_tree = if let Some(parent_id) = commit.parent_ids().first() {
        repo.store()
            .get_commit(parent_id)
            .map_err(|e| Error::new(format!("jj-lib parent commit load failed: {e}")))?
            .tree()
    } else {
        repo.store().root_commit().tree()
    };
    let after_tree = commit.tree();
    let before = jj_materialize_file(repo.store().as_ref(), &before_tree, repo_path.as_ref())?;
    let after = jj_materialize_file(repo.store().as_ref(), &after_tree, repo_path.as_ref())?;
    Ok(jj_simple_unified_diff(
        &path_raw,
        before.as_deref(),
        after.as_deref(),
    ))
}
