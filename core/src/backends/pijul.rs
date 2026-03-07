use crate::config::Store;
use crate::{Error, Result};
use libpijul::changestore::filesystem::FileSystem as PijulChangeStore;
use libpijul::changestore::ChangeStore;
use libpijul::working_copy::filesystem::FileSystem as PijulWorkingCopy;
use libpijul::{Base32, Hash, MutTxnT, MutTxnTExt, TxnT, TxnTExt};
use pijul_identity::Complete as CompleteIdentity;
use pijul_interaction::{self, Spinner};
use pijul_remote::{self as pijul_remote, RemoteRepo};
use pijul_repository::{Repository as PijulRepository, CHANGES_DIR};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::runtime::Runtime;

fn pijul_identity_names() -> Vec<String> {
    CompleteIdentity::load_all()
        .map(|mut identities| identities.drain(..).map(|identity| identity.name).collect())
        .unwrap_or_default()
}

fn open_pijul_repo(store: &Store) -> Result<PijulRepository> {
    PijulRepository::find_root(Some(&store.path))
        .map_err(|e| Error::new(format!("pijul repository open failed: {e}")))
}

fn pijul_changes_dir(repo: &PijulRepository) -> PathBuf {
    repo.path.join(CHANGES_DIR)
}

pub(super) fn pijul_sdk_status(store: &Store) -> Result<String> {
    let repo = open_pijul_repo(store)?;
    let txn = (&repo.pristine)
        .txn_begin()
        .map_err(|e| Error::new(format!("libpijul txn begin failed: {e}")))?;
    let channel_name = txn
        .current_channel()
        .map_err(|e| Error::new(format!("libpijul current channel failed: {e}")))?
        .to_string();
    let channel = txn
        .load_channel(&channel_name)
        .map_err(|e| Error::new(format!("libpijul load channel failed: {e}")))?
        .ok_or_else(|| Error::new(format!("missing pijul channel: {channel_name}")))?;
    let latest = txn
        .reverse_log(&channel.read(), None)
        .map_err(|e| Error::new(format!("libpijul reverse log failed: {e}")))?
        .next()
        .transpose()
        .map_err(|e| Error::new(format!("libpijul reverse log next failed: {e}")))?;
    let mut lines = vec![
        "backend=libpijul".to_string(),
        format!("repo={}", repo.path.display()),
        format!("channel={channel_name}"),
    ];
    if let Some((n, (hash, _))) = latest {
        lines.push(format!("latest_n={n}"));
        lines.push(format!("latest_hash={hash:?}"));
    } else {
        lines.push("latest_n=none".to_string());
    }
    if let Some(default_remote) = &repo.config.default_remote {
        lines.push(format!("default_remote={default_remote}"));
    }
    if let Some(colors) = &repo.config.colors {
        lines.push(format!("colors={colors:?}"));
    }
    if let Some(pager) = &repo.config.pager {
        lines.push(format!("pager={pager:?}"));
    }
    let identities = pijul_identity_names();
    if !identities.is_empty() {
        lines.push(format!("identities={}", identities.join(",")));
    }
    Ok(lines.join("\n") + "\n")
}

pub(super) fn pijul_sdk_log(store: &Store, limit: usize) -> Result<String> {
    let repo = open_pijul_repo(store)?;
    let txn = (&repo.pristine)
        .txn_begin()
        .map_err(|e| Error::new(format!("libpijul txn begin failed: {e}")))?;
    let channel_name = txn
        .current_channel()
        .map_err(|e| Error::new(format!("libpijul current channel failed: {e}")))?
        .to_string();
    let channel = txn
        .load_channel(&channel_name)
        .map_err(|e| Error::new(format!("libpijul load channel failed: {e}")))?
        .ok_or_else(|| Error::new(format!("missing pijul channel: {channel_name}")))?;
    let mut out = String::new();
    for item in txn
        .reverse_log(&channel.read(), None)
        .map_err(|e| Error::new(format!("libpijul reverse log failed: {e}")))?
        .take(limit)
    {
        let (n, (hash, _)) =
            item.map_err(|e| Error::new(format!("libpijul reverse log item failed: {e}")))?;
        out.push_str(&format!("{n} {hash:?}\n"));
    }
    Ok(out)
}

fn pijul_sdk_path_hashes(store: &Store, rel_path: &Path, limit: usize) -> Result<Vec<String>> {
    let repo = open_pijul_repo(store)?;
    let path_raw = rel_path.to_string_lossy().replace('\\', "/");
    let txn = (&repo.pristine)
        .txn_begin()
        .map_err(|e| Error::new(format!("libpijul txn begin failed: {e}")))?;
    let channel_name = txn
        .current_channel()
        .map_err(|e| Error::new(format!("libpijul current channel failed: {e}")))?
        .to_string();
    let channel = txn
        .load_channel(&channel_name)
        .map_err(|e| Error::new(format!("libpijul load channel failed: {e}")))?
        .ok_or_else(|| Error::new(format!("missing pijul channel: {channel_name}")))?;
    let changes = PijulChangeStore::from_root(&repo.path, 32);
    let (pos, _is_dir) = txn
        .follow_oldest_path(&changes, &channel, &path_raw)
        .map_err(|e| Error::new(format!("libpijul follow path failed: {e}")))?;
    let mut out = Vec::new();
    for item in txn
        .rev_log_for_path(&channel.read(), pos, 0)
        .map_err(|e| Error::new(format!("libpijul rev log for path failed: {e}")))?
        .take(limit)
    {
        let h = item.map_err(|e| Error::new(format!("libpijul rev path item failed: {e}")))?;
        out.push(h.to_base32());
    }
    Ok(out)
}

pub(super) fn pijul_sdk_file_log(store: &Store, rel_path: &Path, limit: usize) -> Result<String> {
    let hashes = pijul_sdk_path_hashes(store, rel_path, limit)?;
    Ok(hashes
        .into_iter()
        .map(|h| format!("Change {h}"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n")
}

pub(super) fn pijul_sdk_file_change_ids(
    store: &Store,
    rel_path: &Path,
    limit: usize,
) -> Result<Vec<String>> {
    pijul_sdk_path_hashes(store, rel_path, limit)
}

pub(super) fn pijul_sdk_show_change(store: &Store, change_id: &str) -> Result<String> {
    let repo = open_pijul_repo(store)?;
    let changes = PijulChangeStore::from_root(&repo.path, 32);
    let hash = change_id
        .parse::<Hash>()
        .map_err(|e| Error::new(format!("invalid pijul change hash: {e}")))?;
    let change = changes
        .get_change(&hash)
        .map_err(|e| Error::new(format!("libpijul load change failed: {e}")))?;
    Ok(format!("{change:#?}"))
}

pub(super) fn pijul_sdk_rich_log(store: &Store, limit: usize) -> Result<Vec<super::LogEntry>> {
    let repo = open_pijul_repo(store)?;
    let txn = (&repo.pristine)
        .txn_begin()
        .map_err(|e| Error::new(format!("libpijul txn begin failed: {e}")))?;
    let channel_name = txn
        .current_channel()
        .map_err(|e| Error::new(format!("libpijul current channel failed: {e}")))?
        .to_string();
    let channel = txn
        .load_channel(&channel_name)
        .map_err(|e| Error::new(format!("libpijul load channel failed: {e}")))?
        .ok_or_else(|| Error::new(format!("missing pijul channel: {channel_name}")))?;
    let changes = PijulChangeStore::from_root(&repo.path, 32);
    let mut entries = Vec::new();
    for item in txn
        .reverse_log(&channel.read(), None)
        .map_err(|e| Error::new(format!("libpijul reverse log failed: {e}")))?
        .take(limit)
    {
        let (_n, pair) =
            item.map_err(|e| Error::new(format!("libpijul reverse log item failed: {e}")))?;
        let hash: Hash = pair.0.into();
        let revision = hash.to_base32();
        let (author, timestamp, description) = match changes.get_change(&hash) {
            Ok(change) => {
                let author_name = change
                    .header
                    .authors
                    .first()
                    .and_then(|a| a.0.get("name").or(a.0.get("key")))
                    .cloned()
                    .unwrap_or_default();
                let ts = change.header.timestamp.as_second();
                let desc = change.header.message.clone();
                (author_name, ts, desc)
            }
            Err(_) => (String::new(), 0, String::new()),
        };
        entries.push(super::LogEntry {
            revision,
            timestamp,
            author,
            description,
            changed_files: Vec::new(),
        });
    }
    Ok(entries)
}

pub(super) fn pijul_sdk_file_at_revision(
    store: &Store,
    rel_path: &Path,
    revision: &str,
) -> Result<String> {
    let repo = open_pijul_repo(store)?;
    let txn = (&repo.pristine)
        .arc_txn_begin()
        .map_err(|e| Error::new(format!("libpijul txn begin failed: {e}")))?;

    let channel_name = txn
        .read()
        .current_channel()
        .map_err(|e| Error::new(format!("libpijul current channel failed: {e}")))?
        .to_string();
    let channel = {
        let txn_read = txn.read();
        txn_read
            .load_channel(&channel_name)
            .map_err(|e| Error::new(format!("libpijul load channel failed: {e}")))?
            .ok_or_else(|| Error::new(format!("missing pijul channel: {channel_name}")))?
    };

    // Resolve the target revision hash (supports prefix matching)
    let (target_hash, _) = txn
        .read()
        .hash_from_prefix(revision)
        .map_err(|e| Error::new(format!("could not resolve pijul revision '{revision}': {e}")))?;

    // Find the log position of the target change by walking the reverse log
    let target_n = {
        let txn_read = txn.read();
        let ch = channel.read();
        let mut found = None;
        for item in txn_read
            .reverse_log(&*ch, None)
            .map_err(|e| Error::new(format!("libpijul reverse log failed: {e}")))?
        {
            let (n, pair) =
                item.map_err(|e| Error::new(format!("libpijul reverse log item failed: {e}")))?;
            let hash: Hash = pair.0.into();
            if hash == target_hash {
                found = Some(n);
                break;
            }
        }
        found.ok_or_else(|| Error::new(format!("revision {revision} not found in channel log")))?
    };

    // Fork the channel to a temp channel
    let temp_name = format!("_runes_temp_{}", std::process::id());
    let temp_channel = txn
        .write()
        .fork(&channel, &temp_name)
        .map_err(|e| Error::new(format!("libpijul fork channel failed: {e}")))?;

    // Collect changes newer than target_n and unrecord them
    let changes_to_unrecord: Vec<Hash> = {
        let txn_read = txn.read();
        let ch = temp_channel.read();
        let mut to_remove = Vec::new();
        for item in txn_read
            .reverse_log(&*ch, None)
            .map_err(|e| Error::new(format!("libpijul reverse log failed: {e}")))?
        {
            let (n, pair) =
                item.map_err(|e| Error::new(format!("libpijul reverse log item failed: {e}")))?;
            if n > target_n {
                to_remove.push(pair.0.into());
            } else {
                break;
            }
        }
        to_remove
    };

    let working_copy = PijulWorkingCopy::from_root(&repo.path);
    for hash in &changes_to_unrecord {
        txn.write()
            .unrecord(&repo.changes, &temp_channel, hash, 0, &working_copy)
            .map_err(|e| Error::new(format!("libpijul unrecord failed: {e}")))?;
    }

    // Read the file from the temp channel
    let internal_path = rel_to_internal_path(rel_path)?;
    let (pos, _ambiguous) = txn
        .read()
        .follow_oldest_path(&repo.changes, &temp_channel, &internal_path)
        .map_err(|e| Error::new(format!("libpijul follow path failed: {e}")))?;

    let mut writer = libpijul::vertex_buffer::Writer::new(Vec::<u8>::new());
    libpijul::output::output_file(&repo.changes, &txn, &temp_channel, pos, &mut writer)
        .map_err(|e| Error::new(format!("libpijul output file failed: {e}")))?;
    let bytes = writer.into_inner();

    // Clean up the temp channel — must drop the reference first
    drop(temp_channel);
    txn.write()
        .drop_channel(&temp_name)
        .map_err(|e| Error::new(format!("libpijul drop temp channel failed: {e}")))?;
    txn.commit()
        .map_err(|e| Error::new(format!("libpijul commit failed: {e}")))?;

    String::from_utf8(bytes)
        .map_err(|e| Error::new(format!("file content is not valid UTF-8: {e}")))
}

fn rel_to_internal_path(path: &Path) -> Result<String> {
    let raw = path.to_string_lossy().replace('\\', "/");
    if raw.is_empty() || raw == "." {
        return Err(Error::new("empty path is not valid"));
    }
    Ok(raw)
}

pub(super) fn pijul_sdk_commit_paths(
    store: &Store,
    paths: &[PathBuf],
    message: &str,
) -> Result<()> {
    let repo = open_pijul_repo(store)?;
    let txn = (&repo.pristine)
        .arc_txn_begin()
        .map_err(|e| Error::new(format!("libpijul txn begin failed: {e}")))?;

    let changes = repo.changes.clone();
    for rel_path in paths {
        let full = store.path.join(rel_path);
        if !full.exists() {
            continue;
        }
        let internal = rel_to_internal_path(rel_path)?;
        let is_dir = full.is_dir();
        let add_result = txn.write().add(&internal, is_dir, 0);
        if let Err(libpijul::fs::FsError::AlreadyInRepo(_)) = add_result {
            continue;
        }
        if let Err(e) = add_result {
            return Err(Error::new(format!(
                "libpijul add tracking failed for {}: {e}",
                rel_path.display()
            )));
        }
    }

    let channel_name = txn
        .read()
        .current_channel()
        .map_err(|e| Error::new(format!("libpijul current channel failed: {e}")))?
        .to_string();
    let channel = txn
        .write()
        .open_or_create_channel(&channel_name)
        .map_err(|e| Error::new(format!("libpijul open channel failed: {e}")))?;

    let working_copy = PijulWorkingCopy::from_root(&repo.path);
    let mut record = libpijul::RecordBuilder::new();
    record
        .record(
            txn.clone(),
            libpijul::Algorithm::default(),
            false,
            &libpijul::DEFAULT_SEPARATOR,
            channel.clone(),
            &working_copy,
            &changes,
            "",
            1,
        )
        .map_err(|e| Error::new(format!("libpijul record failed: {e}")))?;
    let recorded = record.finish();

    if !recorded.actions.is_empty() {
        let actions = recorded
            .actions
            .into_iter()
            .map(|a| {
                a.globalize(&*txn.read())
                    .map_err(|e| Error::new(format!("libpijul globalize failed: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;
        let contents = std::mem::take(&mut *recorded.contents.lock());
        let header = libpijul::change::ChangeHeader {
            message: message.to_string(),
            ..Default::default()
        };
        let mut change = libpijul::change::Change::make_change(
            &*txn.read(),
            &channel,
            actions,
            contents,
            header,
            Vec::new(),
        )
        .map_err(|e| Error::new(format!("libpijul make_change failed: {e}")))?;
        let hash = changes
            .save_change(&mut change, |_, _| {
                Ok::<_, libpijul::changestore::filesystem::Error>(())
            })
            .map_err(|e| Error::new(format!("libpijul save_change failed: {e}")))?;
        txn.write()
            .apply_local_change(&channel, &change, &hash, &recorded.updatables)
            .map_err(|e| Error::new(format!("libpijul apply_local_change failed: {e}")))?;
    }
    txn.commit()
        .map_err(|e| Error::new(format!("libpijul commit failed: {e}")))?;
    Ok(())
}

pub(super) fn pijul_sdk_remove_path(store: &Store, path: &Path) -> Result<()> {
    let repo = open_pijul_repo(store)?;
    let pristine = &repo.pristine;
    let txn = pristine
        .arc_txn_begin()
        .map_err(|e| Error::new(format!("libpijul txn begin failed: {e}")))?;
    let internal = rel_to_internal_path(path)?;
    let remove_result = txn.write().remove_file(&internal);
    if let Err(libpijul::fs::FsError::NotFound(_)) = remove_result {
        txn.commit()
            .map_err(|e| Error::new(format!("libpijul commit failed: {e}")))?;
        return Ok(());
    }
    if let Err(e) = remove_result {
        return Err(Error::new(format!(
            "libpijul remove tracking failed for {}: {e}",
            path.display()
        )));
    }
    txn.commit()
        .map_err(|e| Error::new(format!("libpijul commit failed: {e}")))?;
    Ok(())
}

fn normalize_pijul_remote_path(store_root: &Path, raw: &str) -> Option<PathBuf> {
    if raw.contains("://") && !raw.starts_with("file://") {
        return None;
    }
    let stripped = raw.strip_prefix("file://").unwrap_or(raw);
    let p = PathBuf::from(stripped);
    if p.is_absolute() {
        Some(p)
    } else {
        Some(store_root.join(p))
    }
}

pub(super) fn pijul_sdk_sync(store: &Store) -> Result<()> {
    let mut repo = open_pijul_repo(store)?;
    let channel_name = current_pijul_channel(&repo)?;
    let specs = collect_pijul_remote_specs(&repo)?;
    if specs.is_empty() {
        return Ok(());
    }
    let runtime =
        Runtime::new().map_err(|e| Error::new(format!("tokio runtime init failed: {e}")))?;
    for spec in specs {
        let label = spec.label();
        let _spinner = Spinner::new(format!("Syncing {label}"))
            .map_err(|err| Error::new(format!("pijul interaction spinner failed: {err}")))?;
        sync_pijul_remote(&runtime, &mut repo, &channel_name, spec)?;
    }
    Ok(())
}

enum RemoteSpec {
    Named(String),
    Path(PathBuf),
}

impl RemoteSpec {
    fn label(&self) -> String {
        match self {
            RemoteSpec::Named(name) => format!("remote {name}"),
            RemoteSpec::Path(path) => format!("path {}", path.display()),
        }
    }
}

fn current_pijul_channel(repo: &PijulRepository) -> Result<String> {
    let txn = (&repo.pristine)
        .txn_begin()
        .map_err(|e| Error::new(format!("libpijul txn begin failed: {e}")))?;
    let channel_name = txn
        .current_channel()
        .map_err(|e| Error::new(format!("libpijul current channel failed: {e}")))?
        .to_string();
    Ok(channel_name)
}

fn collect_pijul_remote_specs(repo: &PijulRepository) -> Result<Vec<RemoteSpec>> {
    let mut specs = Vec::new();
    let mut seen_names = BTreeSet::new();
    for rc in &repo.config.remotes {
        let name = rc.name().to_string();
        if seen_names.insert(name.clone()) {
            specs.push(RemoteSpec::Named(name));
        }
    }
    if let Some(default) = &repo.config.default_remote {
        if seen_names.insert(default.clone()) {
            specs.push(RemoteSpec::Named(default.clone()));
        }
    }
    let repo_root = canonical_repo_path(&&repo.path);
    let mut seen_paths = BTreeSet::new();
    let txn = (&repo.pristine)
        .txn_begin()
        .map_err(|e| Error::new(format!("libpijul txn begin failed: {e}")))?;
    for item in txn
        .iter_remotes(&libpijul::pristine::RemoteId::nil())
        .map_err(|e| Error::new(format!("libpijul iterate remotes failed: {e}")))?
    {
        let remote = item.map_err(|e| Error::new(format!("libpijul remote item failed: {e}")))?;
        let remote_guard = remote.lock();
        let raw = remote_guard.path.clone();
        drop(remote_guard);
        if let Some(path) = normalize_pijul_remote_path(&&repo.path, raw.as_str()) {
            let canonical = match fs::canonicalize(&path) {
                Ok(canonical) => canonical,
                Err(_) => continue,
            };
            if canonical == repo_root {
                continue;
            }
            if !canonical.join(libpijul::DOT_DIR).is_dir() {
                continue;
            }
            if seen_paths.insert(canonical.clone()) {
                specs.push(RemoteSpec::Path(canonical));
            }
        }
    }
    Ok(specs)
}

fn map_anyhow_error(err: anyhow::Error) -> Error {
    Error::new(err.to_string())
}

fn canonical_repo_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn sync_pijul_remote(
    runtime: &Runtime,
    repo: &mut PijulRepository,
    channel: &str,
    spec: RemoteSpec,
) -> Result<()> {
    let mut remote = match spec {
        RemoteSpec::Named(name) => open_named_remote(runtime, repo, &name, channel)?,
        RemoteSpec::Path(path) => open_path_remote(runtime, repo, &path, channel)?,
    };
    run_pijul_pull(runtime, repo, channel, &mut remote)?;
    run_pijul_push(runtime, repo, channel, &mut remote)?;
    runtime
        .block_on(remote.finish())
        .map_err(map_anyhow_error)?;
    Ok(())
}

fn open_named_remote(
    runtime: &Runtime,
    repo: &mut PijulRepository,
    remote: &str,
    channel: &str,
) -> Result<RemoteRepo> {
    let repo_path = repo.path.to_path_buf();
    runtime
        .block_on(pijul_remote::repository(
            repo,
            Some(repo_path.as_path()),
            None,
            remote,
            channel,
            false,
            true,
        ))
        .map_err(map_anyhow_error)
}

fn open_path_remote(
    runtime: &Runtime,
    repo: &mut PijulRepository,
    remote_path: &Path,
    channel: &str,
) -> Result<RemoteRepo> {
    let remote_str = remote_path
        .to_str()
        .ok_or_else(|| Error::new(format!("invalid remote path: {}", remote_path.display())))?;
    runtime
        .block_on(pijul_remote::unknown_remote(
            Some(repo.path.as_path()),
            None,
            remote_str,
            channel,
            false,
            true,
        ))
        .map_err(map_anyhow_error)
}

fn run_pijul_pull(
    runtime: &Runtime,
    repo: &mut PijulRepository,
    channel: &str,
    remote: &mut RemoteRepo,
) -> Result<()> {
    let txn = (&repo.pristine)
        .arc_txn_begin()
        .map_err(|e| Error::new(format!("libpijul txn begin failed: {e}")))?;
    {
        let mut write = txn.write();
        let mut channel_ref = write
            .open_or_create_channel(channel)
            .map_err(|e| Error::new(format!("libpijul channel open failed: {e}")))?;
        let delta = {
            let repo_read = &*repo;
            runtime
                .block_on(remote.update_changelist_pushpull(
                    &mut *write,
                    &[],
                    &mut channel_ref,
                    None,
                    repo_read,
                    &[] as &[String],
                    true,
                ))
                .map_err(map_anyhow_error)?
        };
        let downloaded = {
            let repo_mut = &mut *repo;
            runtime
                .block_on(remote.pull(
                    repo_mut,
                    &mut *write,
                    &mut channel_ref,
                    delta.to_download.as_slice(),
                    &delta.inodes,
                    true,
                ))
                .map_err(map_anyhow_error)?
        };
        {
            let repo_mut = &mut *repo;
            let txn_read = txn.read();
            runtime
                .block_on(remote.complete_changes(
                    repo_mut,
                    &*txn_read,
                    &mut channel_ref,
                    &downloaded,
                    false,
                ))
                .map_err(map_anyhow_error)?;
        }
    }
    txn.commit()
        .map_err(|e| Error::new(format!("libpijul txn commit failed: {e}")))?;
    Ok(())
}

fn run_pijul_push(
    runtime: &Runtime,
    repo: &mut PijulRepository,
    channel: &str,
    remote: &mut RemoteRepo,
) -> Result<()> {
    let txn = (&repo.pristine)
        .arc_txn_begin()
        .map_err(|e| Error::new(format!("libpijul txn begin failed: {e}")))?;
    {
        let mut write = txn.write();
        let mut channel_ref = write
            .open_or_create_channel(channel)
            .map_err(|e| Error::new(format!("libpijul channel open failed: {e}")))?;
        let delta = {
            let repo_read = &*repo;
            runtime
                .block_on(remote.update_changelist_pushpull(
                    &mut *write,
                    &[],
                    &mut channel_ref,
                    None,
                    repo_read,
                    &[] as &[String],
                    false,
                ))
                .map_err(map_anyhow_error)?
        };
        let push_delta = match remote {
            RemoteRepo::LocalChannel(remote_channel) => {
                let repo_read = &*repo;
                delta
                    .to_local_channel_push(
                        remote_channel,
                        &mut *write,
                        &[],
                        &channel_ref,
                        repo_read,
                    )
                    .map_err(map_anyhow_error)?
            }
            _ => {
                let repo_read = &*repo;
                delta
                    .to_remote_push(&mut *write, &[], &channel_ref, repo_read)
                    .map_err(map_anyhow_error)?
            }
        };
        if !push_delta.to_upload.is_empty() {
            runtime
                .block_on(remote.upload_changes(
                    &mut *write,
                    pijul_changes_dir(repo),
                    None,
                    &push_delta.to_upload,
                ))
                .map_err(map_anyhow_error)?;
        }
    }
    txn.commit()
        .map_err(|e| Error::new(format!("libpijul txn commit failed: {e}")))?;
    Ok(())
}
