use crate::config::{BackendKind, Store};
use crate::{Error, Result};
use libpijul::pristine::sanakirja::Pristine;
use libpijul::DOT_DIR;
use std::path::{Path, PathBuf};
use std::process::Command;

mod jujutsu;
mod pijul;

use jujutsu::{
    jj_sdk_commit_paths, jj_sdk_file_at_revision, jj_sdk_file_before_revision,
    jj_sdk_file_change_ids, jj_sdk_file_log, jj_sdk_file_rich_log, jj_sdk_has_uncommitted_changes,
    jj_sdk_log, jj_sdk_remotes, jj_sdk_rich_log, jj_sdk_show_change, jj_sdk_status, jj_sdk_sync,
    jj_sdk_uncommitted_rune_paths, probe_jj_workspace,
};
use pijul::{
    pijul_sdk_commit_paths, pijul_sdk_file_at_revision, pijul_sdk_file_before_revision,
    pijul_sdk_file_change_ids, pijul_sdk_file_log, pijul_sdk_file_rich_log,
    pijul_sdk_has_uncommitted_changes, pijul_sdk_log, pijul_sdk_remotes, pijul_sdk_remove_path,
    pijul_sdk_rich_log, pijul_sdk_show_change, pijul_sdk_status, pijul_sdk_sync,
    pijul_sdk_uncommitted_rune_paths,
};

/// A structured log entry from the backend.
#[derive(Clone, Debug)]
pub struct LogEntry {
    pub revision: String,
    pub timestamp: i64,
    /// Author display name, falling back to the email when the commit has none.
    pub author: String,
    /// Author email, the key identities are recorded under. May be empty.
    pub author_email: String,
    pub description: String,
    pub changed_files: Vec<String>,
}

impl LogEntry {
    pub fn identity(&self) -> crate::identity::Identity {
        crate::identity::Identity::from_parts(&self.author, &self.author_email)
    }
}

pub trait BackendAdapter {
    fn name(&self) -> &'static str;
    fn init_store(&self, path: &Path) -> Result<()>;
    fn commit_paths(
        &self,
        store: &Store,
        paths: &[PathBuf],
        message: &str,
        author_name: &str,
        author_email: &str,
    ) -> Result<()>;
    fn remove_path(&self, store: &Store, path: &Path) -> Result<()>;
    fn has_uncommitted_changes(&self, store: &Store) -> Result<bool>;
    fn uncommitted_rune_paths(&self, store: &Store) -> Result<Vec<PathBuf>>;
    fn status(&self, store: &Store) -> Result<String>;
    /// Configured remote names, empty when the store only lives locally.
    fn remotes(&self, store: &Store) -> Result<Vec<String>>;
    fn log(&self, store: &Store, limit: usize) -> Result<String>;
    fn rich_log(&self, store: &Store, limit: usize) -> Result<Vec<LogEntry>>;
    fn file_log(&self, store: &Store, rel_path: &Path, limit: usize) -> Result<String>;
    fn file_change_ids(&self, store: &Store, rel_path: &Path, limit: usize) -> Result<Vec<String>>;
    fn file_rich_log(&self, store: &Store, rel_path: &Path, limit: usize) -> Result<Vec<LogEntry>>;
    fn show_change(&self, store: &Store, change_id: &str, rel_path: &Path) -> Result<String>;
    fn file_at_revision(&self, store: &Store, rel_path: &Path, revision: &str) -> Result<String>;
    fn file_before_revision(
        &self,
        store: &Store,
        rel_path: &Path,
        revision: &str,
    ) -> Result<String>;
    fn sync(&self, store: &Store) -> Result<()>;
}

pub struct CliBackend {
    kind: BackendKind,
}

impl CliBackend {
    pub fn new(kind: BackendKind) -> Self {
        Self { kind }
    }

    fn run_checked(cmd: &mut Command, context: &str) -> Result<()> {
        let program = cmd.get_program().to_string_lossy().to_string();
        let output = cmd.output().map_err(|e| spawn_error(&program, e))?;
        if !output.status.success() {
            let stderr = String::from_utf8(output.stderr).unwrap_or_else(|_| String::new());
            return Err(Error::new(format!("{context} failed: {}", stderr.trim())));
        }
        Ok(())
    }
}

fn spawn_error(program: &str, err: std::io::Error) -> Error {
    if err.kind() == std::io::ErrorKind::NotFound {
        return Error::new(missing_backend_message(program));
    }
    Error::new(format!("could not run `{program}`: {err}"))
}

fn missing_backend_message(program: &str) -> String {
    let install = match program {
        "jj" => " (see https://jj-vcs.github.io/jj/latest/install-and-setup/)",
        "pijul" => " (see https://pijul.org/manual/introduction.html#installing)",
        _ => "",
    };
    format!("`{program}` was not found on PATH. Install it to use the {program} backend{install}.")
}

pub fn backend_available(backend: &BackendKind) -> bool {
    let program = backend.as_str();
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| dir.join(program).is_file())
}

pub fn missing_backend_error(backend: &BackendKind) -> Error {
    Error::new(missing_backend_message(backend.as_str()))
}

impl BackendAdapter for CliBackend {
    fn name(&self) -> &'static str {
        match self.kind {
            BackendKind::Jj => "jj",
            BackendKind::Pijul => "pijul",
        }
    }

    fn init_store(&self, path: &Path) -> Result<()> {
        let dot = match self.kind {
            BackendKind::Jj => ".jj",
            BackendKind::Pijul => ".pijul",
        };
        if path.join(dot).exists() {
            return Ok(());
        }
        // Probe before create_dir_all, or a missing binary leaves an empty store behind.
        if !backend_available(&self.kind) {
            return Err(missing_backend_error(&self.kind));
        }
        std::fs::create_dir_all(path)?;
        match self.kind {
            BackendKind::Jj => Self::run_checked(
                Command::new("jj")
                    .arg("git")
                    .arg("init")
                    .arg("--colocate")
                    .arg(path),
                "jj git init --colocate",
            )?,
            BackendKind::Pijul => {
                Self::run_checked(Command::new("pijul").arg("init").arg(path), "pijul init")?
            }
        }
        Ok(())
    }

    fn commit_paths(
        &self,
        store: &Store,
        paths: &[PathBuf],
        message: &str,
        author_name: &str,
        author_email: &str,
    ) -> Result<()> {
        let _ = probe_sdk(store);
        match self.kind {
            BackendKind::Jj => {
                jj_sdk_commit_paths(store, paths, message, author_name, author_email)?;
            }
            BackendKind::Pijul => {
                pijul_sdk_commit_paths(store, paths, message, author_name, author_email)?;
            }
        }
        Ok(())
    }

    fn remove_path(&self, store: &Store, path: &Path) -> Result<()> {
        if self.kind != BackendKind::Pijul {
            return Ok(());
        }
        pijul_sdk_remove_path(store, path)
    }

    fn has_uncommitted_changes(&self, store: &Store) -> Result<bool> {
        match self.kind {
            BackendKind::Jj => jj_sdk_has_uncommitted_changes(store),
            BackendKind::Pijul => pijul_sdk_has_uncommitted_changes(store),
        }
    }

    fn uncommitted_rune_paths(&self, store: &Store) -> Result<Vec<PathBuf>> {
        match self.kind {
            BackendKind::Jj => jj_sdk_uncommitted_rune_paths(store),
            BackendKind::Pijul => pijul_sdk_uncommitted_rune_paths(store),
        }
    }

    fn status(&self, store: &Store) -> Result<String> {
        match self.kind {
            BackendKind::Jj => jj_sdk_status(store),
            BackendKind::Pijul => pijul_sdk_status(store),
        }
    }

    fn remotes(&self, store: &Store) -> Result<Vec<String>> {
        match self.kind {
            BackendKind::Jj => jj_sdk_remotes(store),
            BackendKind::Pijul => pijul_sdk_remotes(store),
        }
    }

    fn log(&self, store: &Store, limit: usize) -> Result<String> {
        match self.kind {
            BackendKind::Jj => jj_sdk_log(store, limit),
            BackendKind::Pijul => pijul_sdk_log(store, limit),
        }
    }

    fn rich_log(&self, store: &Store, limit: usize) -> Result<Vec<LogEntry>> {
        match self.kind {
            BackendKind::Jj => jj_sdk_rich_log(store, limit),
            BackendKind::Pijul => pijul_sdk_rich_log(store, limit),
        }
    }

    fn file_log(&self, store: &Store, rel_path: &Path, limit: usize) -> Result<String> {
        match self.kind {
            BackendKind::Jj => jj_sdk_file_log(store, rel_path, limit),
            BackendKind::Pijul => pijul_sdk_file_log(store, rel_path, limit),
        }
    }

    fn file_change_ids(&self, store: &Store, rel_path: &Path, limit: usize) -> Result<Vec<String>> {
        match self.kind {
            BackendKind::Jj => jj_sdk_file_change_ids(store, rel_path, limit),
            BackendKind::Pijul => pijul_sdk_file_change_ids(store, rel_path, limit),
        }
    }

    fn file_rich_log(&self, store: &Store, rel_path: &Path, limit: usize) -> Result<Vec<LogEntry>> {
        match self.kind {
            BackendKind::Jj => jj_sdk_file_rich_log(store, rel_path, limit),
            BackendKind::Pijul => pijul_sdk_file_rich_log(store, rel_path, limit),
        }
    }

    fn show_change(&self, store: &Store, change_id: &str, rel_path: &Path) -> Result<String> {
        match self.kind {
            BackendKind::Jj => jj_sdk_show_change(store, change_id, rel_path),
            BackendKind::Pijul => pijul_sdk_show_change(store, change_id),
        }
    }

    fn file_at_revision(&self, store: &Store, rel_path: &Path, revision: &str) -> Result<String> {
        match self.kind {
            BackendKind::Jj => jj_sdk_file_at_revision(store, rel_path, revision),
            BackendKind::Pijul => pijul_sdk_file_at_revision(store, rel_path, revision),
        }
    }

    fn file_before_revision(
        &self,
        store: &Store,
        rel_path: &Path,
        revision: &str,
    ) -> Result<String> {
        match self.kind {
            BackendKind::Jj => jj_sdk_file_before_revision(store, rel_path, revision),
            BackendKind::Pijul => pijul_sdk_file_before_revision(store, rel_path, revision),
        }
    }

    fn sync(&self, store: &Store) -> Result<()> {
        let _ = probe_sdk(store);
        match self.kind {
            BackendKind::Jj => jj_sdk_sync(store)?,
            BackendKind::Pijul => pijul_sdk_sync(store)?,
        }
        Ok(())
    }
}

pub fn adapter_for(store: &Store) -> Box<dyn BackendAdapter> {
    Box::new(CliBackend::new(store.backend.clone()))
}

pub fn init_store(path: &Path, backend: BackendKind) -> Result<()> {
    CliBackend::new(backend).init_store(path)
}

pub fn adapter_name(store: &Store) -> String {
    adapter_for(store).name().to_string()
}

pub fn probe_sdk(store: &Store) -> Result<String> {
    match store.backend {
        BackendKind::Jj => {
            let (workspace_root, repo_path) = probe_jj_workspace(store)?;
            Ok(format!(
                "jj-lib ok workspace_root={} repo_path={}",
                workspace_root.display(),
                repo_path.display()
            ))
        }
        BackendKind::Pijul => {
            let dot = store.path.join(DOT_DIR);
            if !dot.exists() {
                return Err(Error::new(format!(
                    "Not a pijul repository: {}",
                    store.path.display()
                )));
            }
            let pristine_path = dot.join("pristine").join("db");
            let _pristine = Pristine::new(&pristine_path)
                .map_err(|e| Error::new(format!("libpijul pristine open failed: {e}")))?;
            Ok(format!(
                "libpijul ok repo_root={} pristine_db={}",
                store.path.display(),
                pristine_path.display()
            ))
        }
    }
}

pub fn commit_paths(
    store: &Store,
    paths: &[PathBuf],
    message: &str,
    author_name: &str,
    author_email: &str,
) -> Result<()> {
    adapter_for(store).commit_paths(store, paths, message, author_name, author_email)
}

pub fn remove_path(store: &Store, path: &Path) -> Result<()> {
    adapter_for(store).remove_path(store, path)
}

pub fn status(store: &Store) -> Result<String> {
    adapter_for(store).status(store)
}

pub fn remotes(store: &Store) -> Result<Vec<String>> {
    adapter_for(store).remotes(store)
}

pub fn log(store: &Store, limit: usize) -> Result<String> {
    adapter_for(store).log(store, limit)
}

pub fn file_log(store: &Store, rel_path: &Path, limit: usize) -> Result<String> {
    adapter_for(store).file_log(store, rel_path, limit)
}

pub fn file_change_ids(store: &Store, rel_path: &Path, limit: usize) -> Result<Vec<String>> {
    adapter_for(store).file_change_ids(store, rel_path, limit)
}

pub fn file_rich_log(store: &Store, rel_path: &Path, limit: usize) -> Result<Vec<LogEntry>> {
    adapter_for(store).file_rich_log(store, rel_path, limit)
}

pub fn show_change(store: &Store, change_id: &str, rel_path: &Path) -> Result<String> {
    adapter_for(store).show_change(store, change_id, rel_path)
}

pub fn rich_log(store: &Store, limit: usize) -> Result<Vec<LogEntry>> {
    adapter_for(store).rich_log(store, limit)
}

pub fn file_at_revision(store: &Store, rel_path: &Path, revision: &str) -> Result<String> {
    adapter_for(store).file_at_revision(store, rel_path, revision)
}

pub fn file_before_revision(store: &Store, rel_path: &Path, revision: &str) -> Result<String> {
    adapter_for(store).file_before_revision(store, rel_path, revision)
}

pub fn has_uncommitted_changes(store: &Store) -> Result<bool> {
    adapter_for(store).has_uncommitted_changes(store)
}

pub fn uncommitted_rune_paths(store: &Store) -> Result<Vec<PathBuf>> {
    adapter_for(store).uncommitted_rune_paths(store)
}

pub fn sync(store: &Store) -> Result<()> {
    adapter_for(store).sync(store)
}
