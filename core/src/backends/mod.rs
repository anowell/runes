use crate::config::{BackendKind, Store};
use crate::{Error, Result};
use libpijul::pristine::sanakirja::Pristine;
use libpijul::DOT_DIR;
use std::path::{Path, PathBuf};
use std::process::Command;

mod jujutsu;
mod pijul;

use jujutsu::{
    jj_sdk_commit_paths, jj_sdk_file_change_ids, jj_sdk_file_log,
    jj_sdk_has_uncommitted_changes, jj_sdk_log, jj_sdk_show_change, jj_sdk_status, jj_sdk_sync,
    probe_jj_workspace,
};
use pijul::{
    pijul_sdk_commit_paths, pijul_sdk_file_change_ids, pijul_sdk_file_log, pijul_sdk_log,
    pijul_sdk_remove_path, pijul_sdk_show_change, pijul_sdk_status, pijul_sdk_sync,
};

pub trait BackendAdapter {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> BackendCapabilities;
    fn init_store(&self, path: &Path) -> Result<()>;
    fn commit_paths(&self, store: &Store, paths: &[PathBuf], message: &str) -> Result<()>;
    fn remove_path(&self, store: &Store, path: &Path) -> Result<()>;
    fn has_uncommitted_changes(&self, store: &Store) -> Result<bool>;
    fn status(&self, store: &Store) -> Result<String>;
    fn log(&self, store: &Store, limit: usize) -> Result<String>;
    fn file_log(&self, store: &Store, rel_path: &Path, limit: usize) -> Result<String>;
    fn file_change_ids(&self, store: &Store, rel_path: &Path, limit: usize) -> Result<Vec<String>>;
    fn show_change(&self, store: &Store, change_id: &str, rel_path: &Path) -> Result<String>;
    fn sync(&self, store: &Store) -> Result<()>;
}

#[derive(Clone, Copy, Debug)]
pub struct BackendCapabilities {
    pub cli_backed: bool,
    pub sdk_probe: bool,
    pub file_scoped_log: bool,
    pub file_change_inspection: bool,
    pub sync_supported: bool,
    pub remove_path_supported: bool,
}

pub struct CliBackend {
    kind: BackendKind,
}

impl CliBackend {
    pub fn new(kind: BackendKind) -> Self {
        Self { kind }
    }

    fn run_checked(cmd: &mut Command, context: &str) -> Result<()> {
        let output = cmd.output()?;
        if !output.status.success() {
            let stderr = String::from_utf8(output.stderr).unwrap_or_else(|_| String::new());
            return Err(Error::new(format!("{context} failed: {}", stderr.trim())));
        }
        Ok(())
    }
}

impl BackendAdapter for CliBackend {
    fn name(&self) -> &'static str {
        match self.kind {
            BackendKind::Jj => "jj-cli",
            BackendKind::Pijul => "pijul-cli",
        }
    }

    fn capabilities(&self) -> BackendCapabilities {
        match self.kind {
            BackendKind::Jj => BackendCapabilities {
                cli_backed: true,
                sdk_probe: true,
                file_scoped_log: true,
                file_change_inspection: true,
                sync_supported: true,
                remove_path_supported: false,
            },
            BackendKind::Pijul => BackendCapabilities {
                cli_backed: true,
                sdk_probe: true,
                file_scoped_log: true,
                file_change_inspection: true,
                sync_supported: true,
                remove_path_supported: true,
            },
        }
    }

    fn init_store(&self, path: &Path) -> Result<()> {
        std::fs::create_dir_all(path)?;
        match self.kind {
            BackendKind::Jj => {
                if path.join(".jj").exists() {
                    return Ok(());
                }
                Self::run_checked(
                    Command::new("jj")
                        .arg("git")
                        .arg("init")
                        .arg("--colocate")
                        .arg(path),
                    "jj git init --colocate",
                )?;
            }
            BackendKind::Pijul => {
                if path.join(".pijul").exists() {
                    return Ok(());
                }
                Self::run_checked(Command::new("pijul").arg("init").arg(path), "pijul init")?;
            }
        }
        Ok(())
    }

    fn commit_paths(&self, store: &Store, paths: &[PathBuf], message: &str) -> Result<()> {
        let _ = probe_sdk(store);
        match self.kind {
            BackendKind::Jj => {
                let _ = paths;
                jj_sdk_commit_paths(store, message)?;
            }
            BackendKind::Pijul => {
                pijul_sdk_commit_paths(store, paths, message)?;
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
            BackendKind::Pijul => {
                // Parse status output for dirty state
                let status = pijul_sdk_status(store)?;
                Ok(!status.contains("working_copy=clean"))
            }
        }
    }

    fn status(&self, store: &Store) -> Result<String> {
        match self.kind {
            BackendKind::Jj => jj_sdk_status(store),
            BackendKind::Pijul => pijul_sdk_status(store),
        }
    }

    fn log(&self, store: &Store, limit: usize) -> Result<String> {
        match self.kind {
            BackendKind::Jj => jj_sdk_log(store, limit),
            BackendKind::Pijul => pijul_sdk_log(store, limit),
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

    fn show_change(&self, store: &Store, change_id: &str, rel_path: &Path) -> Result<String> {
        match self.kind {
            BackendKind::Jj => jj_sdk_show_change(store, change_id, rel_path),
            BackendKind::Pijul => pijul_sdk_show_change(store, change_id),
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

pub fn adapter_capabilities(store: &Store) -> BackendCapabilities {
    adapter_for(store).capabilities()
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

pub fn commit_paths(store: &Store, paths: &[PathBuf], message: &str) -> Result<()> {
    adapter_for(store).commit_paths(store, paths, message)
}

pub fn remove_path(store: &Store, path: &Path) -> Result<()> {
    adapter_for(store).remove_path(store, path)
}

pub fn status(store: &Store) -> Result<String> {
    adapter_for(store).status(store)
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

pub fn show_change(store: &Store, change_id: &str, rel_path: &Path) -> Result<String> {
    adapter_for(store).show_change(store, change_id, rel_path)
}

pub fn has_uncommitted_changes(store: &Store) -> Result<bool> {
    adapter_for(store).has_uncommitted_changes(store)
}

pub fn sync(store: &Store) -> Result<()> {
    adapter_for(store).sync(store)
}
