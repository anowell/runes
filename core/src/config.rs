use crate::{Error, Result};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackendKind {
    Jj,
    Pijul,
}

impl BackendKind {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "jj" => Ok(Self::Jj),
            "pijul" => Ok(Self::Pijul),
            _ => Err(Error::new(format!(
                "Unknown backend '{s}'. Expected 'jj' or 'pijul'"
            ))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Jj => "jj",
            Self::Pijul => "pijul",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Store {
    pub name: String,
    pub backend: BackendKind,
    pub path: PathBuf,
}

/// Detect the backend kind from a store directory by looking for `.pijul` or `.jj`.
/// Returns `None` if neither is found (invalid store).
pub fn detect_backend(path: &Path) -> Option<BackendKind> {
    if path.join(".pijul").is_dir() {
        Some(BackendKind::Pijul)
    } else if path.join(".jj").is_dir() {
        Some(BackendKind::Jj)
    } else {
        None
    }
}

/// Discover stores by scanning `~/.runes/stores/`.
/// Each subdirectory with a recognized VCS directory becomes a store.
/// Directories without `.pijul` or `.jj` are skipped (invalid stores).
pub fn discover_stores() -> Result<Vec<Store>> {
    let home = std::env::var("HOME").map_err(|_| Error::new("HOME is not set"))?;
    let stores_dir = PathBuf::from(home).join(".runes").join("stores");
    if !stores_dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut stores = Vec::new();
    let mut entries: Vec<_> = fs::read_dir(&stores_dir)?.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(backend) = detect_backend(&path) {
            stores.push(Store {
                name,
                backend,
                path,
            });
        }
    }
    Ok(stores)
}

/// Look up a store by name from a list of stores.
pub fn get_store(stores: &[Store], name: &str) -> Result<Store> {
    stores
        .iter()
        .find(|s| s.name == name)
        .cloned()
        .ok_or_else(|| Error::new(format!("Unknown store '{name}'")))
}

pub fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    Ok(())
}
