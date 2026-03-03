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

#[derive(Clone, Debug, Default)]
pub struct Config {
    pub default_store: Option<String>,
    pub stores: Vec<Store>,
}

impl Config {
    pub fn path() -> Result<PathBuf> {
        let home = std::env::var("HOME").map_err(|_| Error::new("HOME is not set"))?;
        Ok(PathBuf::from(home).join(".runes").join("config.txt"))
    }

    pub fn load() -> Result<Self> {
        let path = Self::path()?;
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = fs::read_to_string(path)?;
        let mut cfg = Self::default();
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(rest) = line.strip_prefix("default_store=") {
                cfg.default_store = Some(rest.trim().to_string());
                continue;
            }
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() != 4 || parts[0] != "store" {
                return Err(Error::new(format!("Invalid config line: {line}")));
            }
            cfg.stores.push(Store {
                name: parts[1].to_string(),
                backend: BackendKind::parse(parts[2])?,
                path: PathBuf::from(parts[3]),
            });
        }
        Ok(cfg)
    }

    pub fn save(&self) -> Result<()> {
        let path = Self::path()?;
        let parent = path
            .parent()
            .ok_or_else(|| Error::new("Failed to resolve config parent directory"))?;
        fs::create_dir_all(parent)?;
        let mut out = String::new();
        if let Some(default_store) = &self.default_store {
            out.push_str(&format!("default_store={default_store}\n"));
        }
        for store in &self.stores {
            out.push_str(&format!(
                "store|{}|{}|{}\n",
                store.name,
                store.backend.as_str(),
                store.path.display()
            ));
        }
        fs::write(path, out)?;
        Ok(())
    }

    pub fn upsert_store(&mut self, store: Store) {
        if let Some(existing) = self.stores.iter_mut().find(|s| s.name == store.name) {
            *existing = store;
            return;
        }
        self.stores.push(store);
    }

    pub fn get_store(&self, name: &str) -> Result<Store> {
        self.stores
            .iter()
            .find(|s| s.name == name)
            .cloned()
            .ok_or_else(|| Error::new(format!("Unknown store '{name}'")))
    }

    pub fn default_store(&self) -> Result<Store> {
        let default_name = self
            .default_store
            .as_deref()
            .ok_or_else(|| Error::new("No default store configured"))?;
        self.get_store(default_name)
    }
}

pub fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    Ok(())
}
