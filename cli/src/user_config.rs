use crate::{Error, Result};
use kdl::{KdlDocument, KdlNode};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default)]
pub struct UserConfig {
    pub identity_user: Option<String>,
    pub default_store: Option<String>,
    pub default_query: Option<String>,
    pub creation_defaults: CreationDefaults,
    pub(crate) path_entries: Vec<PathEntry>,
    pub(crate) queries: HashMap<String, QueryDefinition>,
}

#[derive(Debug, Clone)]
pub(crate) struct PathEntry {
    path: PathBuf,
    store: Option<String>,
    query: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct CreationDefaults {
    pub kind: Option<String>,
    pub status: Option<String>,
    pub assignee: Option<String>,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryDefinition {
    pub project: Option<String>,
    pub statuses: Vec<String>,
    pub kind: Option<String>,
    pub archived: Option<String>,
    pub assignee: Option<String>,
}

impl UserConfig {
    #[allow(dead_code)]
    pub fn load() -> Result<Self> {
        let cwd = env::current_dir().map_err(|e| Error::new(e.to_string()))?;
        Self::load_from_dir(&cwd)
    }

    pub fn load_from_dir(start: &Path) -> Result<Self> {
        let mut config = UserConfig::default();
        for path in find_config_paths(start)? {
            let text = fs::read_to_string(&path)?;
            let doc = text
                .parse::<KdlDocument>()
                .map_err(|e| Error::new(format!("Failed to parse {}: {e}", path.display())))?;
            let file_dir = path.parent().unwrap_or_else(|| Path::new("/"));
            config.merge(UserConfig::from_document(&doc, file_dir)?);
        }
        Ok(config)
    }

    fn from_document(doc: &KdlDocument, base_dir: &Path) -> Result<Self> {
        let mut config = UserConfig::default();
        for node in doc.nodes() {
            match node.name().value() {
                "identity" => {
                    if let Some(val) = value_string(node, "default_store") {
                        config.default_store = Some(val);
                    }
                    if let Some(user) = value_string(node, "user") {
                        config.identity_user = Some(user);
                    }
                }
                "default_query" => {
                    if let Some(name) = first_value(node) {
                        config.default_query = Some(name);
                    }
                }
                "path" => {
                    if let Some(path_value) = first_value(node) {
                        let resolved = resolve_path(&path_value, base_dir)?;
                        let store = value_string(node, "store");
                        let query = value_string(node, "query");
                        config.path_entries.push(PathEntry {
                            path: resolved,
                            store,
                            query,
                        });
                    }
                }
                name if name.starts_with("queries.") => {
                    let alias = name.trim_start_matches("queries.");
                    let mut query = QueryDefinition::default();
                    if let Some(project) = value_string(node, "project") {
                        query.project = Some(project);
                    }
                    let statuses = collect_property_values(node, "status");
                    if !statuses.is_empty() {
                        query.statuses = statuses;
                    }
                    if let Some(kind) = value_string(node, "kind") {
                        query.kind = Some(kind);
                    }
                    if let Some(archived) = value_string(node, "archived") {
                        query.archived = Some(archived);
                    }
                    if let Some(assignee) = value_string(node, "assignee") {
                        query.assignee = Some(assignee);
                    }
                    config.queries.insert(alias.to_string(), query);
                }
                "creation" => {
                    let mut defaults = CreationDefaults::default();
                    if let Some(kind) = value_string(node, "type") {
                        defaults.kind = Some(kind);
                    }
                    if let Some(status) = value_string(node, "status") {
                        defaults.status = Some(status);
                    }
                    if let Some(assignee) = value_string(node, "assignee") {
                        defaults.assignee = Some(assignee);
                    }
                    let labels = collect_label_values(node);
                    if !labels.is_empty() {
                        defaults.labels = labels;
                    }
                    config.creation_defaults = defaults;
                }
                _ => {}
            }
        }
        Ok(config)
    }

    fn merge(&mut self, other: UserConfig) {
        let UserConfig {
            identity_user,
            default_store,
            default_query,
            creation_defaults,
            path_entries,
            queries,
        } = other;
        if let Some(user) = identity_user {
            self.identity_user = Some(user);
        }
        if let Some(store) = default_store {
            self.default_store = Some(store);
        }
        if let Some(query) = default_query {
            self.default_query = Some(query);
        }
        let CreationDefaults {
            kind,
            status,
            assignee,
            labels,
        } = creation_defaults;
        if let Some(kind_value) = kind {
            self.creation_defaults.kind = Some(kind_value);
        }
        if let Some(status_value) = status {
            self.creation_defaults.status = Some(status_value);
        }
        if let Some(assignee_value) = assignee {
            self.creation_defaults.assignee = Some(assignee_value);
        }
        if !labels.is_empty() {
            self.creation_defaults.labels = labels;
        }
        self.path_entries.extend(path_entries);
        for (name, query) in queries {
            self.queries.insert(name, query);
        }
    }

    pub fn store_for_path(&self, path: &Path) -> Option<String> {
        for entry in self.path_entries.iter().rev() {
            if path.starts_with(&entry.path) {
                if let Some(store) = &entry.store {
                    return Some(store.clone());
                }
            }
        }
        None
    }

    pub fn query_for_path(&self, path: &Path) -> Option<String> {
        for entry in self.path_entries.iter().rev() {
            if path.starts_with(&entry.path) {
                if let Some(query) = &entry.query {
                    return Some(query.clone());
                }
            }
        }
        None
    }

    pub fn query(&self, name: &str) -> Option<&QueryDefinition> {
        self.queries.get(name)
    }

    pub fn creation_defaults(&self) -> &CreationDefaults {
        &self.creation_defaults
    }

    #[allow(dead_code)]
    pub fn identity_user(&self) -> Option<&str> {
        self.identity_user.as_deref()
    }

    pub fn resolve_user_alias(&self, value: &str) -> Option<String> {
        if value.eq_ignore_ascii_case("self") {
            self.identity_user.clone()
        } else {
            Some(value.to_string())
        }
    }
}

fn value_string(node: &KdlNode, name: &str) -> Option<String> {
    for entry in node.entries() {
        if let Some(key) = entry.name() {
            if key.value() == name {
                if let Some(value) = entry.value().as_string() {
                    return Some(value.to_string());
                }
            }
        }
    }
    collect_child_value(node, name)
}

fn first_value(node: &KdlNode) -> Option<String> {
    for entry in node.entries() {
        if entry.name().is_none() {
            if let Some(value) = entry.value().as_string() {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn collect_label_values(node: &KdlNode) -> Vec<String> {
    collect_property_values(node, "labels")
}

fn collect_property_values(node: &KdlNode, name: &str) -> Vec<String> {
    let mut values = Vec::new();
    values.extend(collect_named_entries(node, name));
    values.extend(collect_child_values(node, name));
    values
}

fn collect_named_entries(node: &KdlNode, name: &str) -> Vec<String> {
    let mut values = Vec::new();
    for entry in node.entries() {
        if let Some(key) = entry.name() {
            if key.value() == name {
                if let Some(value) = entry.value().as_string() {
                    values.push(value.to_string());
                }
            }
        }
    }
    values
}

fn collect_child_values(node: &KdlNode, name: &str) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(children) = node.children() {
        for child in children.nodes() {
            if child.name().value() == name {
                values.extend(string_entries(child));
            }
        }
    }
    values
}

fn collect_child_value(node: &KdlNode, name: &str) -> Option<String> {
    if let Some(children) = node.children() {
        for child in children.nodes() {
            if child.name().value() == name {
                if let Some(value) = first_value(child) {
                    return Some(value);
                }
            }
        }
    }
    None
}

fn string_entries(node: &KdlNode) -> Vec<String> {
    let mut values = Vec::new();
    for entry in node.entries() {
        if let Some(value) = entry.value().as_string() {
            values.push(value.to_string());
        }
    }
    values
}

fn resolve_path(value: &str, base: &Path) -> Result<PathBuf> {
    let expanded = if value.starts_with('~') {
        let home = home_dir()?;
        let without = value.trim_start_matches('~');
        home.join(without.strip_prefix('/').unwrap_or(without))
    } else {
        PathBuf::from(value)
    };
    if expanded.is_absolute() {
        Ok(expanded)
    } else {
        Ok(base.join(expanded))
    }
}

fn home_dir() -> Result<PathBuf> {
    let home = env::var("HOME").map_err(|_| Error::new("HOME is not set"))?;
    Ok(PathBuf::from(home))
}

fn find_config_paths(start: &Path) -> Result<Vec<PathBuf>> {
    let mut config_paths = Vec::new();
    let home = home_dir()?;
    let global = home.join(".runes").join("config.kdl");
    if global.exists() {
        config_paths.push(global);
    }
    let mut dirs = Vec::new();
    let mut cursor = start.to_path_buf();
    loop {
        dirs.push(cursor.clone());
        if cursor == home {
            break;
        }
        if !cursor.pop() {
            break;
        }
    }
    dirs.reverse();
    for dir in dirs {
        let candidate = dir.join("runes.kdl");
        if candidate.exists() {
            config_paths.push(candidate);
        }
    }
    Ok(config_paths)
}
