use crate::{Error, Result};
use kdl::{KdlDocument, KdlEntry, KdlNode, KdlValue};
use runes_core::identity::UserFormat;
use runes_core::state::StateConfig;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default)]
pub struct UserConfig {
    pub identity_email: Option<String>,
    pub identity_name: Option<String>,
    /// `user.format`: how users render in human-facing output.
    pub identity_format: Option<String>,
    pub default_store: Option<String>,
    pub default_query: Option<String>,
    pub default_project: Option<String>,
    pub creation_defaults: HashMap<String, CreationDefaults>,
    pub creation_fallback: CreationDefaults,
    /// `attribution.detect`: automatic agent detection for commit authorship (default on).
    pub attribution_detect: Option<bool>,
    pub(crate) path_entries: Vec<PathEntry>,
    pub(crate) stores: Vec<StoreDefinition>,
    /// Allowed substates, keyed by core state.
    pub(crate) substates: HashMap<String, Vec<String>>,
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

#[derive(Debug, Clone)]
pub struct StoreDefinition {
    pub name: String,
    pub backend: String,
    pub path: String,
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
                // New format nodes
                "user" => {
                    if let Some(email) = value_string(node, "email") {
                        config.identity_email = Some(email);
                    }
                    if let Some(name) = value_string(node, "name") {
                        config.identity_name = Some(name);
                    }
                    if let Some(format) = value_string(node, "format") {
                        config.identity_format = Some(format);
                    }
                }
                "attribution" => {
                    if let Some(detect) = value_string(node, "detect") {
                        config.attribution_detect = Some(parse_bool(&detect));
                    }
                }
                "defaults" => {
                    if let Some(store) = value_string(node, "store") {
                        config.default_store = Some(store);
                    }
                    if let Some(project) = value_string(node, "project") {
                        config.default_project = Some(project);
                    }
                    if let Some(query) = value_string(node, "query") {
                        config.default_query = Some(query);
                    }
                }
                "new" => {
                    let defaults = parse_creation_node(node);
                    if let Some(kind_arg) = first_value(node) {
                        config.creation_defaults.insert(kind_arg, defaults);
                    } else {
                        config.creation_fallback = defaults;
                    }
                }
                "state" => {
                    if let Some(core) = first_value(node) {
                        let substates = split_values(collect_property_values(node, "substate"));
                        config.substates.insert(core, substates);
                    }
                }
                "store" => {
                    if let Some(name) = first_value(node) {
                        let backend = value_string(node, "backend").unwrap_or_default();
                        let path = value_string(node, "path").unwrap_or_default();
                        config.stores.push(StoreDefinition {
                            name,
                            backend,
                            path,
                        });
                    }
                }
                // Old format nodes (backward compat)
                "identity" => {
                    if let Some(val) = value_string(node, "default_store") {
                        config.default_store = Some(val);
                    }
                    if let Some(user) = value_string(node, "user") {
                        config.identity_email = Some(user);
                    }
                }
                "default_query" => {
                    if let Some(name) = first_value(node) {
                        config.default_query = Some(name);
                    }
                }
                "default_project" => {
                    if let Some(value) = first_value(node) {
                        let trimmed = value.trim();
                        if !trimmed.is_empty() {
                            config.default_project = Some(trimmed.to_string());
                        }
                    }
                }
                "creation" => {
                    let defaults = parse_old_creation_node(node);
                    let kind = defaults.kind.clone();
                    if let Some(kind_name) = kind {
                        config.creation_defaults.insert(kind_name, defaults);
                    } else {
                        config.creation_fallback = defaults;
                    }
                }
                // Shared nodes
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
                _ => {}
            }
        }
        Ok(config)
    }

    fn merge(&mut self, other: UserConfig) {
        let UserConfig {
            identity_email,
            identity_name,
            identity_format,
            default_store,
            default_query,
            default_project,
            creation_defaults,
            creation_fallback,
            attribution_detect,
            path_entries,
            stores,
            substates,
        } = other;
        if let Some(detect) = attribution_detect {
            self.attribution_detect = Some(detect);
        }
        if let Some(user) = identity_email {
            self.identity_email = Some(user);
        }
        if let Some(name) = identity_name {
            self.identity_name = Some(name);
        }
        if let Some(format) = identity_format {
            self.identity_format = Some(format);
        }
        if let Some(store) = default_store {
            self.default_store = Some(store);
        }
        if let Some(query) = default_query {
            self.default_query = Some(query);
        }
        if let Some(project) = default_project {
            self.default_project = Some(project);
        }
        // New-format creation defaults: override per kind
        for (kind, defaults) in creation_defaults {
            self.creation_defaults.insert(kind, defaults);
        }
        let CreationDefaults {
            kind,
            status,
            assignee,
            labels,
        } = creation_fallback;
        if kind.is_some() || status.is_some() || assignee.is_some() || !labels.is_empty() {
            if let Some(kind_value) = kind {
                self.creation_fallback.kind = Some(kind_value);
            }
            if let Some(status_value) = status {
                self.creation_fallback.status = Some(status_value);
            }
            if let Some(assignee_value) = assignee {
                self.creation_fallback.assignee = Some(assignee_value);
            }
            if !labels.is_empty() {
                self.creation_fallback.labels = labels;
            }
        }
        self.path_entries.extend(path_entries);
        for (core, values) in substates {
            self.substates.insert(core, values);
        }
        // Stores: later ones override by name
        for store in stores {
            if let Some(existing) = self.stores.iter_mut().find(|s| s.name == store.name) {
                *existing = store;
            } else {
                self.stores.push(store);
            }
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

    /// Core states with the substates this config allows.
    pub fn state_config(&self) -> Result<StateConfig> {
        let mut states = StateConfig::default();
        for (core, substates) in &self.substates {
            states.set_substates(core, substates.clone())?;
        }
        Ok(states)
    }

    /// Backward-compat: returns the "default" creation defaults
    /// (first kind-specific if there's exactly one, else fallback).
    pub fn creation_defaults(&self) -> CreationDefaults {
        if self.creation_defaults.len() == 1 {
            let (kind, defaults) = self.creation_defaults.iter().next().unwrap();
            let mut merged = defaults.clone();
            if merged.kind.is_none() {
                merged.kind = Some(kind.clone());
            }
            // Also merge fallback values
            if merged.status.is_none() {
                merged.status = self.creation_fallback.status.clone();
            }
            if merged.assignee.is_none() {
                merged.assignee = self.creation_fallback.assignee.clone();
            }
            if merged.labels.is_empty() {
                merged.labels = self.creation_fallback.labels.clone();
            }
            return merged;
        }
        self.creation_fallback.clone()
    }

    /// Whether commit authorship may fall back to a detected AI agent identity.
    pub fn attribution_detect(&self) -> bool {
        self.attribution_detect.unwrap_or(true)
    }

    #[allow(dead_code)]
    pub fn identity_email(&self) -> Option<&str> {
        self.identity_email.as_deref()
    }

    /// How users render in human-facing output. Defaults to the full name.
    pub fn user_format(&self) -> Result<UserFormat> {
        match &self.identity_format {
            Some(value) => UserFormat::parse(value),
            None => Ok(UserFormat::default()),
        }
    }

    pub fn resolve_user_alias(&self, value: &str) -> Option<String> {
        if value.eq_ignore_ascii_case("self") {
            self.identity_email.clone()
        } else {
            Some(value.to_string())
        }
    }
}

// --- Config get/set/list/unset operations ---

pub fn global_config_path() -> Result<PathBuf> {
    let home = home_dir()?;
    Ok(home.join(".runes").join("config.kdl"))
}

pub fn local_config_path(start: &Path) -> Option<PathBuf> {
    let root = find_repo_root(start)?;
    // A repo root at $HOME (say, a dotfiles repo) would make the global config
    // double as a local one.
    if let Ok(home) = home_dir() {
        if paths_equal(&root, &home) {
            return None;
        }
    }
    Some(root.join(".runes").join("config.kdl"))
}

/// Equality that survives symlinked ancestors (`/var` vs `/private/var`), which
/// otherwise defeat the $HOME comparisons here.
fn paths_equal(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// Create `dir` (a repo-local `.runes/`) so it ignores itself: a `.gitignore`
/// honored by git and jj, and an `.ignore` for pijul. Nothing about runes may
/// land in the enclosing repo's history.
pub fn ensure_self_ignoring_dir(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir)?;
    for name in [".gitignore", ".ignore"] {
        let marker = dir.join(name);
        if !marker.exists() {
            fs::write(&marker, "*\n")?;
        }
    }
    Ok(())
}

pub fn config_list(path: &Path) -> Result<Vec<(String, String)>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text = fs::read_to_string(path)?;
    let doc = text
        .parse::<KdlDocument>()
        .map_err(|e| Error::new(format!("Failed to parse {}: {e}", path.display())))?;
    let mut pairs = Vec::new();
    for node in doc.nodes() {
        match node.name().value() {
            "user" => {
                if let Some(v) = value_string(node, "email") {
                    pairs.push(("user.email".to_string(), v));
                }
                if let Some(v) = value_string(node, "name") {
                    pairs.push(("user.name".to_string(), v));
                }
                if let Some(v) = value_string(node, "format") {
                    pairs.push(("user.format".to_string(), v));
                }
            }
            "attribution" => {
                if let Some(v) = value_string(node, "detect") {
                    pairs.push(("attribution.detect".to_string(), v));
                }
            }
            "defaults" => {
                for key in &["store", "project", "query"] {
                    if let Some(v) = value_string(node, key) {
                        pairs.push((format!("defaults.{key}"), v));
                    }
                }
            }
            "new" => {
                let kind = first_value(node).unwrap_or_default();
                let prefix = if kind.is_empty() {
                    "new".to_string()
                } else {
                    format!("new.{kind}")
                };
                for key in &["assignee", "status"] {
                    if let Some(v) = value_string(node, key) {
                        pairs.push((format!("{prefix}.{key}"), v));
                    }
                }
                let labels = collect_property_values(node, "labels");
                if !labels.is_empty() {
                    pairs.push((format!("{prefix}.labels"), labels.join(",")));
                }
            }
            "state" => {
                if let Some(core) = first_value(node) {
                    let substates = split_values(collect_property_values(node, "substate"));
                    if !substates.is_empty() {
                        pairs.push((format!("state.{core}.substate"), substates.join(",")));
                    }
                }
            }
            "store" => {
                if let Some(name) = first_value(node) {
                    let prefix = format!("store.{name}");
                    if let Some(v) = value_string(node, "backend") {
                        pairs.push((format!("{prefix}.backend"), v));
                    }
                    if let Some(v) = value_string(node, "path") {
                        pairs.push((format!("{prefix}.path"), v));
                    }
                }
            }
            // old format
            "identity" => {
                if let Some(v) = value_string(node, "user") {
                    pairs.push(("user.email".to_string(), v));
                }
                if let Some(v) = value_string(node, "default_store") {
                    pairs.push(("defaults.store".to_string(), v));
                }
            }
            "default_query" => {
                if let Some(v) = first_value(node) {
                    pairs.push(("defaults.query".to_string(), v));
                }
            }
            "default_project" => {
                if let Some(v) = first_value(node) {
                    pairs.push(("defaults.project".to_string(), v));
                }
            }
            "creation" => {
                let kind = value_string(node, "type").unwrap_or_default();
                let prefix = if kind.is_empty() {
                    "new".to_string()
                } else {
                    format!("new.{kind}")
                };
                if let Some(v) = value_string(node, "assignee") {
                    pairs.push((format!("{prefix}.assignee"), v));
                }
                if let Some(v) = value_string(node, "status") {
                    pairs.push((format!("{prefix}.status"), v));
                }
            }
            "path" => {
                if let Some(p) = first_value(node) {
                    let mut desc = p.clone();
                    if let Some(s) = value_string(node, "store") {
                        desc = format!("{desc} store={s}");
                    }
                    if let Some(q) = value_string(node, "query") {
                        desc = format!("{desc} query={q}");
                    }
                    pairs.push(("path".to_string(), desc));
                }
            }
            _ => {}
        }
    }
    Ok(pairs)
}

pub fn config_get(path: &Path, key: &str) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }
    let pairs = config_list(path)?;
    for (k, v) in pairs {
        if k == key {
            return Ok(Some(v));
        }
    }
    Ok(None)
}

/// Parse a dotted key like "user.email" or "query.mine.status"
/// Returns (node_name, node_arg, property_name)
fn parse_config_key(key: &str) -> Result<(&str, Option<&str>, &str)> {
    let parts: Vec<&str> = key.splitn(3, '.').collect();
    match parts.len() {
        2 => Ok((parts[0], None, parts[1])),
        3 => Ok((parts[0], Some(parts[1]), parts[2])),
        _ => Err(Error::new(format!("Invalid config key '{key}'"))),
    }
}

pub fn config_set(path: &Path, key: &str, value: &str) -> Result<()> {
    let (node_name, node_arg, prop) = parse_config_key(key)?;
    let mut doc = load_or_empty_doc(path)?;
    let node = find_or_create_node(&mut doc, node_name, node_arg);
    set_child_value(node, prop, value);
    write_doc(path, &doc)
}

pub fn config_unset(path: &Path, key: &str) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let (node_name, node_arg, prop) = parse_config_key(key)?;
    let text = fs::read_to_string(path)?;
    let mut doc = text
        .parse::<KdlDocument>()
        .map_err(|e| Error::new(format!("Failed to parse {}: {e}", path.display())))?;
    if let Some(node) = find_node_mut(&mut doc, node_name, node_arg) {
        remove_child_value(node, prop);
    }
    write_doc(path, &doc)
}

fn load_or_empty_doc(path: &Path) -> Result<KdlDocument> {
    if path.exists() {
        let text = fs::read_to_string(path)?;
        text.parse::<KdlDocument>()
            .map_err(|e| Error::new(format!("Failed to parse {}: {e}", path.display())))
    } else {
        Ok(KdlDocument::new())
    }
}

fn find_node_mut<'a>(
    doc: &'a mut KdlDocument,
    name: &str,
    arg: Option<&str>,
) -> Option<&'a mut KdlNode> {
    doc.nodes_mut().iter_mut().find(|n| {
        if n.name().value() != name {
            return false;
        }
        match arg {
            Some(expected) => first_value_ref(n) == Some(expected),
            None => first_value_ref(n).is_none(),
        }
    })
}

fn first_value_ref(node: &KdlNode) -> Option<&str> {
    for entry in node.entries() {
        if entry.name().is_none() {
            return entry.value().as_string();
        }
    }
    None
}

fn find_or_create_node<'a>(
    doc: &'a mut KdlDocument,
    name: &str,
    arg: Option<&str>,
) -> &'a mut KdlNode {
    let idx = doc.nodes().iter().position(|n| {
        if n.name().value() != name {
            return false;
        }
        match arg {
            Some(expected) => first_value_ref(n) == Some(expected),
            None => first_value_ref(n).is_none(),
        }
    });
    if let Some(i) = idx {
        &mut doc.nodes_mut()[i]
    } else {
        let mut node = KdlNode::new(name);
        if let Some(arg_val) = arg {
            node.push(KdlEntry::new(KdlValue::String(arg_val.to_string())));
        }
        doc.nodes_mut().push(node);
        doc.nodes_mut().last_mut().unwrap()
    }
}

fn set_child_value(node: &mut KdlNode, prop: &str, value: &str) {
    let children = node.children_mut().get_or_insert_with(KdlDocument::new);
    // Find existing child node with this name
    if let Some(child) = children
        .nodes_mut()
        .iter_mut()
        .find(|n| n.name().value() == prop)
    {
        // Replace all entries with new value
        child.entries_mut().clear();
        child.push(KdlEntry::new(KdlValue::String(value.to_string())));
    } else {
        let mut child = KdlNode::new(prop);
        child.push(KdlEntry::new(KdlValue::String(value.to_string())));
        children.nodes_mut().push(child);
    }
}

fn remove_child_value(node: &mut KdlNode, prop: &str) {
    if let Some(children) = node.children_mut().as_mut() {
        children.nodes_mut().retain(|n| n.name().value() != prop);
    }
}

fn write_doc(path: &Path, doc: &KdlDocument) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut formatted = doc.clone();
    formatted.autoformat();
    fs::write(path, formatted.to_string())?;
    Ok(())
}

// --- Parsing helpers ---

fn parse_creation_node(node: &KdlNode) -> CreationDefaults {
    CreationDefaults {
        kind: first_value(node),
        status: value_string(node, "status"),
        assignee: value_string(node, "assignee"),
        labels: collect_property_values(node, "labels"),
    }
}

fn parse_old_creation_node(node: &KdlNode) -> CreationDefaults {
    CreationDefaults {
        kind: value_string(node, "type"),
        status: value_string(node, "status"),
        assignee: value_string(node, "assignee"),
        labels: collect_property_values(node, "labels"),
    }
}

fn parse_bool(value: &str) -> bool {
    value.eq_ignore_ascii_case("true") || value == "1"
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

/// Split comma-separated values, which is how `runes config set` writes lists.
fn split_values(values: Vec<String>) -> Vec<String> {
    values
        .iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(String::from)
        .collect()
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

pub fn find_repo_root(start: &Path) -> Option<PathBuf> {
    // `~/.runes` holds global state, so it must not make $HOME a repo root.
    let home = home_dir().ok();
    let mut cursor = start.to_path_buf();
    loop {
        let runes_dir = cursor.join(".runes").is_dir()
            && home
                .as_deref()
                .is_none_or(|home| !paths_equal(&cursor, home));
        if runes_dir
            || cursor.join(".git").exists()
            || cursor.join(".jj").exists()
            || cursor.join(".pijul").exists()
        {
            return Some(cursor);
        }
        if !cursor.pop() {
            return None;
        }
    }
}

fn find_config_paths(start: &Path) -> Result<Vec<PathBuf>> {
    let mut config_paths = Vec::new();
    let global = global_config_path()?;
    if global.exists() {
        config_paths.push(global);
    }
    if let Some(local) = local_config_path(start) {
        if local.exists() {
            config_paths.push(local);
        }
    }
    Ok(config_paths)
}
