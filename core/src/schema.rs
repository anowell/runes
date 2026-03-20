use crate::{Error, Result};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

// Built-in kind template (content after "# {title}\n\n")
const BUILTIN_TEMPLATE: &str = "## Description\n\n\n\n## Acceptance\n\n- [ ] ...\n";

/// A field definition from the schema: either status or a custom property.
#[derive(Clone, Debug)]
pub struct FieldDef {
    pub name: String,
    /// Allowed values. Empty means any string value is accepted.
    pub values: Vec<String>,
    /// Whether this field is optional. Default is false (i.e. required).
    pub optional: bool,
}

/// Per-kind overrides declared in the schema.
#[derive(Clone, Debug, Default)]
pub struct KindDef {
    /// Field overrides for this kind (keyed by field name).
    pub fields: HashMap<String, FieldDef>,
}

/// Per-kind terminal status declarations.
#[derive(Clone, Debug, Default)]
pub struct KindTerminals {
    /// Statuses considered terminal/finished. Empty means use schema-level terminals.
    pub terminal: Vec<String>,
}

/// The store/project schema loaded from `.kinds/schema.kdl`.
#[derive(Clone, Debug)]
pub struct StoreSchema {
    /// Global allowed statuses. Empty means any status is valid.
    pub statuses: Vec<String>,
    /// Global terminal statuses. If empty, defaults to last status in list.
    pub terminal: Vec<String>,
    /// Global custom field definitions (keyed by field name).
    pub fields: HashMap<String, FieldDef>,
    /// Per-kind overrides.
    pub kinds: HashMap<String, KindDef>,
    /// Per-kind terminal status declarations.
    pub kind_terminals: HashMap<String, KindTerminals>,
    /// The directory from which the schema was loaded (for resolving templates).
    pub kinds_dir: PathBuf,
}

const DEFAULT_SCHEMA: &str = include_str!("default_schema.kdl");

impl Default for StoreSchema {
    fn default() -> Self {
        parse_schema_text(DEFAULT_SCHEMA, PathBuf::new())
            .expect("built-in default schema must be valid")
    }
}

impl StoreSchema {
    /// Get the allowed statuses for a given kind. Returns kind-specific overrides
    /// if present, otherwise the global statuses.
    pub fn statuses_for_kind(&self, kind: &str) -> &[String] {
        if let Some(kind_def) = self.kinds.get(kind) {
            if let Some(status_field) = kind_def.fields.get("status") {
                if !status_field.values.is_empty() {
                    return &status_field.values;
                }
            }
        }
        &self.statuses
    }

    /// Get the terminal statuses for a given kind. Returns kind-specific terminals
    /// if declared, otherwise the global terminals. If no terminals are declared
    /// anywhere, defaults to the last status in the applicable status list.
    pub fn terminal_statuses_for_kind(&self, kind: &str) -> Vec<String> {
        // Check kind-specific terminals first
        if let Some(kt) = self.kind_terminals.get(kind) {
            if !kt.terminal.is_empty() {
                return kt.terminal.clone();
            }
        }
        // Fall back to global terminals
        if !self.terminal.is_empty() {
            return self.terminal.clone();
        }
        // Default to last status in the applicable list
        let statuses = self.statuses_for_kind(kind);
        if let Some(last) = statuses.last() {
            vec![last.clone()]
        } else {
            Vec::new()
        }
    }

    /// Check if a status is terminal for a given kind.
    pub fn is_terminal(&self, kind: &str, status: &str) -> bool {
        let terminals = self.terminal_statuses_for_kind(kind);
        terminals.iter().any(|t| t == status)
    }

    /// Validate a status value for a given kind.
    pub fn validate_status(&self, kind: &str, status: &str) -> Result<()> {
        let allowed = self.statuses_for_kind(kind);
        if allowed.is_empty() {
            return Ok(());
        }
        if allowed.iter().any(|s| s == status) {
            Ok(())
        } else {
            Err(Error::new(format!(
                "Invalid status '{}' for kind '{}'. Allowed: {}",
                status,
                kind,
                allowed.join(", ")
            )))
        }
    }

    /// Validate a kind value against known kinds.
    /// Kinds are known if they appear in the schema or have template files.
    pub fn validate_kind(&self, kind: &str) -> Result<()> {
        let known = self.available_kinds();
        if known.is_empty() {
            return Ok(());
        }
        if known.iter().any(|k| k == kind) {
            Ok(())
        } else {
            Err(Error::new(format!(
                "Invalid kind '{}'. Allowed: {}",
                kind,
                known.join(", ")
            )))
        }
    }

    /// Validate custom fields (from frontmatter_extra) against the schema.
    /// Returns errors for invalid enum values on required or present fields.
    pub fn validate_custom_fields(&self, kind: &str, extra_lines: &[String]) -> Result<()> {
        let mut errors = Vec::new();

        // Build effective field defs: global merged with kind-specific
        let mut effective_fields = self.fields.clone();
        if let Some(kind_def) = self.kinds.get(kind) {
            for (name, field) in &kind_def.fields {
                if name != "status" {
                    effective_fields.insert(name.clone(), field.clone());
                }
            }
        }

        // Parse present custom fields from extra lines
        let mut present_fields: HashMap<String, String> = HashMap::new();
        for line in extra_lines {
            let trimmed = line.trim();
            if let Some(space_idx) = trimmed.find(' ') {
                let field_name = &trimmed[..space_idx];
                let rest = &trimmed[space_idx..];
                // Extract first quoted value
                if let Some(open) = rest.find('"') {
                    if let Some(close) = rest[open + 1..].find('"') {
                        let value = &rest[open + 1..open + 1 + close];
                        present_fields.insert(field_name.to_string(), value.to_string());
                    }
                }
            } else {
                // Field with no value
                present_fields.insert(trimmed.to_string(), String::new());
            }
        }

        // Check required fields are present
        for (name, field) in &effective_fields {
            if !field.optional && !present_fields.contains_key(name) {
                errors.push(format!("Required field '{}' is missing", name));
            }
        }

        // Validate enum values for present fields
        for (name, value) in &present_fields {
            if let Some(field) = effective_fields.get(name) {
                if !field.values.is_empty() && !field.values.iter().any(|v| v == value) {
                    errors.push(format!(
                        "Invalid value '{}' for field '{}'. Allowed: {}",
                        value,
                        name,
                        field.values.join(", ")
                    ));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::new(errors.join("\n")))
        }
    }

    /// Get the list of all known kinds (from schema declarations + template files).
    pub fn available_kinds(&self) -> Vec<String> {
        let mut kinds: Vec<String> = Vec::new();

        // Built-in kinds
        for builtin in &["task", "bug", "milestone"] {
            kinds.push(builtin.to_string());
        }

        // Schema-declared kinds
        for name in self.kinds.keys() {
            if !kinds.iter().any(|k| k == name) {
                kinds.push(name.clone());
            }
        }

        // Kinds from template files
        if self.kinds_dir.is_dir() {
            if let Ok(entries) = fs::read_dir(&self.kinds_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|s| s.to_str()) == Some("md") {
                        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                            if stem != "schema" && !kinds.iter().any(|k| k == stem) {
                                kinds.push(stem.to_string());
                            }
                        }
                    }
                }
            }
        }

        kinds.sort();
        kinds
    }
}

/// Return the built-in default template used when no custom template file exists.
pub fn builtin_template() -> &'static str {
    BUILTIN_TEMPLATE
}

/// Find the path to a custom kind template file, if one exists.
/// Searches project `.kinds/` first, then store `.kinds/`.
/// Returns `None` if only built-in defaults would be used.
pub fn find_kind_template_path(
    store_path: &Path,
    project: Option<&str>,
    kind: &str,
) -> Option<PathBuf> {
    if let Some(proj) = project {
        let project_template = store_path
            .join(proj)
            .join(".kinds")
            .join(format!("{kind}.md"));
        if project_template.exists() {
            return Some(project_template);
        }
    }
    let store_template = store_path.join(".kinds").join(format!("{kind}.md"));
    if store_template.exists() {
        return Some(store_template);
    }
    None
}

/// Load the body template for a given kind.
/// Searches project `.kinds/` first, then store `.kinds/`, then built-in defaults.
/// Returns the body content (after `# {title}\n\n`).
pub fn load_kind_template(store_path: &Path, project: Option<&str>, kind: &str) -> String {
    // Search project-level first
    if let Some(proj) = project {
        let project_template = store_path
            .join(proj)
            .join(".kinds")
            .join(format!("{kind}.md"));
        if let Ok(content) = fs::read_to_string(&project_template) {
            return content;
        }
    }

    // Then store-level
    let store_template = store_path.join(".kinds").join(format!("{kind}.md"));
    if let Ok(content) = fs::read_to_string(&store_template) {
        return content;
    }

    // Fall back to built-in default
    BUILTIN_TEMPLATE.to_string()
}

/// Load the schema from `.kinds/schema.kdl`.
/// Searches project-level first, then store-level.
/// Returns default schema if no schema file is found.
pub fn load_schema(store_path: &Path, project: Option<&str>) -> Result<StoreSchema> {
    // Search project-level first
    if let Some(proj) = project {
        let project_schema = store_path.join(proj).join(".kinds").join("schema.kdl");
        if project_schema.exists() {
            return parse_schema_file(&project_schema);
        }
    }

    // Then store-level
    let store_schema = store_path.join(".kinds").join("schema.kdl");
    if store_schema.exists() {
        return parse_schema_file(&store_schema);
    }

    // Default schema
    Ok(StoreSchema::default())
}

fn parse_schema_file(path: &Path) -> Result<StoreSchema> {
    let text = fs::read_to_string(path)?;
    let kinds_dir = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    parse_schema_text(&text, kinds_dir)
}

fn parse_schema_text(text: &str, kinds_dir: PathBuf) -> Result<StoreSchema> {
    let mut schema = StoreSchema {
        statuses: Vec::new(),
        terminal: Vec::new(),
        fields: HashMap::new(),
        kinds: HashMap::new(),
        kind_terminals: HashMap::new(),
        kinds_dir,
    };

    // Simple line-by-line KDL parsing (we parse a minimal subset)
    let lines: Vec<&str> = text.lines().collect();
    let mut i = 0;
    while i < lines.len() {
        let trimmed = lines[i].trim();

        // Skip comments and empty lines
        if trimmed.is_empty() || trimmed.starts_with("//") {
            i += 1;
            continue;
        }

        if trimmed.starts_with("status ") {
            schema.statuses = extract_quoted_values(trimmed);
            i += 1;
            continue;
        }

        if trimmed.starts_with("terminal ") {
            schema.terminal = extract_quoted_values(trimmed);
            i += 1;
            continue;
        }

        if trimmed.starts_with("kind ") {
            let kind_name = extract_first_quoted_value(trimmed);
            if let Some(name) = kind_name {
                if trimmed.contains('{') {
                    // Parse kind block
                    let mut kind_def = KindDef::default();
                    let mut kind_terminal = KindTerminals::default();
                    i += 1;
                    while i < lines.len() {
                        let inner = lines[i].trim();
                        if inner == "}" {
                            break;
                        }
                        if !inner.is_empty() && !inner.starts_with("//") {
                            if inner.starts_with("terminal ") {
                                kind_terminal.terminal = extract_quoted_values(inner);
                            } else if let Some(field) = parse_field_line(inner, true) {
                                kind_def.fields.insert(field.name.clone(), field);
                            }
                        }
                        i += 1;
                    }
                    if !kind_terminal.terminal.is_empty() {
                        schema.kind_terminals.insert(name.clone(), kind_terminal);
                    }
                    schema.kinds.insert(name, kind_def);
                } else {
                    // Kind with no block - just declares the kind exists
                    schema.kinds.insert(name, KindDef::default());
                }
            }
            i += 1;
            continue;
        }

        // Any other top-level line is a global field definition
        if let Some(field) = parse_field_line(trimmed, false) {
            schema.fields.insert(field.name.clone(), field);
        }

        i += 1;
    }

    Ok(schema)
}

/// Parse a field definition line. Used for both global fields and kind-block fields.
/// `inside_kind` indicates whether this is inside a `kind` block (where `status` is
/// a valid field override) or at the top level (where `status` is handled separately).
fn parse_field_line(line: &str, inside_kind: bool) -> Option<FieldDef> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with("//") {
        return None;
    }

    let mut parts = trimmed.split_whitespace();
    let name = parts.next()?;

    // `kind` and `terminal` are never field definitions
    if name == "kind" || name == "terminal" {
        return None;
    }

    // `status` is only a field def inside kind blocks (where it overrides the global statuses)
    if name == "status" && !inside_kind {
        return None;
    }

    let values = extract_quoted_values(trimmed);
    let optional = is_optional(trimmed);

    Some(FieldDef {
        name: name.to_string(),
        values,
        optional,
    })
}

/// Check if a line contains `optional` or `optional=#true`
fn is_optional(line: &str) -> bool {
    // Check for `optional=#true` property
    if line.contains("optional=#true") || line.contains("optional=true") {
        return true;
    }
    // Check for bare `optional` word (not inside quotes)
    let mut in_quote = false;
    for token in line.split_whitespace() {
        if token.starts_with('"') {
            in_quote = true;
        }
        if in_quote && token.ends_with('"') {
            in_quote = false;
            continue;
        }
        if !in_quote && token == "optional" {
            return true;
        }
    }
    false
}

fn extract_quoted_values(line: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut start = 0usize;
    while let Some(open_rel) = line[start..].find('"') {
        let open = start + open_rel + 1;
        if let Some(close_rel) = line[open..].find('"') {
            let close = open + close_rel;
            values.push(line[open..close].to_string());
            start = close + 1;
        } else {
            break;
        }
    }
    values
}

fn extract_first_quoted_value(line: &str) -> Option<String> {
    extract_quoted_values(line).into_iter().next()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_default_schema() {
        let schema = StoreSchema::default();
        assert_eq!(schema.statuses, vec!["todo", "in-progress", "done"]);
        assert!(schema.fields.is_empty());
        // Default schema declares task, bug, milestone kinds
        assert!(schema.kinds.contains_key("task"));
        assert!(schema.kinds.contains_key("bug"));
        assert!(schema.kinds.contains_key("milestone"));
        // All use global statuses by default
        assert_eq!(
            schema.statuses_for_kind("task"),
            &["todo", "in-progress", "done"]
        );
        assert_eq!(
            schema.statuses_for_kind("milestone"),
            &["todo", "in-progress", "done"]
        );
    }

    #[test]
    fn parse_schema_with_statuses() {
        let text = r#"status "open" "closed""#;
        let schema = parse_schema_text(text, PathBuf::new()).unwrap();
        assert_eq!(schema.statuses, vec!["open", "closed"]);
    }

    #[test]
    fn parse_schema_with_kinds() {
        let text = r#"
status "todo" "in-progress" "done"

kind "task"

kind "bug" {
    status "open" "investigating" "resolved" "closed"
    severity "low" "medium" "high" optional=#true
}
"#;
        let schema = parse_schema_text(text, PathBuf::new()).unwrap();
        assert_eq!(schema.statuses, vec!["todo", "in-progress", "done"]);
        assert!(schema.kinds.contains_key("task"));
        assert!(schema.kinds.contains_key("bug"));

        let bug = &schema.kinds["bug"];
        let bug_statuses = &bug.fields["status"];
        assert_eq!(
            bug_statuses.values,
            vec!["open", "investigating", "resolved", "closed"]
        );
        let severity = &bug.fields["severity"];
        assert_eq!(severity.values, vec!["low", "medium", "high"]);
        assert!(severity.optional);
    }

    #[test]
    fn parse_schema_with_global_fields() {
        let text = r#"
status "todo" "done"
component
priority "low" "medium" "high" optional
"#;
        let schema = parse_schema_text(text, PathBuf::new()).unwrap();
        let component = &schema.fields["component"];
        assert!(!component.optional);
        assert!(component.values.is_empty());

        let priority = &schema.fields["priority"];
        assert!(priority.optional);
        assert_eq!(priority.values, vec!["low", "medium", "high"]);
    }

    #[test]
    fn validate_status_global() {
        let schema = StoreSchema::default();
        assert!(schema.validate_status("task", "todo").is_ok());
        assert!(schema.validate_status("task", "invalid").is_err());
    }

    #[test]
    fn validate_status_kind_override() {
        let text = r#"
status "todo" "done"
kind "bug" {
    status "open" "closed"
}
"#;
        let schema = parse_schema_text(text, PathBuf::new()).unwrap();
        // bug uses its own statuses
        assert!(schema.validate_status("bug", "open").is_ok());
        assert!(schema.validate_status("bug", "todo").is_err());
        // task uses global statuses
        assert!(schema.validate_status("task", "todo").is_ok());
        assert!(schema.validate_status("task", "open").is_err());
    }

    #[test]
    fn statuses_for_unknown_kind_uses_global() {
        let schema = StoreSchema::default();
        let statuses = schema.statuses_for_kind("unknown");
        assert_eq!(statuses, &["todo", "in-progress", "done"]);
    }

    #[test]
    fn builtin_templates() {
        let task = load_kind_template(Path::new("/nonexistent"), None, "task");
        assert!(task.contains("## Description"));
        assert!(task.contains("## Acceptance"));

        let bug = load_kind_template(Path::new("/nonexistent"), None, "bug");
        assert!(bug.contains("## Description"));
        assert!(bug.contains("## Acceptance"));

        let milestone = load_kind_template(Path::new("/nonexistent"), None, "milestone");
        assert!(milestone.contains("## Description"));
        assert!(milestone.contains("## Acceptance"));

        // Unknown kinds also use the same builtin template
        let custom = load_kind_template(Path::new("/nonexistent"), None, "custom");
        assert!(custom.contains("## Description"));
        assert!(custom.contains("## Acceptance"));
    }

    #[test]
    fn available_kinds_includes_builtins() {
        let schema = StoreSchema::default();
        let kinds = schema.available_kinds();
        assert!(kinds.contains(&"task".to_string()));
        assert!(kinds.contains(&"bug".to_string()));
        assert!(kinds.contains(&"milestone".to_string()));
    }

    #[test]
    fn available_kinds_includes_schema_declared() {
        let text = r#"
kind "feature"
kind "task"
"#;
        let schema = parse_schema_text(text, PathBuf::new()).unwrap();
        let kinds = schema.available_kinds();
        assert!(kinds.contains(&"feature".to_string()));
        assert!(kinds.contains(&"task".to_string()));
    }

    #[test]
    fn validate_kind_rejects_unknown() {
        let text = r#"kind "task""#;
        let schema = parse_schema_text(text, PathBuf::new()).unwrap();
        assert!(schema.validate_kind("task").is_ok());
        assert!(schema.validate_kind("unknown").is_err());
    }

    #[test]
    fn validate_custom_fields_required() {
        let text = r#"
component
"#;
        let schema = parse_schema_text(text, PathBuf::new()).unwrap();
        // Missing required field
        let result = schema.validate_custom_fields("task", &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("component"));

        // Present required field
        let result = schema.validate_custom_fields("task", &["component \"backend\"".to_string()]);
        assert!(result.is_ok());
    }

    #[test]
    fn validate_custom_fields_enum() {
        let text = r#"
priority "low" "medium" "high" optional
"#;
        let schema = parse_schema_text(text, PathBuf::new()).unwrap();
        // Valid value
        let result = schema.validate_custom_fields("task", &["priority \"medium\"".to_string()]);
        assert!(result.is_ok());

        // Invalid value
        let result = schema.validate_custom_fields("task", &["priority \"critical\"".to_string()]);
        assert!(result.is_err());

        // Missing optional field is OK
        let result = schema.validate_custom_fields("task", &[]);
        assert!(result.is_ok());
    }

    #[test]
    fn is_optional_detection() {
        assert!(is_optional(r#"priority "low" "high" optional"#));
        assert!(is_optional(r#"severity "low" "high" optional=#true"#));
        assert!(!is_optional(r#"component"#));
        // "optional" inside quotes should not trigger
        assert!(!is_optional(r#"field "optional" "required""#));
    }

    #[test]
    fn terminal_defaults_to_last_status() {
        let text = r#"status "todo" "in-progress" "done""#;
        let schema = parse_schema_text(text, PathBuf::new()).unwrap();
        assert_eq!(schema.terminal_statuses_for_kind("task"), vec!["done"]);
        assert!(schema.is_terminal("task", "done"));
        assert!(!schema.is_terminal("task", "todo"));
    }

    #[test]
    fn terminal_explicit_global() {
        let text = r#"
status "todo" "in-progress" "done"
terminal "done"
"#;
        let schema = parse_schema_text(text, PathBuf::new()).unwrap();
        assert_eq!(schema.terminal_statuses_for_kind("task"), vec!["done"]);
        assert!(schema.is_terminal("task", "done"));
    }

    #[test]
    fn terminal_per_kind() {
        let text = r#"
status "todo" "in-progress" "done"
terminal "done"

kind "bug" {
    status "open" "investigating" "resolved" "closed"
    terminal "resolved" "closed"
}
"#;
        let schema = parse_schema_text(text, PathBuf::new()).unwrap();
        // Bug uses kind-specific terminals
        assert_eq!(
            schema.terminal_statuses_for_kind("bug"),
            vec!["resolved", "closed"]
        );
        assert!(schema.is_terminal("bug", "resolved"));
        assert!(schema.is_terminal("bug", "closed"));
        assert!(!schema.is_terminal("bug", "open"));
        // Task falls back to global terminal
        assert_eq!(schema.terminal_statuses_for_kind("task"), vec!["done"]);
    }

    #[test]
    fn terminal_omitted_defaults_to_last_kind_status() {
        let text = r#"
status "todo" "done"

kind "bug" {
    status "open" "investigating" "resolved" "closed"
}
"#;
        let schema = parse_schema_text(text, PathBuf::new()).unwrap();
        // Bug has no terminal declared, no global terminal, defaults to last in its status list
        assert_eq!(schema.terminal_statuses_for_kind("bug"), vec!["closed"]);
        // Task defaults to last in global list
        assert_eq!(schema.terminal_statuses_for_kind("task"), vec!["done"]);
    }
}
