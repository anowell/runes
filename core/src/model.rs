use crate::{Error, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const BASE32: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";

#[derive(Clone, Debug, Default)]
pub struct RuneDoc {
    pub kind: String,
    pub id: String,
    pub status: String,
    pub assignee: Option<String>,
    pub labels: Vec<String>,
    pub milestone: Option<String>,
    pub relations: Vec<(String, String)>,
    pub deps: Vec<String>,
    pub frontmatter_extra: Vec<String>,
    pub title: String,
    pub body: String,
    pub path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct ParsedId {
    pub project: String,
    pub short: String,
    pub full: String,
}

pub fn parse_full_id(id: &str) -> Result<ParsedId> {
    let (project, short) = id
        .split_once('-')
        .ok_or_else(|| Error::new(format!("Invalid id '{id}', expected <project>-<short>")))?;
    if project.is_empty() || short.is_empty() {
        return Err(Error::new(format!("Invalid id '{id}'")));
    }
    Ok(ParsedId {
        project: project.to_string(),
        short: short.to_string(),
        full: id.to_string(),
    })
}

pub fn slugify(title: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for c in title.chars() {
        let lower = c.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() {
            out.push(lower);
            prev_dash = false;
        } else if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        "untitled".to_string()
    } else {
        out
    }
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

fn find_root_line_index(fm_lines: &[String]) -> Option<usize> {
    fm_lines.iter().position(|line| !line.trim().is_empty())
}

fn parse_root_line(line: &str, doc: &mut RuneDoc) -> Result<()> {
    let trimmed = line.trim();
    let mut parts = trimmed.split_whitespace();
    let kind = parts
        .next()
        .ok_or_else(|| Error::new("frontmatter missing root node kind"))?;
    doc.kind = kind.to_string();
    if let Some(id_value) = extract_first_quoted_value(trimmed) {
        doc.id = id_value;
    } else {
        return Err(Error::new("frontmatter missing rune id"));
    }
    Ok(())
}

fn collect_block_lines(fm_lines: &[String], root_idx: usize) -> Vec<String> {
    let mut block_lines = Vec::new();
    let root_line = fm_lines.get(root_idx).map(|line| line.trim()).unwrap_or("");
    let mut depth = root_line.matches('{').count() as i32;
    depth -= root_line.matches('}').count() as i32;
    let mut collecting = depth > 0;
    for line in fm_lines.iter().skip(root_idx + 1) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !collecting {
            if trimmed.contains('{') {
                collecting = true;
                depth += trimmed.matches('{').count() as i32;
                depth -= trimmed.matches('}').count() as i32;
            }
            continue;
        }
        let opens = trimmed.matches('{').count() as i32;
        let closes = trimmed.matches('}').count() as i32;
        if trimmed == "}" && depth <= 1 {
            break;
        }
        block_lines.push(trimmed.to_string());
        depth += opens;
        depth -= closes;
    }
    block_lines
}

fn parse_block_lines(block_lines: &[String], doc: &mut RuneDoc) {
    let mut in_relations = false;
    for line in block_lines {
        if in_relations {
            if line == "}" {
                in_relations = false;
                continue;
            }
            parse_relation_line(doc, line);
            continue;
        }
        if line == "relations {" {
            in_relations = true;
            continue;
        }
        if parse_property_line(doc, line) {
            continue;
        }
        doc.frontmatter_extra.push(line.clone());
    }
}

fn parse_property_line(doc: &mut RuneDoc, trimmed: &str) -> bool {
    if trimmed.starts_with("status ") {
        if let Some(value) = extract_first_quoted_value(trimmed) {
            doc.status = value;
            return true;
        }
    }
    if trimmed.starts_with("assignee ") {
        if let Some(value) = extract_first_quoted_value(trimmed) {
            if value.eq_ignore_ascii_case("none") {
                doc.assignee = None;
            } else {
                doc.assignee = Some(value);
            }
            return true;
        }
    }
    if trimmed.starts_with("label ") || trimmed.starts_with("labels ") {
        doc.labels.extend(extract_quoted_values(trimmed));
        return true;
    }
    if trimmed.starts_with("milestone ") {
        doc.milestone = extract_quoted_values(trimmed).into_iter().next();
        return true;
    }
    if trimmed.starts_with("dep ") {
        if let Some(value) = extract_first_quoted_value(trimmed) {
            doc.deps.push(value);
            return true;
        }
    }
    if trimmed.starts_with("deps ") {
        doc.deps.extend(extract_quoted_values(trimmed));
        return true;
    }
    false
}

fn parse_relation_line(doc: &mut RuneDoc, trimmed: &str) {
    let mut parts = trimmed.split_whitespace();
    if let Some(kind) = parts.next() {
        if let Some(rest) = trimmed.strip_prefix(kind) {
            let vals = extract_quoted_values(rest);
            if let Some(id) = vals.first() {
                doc.relations.push((kind.to_string(), id.to_string()));
            }
        }
    }
}

pub fn parse_doc(path: &Path) -> Result<RuneDoc> {
    let text = fs::read_to_string(path)?;
    let mut lines = text.lines();
    let first = lines
        .next()
        .ok_or_else(|| Error::new(format!("{} is empty", path.display())))?;
    if first.trim() != "---" {
        return Err(Error::new(format!(
            "{} is missing frontmatter start '---'",
            path.display()
        )));
    }

    let mut fm_lines = Vec::new();
    let mut body_lines = Vec::new();
    let mut in_fm = true;
    for line in lines {
        if in_fm && line.trim() == "---" {
            in_fm = false;
            continue;
        }
        if in_fm {
            fm_lines.push(line.to_string());
        } else {
            body_lines.push(line.to_string());
        }
    }

    if in_fm {
        return Err(Error::new(format!(
            "{} is missing frontmatter closing '---'",
            path.display()
        )));
    }

    let mut doc = RuneDoc {
        path: path.to_path_buf(),
        status: "todo".to_string(),
        ..Default::default()
    };

    let root_idx = find_root_line_index(&fm_lines)
        .ok_or_else(|| Error::new(format!("{} frontmatter missing root node", path.display())))?;
    parse_root_line(&fm_lines[root_idx], &mut doc)?;
    let block_lines = collect_block_lines(&fm_lines, root_idx);
    parse_block_lines(&block_lines, &mut doc);

    if doc.id.is_empty() {
        return Err(Error::new(format!(
            "{} frontmatter missing rune id",
            path.display()
        )));
    }

    let mut title = String::new();
    for line in &body_lines {
        if let Some(rest) = line.strip_prefix("# ") {
            title = rest.trim().to_string();
            break;
        }
    }
    if title.is_empty() {
        title = doc.id.clone();
    }
    doc.title = title;
    doc.body = body_lines.join("\n");
    Ok(doc)
}

pub fn render_doc(doc: &RuneDoc) -> String {
    let mut out = String::new();
    out.push_str("---\n");
    out.push_str(&format!("{} \"{}\"", doc.kind, doc.id));
    out.push_str(" {\n");
    out.push_str(&format!("  status \"{}\"\n", doc.status));
    if let Some(assignee) = &doc.assignee {
        out.push_str(&format!("  assignee \"{assignee}\"\n"));
    }
    if !doc.labels.is_empty() {
        out.push_str("  labels");
        for label in &doc.labels {
            out.push_str(&format!(" \"{label}\""));
        }
        out.push('\n');
    }
    if let Some(milestone) = &doc.milestone {
        out.push_str(&format!("  milestone \"{milestone}\"\n"));
    }
    if !doc.relations.is_empty() {
        out.push_str("  relations {\n");
        for (kind, id) in &doc.relations {
            out.push_str(&format!("    {kind} \"{id}\"\n"));
        }
        out.push_str("  }\n");
    }
    for dep in &doc.deps {
        out.push_str(&format!("  dep \"{dep}\"\n"));
    }
    for line in &doc.frontmatter_extra {
        if line.is_empty() {
            out.push('\n');
        } else {
            out.push_str("  ");
            out.push_str(line);
            out.push('\n');
        }
    }
    out.push_str("}\n");
    out.push_str("---\n\n");
    out.push_str(&doc.body);
    if !doc.body.ends_with('\n') {
        out.push('\n');
    }
    out
}

/// Create a new rune doc with a specific kind and body template.
pub fn new_rune_doc(
    id: &str,
    kind: &str,
    title: &str,
    body_template: &str,
    milestone: Option<&str>,
) -> RuneDoc {
    let body = format!("# {title}\n\n{body_template}");
    RuneDoc {
        kind: kind.to_string(),
        id: id.to_string(),
        status: "todo".to_string(),
        labels: Vec::new(),
        milestone: milestone.map(|s| s.to_string()),
        relations: Vec::new(),
        deps: Vec::new(),
        frontmatter_extra: Vec::new(),
        assignee: None,
        title: title.to_string(),
        body,
        path: PathBuf::new(),
    }
}

pub fn new_milestone_doc(id: &str, title: &str, body_template: &str) -> RuneDoc {
    let body = format!("# {title}\n\n{body_template}");
    RuneDoc {
        kind: "milestone".to_string(),
        id: id.to_string(),
        status: "todo".to_string(),
        labels: Vec::new(),
        milestone: None,
        relations: Vec::new(),
        deps: Vec::new(),
        frontmatter_extra: Vec::new(),
        assignee: None,
        title: title.to_string(),
        body,
        path: PathBuf::new(),
    }
}

pub fn replace_title(body: &str, title: &str) -> String {
    let mut replaced = false;
    let mut out = Vec::new();
    for line in body.lines() {
        if !replaced && line.starts_with("# ") {
            out.push(format!("# {title}"));
            replaced = true;
        } else {
            out.push(line.to_string());
        }
    }
    if !replaced {
        out.insert(0, format!("# {title}"));
    }
    let mut joined = out.join("\n");
    if !joined.ends_with('\n') {
        joined.push('\n');
    }
    joined
}

/// Extract the title from the body's first H1 line, if any.
pub fn extract_body_title(body: &str) -> Option<String> {
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("# ") {
            let trimmed = rest.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

/// Ensure the body has an H1 title line. If missing, reinsert the given title.
/// Returns the (possibly updated) body and the effective title.
pub fn ensure_title(body: &str, original_title: &str) -> (String, String) {
    match extract_body_title(body) {
        Some(new_title) => (body.to_string(), new_title),
        None => {
            let restored = replace_title(body, original_title);
            (restored, original_title.to_string())
        }
    }
}

pub fn discover_project_docs(project_root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    walk_markdown(project_root, &mut out)?;
    Ok(out)
}

fn walk_markdown(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = entry.file_name().to_string_lossy().to_string();
        if path.is_dir() {
            if file_name == ".git"
                || file_name == ".jj"
                || file_name == ".pijul"
                || file_name == ".kinds"
            {
                continue;
            }
            walk_markdown(&path, out)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("md") {
            out.push(path);
        }
    }
    Ok(())
}

pub fn resolve_issue_path(store_path: &Path, full_id: &str) -> Result<PathBuf> {
    let parsed = parse_full_id(full_id)?;
    let project_root = store_path.join(&parsed.project);
    let docs = discover_project_docs(&project_root)?;
    let needle = format!("{}--", parsed.short);
    let mut matches = Vec::new();
    for path in docs {
        if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
            if name.starts_with('_') {
                if let Ok(doc) = parse_doc(&path) {
                    if doc.id == full_id {
                        matches.push(path);
                    }
                }
            } else if name.starts_with(&needle) {
                matches.push(path);
            }
        }
    }
    match matches.len() {
        0 => Err(Error::new(format!("No file found for id '{full_id}'"))),
        1 => Ok(matches.remove(0)),
        _ => Err(Error::new(format!(
            "Multiple files matched id '{full_id}'. Narrow scope first."
        ))),
    }
}

pub fn next_short_id(project: &str, project_root: &Path, len: usize) -> Result<String> {
    let docs = discover_project_docs(project_root)?;
    for _ in 0..500 {
        let short = random_base32(len)?;
        let prefix = format!("{project}-{short}");
        let exists = docs
            .iter()
            .any(|path| parse_doc(path).map(|d| d.id == prefix).unwrap_or(false));
        if !exists {
            return Ok(short);
        }
    }
    Err(Error::new(
        "Failed to generate unique ID after many attempts",
    ))
}

fn random_base32(len: usize) -> Result<String> {
    let mut seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::new(e.to_string()))?
        .as_nanos();
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        seed ^= seed << 7;
        seed ^= seed >> 9;
        seed ^= seed << 8;
        let idx = (seed % BASE32.len() as u128) as usize;
        out.push(BASE32[idx] as char);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn write_temp_doc(contents: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("runes-test-doc-{nanos}.md"));
        fs::write(&path, contents).expect("write temp doc");
        path
    }

    #[test]
    fn parse_new_frontmatter_nodes() {
        let contents = r#"---
	task "runes-test" {
  status "done"
  assignee "tester"
  labels "infra" "cli"
  milestone "runes-m01"
  relations {
    blocks "runes-other"
  }
  dep "runes-rf1"
  dep "runes-tnv"
}
---
# Title
Body
"#;
        let path = write_temp_doc(contents);
        let doc = parse_doc(&path).expect("parse doc");
        assert_eq!(doc.id, "runes-test");
        assert_eq!(doc.kind, "task");
        assert_eq!(doc.status, "done");
        assert_eq!(doc.assignee.as_deref(), Some("tester"));
        assert_eq!(doc.labels, vec!["infra", "cli"]);
        assert_eq!(doc.milestone.as_deref(), Some("runes-m01"));
        assert!(doc
            .relations
            .contains(&("blocks".to_string(), "runes-other".to_string())));
        let expected_deps = vec!["runes-rf1".to_string(), "runes-tnv".to_string()];
        assert_eq!(doc.deps, expected_deps);
        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn render_new_frontmatter_nodes() {
        let doc = RuneDoc {
            kind: "task".to_string(),
            id: "runes-tfc".to_string(),
            status: "todo".to_string(),
            assignee: Some("anowell@gmail.com".to_string()),
            labels: vec!["infra".to_string(), "cli".to_string()],
            milestone: Some("runes-m01".to_string()),
            relations: vec![("blocks".to_string(), "runes-rf1".to_string())],
            deps: vec!["runes-rf2".to_string()],
            frontmatter_extra: vec!["notes \"quarantine\"".to_string()],
            title: "test".to_string(),
            body: "# Title\n".to_string(),
            path: PathBuf::new(),
        };
        let rendered = render_doc(&doc);
        assert!(rendered.contains("task \"runes-tfc\""));
        assert!(rendered.contains("status \"todo\""));
        assert!(rendered.contains("assignee \"anowell@gmail.com\""));
        assert!(rendered.contains("labels \"infra\" \"cli\""));
        assert!(rendered.contains("milestone \"runes-m01\""));
        assert!(rendered.contains("relations {"));
        assert!(rendered.contains("dep \"runes-rf2\""));
        assert!(rendered.contains("notes \"quarantine\""));
    }

    #[test]
    fn extract_body_title_from_h1() {
        assert_eq!(
            extract_body_title("# My Title\n\nBody text"),
            Some("My Title".to_string())
        );
    }

    #[test]
    fn extract_body_title_none_when_missing() {
        assert_eq!(extract_body_title("No heading here\nJust body"), None);
    }

    #[test]
    fn extract_body_title_none_when_empty_h1() {
        assert_eq!(extract_body_title("# \n\nBody text"), None);
    }

    #[test]
    fn extract_body_title_uses_first_h1() {
        assert_eq!(
            extract_body_title("## Not this\n# First\n# Second"),
            Some("First".to_string())
        );
    }

    #[test]
    fn ensure_title_preserves_changed_title() {
        let body = "# New Title\n\nBody text\n";
        let (result_body, title) = ensure_title(body, "Old Title");
        assert_eq!(title, "New Title");
        assert!(result_body.contains("# New Title"));
        assert!(!result_body.contains("Old Title"));
    }

    #[test]
    fn ensure_title_restores_deleted_title() {
        let body = "Body text without heading\n";
        let (result_body, title) = ensure_title(body, "Original Title");
        assert_eq!(title, "Original Title");
        assert!(result_body.starts_with("# Original Title\n"));
        assert!(result_body.contains("Body text without heading"));
    }

    #[test]
    fn ensure_title_restores_when_h1_emptied() {
        let body = "# \n\nBody text\n";
        let (result_body, title) = ensure_title(body, "Original Title");
        assert_eq!(title, "Original Title");
        assert!(result_body.contains("# Original Title"));
    }

    #[test]
    fn replace_title_changes_existing_h1() {
        let body = "# Old Title\n\nBody\n";
        let result = replace_title(body, "New Title");
        assert!(result.contains("# New Title"));
        assert!(!result.contains("Old Title"));
        assert!(result.contains("Body"));
    }

    #[test]
    fn replace_title_inserts_h1_when_missing() {
        let body = "Body without heading\n";
        let result = replace_title(body, "Inserted Title");
        assert!(result.starts_with("# Inserted Title\n"));
        assert!(result.contains("Body without heading"));
    }

    #[test]
    fn title_roundtrip_through_parse_and_render() {
        let contents =
            "---\ntask \"proj-abc\" {\n  status \"todo\"\n}\n---\n\n# My Task\n\n## Summary\n";
        let path = write_temp_doc(contents);
        let doc = parse_doc(&path).expect("parse");
        assert_eq!(doc.title, "My Task");
        let rendered = render_doc(&doc);
        let path2 = write_temp_doc(&rendered);
        let doc2 = parse_doc(&path2).expect("re-parse");
        assert_eq!(doc2.title, "My Task");
        fs::remove_file(&path).unwrap();
        fs::remove_file(&path2).unwrap();
    }

    #[test]
    fn title_falls_back_to_id_when_no_h1() {
        let contents = "---\ntask \"proj-abc\" {\n  status \"todo\"\n}\n---\n\nBody only\n";
        let path = write_temp_doc(contents);
        let doc = parse_doc(&path).expect("parse");
        assert_eq!(doc.title, "proj-abc");
        fs::remove_file(&path).unwrap();
    }
}
