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
    pub priority: Option<i32>,
    pub labels: Vec<String>,
    pub milestone: Option<String>,
    pub relations: Vec<(String, String)>,
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

fn extract_quoted_value(line: &str, key: &str) -> Option<String> {
    let pat = format!("{key}=\"");
    let idx = line.find(&pat)?;
    let rest = &line[idx + pat.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn extract_int_value(line: &str, key: &str) -> Option<i32> {
    let pat = format!("{key}=");
    let idx = line.find(&pat)?;
    let rest = &line[idx + pat.len()..];
    let end = rest.find(' ').unwrap_or(rest.len());
    rest[..end].parse::<i32>().ok()
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

    let mut doc = RuneDoc::default();
    doc.path = path.to_path_buf();
    doc.kind = "issue".to_string();
    doc.status = "todo".to_string();

    let mut in_relations = false;
    for line in &fm_lines {
        let trimmed = line.trim();
        if in_relations {
            if trimmed == "}" {
                in_relations = false;
                continue;
            }
            let mut split = trimmed.split_whitespace();
            if let Some(kind) = split.next() {
                if let Some(rest) = trimmed.strip_prefix(kind) {
                    let vals = extract_quoted_values(rest);
                    if let Some(id) = vals.first() {
                        doc.relations.push((kind.to_string(), id.to_string()));
                        continue;
                    }
                }
            }
            doc.frontmatter_extra.push(line.clone());
            continue;
        }
        if trimmed.starts_with("doc ") {
            if let Some(v) = extract_quoted_value(trimmed, "kind") {
                doc.kind = v;
            }
            if let Some(v) = extract_quoted_value(trimmed, "id") {
                doc.id = v;
            }
            if let Some(v) = extract_quoted_value(trimmed, "status") {
                doc.status = v;
            }
            if let Some(v) = extract_quoted_value(trimmed, "assignee") {
                doc.assignee = Some(v);
            }
            doc.priority = extract_int_value(trimmed, "priority");
        } else if trimmed.starts_with("labels ") {
            doc.labels = extract_quoted_values(trimmed);
        } else if trimmed.starts_with("milestone ") {
            doc.milestone = extract_quoted_values(trimmed).into_iter().next();
        } else if trimmed == "relations {" {
            in_relations = true;
        } else {
            doc.frontmatter_extra.push(line.clone());
        }
    }

    if doc.id.is_empty() {
        return Err(Error::new(format!(
            "{} frontmatter missing doc id",
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
    out.push_str(&format!(
        "doc kind=\"{}\" id=\"{}\" status=\"{}\"",
        doc.kind, doc.id, doc.status
    ));
    if let Some(assignee) = &doc.assignee {
        out.push_str(&format!(" assignee=\"{assignee}\""));
    }
    if let Some(priority) = doc.priority {
        out.push_str(&format!(" priority={priority}"));
    }
    out.push('\n');
    if !doc.labels.is_empty() {
        out.push_str("labels");
        for label in &doc.labels {
            out.push_str(&format!(" \"{label}\""));
        }
        out.push('\n');
    }
    if let Some(milestone) = &doc.milestone {
        out.push_str(&format!("milestone \"{milestone}\"\n"));
    }
    if !doc.relations.is_empty() {
        out.push_str("relations {\n");
        for (kind, id) in &doc.relations {
            out.push_str(&format!("  {kind} \"{id}\"\n"));
        }
        out.push_str("}\n");
    }
    for line in &doc.frontmatter_extra {
        out.push_str(line);
        out.push('\n');
    }
    out.push_str("---\n\n");
    out.push_str(&doc.body);
    if !doc.body.ends_with('\n') {
        out.push('\n');
    }
    out
}

pub fn new_issue_doc(id: &str, title: &str, milestone: Option<&str>) -> RuneDoc {
    let body = format!("# {title}\n\n## Summary\n\n## Design\n\n## Acceptance\n\n## Comments\n");
    RuneDoc {
        kind: "issue".to_string(),
        id: id.to_string(),
        status: "todo".to_string(),
        priority: Some(2),
        labels: Vec::new(),
        milestone: milestone.map(|s| s.to_string()),
        relations: Vec::new(),
        frontmatter_extra: Vec::new(),
        assignee: None,
        title: title.to_string(),
        body,
        path: PathBuf::new(),
    }
}

pub fn new_milestone_doc(id: &str, title: &str) -> RuneDoc {
    let body = format!(
        "# {title}\n\n## Goal\n\n## Exit Criteria\n\n## Scope\n\n## Risks\n\n## Tracking\n- Active\n"
    );
    RuneDoc {
        kind: "milestone".to_string(),
        id: id.to_string(),
        status: "active".to_string(),
        priority: Some(1),
        labels: vec!["v1".to_string()],
        milestone: None,
        relations: Vec::new(),
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
            if file_name == ".git" || file_name == ".jj" || file_name == ".pijul" {
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
