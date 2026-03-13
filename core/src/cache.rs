use crate::config::Store;
use crate::model::{discover_project_docs, parse_doc};
use crate::{Error, Result};
use rusqlite::{params, Connection};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchivedMode {
    Exclude,
    Only,
    Include,
}

impl ArchivedMode {
    pub fn from_keyword(value: &str) -> Option<Self> {
        match value.to_lowercase().as_str() {
            "only" | "archived-only" => Some(ArchivedMode::Only),
            "archived" | "include" | "with-archived" => Some(ArchivedMode::Include),
            "exclude" | "open" | "active" => Some(ArchivedMode::Exclude),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CacheFilter {
    pub project: Option<String>,
    pub statuses: Vec<String>,
    pub kind: Option<String>,
    pub assignee: Option<String>,
    pub labels: Vec<String>,
    pub archived: Option<ArchivedMode>,
}

pub fn cache_path(store: &Store) -> Result<PathBuf> {
    let home = std::env::var("HOME").map_err(|_| Error::new("HOME is not set"))?;
    let root = PathBuf::from(home).join(".runes").join("cache");
    std::fs::create_dir_all(&root)?;
    Ok(root.join(format!("{}.sqlite", store.name)))
}

fn open_db(store: &Store) -> Result<Connection> {
    let db_path = cache_path(store)?;
    let conn = Connection::open(&db_path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
    Ok(conn)
}

pub fn rebuild_cache(store: &Store) -> Result<()> {
    let conn = open_db(store)?;

    conn.execute_batch(
        "DROP TABLE IF EXISTS runes;
         CREATE TABLE runes (
           id TEXT PRIMARY KEY,
           short_id TEXT NOT NULL,
           project TEXT NOT NULL,
           kind TEXT NOT NULL,
           status TEXT NOT NULL,
           assignee TEXT,
           title TEXT NOT NULL,
           path TEXT NOT NULL,
           labels TEXT NOT NULL
         );
         CREATE INDEX idx_runes_project ON runes(project);
         CREATE INDEX idx_runes_status ON runes(status);
         CREATE INDEX idx_runes_kind ON runes(kind);
         CREATE INDEX idx_runes_assignee ON runes(assignee);",
    )?;

    // Use a transaction for bulk inserts - much faster than autocommit per row.
    // unchecked_transaction is fine here: we just created the tables above on this
    // same connection, so there's no existing transaction to nest into.
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT OR REPLACE INTO runes (id, short_id, project, kind, status, assignee, title, path, labels)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )?;

        for entry in std::fs::read_dir(&store.path)? {
            let entry = entry?;
            let project_root = entry.path();
            if !project_root.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') || name.starts_with('_') {
                continue;
            }
            let docs = discover_project_docs(&project_root)?;
            for doc_path in docs {
                let doc = match parse_doc(&doc_path) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let short_id = doc.id.split('-').nth(1).unwrap_or("").to_string();
                let project = doc.id.split('-').next().unwrap_or("").to_string();
                let rel_path = doc_path
                    .strip_prefix(&store.path)
                    .map_err(|e| Error::new(e.to_string()))?
                    .display()
                    .to_string();
                let labels = doc.labels.join(",");
                let assignee = doc.assignee.as_deref().unwrap_or("");
                stmt.execute(params![
                    doc.id, short_id, project, doc.kind, doc.status, assignee, doc.title, rel_path,
                    labels,
                ])?;
            }
        }
    }
    tx.commit()?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct CacheRow {
    pub id: String,
    pub project: String,
    pub kind: String,
    pub status: String,
    pub assignee: String,
    pub title: String,
    pub path: String,
    pub labels: Vec<String>,
}

pub fn query_cache(store: &Store, filter: &CacheFilter) -> Result<Vec<CacheRow>> {
    let db_path = cache_path(store)?;
    if !db_path.exists() {
        rebuild_cache(store)?;
    }
    let conn = open_db(store)?;

    let mut conditions = Vec::new();
    let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    if let Some(ref project) = filter.project {
        conditions.push("project = ?".to_string());
        param_values.push(Box::new(project.clone()));
    }

    if !filter.statuses.is_empty() {
        let placeholders: Vec<&str> = filter
            .statuses
            .iter()
            .map(|s| {
                param_values.push(Box::new(s.clone()));
                "?"
            })
            .collect();
        conditions.push(format!("status IN ({})", placeholders.join(",")));
    }

    if let Some(ref kind) = filter.kind {
        conditions.push("kind = ?".to_string());
        param_values.push(Box::new(kind.clone()));
    }

    if let Some(ref assignee) = filter.assignee {
        conditions.push("assignee = ?".to_string());
        param_values.push(Box::new(assignee.clone()));
    }

    // Label matching: comma-separated field, check exact or boundary matches
    for label in &filter.labels {
        // Use ',' || labels || ',' to normalize boundaries, then LIKE '%,label,%'
        // Escape LIKE wildcards in the label value to prevent unintended matches
        conditions.push("(',' || labels || ',') LIKE ? ESCAPE '\\'".to_string());
        let escaped_label = label.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
        param_values.push(Box::new(format!("%,{},%", escaped_label)));
    }

    match filter.archived {
        Some(ArchivedMode::Exclude) => {
            conditions.push("path NOT LIKE '%/_archive/%'".to_string());
        }
        Some(ArchivedMode::Only) => {
            conditions.push("path LIKE '%/_archive/%'".to_string());
        }
        Some(ArchivedMode::Include) | None => {}
    }

    let where_clause = if conditions.is_empty() {
        "1=1".to_string()
    } else {
        conditions.join(" AND ")
    };

    let sql = format!(
        "SELECT id, project, kind, status, assignee, title, path, labels FROM runes WHERE {} ORDER BY id",
        where_clause
    );

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        let labels_str: String = row.get(7)?;
        let labels = if labels_str.is_empty() {
            Vec::new()
        } else {
            labels_str.split(',').map(|s| s.to_string()).collect()
        };
        Ok(CacheRow {
            id: row.get(0)?,
            project: row.get(1)?,
            kind: row.get(2)?,
            status: row.get(3)?,
            assignee: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
            title: row.get(5)?,
            path: row.get(6)?,
            labels,
        })
    })?;

    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

pub fn lookup_status(store: &Store, id: &str) -> Result<Option<String>> {
    let db_path = cache_path(store)?;
    if !db_path.exists() {
        rebuild_cache(store)?;
    }
    let conn = open_db(store)?;

    let mut stmt = conn.prepare("SELECT status FROM runes WHERE id = ?1 LIMIT 1")?;
    let result = stmt.query_row(params![id], |row| row.get::<_, String>(0));
    match result {
        Ok(status) => Ok(Some(status)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}
