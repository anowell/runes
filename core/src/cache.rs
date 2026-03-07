use crate::config::Store;
use crate::model::{discover_project_docs, parse_doc};
use crate::{Error, Result};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

fn run_sql(db_path: &Path, sql: &str) -> Result<()> {
    let status = Command::new("sqlite3").arg(db_path).arg(sql).status()?;
    if !status.success() {
        return Err(Error::new(format!(
            "sqlite3 command failed for {}",
            db_path.display()
        )));
    }
    Ok(())
}

pub fn cache_path(store: &Store) -> Result<PathBuf> {
    let home = std::env::var("HOME").map_err(|_| Error::new("HOME is not set"))?;
    let root = PathBuf::from(home).join(".runes").join("cache");
    std::fs::create_dir_all(&root)?;
    Ok(root.join(format!("{}.sqlite", store.name)))
}

pub fn rebuild_cache(store: &Store) -> Result<()> {
    let db_path = cache_path(store)?;
    run_sql(
        &db_path,
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
            let rel_path = doc_path
                .strip_prefix(&store.path)
                .map_err(|e| Error::new(e.to_string()))?
                .display()
                .to_string();
            let labels = doc.labels.join(",");
            let assignee = doc.assignee.as_deref().unwrap_or("");
            let sql = format!(
                "INSERT OR REPLACE INTO runes (id, short_id, project, kind, status, assignee, title, path, labels)
                 VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}');",
                sql_escape(&doc.id),
                sql_escape(&short_id),
                sql_escape(doc.id.split('-').next().unwrap_or("")),
                sql_escape(&doc.kind),
                sql_escape(&doc.status),
                sql_escape(assignee),
                sql_escape(&doc.title),
                sql_escape(&rel_path),
                sql_escape(&labels),
            );
            run_sql(&db_path, &sql)?;
        }
    }
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
}

pub fn query_cache(store: &Store, where_clause: &str) -> Result<Vec<CacheRow>> {
    let db_path = cache_path(store)?;
    if !db_path.exists() {
        rebuild_cache(store)?;
    }
    let sql = format!(
        "SELECT id, project, kind, status, assignee, title, path FROM runes WHERE {} ORDER BY id;",
        where_clause
    );
    let output = Command::new("sqlite3")
        .arg("-separator")
        .arg("\t")
        .arg(&db_path)
        .arg(&sql)
        .stdout(Stdio::piped())
        .output()?;
    if !output.status.success() {
        return Err(Error::new(format!(
            "sqlite query failed for {}",
            db_path.display()
        )));
    }
    let stdout = String::from_utf8(output.stdout)?;
    let mut rows = Vec::new();
    for line in stdout.lines() {
        let cols: Vec<&str> = line.splitn(7, '\t').collect();
        if cols.len() >= 7 {
            rows.push(CacheRow {
                id: cols[0].to_string(),
                project: cols[1].to_string(),
                kind: cols[2].to_string(),
                status: cols[3].to_string(),
                assignee: cols[4].to_string(),
                title: cols[5].to_string(),
                path: cols[6].to_string(),
            });
        }
    }
    Ok(rows)
}

pub fn lookup_status(store: &Store, id: &str) -> Result<Option<String>> {
    let db_path = cache_path(store)?;
    if !db_path.exists() {
        rebuild_cache(store)?;
    }
    let sql = format!(
        "SELECT status FROM runes WHERE id='{}' LIMIT 1;",
        sql_escape(id)
    );
    let output = Command::new("sqlite3")
        .arg(&db_path)
        .arg(&sql)
        .stdout(Stdio::piped())
        .output()?;
    if !output.status.success() {
        return Ok(None);
    }
    let stdout = String::from_utf8(output.stdout)?;
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_string()))
    }
}
