use crate::backend;
use crate::config::Store;
use crate::model::{discover_project_docs, parse_doc};
use crate::state;
use crate::{Error, Result};
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchivedMode {
    Exclude,
    Only,
    Include,
}

#[derive(Debug, Clone, Default)]
pub struct CacheFilter {
    pub project: Option<String>,
    pub statuses: Vec<String>,
    pub kind: Option<String>,
    pub assignee: Option<String>,
    pub labels: Vec<String>,
    pub archived: Option<ArchivedMode>,
    /// Filter to runes that have at least one unresolved dep.
    pub blocked: Option<bool>,
    /// Filter to runes that depend on this specific ID.
    pub blocked_by: Option<String>,
    /// Filter to runes that are a dependency OF this specific ID (reverse lookup).
    pub blocks: Option<String>,
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
         DROP TABLE IF EXISTS rune_deps;
         DROP TABLE IF EXISTS rune_fts;
         CREATE TABLE runes (
           id TEXT PRIMARY KEY,
           short_id TEXT NOT NULL,
           project TEXT NOT NULL,
           kind TEXT NOT NULL,
           status TEXT NOT NULL,
           assignee TEXT,
           title TEXT NOT NULL,
           path TEXT NOT NULL,
           labels TEXT NOT NULL,
           updated INTEGER,
           blocked INTEGER NOT NULL DEFAULT 0
         );
         CREATE TABLE rune_deps (
           rune_id TEXT NOT NULL,
           dep_id TEXT NOT NULL,
           PRIMARY KEY (rune_id, dep_id)
         );
         CREATE INDEX idx_runes_project ON runes(project);
         CREATE INDEX idx_runes_status ON runes(status);
         CREATE INDEX idx_runes_kind ON runes(kind);
         CREATE INDEX idx_runes_assignee ON runes(assignee);
         CREATE INDEX idx_runes_blocked ON runes(blocked);
         CREATE INDEX idx_rune_deps_rune ON rune_deps(rune_id);
         CREATE INDEX idx_rune_deps_dep ON rune_deps(dep_id);
         CREATE VIRTUAL TABLE rune_fts USING fts5(id UNINDEXED, title, body);",
    )?;

    // Collect all docs with their deps for a two-pass approach:
    // 1. Insert all runes
    // 2. Insert deps and compute the blocked flag
    struct DocInfo {
        id: String,
        deps: Vec<String>,
    }
    let mut all_docs: Vec<DocInfo> = Vec::new();

    // Use a transaction for bulk inserts - much faster than autocommit per row.
    // unchecked_transaction is fine here: we just created the tables above on this
    // same connection, so there's no existing transaction to nest into.
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT OR REPLACE INTO runes (id, short_id, project, kind, status, assignee, title, path, labels)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )?;
        let mut dep_stmt =
            tx.prepare("INSERT OR IGNORE INTO rune_deps (rune_id, dep_id) VALUES (?1, ?2)")?;
        let mut fts_stmt =
            tx.prepare("INSERT INTO rune_fts (id, title, body) VALUES (?1, ?2, ?3)")?;

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
                // Comments live in the body, so indexing it covers them too.
                fts_stmt.execute(params![doc.id, doc.title, doc.body])?;
                for dep in &doc.deps {
                    dep_stmt.execute(params![doc.id, dep])?;
                }
                if !doc.deps.is_empty() {
                    all_docs.push(DocInfo {
                        id: doc.id.clone(),
                        deps: doc.deps.clone(),
                    });
                }
            }
        }
    }
    tx.commit()?;

    // Populate updated timestamps from VCS log
    if let Ok(entries) = backend::rich_log(store, 10000) {
        let mut ts_map: HashMap<String, i64> = HashMap::new();
        for entry in &entries {
            for file in &entry.changed_files {
                ts_map.entry(file.clone()).or_insert(entry.timestamp);
            }
        }
        let mut stmt = conn.prepare("UPDATE runes SET updated = ?1 WHERE path = ?2")?;
        for (path, ts) in &ts_map {
            stmt.execute(params![ts, path])?;
        }
    }

    // A rune is blocked if any of its deps is not closed.
    let mut status_map: HashMap<String, String> = HashMap::new();
    {
        let mut stmt = conn.prepare("SELECT id, status FROM runes")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (id, status) = row?;
            status_map.insert(id, status);
        }
    }

    let mut update_stmt = conn.prepare("UPDATE runes SET blocked = 1 WHERE id = ?1")?;
    for doc_info in &all_docs {
        let mut is_blocked = false;
        for dep_id in &doc_info.deps {
            if let Some(dep_status) = status_map.get(dep_id) {
                if !state::is_terminal(dep_status) {
                    is_blocked = true;
                    break;
                }
            } else {
                // Dep references an unknown rune — treat as blocked (unresolved dep)
                is_blocked = true;
                break;
            }
        }
        if is_blocked {
            update_stmt.execute(params![doc_info.id])?;
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
    pub labels: Vec<String>,
    pub updated: Option<i64>,
    pub blocked: bool,
}

/// SQL fragments for a `CacheFilter`. Bindings are positional, so `params` must
/// follow any param appearing earlier in the composed statement.
struct FilterSql {
    joins: Vec<String>,
    conditions: Vec<String>,
    params: Vec<Box<dyn rusqlite::types::ToSql>>,
}

/// Escape LIKE wildcards so a filter value matches literally (`ESCAPE '\'`).
fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn filter_sql(filter: &CacheFilter) -> FilterSql {
    let mut conditions = Vec::new();
    let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    let mut joins = Vec::new();

    if let Some(ref project) = filter.project {
        conditions.push("runes.project = ?".to_string());
        param_values.push(Box::new(project.clone()));
    }

    if !filter.statuses.is_empty() {
        // A bare core state also matches its substates: `closed` covers `closed:canceled`.
        let clauses: Vec<String> = filter
            .statuses
            .iter()
            .map(|status| {
                param_values.push(Box::new(status.clone()));
                if status.contains(':') {
                    "runes.status = ?".to_string()
                } else {
                    param_values.push(Box::new(format!("{}:%", escape_like(status))));
                    "(runes.status = ? OR runes.status LIKE ? ESCAPE '\\')".to_string()
                }
            })
            .collect();
        conditions.push(format!("({})", clauses.join(" OR ")));
    }

    if let Some(ref kind) = filter.kind {
        conditions.push("runes.kind = ?".to_string());
        param_values.push(Box::new(kind.clone()));
    }

    if let Some(ref assignee) = filter.assignee {
        conditions.push("runes.assignee = ?".to_string());
        param_values.push(Box::new(assignee.clone()));
    }

    // Label matching: comma-separated field, check exact or boundary matches
    for label in &filter.labels {
        conditions.push("(',' || runes.labels || ',') LIKE ? ESCAPE '\\'".to_string());
        param_values.push(Box::new(format!("%,{},%", escape_like(label))));
    }

    match filter.archived {
        Some(ArchivedMode::Exclude) => {
            conditions.push("runes.path NOT LIKE '%/_archive/%'".to_string());
        }
        Some(ArchivedMode::Only) => {
            conditions.push("runes.path LIKE '%/_archive/%'".to_string());
        }
        Some(ArchivedMode::Include) | None => {}
    }

    // Blocked/ready filter
    if let Some(blocked) = filter.blocked {
        conditions.push(format!("runes.blocked = {}", if blocked { 1 } else { 0 }));
    }

    // --blocked-by X: runes that depend on X
    if let Some(ref blocked_by_id) = filter.blocked_by {
        joins.push("JOIN rune_deps bd ON bd.rune_id = runes.id".to_string());
        conditions.push("bd.dep_id = ?".to_string());
        param_values.push(Box::new(blocked_by_id.clone()));
    }

    // --blocks X: runes that are a dep OF X (i.e. X depends on this rune)
    if let Some(ref blocks_id) = filter.blocks {
        joins.push("JOIN rune_deps bl ON bl.dep_id = runes.id".to_string());
        conditions.push("bl.rune_id = ?".to_string());
        param_values.push(Box::new(blocks_id.clone()));
    }

    FilterSql {
        joins,
        conditions,
        params: param_values,
    }
}

const ROW_COLUMNS: &str = "runes.id, runes.project, runes.kind, runes.status, runes.assignee, runes.title, runes.path, runes.labels, runes.updated, runes.blocked";

fn map_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CacheRow> {
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
        updated: row.get(8)?,
        blocked: row.get::<_, i32>(9)? != 0,
    })
}

fn collect_rows(
    conn: &Connection,
    sql: &str,
    params: &[Box<dyn rusqlite::types::ToSql>],
) -> Result<Vec<CacheRow>> {
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), map_row)?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

fn where_clause(conditions: &[String]) -> String {
    if conditions.is_empty() {
        "1=1".to_string()
    } else {
        conditions.join(" AND ")
    }
}

pub fn query_cache(store: &Store, filter: &CacheFilter) -> Result<Vec<CacheRow>> {
    let db_path = cache_path(store)?;
    if !db_path.exists() {
        rebuild_cache(store)?;
    }
    let conn = open_db(store)?;

    let parts = filter_sql(filter);
    let sql = format!(
        "SELECT {} FROM runes {} WHERE {} ORDER BY runes.updated DESC NULLS LAST, runes.id",
        ROW_COLUMNS,
        parts.joins.join(" "),
        where_clause(&parts.conditions)
    );
    collect_rows(&conn, &sql, &parts.params)
}

/// Build an FTS5 MATCH expression from a user term. Each token becomes a quoted
/// prefix phrase so punctuation and FTS operators are matched literally rather
/// than changing the query. `None` when the term has no searchable token.
fn fts_match_expr(term: &str) -> Option<String> {
    let tokens: Vec<String> = term
        .split_whitespace()
        .filter(|token| token.chars().any(|c| c.is_alphanumeric()))
        .map(|token| format!("\"{}\"*", token.replace('"', "\"\"")))
        .collect();
    if tokens.is_empty() {
        return None;
    }
    Some(tokens.join(" AND "))
}

/// Full-text search over rune titles and bodies, ranked with title matches first.
/// Every status is searched — closed runes match unless the caller narrows `filter`.
pub fn search_cache(store: &Store, term: &str, filter: &CacheFilter) -> Result<Vec<CacheRow>> {
    let Some(match_expr) = fts_match_expr(term) else {
        return Ok(Vec::new());
    };
    ensure_search_index(store)?;
    let conn = open_db(store)?;

    let parts = filter_sql(filter);
    // Title hits sort above body-only hits; bm25 (lower is better) breaks ties.
    let sql = format!(
        "SELECT {} FROM rune_fts \
         JOIN runes ON runes.id = rune_fts.id \
         LEFT JOIN (SELECT id FROM rune_fts WHERE rune_fts MATCH ?) title_hit ON title_hit.id = runes.id {} \
         WHERE rune_fts MATCH ? AND {} \
         ORDER BY title_hit.id IS NULL, bm25(rune_fts), runes.updated DESC NULLS LAST, runes.id",
        ROW_COLUMNS,
        parts.joins.join(" "),
        where_clause(&parts.conditions)
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![
        Box::new(format!("title : ({match_expr})")),
        Box::new(match_expr),
    ];
    params.extend(parts.params);
    collect_rows(&conn, &sql, &params)
}

/// Rebuild caches that are missing or predate the search index (built by an
/// older binary).
fn ensure_search_index(store: &Store) -> Result<()> {
    if !cache_path(store)?.exists() {
        return rebuild_cache(store);
    }
    let has_index: i64 = open_db(store)?.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'rune_fts'",
        [],
        |row| row.get(0),
    )?;
    if has_index == 0 {
        rebuild_cache(store)?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_filter_matches_substates_of_a_core_state() {
        let filter = CacheFilter {
            statuses: vec!["closed".to_string(), "wip:review".to_string()],
            ..CacheFilter::default()
        };
        let parts = filter_sql(&filter);
        assert_eq!(
            parts.conditions,
            vec![
                "((runes.status = ? OR runes.status LIKE ? ESCAPE '\\') OR runes.status = ?)"
                    .to_string()
            ]
        );
        // `closed` binds both the exact value and the `closed:%` prefix
        assert_eq!(parts.params.len(), 3);
    }

    #[test]
    fn fts_expr_quotes_and_ands_tokens() {
        assert_eq!(fts_match_expr("login"), Some("\"login\"*".to_string()));
        assert_eq!(
            fts_match_expr("login  flow"),
            Some("\"login\"* AND \"flow\"*".to_string())
        );
    }

    #[test]
    fn fts_expr_neutralizes_operators() {
        // Operators and punctuation are matched literally, not interpreted.
        assert_eq!(
            fts_match_expr("OR rn-bpp"),
            Some("\"OR\"* AND \"rn-bpp\"*".to_string())
        );
        assert_eq!(
            fts_match_expr("say \"hi\""),
            Some("\"say\"* AND \"\"\"hi\"\"\"*".to_string())
        );
        assert_eq!(fts_match_expr("  -- "), None);
    }
}
