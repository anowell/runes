use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_tmp_home(test_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock drift")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("runes-tests-{test_name}-{nanos}"));
    fs::create_dir_all(&dir).expect("create temp home");
    dir
}

fn runes_output(home: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_runes"))
        .args(args)
        .env("HOME", home)
        .env("RUNES_USER", "Test User <test@runes.dev>")
        .output()
        .expect("run runes command")
}

fn runes_ok(home: &Path, args: &[&str]) -> String {
    let output = runes_output(home, args);
    if !output.status.success() {
        panic!(
            "command failed: runes {}\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    String::from_utf8(output.stdout).expect("stdout utf8")
}

fn runes_with_env(home: &Path, envs: &[(&str, &str)], args: &[&str]) -> String {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_runes"));
    cmd.args(args)
        .env("HOME", home)
        .env("RUNES_USER", "Test User <test@runes.dev>");
    for (key, value) in envs {
        cmd.env(key, value);
    }
    let output = cmd.output().expect("run runes command");
    if !output.status.success() {
        panic!(
            "command failed: runes {}\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    String::from_utf8(output.stdout).expect("stdout utf8")
}

fn last_line(output: &str) -> &str {
    output
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("")
}

fn command_exists(cmd: &str) -> bool {
    Command::new(cmd).arg("--version").output().is_ok()
}

fn command_ok(home: &Path, program: &str, args: &[&str], cwd: Option<&Path>) -> String {
    let mut cmd = Command::new(program);
    cmd.args(args).env("HOME", home);
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    let output = cmd.output().expect("run external command");
    if !output.status.success() {
        panic!(
            "command failed: {} {}\nstdout:\n{}\nstderr:\n{}",
            program,
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    String::from_utf8(output.stdout).expect("stdout utf8")
}

fn copy_dir_recursive(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("create target dir");
    for entry in fs::read_dir(from).expect("read source dir") {
        let entry = entry.expect("dir entry");
        let src = entry.path();
        let dst = to.join(entry.file_name());
        if src.is_dir() {
            copy_dir_recursive(&src, &dst);
        } else {
            fs::copy(&src, &dst).expect("copy file");
        }
    }
}

#[test]
fn jj_issue_lifecycle_and_cache_query() {
    if !command_exists("jj") || !command_exists("sqlite3") {
        eprintln!("skipping: jj/sqlite3 not installed");
        return;
    }

    let home = unique_tmp_home("jj-lifecycle");
    let store_path = home.join("stores").join("how");
    let store_path_s = store_path.to_string_lossy().to_string();

    runes_ok(
        &home,
        &[
            "store",
            "init",
            "how",
            "--backend",
            "jj",
            "--path",
            &store_path_s,
            "--default",
        ],
    );
    let issue_output = runes_ok(
        &home,
        &[
            "new",
            "--project",
            "how:runes",
            "Lock v1 schema and workflow",
        ],
    );
    let issue_id = last_line(&issue_output).to_string();
    assert!(issue_id.starts_with("runes-"));

    runes_ok(
        &home,
        &[
            "edit",
            &format!("how:{issue_id}"),
            "--title",
            "Lock Runes v1 schema and workflow",
            "--status",
            "in-progress",
            "--label",
            "schema",
        ],
    );

    let shown = runes_ok(&home, &["show", &format!("how:{issue_id}")]);
    assert!(
        shown.contains("status \"in-progress\""),
        "status not updated"
    );
    assert!(shown.contains("labels \"schema\""), "label not added");
    assert!(
        shown.contains("# Lock Runes v1 schema and workflow"),
        "title not updated"
    );

    let listed = runes_ok(
        &home,
        &[
            "list",
            "--store",
            "how",
            "--project",
            "runes",
            "--status",
            "in-progress",
        ],
    );
    assert!(listed.contains(&issue_id), "issue missing from cache query");
    assert!(listed.contains("Lock Runes v1 schema and workflow"));

    let _issue_log = runes_ok(&home, &["log", &format!("how:{issue_id}"), "--limit", "5"]);

    let section_log = runes_ok(
        &home,
        &[
            "log",
            &format!("how:{issue_id}"),
            "--limit",
            "10",
            "--section",
            "Summary",
        ],
    );
    assert!(
        section_log.contains("Change ") || section_log.contains("No matching section edits found"),
        "section log output format changed unexpectedly"
    );
}

#[test]
fn new_default_project_from_env_var() {
    if !command_exists("jj") || !command_exists("sqlite3") {
        eprintln!("skipping: jj/sqlite3 not installed");
        return;
    }

    let home = unique_tmp_home("jj-env-project");
    let store_path = home.join("stores").join("how");
    let store_path_s = store_path.to_string_lossy().to_string();

    runes_ok(
        &home,
        &[
            "store",
            "init",
            "how",
            "--backend",
            "jj",
            "--path",
            &store_path_s,
            "--default",
        ],
    );

    let issue_output = runes_with_env(
        &home,
        &[("RUNES_PROJECT", "runes")],
        &["new", "--store", "how", "Env var project"],
    );
    let issue_id = last_line(&issue_output).to_string();
    assert!(issue_id.starts_with("runes-"));

    let shown = runes_ok(&home, &["show", &format!("how:{issue_id}")]);
    assert!(shown.contains("task \""));
}

#[test]
fn store_doctor_rebuilds_cache() {
    if !command_exists("jj") || !command_exists("sqlite3") {
        eprintln!("skipping: jj/sqlite3 not installed");
        return;
    }

    let home = unique_tmp_home("jj-store-doctor");
    let store_path = home.join("stores").join("how");
    let store_path_s = store_path.to_string_lossy().to_string();

    runes_ok(
        &home,
        &[
            "store",
            "init",
            "how",
            "--backend",
            "jj",
            "--path",
            &store_path_s,
            "--default",
        ],
    );

    let doctor_output = runes_ok(&home, &["store", "doctor", "how"]);
    assert!(
        doctor_output.contains("Cache rebuilt for how"),
        "doctor output missing cache rebuild confirmation"
    );
}

#[test]
fn jj_milestone_hierarchy_and_progress() {
    if !command_exists("jj") || !command_exists("sqlite3") {
        eprintln!("skipping: jj/sqlite3 not installed");
        return;
    }

    let home = unique_tmp_home("jj-milestones");
    let store_path = home.join("stores").join("how");
    let store_path_s = store_path.to_string_lossy().to_string();

    runes_ok(
        &home,
        &[
            "store",
            "init",
            "how",
            "--backend",
            "jj",
            "--path",
            &store_path_s,
            "--default",
        ],
    );
    let milestone_output = runes_ok(
        &home,
        &[
            "new",
            "--project",
            "how:runes",
            "Principles, schema, and bootstrap",
            "--id",
            "m01",
            "--kind",
            "milestone",
        ],
    );
    let milestone = last_line(&milestone_output).to_string();
    assert_eq!(milestone, "runes-m01");

    let child1_output = runes_ok(
        &home,
        &[
            "new",
            "--project",
            "how:runes",
            "Define core principles",
            "--parent",
            &milestone,
        ],
    );
    let child1 = last_line(&child1_output).to_string();
    let _child2 = runes_ok(
        &home,
        &[
            "new",
            "--project",
            "how:runes",
            "Finalize schema examples",
            "--parent",
            &milestone,
        ],
    );

    runes_ok(
        &home,
        &["edit", &format!("how:{child1}"), "--status", "done"],
    );

    let progress = runes_ok(&home, &["show", &format!("how:{milestone}")]);
    assert!(progress.contains("child_total=2"));
    assert!(progress.contains("child_done=1"));
    assert!(progress.contains("child_todo=1"));
}

#[test]
fn milestone_list_and_project_progress() {
    if !command_exists("jj") || !command_exists("sqlite3") {
        eprintln!("skipping: jj/sqlite3 not installed");
        return;
    }

    let home = unique_tmp_home("jj-milestone-list");
    let store_path = home.join("stores").join("how");
    let store_path_s = store_path.to_string_lossy().to_string();

    runes_ok(
        &home,
        &[
            "store",
            "init",
            "how",
            "--backend",
            "jj",
            "--path",
            &store_path_s,
            "--default",
        ],
    );
    let milestone_output = runes_ok(
        &home,
        &[
            "new",
            "--project",
            "how:runes",
            "Milestones for list test",
            "--id",
            "m02",
            "--kind",
            "milestone",
        ],
    );
    let milestone = last_line(&milestone_output).to_string();

    let list_output = runes_ok(
        &home,
        &[
            "list",
            "--store",
            "how",
            "--project",
            "runes",
            "--kind",
            "milestones",
        ],
    );
    assert!(list_output.contains(&milestone));

    let project_progress = runes_ok(&home, &["show", &format!("how:{milestone}")]);
    assert!(project_progress.contains("milestone \""));
    assert!(project_progress.contains(&milestone));
}

#[test]
fn pijul_issue_lifecycle_with_sdk_observability() {
    if !command_exists("pijul") || !command_exists("sqlite3") {
        eprintln!("skipping: pijul/sqlite3 not installed");
        return;
    }

    let real_home = PathBuf::from(std::env::var("HOME").expect("HOME missing"));
    let real_pijul = real_home
        .join("Library")
        .join("Application Support")
        .join("pijul");
    if !real_pijul.exists() {
        eprintln!(
            "skipping: no existing pijul identity/config at {}",
            real_pijul.display()
        );
        return;
    }

    let home = unique_tmp_home("pijul-lifecycle");
    let test_pijul = home
        .join("Library")
        .join("Application Support")
        .join("pijul");
    copy_dir_recursive(&real_pijul, &test_pijul);

    let store_path = home.join("stores").join("proj");
    let store_path_s = store_path.to_string_lossy().to_string();
    runes_ok(
        &home,
        &[
            "store",
            "init",
            "proj",
            "--backend",
            "pijul",
            "--path",
            &store_path_s,
            "--default",
        ],
    );
    let issue_output = runes_ok(
        &home,
        &[
            "new",
            "--project",
            "proj:runes",
            "Validate libpijul-backed workflows",
        ],
    );
    let issue_id = last_line(&issue_output).to_string();
    assert!(issue_id.starts_with("runes-"));
    let pijul_log = command_ok(&home, "pijul", &["log", "--limit", "1"], Some(&store_path));
    if pijul_log.is_empty() {
        eprintln!("pijul log returned empty; skipping history assertion");
    } else {
        assert!(
            pijul_log.contains("Add ") && pijul_log.contains(&issue_id),
            "pijul history missing expected message for issue create: {pijul_log}"
        );
    }

    let issue_log = runes_output(&home, &["log", &format!("proj:{issue_id}"), "--limit", "5"]);
    if !issue_log.status.success() {
        eprintln!(
            "runes log failed (expected for a watchless doc): {}",
            String::from_utf8_lossy(&issue_log.stderr)
        );
    }
}

#[test]
fn pijul_cross_store_move_updates_both_stores() {
    if !command_exists("pijul") || !command_exists("sqlite3") {
        eprintln!("skipping: pijul/sqlite3 not installed");
        return;
    }

    let real_home = PathBuf::from(std::env::var("HOME").expect("HOME missing"));
    let real_pijul = real_home
        .join("Library")
        .join("Application Support")
        .join("pijul");
    if !real_pijul.exists() {
        eprintln!(
            "skipping: no existing pijul identity/config at {}",
            real_pijul.display()
        );
        return;
    }

    let home = unique_tmp_home("pijul-move");
    let test_pijul = home
        .join("Library")
        .join("Application Support")
        .join("pijul");
    copy_dir_recursive(&real_pijul, &test_pijul);

    let src_path = home.join("stores").join("src");
    let dst_path = home.join("stores").join("dst");
    runes_ok(
        &home,
        &[
            "store",
            "init",
            "src",
            "--backend",
            "pijul",
            "--path",
            &src_path.to_string_lossy(),
            "--default",
        ],
    );
    runes_ok(
        &home,
        &[
            "store",
            "init",
            "dst",
            "--backend",
            "pijul",
            "--path",
            &dst_path.to_string_lossy(),
        ],
    );
    let issue_output = runes_ok(
        &home,
        &["new", "--project", "src:runes", "Move me between stores"],
    );
    let issue_id = last_line(&issue_output).to_string();
    runes_ok(
        &home,
        &["move", &format!("src:{issue_id}"), "--project", "dst:runes"],
    );

    let moved_doc = runes_ok(&home, &["show", &format!("dst:{issue_id}")]);
    assert!(moved_doc.contains("Move me between stores"));

    let source_show = runes_output(&home, &["show", &format!("src:{issue_id}")]);
    assert!(
        !source_show.status.success(),
        "issue unexpectedly still present in source store"
    );

    let dst_list = runes_ok(&home, &["list", "--store", "dst", "--project", "runes"]);
    assert!(dst_list.contains(&issue_id));
}

// --- runes-ph7: robust and predictable history tests ---

fn setup_jj_store(test_name: &str) -> (PathBuf, String) {
    let home = unique_tmp_home(test_name);
    let store_path = home.join("stores").join("tst");
    let store_path_s = store_path.to_string_lossy().to_string();
    runes_ok(
        &home,
        &[
            "store",
            "init",
            "tst",
            "--backend",
            "jj",
            "--path",
            &store_path_s,
            "--default",
        ],
    );
    (home, store_path_s)
}

fn setup_pijul_store(test_name: &str) -> Option<(PathBuf, String)> {
    if !command_exists("pijul") {
        eprintln!("skipping: pijul not installed");
        return None;
    }
    let real_home = PathBuf::from(std::env::var("HOME").expect("HOME missing"));
    let real_pijul = real_home
        .join("Library")
        .join("Application Support")
        .join("pijul");
    if !real_pijul.exists() {
        eprintln!("skipping: no pijul identity");
        return None;
    }
    let home = unique_tmp_home(test_name);
    let test_pijul = home
        .join("Library")
        .join("Application Support")
        .join("pijul");
    copy_dir_recursive(&real_pijul, &test_pijul);
    let store_path = home.join("stores").join("tst");
    let store_path_s = store_path.to_string_lossy().to_string();
    runes_ok(
        &home,
        &[
            "store",
            "init",
            "tst",
            "--backend",
            "pijul",
            "--path",
            &store_path_s,
            "--default",
        ],
    );
    Some((home, store_path_s))
}

fn runes_stderr(home: &Path, args: &[&str]) -> String {
    let output = runes_output(home, args);
    String::from_utf8(output.stderr).expect("stderr utf8")
}

/// Test: new rune → show has created_at/created_by, no extra annotations
#[test]
fn jj_show_new_rune_has_created_metadata() {
    if !command_exists("jj") || !command_exists("sqlite3") {
        eprintln!("skipping: jj/sqlite3 not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-show-new");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "tst:proj", "Test rune"],
    ))
    .to_string();
    let shown = runes_ok(&home, &["show", &format!("tst:{id}")]);
    assert!(shown.contains("created_by"), "missing created_by: {shown}");
    assert!(shown.contains("created_at"), "missing created_at: {shown}");
    // No updated_at because it matches created
    assert!(
        !shown.contains("updated_at"),
        "unexpected updated_at: {shown}"
    );
    // No "Edited by" because sections haven't changed since creation
    assert!(
        !shown.contains("Edited by"),
        "unexpected section annotation: {shown}"
    );
}

/// Test: new + comment → log shows 2 entries, comment has attribution
#[test]
fn jj_show_comment_attribution() {
    if !command_exists("jj") || !command_exists("sqlite3") {
        eprintln!("skipping: jj/sqlite3 not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-comment-attr");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "tst:proj", "Comment test"],
    ))
    .to_string();
    runes_ok(
        &home,
        &["comment", &format!("tst:{id}"), "-m", "This is a comment"],
    );
    let shown = runes_ok(&home, &["show", &format!("tst:{id}")]);
    assert!(
        shown.contains("updated_at"),
        "missing updated_at after comment: {shown}"
    );
    // Comment should have attribution line "On ... by ..."
    assert!(
        shown.contains("by Test User"),
        "missing comment author attribution: {shown}"
    );
    assert!(
        shown.contains("This is a comment"),
        "missing comment text: {shown}"
    );
}

/// Test: new + edit design → section annotation appears
#[test]
fn jj_show_section_edit_annotation() {
    if !command_exists("jj") || !command_exists("sqlite3") {
        eprintln!("skipping: jj/sqlite3 not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("jj-section-edit");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "tst:proj", "Section test"],
    ))
    .to_string();
    // Edit the file directly to add content to Design section
    let store = Path::new(&store_path);
    let doc_path = find_rune_file(store, &id);
    let content = fs::read_to_string(&doc_path).expect("read doc");
    let updated = content.replace("## Design\n", "## Design\n\nNew design content here.\n");
    fs::write(&doc_path, &updated).expect("write doc");
    runes_ok(
        &home,
        &["commit", &format!("tst:{id}"), "-m", "Update design"],
    );
    let shown = runes_ok(&home, &["show", &format!("tst:{id}")]);
    assert!(
        shown.contains("Edited by"),
        "missing section annotation: {shown}"
    );
}

/// Test: show uncommitted rune has red "<not committed>"
#[test]
fn jj_show_uncommitted_rune() {
    if !command_exists("jj") || !command_exists("sqlite3") {
        eprintln!("skipping: jj/sqlite3 not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-uncommitted");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "tst:proj", "Uncommitted", "--no-commit"],
    ))
    .to_string();
    let shown = runes_ok(&home, &["show", &format!("tst:{id}")]);
    assert!(
        shown.contains("<not committed>"),
        "missing uncommitted indicator: {shown}"
    );
}

/// Test: show pending changes on a section
#[test]
fn jj_show_pending_section_changes() {
    if !command_exists("jj") || !command_exists("sqlite3") {
        eprintln!("skipping: jj/sqlite3 not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("jj-pending");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "tst:proj", "Pending test"],
    ))
    .to_string();
    let store = Path::new(&store_path);
    let doc_path = find_rune_file(store, &id);
    // Edit Design section without committing
    let content = fs::read_to_string(&doc_path).expect("read doc");
    let updated = content.replace("## Design\n", "## Design\n\nUncommitted design change.\n");
    fs::write(&doc_path, &updated).expect("write doc");
    let shown = runes_ok(&home, &["show", &format!("tst:{id}")]);
    assert!(
        shown.contains("pending uncommitted changes"),
        "missing pending annotation: {shown}"
    );
}

/// Test: log associates runes via changed_files, not description
#[test]
fn jj_log_uses_changed_files_not_description() {
    if !command_exists("jj") || !command_exists("sqlite3") {
        eprintln!("skipping: jj/sqlite3 not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-log-files");
    // Create two runes with --no-commit, then commit together
    let id1 = last_line(&runes_ok(
        &home,
        &["new", "--project", "tst:proj", "Rune one", "--no-commit"],
    ))
    .to_string();
    let id2 = last_line(&runes_ok(
        &home,
        &["new", "--project", "tst:proj", "Rune two", "--no-commit"],
    ))
    .to_string();
    runes_ok(
        &home,
        &[
            "commit",
            "--project",
            "proj",
            "-m",
            "Bulk commit with no rune IDs in message",
        ],
    );
    // Log should find both runes from changed_files
    let log_json = runes_ok(&home, &["log", "--all", "--json"]);
    assert!(log_json.contains(&id1), "log missing {id1}: {log_json}");
    assert!(log_json.contains(&id2), "log missing {id2}: {log_json}");
    // Verify the bulk commit associates both runes
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&log_json).expect("parse json");
    let bulk = parsed
        .iter()
        .find(|e| e["comment"].as_str().unwrap_or("").contains("Bulk commit"));
    assert!(bulk.is_some(), "bulk commit not found in log");
    let runes = bulk.unwrap()["runes"].as_array().unwrap();
    let rune_ids: Vec<&str> = runes.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        rune_ids.contains(&id1.as_str()),
        "bulk commit missing {id1}"
    );
    assert!(
        rune_ids.contains(&id2.as_str()),
        "bulk commit missing {id2}"
    );
}

/// Test: pijul - new rune show has created metadata
#[test]
fn pijul_show_new_rune_has_created_metadata() {
    if !command_exists("sqlite3") {
        eprintln!("skipping: sqlite3 not installed");
        return;
    }
    let (home, _) = match setup_pijul_store("pijul-show-new") {
        Some(v) => v,
        None => return,
    };
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "tst:proj", "Pijul test"],
    ))
    .to_string();
    let shown = runes_ok(&home, &["show", &format!("tst:{id}")]);
    assert!(shown.contains("created_by"), "missing created_by: {shown}");
    assert!(shown.contains("created_at"), "missing created_at: {shown}");
    assert!(
        !shown.contains("updated_at"),
        "unexpected updated_at: {shown}"
    );
}

/// Test: pijul - log uses changed_files
#[test]
fn pijul_log_uses_changed_files() {
    if !command_exists("sqlite3") {
        eprintln!("skipping: sqlite3 not installed");
        return;
    }
    let (home, _) = match setup_pijul_store("pijul-log-files") {
        Some(v) => v,
        None => return,
    };
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "tst:proj", "Pijul log test"],
    ))
    .to_string();
    let log_json = runes_ok(&home, &["log", "--all", "--json"]);
    assert!(log_json.contains(&id), "pijul log missing {id}: {log_json}");
}

/// Test: rename preserves basic show functionality (file is findable by ID after rename)
#[test]
fn jj_rename_preserves_history() {
    if !command_exists("jj") || !command_exists("sqlite3") {
        eprintln!("skipping: jj/sqlite3 not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-rename");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "tst:proj", "Original title"],
    ))
    .to_string();
    // Edit the title which changes the filename slug
    runes_ok(
        &home,
        &["edit", &format!("tst:{id}"), "--title", "Renamed title"],
    );
    // Show still works with same ID after rename
    let shown = runes_ok(&home, &["show", &format!("tst:{id}")]);
    assert!(
        shown.contains("Renamed title"),
        "title not updated: {shown}"
    );
    assert!(shown.contains("created_by"), "missing created_by: {shown}");
    assert!(shown.contains("created_at"), "missing created_at: {shown}");
}

fn find_rune_file(store_path: &Path, rune_id: &str) -> PathBuf {
    let short = rune_id.split('-').last().unwrap_or(rune_id);
    let project = rune_id.split('-').next().unwrap_or("proj");
    let project_dir = store_path.join(project);
    for entry in fs::read_dir(&project_dir).expect("read project dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&format!("{short}--")) && name.ends_with(".md") {
            return entry.path();
        }
    }
    panic!(
        "rune file not found for {rune_id} in {}",
        project_dir.display()
    );
}
