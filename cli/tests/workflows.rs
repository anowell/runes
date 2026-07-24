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
fn init_outside_repo() {
    let home = unique_tmp_home("init-outside-repo");
    let work = home.join("work");
    fs::create_dir_all(&work).expect("create work dir");

    // --stealth needs .git/info/exclude, so it must fail outside a git repo
    let output = Command::new(env!("CARGO_BIN_EXE_runes"))
        .args(["init", "--project", "demo", "--stealth"])
        .current_dir(&work)
        .env("HOME", &home)
        .env("RUNES_USER", "Test User <test@runes.dev>")
        .output()
        .expect("run runes command");
    assert!(
        !output.status.success(),
        "init --stealth should fail outside a git repo"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--stealth only works in a git repo"),
        "unexpected stderr: {stderr}"
    );
    assert!(!work.join("runes.kdl").exists());

    // Without --stealth the local config is created even outside a repo
    runes_ok(
        &home,
        &["config", "set", "user.email", "test@runes.dev", "--global"],
    );
    let output = Command::new(env!("CARGO_BIN_EXE_runes"))
        .args(["init", "--project", "demo"])
        .current_dir(&work)
        .env("HOME", &home)
        .env("RUNES_USER", "Test User <test@runes.dev>")
        .output()
        .expect("run runes command");
    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("Global config already exists"),
        "init should not mention existing global config: {stdout}"
    );
    let local = fs::read_to_string(work.join("runes.kdl")).expect("local config created");
    assert!(
        local.contains("demo"),
        "project missing from config: {local}"
    );
}

#[test]
fn jj_issue_lifecycle_and_cache_query() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }

    let home = unique_tmp_home("jj-lifecycle");
    let store_path = home.join(".runes").join("stores").join("test");
    let store_path_s = store_path.to_string_lossy().to_string();

    runes_ok(
        &home,
        &[
            "store",
            "init",
            "test",
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
            "test:runes",
            "Lock v1 schema and workflow",
        ],
    );
    let issue_id = last_line(&issue_output).to_string();
    assert!(issue_id.starts_with("runes-"));

    runes_ok(
        &home,
        &[
            "edit",
            &format!("test:{issue_id}"),
            "--title",
            "Lock Runes v1 schema and workflow",
            "--status",
            "in-progress",
            "--label",
            "schema",
        ],
    );

    let shown = runes_ok(&home, &["show", &format!("test:{issue_id}")]);
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
            "test",
            "--project",
            "runes",
            "--status",
            "in-progress",
        ],
    );
    assert!(listed.contains(&issue_id), "issue missing from cache query");
    assert!(listed.contains("Lock Runes v1 schema and workflow"));

    let _issue_log = runes_ok(&home, &["log", &format!("test:{issue_id}"), "--limit", "5"]);

    let section_log = runes_ok(
        &home,
        &[
            "log",
            &format!("test:{issue_id}"),
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
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }

    let home = unique_tmp_home("jj-env-project");
    let store_path = home.join(".runes").join("stores").join("test");
    let store_path_s = store_path.to_string_lossy().to_string();

    runes_ok(
        &home,
        &[
            "store",
            "init",
            "test",
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
        &["new", "--store", "test", "Env var project"],
    );
    let issue_id = last_line(&issue_output).to_string();
    assert!(issue_id.starts_with("runes-"));

    let shown = runes_ok(&home, &["show", &format!("test:{issue_id}")]);
    assert!(shown.contains("task \""));
}

#[test]
fn store_doctor_rebuilds_cache() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }

    let home = unique_tmp_home("jj-store-doctor");
    let store_path = home.join(".runes").join("stores").join("test");
    let store_path_s = store_path.to_string_lossy().to_string();

    runes_ok(
        &home,
        &[
            "store",
            "init",
            "test",
            "--backend",
            "jj",
            "--path",
            &store_path_s,
            "--default",
        ],
    );

    let doctor_output = runes_ok(&home, &["store", "doctor", "test"]);
    assert!(
        doctor_output.contains("Cache rebuilt for test"),
        "doctor output missing cache rebuild confirmation"
    );
}

#[test]
fn jj_milestone_hierarchy_and_progress() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }

    let home = unique_tmp_home("jj-milestones");
    let store_path = home.join(".runes").join("stores").join("test");
    let store_path_s = store_path.to_string_lossy().to_string();

    runes_ok(
        &home,
        &[
            "store",
            "init",
            "test",
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
            "test:runes",
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
            "test:runes",
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
            "test:runes",
            "Finalize schema examples",
            "--parent",
            &milestone,
        ],
    );

    runes_ok(
        &home,
        &["edit", &format!("test:{child1}"), "--status", "done"],
    );

    let progress = runes_ok(&home, &["show", &format!("test:{milestone}")]);
    assert!(progress.contains("child_total=2"));
    assert!(progress.contains("child_done=1"));
    assert!(progress.contains("child_todo=1"));
}

#[test]
fn milestone_list_and_project_progress() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }

    let home = unique_tmp_home("jj-milestone-list");
    let store_path = home.join(".runes").join("stores").join("test");
    let store_path_s = store_path.to_string_lossy().to_string();

    runes_ok(
        &home,
        &[
            "store",
            "init",
            "test",
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
            "test:runes",
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
            "test",
            "--project",
            "runes",
            "--kind",
            "milestones",
        ],
    );
    assert!(list_output.contains(&milestone));

    let project_progress = runes_ok(&home, &["show", &format!("test:{milestone}")]);
    assert!(project_progress.contains("milestone \""));
    assert!(project_progress.contains(&milestone));
}

#[test]
fn pijul_issue_lifecycle_with_sdk_observability() {
    if !command_exists("pijul") {
        eprintln!("skipping: pijul not installed");
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

    let store_path = home.join(".runes").join("stores").join("test-pijul");
    let store_path_s = store_path.to_string_lossy().to_string();
    runes_ok(
        &home,
        &[
            "store",
            "init",
            "test-pijul",
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
            "test-pijul:runes",
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

    let issue_log = runes_output(
        &home,
        &["log", &format!("test-pijul:{issue_id}"), "--limit", "5"],
    );
    if !issue_log.status.success() {
        eprintln!(
            "runes log failed (expected for a watchless doc): {}",
            String::from_utf8_lossy(&issue_log.stderr)
        );
    }
}

#[test]
fn pijul_cross_store_move_updates_both_stores() {
    if !command_exists("pijul") {
        eprintln!("skipping: pijul not installed");
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

    let src_path = home.join(".runes").join("stores").join("test-src");
    let dst_path = home.join(".runes").join("stores").join("test-dst");
    runes_ok(
        &home,
        &[
            "store",
            "init",
            "test-src",
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
            "test-dst",
            "--backend",
            "pijul",
            "--path",
            &dst_path.to_string_lossy(),
        ],
    );
    let issue_output = runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test-src:runes",
            "Move me between stores",
        ],
    );
    let issue_id = last_line(&issue_output).to_string();
    runes_ok(
        &home,
        &[
            "move",
            &format!("test-src:{issue_id}"),
            "--project",
            "test-dst:runes",
        ],
    );

    let moved_doc = runes_ok(&home, &["show", &format!("test-dst:{issue_id}")]);
    assert!(moved_doc.contains("Move me between stores"));

    let source_show = runes_output(&home, &["show", &format!("test-src:{issue_id}")]);
    assert!(
        !source_show.status.success(),
        "issue unexpectedly still present in source store"
    );

    let dst_list = runes_ok(
        &home,
        &["list", "--store", "test-dst", "--project", "runes"],
    );
    assert!(dst_list.contains(&issue_id));
}

// --- runes-ph7: robust and predictable history tests ---

fn setup_jj_store(test_name: &str) -> (PathBuf, String) {
    let home = unique_tmp_home(test_name);
    let store_path = home.join(".runes").join("stores").join("test");
    let store_path_s = store_path.to_string_lossy().to_string();
    runes_ok(
        &home,
        &[
            "store",
            "init",
            "test",
            "--backend",
            "jj",
            "--path",
            &store_path_s,
            "--default",
        ],
    );
    (home, store_path_s)
}

/// Cleared before each run: the test runner may itself be an agent shell.
const AGENT_ENV_VARS: &[&str] = &[
    "RUNES_AGENT",
    "AI_AGENT",
    "AGENT",
    "CLAUDECODE",
    "GEMINI_CLI",
    "CODEX_SANDBOX",
    "CODEX_THREAD_ID",
    "CURSOR_AGENT",
    "CURSOR_EXTENSION_HOST_ROLE",
    "AUGMENT_AGENT",
    "OPENCODE",
    "OPENCODE_CLIENT",
    "JUNIE_DATA",
    "JUNIE_SHIM_PATH",
    "CLINE_ACTIVE",
];

/// Run runes with RUNES_USER and all agent markers unset, then apply `envs`.
fn runes_no_user(home: &Path, envs: &[(&str, &str)], args: &[&str]) -> String {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_runes"));
    cmd.args(args).env("HOME", home).env_remove("RUNES_USER");
    for var in AGENT_ENV_VARS {
        cmd.env_remove(var);
    }
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
    let store_path = home.join(".runes").join("stores").join("test");
    let store_path_s = store_path.to_string_lossy().to_string();
    runes_ok(
        &home,
        &[
            "store",
            "init",
            "test",
            "--backend",
            "pijul",
            "--path",
            &store_path_s,
            "--default",
        ],
    );
    Some((home, store_path_s))
}

/// Test: new rune → show has created_at/created_by, no extra annotations
#[test]
fn jj_show_new_rune_has_created_metadata() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-show-new");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Test rune"],
    ))
    .to_string();
    let shown = runes_ok(&home, &["show", &format!("test:{id}")]);
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
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-comment-attr");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Comment test"],
    ))
    .to_string();
    runes_ok(
        &home,
        &["comment", &format!("test:{id}"), "-m", "This is a comment"],
    );
    let shown = runes_ok(&home, &["show", &format!("test:{id}")]);
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

/// Test: with RUNES_USER unset, an agent shell is attributed to the agent
/// identity, on behalf of the configured human; the detect knob turns it off.
#[test]
fn jj_agent_attribution() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("jj-agent-attr");
    runes_ok(
        &home,
        &[
            "config",
            "set",
            "user.email",
            "human@example.com",
            "--global",
        ],
    );

    // Both vars set, as a real Claude Code shell exports them: the canonical
    // marker must win over the version-stamped generic value.
    let id = last_line(&runes_no_user(
        &home,
        &[
            ("CLAUDECODE", "1"),
            ("AI_AGENT", "claude-code_2-1-218_agent"),
        ],
        &["new", "--project", "test:proj", "Agent attribution"],
    ))
    .to_string();
    let shown = runes_ok(&home, &["show", &format!("test:{id}")]);
    assert!(
        shown.contains("created_by \"claude (on behalf of human@example.com)\""),
        "missing agent attribution: {shown}"
    );
    let authors = command_ok(
        &home,
        "jj",
        &[
            "log",
            "--no-graph",
            "-r",
            "all()",
            "-T",
            r#"author.email() ++ "\n""#,
        ],
        Some(Path::new(&store_path)),
    );
    assert!(
        authors.contains("claude@agents.localhost"),
        "commit not authored by the agent: {authors}"
    );

    runes_no_user(
        &home,
        &[("CLAUDECODE", "1")],
        &["comment", &format!("test:{id}"), "-m", "Agent comment"],
    );
    let shown = runes_ok(&home, &["show", &format!("test:{id}")]);
    assert!(
        shown.contains("by claude (on behalf of human@example.com)"),
        "missing on-behalf-of attribution: {shown}"
    );

    // Detection disabled → commits fall back to the configured human identity
    runes_ok(
        &home,
        &["config", "set", "attribution.detect", "false", "--global"],
    );
    let id = last_line(&runes_no_user(
        &home,
        &[("CLAUDECODE", "1")],
        &["new", "--project", "test:proj", "Detection disabled"],
    ))
    .to_string();
    let shown = runes_ok(&home, &["show", &format!("test:{id}")]);
    assert!(
        shown.contains("created_by \"human@example.com\"") && !shown.contains("claude"),
        "detection knob ignored: {shown}"
    );
}

/// Test: new + edit description → section annotation appears
#[test]
fn jj_show_section_edit_annotation() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("jj-section-edit");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Section test"],
    ))
    .to_string();
    // Edit the file directly to add content to Description section
    let store = Path::new(&store_path);
    let doc_path = find_rune_file(store, &id);
    let content = fs::read_to_string(&doc_path).expect("read doc");
    let updated = content.replace(
        "## Description\n",
        "## Description\n\nNew description content here.\n",
    );
    fs::write(&doc_path, &updated).expect("write doc");
    runes_ok(
        &home,
        &["commit", &format!("test:{id}"), "-m", "Update description"],
    );
    let shown = runes_ok(&home, &["show", &format!("test:{id}")]);
    assert!(
        shown.contains("Edited by"),
        "missing section annotation: {shown}"
    );
}

/// Test: show uncommitted rune has red "<not committed>"
#[test]
fn jj_show_uncommitted_rune() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-uncommitted");
    let id = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:proj",
            "Uncommitted",
            "--no-commit",
        ],
    ))
    .to_string();
    let shown = runes_ok(&home, &["show", &format!("test:{id}")]);
    assert!(
        shown.contains("<not committed>"),
        "missing uncommitted indicator: {shown}"
    );
}

/// Test: show pending changes on a section
#[test]
fn jj_show_pending_section_changes() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("jj-pending");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Pending test"],
    ))
    .to_string();
    let store = Path::new(&store_path);
    let doc_path = find_rune_file(store, &id);
    // Edit Description section without committing
    let content = fs::read_to_string(&doc_path).expect("read doc");
    let updated = content.replace(
        "## Description\n",
        "## Description\n\nUncommitted change.\n",
    );
    fs::write(&doc_path, &updated).expect("write doc");
    let shown = runes_ok(&home, &["show", &format!("test:{id}")]);
    assert!(
        shown.contains("pending uncommitted changes"),
        "missing pending annotation: {shown}"
    );
}

/// Test: log associates runes via changed_files, not description
#[test]
fn jj_log_uses_changed_files_not_description() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-log-files");
    // Create two runes with --no-commit, then commit together
    let id1 = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Rune one", "--no-commit"],
    ))
    .to_string();
    let id2 = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Rune two", "--no-commit"],
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
    let (home, _) = match setup_pijul_store("pijul-show-new") {
        Some(v) => v,
        None => return,
    };
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Pijul test"],
    ))
    .to_string();
    let shown = runes_ok(&home, &["show", &format!("test:{id}")]);
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
    let (home, _) = match setup_pijul_store("pijul-log-files") {
        Some(v) => v,
        None => return,
    };
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Pijul log test"],
    ))
    .to_string();
    let log_json = runes_ok(&home, &["log", "--all", "--json"]);
    assert!(log_json.contains(&id), "pijul log missing {id}: {log_json}");
}

/// Test: rename preserves basic show functionality (file is findable by ID after rename)
#[test]
fn jj_rename_preserves_history() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-rename");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Original title"],
    ))
    .to_string();
    // Edit the title which changes the filename slug
    runes_ok(
        &home,
        &["edit", &format!("test:{id}"), "--title", "Renamed title"],
    );
    // Show still works with same ID after rename
    let shown = runes_ok(&home, &["show", &format!("test:{id}")]);
    assert!(
        shown.contains("Renamed title"),
        "title not updated: {shown}"
    );
    assert!(shown.contains("created_by"), "missing created_by: {shown}");
    assert!(shown.contains("created_at"), "missing created_at: {shown}");
}

/// Position of a rune ID in command output, for rank assertions.
fn rank_of(output: &str, id: &str) -> usize {
    output
        .find(id)
        .unwrap_or_else(|| panic!("{id} missing from output:\n{output}"))
}

/// Test: search matches body text (including comments), spans every status,
/// ranks title hits first, and honours project/archive scoping.
#[test]
fn search_matches_body_and_closed_runes() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("search-fts");

    let body_match = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Fix the auth flow"],
    ))
    .to_string();
    runes_ok(
        &home,
        &[
            "comment",
            &format!("test:{body_match}"),
            "-m",
            "users cannot login after a redirect",
        ],
    );
    // A done rune must still be findable — that is the point of search.
    runes_ok(
        &home,
        &["edit", &format!("test:{body_match}"), "--status", "done"],
    );
    let title_match = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Login page redesign"],
    ))
    .to_string();
    let other_project = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:other", "Login provider setup"],
    ))
    .to_string();

    let found = runes_ok(&home, &["search", "login", "--project", "proj"]);
    assert!(
        found.contains(&body_match),
        "body-only match missing: {found}"
    );
    assert!(found.contains(&title_match), "title match missing: {found}");
    assert!(
        !found.contains(&other_project),
        "search leaked another project: {found}"
    );
    assert!(
        rank_of(&found, &title_match) < rank_of(&found, &body_match),
        "title match should rank above body-only match: {found}"
    );

    let none = runes_ok(&home, &["search", "kubernetes", "--project", "proj"]);
    assert!(none.contains("No runes match"), "unexpected output: {none}");

    // --project '' searches every project in the store.
    let all_projects = runes_ok(&home, &["search", "login", "--project", ""]);
    assert!(
        all_projects.contains(&other_project),
        "--project '' should search all projects: {all_projects}"
    );

    let json = runes_ok(&home, &["search", "login", "--project", "proj", "--json"]);
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).expect("parse search json");
    let ids: Vec<&str> = parsed.iter().filter_map(|r| r["id"].as_str()).collect();
    assert_eq!(
        ids,
        vec![title_match.as_str(), body_match.as_str()],
        "json rows should mirror ranked list output: {json}"
    );
    assert_eq!(parsed[1]["status"].as_str(), Some("done"));

    // Archived runes are excluded by default and opt-in via --with-archived.
    runes_ok(&home, &["archive", &format!("test:{title_match}")]);
    let default_scope = runes_ok(&home, &["search", "login", "--project", "proj"]);
    assert!(
        !default_scope.contains(&title_match),
        "archived rune should be hidden by default: {default_scope}"
    );
    let with_archived = runes_ok(
        &home,
        &["search", "login", "--project", "proj", "--with-archived"],
    );
    assert!(
        with_archived.contains(&title_match),
        "--with-archived should include archived runes: {with_archived}"
    );
}

/// Test: a cache without the search index (built by an older binary) is rebuilt
/// on demand, and `store doctor` regenerates the index too.
#[test]
fn search_rebuilds_cache_without_index() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("search-reindex");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Rune about telemetry"],
    ))
    .to_string();

    let cache_file = home.join(".runes").join("cache").join("test.sqlite");
    let conn = rusqlite::Connection::open(&cache_file).expect("open cache");
    conn.execute_batch("DROP TABLE rune_fts;")
        .expect("drop fts");
    drop(conn);

    let found = runes_ok(&home, &["search", "telemetry", "--project", "proj"]);
    assert!(found.contains(&id), "index not rebuilt on demand: {found}");

    runes_ok(&home, &["store", "doctor", "test"]);
    let after_doctor = runes_ok(&home, &["search", "telemetry", "--project", "proj"]);
    assert!(
        after_doctor.contains(&id),
        "doctor did not regenerate the index: {after_doctor}"
    );
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
