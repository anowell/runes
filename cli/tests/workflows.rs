use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

fn runes_output_with_env(home: &Path, envs: &[(&str, &str)], args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_runes"));
    cmd.args(args)
        .env("HOME", home)
        .env("RUNES_USER", "Test User <test@runes.dev>");
    for (key, value) in envs {
        cmd.env(key, value);
    }
    cmd.output().expect("run runes command")
}

fn runes_with_env(home: &Path, envs: &[(&str, &str)], args: &[&str]) -> String {
    let output = runes_output_with_env(home, envs, args);
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
    // `in-progress` is input sugar for `wip`, and is never written back out.
    assert!(shown.contains("status \"wip\""), "status not updated");
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
        &[
            "edit",
            &format!("test:{child1}"),
            "--status",
            "closed:duplicate",
        ],
    );

    // Rollups count by core state, so a substate still counts as closed.
    let progress = runes_ok(&home, &["show", &format!("test:{milestone}")]);
    assert!(progress.contains("child_total=2"));
    assert!(progress.contains("child_closed=1"));
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

/// Shared assertions: the rune/project filter must be applied before `--limit`
/// truncates the history, and an empty result must say so.
fn assert_log_filters_before_limit(home: &Path) {
    let old = last_line(&runes_ok(
        home,
        &["new", "--project", "test:proj", "Oldest rune"],
    ))
    .to_string();
    // Bury the old rune's only commit well past the default limit of 50
    for i in 0..55 {
        runes_ok(
            home,
            &["new", "--project", "test:proj", &format!("Filler {i}")],
        );
    }

    let log = runes_ok(home, &["log", &format!("test:{old}"), "--no-pager"]);
    assert!(
        log.contains(&old),
        "log for {old} missing its commit: {log}"
    );
    let log_json = runes_ok(home, &["log", &format!("test:{old}"), "--json"]);
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&log_json).expect("parse json");
    assert_eq!(parsed.len(), 1, "expected one entry: {log_json}");

    // Project log limits *matching* commits, not raw commits walked
    let project_log = runes_ok(
        home,
        &["log", "--project", "proj", "--limit", "3", "--no-pager"],
    );
    assert_eq!(
        project_log.lines().count(),
        3,
        "project log should emit exactly 3 rows: {project_log}"
    );

    let empty = runes_ok(
        home,
        &["log", "--changed-by", "nobody@example.com", "--no-pager"],
    );
    assert!(
        empty.contains("No matching changes"),
        "empty log should say so: {empty}"
    );
    let empty_json = runes_ok(
        home,
        &["log", "--changed-by", "nobody@example.com", "--json"],
    );
    assert_eq!(empty_json.trim(), "[]", "empty json log: {empty_json}");
}

/// Test: jj - rune older than the default limit still shows its history
#[test]
fn jj_log_filters_before_limit() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-log-filter-limit");
    assert_log_filters_before_limit(&home);
}

/// Test: pijul - rune older than the default limit still shows its history
#[test]
fn pijul_log_filters_before_limit() {
    let (home, _) = match setup_pijul_store("pijul-log-filter-limit") {
        Some(v) => v,
        None => return,
    };
    assert_log_filters_before_limit(&home);
}

/// Shared assertions: `--limit` counts matching *commits* in both output modes,
/// so a commit touching several runes emits all its rows but is counted once.
fn assert_log_limit_counts_commits(home: &Path) {
    let solo = last_line(&runes_ok(
        home,
        &["new", "--project", "test:proj", "Solo rune"],
    ))
    .to_string();
    let mut bulk_ids = Vec::new();
    for i in 0..3 {
        bulk_ids.push(
            last_line(&runes_ok(
                home,
                &[
                    "new",
                    "--project",
                    "test:proj",
                    &format!("Bulk rune {i}"),
                    "--no-commit",
                ],
            ))
            .to_string(),
        );
    }
    runes_ok(
        home,
        &["commit", "--project", "proj", "-m", "Touch three runes"],
    );

    // Text mode: the newest commit spends 1 of the limit but prints all 3 rows
    let text = runes_ok(
        home,
        &["log", "--project", "proj", "--limit", "1", "--no-pager"],
    );
    assert_eq!(
        text.lines().count(),
        3,
        "one commit touching 3 runes should print 3 rows: {text}"
    );
    for id in &bulk_ids {
        assert!(text.contains(id.as_str()), "missing {id}: {text}");
    }
    assert!(!text.contains(&solo), "limit 1 leaked older commit: {text}");

    // JSON mode: same limit, one entry listing all 3 runes
    let json = runes_ok(
        home,
        &["log", "--project", "proj", "--limit", "1", "--json"],
    );
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).expect("parse json");
    assert_eq!(parsed.len(), 1, "expected one commit: {json}");
    let runes = parsed[0]["runes"].as_array().expect("runes array");
    assert_eq!(runes.len(), 3, "expected 3 runes on the commit: {json}");

    // Both modes advance the limit at the same rate: 2 commits = 3 + 1 rows
    let text2 = runes_ok(
        home,
        &["log", "--project", "proj", "--limit", "2", "--no-pager"],
    );
    assert_eq!(
        text2.lines().count(),
        4,
        "2 commits (3 runes + 1 rune) should print 4 rows: {text2}"
    );
    let json2 = runes_ok(
        home,
        &["log", "--project", "proj", "--limit", "2", "--json"],
    );
    let parsed2: Vec<serde_json::Value> = serde_json::from_str(&json2).expect("parse json");
    assert_eq!(parsed2.len(), 2, "expected two commits: {json2}");
    assert!(text2.contains(&solo), "older commit missing: {text2}");
}

/// Test: jj - --limit counts commits, not rune rows
#[test]
fn jj_log_limit_counts_commits() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-log-limit-commits");
    assert_log_limit_counts_commits(&home);
}

/// Test: pijul - --limit counts commits, not rune rows
#[test]
fn pijul_log_limit_counts_commits() {
    let (home, _) = match setup_pijul_store("pijul-log-limit-commits") {
        Some(v) => v,
        None => return,
    };
    assert_log_limit_counts_commits(&home);
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

// --- milestone --json, draft location, stale draft cleanup, broken pipes ---

fn drafts_dir(home: &Path, store: &str, project: &str) -> PathBuf {
    home.join(".runes").join("drafts").join(store).join(project)
}

fn list_drafts(dir: &Path) -> Vec<String> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .collect();
    names.sort();
    names
}

fn write_editor_script(home: &Path, name: &str, body: &str) -> PathBuf {
    let path = home.join(name);
    fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("write editor script");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod editor script");
    path
}

/// Run the CLI under a pty, for paths that only open an editor on a terminal.
fn pty_runes(home: &Path, envs: &[(&str, &str)], args: &[&str]) -> Output {
    let exe = env!("CARGO_BIN_EXE_runes");
    let mut cmd = Command::new("script");
    if cfg!(target_os = "linux") {
        let script = std::iter::once(exe)
            .chain(args.iter().copied())
            .collect::<Vec<_>>()
            .join(" ");
        cmd.args(["-q", "-e", "-c", &script, "/dev/null"]);
    } else {
        cmd.args(["-q", "/dev/null", exe]).args(args);
    }
    cmd.env("HOME", home)
        .env("RUNES_USER", "Test User <test@runes.dev>")
        .stdin(Stdio::null());
    for (key, value) in envs {
        cmd.env(key, value);
    }
    cmd.output().expect("run runes under pty")
}

#[test]
fn milestone_list_json_includes_child_rollup() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-milestone-json");
    let milestone = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:runes",
            "Dual backend",
            "--id",
            "m03",
            "--kind",
            "milestone",
        ],
    ))
    .to_string();
    let child = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:runes",
            "Wire jj-lib",
            "--parent",
            &milestone,
        ],
    ))
    .to_string();
    runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:runes",
            "Wire libpijul",
            "--parent",
            &milestone,
        ],
    );
    runes_ok(
        &home,
        &["edit", &format!("test:{child}"), "--status", "done"],
    );

    let out = runes_ok(
        &home,
        &[
            "list",
            "--store",
            "test",
            "--project",
            "runes",
            "--kind",
            "milestones",
            "--json",
        ],
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&out).unwrap_or_else(|e| panic!("invalid json: {e}\n{out}"));
    let rows = parsed.as_array().expect("json array");
    assert_eq!(rows.len(), 1, "unexpected rows: {out}");
    let row = &rows[0];
    assert_eq!(row["kind"].as_str(), Some("milestone"));
    assert_eq!(row["id"].as_str(), Some(milestone.as_str()));
    assert_eq!(row["title"].as_str(), Some("Dual backend"));
    assert_eq!(row["store"].as_str(), Some("test"));
    assert_eq!(row["project"].as_str(), Some("runes"));
    assert_eq!(row["status"].as_str(), Some("todo"));
    assert_eq!(row["archived"].as_bool(), Some(false));
    assert!(row["labels"].is_array(), "labels not an array: {row}");
    assert!(
        row["path"]
            .as_str()
            .unwrap_or_default()
            .ends_with("_milestone.md"),
        "unexpected path: {row}"
    );
    assert_eq!(row["child_total"].as_u64(), Some(2));
    assert_eq!(row["child_closed"].as_u64(), Some(1));
    assert_eq!(row["child_wip"].as_u64(), Some(0));
    assert_eq!(row["child_todo"].as_u64(), Some(1));
    assert_eq!(row["complete_pct"].as_f64(), Some(50.0));

    // Empty listings stay parseable rather than erroring out
    let empty = runes_ok(
        &home,
        &[
            "list",
            "--store",
            "test",
            "--project",
            "nomiles",
            "--kind",
            "milestones",
            "--json",
        ],
    );
    assert_eq!(empty.trim(), "[]");
}

/// A failed editor edit keeps its draft under `~/.runes/drafts/<store>/<project>/`,
/// `-f` reapplies it, and a successful edit clears that rune's stale drafts.
#[test]
fn edit_draft_survives_failure_then_is_pruned_on_recovery() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("jj-edit-drafts");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Draft me"],
    ))
    .to_string();
    let target = format!("test:{id}");

    let editor = write_editor_script(
        &home,
        "bad-editor.sh",
        &format!(
            "cat > \"$1\" <<'EOF'\n---\ntask \"{id}\" {{\n  status \"bogus\"\n}}\n---\n\n# Draft me\n\nrecovered body\nEOF"
        ),
    );
    let output = runes_output_with_env(
        &home,
        &[("EDITOR", editor.to_str().expect("editor path"))],
        &["edit", &target, "-e"],
    );
    assert!(
        !output.status.success(),
        "invalid status should fail the edit"
    );

    let drafts = drafts_dir(&home, "test", "proj");
    let saved = list_drafts(&drafts);
    assert_eq!(saved.len(), 1, "expected a kept draft, found {saved:?}");
    let draft = drafts.join(&saved[0]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&draft.display().to_string()),
        "recovery hint missing draft path: {stderr}"
    );

    // A leftover from an earlier aborted session should not survive the next edit
    let stale = drafts.join(format!("{id}--0000000--draft-me.md"));
    fs::write(&stale, "abandoned").expect("write stale draft");

    let fixed = fs::read_to_string(&draft)
        .expect("read draft")
        .replace("\"bogus\"", "\"closed:canceled\"");
    fs::write(&draft, fixed).expect("rewrite draft");
    runes_ok(
        &home,
        &["edit", &target, "-f", draft.to_str().expect("draft path")],
    );

    let doc = fs::read_to_string(find_rune_file(Path::new(&store_path), &id)).expect("read rune");
    assert!(
        doc.contains("status \"closed:canceled\""),
        "draft status not applied: {doc}"
    );
    assert!(
        doc.contains("recovered body"),
        "draft body not applied: {doc}"
    );
    assert_eq!(
        doc.matches(&format!("task \"{id}\"")).count(),
        1,
        "draft frontmatter was nested into the body: {doc}"
    );
    assert!(
        list_drafts(&drafts).is_empty(),
        "drafts left behind: {:?}",
        list_drafts(&drafts)
    );
}

#[test]
fn store_doctor_prunes_aged_drafts() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-draft-prune");
    let drafts = drafts_dir(&home, "test", "proj");
    fs::create_dir_all(&drafts).expect("create drafts dir");
    let aged = drafts.join("proj-old--1111111--ancient.md");
    let recent = drafts.join("proj-new--2222222--recent.md");
    fs::write(&aged, "abandoned in April").expect("write aged draft");
    fs::write(&recent, "still useful").expect("write recent draft");
    fs::OpenOptions::new()
        .write(true)
        .open(&aged)
        .expect("open aged draft")
        .set_modified(SystemTime::now() - Duration::from_secs(40 * 24 * 60 * 60))
        .expect("backdate aged draft");

    let out = runes_ok(&home, &["store", "doctor", "test"]);
    assert!(out.contains("Pruned 1 draft"), "no prune reported: {out}");
    assert!(!aged.exists(), "aged draft was not pruned");
    assert!(recent.exists(), "recent draft must stay recoverable");
}

#[test]
fn comment_editor_draft_lives_under_runes_drafts() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    if !command_exists("script") {
        eprintln!("skipping: no script(1) to provide a pty");
        return;
    }
    let (home, _) = setup_jj_store("jj-comment-draft");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Comment target"],
    ))
    .to_string();
    let target = format!("test:{id}");
    let recorded = home.join("draft-path.txt");
    let editor = write_editor_script(
        &home,
        "comment-editor.sh",
        &format!(
            "echo \"$1\" > {}\necho 'typed in the editor' > \"$1\"",
            recorded.display()
        ),
    );
    let output = pty_runes(
        &home,
        &[("EDITOR", editor.to_str().expect("editor path"))],
        &["comment", &target],
    );
    let used = fs::read_to_string(&recorded).unwrap_or_else(|e| {
        panic!(
            "editor never ran: {e}\nstdout:\n{}",
            String::from_utf8_lossy(&output.stdout)
        )
    });
    let drafts = drafts_dir(&home, "test", "proj");
    assert!(
        used.trim()
            .starts_with(drafts.to_str().expect("drafts dir")),
        "comment draft not under {}: {used}",
        drafts.display()
    );
    assert!(
        used.contains(&id),
        "comment draft not named for the rune: {used}"
    );

    let shown = runes_ok(&home, &["show", &target]);
    assert!(
        shown.contains("typed in the editor"),
        "comment missing: {shown}"
    );
    assert!(
        list_drafts(&drafts).is_empty(),
        "comment draft left behind: {:?}",
        list_drafts(&drafts)
    );
}

/// `runes show <id> | head -3` must not panic once the reader goes away.
#[test]
fn output_to_a_closed_pipe_does_not_panic() {
    let home = unique_tmp_home("broken-pipe");
    let mut child = Command::new(env!("CARGO_BIN_EXE_runes"))
        .arg("quickstart")
        .env("HOME", &home)
        .env("RUNES_USER", "Test User <test@runes.dev>")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn runes");
    // Closing the read end mid-stream is what `| head -3` does
    drop(child.stdout.take());
    let output = child.wait_with_output().expect("wait for runes");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("panicked") && !stderr.contains("Broken pipe"),
        "broken pipe surfaced to the user: {stderr}"
    );
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
    // A closed rune must still be findable — that is the point of search.
    runes_ok(
        &home,
        &["edit", &format!("test:{body_match}"), "--status", "closed"],
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
    assert_eq!(parsed[1]["status"].as_str(), Some("closed"));

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

/// Test: the built-in views work with zero config, and a config-defined view
/// still works but warns that custom views are deprecated.
#[test]
fn builtin_views_need_no_config_and_custom_views_warn() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("builtin-views");
    runes_ok(
        &home,
        &["config", "set", "user.email", "test@runes.dev", "--global"],
    );

    let mine = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:proj",
            "Mine and open",
            "--assignee",
            "test@runes.dev",
        ],
    ))
    .to_string();
    let theirs = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:proj",
            "Theirs and open",
            "--assignee",
            "other@runes.dev",
        ],
    ))
    .to_string();
    let closed = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Already finished"],
    ))
    .to_string();
    runes_ok(
        &home,
        &[
            "edit",
            &format!("test:{closed}"),
            "--status",
            "closed:canceled",
        ],
    );
    let reviewing = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:proj",
            "Under review",
            "--status",
            "wip:review",
            "--assignee",
            "test@runes.dev",
        ],
    ))
    .to_string();

    let scope = ["--store", "test", "--project", "proj"];
    let list_view = |view: &[&str]| {
        let mut args = vec!["list"];
        args.extend_from_slice(view);
        args.extend_from_slice(&scope);
        let output = runes_output(&home, &args);
        assert!(
            output.status.success(),
            "runes {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        (
            String::from_utf8(output.stdout).expect("stdout utf8"),
            String::from_utf8(output.stderr).expect("stderr utf8"),
        )
    };

    // Default view is `open`: todo and wip (with substates), never closed.
    let (default_view, default_stderr) = list_view(&[]);
    assert!(default_view.contains(&mine), "default view: {default_view}");
    assert!(
        default_view.contains(&theirs),
        "default view: {default_view}"
    );
    assert!(
        default_view.contains(&reviewing),
        "default view should include wip substates: {default_view}"
    );
    assert!(
        !default_view.contains(&closed),
        "default view should hide closed runes: {default_view}"
    );
    assert!(
        !default_stderr.contains("deprecated"),
        "built-in views must not warn: {default_stderr}"
    );

    let (open_view, _) = list_view(&["open"]);
    assert_eq!(open_view, default_view, "`open` is the default view");

    for all_form in [vec!["all"], vec!["--all"]] {
        let (all_view, _) = list_view(&all_form);
        for id in [&mine, &theirs, &closed, &reviewing] {
            assert!(
                all_view.contains(id),
                "`list {}` missing {id}: {all_view}",
                all_form.join(" ")
            );
        }
    }

    // `closed` covers every closed substate.
    let (closed_view, _) = list_view(&["closed"]);
    assert!(
        closed_view.contains(&closed),
        "`list closed` missing the closed:canceled rune: {closed_view}"
    );
    assert!(
        !closed_view.contains(&mine) && !closed_view.contains(&theirs),
        "`list closed` leaked open runes: {closed_view}"
    );

    let (mine_view, _) = list_view(&["mine"]);
    assert!(
        mine_view.contains(&mine) && mine_view.contains(&reviewing),
        "`list mine` missing my runes: {mine_view}"
    );
    assert!(
        !mine_view.contains(&theirs) && !mine_view.contains(&closed),
        "`list mine` should be open runes assigned to me: {mine_view}"
    );

    // A config-defined view still applies, but says it is on the way out.
    runes_ok(
        &home,
        &["config", "set", "query.finished.status", "done", "--global"],
    );
    let (custom_view, custom_stderr) = list_view(&["finished"]);
    assert!(
        custom_view.contains(&closed) && !custom_view.contains(&mine),
        "custom view should still filter: {custom_view}"
    );
    assert!(
        custom_stderr.contains("custom views are deprecated"),
        "custom view should warn: {custom_stderr}"
    );

    // A custom view named like a built-in shadows it, and warns as well.
    runes_ok(
        &home,
        &["config", "set", "query.closed.status", "todo", "--global"],
    );
    let (shadowed, shadow_stderr) = list_view(&["closed"]);
    assert!(
        shadowed.contains(&mine) && !shadowed.contains(&closed),
        "config view should shadow the built-in: {shadowed}"
    );
    assert!(
        shadow_stderr.contains("custom views are deprecated"),
        "shadowing view should warn: {shadow_stderr}"
    );
}

/// Test: built-in views are discoverable from `list --help` and quickstart.
#[test]
fn builtin_views_are_documented_in_help_and_quickstart() {
    let home = unique_tmp_home("builtin-views-help");
    let help = runes_ok(&home, &["list", "--help"]);
    let quickstart = runes_ok(&home, &["quickstart"]);
    for view in ["open", "mine", "all", "closed"] {
        assert!(help.contains(view), "`list --help` missing {view}: {help}");
        assert!(
            quickstart.contains(&format!("runes list {view}")),
            "quickstart missing {view}: {quickstart}"
        );
    }
    assert!(
        help.contains("Built-in views"),
        "help should label the views: {help}"
    );
}

/// Test: committing one rune leaves another dirty rune uncommitted
#[test]
fn jj_commit_is_scoped_to_requested_paths() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("jj-commit-scope");
    let store = Path::new(&store_path);
    let id_a = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Rune A"],
    ))
    .to_string();
    let id_b = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Rune B"],
    ))
    .to_string();
    let path_a = find_rune_file(store, &id_a);
    let path_b = find_rune_file(store, &id_b);

    // Dirty both runes without committing
    for path in [&path_a, &path_b] {
        let content = fs::read_to_string(path).expect("read doc");
        let updated = content.replace("## Description\n", "## Description\n\nEdited.\n");
        assert_ne!(content, updated, "description marker not found");
        fs::write(path, updated).expect("write doc");
    }

    // Commit only rune A
    runes_ok(
        &home,
        &["commit", &format!("test:{id_a}"), "-m", "Scoped commit"],
    );

    let name_a = path_a.file_name().unwrap().to_string_lossy().to_string();
    let name_b = path_b.file_name().unwrap().to_string_lossy().to_string();
    let summary = command_ok(&home, "jj", &["diff", "-r", "@-", "--summary"], Some(store));
    assert!(
        summary.contains(&name_a),
        "commit missing rune A ({name_a}): {summary}"
    );
    assert!(
        !summary.contains(&name_b),
        "commit swept in rune B ({name_b}): {summary}"
    );

    let described = command_ok(
        &home,
        "jj",
        &[
            "log",
            "--no-graph",
            "-r",
            "@-",
            "-T",
            r#"description ++ "|" ++ author.email()"#,
        ],
        Some(store),
    );
    assert!(
        described.contains("Scoped commit"),
        "unexpected description: {described}"
    );
    assert!(described.contains('@'), "missing author email: {described}");

    let diff_a = runes_ok(&home, &["diff", &format!("test:{id_a}")]);
    assert!(diff_a.trim().is_empty(), "rune A still dirty: {diff_a}");
    let diff_b = runes_ok(&home, &["diff", &format!("test:{id_b}")]);
    assert!(
        diff_b.contains("Edited."),
        "rune B lost its pending change: {diff_b}"
    );
    let content_b = fs::read_to_string(&path_b).expect("read doc b");
    assert!(content_b.contains("Edited."), "rune B file was reverted");

    // Re-committing the now-clean rune A must not record rune B under A's message
    runes_ok(
        &home,
        &["commit", &format!("test:{id_a}"), "-m", "Should be a no-op"],
    );
    let head = command_ok(
        &home,
        "jj",
        &["log", "--no-graph", "-r", "@-", "-T", "description"],
        Some(store),
    );
    assert!(
        head.contains("Scoped commit"),
        "no-op commit recorded rune B: {head}"
    );

    // Bare `runes commit` still sweeps up everything in scope
    runes_ok(&home, &["commit", "-m", "Record the rest"]);
    let diff_b = runes_ok(&home, &["diff", &format!("test:{id_b}")]);
    assert!(
        diff_b.trim().is_empty(),
        "bare commit left rune B dirty: {diff_b}"
    );
}

/// Test: `edit -f` with a full doc (as printed by `show`) replaces metadata and
/// body without duplicating frontmatter, and stays stable across round-trips
#[test]
fn jj_edit_file_accepts_full_doc() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("jj-edit-full-doc");
    let store_path = PathBuf::from(store_path);
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Round trip me"],
    ))
    .to_string();
    let target = format!("test:{id}");
    let input = home.join("full-doc.md");
    let input_s = input.to_string_lossy().to_string();

    // Copy the whole doc, edit metadata and body, feed it back
    let shown = runes_ok(&home, &["show", &target]);
    let edited = shown
        .replace(
            "status \"todo\"",
            "status \"in-progress\"\n  labels \"roundtrip\"",
        )
        .replace("## Description", "## Description\n\nEdited in place.");
    fs::write(&input, &edited).expect("write full doc");
    runes_ok(&home, &["edit", &target, "-f", &input_s]);

    let stored = fs::read_to_string(find_rune_file(&store_path, &id)).expect("read rune doc");
    assert_eq!(
        stored.lines().filter(|line| line.trim() == "---").count(),
        2,
        "duplicated frontmatter: {stored}"
    );
    // The full doc's legacy `in-progress` normalizes on the way in, like any other input.
    assert!(
        stored.contains("status \"wip\"") && stored.contains("labels \"roundtrip\""),
        "metadata changes not applied: {stored}"
    );
    assert!(
        stored.contains("Edited in place."),
        "body changes not applied: {stored}"
    );
    assert!(
        !stored.contains("created_by") && !stored.contains("created_at"),
        "show-only metadata leaked into the doc: {stored}"
    );

    // Feeding the stored doc back unchanged is a no-op (no whitespace drift)
    fs::write(&input, &stored).expect("write full doc");
    runes_ok(&home, &["edit", &target, "-f", &input_s]);
    let restored = fs::read_to_string(find_rune_file(&store_path, &id)).expect("read rune doc");
    assert_eq!(stored, restored, "round-trip is not stable");

    // Body-only input still replaces just the body
    fs::write(&input, "# Round trip me\n\nBody only.\n").expect("write body");
    runes_ok(&home, &["edit", &target, "-f", &input_s]);
    let body_only = fs::read_to_string(find_rune_file(&store_path, &id)).expect("read rune doc");
    assert!(
        body_only.contains("status \"wip\"") && body_only.contains("labels \"roundtrip\""),
        "body-only input dropped metadata: {body_only}"
    );
    assert!(
        body_only.contains("Body only.") && !body_only.contains("Edited in place."),
        "body-only input did not replace the body: {body_only}"
    );

    // A full doc carrying the pre-substate vocabulary lands as the core state
    fs::write(
        &input,
        body_only.replace("status \"wip\"", "status \"done\""),
    )
    .expect("write full doc");
    runes_ok(&home, &["edit", &target, "-f", &input_s]);
    let closed = fs::read_to_string(find_rune_file(&store_path, &id)).expect("read rune doc");
    assert!(
        closed.contains("status \"closed\"") && !closed.contains("\"done\""),
        "legacy status in a full doc was not normalized: {closed}"
    );
}

/// Test: `edit -f` full-doc input is validated against the target and the schema
#[test]
fn jj_edit_file_full_doc_validation() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("jj-edit-full-doc-invalid");
    let id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Validate me"],
    ))
    .to_string();
    let target = format!("test:{id}");
    let shown = runes_ok(&home, &["show", &target]);
    let input = home.join("full-doc.md");
    let input_s = input.to_string_lossy().to_string();

    let cases = [
        (
            shown.replace(&id, "proj-zzz"),
            vec![id.as_str(), "proj-zzz"],
        ),
        (
            shown.replace("status \"todo\"", "status \"bogus\""),
            vec!["Invalid status", "bogus", "todo, wip, closed"],
        ),
        // Substates go through the same allowlist as `--status`
        (
            shown.replace("status \"todo\"", "status \"wip:qa\""),
            vec!["Invalid status", "wip, wip:design, wip:impl, wip:review"],
        ),
        (
            shown.replace("task \"", "epic \""),
            vec!["Invalid kind", "epic"],
        ),
    ];
    for (contents, expected) in cases {
        fs::write(&input, &contents).expect("write full doc");
        let output = runes_output(&home, &["edit", &target, "-f", &input_s]);
        assert!(
            !output.status.success(),
            "expected failure for input:\n{contents}"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        for needle in expected {
            assert!(
                stderr.contains(needle),
                "missing {needle} in stderr: {stderr}"
            );
        }
    }
}

/// Test: `new -f` with a full doc adopts its fields under a fresh id
#[test]
fn jj_new_file_accepts_full_doc() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("jj-new-full-doc");
    let store_path = PathBuf::from(store_path);
    let id = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:proj",
            "Template rune",
            "--kind",
            "bug",
            "--status",
            "wip:review",
            "--label",
            "copied",
        ],
    ))
    .to_string();
    let input = home.join("full-doc.md");
    let input_s = input.to_string_lossy().to_string();
    fs::write(&input, runes_ok(&home, &["show", &format!("test:{id}")])).expect("write full doc");

    // Frontmatter fields are defaults, and the CLI still owns the id
    let copy_id = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Copy", "-f", &input_s],
    ))
    .to_string();
    assert_ne!(copy_id, id, "id from input frontmatter was reused");
    let copy = fs::read_to_string(find_rune_file(&store_path, &copy_id)).expect("read copy");
    assert!(
        copy.contains(&format!("bug \"{copy_id}\"")),
        "kind or id not applied: {copy}"
    );
    assert!(
        copy.contains("status \"wip:review\"") && copy.contains("labels \"copied\""),
        "frontmatter fields not applied: {copy}"
    );
    assert_eq!(
        copy.lines().filter(|line| line.trim() == "---").count(),
        2,
        "duplicated frontmatter: {copy}"
    );

    // Explicit flags win over the input frontmatter
    let flagged_id = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:proj",
            "Copy",
            "-f",
            &input_s,
            "--status",
            "done",
            "--label",
            "extra",
        ],
    ))
    .to_string();
    let flagged = fs::read_to_string(find_rune_file(&store_path, &flagged_id)).expect("read copy");
    // `done` is input sugar for `closed`, and still beats the frontmatter
    assert!(
        flagged.contains("status \"closed\""),
        "--status did not override frontmatter: {flagged}"
    );
    assert!(
        flagged.contains("labels \"extra\" \"copied\""),
        "labels not merged: {flagged}"
    );

    // Schema validation still applies to the supplied frontmatter
    fs::write(
        &input,
        fs::read_to_string(&input)
            .expect("read input")
            .replace("status \"wip:review\"", "status \"bogus\""),
    )
    .expect("write full doc");
    let output = runes_output(
        &home,
        &["new", "--project", "test:proj", "Copy", "-f", &input_s],
    );
    assert!(!output.status.success(), "invalid status was accepted");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid status") && stderr.contains("bogus"),
        "unexpected stderr: {stderr}"
    );
}

/// Test: `show` decorates the body with a dep list, per-heading edit annotations and
/// comment attributions; feeding that back through `edit -f` leaves the stored doc
/// byte-identical, twice over, so nothing accumulates
#[test]
fn jj_edit_file_full_doc_round_trip_is_byte_stable() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("jj-edit-full-doc-stable");
    let store_path = PathBuf::from(store_path);
    let dep = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Dependency"],
    ))
    .to_string();
    let id = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:proj",
            "Round trip me",
            "--dep",
            &dep,
        ],
    ))
    .to_string();
    let target = format!("test:{id}");
    // A second revision is what makes `show` annotate the edited headings
    runes_ok(&home, &["edit", &target, "--status", "in-progress"]);
    runes_ok(&home, &["comment", &target, "-m", "a comment"]);

    let shown = runes_ok(&home, &["show", &target]);
    assert!(
        shown.contains("deps:") && shown.contains(&format!("  {dep} (")),
        "expected a deps block to strip: {shown}"
    );
    assert!(
        shown.contains("Edited by "),
        "expected an edit annotation to strip: {shown}"
    );
    assert!(
        shown.contains(" by Test User"),
        "expected a comment attribution to strip: {shown}"
    );

    let original = fs::read_to_string(find_rune_file(&store_path, &id)).expect("read rune doc");
    let input = home.join("full-doc.md");
    let input_s = input.to_string_lossy().to_string();
    for pass in 1..=2 {
        fs::write(&input, runes_ok(&home, &["show", &target])).expect("write full doc");
        runes_ok(&home, &["edit", &target, "-f", &input_s]);
        let stored = fs::read_to_string(find_rune_file(&store_path, &id)).expect("read rune doc");
        assert_eq!(original, stored, "round-trip {pass} changed the stored doc");
    }
}

/// Test: `edit -f` composes with field flags, and the flags win over the input
#[test]
fn jj_edit_file_with_field_flags() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("jj-edit-file-flags");
    let store_path = PathBuf::from(store_path);
    let id = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:proj",
            "Flag me",
            "--label",
            "keep",
        ],
    ))
    .to_string();
    let target = format!("test:{id}");
    let input = home.join("full-doc.md");
    let input_s = input.to_string_lossy().to_string();

    // Full doc plus flags: the file supplies the body, the flags win on fields
    fs::write(
        &input,
        runes_ok(&home, &["show", &target])
            .replace("## Description", "## Description\n\nFrom the file."),
    )
    .expect("write full doc");
    runes_ok(
        &home,
        &[
            "edit", &target, "-f", &input_s, "--status", "done", "--label", "urgent",
        ],
    );
    let stored = fs::read_to_string(find_rune_file(&store_path, &id)).expect("read rune doc");
    // `done` normalizes to `closed` on the way in, like any other status input
    assert!(
        stored.contains("status \"closed\""),
        "--status did not override the file: {stored}"
    );
    assert!(
        stored.contains("\"keep\"") && stored.contains("\"urgent\""),
        "labels not merged: {stored}"
    );
    assert!(
        stored.contains("From the file."),
        "body from the file was dropped: {stored}"
    );

    // The frontmatter still loses even when it carries an explicit conflicting value
    fs::write(
        &input,
        stored.replace("status \"closed\"", "status \"todo\""),
    )
    .expect("write full doc");
    runes_ok(
        &home,
        &["edit", &target, "-f", &input_s, "--status", "wip:impl"],
    );
    let stored = fs::read_to_string(find_rune_file(&store_path, &id)).expect("read rune doc");
    assert!(
        stored.contains("status \"wip:impl\""),
        "frontmatter beat the explicit flag: {stored}"
    );

    // Body-only input composes with flags the same way
    fs::write(&input, "# Flag me\n\nBody only.\n").expect("write body");
    runes_ok(
        &home,
        &["edit", &target, "-f", &input_s, "--status", "todo"],
    );
    let stored = fs::read_to_string(find_rune_file(&store_path, &id)).expect("read rune doc");
    assert!(
        stored.contains("status \"todo\"") && stored.contains("Body only."),
        "body-only input plus flags did not apply: {stored}"
    );
}

/// Test: core states take substates, invalid ones are rejected with the allowed
/// list, the substate allowlist is configurable, and legacy names are input sugar.
#[test]
fn states_accept_configured_substates_and_legacy_aliases() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("state-substates");
    let store = Path::new(&store_path);

    let reviewing = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:proj",
            "Ship the parser",
            "--status",
            "wip:review",
        ],
    ))
    .to_string();
    let doc = fs::read_to_string(find_rune_file(store, &reviewing)).expect("read rune");
    assert!(doc.contains("status \"wip:review\""), "{doc}");

    let bad_substate = runes_output(
        &home,
        &[
            "new",
            "--project",
            "test:proj",
            "Nope",
            "--status",
            "wip:qa",
        ],
    );
    assert!(!bad_substate.status.success(), "wip:qa should be rejected");
    let stderr = String::from_utf8_lossy(&bad_substate.stderr);
    assert!(
        stderr.contains("wip, wip:design, wip:impl, wip:review"),
        "error should list the allowed substates: {stderr}"
    );

    let bad_state = runes_output(
        &home,
        &["new", "--project", "test:proj", "Nope", "--status", "doing"],
    );
    assert!(!bad_state.status.success(), "`doing` should be rejected");
    let stderr = String::from_utf8_lossy(&bad_state.stderr);
    assert!(
        stderr.contains("todo, wip, closed"),
        "error should list the core states: {stderr}"
    );

    // Legacy names are accepted on input and rewritten to core states on the way in.
    runes_ok(
        &home,
        &["edit", &format!("test:{reviewing}"), "--status", "done"],
    );
    let doc = fs::read_to_string(find_rune_file(store, &reviewing)).expect("read rune");
    assert!(doc.contains("status \"closed\""), "{doc}");
    assert!(!doc.contains("done"), "legacy status was emitted: {doc}");

    // Substates are configurable; core states are not.
    runes_ok(
        &home,
        &[
            "config",
            "set",
            "state.wip.substate",
            "qa,review",
            "--global",
        ],
    );
    runes_ok(
        &home,
        &["edit", &format!("test:{reviewing}"), "--status", "wip:qa"],
    );
    let now_invalid = runes_output(
        &home,
        &[
            "edit",
            &format!("test:{reviewing}"),
            "--status",
            "wip:design",
        ],
    );
    assert!(
        !now_invalid.status.success(),
        "config should replace the default substates"
    );
}

/// Test: `closed` is terminal for dep resolution and matches every closed substate,
/// while an exact `state:substate` filter stays exact.
#[test]
fn closed_substates_are_terminal_and_filterable() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, _) = setup_jj_store("closed-substates");
    let blocker = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Blocking work"],
    ))
    .to_string();
    let blocked = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:proj",
            "Waiting on the blocker",
            "--dep",
            &blocker,
        ],
    ))
    .to_string();

    let scope = ["--store", "test", "--project", "proj"];
    let list = |extra: &[&str]| {
        let mut args = vec!["list"];
        args.extend_from_slice(extra);
        args.extend_from_slice(&scope);
        runes_ok(&home, &args)
    };

    assert!(
        list(&["--blocked"]).contains(&blocked),
        "dep on an open rune should block"
    );
    assert!(
        !list(&["--ready"]).contains(&blocked),
        "blocked rune should not be ready"
    );

    runes_ok(
        &home,
        &[
            "edit",
            &format!("test:{blocker}"),
            "--status",
            "closed:canceled",
        ],
    );

    let ready = list(&["--ready"]);
    assert!(
        ready.contains(&blocked),
        "closed:canceled dep should unblock: {ready}"
    );
    assert!(
        !list(&["--blocked"]).contains(&blocked),
        "closed:canceled dep should unblock"
    );

    let closed_filter = list(&["--status", "closed"]);
    assert!(
        closed_filter.contains(&blocker),
        "`--status closed` should match closed:canceled: {closed_filter}"
    );
    let exact = list(&["--status", "closed:canceled"]);
    assert!(exact.contains(&blocker), "exact substate filter: {exact}");
    let other = list(&["--status", "closed:duplicate"]);
    assert!(
        !other.contains(&blocker),
        "exact substate filter should not match another substate: {other}"
    );
}

/// Test: `store doctor` migrates runes written with the old status vocabulary
/// and commits the rewrite.
#[test]
fn store_doctor_migrates_legacy_statuses() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("doctor-migrate");
    let store = Path::new(&store_path);
    let legacy = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Written by an older runes"],
    ))
    .to_string();

    // Rewrite the frontmatter the way a pre-substate store had it.
    let path = find_rune_file(store, &legacy);
    let original = fs::read_to_string(&path).expect("read rune");
    fs::write(
        &path,
        original.replace("status \"todo\"", "status \"done\""),
    )
    .expect("write rune");

    let doctored = runes_ok(&home, &["store", "doctor", "test"]);
    assert!(
        doctored.contains("Migrated 1 rune(s)"),
        "doctor should report the migration: {doctored}"
    );
    let migrated = fs::read_to_string(&path).expect("read rune");
    assert!(migrated.contains("status \"closed\""), "{migrated}");
    assert!(
        migrated.contains("# Written by an older runes"),
        "migration should leave the body alone: {migrated}"
    );

    let log = runes_ok(&home, &["log", &format!("test:{legacy}"), "--limit", "5"]);
    assert!(
        log.contains("Migrate statuses to todo/wip/closed"),
        "migration should be committed: {log}"
    );

    // The rebuilt cache sees the migrated state.
    let closed = runes_ok(
        &home,
        &["list", "closed", "--store", "test", "--project", "proj"],
    );
    assert!(closed.contains(&legacy), "migrated rune missing: {closed}");

    // A second run has nothing left to migrate.
    let again = runes_ok(&home, &["store", "doctor", "test"]);
    assert!(
        !again.contains("Migrated"),
        "doctor should be idempotent: {again}"
    );
}

/// Test: `store doctor` migrates archived runes too, not just live ones.
#[test]
fn store_doctor_migrates_archived_runes() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("doctor-migrate-archived");
    let store = Path::new(&store_path);
    let legacy = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Old and put away"],
    ))
    .to_string();

    let path = find_rune_file(store, &legacy);
    let file_name = path.file_name().expect("file name").to_os_string();
    set_status_on_disk(store, &legacy, "done");
    runes_ok(&home, &["archive", &format!("test:{legacy}")]);

    let archived_path = store.join("proj").join("_archive").join(&file_name);
    assert!(
        archived_path.exists(),
        "rune not archived: {}",
        archived_path.display()
    );

    let doctored = runes_ok(&home, &["store", "doctor", "test"]);
    assert!(
        doctored.contains("Migrated 1 rune(s)"),
        "doctor should migrate the archived rune: {doctored}"
    );
    let migrated = fs::read_to_string(&archived_path).expect("read archived rune");
    assert!(
        migrated.contains("status \"closed\""),
        "archived rune not migrated: {migrated}"
    );

    let again = runes_ok(&home, &["store", "doctor", "test"]);
    assert!(
        !again.contains("Migrated"),
        "doctor should be idempotent: {again}"
    );
}

/// Test: a store that still holds the pre-substate vocabulary reads correctly
/// with no `store doctor` run — statuses normalize on the read path, so listing,
/// dep resolution and coloring all see core states.
#[test]
fn legacy_statuses_on_disk_read_as_core_states_without_doctor() {
    if !command_exists("jj") {
        eprintln!("skipping: jj not installed");
        return;
    }
    let (home, store_path) = setup_jj_store("legacy-read-path");
    let store = Path::new(&store_path);

    let started = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Still going"],
    ))
    .to_string();
    let finished = last_line(&runes_ok(
        &home,
        &["new", "--project", "test:proj", "Long since landed"],
    ))
    .to_string();
    let dependent = last_line(&runes_ok(
        &home,
        &[
            "new",
            "--project",
            "test:proj",
            "Waits on the one that landed",
            "--dep",
            &finished,
        ],
    ))
    .to_string();

    // Leave the store exactly as an older runes would have: legacy statuses on
    // disk, and a cache that has never seen `store doctor`.
    set_status_on_disk(store, &started, "in-progress");
    set_status_on_disk(store, &finished, "done");
    remove_cache(&home, "test");

    let scope = ["--store", "test", "--project", "proj"];
    let list = |extra: &[&str]| {
        let mut args = vec!["list"];
        args.extend_from_slice(extra);
        args.extend_from_slice(&scope);
        runes_ok(&home, &args)
    };

    let default_view = list(&[]);
    assert!(
        default_view.contains(&started) && default_view.contains(&dependent),
        "default view lost the open runes: {default_view}"
    );
    assert!(
        !default_view.contains(&finished),
        "default view should hide the legacy done rune: {default_view}"
    );
    assert!(
        !default_view.contains("in-progress"),
        "legacy status leaked into the listing: {default_view}"
    );
    assert!(
        default_view.contains("wip"),
        "legacy in-progress should read as wip: {default_view}"
    );

    let closed_view = list(&["closed"]);
    assert!(
        closed_view.contains(&finished),
        "`list closed` should show the legacy done rune: {closed_view}"
    );
    assert!(
        !closed_view.contains(&started),
        "`list closed` leaked an open rune: {closed_view}"
    );

    // A legacy `done` dep is terminal, so its dependent is ready, not blocked.
    let ready = list(&["--ready"]);
    assert!(
        ready.contains(&dependent),
        "legacy done dep should resolve: {ready}"
    );
    let blocked = list(&["--blocked"]);
    assert!(
        !blocked.contains(&dependent),
        "legacy done dep should not block: {blocked}"
    );
    assert!(
        !default_view.contains("[blocked]"),
        "legacy done dep marked its dependent blocked: {default_view}"
    );

    // Statuses colorize off the core state, so legacy values still render.
    let mut colored_args = vec!["list", "all"];
    colored_args.extend_from_slice(&scope);
    let colored = runes_with_env(&home, &[("FORCE_COLOR", "1")], &colored_args);
    assert!(
        colored.contains("\x1b[32mwip\x1b[0m"),
        "wip should be green: {colored:?}"
    );
    assert!(
        colored.contains("\x1b[90mclosed\x1b[0m"),
        "closed should be gray: {colored:?}"
    );
}

/// Rewrite a rune's on-disk status, bypassing the CLI's input normalization.
fn set_status_on_disk(store_path: &Path, rune_id: &str, status: &str) {
    let path = find_rune_file(store_path, rune_id);
    let text = fs::read_to_string(&path).expect("read rune");
    let updated = text.replace("status \"todo\"", &format!("status \"{status}\""));
    assert_ne!(text, updated, "no todo status in {}", path.display());
    fs::write(&path, updated).expect("write rune");
}

/// Drop a store's cache so the next command rebuilds it from disk.
fn remove_cache(home: &Path, store_name: &str) {
    let cache_dir = home.join(".runes").join("cache");
    for suffix in ["", "-wal", "-shm"] {
        let _ = fs::remove_file(cache_dir.join(format!("{store_name}.sqlite{suffix}")));
    }
}

fn find_rune_file(store_path: &Path, rune_id: &str) -> PathBuf {
    let short = rune_id.split('-').next_back().unwrap_or(rune_id);
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
