use atty::Stream;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use syntect::easy::HighlightLines;
use syntect::highlighting::ThemeSet;
use syntect::parsing::{SyntaxDefinition, SyntaxSet};
use syntect::util::{as_24_bit_terminal_escaped, LinesWithEndings};

static COLOR_ENABLED: OnceLock<bool> = OnceLock::new();
static SYNTAX_SET: OnceLock<SyntaxSet> = OnceLock::new();
static THEME_SET: OnceLock<ThemeSet> = OnceLock::new();
static MD_THEME: OnceLock<syntect::highlighting::Theme> = OnceLock::new();

pub fn is_color_enabled() -> bool {
    *COLOR_ENABLED.get_or_init(|| {
        if std::env::var("NO_COLOR").is_ok() {
            return false;
        }
        if std::env::var("FORCE_COLOR").is_ok() {
            return true;
        }
        atty::is(Stream::Stdout)
    })
}

fn syntax_set() -> &'static SyntaxSet {
    SYNTAX_SET.get_or_init(|| {
        let mut builder = SyntaxSet::load_defaults_newlines().into_builder();
        let kdl_yaml = include_str!("../syntaxes/KDL.sublime-syntax");
        if let Ok(def) = SyntaxDefinition::load_from_str(kdl_yaml, true, None) {
            builder.add(def);
        }
        builder.build()
    })
}

fn theme_set() -> &'static ThemeSet {
    THEME_SET.get_or_init(ThemeSet::load_defaults)
}

fn bundled_theme() -> &'static syntect::highlighting::Theme {
    MD_THEME.get_or_init(|| {
        if let Ok(name) = std::env::var("RUNES_THEME") {
            if let Some(theme) = theme_set().themes.get(name.as_str()) {
                return theme.clone();
            }
        }
        let mut cursor = std::io::Cursor::new(include_bytes!("../themes/Coldark-Dark.tmTheme"));
        ThemeSet::load_from_reader(&mut cursor)
            .unwrap_or_else(|_| theme_set().themes["base16-eighties.dark"].clone())
    })
}

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const RED: &str = "\x1b[31m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const MAGENTA: &str = "\x1b[35m";
const CYAN: &str = "\x1b[36m";
const BRIGHT_BLACK: &str = "\x1b[90m";

fn wrap(code: &str, s: &str) -> String {
    if is_color_enabled() {
        format!("{code}{s}{RESET}")
    } else {
        s.to_string()
    }
}

pub fn purple(s: &str) -> String { wrap(MAGENTA, s) }
pub fn dim(s: &str) -> String { wrap(DIM, s) }
pub fn green(s: &str) -> String { wrap(GREEN, s) }
pub fn yellow(s: &str) -> String { wrap(YELLOW, s) }
pub fn teal(s: &str) -> String { wrap(CYAN, s) }
pub fn gray(s: &str) -> String { wrap(BRIGHT_BLACK, s) }
pub fn bright_black(s: &str) -> String { wrap(BRIGHT_BLACK, s) }

pub fn status_color(status: &str) -> String {
    match status {
        "done" => bright_black(status),
        "in-progress" => green(status),
        _ => status.to_string(),
    }
}

pub fn colored_id(id: &str) -> String {
    if let Some((project, short)) = id.split_once('-') {
        format!("{}{}", dim(&format!("{project}-")), purple(short))
    } else {
        id.to_string()
    }
}

pub fn diff_added(s: &str) -> String { wrap(GREEN, s) }
pub fn diff_removed(s: &str) -> String { wrap(RED, s) }
pub fn diff_hunk_header(s: &str) -> String { wrap(CYAN, s) }
pub fn diff_file_header(s: &str) -> String { wrap(BOLD, s) }

/// Highlight KDL frontmatter with scope-aware overrides:
/// - Root node names (entity.name.tag) stay prominent
/// - Child node names (variable.other.member) are dimmed
/// - `---` delimiters are dimmed
pub fn highlight_kdl(content: &str) {
    if !is_color_enabled() {
        print!("{content}");
        return;
    }
    let ss = syntax_set();
    let syntax = ss.find_syntax_by_extension("kdl")
        .unwrap_or_else(|| ss.find_syntax_plain_text());
    let theme = bundled_theme();
    let fg = theme.settings.foreground
        .unwrap_or(syntect::highlighting::Color { r: 211, g: 208, b: 200, a: 255 });
    let dim_fg = syntect::highlighting::Color {
        r: fg.r / 2, g: fg.g / 2, b: fg.b / 2, a: fg.a,
    };
    let highlighter = syntect::highlighting::Highlighter::new(theme);
    let mut parse_state = syntect::parsing::ParseState::new(syntax);
    let mut highlight_state = syntect::highlighting::HighlightState::new(
        &highlighter,
        syntect::parsing::ScopeStack::new(),
    );
    let mut scope_stack = syntect::parsing::ScopeStack::new();
    for line in LinesWithEndings::from(content) {
        // Dim `---` delimiters
        if line.trim().trim_end_matches('\n') == "---" {
            print!("{DIM}{}{RESET}", line.trim_end_matches('\n'));
            if line.ends_with('\n') {
                println!();
            }
            // Keep parse state in sync even though we skip highlighting
            let ops = parse_state.parse_line(line, ss).unwrap_or_default();
            for (_, op) in &ops {
                scope_stack.apply(op).ok();
            }
            continue;
        }
        let ops = parse_state.parse_line(line, ss).unwrap_or_default();
        let iter = syntect::highlighting::RangedHighlightIterator::new(
            &mut highlight_state,
            &ops,
            line,
            &highlighter,
        );
        let ranges: Vec<_> = iter.collect();
        let mut op_idx = 0;
        for (mut style, _scope_change, range) in &ranges {
            let text = &line[range.clone()];
            while op_idx < ops.len() && ops[op_idx].0 <= range.start {
                scope_stack.apply(&ops[op_idx].1).ok();
                op_idx += 1;
            }
            // Dim child node names (status, assignee, etc.)
            let is_child_node = scope_stack.as_slice().iter()
                .any(|s| s.build_string().starts_with("variable.other.member"));
            if is_child_node {
                style.foreground = dim_fg;
            }
            let escaped = as_24_bit_terminal_escaped(&[(style, text)], false);
            print!("{escaped}");
        }
        while op_idx < ops.len() {
            scope_stack.apply(&ops[op_idx].1).ok();
            op_idx += 1;
        }
    }
    print!("{RESET}");
}

/// Pipe output through a pager (like `less -FRX`) when stdout is a TTY.
/// If `no_pager` is true or stdout is not a TTY, prints directly.
pub fn print_with_pager(output: &str, no_pager: bool) {
    if no_pager || !is_color_enabled() {
        print!("{output}");
        return;
    }
    let pager_cmd = std::env::var("PAGER").unwrap_or_else(|_| "less".to_string());
    let mut parts = pager_cmd.split_whitespace();
    let program = match parts.next() {
        Some(p) => p,
        None => {
            print!("{output}");
            return;
        }
    };
    let mut args: Vec<&str> = parts.collect();
    // Add default flags for less if no custom args were provided
    if program == "less" && args.is_empty() {
        args = vec!["-FRX"];
    }
    match Command::new(program)
        .args(&args)
        .stdin(Stdio::piped())
        .spawn()
    {
        Ok(mut child) => {
            if let Some(ref mut stdin) = child.stdin {
                let _ = stdin.write_all(output.as_bytes());
            }
            drop(child.stdin.take());
            let _ = child.wait();
        }
        Err(_) => {
            // Pager not available, fall back to direct output
            print!("{output}");
        }
    }
}

/// Highlight Markdown body using syntect with bundled Coldark Dark theme.
pub fn highlight_markdown(content: &str) {
    if !is_color_enabled() {
        print!("{content}");
        return;
    }
    let ss = syntax_set();
    let syntax = ss.find_syntax_by_extension("md")
        .unwrap_or_else(|| ss.find_syntax_plain_text());
    let theme = bundled_theme();
    let mut h = HighlightLines::new(syntax, theme);
    for line in LinesWithEndings::from(content) {
        match h.highlight_line(line, ss) {
            Ok(ranges) => {
                let escaped = as_24_bit_terminal_escaped(&ranges[..], false);
                print!("{escaped}");
            }
            Err(_) => print!("{line}"),
        }
    }
    print!("{RESET}");
}
