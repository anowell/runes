//! Canonical user identities and how they render.
//!
//! A rune doc and a commit both record a user by email — the stable key. The
//! human-readable name lives once per store, in a `.mailmap` at the store root,
//! so renaming a person is one edit instead of a rewrite of every doc. Reading
//! commands resolve the email through the mailmap and render whatever
//! `user.format` asks for.

use crate::Result;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Identity {
    pub name: Option<String>,
    pub email: Option<String>,
}

impl Identity {
    /// Parse `Name <email>`, a bare email, or a bare name.
    pub fn parse(raw: &str) -> Self {
        let raw = raw.trim();
        if let Some((name, email)) = split_angle_email(raw) {
            return Self {
                name: non_empty(name),
                email: non_empty(email),
            };
        }
        if looks_like_email(raw) {
            Self {
                name: None,
                email: non_empty(raw),
            }
        } else {
            Self {
                name: non_empty(raw),
                email: None,
            }
        }
    }

    pub fn from_parts(name: &str, email: &str) -> Self {
        Self {
            name: non_empty(name),
            email: non_empty(email),
        }
    }

    /// The value written to disk: the email when there is one, else the name.
    pub fn canonical(&self) -> String {
        self.email
            .clone()
            .or_else(|| self.name.clone())
            .unwrap_or_default()
    }

    /// The name when it is a real name rather than a restatement of the email.
    /// Author resolution defaults an unset name to the email, and that default
    /// must not be mistaken for something worth writing to the mailmap or
    /// splitting into first/last.
    pub fn real_name(&self) -> Option<&str> {
        let name = self.name.as_deref()?;
        if looks_like_email(name) {
            return None;
        }
        match &self.email {
            Some(email) if email.eq_ignore_ascii_case(name) => None,
            _ => Some(name),
        }
    }

    fn email_username(&self) -> Option<&str> {
        let email = self.email.as_deref()?;
        Some(email.split('@').next().unwrap_or(email))
    }

    /// Whether `needle` names this identity: the whole name, one word of it, or
    /// the email username, all case-insensitively.
    pub fn matches_name(&self, needle: &str) -> bool {
        let needle = needle.trim();
        if needle.is_empty() {
            return false;
        }
        if let Some(name) = self.real_name() {
            if name.eq_ignore_ascii_case(needle)
                || name
                    .split_whitespace()
                    .any(|w| w.eq_ignore_ascii_case(needle))
            {
                return true;
            }
        }
        self.email_username()
            .is_some_and(|user| user.eq_ignore_ascii_case(needle))
    }
}

fn non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn looks_like_email(value: &str) -> bool {
    let value = value.trim();
    value.contains('@') && !value.contains(char::is_whitespace)
}

/// Byte offsets of the first `<`…`>` pair in `value`.
fn angle_span(value: &str) -> Option<(usize, usize)> {
    let open = value.find('<')?;
    let close = value[open + 1..].find('>')? + open + 1;
    Some((open, close))
}

/// Split `Name <email>` into its two halves, ignoring anything after the `>`.
fn split_angle_email(value: &str) -> Option<(&str, &str)> {
    let (open, close) = angle_span(value)?;
    Some((&value[..open], &value[open + 1..close]))
}

/// How a user is rendered in human-facing output.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UserFormat {
    /// Full name, falling back to the email when no name is known.
    #[default]
    Name,
    Email,
    /// `Name <email>`.
    NameEmail,
    /// Local part of the email.
    Username,
    /// First word of the name.
    FirstName,
    /// Last word of the name.
    LastName,
}

impl UserFormat {
    /// Parse a `user.format` config value. Underscores and spaces are accepted
    /// wherever a dash is, so `email_username` and `name <email>` both work.
    pub fn parse(value: &str) -> Result<Self> {
        let normalized = value
            .trim()
            .to_ascii_lowercase()
            .replace(['_', ' '], "-")
            .replace(['<', '>'], "");
        match normalized.trim_matches('-') {
            "name" | "full-name" => Ok(Self::Name),
            "email" => Ok(Self::Email),
            "name-email" | "full" => Ok(Self::NameEmail),
            "username" | "email-username" => Ok(Self::Username),
            "first" | "first-name" | "firstname" => Ok(Self::FirstName),
            "last" | "last-name" | "lastname" => Ok(Self::LastName),
            _ => Err(crate::Error::new(format!(
                "Unknown user.format '{value}'. Expected one of: name, email, name-email, username, first-name, last-name"
            ))),
        }
    }
}

/// The store's `.mailmap`: email → canonical name, plus the raw lines so
/// comments and hand-written aliases survive a rewrite.
#[derive(Clone, Debug, Default)]
pub struct Mailmap {
    lines: Vec<String>,
    /// Lowercased commit email → resolved identity. Ordered so a name lookup
    /// that could match two people always picks the same one.
    entries: BTreeMap<String, Identity>,
    /// Lowercased commit email → index into `lines`, for in-place updates.
    line_of: HashMap<String, usize>,
}

pub const MAILMAP_FILE: &str = ".mailmap";

impl Mailmap {
    pub fn path(store_path: &Path) -> PathBuf {
        store_path.join(MAILMAP_FILE)
    }

    /// A missing or unreadable file reads as an empty map: identity display is
    /// never worth failing a command over.
    pub fn load(store_path: &Path) -> Self {
        match fs::read_to_string(Self::path(store_path)) {
            Ok(text) => Self::parse(&text),
            Err(_) => Self::default(),
        }
    }

    pub fn parse(text: &str) -> Self {
        let mut map = Self::default();
        for line in text.lines() {
            map.push_line(line.to_string());
        }
        map
    }

    fn push_line(&mut self, line: String) {
        let idx = self.lines.len();
        if let Some((key, identity)) = parse_mailmap_line(&line) {
            self.entries.insert(key.clone(), identity);
            self.line_of.insert(key, idx);
        }
        self.lines.push(line);
    }

    fn lookup(&self, email: &str) -> Option<&Identity> {
        self.entries.get(&email.to_ascii_lowercase())
    }

    /// Parse `raw` and fill in whatever the mailmap knows that it left out.
    fn resolve(&self, raw: &str) -> Identity {
        self.resolve_identity(Identity::parse(raw))
    }

    fn resolve_identity(&self, mut identity: Identity) -> Identity {
        let Some(email) = identity.email.clone() else {
            return identity;
        };
        if let Some(known) = self.lookup(&email) {
            if identity.real_name().is_none() {
                identity.name = known.name.clone();
            }
            if let Some(canonical) = &known.email {
                identity.email = Some(canonical.clone());
            }
        }
        identity
    }

    /// Resolve a user the way someone would type it in a filter — a name, a
    /// first name, an email username, or `Name <email>` — down to the canonical
    /// value docs are keyed by. Anything unrecognized is returned untouched.
    pub fn canonical_query(&self, raw: &str) -> String {
        let identity = Identity::parse(raw);
        if let Some(email) = &identity.email {
            return self
                .resolve_identity(identity.clone())
                .email
                .unwrap_or_else(|| email.clone());
        }
        let Some(needle) = identity.name.as_deref() else {
            return raw.trim().to_string();
        };
        self.entries
            .values()
            .find(|known| known.matches_name(needle))
            .map(|known| known.canonical())
            .unwrap_or_else(|| needle.to_string())
    }

    /// Record `identity`, replacing any earlier line for the same email.
    /// Returns true when the file's contents changed.
    pub fn upsert(&mut self, identity: &Identity) -> bool {
        let (Some(name), Some(email)) = (identity.real_name(), identity.email.as_deref()) else {
            return false;
        };
        let key = email.to_ascii_lowercase();
        let line = format!("{name} <{email}>");
        if let Some(&idx) = self.line_of.get(&key) {
            if self.lines[idx] == line {
                return false;
            }
            self.lines[idx] = line;
        } else {
            self.line_of.insert(key.clone(), self.lines.len());
            self.lines.push(line);
        }
        self.entries.insert(
            key,
            Identity {
                name: Some(name.to_string()),
                email: Some(email.to_string()),
            },
        );
        true
    }

    fn render(&self) -> String {
        let mut out = self.lines.join("\n");
        if !out.is_empty() && !out.ends_with('\n') {
            out.push('\n');
        }
        out
    }

    pub fn save(&self, store_path: &Path) -> Result<()> {
        fs::write(Self::path(store_path), self.render())?;
        Ok(())
    }
}

/// Parse one mailmap line into (lowercased commit email, canonical identity).
/// Supports the git forms `Proper Name <commit@email>`,
/// `<proper@email> <commit@email>`, and
/// `Proper Name <proper@email> <commit@email>`.
fn parse_mailmap_line(line: &str) -> Option<(String, Identity)> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    let name = non_empty(trimmed.split('<').next().unwrap_or_default());
    let mut emails = Vec::new();
    let mut rest = trimmed;
    while let Some((open, close)) = angle_span(rest) {
        emails.push(rest[open + 1..close].trim().to_string());
        rest = &rest[close + 1..];
    }
    match emails.len() {
        0 => None,
        1 => {
            let email = emails.remove(0);
            let key = email.to_ascii_lowercase();
            Some((
                key,
                Identity {
                    name,
                    email: non_empty(&email),
                },
            ))
        }
        _ => {
            let canonical = emails.remove(0);
            let commit = emails.remove(0);
            Some((
                commit.to_ascii_lowercase(),
                Identity {
                    name,
                    email: non_empty(&canonical),
                },
            ))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct UserDisplay {
    mailmap: Mailmap,
    format: UserFormat,
}

impl UserDisplay {
    pub fn new(mailmap: Mailmap, format: UserFormat) -> Self {
        Self { mailmap, format }
    }

    pub fn mailmap(&self) -> &Mailmap {
        &self.mailmap
    }

    /// Render a stored value (`ana@example.com`, `Ana Ruiz <ana@example.com>`,
    /// or a bare handle) the way `user.format` asks for.
    pub fn render(&self, raw: &str) -> String {
        if raw.trim().is_empty() {
            return String::new();
        }
        self.render_identity(&self.mailmap.resolve(raw))
    }

    pub fn render_parts(&self, name: &str, email: &str) -> String {
        let identity = self
            .mailmap
            .resolve_identity(Identity::from_parts(name, email));
        self.render_identity(&identity)
    }

    fn render_identity(&self, identity: &Identity) -> String {
        let fallback = || {
            identity
                .real_name()
                .map(str::to_string)
                .or_else(|| identity.email.clone())
                .or_else(|| identity.name.clone())
                .unwrap_or_default()
        };
        match self.format {
            UserFormat::Name => fallback(),
            UserFormat::Email => identity
                .email
                .clone()
                .or_else(|| identity.name.clone())
                .unwrap_or_default(),
            UserFormat::NameEmail => match (identity.real_name(), identity.email.as_deref()) {
                (Some(name), Some(email)) => format!("{name} <{email}>"),
                _ => fallback(),
            },
            UserFormat::Username => identity
                .email_username()
                .map(str::to_string)
                .unwrap_or_else(fallback),
            UserFormat::FirstName => name_word(identity, true).unwrap_or_else(fallback),
            UserFormat::LastName => name_word(identity, false).unwrap_or_else(fallback),
        }
    }
}

/// First or last word of the real name; the email username stands in when no
/// real name is known, since a bare email has no word to pick.
fn name_word(identity: &Identity, first: bool) -> Option<String> {
    match identity.real_name() {
        Some(name) => {
            let mut words = name.split_whitespace();
            if first {
                words.next().map(str::to_string)
            } else {
                words.next_back().map(str::to_string)
            }
        }
        None => identity.email_username().map(str::to_string),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_identity_forms() {
        let full = Identity::parse("Ana Ruiz <ana@example.com>");
        assert_eq!(full.name.as_deref(), Some("Ana Ruiz"));
        assert_eq!(full.email.as_deref(), Some("ana@example.com"));

        let email = Identity::parse("ana@example.com");
        assert_eq!(email.name, None);
        assert_eq!(email.email.as_deref(), Some("ana@example.com"));

        let handle = Identity::parse("ana");
        assert_eq!(handle.name.as_deref(), Some("ana"));
        assert_eq!(handle.email, None);

        let bare_angle = Identity::parse("<ana@example.com>");
        assert_eq!(bare_angle.name, None);
        assert_eq!(bare_angle.email.as_deref(), Some("ana@example.com"));
    }

    #[test]
    fn real_name_ignores_email_shaped_names() {
        let echoed = Identity::from_parts("ana@example.com", "ana@example.com");
        assert_eq!(echoed.real_name(), None);

        let named = Identity::from_parts("Ana Ruiz", "ana@example.com");
        assert_eq!(named.real_name(), Some("Ana Ruiz"));
    }

    #[test]
    fn mailmap_roundtrips_and_preserves_comments() {
        let mut map = Mailmap::parse("# people\nAna Ruiz <ana@example.com>\n");
        assert_eq!(
            map.lookup("ANA@example.com").and_then(|i| i.name.clone()),
            Some("Ana Ruiz".to_string())
        );
        assert!(!map.upsert(&Identity::from_parts("Ana Ruiz", "ana@example.com")));
        assert!(map.upsert(&Identity::from_parts("Ana Ruiz-Diaz", "ana@example.com")));
        assert!(map.upsert(&Identity::from_parts("Bo Chen", "bo@example.com")));
        assert_eq!(
            map.render(),
            "# people\nAna Ruiz-Diaz <ana@example.com>\nBo Chen <bo@example.com>\n"
        );
    }

    #[test]
    fn mailmap_reads_two_email_forms() {
        let map = Mailmap::parse("Ana Ruiz <ana@example.com> <old@example.com>\n");
        let resolved = map.resolve("old@example.com");
        assert_eq!(resolved.name.as_deref(), Some("Ana Ruiz"));
        assert_eq!(resolved.email.as_deref(), Some("ana@example.com"));
    }

    #[test]
    fn canonical_query_resolves_the_ways_a_filter_is_typed() {
        let map = Mailmap::parse("Ana Ruiz <ana@example.com>\n");
        for typed in [
            "ana@example.com",
            "Ana Ruiz <ana@example.com>",
            "Ana Ruiz",
            "ana ruiz",
            "Ruiz",
            "ana",
        ] {
            assert_eq!(
                map.canonical_query(typed),
                "ana@example.com",
                "typed {typed}"
            );
        }
        // Nothing in the mailmap matches, so the filter is left alone.
        assert_eq!(map.canonical_query("bo"), "bo");
        assert_eq!(map.canonical_query("bo@example.com"), "bo@example.com");
    }

    /// A bare email or handle names nobody, so it must never reach the file
    /// every reader resolves names through.
    #[test]
    fn mailmap_ignores_identities_that_name_nobody() {
        let mut map = Mailmap::default();
        assert!(!map.upsert(&Identity::parse("ana@example.com")));
        assert!(!map.upsert(&Identity::parse("ana")));
        assert!(!map.upsert(&Identity::from_parts("ana@example.com", "ana@example.com")));
        assert_eq!(map.render(), "");
    }

    fn display(format: UserFormat) -> UserDisplay {
        UserDisplay::new(Mailmap::parse("Ana Ruiz <ana@example.com>\n"), format)
    }

    #[test]
    fn renders_every_format_from_a_bare_email() {
        for (format, expected) in [
            (UserFormat::Name, "Ana Ruiz"),
            (UserFormat::Email, "ana@example.com"),
            (UserFormat::NameEmail, "Ana Ruiz <ana@example.com>"),
            (UserFormat::Username, "ana"),
            (UserFormat::FirstName, "Ana"),
            (UserFormat::LastName, "Ruiz"),
        ] {
            assert_eq!(display(format).render("ana@example.com"), expected);
        }
    }

    #[test]
    fn unknown_email_falls_back_without_a_mailmap_entry() {
        for (format, expected) in [
            (UserFormat::Name, "bo@example.com"),
            (UserFormat::NameEmail, "bo@example.com"),
            (UserFormat::Username, "bo"),
            (UserFormat::FirstName, "bo"),
            (UserFormat::LastName, "bo"),
        ] {
            assert_eq!(display(format).render("bo@example.com"), expected);
        }
    }

    #[test]
    fn bare_handles_render_unchanged() {
        for format in [
            UserFormat::Name,
            UserFormat::Email,
            UserFormat::NameEmail,
            UserFormat::Username,
        ] {
            assert_eq!(display(format).render("unassigned-bot"), "unassigned-bot");
        }
        assert_eq!(display(UserFormat::Name).render(""), "");
    }

    #[test]
    fn commit_parts_prefer_the_mailmap_name() {
        let display = display(UserFormat::Name);
        // A commit that only carried the email still shows the recorded name.
        assert_eq!(
            display.render_parts("ana@example.com", "ana@example.com"),
            "Ana Ruiz"
        );
        // An explicit commit name wins over the mailmap.
        assert_eq!(
            display.render_parts("Ana R. (bot)", "ana@example.com"),
            "Ana R. (bot)"
        );
    }

    #[test]
    fn format_parse_accepts_aliases_and_rejects_junk() {
        assert_eq!(UserFormat::parse("name").unwrap(), UserFormat::Name);
        assert_eq!(UserFormat::parse("Full Name").unwrap(), UserFormat::Name);
        assert_eq!(
            UserFormat::parse("name <email>").unwrap(),
            UserFormat::NameEmail
        );
        assert_eq!(
            UserFormat::parse("email_username").unwrap(),
            UserFormat::Username
        );
        assert_eq!(
            UserFormat::parse("firstName").unwrap(),
            UserFormat::FirstName
        );
        assert_eq!(UserFormat::parse("lastName").unwrap(), UserFormat::LastName);
        assert!(UserFormat::parse("nickname").is_err());
    }
}
