//! Rune states. A `status` is a core state plus an optional substate, written
//! `state:substate`: in `wip:review`, `wip` is the state and `review` the
//! substate. The core states `todo`, `wip` and `closed` are fixed; their
//! substates are configurable. Bare `closed` means completed, as does
//! `closed:done`.

use crate::{Error, Result};
use std::collections::HashMap;

pub const TODO: &str = "todo";
pub const WIP: &str = "wip";
pub const CLOSED: &str = "closed";

/// The core states, in lifecycle order.
pub const CORE_STATES: [&str; 3] = [TODO, WIP, CLOSED];

/// Core states that are not terminal — what the `open` view lists.
pub const OPEN_STATES: [&str; 2] = [TODO, WIP];

/// State names from the pre-substate vocabulary, accepted on input but never emitted.
const ALIASES: [(&str, &str); 2] = [("done", CLOSED), ("in-progress", WIP)];

/// Split a status into its core state and optional substate.
pub fn split(status: &str) -> (&str, Option<&str>) {
    match status.split_once(':') {
        Some((core, substate)) => (core, Some(substate)),
        None => (status, None),
    }
}

/// The core state of a status, ignoring any substate.
pub fn core_of(status: &str) -> &str {
    split(status).0
}

/// Whether a status is terminal. Every `closed:*` status is.
pub fn is_terminal(status: &str) -> bool {
    core_of(status) == CLOSED
}

/// Map legacy state names onto core states, preserving any substate.
/// Applied to input everywhere so `done` and `in-progress` keep working.
pub fn normalize(status: &str) -> String {
    let (core, substate) = split(status.trim());
    let core = ALIASES
        .iter()
        .find(|(alias, _)| *alias == core)
        .map_or(core, |(_, core_state)| *core_state);
    match substate {
        Some(substate) => format!("{core}:{substate}"),
        None => core.to_string(),
    }
}

/// Allowed substates per core state, from `runes.kdl` (global or repo).
#[derive(Clone, Debug)]
pub struct StateConfig {
    /// A core state with no entry here accepts any substate.
    allowed: HashMap<String, Vec<String>>,
}

impl Default for StateConfig {
    fn default() -> Self {
        let allowed = HashMap::from([
            (WIP.to_string(), owned(&["design", "impl", "review"])),
            (
                CLOSED.to_string(),
                owned(&["done", "canceled", "duplicate"]),
            ),
        ]);
        Self { allowed }
    }
}

fn owned(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

impl StateConfig {
    /// Restrict a core state to the given substates (empty means "no substates").
    pub fn set_substates(&mut self, core: &str, substates: Vec<String>) -> Result<()> {
        if !CORE_STATES.contains(&core) {
            return Err(Error::new(format!(
                "Unknown state '{}'. Allowed: {}",
                core,
                CORE_STATES.join(", ")
            )));
        }
        self.allowed.insert(core.to_string(), substates);
        Ok(())
    }

    /// Allowed substates for a core state, or `None` when any substate is accepted.
    pub fn substates(&self, core: &str) -> Option<&[String]> {
        self.allowed.get(core).map(Vec::as_slice)
    }

    /// Validate a `state` or `state:substate` status.
    pub fn validate(&self, status: &str) -> Result<()> {
        let (core, substate) = split(status);
        if !CORE_STATES.contains(&core) {
            return Err(Error::new(format!(
                "Invalid status '{}'. Allowed states: {} (each takes an optional substate, e.g. wip:review)",
                status,
                CORE_STATES.join(", ")
            )));
        }
        let Some(substate) = substate else {
            return Ok(());
        };
        let allowed = !substate.is_empty()
            && self
                .substates(core)
                .is_none_or(|allowed| allowed.iter().any(|value| value == substate));
        if allowed {
            Ok(())
        } else {
            Err(Error::new(format!(
                "Invalid status '{}'. Allowed: {}",
                status,
                self.allowed_display(core)
            )))
        }
    }

    /// The statuses a core state accepts, e.g. `wip, wip:design, wip:impl, wip:review`.
    pub fn allowed_display(&self, core: &str) -> String {
        match self.substates(core) {
            Some(substates) => std::iter::once(core.to_string())
                .chain(substates.iter().map(|value| format!("{core}:{value}")))
                .collect::<Vec<_>>()
                .join(", "),
            None => format!("{core}, {core}:*"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_state_and_terminality_ignore_substates() {
        assert_eq!(core_of("closed:canceled"), CLOSED);
        assert_eq!(core_of("closed"), CLOSED);
        assert!(is_terminal("closed"));
        assert!(is_terminal("closed:duplicate"));
        assert!(!is_terminal("wip:review"));
        assert!(!is_terminal("todo"));
    }

    #[test]
    fn normalize_maps_legacy_names() {
        assert_eq!(normalize("done"), "closed");
        assert_eq!(normalize("closed:done"), "closed:done");
        assert_eq!(normalize("in-progress"), "wip");
        assert_eq!(normalize(" wip:review "), "wip:review");
        assert_eq!(normalize("in-progress:review"), "wip:review");
        assert_eq!(normalize("todo"), "todo");
    }

    #[test]
    fn validate_accepts_core_states_and_default_substates() {
        let states = StateConfig::default();
        for status in [
            "todo",
            "wip",
            "closed",
            "wip:review",
            "closed:done",
            "closed:canceled",
        ] {
            assert!(states.validate(status).is_ok(), "rejected {status}");
        }
        // todo takes any substate by default
        assert!(states.validate("todo:next").is_ok());
    }

    #[test]
    fn validate_rejects_unknown_states_and_substates() {
        let states = StateConfig::default();
        let err = states.validate("doing").unwrap_err().to_string();
        assert!(err.contains("todo, wip, closed"), "{err}");

        let err = states.validate("wip:qa").unwrap_err().to_string();
        assert!(
            err.contains("wip, wip:design, wip:impl, wip:review"),
            "{err}"
        );

        // An empty substate is never a status
        assert!(states.validate("closed:").is_err());
    }

    #[test]
    fn configured_substates_replace_the_defaults() {
        let mut states = StateConfig::default();
        states.set_substates(WIP, owned(&["review"])).unwrap();
        states.set_substates(TODO, owned(&["next"])).unwrap();
        assert!(states.validate("wip:review").is_ok());
        assert!(states.validate("wip:design").is_err());
        assert!(states.validate("todo:next").is_ok());
        assert!(states.validate("todo:someday").is_err());
        assert!(states.set_substates("started", owned(&["x"])).is_err());
    }
}
