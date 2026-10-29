//! What one session may do: the role's matrix, narrowed to the person's
//! tabs — `core/permissions.py` and `effective_permissions`.
//!
//! The matrix comes from `app.role_permissions`. Python seeds that table on
//! its first read in a process — every (role, feature) pair the code knows and
//! the table lacks, written with the code's default, never overwriting a row
//! — and then reads the role. This never writes: it reads the role's rows and
//! fills in what the seed would have added, which is the same answer. A role
//! with no rows at all (a level the code does not know) gets the code's
//! matrix, which grants it nothing; so does any role whose read fails.
//!
//! Then the tab set (`apply_feature_override`): a tab the person holds is
//! viewable whatever the role says, its `edit` and `delete` still come from
//! the role, and a tab not held is shut. No tab set means the level's default
//! set (`DEFAULT_TABS`), and an admin's default is no narrowing at all. The
//! empty set is a real state — no tabs — and never the same as no set.

use std::collections::{BTreeMap, BTreeSet};

use super::contract::Contract;
use crate::pyrt::py_strip;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Actions {
    pub view: bool,
    pub edit: bool,
    pub delete: bool,
}

impl Actions {
    /// `perms.get(action, False)`; an action Python does not know is False.
    pub fn allows(self, action: &str) -> bool {
        match action {
            "view" => self.view,
            "edit" => self.edit,
            "delete" => self.delete,
            _ => false,
        }
    }

    fn from_names(names: &[String]) -> Self {
        let has = |a: &str| names.iter().any(|n| n == a);
        Self {
            view: has("view"),
            edit: has("edit"),
            delete: has("delete"),
        }
    }
}

/// Feature → actions.
pub type Matrix = BTreeMap<String, Actions>;

/// One row of `app.role_permissions` for the role asked about.
#[derive(Clone, Debug)]
pub struct StoredPermission {
    pub feature: String,
    pub actions: Actions,
}

/// `get_permissions_for_role(role)`: every feature, False where the code grants none.
pub fn coded(contract: &Contract, role: &str) -> Matrix {
    let grants = contract.role_permissions.get(role);
    contract
        .features
        .iter()
        .map(|f| {
            let names = grants.and_then(|g| g.get(f)).map_or(&[][..], Vec::as_slice);
            (f.clone(), Actions::from_names(names))
        })
        .collect()
}

/// `get_permissions_for_role_async(role)` after a read that succeeded.
pub fn stored(contract: &Contract, role: &str, rows: &[StoredPermission]) -> Matrix {
    let mut held: BTreeMap<String, Actions> = rows
        .iter()
        .map(|r| (r.feature.clone(), r.actions))
        .collect();
    if let Some(defaults) = contract.role_permissions.get(role) {
        for (feature, names) in defaults {
            held.entry(feature.clone())
                .or_insert_with(|| Actions::from_names(names));
        }
    }
    if held.is_empty() {
        return coded(contract, role);
    }
    contract
        .features
        .iter()
        .map(|f| (f.clone(), held.get(f).copied().unwrap_or_default()))
        .collect()
}

/// `normalize_features`: the known tab keys, in their order, once each.
pub fn normalize<'a>(
    contract: &Contract,
    values: impl IntoIterator<Item = &'a str>,
) -> Vec<String> {
    let wanted: BTreeSet<String> = values
        .into_iter()
        .map(|v| py_strip(v).to_lowercase())
        .collect();
    contract
        .tab_features
        .iter()
        .filter(|k| wanted.contains(*k))
        .cloned()
        .collect()
}

/// `parse_features`: NULL is "as the role", an empty string is "no tabs".
pub fn parse_features(contract: &Contract, raw: Option<&str>) -> Option<Vec<String>> {
    let text = py_strip(raw?);
    if text.is_empty() {
        return Some(Vec::new());
    }
    Some(normalize(contract, text.split(',')))
}

/// `apply_feature_override(permissions, allowed, role)`.
pub fn apply_feature_override(
    contract: &Contract,
    permissions: Matrix,
    allowed: Option<&[String]>,
    role: &str,
) -> Matrix {
    let allowed: Option<Vec<String>> = match allowed {
        Some(tabs) => Some(tabs.to_vec()),
        None if role.is_empty() => None,
        None => contract.default_tabs.get(role).cloned().flatten(),
    };
    let Some(allowed) = allowed else {
        return permissions;
    };
    let granted: BTreeSet<String> = normalize(contract, allowed.iter().map(String::as_str))
        .into_iter()
        .collect();
    permissions
        .into_iter()
        .map(|(feature, actions)| {
            if !contract.tab_features.contains(&feature) {
                return (feature, actions);
            }
            let visible = granted.contains(&feature);
            let narrowed = Actions {
                view: visible,
                edit: visible && actions.edit,
                delete: visible && actions.delete,
            };
            (feature, narrowed)
        })
        .collect()
}
