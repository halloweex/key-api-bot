//! The Rust expense-type load against the Python one's own answers.
//!
//! `contract/expense_types.json` is exported from the Python app by
//! `tests/rust_k2_contract.py`, and `tests/unit/test_rust_k2_contract.py`
//! fails the Python suite the day the app parses or writes the dictionary
//! differently. Then this fails until the Rust side follows: the table, its
//! columns and its statement, the localisation prefix, what every payload
//! parses to, and `str.title()` over ASCII. What asyncpg makes of each
//! parameter is the harness's to ask (`tests/integration/
//! test_rust_k2_expense_types.py`), against a real asyncpg.

use ks_worker::landing::expense_types::{
    self, COLUMNS, LIVE_TABLE, LOCALISATION_PREFIX, Reason, UNKNOWN_NAME,
};
use serde_json::Value;

fn contract() -> Value {
    serde_json::from_str(include_str!("../../../contract/expense_types.json"))
        .expect("contract/expense_types.json is JSON")
}

fn text(value: &Value) -> &str {
    value.as_str().expect("a string in the contract")
}

#[test]
fn the_table_its_columns_and_its_statement_are_pythons() {
    let contract = contract();
    assert_eq!(LIVE_TABLE, text(&contract["table"]));
    let columns: Vec<&str> = contract["columns"]
        .as_array()
        .expect("the columns")
        .iter()
        .map(text)
        .collect();
    assert_eq!(COLUMNS.to_vec(), columns);
    assert_eq!(LOCALISATION_PREFIX, text(&contract["localisation_prefix"]));
    assert_eq!(UNKNOWN_NAME, text(&contract["unknown_name"]));
    assert_eq!(
        expense_types::upsert_sql(LIVE_TABLE),
        text(&contract["upsert_sql"])
    );
}

#[test]
fn every_payload_parses_as_python_parses_it() {
    let contract = contract();
    let cases = contract["parse_cases"].as_array().expect("the parse cases");
    assert!(cases.len() >= 40, "the contract lost its payloads");
    for case in cases {
        let label = text(&case["label"]);
        let python = &case["python"];
        let gap = case.get("gap").map(text);
        match (expense_types::parse(0, &case["payload"]), python.get("row")) {
            (Ok(parsed), Some(row)) => {
                assert_eq!(gap, None, "{label}: the contract says Rust refuses this");
                assert_eq!(parsed.id, row["id"], "{label}: id");
                assert_eq!(parsed.name, text(&row["name"]), "{label}: name");
                assert_eq!(parsed.alias, row["alias"], "{label}: alias");
                assert_eq!(parsed.is_active, row["is_active"], "{label}: is_active");
            }
            (Err(refusal), Some(_)) => {
                assert_eq!(
                    gap,
                    Some("title_not_ported"),
                    "{label}: Python parses it, and Rust refused: {refusal}"
                );
                assert_eq!(refusal.reason, Reason::TitleNotPorted, "{label}");
            }
            (Err(refusal), None) => {
                assert!(
                    python.get("error").is_some(),
                    "{label}: the contract holds neither a row nor an error"
                );
                assert_ne!(
                    refusal.reason,
                    Reason::TitleNotPorted,
                    "{label}: Python raises before any title is taken"
                );
            }
            (Ok(parsed), None) => panic!(
                "{label}: Python raises {} and Rust parsed {parsed:?}",
                python["error"]
            ),
        }
    }
}

#[test]
fn ascii_title_is_pythons() {
    let contract = contract();
    let cases = contract["titles"]["ascii"].as_array().expect("the titles");
    assert!(cases.len() >= 128 * 5, "the contract lost its titles");
    for case in cases {
        let (given, titled) = (text(&case[0]), text(&case[1]));
        assert_eq!(
            expense_types::title(given).as_deref(),
            Some(titled),
            "{given:?}"
        );
    }
}

#[test]
fn a_title_of_other_text_is_refused_not_guessed() {
    let contract = contract();
    let cases = contract["titles"]["not_ported"]
        .as_array()
        .expect("the texts that are not ASCII");
    assert!(!cases.is_empty());
    for case in cases {
        let given = text(&case[0]);
        assert_eq!(
            expense_types::title(given),
            None,
            "{given:?}: Python titles it {:?}",
            text(&case[1])
        );
    }
}
