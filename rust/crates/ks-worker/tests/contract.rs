//! The Rust `ch_sync` against the Python one's own texts.
//!
//! `contract/ch_sync.json` is exported from the Python app by
//! `tests/rust_k1_contract.py`, and `tests/unit/test_rust_k1_contract.py`
//! fails the Python suite the day the app renders anything here
//! differently. Then this fails until the Rust side follows: column lists
//! and types, every DDL, the Gold derivation, the watermark statements,
//! TabSeparated for adversarial rows, and the Kyiv date of instants either
//! side of every clock change.

use chrono::{DateTime, NaiveDate, Utc};
use ks_core::tsv::{self, Cell, Column};
use ks_core::{kyiv, warehouse, watermark};
use ks_worker::ch_sync::{
    self, GOLD_DDL, HISTORY_COLUMNS, HISTORY_DDL, Names, SILVER_COLUMNS, SILVER_DDL, Target,
};
use serde_json::Value;

fn contract() -> Value {
    serde_json::from_str(include_str!("../../../contract/ch_sync.json"))
        .expect("contract/ch_sync.json is JSON")
}

fn text(value: &Value) -> &str {
    value.as_str().expect("a string in the contract")
}

fn typed(columns: &[Column]) -> Vec<(String, String)> {
    columns
        .iter()
        .map(|c| (c.name.to_owned(), c.kind.python_key().to_owned()))
        .collect()
}

fn pairs(value: &Value) -> Vec<(String, String)> {
    value
        .as_array()
        .expect("a list of pairs")
        .iter()
        .map(|pair| (text(&pair[0]).to_owned(), text(&pair[1]).to_owned()))
        .collect()
}

#[test]
fn the_columns_and_their_types_are_pythons() {
    let contract = contract();
    assert_eq!(typed(&SILVER_COLUMNS), pairs(&contract["silver_columns"]));
    assert_eq!(typed(&HISTORY_COLUMNS), pairs(&contract["history_columns"]));
    let gold: Vec<&str> = contract["gold_columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(text)
        .collect();
    assert_eq!(warehouse::gold_columns(), gold);
}

#[test]
fn the_ddl_is_pythons() {
    let contract = contract();
    assert_eq!(SILVER_DDL, text(&contract["silver_ddl"]));
    assert_eq!(GOLD_DDL, text(&contract["gold_ddl"]));
    assert_eq!(HISTORY_DDL, text(&contract["history_ddl"]));
}

#[test]
fn the_gold_derivation_is_pythons() {
    let contract = contract();
    let measures: Vec<(String, String)> = warehouse::gold_measures()
        .into_iter()
        .map(|(name, expression)| (name.to_owned(), expression))
        .collect();
    assert_eq!(measures, pairs(&contract["gold_measures"]));
    let (fine, rollup) = ch_sync::derive_gold_sql(&Names::new(Target::Live, "history"));
    assert_eq!(fine, text(&contract["derive_gold_fine"]));
    assert_eq!(rollup, text(&contract["derive_gold_rollup"]));
}

#[test]
fn the_names_are_pythons() {
    let contract = contract();
    let live = Names::new(Target::Live, "history");
    let tables = &contract["tables"];
    assert_eq!(live.silver, text(&tables["silver"]));
    assert_eq!(live.silver_staging, text(&tables["silver_staging"]));
    assert_eq!(live.gold, text(&tables["gold"]));
    assert_eq!(live.gold_staging, text(&tables["gold_staging"]));
    assert_eq!(live.history, text(&tables["history"]));
    let states = &contract["state_rows"];
    assert_eq!(live.silver_state, text(&states["silver"]));
    assert_eq!(live.gold_state, text(&states["gold"]));
    assert_eq!(live.history_state, text(&states["history"]));
    assert_eq!(live.heal_state, text(&states["history_heal"]));
}

#[test]
fn the_watermark_statements_are_pythons() {
    let contract = contract();
    assert_eq!(watermark::MARK_OK_SQL, text(&contract["mark_ok_sql"]));
    assert_eq!(
        watermark::MARK_FAILED_SQL,
        text(&contract["mark_failed_sql"])
    );
}

#[test]
fn the_escapes_are_pythons() {
    let escapes: Vec<(String, String)> = tsv::ESCAPES
        .iter()
        .map(|(raw, escaped)| (raw.to_string(), (*escaped).to_owned()))
        .collect();
    assert_eq!(escapes, pairs(&contract()["escapes"]));
}

fn cell(value: &Value) -> Cell {
    let (kind, value) = (text(&value[0]), text(&value[1]));
    match kind {
        "null" => Cell::Null,
        "int" => Cell::Int(value.parse().unwrap()),
        "decimal" => Cell::Money(value.parse().unwrap()),
        "str" => Cell::Str(value.to_owned()),
        "date" => Cell::Date(NaiveDate::parse_from_str(value, "%Y-%m-%d").unwrap()),
        "bool" => Cell::Bool(value == "true"),
        "ts" => Cell::Time(
            DateTime::parse_from_rfc3339(value)
                .unwrap()
                .with_timezone(&Utc),
        ),
        other => panic!("a cell of kind {other}"),
    }
}

#[test]
fn rows_render_as_python_renders_them() {
    let contract = contract();
    let samples = contract["tsv_samples"].as_array().unwrap();
    assert!(!samples.is_empty());
    for sample in samples {
        let columns: &[Column] = match text(&sample["table"]) {
            "silver" => &SILVER_COLUMNS,
            "history" => &HISTORY_COLUMNS,
            other => panic!("a sample of table {other}"),
        };
        let rows: Vec<Vec<Cell>> = sample["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row.as_array().unwrap().iter().map(cell).collect())
            .collect();
        let rendered = tsv::render(&rows, columns).unwrap();
        assert_eq!(
            String::from_utf8(rendered).unwrap(),
            text(&sample["rendered"]),
            "{} rows",
            text(&sample["table"])
        );
    }
}

#[test]
fn kyiv_dates_are_zoneinfos() {
    let contract = contract();
    let dates = contract["kyiv_dates"].as_array().unwrap();
    assert!(dates.len() > 100);
    for pair in dates {
        let instant = DateTime::parse_from_rfc3339(text(&pair[0]))
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            kyiv::date_of(instant).to_string(),
            text(&pair[1]),
            "{}",
            text(&pair[0])
        );
    }
}
