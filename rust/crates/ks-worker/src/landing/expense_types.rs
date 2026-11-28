//! Chain 6a's dictionary in Rust (the plan, revision 4, К2, step 1):
//! `bronze.expense_types`, the names of every order-level cost on /expenses.
//! A KeyCRM payload is parsed as `core.landing_rows.expense_type_row` parses
//! it, its four parameters are typed as asyncpg types them, and the batch is
//! written as `core.pg_expense_types_write.upsert_expense_types` writes it —
//! into the shadow beside the live table, [`SHADOW_TABLE`]. The live table
//! stays Python's: nothing here writes it, and only the contract test names
//! it.
//!
//! WHY THIS TABLE FIRST
//!
//! "Cheap first", the plan's К2 row. One writer, a few dozen rows, re-fetched
//! from KeyCRM by every Sunday's full sync, so the worst a mistake can cost
//! is a week of wrong names. And it is the one landing table whose parse
//! transforms rather than copies: some names arrive as localisation keys
//! (`dictionaries.expense_types.delivery`) and become the alias in Title
//! Case, so the harness proves a rule and not a passthrough.
//!
//! PYTHON'S BEHAVIOUR, INCLUDING ITS ACCIDENTS
//!
//! What Python writes, this writes; what Python refuses, this refuses; and
//! either way the batch lands whole or not at all, in one transaction.
//! Python refuses in two places: its parse raises (`.get` on a payload that
//! is not an object, `.startswith` on a name that is not text, `.replace` on
//! a truthy alias that is not text), or asyncpg's encoders do, on a
//! parameter of the wrong type. The second set is checked here at [`bind`],
//! before the transaction opens: that moves where a refusal is found, not
//! what is refused. Kept as they are, because changing any of them is a
//! decision about the dictionary and not about the port:
//!
//! - a name absent from the payload is `Unknown`; one present and null is
//!   refused (`None.startswith`);
//! - a float id is truncated toward zero, and `true` is 1: asyncpg's int4
//!   encoder calls `int()` on anything that has `__int__`;
//! - `is_active` absent is true, and present and null is NULL;
//! - a falsy alias (`""`, `0`, `[]`) makes the key's own tail the name, and
//!   is then stored: `""` as text, the others refused by the text encoder;
//! - `replace` removes every copy of the prefix, not only the leading one;
//! - a batch that names one id twice keeps the later payload: `executemany`
//!   runs the upsert row by row, and so does this.
//!
//! TWO REFUSALS PYTHON DOES NOT MAKE
//!
//! Both are loud, neither writes a different row, and the harness pins both.
//!
//! 1. `str.title()` of text that is not ASCII. CPython titlecases through
//!    Unicode's Cased and Case_Ignorable properties and its full titlecase
//!    and lowercase mappings (final sigma, digraphs, ligatures), which Rust's
//!    standard library does not carry. The text it applies to is a system
//!    type's alias or key, which KeyCRM spells in ASCII. A custom type's name
//!    is stored as typed, Cyrillic included, and never titlecased.
//! 2. What Python's `json` accepts and JSON does not: `NaN` and `Infinity`,
//!    and nesting deeper than serde_json's limit of 128. KeyCRM answers in
//!    JSON.

use std::fmt;
use std::sync::LazyLock;
use std::time::Duration;

use ks_core::pg::within;
use ks_core::watermark;
use serde_json::{Map, Value, json};
use sqlx::PgPool;

/// The live table, Python's (chain 6a). Named for the contract test alone.
pub const LIVE_TABLE: &str = "bronze.expense_types";
/// The shadow: the live name with `_rust`, as К1's ClickHouse shadow is
/// named. In `bronze` itself, because `ks_app`, which runs the migrations,
/// owns that schema and cannot create another.
pub const SHADOW_TABLE: &str = "bronze.expense_types_rust";
/// The shadow's row in `meta.mirror_state`: К1's prefix on the live name.
pub const SHADOW_STATE: &str = "rust_shadow.bronze.expense_types";
/// `core.landing_rows._LOCALISATION_PREFIX`.
pub const LOCALISATION_PREFIX: &str = "dictionaries.expense_types.";
/// `core.landing_rows.EXPENSE_TYPE_COLUMNS`: the write order.
pub const COLUMNS: [&str; 4] = ["id", "name", "alias", "is_active"];
/// The name of a payload that carries none.
pub const UNKNOWN_NAME: &str = "Unknown";
/// The answer's keys: `SyncService.full_sync`'s `stats`.
pub const ANSWER_KEY: &str = "expense_types";
pub const ERROR_KEY: &str = "expense_types_error";

/// Which part of a payload a refusal is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Field {
    Payload,
    Id,
    Name,
    Alias,
    IsActive,
}

/// Why it is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    /// Not a JSON object: Python's `.get` raises.
    NotAnObject,
    /// Present and not text: `.startswith` or `.replace` raises.
    NotText,
    /// Stored, and neither text nor null: asyncpg's text encoder raises.
    NotTextOrNull,
    /// Stored, and neither a boolean nor null: asyncpg's bool encoder raises.
    NotBoolOrNull,
    /// Absent or null, and the column is the primary key.
    Null,
    /// Neither a number nor a boolean: asyncpg's int4 encoder raises.
    NotInteger,
    /// Outside int4, after `int()`.
    OutOfRange,
    /// The name needs `str.title()` of text that is not ASCII, which this
    /// port does not reproduce (the module's docs).
    TitleNotPorted,
}

/// A batch this port does not write, and the payload that stopped it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub index: usize,
    pub field: Field,
    pub reason: Reason,
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field = match self.field {
            Field::Payload => "the payload",
            Field::Id => "id",
            Field::Name => "name",
            Field::Alias => "alias",
            Field::IsActive => "is_active",
        };
        let reason = match self.reason {
            Reason::NotAnObject => "is not a JSON object",
            Reason::NotText => "is not text",
            Reason::NotTextOrNull => "is neither text nor null",
            Reason::NotBoolOrNull => "is neither a boolean nor null",
            Reason::Null => "is absent or null",
            Reason::NotInteger => "is neither a number nor a boolean",
            Reason::OutOfRange => "is outside int4",
            Reason::TitleNotPorted => {
                "needs str.title() of text that is not ASCII, which the Rust port refuses"
            }
        };
        write!(f, "expense-type payload {}: {field} {reason}", self.index)
    }
}

/// `expense_type_row`'s answer before asyncpg sees it: `id`, `alias` and
/// `is_active` as the payload carried them (Python passes them through),
/// `name` after the rule.
#[derive(Clone, Debug, PartialEq)]
pub struct Parsed {
    pub id: Value,
    pub name: String,
    pub alias: Value,
    pub is_active: Value,
}

/// The row asyncpg sends, typed as its encoders type it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpenseTypeRow {
    pub id: i32,
    pub name: String,
    pub alias: Option<String>,
    pub is_active: Option<bool>,
}

/// `core.landing_rows.expense_type_row`, for the payload at `index`.
pub fn parse(index: usize, payload: &Value) -> Result<Parsed, Refusal> {
    let refuse = |field, reason| Refusal {
        index,
        field,
        reason,
    };
    let Value::Object(fields) = payload else {
        return Err(refuse(Field::Payload, Reason::NotAnObject));
    };
    let name = match fields.get("name") {
        None => UNKNOWN_NAME.to_owned(),
        Some(Value::String(name)) => name.clone(),
        Some(_) => return Err(refuse(Field::Name, Reason::NotText)),
    };
    let alias = got(fields, "alias", Value::Null);
    let name = if name.starts_with(LOCALISATION_PREFIX) {
        let text = if truthy(&alias) {
            let Value::String(alias) = &alias else {
                return Err(refuse(Field::Alias, Reason::NotText));
            };
            alias.replace('_', " ")
        } else {
            name.replace(LOCALISATION_PREFIX, "").replace('_', " ")
        };
        title(&text).ok_or(refuse(Field::Name, Reason::TitleNotPorted))?
    } else {
        name
    };
    Ok(Parsed {
        id: got(fields, "id", Value::Null),
        name,
        alias,
        is_active: got(fields, "is_active", Value::Bool(true)),
    })
}

/// asyncpg's encoders, for the parameters of the row at `index`.
pub fn bind(index: usize, parsed: Parsed) -> Result<ExpenseTypeRow, Refusal> {
    let refuse = |field, reason| Refusal {
        index,
        field,
        reason,
    };
    let id = int4(&parsed.id).map_err(|reason| refuse(Field::Id, reason))?;
    let alias = match parsed.alias {
        Value::Null => None,
        Value::String(alias) => Some(alias),
        _ => return Err(refuse(Field::Alias, Reason::NotTextOrNull)),
    };
    let is_active = match parsed.is_active {
        Value::Null => None,
        Value::Bool(active) => Some(active),
        _ => return Err(refuse(Field::IsActive, Reason::NotBoolOrNull)),
    };
    Ok(ExpenseTypeRow {
        id,
        name: parsed.name,
        alias,
        is_active,
    })
}

/// The batch as Python handles it: every payload parsed, then every row
/// typed, in order, so the first refusal is the one Python raises first.
pub fn rows(payloads: &[Value]) -> Result<Vec<ExpenseTypeRow>, Refusal> {
    let parsed = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| parse(index, payload))
        .collect::<Result<Vec<_>, _>>()?;
    parsed
        .into_iter()
        .enumerate()
        .map(|(index, parsed)| bind(index, parsed))
        .collect()
}

/// `str.title()` of ASCII text, and `None` for any other.
///
/// CPython's `do_title`: a character is titlecased when the one before it
/// was not cased, and lowercased when it was. Over ASCII the cased
/// characters are the letters, and a letter's titlecase is its capital, so
/// `o'neil 2nd` becomes `O'Neil 2Nd`, as it does in Python.
pub fn title(text: &str) -> Option<String> {
    if !text.is_ascii() {
        return None;
    }
    let mut previous_is_cased = false;
    Some(
        text.chars()
            .map(|c| {
                let mapped = if previous_is_cased {
                    c.to_ascii_lowercase()
                } else {
                    c.to_ascii_uppercase()
                };
                previous_is_cased = c.is_ascii_alphabetic();
                mapped
            })
            .collect(),
    )
}

/// `upsert_expense_types`' statement, for `table`: Python's text, with the
/// table named.
pub fn upsert_sql(table: &str) -> String {
    let placeholders: Vec<String> = (1..=COLUMNS.len()).map(|i| format!("${i}")).collect();
    let updates: Vec<String> = COLUMNS
        .iter()
        .filter(|column| **column != "id")
        .map(|column| format!("{column} = EXCLUDED.{column}"))
        .collect();
    format!(
        "INSERT INTO {table} ({}, mirrored_at) VALUES ({}, now()) \
         ON CONFLICT (id) DO UPDATE SET {}, mirrored_at = EXCLUDED.mirrored_at",
        COLUMNS.join(", "),
        placeholders.join(", "),
        updates.join(", "),
    )
}

/// The shadow's statement, made once. sqlx takes only `&'static str` for a
/// query, so that a text built from data cannot reach one: this one is built
/// from the constants above and nothing else.
static SHADOW_UPSERT: LazyLock<String> = LazyLock::new(|| upsert_sql(SHADOW_TABLE));

/// The batch into the shadow, then the shadow's state row. The answer is in
/// `full_sync`'s keys: `{"expense_types": n}`, or `{"expense_types_error":
/// why}` with nothing written and the failure counted.
pub async fn ship_shadow(pool: &PgPool, timeout: Duration, payloads: &[Value]) -> Value {
    match write(
        pool,
        timeout,
        SHADOW_UPSERT.as_str(),
        SHADOW_STATE,
        payloads,
    )
    .await
    {
        Ok(written) => json!({ ANSWER_KEY: written }),
        Err(error) => refused(pool, timeout, &error).await,
    }
}

/// A batch that never reached the parse — a file that is not JSON this port
/// reads, or not a list — counted on the shadow's state row like any other
/// refusal.
pub async fn refused(pool: &PgPool, timeout: Duration, error: &str) -> Value {
    watermark::mark_failed(pool, timeout, SHADOW_STATE, error).await;
    json!({ ERROR_KEY: error })
}

/// One transaction: the rows in Python's order, then the state row, so the
/// two land together or neither does. A batch of none writes the state row
/// alone, as Python's sync stamps its watermark after storing nothing.
async fn write(
    pool: &PgPool,
    timeout: Duration,
    upsert: &'static str,
    state_row: &str,
    payloads: &[Value],
) -> Result<usize, String> {
    let rows = rows(payloads).map_err(|refusal| refusal.to_string())?;
    let mut tx = within(timeout, pool.begin())
        .await
        .map_err(|error| error.to_string())?;
    for row in &rows {
        within(
            timeout,
            sqlx::query(upsert)
                .bind(row.id)
                .bind(&row.name)
                .bind(&row.alias)
                .bind(row.is_active)
                .execute(&mut *tx),
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    let written = i64::try_from(rows.len()).unwrap_or(i64::MAX);
    within(
        timeout,
        sqlx::query(watermark::MARK_OK_SQL)
            .bind(state_row)
            .bind(written)
            .execute(&mut *tx),
    )
    .await
    .map_err(|error| error.to_string())?;
    within(timeout, tx.commit())
        .await
        .map_err(|error| error.to_string())?;
    Ok(rows.len())
}

/// Python's `payload.get(key, default)`: a key present and null is null.
fn got(fields: &Map<String, Value>, key: &str, default: Value) -> Value {
    fields.get(key).cloned().unwrap_or(default)
}

/// Python's truth of a value its `json` produced.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => number.as_f64().is_some_and(|n| n != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(fields) => !fields.is_empty(),
    }
}

/// asyncpg's `int4_encode`: an int as it is; anything else with `__int__`,
/// a bool or a float, through `int()`, which truncates toward zero; then
/// int32's range or `OverflowError`. `None` goes as NULL, which the primary
/// key refuses.
fn int4(value: &Value) -> Result<i32, Reason> {
    match value {
        Value::Null => Err(Reason::Null),
        Value::Bool(value) => Ok(i32::from(*value)),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                i32::try_from(integer).map_err(|_| Reason::OutOfRange)
            } else if number.is_u64() {
                Err(Reason::OutOfRange)
            } else {
                let truncated = number.as_f64().unwrap_or(f64::NAN).trunc();
                if (f64::from(i32::MIN)..=f64::from(i32::MAX)).contains(&truncated) {
                    // In range and whole, so the cast is exact.
                    Ok(truncated as i32)
                } else {
                    Err(Reason::OutOfRange)
                }
            }
        }
        Value::String(_) | Value::Array(_) | Value::Object(_) => Err(Reason::NotInteger),
    }
}

#[cfg(test)]
mod tests {
    //! What the contract cannot say: asyncpg's typing, which only a real
    //! asyncpg answers (the harness), restated here so `cargo test` fails
    //! first.

    use super::*;

    fn bound(payload: Value) -> Result<ExpenseTypeRow, Refusal> {
        rows(&[payload]).map(|mut rows| rows.remove(0))
    }

    #[test]
    fn an_id_is_typed_as_asyncpgs_int4_encoder_types_it() {
        let id = |value: Value| bound(json!({ "id": value, "name": "x" })).map(|row| row.id);
        assert_eq!(id(json!(7)), Ok(7));
        assert_eq!(id(json!(7.9)), Ok(7));
        assert_eq!(id(json!(-7.9)), Ok(-7));
        assert_eq!(id(json!(true)), Ok(1));
        assert_eq!(id(json!(false)), Ok(0));
        assert_eq!(id(json!(2_147_483_647)), Ok(i32::MAX));
        assert_eq!(id(json!(-2_147_483_648_i64)), Ok(i32::MIN));
        assert_eq!(id(json!(2_147_483_647.9)), Ok(i32::MAX));
        let refused = |value: Value| {
            bound(json!({ "id": value, "name": "x" }))
                .unwrap_err()
                .reason
        };
        assert_eq!(refused(json!(2_147_483_648_i64)), Reason::OutOfRange);
        assert_eq!(refused(json!(u64::MAX)), Reason::OutOfRange);
        assert_eq!(refused(json!(1e20)), Reason::OutOfRange);
        assert_eq!(refused(json!("7")), Reason::NotInteger);
        assert_eq!(refused(json!([7])), Reason::NotInteger);
        assert_eq!(refused(Value::Null), Reason::Null);
        assert_eq!(
            bound(json!({ "name": "x" })).unwrap_err().reason,
            Reason::Null
        );
    }

    #[test]
    fn alias_and_is_active_take_only_what_their_encoders_take() {
        let row = bound(json!({ "id": 1, "name": "x", "alias": "a", "is_active": false }));
        assert_eq!(row.unwrap().is_active, Some(false));
        let row = bound(json!({ "id": 1, "name": "x", "alias": null, "is_active": null }));
        assert_eq!(row.clone().unwrap().alias, None);
        assert_eq!(row.unwrap().is_active, None);
        assert_eq!(
            bound(json!({ "id": 1, "name": "x" })).unwrap().is_active,
            Some(true)
        );
        let refusal = |payload| bound(payload).unwrap_err();
        assert_eq!(
            refusal(json!({ "id": 1, "name": "x", "alias": 7 })),
            Refusal {
                index: 0,
                field: Field::Alias,
                reason: Reason::NotTextOrNull
            }
        );
        assert_eq!(
            refusal(json!({ "id": 1, "name": "x", "is_active": 1 })).reason,
            Reason::NotBoolOrNull
        );
        assert_eq!(
            refusal(json!({ "id": 1, "name": "x", "is_active": "true" })).reason,
            Reason::NotBoolOrNull
        );
    }

    #[test]
    fn the_first_refusal_is_pythons_first() {
        // Python parses the whole batch before asyncpg types a row: a parse
        // failure later in the batch is raised before a typing one earlier.
        let refusal = rows(&[
            json!({ "id": "bad", "name": "x" }),
            json!({ "id": 2, "name": null }),
        ])
        .unwrap_err();
        assert_eq!(
            refusal,
            Refusal {
                index: 1,
                field: Field::Name,
                reason: Reason::NotText
            }
        );
    }

    #[test]
    fn the_statement_names_the_table_it_is_given() {
        assert!(upsert_sql(SHADOW_TABLE).starts_with("INSERT INTO bronze.expense_types_rust ("));
        assert!(!upsert_sql(SHADOW_TABLE).contains("bronze.expense_types "));
    }
}
