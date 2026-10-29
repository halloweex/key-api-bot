//! The inventory arithmetic, case by case, against CPython itself.
//!
//! `fixtures/inventory_semantics.jsonl` holds rows and the answers CPython
//! 3.14 gave (`gen_inventory_fixtures.py` writes it): `get_dead_stock_deep`,
//! `get_inventory_turnover` and `get_brand_rotation` themselves, their reads
//! answered with the line's rows, as the bytes the app writes. A numeric
//! arrives as the float Python makes of it, or as its text where Python
//! cuts or hands on the `Decimal`. Compared byte for byte.

use std::collections::BTreeMap;

use ks_api::inventory::{
    AbcRow, BrandRow, DeepRow, ExcessRow, TurnoverParams, TurnoverRows, brands_json, deep_json,
    turnover_json,
};
use serde_json::Value;

const FIXTURES: &str = include_str!("fixtures/inventory_semantics.jsonl");

fn float(v: &Value) -> Option<f64> {
    v.as_str().map(|hex| {
        f64::from_bits(u64::from_str_radix(hex, 16).expect("a float is its bits in hex"))
    })
}

fn int(v: &Value) -> Option<i64> {
    v.as_i64()
}

fn text(v: &Value) -> Option<String> {
    v.as_str().map(str::to_owned)
}

fn deep_row(r: &Value) -> DeepRow {
    DeepRow {
        offer_id: int(&r["offer_id"]),
        sku: text(&r["sku"]),
        name: text(&r["name"]),
        brand: text(&r["brand"]),
        category_name: text(&r["category_name"]),
        available: int(&r["available"]),
        price: float(&r["price"]),
        purchased_price: float(&r["purchased_price"]),
        effective_unit_cost: float(&r["effective_unit_cost"]),
        cost_quality: text(&r["cost_quality"]),
        sale_value: float(&r["sale_value"]),
        cost_basis: float(&r["cost_basis"]),
        days_since_sale: int(&r["days_since_sale"]),
        days_in_stock: int(&r["days_in_stock"]),
        qty_sold_30d: int(&r["qty_sold_30d"]),
        qty_sold_90d: int(&r["qty_sold_90d"]),
        revenue_30d: float(&r["revenue_30d"]),
        revenue_90d: float(&r["revenue_90d"]),
        avg_daily_sales_30d: float(&r["avg_daily_sales_30d"]),
        avg_daily_sales_90d: float(&r["avg_daily_sales_90d"]),
        days_of_supply: float(&r["days_of_supply"]),
        velocity_tier: text(&r["velocity_tier"]),
        velocity_ratio_30_90: float(&r["velocity_ratio_30_90"]),
        abc_class: text(&r["abc_class"]),
        annual_gross_profit: float(&r["annual_gross_profit"]),
        gmroi: float(&r["gmroi"]),
    }
}

fn items(v: &Value) -> &Vec<Value> {
    v.as_array().expect("a list")
}

fn deep(case: &Value) -> Vec<u8> {
    let rows: Vec<DeepRow> = items(&case["rows"]).iter().map(deep_row).collect();
    deep_json(
        &rows,
        float(&case["carrying_rate"]).expect("a rate"),
        float(&case["discount"]).expect("a discount"),
        case["limit"].as_u64().map(|n| n as usize),
    )
    .expect("CPython answered this case")
}

fn turnover(case: &Value) -> Vec<u8> {
    let p = &case["params"];
    let (stock, st) = (&case["stock"], &case["st"]);
    let rows = TurnoverRows {
        revenue: float(&case["revenue"]),
        revenue_days: int(&case["revenue_days"]),
        stock_sale: float(&stock[0]),
        stock_cost: float(&stock[1]),
        stock_units: int(&stock[2]),
        active_skus: int(&stock[3]),
        abc: items(&case["abc"])
            .iter()
            .map(|r| AbcRow {
                class: text(&r[0]),
                sku_count: int(&r[1]),
                total_units: int(&r[2]),
                stock_value: float(&r[3]),
                revenue: float(&r[4]),
                stock_pct: float(&r[5]),
                revenue_pct: float(&r[6]),
            })
            .collect(),
        fast: int(&st[0]),
        slow: int(&st[1]),
        zero: int(&st[2]),
        avg_rate: float(&st[3]),
        median_days: float(&st[4]),
        excess: items(&case["excess"])
            .iter()
            .map(|r| ExcessRow {
                offer_id: int(&r[0]),
                sku: text(&r[1]),
                name: text(&r[2]),
                brand: text(&r[3]),
                category_name: text(&r[4]),
                available: int(&r[5]),
                value: float(&r[6]),
                days_of_supply: text(&r[7]),
                sell_through_rate: float(&r[8]),
                avg_daily_sales: float(&r[9]),
                revenue_90d: float(&r[10]),
            })
            .collect(),
    };
    turnover_json(
        &rows,
        TurnoverParams {
            days: int(&p["days"]).expect("days"),
            lead_time: int(&p["lead_time"]).expect("a lead time"),
            safety_multiplier: float(&p["safety_multiplier"]).expect("a multiplier"),
            buffer_days: int(&p["buffer_days"]).expect("buffer days"),
            max_acceptable_days: int(&p["max_acceptable_days"]).expect("max days"),
        },
    )
    .expect("CPython answered this case")
}

fn brands(case: &Value) -> Vec<u8> {
    let rows: Vec<BrandRow> = items(&case["rows"])
        .iter()
        .map(|r| BrandRow {
            brand: text(&r[0]),
            sku_count: int(&r[1]),
            frozen_skus: int(&r[2]),
            units: int(&r[3]),
            cost_basis: float(&r[4]),
            sale_value: float(&r[5]),
            revenue_90d: float(&r[6]),
            qty_sold_90d: text(&r[7]),
            annual_gross_profit: float(&r[8]),
        })
        .collect();
    brands_json(&rows).expect("CPython answered this case")
}

fn agrees(case: &Value) -> bool {
    let ours = match case["kind"].as_str().expect("a kind") {
        "made_by" => return true,
        "deep" => deep(case),
        "turnover" => turnover(case),
        "brands" => brands(case),
        other => panic!("a fixture kind this test does not know: {other}"),
    };
    ours == case["json"].as_str().expect("bytes").as_bytes()
}

#[test]
fn every_case_answers_as_cpython_did() {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut disagree = Vec::new();
    for line in FIXTURES.lines() {
        let case: Value = serde_json::from_str(line).expect("a JSON line");
        *seen
            .entry(case["kind"].as_str().unwrap_or("?").to_owned())
            .or_default() += 1;
        if !agrees(&case) {
            disagree.push(line.chars().take(400).collect::<String>());
        }
    }
    assert!(
        disagree.is_empty(),
        "{} case(s) answer differently from CPython, first ones:\n{}",
        disagree.len(),
        disagree
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    // The file is regenerated by hand; a kind that vanished would pass empty.
    for kind in ["deep", "turnover", "brands"] {
        assert!(
            seen.get(kind).copied().unwrap_or(0) >= 100,
            "too few {kind} cases: {seen:?}"
        );
    }
}
