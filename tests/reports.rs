use assert_cmd::prelude::*;
use serde_json::{Value, json};
use std::process::Command;

fn run_json(args: &[&str]) -> Value {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("ccusage"));
    let output = cmd
        .args(args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    serde_json::from_slice(&output).expect("valid json output")
}

#[test]
fn daily_report_matches_fixture() {
    let v = run_json(&[
        "--data-dir",
        "tests/fixtures",
        "daily",
        "--json",
        "--order",
        "asc",
        "--breakdown",
    ]);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["key"], "2024-12-01");
    assert_eq!(rows[0]["total_tokens"], json!(555));
    assert_eq!(rows[1]["key"], "2024-12-02");
    assert_eq!(rows[1]["total_tokens"], json!(235));
    assert_eq!(rows[2]["key"], "2024-12-03");
    assert_eq!(rows[2]["total_tokens"], json!(495));
    assert_eq!(v["totals"]["total_tokens"], json!(1285));
    assert_eq!(v["totals"]["cost_usd"], json!(1.05));
    assert_eq!(v["model_breakdowns"].as_array().unwrap().len(), 4);
}

#[test]
fn monthly_report_matches_fixture() {
    let v = run_json(&[
        "--data-dir",
        "tests/fixtures",
        "monthly",
        "--json",
        "--order",
        "asc",
        "--breakdown",
    ]);
    assert_eq!(v["rows"].as_array().unwrap().len(), 1);
    let row = &v["rows"][0];
    assert_eq!(row["key"], "2024-12");
    assert_eq!(row["total_tokens"], json!(1285));
    assert_eq!(row["cost_usd"], json!(1.05));
    assert_eq!(v["model_breakdowns"].as_array().unwrap().len(), 4);
}

#[test]
fn sessions_report_matches_fixture() {
    let v = run_json(&["--data-dir", "tests/fixtures", "sessions", "--json"]);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    let first = &rows[0]; // sorted by last_activity desc
    assert_eq!(first["session_id"], "sess-beta-2");
    assert_eq!(first["total_tokens"], json!(495));
    assert_eq!(v["totals"]["total_tokens"], json!(1285));
}

#[test]
fn blocks_report_matches_fixture() {
    let v = run_json(&[
        "--data-dir",
        "tests/fixtures",
        "blocks",
        "--json",
        "--order",
        "asc",
        "--breakdown",
    ]);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().any(
        |r| r["block_start"] == "2024-12-01T10:00:00+00:00" && r["total_tokens"] == json!(485)
    ));
    assert!(v["totals"]["total_tokens"] == json!(1285));
    assert_eq!(v["model_breakdowns"].as_array().unwrap().len(), 4);
}
