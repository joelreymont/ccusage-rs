#![deny(warnings)]

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::File,
    io::{BufRead, BufReader, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::mpsc::{Receiver, RecvTimeoutError, channel},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use colored::Colorize;
use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, Timelike, Utc};
use chrono_tz::Tz;
use clap::{Args, Parser, Subcommand, ValueEnum};
use crossterm::{
    ExecutableCommand,
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode, size as terminal_size},
};
use directories::ProjectDirs;
use jsonschema::{Draft, JSONSchema};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use rayon::prelude::*;
use num_format::{Locale, ToFormattedString};
use once_cell::sync::OnceCell;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Modifier, Style},
    text::Span,
    widgets::{Block as TuiBlock, Borders, Cell, Row as TuiRow, Table as TuiTable},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use walkdir::WalkDir;

const PROJECTS_DIR: &str = "projects";
const DEFAULT_BLOCK_HOURS: u32 = 5;
const DEFAULT_RECENT_DAYS: u32 = 3;
const DEFAULT_REFRESH_SECONDS: u64 = 5;

#[derive(Copy, Clone, Debug, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Order {
    Asc,
    Desc,
}

impl Default for Order {
    fn default() -> Self {
        Order::Desc
    }
}

#[derive(Copy, Clone, Debug, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum WeekStart {
    Sunday,
    Monday,
}

impl Default for WeekStart {
    fn default() -> Self {
        WeekStart::Monday
    }
}

#[derive(Copy, Clone, Debug, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CostMode {
    Auto,
    PreferField,
    Calculate,
}

impl Default for CostMode {
    fn default() -> Self {
        CostMode::Auto
    }
}

#[derive(Parser, Debug)]
#[command(name = "ccusage-rs", about = "Analyze Claude Code JSONL usage locally")]
struct Cli {
    /// Override Claude data dirs (defaults: ~/.config/claude, ~/.claude). You can repeat this flag.
    #[arg(long = "data-dir", global = true)]
    data_dirs: Vec<PathBuf>,

    /// Optional config file (JSON). If omitted, tries ./ccusage.json then ~/.config/ccusage/config.json
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    /// Timezone (IANA name, e.g. UTC, America/Los_Angeles)
    #[arg(long, global = true)]
    timezone: Option<String>,

    /// Locale for number formatting (e.g. en, fr, de). Falls back to en.
    #[arg(long, global = true)]
    locale: Option<String>,

    /// Output JSON instead of tables (can be passed as --json or --json=false)
    #[arg(long, global = true, num_args = 0..=1, default_missing_value = "true")]
    json: Option<bool>,

    /// Compact table (drop cache columns)
    #[arg(long, global = true, num_args = 0..=1, default_missing_value = "true")]
    compact: Option<bool>,

    /// Include per-model breakdowns
    #[arg(long, global = true, num_args = 0..=1, default_missing_value = "true")]
    breakdown: Option<bool>,

    /// Offline mode (pricing stays local; currently always local)
    #[arg(long, global = true, num_args = 0..=1, default_missing_value = "true")]
    offline: Option<bool>,

    /// Cost calculation mode (auto: use costUSD if present, otherwise calculate; calculate: always calculate; prefer_field: only use costUSD when present)
    #[arg(long, global = true, value_enum)]
    cost_mode: Option<CostMode>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
    /// Default: daily usage aggregated by date
    Daily(RangeArgs),
    /// Weekly usage aggregated by week
    Weekly(WeeklyArgs),
    /// Monthly usage aggregated by month
    Monthly(RangeArgs),
    /// Session totals aggregated by session id
    Sessions(RangeArgs),
    /// 5-hour billing window view
    Blocks(BlocksArgs),
    /// Compact statusline summary for most recent day
    Statusline(RangeArgs),
}

#[derive(Args, Debug, Clone, Default)]
struct RangeArgs {
    /// Filter start date (YYYY-MM-DD, inclusive, in selected timezone)
    #[arg(long)]
    since: Option<String>,

    /// Filter end date (YYYY-MM-DD, inclusive, in selected timezone)
    #[arg(long)]
    until: Option<String>,

    /// Only include a specific project name (derived from path under projects/)
    #[arg(long)]
    project: Option<String>,

    /// Group daily/weekly/monthly output by project/instance as well
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    instances: Option<bool>,

    /// Sort order
    #[arg(long, value_enum)]
    order: Option<Order>,
}

#[derive(Args, Debug, Clone, Default)]
struct WeeklyArgs {
    #[command(flatten)]
    range: RangeArgs,

    /// Start of week (default monday)
    #[arg(long = "start-of-week", value_enum)]
    start_of_week: Option<WeekStart>,
}

#[derive(Args, Debug, Clone, Default)]
struct BlocksArgs {
    #[command(flatten)]
    range: RangeArgs,

    /// Only include recent N days (default 3)
    #[arg(long)]
    recent_days: Option<u32>,

    /// Token limit per 5-hour block (for % used)
    #[arg(long)]
    token_limit: Option<u64>,

    /// Length of a billing block in hours (default 5)
    #[arg(long)]
    session_length_hours: Option<u32>,

    /// Live mode: refresh every interval until Ctrl+C
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    live: Option<bool>,

    /// Refresh interval seconds for live mode (default 5)
    #[arg(long)]
    refresh_seconds: Option<u64>,

    /// Render live mode as a TUI dashboard
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    tui: Option<bool>,
}

#[derive(Default, Deserialize)]
struct DefaultsConfig {
    json: Option<bool>,
    compact: Option<bool>,
    breakdown: Option<bool>,
    offline: Option<bool>,
    cost_mode: Option<CostMode>,
    timezone: Option<String>,
    locale: Option<String>,
    order: Option<Order>,
    instances: Option<bool>,
    since: Option<String>,
    until: Option<String>,
    project: Option<String>,
    start_of_week: Option<WeekStart>,
    token_limit: Option<u64>,
    recent_days: Option<u32>,
    session_length_hours: Option<u32>,
    refresh_seconds: Option<u64>,
    live: Option<bool>,
    tui: Option<bool>,
}

#[derive(Default, Deserialize)]
struct CommandConfigs {
    daily: Option<DefaultsConfig>,
    weekly: Option<DefaultsConfig>,
    monthly: Option<DefaultsConfig>,
    sessions: Option<DefaultsConfig>,
    blocks: Option<DefaultsConfig>,
    statusline: Option<DefaultsConfig>,
}

#[derive(Default, Deserialize)]
struct FileConfig {
    defaults: Option<DefaultsConfig>,
    commands: Option<CommandConfigs>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct RawRecord {
    #[serde(default)]
    cwd: Option<String>,
    #[serde(rename = "sessionId", default)]
    session_id: Option<String>,
    timestamp: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    message: Option<RawMessage>,
    #[serde(default)]
    usage: Option<RawUsage>,
    #[serde(rename = "costUSD", default)]
    cost_usd: Option<f64>,
    #[serde(rename = "requestId", default)]
    request_id: Option<String>,
    #[serde(default)]
    is_api_error_message: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct RawMessage {
    #[serde(default)]
    usage: Option<RawUsage>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cache_creation_input_tokens: Option<u64>,
    cache_read_input_tokens: Option<u64>,
}

#[derive(Debug, Clone)]
struct UsageEvent {
    timestamp: DateTime<Utc>,
    project: String,
    session_id: String,
    model: Option<String>,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_tokens: u64,
    cache_read_tokens: u64,
    cost_usd: f64,
}

#[derive(Debug, Serialize, Clone)]
struct ModelBreakdown {
    model: String,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_tokens: u64,
    cache_read_tokens: u64,
    total_tokens: u64,
    cost_usd: f64,
}

#[derive(Debug, Serialize)]
struct Row {
    key: String,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_tokens: u64,
    cache_read_tokens: u64,
    total_tokens: u64,
    cost_usd: f64,
    models: BTreeSet<String>,
    projects: BTreeSet<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    model_breakdowns: Vec<ModelBreakdown>,
}

#[derive(Debug, Serialize)]
struct Totals {
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_tokens: u64,
    cache_read_tokens: u64,
    total_tokens: u64,
    cost_usd: f64,
}

#[derive(Debug, Serialize)]
struct JsonReport {
    kind: &'static str,
    timezone: String,
    locale: String,
    since: Option<String>,
    until: Option<String>,
    rows: Vec<Row>,
    totals: Totals,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    model_breakdowns: Vec<ModelBreakdown>,
}

#[derive(Debug, Serialize)]
struct SessionRow {
    session_id: String,
    project: String,
    last_activity: String,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_tokens: u64,
    cache_read_tokens: u64,
    total_tokens: u64,
    cost_usd: f64,
    models: BTreeSet<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    model_breakdowns: Vec<ModelBreakdown>,
}

#[derive(Debug, Serialize)]
struct SessionReport {
    kind: &'static str,
    timezone: String,
    locale: String,
    since: Option<String>,
    until: Option<String>,
    rows: Vec<SessionRow>,
    totals: Totals,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    model_breakdowns: Vec<ModelBreakdown>,
}

#[derive(Debug, Serialize)]
struct BlockRow {
    block_start: String,
    block_end: String,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_tokens: u64,
    cache_read_tokens: u64,
    total_tokens: u64,
    cost_usd: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    percent_of_limit: Option<f64>,
    models: BTreeSet<String>,
    projects: BTreeSet<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    model_breakdowns: Vec<ModelBreakdown>,
}

#[derive(Debug, Serialize)]
struct BlocksReport {
    kind: &'static str,
    timezone: String,
    locale: String,
    recent_days: u32,
    token_limit: Option<u64>,
    rows: Vec<BlockRow>,
    totals: Totals,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    model_breakdowns: Vec<ModelBreakdown>,
}

#[derive(Debug, Serialize)]
struct StatuslineReport {
    kind: &'static str,
    timezone: String,
    locale: String,
    last_date: Option<String>,
    totals: Totals,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Pricing {
    input_per_million: f64,
    output_per_million: f64,
    #[serde(default)]
    cache_create_per_million: f64,
    #[serde(default)]
    cache_read_per_million: f64,
}

#[derive(Clone)]
struct PricingIndex {
    entries: Vec<(String, Pricing)>,
}

impl PricingIndex {
    fn from_map(map: HashMap<String, Pricing>) -> Self {
        let mut entries: Vec<_> = map.into_iter().collect();
        // Longest prefix first for matching
        entries.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
        PricingIndex { entries }
    }

    fn find(&self, model: &str) -> Option<&Pricing> {
        self.entries
            .iter()
            .find(|(prefix, _)| model.starts_with(prefix))
            .map(|(_, p)| p)
    }
}

static PRICING_INDEX: OnceCell<PricingIndex> = OnceCell::new();
static COST_MODE: OnceCell<CostMode> = OnceCell::new();
static CONFIG_SCHEMA: OnceCell<JSONSchema> = OnceCell::new();
static CONFIG_SCHEMA_JSON: OnceCell<Value> = OnceCell::new();

fn main() -> Result<()> {
    let cli = Cli::parse();
    let file_cfg = load_config(cli.config.as_ref())?;

    let global_defaults = file_cfg.defaults.as_ref();

    let tz_raw = resolve_string(
        cli.timezone.as_ref(),
        None,
        global_defaults.and_then(|d| d.timezone.as_ref()),
    )
    .unwrap_or_else(|| "UTC".to_string());
    let tz = parse_timezone(Some(tz_raw))?;

    let locale_raw = resolve_string(
        cli.locale.as_ref(),
        None,
        global_defaults.and_then(|d| d.locale.as_ref()),
    )
    .unwrap_or_else(|| "en".to_string());
    let locale = resolve_locale(Some(locale_raw));

    let offline_pricing = resolve_bool(
        cli.offline,
        None,
        global_defaults.and_then(|d| d.offline),
        false,
    );
    let cost_mode = resolve_cost_mode(cli.cost_mode, global_defaults.and_then(|d| d.cost_mode));
    let pricing_index = build_pricing_index(load_pricing(offline_pricing)?);
    let _ = PRICING_INDEX.set(pricing_index);
    let _ = COST_MODE.set(cost_mode);

    let data_dirs = resolve_data_dirs(&cli.data_dirs)?;
    let files = collect_jsonl_files(&data_dirs);

    if files.is_empty() {
        println!(
            "No Claude JSONL files found. Looked under: {}",
            display_paths(&data_dirs)
        );
        return Ok(());
    }

    let events = load_events(&files)?;
    if events.is_empty() {
        println!("No usage entries parsed from JSONL files.");
        return Ok(());
    }

    let cmd = cli
        .command
        .clone()
        .unwrap_or(Command::Daily(RangeArgs::default()));

    match cmd {
        Command::Daily(args) => {
            let cmd_cfg = file_cfg.commands.as_ref().and_then(|c| c.daily.as_ref());
            let opts = resolve_common(&cli, cmd_cfg, global_defaults);
            let range = resolve_range(&args, cmd_cfg, global_defaults)?;
            let order = resolve_order(
                args.order,
                cmd_cfg.and_then(|c| c.order),
                global_defaults.and_then(|d| d.order),
            );
            let instances = resolve_bool(
                args.instances,
                cmd_cfg.and_then(|c| c.instances),
                global_defaults.and_then(|d| d.instances),
                false,
            );
            let report = build_daily_report(
                &events,
                &tz,
                &locale,
                &range,
                order,
                instances,
                opts.breakdown,
            )?;
            output_rows(
                report,
                opts.json,
                opts.compact,
                opts.breakdown,
                &locale,
                "Daily",
            );
        }
        Command::Weekly(args) => {
            let cmd_cfg = file_cfg.commands.as_ref().and_then(|c| c.weekly.as_ref());
            let opts = resolve_common(&cli, cmd_cfg, global_defaults);
            let range = resolve_range(&args.range, cmd_cfg, global_defaults)?;
            let order = resolve_order(
                args.range.order,
                cmd_cfg.and_then(|c| c.order),
                global_defaults.and_then(|d| d.order),
            );
            let instances = resolve_bool(
                args.range.instances,
                cmd_cfg.and_then(|c| c.instances),
                global_defaults.and_then(|d| d.instances),
                false,
            );
            let start_of_week = resolve_week_start(
                args.start_of_week,
                cmd_cfg.and_then(|c| c.start_of_week),
                global_defaults.and_then(|d| d.start_of_week),
            );
            let report = build_weekly_report(
                &events,
                &tz,
                &locale,
                &range,
                order,
                instances,
                start_of_week,
                opts.breakdown,
            )?;
            output_rows(
                report,
                opts.json,
                opts.compact,
                opts.breakdown,
                &locale,
                "Weekly",
            );
        }
        Command::Monthly(args) => {
            let cmd_cfg = file_cfg.commands.as_ref().and_then(|c| c.monthly.as_ref());
            let opts = resolve_common(&cli, cmd_cfg, global_defaults);
            let range = resolve_range(&args, cmd_cfg, global_defaults)?;
            let order = resolve_order(
                args.order,
                cmd_cfg.and_then(|c| c.order),
                global_defaults.and_then(|d| d.order),
            );
            let instances = resolve_bool(
                args.instances,
                cmd_cfg.and_then(|c| c.instances),
                global_defaults.and_then(|d| d.instances),
                false,
            );
            let report = build_monthly_report(
                &events,
                &tz,
                &locale,
                &range,
                order,
                instances,
                opts.breakdown,
            )?;
            output_rows(
                report,
                opts.json,
                opts.compact,
                opts.breakdown,
                &locale,
                "Monthly",
            );
        }
        Command::Sessions(args) => {
            let cmd_cfg = file_cfg.commands.as_ref().and_then(|c| c.sessions.as_ref());
            let opts = resolve_common(&cli, cmd_cfg, global_defaults);
            let range = resolve_range(&args, cmd_cfg, global_defaults)?;
            let report = build_session_report(&events, &tz, &locale, &range, opts.breakdown)?;
            output_sessions(report, opts.json, &locale);
        }
        Command::Blocks(args) => {
            let cmd_cfg = file_cfg.commands.as_ref().and_then(|c| c.blocks.as_ref());
            let opts = resolve_common(&cli, cmd_cfg, global_defaults);
            let range = resolve_range(&args.range, cmd_cfg, global_defaults)?;
            let token_limit = resolve_u64(
                args.token_limit,
                cmd_cfg.and_then(|c| c.token_limit),
                global_defaults.and_then(|d| d.token_limit),
                500_000,
            );
            let recent_days = resolve_u32(
                args.recent_days,
                cmd_cfg.and_then(|c| c.recent_days),
                global_defaults.and_then(|d| d.recent_days),
                DEFAULT_RECENT_DAYS,
            );
            let session_length_hours = resolve_u32(
                args.session_length_hours,
                cmd_cfg.and_then(|c| c.session_length_hours),
                global_defaults.and_then(|d| d.session_length_hours),
                DEFAULT_BLOCK_HOURS,
            );
            let live = resolve_bool(
                args.live,
                cmd_cfg.and_then(|c| c.live),
                global_defaults.and_then(|d| d.live),
                false,
            );
            let refresh_seconds = resolve_u64(
                args.refresh_seconds,
                cmd_cfg.and_then(|c| c.refresh_seconds),
                global_defaults.and_then(|d| d.refresh_seconds),
                DEFAULT_REFRESH_SECONDS,
            );
            let use_tui = resolve_bool(
                args.tui,
                cmd_cfg.and_then(|c| c.tui),
                global_defaults.and_then(|d| d.tui),
                false,
            );

            if live {
                let mut live_source =
                    LiveEventSource::from_existing(data_dirs.clone(), &files, events.clone())?;
                let (_watcher, rx) = watch_data_dirs(&data_dirs)?;
                if use_tui {
                    run_blocks_live_tui(
                        &mut live_source,
                        &tz,
                        &locale,
                        &range,
                        token_limit,
                        recent_days,
                        session_length_hours,
                        opts.breakdown,
                        refresh_seconds,
                        rx,
                    )?;
                } else {
                    run_blocks_live_cli(
                        &mut live_source,
                        &tz,
                        &locale,
                        &range,
                        token_limit,
                        recent_days,
                        session_length_hours,
                        opts,
                        refresh_seconds,
                        rx,
                    )?;
                }
            } else {
                let report = build_blocks_report(
                    &events,
                    &tz,
                    &locale,
                    &range,
                    token_limit,
                    recent_days,
                    session_length_hours,
                    opts.breakdown,
                )?;
                output_blocks(report, opts.json, opts.compact, opts.breakdown, &locale);
            }
        }
        Command::Statusline(args) => {
            let cmd_cfg = file_cfg
                .commands
                .as_ref()
                .and_then(|c| c.statusline.as_ref());
            let opts = resolve_common(&cli, cmd_cfg, global_defaults);
            let range = resolve_range(&args, cmd_cfg, global_defaults)?;
            let report = build_statusline_report(&events, &tz, &range)?;
            output_statusline(report, opts.json);
        }
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct CommonOptions {
    json: bool,
    compact: bool,
    breakdown: bool,
}

fn resolve_common(
    cli: &Cli,
    cmd: Option<&DefaultsConfig>,
    defaults: Option<&DefaultsConfig>,
) -> CommonOptions {
    CommonOptions {
        json: resolve_bool(
            cli.json,
            cmd.and_then(|c| c.json),
            defaults.and_then(|d| d.json),
            false,
        ),
        compact: resolve_bool(
            cli.compact,
            cmd.and_then(|c| c.compact),
            defaults.and_then(|d| d.compact),
            false,
        ),
        breakdown: resolve_bool(
            cli.breakdown,
            cmd.and_then(|c| c.breakdown),
            defaults.and_then(|d| d.breakdown),
            false,
        ),
    }
}

#[derive(Clone)]
struct RangeFilter {
    since: Option<NaiveDate>,
    until: Option<NaiveDate>,
    project: Option<String>,
}

fn resolve_range(
    args: &RangeArgs,
    cmd: Option<&DefaultsConfig>,
    defaults: Option<&DefaultsConfig>,
) -> Result<RangeFilter> {
    let since = resolve_string(
        args.since.as_ref(),
        cmd.and_then(|c| c.since.as_ref()),
        defaults.and_then(|d| d.since.as_ref()),
    );
    let until = resolve_string(
        args.until.as_ref(),
        cmd.and_then(|c| c.until.as_ref()),
        defaults.and_then(|d| d.until.as_ref()),
    );

    Ok(RangeFilter {
        since: parse_date_opt(since.as_deref())?,
        until: parse_date_opt(until.as_deref())?,
        project: resolve_string(
            args.project.as_ref(),
            cmd.and_then(|c| c.project.as_ref()),
            defaults.and_then(|d| d.project.as_ref()),
        ),
    })
}

fn resolve_order(cli: Option<Order>, cmd: Option<Order>, defaults: Option<Order>) -> Order {
    cli.or(cmd).or(defaults).unwrap_or(Order::Desc)
}

fn resolve_week_start(
    cli: Option<WeekStart>,
    cmd: Option<WeekStart>,
    defaults: Option<WeekStart>,
) -> WeekStart {
    cli.or(cmd).or(defaults).unwrap_or(WeekStart::Monday)
}

fn resolve_cost_mode(cli: Option<CostMode>, defaults: Option<CostMode>) -> CostMode {
    cli.or(defaults).unwrap_or(CostMode::Auto)
}

fn resolve_bool(
    cli: Option<bool>,
    cmd: Option<bool>,
    defaults: Option<bool>,
    fallback: bool,
) -> bool {
    cli.or(cmd).or(defaults).unwrap_or(fallback)
}

fn resolve_u64(cli: Option<u64>, cmd: Option<u64>, defaults: Option<u64>, fallback: u64) -> u64 {
    cli.or(cmd).or(defaults).unwrap_or(fallback)
}

fn resolve_u32(cli: Option<u32>, cmd: Option<u32>, defaults: Option<u32>, fallback: u32) -> u32 {
    cli.or(cmd).or(defaults).unwrap_or(fallback)
}

fn resolve_string(
    cli: Option<&String>,
    cmd: Option<&String>,
    defaults: Option<&String>,
) -> Option<String> {
    cli.cloned()
        .or_else(|| cmd.cloned())
        .or_else(|| defaults.cloned())
}

fn load_config(path: Option<&PathBuf>) -> Result<FileConfig> {
    let candidate = if let Some(p) = path {
        Some(p.clone())
    } else {
        let cwd = PathBuf::from("ccusage.json");
        if cwd.exists() {
            Some(cwd)
        } else if let Some(home) = home_dir() {
            let default = home.join(".config/ccusage/config.json");
            if default.exists() {
                Some(default)
            } else {
                None
            }
        } else {
            None
        }
    };

    if let Some(path) = candidate {
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file {}", path.display()))?;
        let value: Value = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse config file {}", path.display()))?;
        validate_config(&value)?;
        let cfg: FileConfig = serde_json::from_value(value)
            .with_context(|| format!("Failed to deserialize config file {}", path.display()))?;
        Ok(cfg)
    } else {
        Ok(FileConfig::default())
    }
}

fn validate_config(value: &Value) -> Result<()> {
    let schema_value = CONFIG_SCHEMA_JSON.get_or_try_init(|| {
        let schema_str = include_str!("../config-schema.json");
        serde_json::from_str(schema_str).context("parsing config schema")
    })?;
    let schema = CONFIG_SCHEMA.get_or_try_init(|| {
        JSONSchema::options()
            .with_draft(Draft::Draft7)
            .compile(schema_value)
            .context("compiling config schema")
    })?;

    if let Err(errors) = schema.validate(value) {
        let mut msg = String::from("Config validation failed:\n");
        for err in errors {
            msg.push_str(&format!(" - {} at {}\n", err, err.instance_path));
        }
        anyhow::bail!(msg.trim_end().to_string());
    }
    Ok(())
}

fn resolve_locale(raw: Option<String>) -> Locale {
    match raw.as_deref() {
        Some("en") | Some("en-US") | Some("en-GB") | None => Locale::en,
        Some("fr") | Some("fr-FR") => Locale::fr,
        Some("de") | Some("de-DE") => Locale::de,
        Some("es") | Some("es-ES") => Locale::es,
        Some("it") | Some("it-IT") => Locale::it,
        Some("ja") | Some("ja-JP") => Locale::ja,
        _ => Locale::en,
    }
}

fn parse_timezone(raw: Option<String>) -> Result<Tz> {
    let tz_str = raw.unwrap_or_else(|| "UTC".to_string());
    tz_str.parse::<Tz>().map_err(|e| {
        anyhow!(
            "Invalid timezone '{}': {}. Example: 'UTC' or 'America/Los_Angeles'",
            tz_str,
            e
        )
    })
}

fn resolve_data_dirs(cli_dirs: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    if !cli_dirs.is_empty() {
        for dir in cli_dirs {
            dirs.push(expand_tilde(dir));
        }
        return Ok(dirs);
    }

    if let Ok(env_paths) = std::env::var("CLAUDE_CONFIG_DIR") {
        for p in env_paths.split(',') {
            if !p.trim().is_empty() {
                dirs.push(expand_tilde(&PathBuf::from(p.trim())));
            }
        }
    }

    if dirs.is_empty() {
        if let Some(home) = home_dir() {
            dirs.push(home.join(".config/claude"));
            dirs.push(home.join(".claude"));
        } else {
            anyhow::bail!(
                "Could not determine home directory. Set --data-dir or CLAUDE_CONFIG_DIR."
            );
        }
    }

    Ok(dirs)
}

fn expand_tilde(path: &Path) -> PathBuf {
    if let Some(path_str) = path.to_str() {
        if let Some(rest) = path_str.strip_prefix("~/") {
            if let Some(home) = home_dir() {
                return home.join(rest);
            }
        }
    }
    path.to_path_buf()
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

fn load_pricing(offline: bool) -> Result<HashMap<String, Pricing>> {
    if offline {
        return load_bundled_pricing();
    }
    if let Some(cache_path) = pricing_cache_path() {
        if let Ok(map) = load_pricing_file(&cache_path) {
            return Ok(map);
        }
    }
    match fetch_remote_pricing() {
        Ok(map) => {
            if let Some(cache_path) = pricing_cache_path() {
                let _ = save_pricing_cache(&cache_path, &map);
            }
            Ok(map)
        }
        Err(err) => {
            eprintln!("Failed to fetch pricing remotely ({err}); falling back to bundled pricing");
            if let Some(cache_path) = pricing_cache_path() {
                if let Ok(map) = load_pricing_file(&cache_path) {
                    return Ok(map);
                }
            }
            load_bundled_pricing()
        }
    }
}

fn fetch_remote_pricing() -> Result<HashMap<String, Pricing>> {
    // LiteLLM pricing dataset
    const URL: &str = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";
    let resp = ureq::get(URL)
        .timeout(std::time::Duration::from_secs(5))
        .call()
        .context("fetching remote pricing")?;
    let json: serde_json::Value = resp.into_json().context("parsing pricing json")?;
    let mut out = HashMap::new();
    if let Some(obj) = json.as_object() {
        for (key, val) in obj {
            let Some(input_per_token) = val.get("input_cost_per_token").and_then(|v| v.as_f64())
            else {
                continue;
            };
            let Some(output_per_token) = val.get("output_cost_per_token").and_then(|v| v.as_f64())
            else {
                continue;
            };
            let cache_create = val
                .get("cache_creation_input_token_cost")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let cache_read = val
                .get("cache_read_input_token_cost")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            out.insert(
                key.clone(),
                Pricing {
                    input_per_million: input_per_token * 1_000_000.0,
                    output_per_million: output_per_token * 1_000_000.0,
                    cache_create_per_million: cache_create * 1_000_000.0,
                    cache_read_per_million: cache_read * 1_000_000.0,
                },
            );
        }
    }
    if out.is_empty() {
        anyhow::bail!("remote pricing dataset empty");
    }
    Ok(out)
}

fn load_bundled_pricing() -> Result<HashMap<String, Pricing>> {
    let raw = include_str!("../data/pricing.json");
    let map: HashMap<String, Pricing> =
        serde_json::from_str(raw).context("parsing bundled pricing")?;
    Ok(map)
}

fn pricing_cache_path() -> Option<PathBuf> {
    ProjectDirs::from("com", "ccusage", "ccusage-rs")
        .map(|dirs| dirs.cache_dir().join("pricing.json"))
}

fn load_pricing_file(path: &Path) -> Result<HashMap<String, Pricing>> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("reading pricing cache {}", path.display()))?;
    let map: HashMap<String, Pricing> = serde_json::from_str(&contents)
        .with_context(|| format!("parsing pricing cache {}", path.display()))?;
    Ok(map)
}

fn save_pricing_cache(path: &Path, map: &HashMap<String, Pricing>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating cache dir {}", parent.display()))?;
    }
    let contents = serde_json::to_string_pretty(map).context("serializing pricing cache")?;
    std::fs::write(path, contents)
        .with_context(|| format!("writing pricing cache {}", path.display()))?;
    Ok(())
}

fn build_pricing_index(map: HashMap<String, Pricing>) -> PricingIndex {
    let mut combined: HashMap<String, Pricing> = HashMap::new();
    for (key, pricing) in map {
        combined
            .entry(key.clone())
            .or_insert_with(|| pricing.clone());
        let normalized = normalize_model_for_pricing(&key);
        combined.entry(normalized).or_insert(pricing);
    }
    PricingIndex::from_map(combined)
}

fn watch_data_dirs(paths: &[PathBuf]) -> Result<(RecommendedWatcher, Receiver<()>)> {
    let (tx, rx) = channel();
    let mut watcher = notify::recommended_watcher(move |res| {
        if let Err(err) = res {
            eprintln!("watch error: {err}");
            return;
        }
        let _ = tx.send(());
    })
    .context("creating file watcher")?;

    for dir in paths {
        watcher
            .watch(dir, RecursiveMode::Recursive)
            .with_context(|| format!("watching {}", dir.display()))?;
    }

    Ok((watcher, rx))
}

fn collect_jsonl_files(data_dirs: &[PathBuf]) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for dir in data_dirs {
        let project_root = dir.join(PROJECTS_DIR);
        if !project_root.exists() {
            continue;
        }
        for entry in WalkDir::new(project_root)
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.into_path();
            if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                files.push(path);
            }
        }
    }
    files
}

fn load_events(files: &[PathBuf]) -> Result<Vec<UsageEvent>> {
    let events: Vec<UsageEvent> = files
        .par_iter()
        .flat_map(|file| {
            let project = extract_project_name(file);
            let session_id = file
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string();

            let fh = match File::open(file) {
                Ok(f) => f,
                Err(_) => return Vec::new(),
            };
            let mut file_events = Vec::new();
            for line in BufReader::new(fh).lines() {
                let line = match line {
                    Ok(l) => l,
                    Err(_) => continue,
                };
                // Fast pre-filter: skip lines without usage data
                if !line.contains("input_tokens") {
                    continue;
                }
                let parsed: RawRecord = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if let Some(event) = to_usage_event(parsed, project.clone(), session_id.clone()) {
                    file_events.push(event);
                }
            }
            file_events
        })
        .collect();
    Ok(events)
}

struct LiveEventSource {
    data_dirs: Vec<PathBuf>,
    offsets: HashMap<PathBuf, u64>,
    events: Vec<UsageEvent>,
}

impl LiveEventSource {
    fn from_existing(
        data_dirs: Vec<PathBuf>,
        files: &[PathBuf],
        events: Vec<UsageEvent>,
    ) -> Result<Self> {
        let mut offsets = HashMap::new();
        for file in files {
            let len = std::fs::metadata(file)
                .with_context(|| format!("stat {}", file.display()))?
                .len();
            offsets.insert(file.clone(), len);
        }
        Ok(Self {
            data_dirs,
            offsets,
            events,
        })
    }

    fn events(&self) -> &[UsageEvent] {
        &self.events
    }

    fn refresh(&mut self) -> Result<()> {
        let files = collect_jsonl_files(&self.data_dirs);
        for file in files {
            let current_len = std::fs::metadata(&file)
                .with_context(|| format!("stat {}", file.display()))?
                .len();
            let offset = self.offsets.get(&file).copied().unwrap_or(0);
            let start = if current_len < offset { 0 } else { offset };
            let (new_offset, mut new_events) = read_new_events(&file, start)?;
            if !new_events.is_empty() {
                self.events.append(&mut new_events);
            }
            self.offsets.insert(file, new_offset);
        }
        Ok(())
    }
}

fn read_new_events(path: &Path, start: u64) -> Result<(u64, Vec<UsageEvent>)> {
    let project = extract_project_name(path);
    let session_id = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    let mut events = Vec::new();
    let mut file =
        File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
    let mut reader = BufReader::new(&mut file);
    reader.seek(SeekFrom::Start(start))?;
    let mut position = start;

    loop {
        let mut buf = String::new();
        let bytes = reader.read_line(&mut buf)?;
        if bytes == 0 {
            break;
        }
        position += bytes as u64;
        if buf.trim().is_empty() {
            continue;
        }
        let parsed: RawRecord = match serde_json::from_str(&buf) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(event) = to_usage_event(parsed, project.clone(), session_id.clone()) {
            events.push(event);
        }
    }

    Ok((position, events))
}

fn to_usage_event(raw: RawRecord, project: String, session_id: String) -> Option<UsageEvent> {
    let ts = parse_timestamp(&raw.timestamp)?;
    let message = raw.message;
    let usage = message
        .as_ref()
        .and_then(|m| m.usage.as_ref())
        .or(raw.usage.as_ref())?;

    let input_tokens = usage.input_tokens.unwrap_or(0);
    let output_tokens = usage.output_tokens.unwrap_or(0);
    let cache_creation_tokens = usage.cache_creation_input_tokens.unwrap_or(0);
    let cache_read_tokens = usage.cache_read_input_tokens.unwrap_or(0);
    let cost = match COST_MODE.get().copied().unwrap_or(CostMode::Auto) {
        CostMode::Calculate => calculate_cost(
            message.as_ref().and_then(|m| m.model.as_deref()),
            input_tokens,
            output_tokens,
            cache_creation_tokens,
            cache_read_tokens,
        ),
        CostMode::PreferField | CostMode::Auto => raw.cost_usd.unwrap_or_else(|| {
            calculate_cost(
                message.as_ref().and_then(|m| m.model.as_deref()),
                input_tokens,
                output_tokens,
                cache_creation_tokens,
                cache_read_tokens,
            )
        }),
    };

    Some(UsageEvent {
        timestamp: ts,
        project,
        session_id: raw.session_id.unwrap_or(session_id),
        model: message.and_then(|m| m.model),
        input_tokens,
        output_tokens,
        cache_creation_tokens,
        cache_read_tokens,
        cost_usd: cost,
    })
}

fn parse_timestamp(ts: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn normalize_model_for_pricing(model: &str) -> String {
    let mut m = model.to_lowercase();
    if let Some(rest) = m.strip_prefix("anthropic/") {
        m = rest.to_string();
    }
    if let Some(rest) = m.strip_prefix("openrouter/") {
        m = rest.to_string();
    }
    if let Some(rest) = m.strip_prefix("openai/") {
        m = rest.to_string();
    }
    m
}

fn calculate_cost(
    model: Option<&str>,
    input: u64,
    output: u64,
    cache_creation: u64,
    cache_read: u64,
) -> f64 {
    let Some(model_name) = model else { return 0.0 };
    let normalized = normalize_model_for_pricing(model_name);
    let pricing = PRICING_INDEX.get().and_then(|idx| idx.find(&normalized));

    let Some(pricing) = pricing else { return 0.0 };
    let input_tokens = input + cache_creation + cache_read;
    let input_cost = (input_tokens as f64 / 1_000_000_f64) * pricing.input_per_million
        + (cache_creation as f64 / 1_000_000_f64) * pricing.cache_create_per_million
        + (cache_read as f64 / 1_000_000_f64) * pricing.cache_read_per_million;
    let output_cost = (output as f64 / 1_000_000_f64) * pricing.output_per_million;
    input_cost + output_cost
}

fn build_daily_report(
    events: &[UsageEvent],
    tz: &Tz,
    locale: &Locale,
    range: &RangeFilter,
    order: Order,
    instances: bool,
    breakdown: bool,
) -> Result<JsonReport> {
    let mut map: BTreeMap<(NaiveDate, Option<String>), RowAccumulator> = BTreeMap::new();

    for ev in events {
        if let Some(project) = &range.project {
            if &ev.project != project {
                continue;
            }
        }
        let date = ev.timestamp.with_timezone(tz).date_naive();
        if !in_range(date, range.since, range.until) {
            continue;
        }
        let key = if instances {
            (date, Some(ev.project.clone()))
        } else {
            (date, None)
        };
        let display_key = if let Some(project) = key.1.as_ref() {
            format!("{} ({})", date.format("%Y-%m-%d"), project)
        } else {
            date.format("%Y-%m-%d").to_string()
        };
        let entry = map
            .entry(key)
            .or_insert_with(|| RowAccumulator::new(display_key));
        entry.add_event(ev);
    }

    let mut rows: Vec<Row> = map.into_iter().map(|(_, acc)| acc.finish()).collect();
    sort_rows(&mut rows, order);
    let totals = calculate_totals(&rows);
    let model_breakdowns = if breakdown {
        aggregate_models_from_rows(&rows)
    } else {
        Vec::new()
    };
    Ok(JsonReport {
        kind: "daily",
        timezone: tz.name().to_string(),
        locale: locale_to_string(locale),
        since: range.since.map(|d| d.format("%Y-%m-%d").to_string()),
        until: range.until.map(|d| d.format("%Y-%m-%d").to_string()),
        rows,
        totals,
        model_breakdowns,
    })
}

fn build_weekly_report(
    events: &[UsageEvent],
    tz: &Tz,
    locale: &Locale,
    range: &RangeFilter,
    order: Order,
    instances: bool,
    start_of_week: WeekStart,
    breakdown: bool,
) -> Result<JsonReport> {
    let mut map: BTreeMap<(NaiveDate, Option<String>), RowAccumulator> = BTreeMap::new();

    for ev in events {
        if let Some(project) = &range.project {
            if &ev.project != project {
                continue;
            }
        }
        let date = ev.timestamp.with_timezone(tz).date_naive();
        if !in_range(date, range.since, range.until) {
            continue;
        }
        let week_start = week_start_for_date(date, start_of_week);
        let key = if instances {
            (week_start, Some(ev.project.clone()))
        } else {
            (week_start, None)
        };
        let display_key = if let Some(project) = key.1.as_ref() {
            format!("{} ({})", week_start.format("%Y-%m-%d"), project)
        } else {
            week_start.format("%Y-%m-%d").to_string()
        };
        let entry = map
            .entry(key)
            .or_insert_with(|| RowAccumulator::new(display_key));
        entry.add_event(ev);
    }

    let mut rows: Vec<Row> = map.into_iter().map(|(_, acc)| acc.finish()).collect();
    sort_rows(&mut rows, order);
    let totals = calculate_totals(&rows);
    let model_breakdowns = if breakdown {
        aggregate_models_from_rows(&rows)
    } else {
        Vec::new()
    };
    Ok(JsonReport {
        kind: "weekly",
        timezone: tz.name().to_string(),
        locale: locale_to_string(locale),
        since: range.since.map(|d| d.format("%Y-%m-%d").to_string()),
        until: range.until.map(|d| d.format("%Y-%m-%d").to_string()),
        rows,
        totals,
        model_breakdowns,
    })
}

fn build_monthly_report(
    events: &[UsageEvent],
    tz: &Tz,
    locale: &Locale,
    range: &RangeFilter,
    order: Order,
    instances: bool,
    breakdown: bool,
) -> Result<JsonReport> {
    let mut map: BTreeMap<((i32, u32), Option<String>), RowAccumulator> = BTreeMap::new();

    for ev in events {
        if let Some(project) = &range.project {
            if &ev.project != project {
                continue;
            }
        }
        let date = ev.timestamp.with_timezone(tz).date_naive();
        if !in_range(date, range.since, range.until) {
            continue;
        }
        let key_base = (date.year(), date.month());
        let key = if instances {
            (key_base, Some(ev.project.clone()))
        } else {
            (key_base, None)
        };
        let display_key = if let Some(project) = key.1.as_ref() {
            format!("{:04}-{:02} ({})", date.year(), date.month(), project)
        } else {
            format!("{:04}-{:02}", date.year(), date.month())
        };
        let entry = map
            .entry(key)
            .or_insert_with(|| RowAccumulator::new(display_key));
        entry.add_event(ev);
    }

    let mut rows: Vec<Row> = map.into_iter().map(|(_, acc)| acc.finish()).collect();
    sort_rows(&mut rows, order);
    let totals = calculate_totals(&rows);
    let model_breakdowns = if breakdown {
        aggregate_models_from_rows(&rows)
    } else {
        Vec::new()
    };
    Ok(JsonReport {
        kind: "monthly",
        timezone: tz.name().to_string(),
        locale: locale_to_string(locale),
        since: range.since.map(|d| d.format("%Y-%m-%d").to_string()),
        until: range.until.map(|d| d.format("%Y-%m-%d").to_string()),
        rows,
        totals,
        model_breakdowns,
    })
}

fn build_session_report(
    events: &[UsageEvent],
    tz: &Tz,
    locale: &Locale,
    range: &RangeFilter,
    breakdown: bool,
) -> Result<SessionReport> {
    let mut map: BTreeMap<String, SessionAccumulator> = BTreeMap::new();

    for ev in events {
        if let Some(project) = &range.project {
            if &ev.project != project {
                continue;
            }
        }
        let date = ev.timestamp.with_timezone(tz).date_naive();
        if !in_range(date, range.since, range.until) {
            continue;
        }
        let entry = map
            .entry(ev.session_id.clone())
            .or_insert_with(|| SessionAccumulator::new(ev.session_id.clone(), ev.project.clone()));
        entry.add_event(ev);
    }

    let mut rows: Vec<SessionRow> = map.into_iter().map(|(_, acc)| acc.finish()).collect();
    rows.sort_by(|a, b| b.last_activity.cmp(&a.last_activity));
    let totals = calculate_session_totals(&rows);
    let model_breakdowns = if breakdown {
        aggregate_models_from_session_rows(&rows)
    } else {
        Vec::new()
    };

    Ok(SessionReport {
        kind: "sessions",
        timezone: tz.name().to_string(),
        locale: locale_to_string(locale),
        since: range.since.map(|d| d.format("%Y-%m-%d").to_string()),
        until: range.until.map(|d| d.format("%Y-%m-%d").to_string()),
        rows,
        totals,
        model_breakdowns,
    })
}

fn build_blocks_report(
    events: &[UsageEvent],
    tz: &Tz,
    locale: &Locale,
    range: &RangeFilter,
    token_limit: u64,
    recent_days: u32,
    session_length_hours: u32,
    breakdown: bool,
) -> Result<BlocksReport> {
    let mut map: BTreeMap<DateTime<Tz>, RowAccumulator> = BTreeMap::new();

    // determine cutoff
    let latest_date = events
        .iter()
        .map(|e| e.timestamp.with_timezone(tz).date_naive())
        .max();
    let cutoff = latest_date.map(|d| d - ChronoDuration::days(recent_days as i64));

    for ev in events {
        if let Some(project) = &range.project {
            if &ev.project != project {
                continue;
            }
        }
        let local_dt = ev.timestamp.with_timezone(tz);
        let date = local_dt.date_naive();
        if let Some(cut) = cutoff {
            if date < cut {
                continue;
            }
        }
        if !in_range(date, range.since, range.until) {
            continue;
        }
        let block_hours = session_length_hours.max(1);
        let block_start_hour = (local_dt.hour() / block_hours) * block_hours;
        let block_start = local_dt
            .with_hour(block_start_hour)
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_nanosecond(0))
            .expect("valid datetime");
        let entry = map
            .entry(block_start)
            .or_insert_with(|| RowAccumulator::new(format_block_range(block_start, block_hours)));
        entry.add_event(ev);
    }

    let mut rows: Vec<BlockRow> = map
        .into_iter()
        .map(|(start, acc)| {
            let row = acc.finish();
            let hours = session_length_hours.max(1);
            let end = start + ChronoDuration::hours(hours as i64);
            let percent = if token_limit > 0 {
                Some((row.total_tokens as f64 / token_limit as f64) * 100.0)
            } else {
                None
            };
            BlockRow {
                block_start: start.to_rfc3339(),
                block_end: end.to_rfc3339(),
                input_tokens: row.input_tokens,
                output_tokens: row.output_tokens,
                cache_creation_tokens: row.cache_creation_tokens,
                cache_read_tokens: row.cache_read_tokens,
                total_tokens: row.total_tokens,
                cost_usd: row.cost_usd,
                percent_of_limit: percent,
                models: row.models,
                projects: row.projects,
                model_breakdowns: row.model_breakdowns,
            }
        })
        .collect();
    rows.sort_by(|a, b| a.block_start.cmp(&b.block_start));

    let totals = calculate_block_totals(&rows);
    let model_breakdowns = if breakdown {
        aggregate_models_from_blocks(&rows)
    } else {
        Vec::new()
    };

    Ok(BlocksReport {
        kind: "blocks",
        timezone: tz.name().to_string(),
        locale: locale_to_string(locale),
        recent_days,
        token_limit: Some(token_limit),
        rows,
        totals,
        model_breakdowns,
    })
}

fn build_statusline_report(
    events: &[UsageEvent],
    tz: &Tz,
    range: &RangeFilter,
) -> Result<StatuslineReport> {
    let mut map: BTreeMap<NaiveDate, Totals> = BTreeMap::new();
    for ev in events {
        if let Some(project) = &range.project {
            if &ev.project != project {
                continue;
            }
        }
        let date = ev.timestamp.with_timezone(tz).date_naive();
        if !in_range(date, range.since, range.until) {
            continue;
        }
        let entry = map.entry(date).or_insert(Totals {
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_tokens: 0,
            cache_read_tokens: 0,
            total_tokens: 0,
            cost_usd: 0.0,
        });
        entry.input_tokens += ev.input_tokens;
        entry.output_tokens += ev.output_tokens;
        entry.cache_creation_tokens += ev.cache_creation_tokens;
        entry.cache_read_tokens += ev.cache_read_tokens;
        entry.total_tokens +=
            ev.input_tokens + ev.output_tokens + ev.cache_creation_tokens + ev.cache_read_tokens;
        entry.cost_usd += ev.cost_usd;
    }
    let (last_date, totals) = if let Some((date, totals)) = map.into_iter().max_by_key(|(d, _)| *d)
    {
        (Some(date.format("%Y-%m-%d").to_string()), totals)
    } else {
        (
            None,
            Totals {
                input_tokens: 0,
                output_tokens: 0,
                cache_creation_tokens: 0,
                cache_read_tokens: 0,
                total_tokens: 0,
                cost_usd: 0.0,
            },
        )
    };

    Ok(StatuslineReport {
        kind: "statusline",
        timezone: tz.name().to_string(),
        locale: "en".to_string(),
        last_date,
        totals,
    })
}

fn week_start_for_date(date: NaiveDate, start: WeekStart) -> NaiveDate {
    let weekday = date.weekday().num_days_from_sunday() as i64;
    let start_day = match start {
        WeekStart::Sunday => 0,
        WeekStart::Monday => 1,
    } as i64;
    let diff = (7 + weekday - start_day) % 7;
    date - ChronoDuration::days(diff)
}

fn parse_date_opt(raw: Option<&str>) -> Result<Option<NaiveDate>> {
    match raw {
        None => Ok(None),
        Some(s) => {
            let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .with_context(|| format!("Could not parse date '{}', expected YYYY-MM-DD", s))?;
            Ok(Some(d))
        }
    }
}

fn in_range(date: NaiveDate, since: Option<NaiveDate>, until: Option<NaiveDate>) -> bool {
    if let Some(s) = since {
        if date < s {
            return false;
        }
    }
    if let Some(u) = until {
        if date > u {
            return false;
        }
    }
    true
}

#[derive(Default)]
struct RowAccumulator {
    key: String,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_tokens: u64,
    cache_read_tokens: u64,
    cost_usd: f64,
    models: BTreeSet<String>,
    projects: BTreeSet<String>,
    per_model: BTreeMap<String, ModelAccumulator>,
}

impl RowAccumulator {
    fn new(key: String) -> Self {
        RowAccumulator {
            key,
            ..Default::default()
        }
    }

    fn add_event(&mut self, ev: &UsageEvent) {
        self.input_tokens += ev.input_tokens;
        self.output_tokens += ev.output_tokens;
        self.cache_creation_tokens += ev.cache_creation_tokens;
        self.cache_read_tokens += ev.cache_read_tokens;
        self.cost_usd += ev.cost_usd;
        if let Some(model) = &ev.model {
            self.models.insert(model.clone());
            self.per_model
                .entry(model.clone())
                .or_insert_with(ModelAccumulator::default)
                .add(ev);
        }
        self.projects.insert(ev.project.clone());
    }

    fn finish(self) -> Row {
        let total_tokens = self.input_tokens
            + self.output_tokens
            + self.cache_creation_tokens
            + self.cache_read_tokens;
        let model_breakdowns = self
            .per_model
            .into_iter()
            .map(|(model, acc)| acc.finish(model))
            .collect::<Vec<_>>();
        Row {
            key: self.key,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_creation_tokens: self.cache_creation_tokens,
            cache_read_tokens: self.cache_read_tokens,
            total_tokens,
            cost_usd: self.cost_usd,
            models: self.models,
            projects: self.projects,
            model_breakdowns,
        }
    }
}

#[derive(Default)]
struct ModelAccumulator {
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_tokens: u64,
    cache_read_tokens: u64,
    cost_usd: f64,
}

impl ModelAccumulator {
    fn add(&mut self, ev: &UsageEvent) {
        self.input_tokens += ev.input_tokens;
        self.output_tokens += ev.output_tokens;
        self.cache_creation_tokens += ev.cache_creation_tokens;
        self.cache_read_tokens += ev.cache_read_tokens;
        self.cost_usd += ev.cost_usd;
    }

    fn finish(self, model: String) -> ModelBreakdown {
        ModelBreakdown {
            model,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_creation_tokens: self.cache_creation_tokens,
            cache_read_tokens: self.cache_read_tokens,
            total_tokens: self.input_tokens
                + self.output_tokens
                + self.cache_creation_tokens
                + self.cache_read_tokens,
            cost_usd: self.cost_usd,
        }
    }
}

struct SessionAccumulator {
    session_id: String,
    project: String,
    last_activity: DateTime<Utc>,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_tokens: u64,
    cache_read_tokens: u64,
    cost_usd: f64,
    models: BTreeSet<String>,
    per_model: BTreeMap<String, ModelAccumulator>,
}

impl SessionAccumulator {
    fn new(session_id: String, project: String) -> Self {
        SessionAccumulator {
            session_id,
            project,
            last_activity: DateTime::<Utc>::MIN_UTC,
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_tokens: 0,
            cache_read_tokens: 0,
            cost_usd: 0.0,
            models: BTreeSet::new(),
            per_model: BTreeMap::new(),
        }
    }

    fn add_event(&mut self, ev: &UsageEvent) {
        self.input_tokens += ev.input_tokens;
        self.output_tokens += ev.output_tokens;
        self.cache_creation_tokens += ev.cache_creation_tokens;
        self.cache_read_tokens += ev.cache_read_tokens;
        self.cost_usd += ev.cost_usd;
        if ev.timestamp > self.last_activity {
            self.last_activity = ev.timestamp;
        }
        if let Some(model) = &ev.model {
            self.models.insert(model.clone());
            self.per_model
                .entry(model.clone())
                .or_insert_with(ModelAccumulator::default)
                .add(ev);
        }
        // ensure project reflects latest known (in case of path changes)
        self.project = ev.project.clone();
    }

    fn finish(self) -> SessionRow {
        let total_tokens = self.input_tokens
            + self.output_tokens
            + self.cache_creation_tokens
            + self.cache_read_tokens;
        let model_breakdowns = self
            .per_model
            .into_iter()
            .map(|(model, acc)| acc.finish(model))
            .collect::<Vec<_>>();
        SessionRow {
            session_id: self.session_id,
            project: self.project,
            last_activity: self.last_activity.to_rfc3339(),
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_creation_tokens: self.cache_creation_tokens,
            cache_read_tokens: self.cache_read_tokens,
            total_tokens,
            cost_usd: self.cost_usd,
            models: self.models,
            model_breakdowns,
        }
    }
}

fn calculate_totals(rows: &[Row]) -> Totals {
    let mut totals = Totals {
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_tokens: 0,
        cache_read_tokens: 0,
        total_tokens: 0,
        cost_usd: 0.0,
    };
    for row in rows {
        totals.input_tokens += row.input_tokens;
        totals.output_tokens += row.output_tokens;
        totals.cache_creation_tokens += row.cache_creation_tokens;
        totals.cache_read_tokens += row.cache_read_tokens;
        totals.total_tokens += row.total_tokens;
        totals.cost_usd += row.cost_usd;
    }
    totals
}

fn calculate_session_totals(rows: &[SessionRow]) -> Totals {
    let mut totals = Totals {
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_tokens: 0,
        cache_read_tokens: 0,
        total_tokens: 0,
        cost_usd: 0.0,
    };
    for row in rows {
        totals.input_tokens += row.input_tokens;
        totals.output_tokens += row.output_tokens;
        totals.cache_creation_tokens += row.cache_creation_tokens;
        totals.cache_read_tokens += row.cache_read_tokens;
        totals.total_tokens += row.total_tokens;
        totals.cost_usd += row.cost_usd;
    }
    totals
}

fn calculate_block_totals(rows: &[BlockRow]) -> Totals {
    let mut totals = Totals {
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_tokens: 0,
        cache_read_tokens: 0,
        total_tokens: 0,
        cost_usd: 0.0,
    };
    for row in rows {
        totals.input_tokens += row.input_tokens;
        totals.output_tokens += row.output_tokens;
        totals.cache_creation_tokens += row.cache_creation_tokens;
        totals.cache_read_tokens += row.cache_read_tokens;
        totals.total_tokens += row.total_tokens;
        totals.cost_usd += row.cost_usd;
    }
    totals
}

fn output_rows(
    report: JsonReport,
    json: bool,
    compact: bool,
    breakdown: bool,
    locale: &Locale,
    title: &str,
) {
    if json {
        serde_json::to_writer_pretty(std::io::stdout(), &report).expect("write json");
        println!();
        return;
    }
    print_rows_table(&report.rows, &report.totals, compact, locale, title);
    if breakdown && !report.model_breakdowns.is_empty() {
        print_model_breakdowns(&report.model_breakdowns, locale);
    }
}

fn output_sessions(report: SessionReport, json: bool, locale: &Locale) {
    if json {
        serde_json::to_writer_pretty(std::io::stdout(), &report).expect("write json");
        println!();
        return;
    }
    print_sessions_table(&report.rows, &report.totals, locale);
    if !report.model_breakdowns.is_empty() {
        print_model_breakdowns(&report.model_breakdowns, locale);
    }
}

fn output_blocks(
    report: BlocksReport,
    json: bool,
    compact: bool,
    breakdown: bool,
    locale: &Locale,
) {
    if json {
        serde_json::to_writer_pretty(std::io::stdout(), &report).expect("write json");
        println!();
        return;
    }
    print_blocks_table(
        &report.rows,
        &report.totals,
        compact,
        report.token_limit,
        locale,
    );
    if breakdown && !report.model_breakdowns.is_empty() {
        print_model_breakdowns(&report.model_breakdowns, locale);
    }
}

fn run_blocks_live_cli(
    live_source: &mut LiveEventSource,
    tz: &Tz,
    locale: &Locale,
    range: &RangeFilter,
    token_limit: u64,
    recent_days: u32,
    session_length_hours: u32,
    opts: CommonOptions,
    refresh_seconds: u64,
    rx: Receiver<()>,
) -> Result<()> {
    loop {
        live_source.refresh()?;
        let report = build_blocks_report(
            live_source.events(),
            tz,
            locale,
            range,
            token_limit,
            recent_days,
            session_length_hours,
            opts.breakdown,
        )?;
        print!("\x1B[2J\x1B[H");
        output_blocks(report, opts.json, opts.compact, opts.breakdown, locale);
        match rx.recv_timeout(Duration::from_secs(refresh_seconds)) {
            Ok(_) | Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(())
}

fn run_blocks_live_tui(
    live_source: &mut LiveEventSource,
    tz: &Tz,
    locale: &Locale,
    range: &RangeFilter,
    token_limit: u64,
    recent_days: u32,
    session_length_hours: u32,
    breakdown: bool,
    refresh_seconds: u64,
    rx: Receiver<()>,
) -> Result<()> {
    let mut stdout = std::io::stdout();
    enable_raw_mode().context("enable raw mode")?;
    stdout
        .execute(EnterAlternateScreen)
        .context("enter alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("init terminal")?;
    terminal.clear().ok();

    let res = (|| -> Result<()> {
        loop {
            live_source.refresh()?;
            let report = build_blocks_report(
                live_source.events(),
                tz,
                locale,
                range,
                token_limit,
                recent_days,
                session_length_hours,
                breakdown,
            )?;
            terminal.draw(|f| render_blocks_tui(f, &report.rows, &report.totals, locale))?;

            while event::poll(Duration::from_millis(100))? {
                if let Event::Key(k) = event::read()? {
                    if k.kind == KeyEventKind::Press
                        && matches!(k.code, KeyCode::Char('q') | KeyCode::Esc)
                    {
                        return Ok(());
                    }
                }
            }

            match rx.recv_timeout(Duration::from_secs(refresh_seconds)) {
                Ok(_) | Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        Ok(())
    })();

    disable_raw_mode().ok();
    let _ = terminal.backend_mut().execute(LeaveAlternateScreen);
    let _ = terminal.show_cursor();
    res
}

fn render_blocks_tui(
    f: &mut ratatui::Frame<'_>,
    rows: &[BlockRow],
    totals: &Totals,
    locale: &Locale,
) {
    let chunks = Layout::default()
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(f.size());

    let totals_text = format!(
        "Input {} | Output {} | Cache {} | Total {} | Cost ${:.4} (q to quit)",
        format_tokens(totals.input_tokens, locale),
        format_tokens(totals.output_tokens, locale),
        format_tokens(
            totals.cache_creation_tokens + totals.cache_read_tokens,
            locale
        ),
        format_tokens(totals.total_tokens, locale),
        totals.cost_usd
    );
    let totals_block = TuiBlock::default()
        .borders(Borders::ALL)
        .title(Span::raw("Totals"));
    let totals_table = TuiTable::new(
        vec![TuiRow::new(vec![Cell::from(totals_text)])],
        [Constraint::Percentage(100)],
    )
    .block(totals_block);
    f.render_widget(totals_table, chunks[0]);

    let header = ["Block", "Total", "Cost", "Models"];
    let body: Vec<TuiRow> = rows
        .iter()
        .take(12)
        .map(|r| {
            TuiRow::new(vec![
                Cell::from(r.block_start.clone()),
                Cell::from(format_tokens(r.total_tokens, locale)),
                Cell::from(format_cost(r.cost_usd)),
                Cell::from(join_set(&r.models)),
            ])
        })
        .collect();

    let table = TuiTable::new(
        body,
        [
            Constraint::Percentage(40),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Percentage(30),
        ],
    )
    .header(
        TuiRow::new(header.iter().map(|h| Cell::from(*h)))
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        TuiBlock::default()
            .borders(Borders::ALL)
            .title(Span::raw("Blocks")),
    );
    f.render_widget(table, chunks[1]);
}

fn output_statusline(report: StatuslineReport, json: bool) {
    if json {
        serde_json::to_writer_pretty(std::io::stdout(), &report).expect("write json");
        println!();
        return;
    }
    if let Some(day) = report.last_date {
        println!(
            "{} {} {} {} {} {} {} {} {} {} {} {}",
            day.cyan().bold(),
            "|".dimmed(),
            "in".dimmed(),
            format_tokens_compact(report.totals.input_tokens),
            "|".dimmed(),
            "out".dimmed(),
            format_tokens_compact(report.totals.output_tokens),
            "|".dimmed(),
            "total".dimmed(),
            format_tokens_compact(report.totals.total_tokens),
            "|".dimmed(),
            format_cost_compact(report.totals.cost_usd).yellow().bold(),
        );
    } else {
        println!("No data");
    }
}

fn print_rows_table(rows: &[Row], totals: &Totals, compact: bool, locale: &Locale, title: &str) {
    if rows.is_empty() {
        println!("No matching usage for {}", title.to_lowercase());
        return;
    }

    let term_width = get_terminal_width();
    let use_compact = compact || term_width < 140;
    let max_projects = if term_width < 120 { 1 } else if term_width < 160 { 2 } else { 3 };

    println!("{}", format!("{title} usage").bold());

    if use_compact {
        let mut table = SimpleTable::new(vec!["Period", "In", "Out", "Total", "Cost", "Projects", "Models"])
            .header_style(|s| s.cyan().bold());
        for row in rows {
            table.add_row(vec![
                row.key.clone(),
                format_tokens_compact(row.input_tokens),
                format_tokens_compact(row.output_tokens),
                format_tokens_compact(row.total_tokens),
                format_cost_compact(row.cost_usd),
                format_projects(&row.projects, max_projects),
                format_models(&row.models),
            ]);
        }
        table.set_footer(vec![
            "Total".yellow().bold().to_string(),
            format_tokens_compact(totals.input_tokens).yellow().to_string(),
            format_tokens_compact(totals.output_tokens).yellow().to_string(),
            format_tokens_compact(totals.total_tokens).yellow().to_string(),
            format_cost_compact(totals.cost_usd).yellow().to_string(),
            String::new(),
            String::new(),
        ]);
        table.print();
    } else {
        let mut table = SimpleTable::new(vec!["Period", "Input", "Output", "C/W", "C/R", "Total", "Cost", "Projects", "Models"])
            .header_style(|s| s.cyan().bold());
        for row in rows {
            table.add_row(vec![
                row.key.clone(),
                format_tokens(row.input_tokens, locale),
                format_tokens(row.output_tokens, locale),
                format_tokens(row.cache_creation_tokens, locale),
                format_tokens(row.cache_read_tokens, locale),
                format_tokens(row.total_tokens, locale),
                format_cost(row.cost_usd),
                format_projects(&row.projects, max_projects),
                format_models(&row.models),
            ]);
        }
        table.set_footer(vec![
            "Total".yellow().bold().to_string(),
            format_tokens(totals.input_tokens, locale).yellow().to_string(),
            format_tokens(totals.output_tokens, locale).yellow().to_string(),
            format_tokens(totals.cache_creation_tokens, locale).yellow().to_string(),
            format_tokens(totals.cache_read_tokens, locale).yellow().to_string(),
            format_tokens(totals.total_tokens, locale).yellow().to_string(),
            format_cost(totals.cost_usd).yellow().to_string(),
            String::new(),
            String::new(),
        ]);
        table.print();
    }
}

fn print_sessions_table(rows: &[SessionRow], totals: &Totals, locale: &Locale) {
    if rows.is_empty() {
        println!("No matching session usage.");
        return;
    }

    let term_width = get_terminal_width();
    let use_compact = term_width < 140;

    println!("{}", "Session usage".bold());

    if use_compact {
        let mut table = SimpleTable::new(vec!["Session", "Project", "In", "Out", "Total", "Cost", "Models"])
            .header_style(|s| s.cyan().bold());
        for row in rows {
            table.add_row(vec![
                truncate_str(&row.session_id, 12),
                shorten_project_name(&row.project),
                format_tokens_compact(row.input_tokens),
                format_tokens_compact(row.output_tokens),
                format_tokens_compact(row.total_tokens),
                format_cost_compact(row.cost_usd),
                format_models(&row.models),
            ]);
        }
        table.set_footer(vec![
            "TOTAL".yellow().bold().to_string(),
            String::new(),
            format_tokens_compact(totals.input_tokens).yellow().to_string(),
            format_tokens_compact(totals.output_tokens).yellow().to_string(),
            format_tokens_compact(totals.total_tokens).yellow().to_string(),
            format_cost_compact(totals.cost_usd).yellow().to_string(),
            String::new(),
        ]);
        table.print();
    } else {
        let mut table = SimpleTable::new(vec!["Session", "Project", "Last Activity", "Input", "Output", "C/W", "C/R", "Total", "Cost", "Models"])
            .header_style(|s| s.cyan().bold());
        for row in rows {
            table.add_row(vec![
                truncate_str(&row.session_id, 16),
                shorten_project_name(&row.project),
                row.last_activity.clone(),
                format_tokens(row.input_tokens, locale),
                format_tokens(row.output_tokens, locale),
                format_tokens(row.cache_creation_tokens, locale),
                format_tokens(row.cache_read_tokens, locale),
                format_tokens(row.total_tokens, locale),
                format_cost(row.cost_usd),
                format_models(&row.models),
            ]);
        }
        table.set_footer(vec![
            "TOTAL".yellow().bold().to_string(),
            String::new(),
            String::new(),
            format_tokens(totals.input_tokens, locale).yellow().to_string(),
            format_tokens(totals.output_tokens, locale).yellow().to_string(),
            format_tokens(totals.cache_creation_tokens, locale).yellow().to_string(),
            format_tokens(totals.cache_read_tokens, locale).yellow().to_string(),
            format_tokens(totals.total_tokens, locale).yellow().to_string(),
            format_cost(totals.cost_usd).yellow().to_string(),
            String::new(),
        ]);
        table.print();
    }
}

fn print_blocks_table(
    rows: &[BlockRow],
    totals: &Totals,
    compact: bool,
    token_limit: Option<u64>,
    locale: &Locale,
) {
    if rows.is_empty() {
        println!("No matching block usage.");
        return;
    }

    let term_width = get_terminal_width();
    let use_compact = compact || term_width < 120;

    println!("{}", "Blocks usage".bold());

    if use_compact {
        let mut table = SimpleTable::new(vec!["Block", "Total", "%Lim", "Cost", "Models"])
            .header_style(|s| s.cyan().bold());
        for row in rows {
            table.add_row(vec![
                row.block_start.clone(),
                format_tokens_compact(row.total_tokens),
                row.percent_of_limit.map(|p| format!("{:.0}%", p)).unwrap_or_else(|| "-".into()),
                format_cost_compact(row.cost_usd),
                format_models(&row.models),
            ]);
        }
        table.set_footer(vec![
            "TOTAL".yellow().bold().to_string(),
            format_tokens_compact(totals.total_tokens).yellow().to_string(),
            String::new(),
            format_cost_compact(totals.cost_usd).yellow().to_string(),
            String::new(),
        ]);
        table.print();
    } else {
        let mut table = SimpleTable::new(vec!["Block Start", "Block End", "Input", "Output", "C/W", "C/R", "Total", "%Lim", "Cost", "Models"])
            .header_style(|s| s.cyan().bold());
        for row in rows {
            table.add_row(vec![
                row.block_start.clone(),
                row.block_end.clone(),
                format_tokens(row.input_tokens, locale),
                format_tokens(row.output_tokens, locale),
                format_tokens(row.cache_creation_tokens, locale),
                format_tokens(row.cache_read_tokens, locale),
                format_tokens(row.total_tokens, locale),
                row.percent_of_limit.map(|p| format!("{:.1}%", p)).unwrap_or_else(|| "-".into()),
                format_cost(row.cost_usd),
                format_models(&row.models),
            ]);
        }
        let total_pct = token_limit
            .map(|l| format!("{:.1}%", (totals.total_tokens as f64 / l as f64) * 100.0))
            .unwrap_or_else(|| "-".into());
        table.set_footer(vec![
            "TOTAL".yellow().bold().to_string(),
            String::new(),
            format_tokens(totals.input_tokens, locale).yellow().to_string(),
            format_tokens(totals.output_tokens, locale).yellow().to_string(),
            format_tokens(totals.cache_creation_tokens, locale).yellow().to_string(),
            format_tokens(totals.cache_read_tokens, locale).yellow().to_string(),
            format_tokens(totals.total_tokens, locale).yellow().to_string(),
            total_pct.yellow().to_string(),
            format_cost(totals.cost_usd).yellow().to_string(),
            String::new(),
        ]);
        table.print();
    }
}

fn print_model_breakdowns(models: &[ModelBreakdown], locale: &Locale) {
    if models.is_empty() {
        return;
    }

    let mut sorted = models.to_vec();
    sorted.sort_by(|a, b| {
        b.cost_usd
            .partial_cmp(&a.cost_usd)
            .unwrap_or(Ordering::Equal)
    });

    println!("\n{}", "Model breakdowns".bold());
    let mut table = SimpleTable::new(vec!["Model", "Input", "Output", "C/W", "C/R", "Total", "Cost"])
        .header_style(|s| s.cyan().bold());
    for row in sorted {
        table.add_row(vec![
            format_model_name(&row.model).dimmed().to_string(),
            format_tokens(row.input_tokens, locale).dimmed().to_string(),
            format_tokens(row.output_tokens, locale).dimmed().to_string(),
            format_tokens(row.cache_creation_tokens, locale).dimmed().to_string(),
            format_tokens(row.cache_read_tokens, locale).dimmed().to_string(),
            format_tokens(row.total_tokens, locale).dimmed().to_string(),
            format_cost(row.cost_usd).dimmed().to_string(),
        ]);
    }
    table.print();
}

fn format_tokens(value: u64, locale: &Locale) -> String {
    value.to_formatted_string(locale)
}

fn format_tokens_compact(value: u64) -> String {
    if value >= 1_000_000_000 {
        format!("{:.1}B", value as f64 / 1_000_000_000.0)
    } else if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn format_cost(value: f64) -> String {
    let int_part = value.trunc() as u64;
    let frac_part = ((value.fract() * 100.0).round() as u64) % 100;
    format!("${}.{:02}", int_part.to_formatted_string(&Locale::en), frac_part)
}

fn format_cost_compact(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("${:.1}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("${:.0}K", value / 1_000.0)
    } else {
        format!("${:.0}", value)
    }
}

fn join_set(set: &BTreeSet<String>) -> String {
    if set.is_empty() {
        return "-".into();
    }
    set.iter().cloned().collect::<Vec<_>>().join(", ")
}

/// Format model names for display (e.g., "claude-sonnet-4-20250514" -> "sonnet-4")
fn format_model_name(model: &str) -> String {
    // Match patterns like "claude-sonnet-4-20250514" or "claude-opus-4-5-20250929"
    if let Some(rest) = model.strip_prefix("claude-") {
        // Find the date suffix (8 digits at the end)
        if let Some(idx) = rest.rfind('-') {
            let (name_part, date_part) = rest.split_at(idx);
            if date_part.len() == 9 && date_part[1..].chars().all(|c| c.is_ascii_digit()) {
                return name_part.to_string();
            }
        }
    }
    model.to_string()
}

fn format_models(set: &BTreeSet<String>) -> String {
    let models: Vec<_> = set.iter()
        .filter(|m| *m != "<synthetic>")
        .map(|m| format_model_name(m))
        .collect();
    if models.is_empty() {
        return "-".into();
    }
    models.join(", ")
}

/// Get terminal width, defaulting to 120 if unavailable
fn get_terminal_width() -> u16 {
    terminal_size().map(|(w, _)| w).unwrap_or(120)
}

/// Shorten a project path by removing common prefixes
fn shorten_project_name(name: &str) -> String {
    // Remove common path prefixes like "-Users-joel-Work-"
    let name = name.strip_prefix("-Users-").unwrap_or(name);
    // Find the last component after removing user path
    if let Some(idx) = name.find('-') {
        // Skip username, get the rest
        let rest = &name[idx + 1..];
        if let Some(idx2) = rest.find('-') {
            // Skip "Work" or similar, get actual project
            return rest[idx2 + 1..].to_string();
        }
        return rest.to_string();
    }
    name.to_string()
}

/// Truncate a string to max length, adding "..." if needed
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else if max_len > 3 {
        format!("{}...", &s[..max_len - 3])
    } else {
        s[..max_len].to_string()
    }
}

/// Format projects for display, limiting count and shortening names
fn format_projects(set: &BTreeSet<String>, max_display: usize) -> String {
    if set.is_empty() {
        return "-".into();
    }
    let shortened: Vec<String> = set.iter().map(|p| shorten_project_name(p)).collect();
    if shortened.len() <= max_display {
        shortened.join(", ")
    } else {
        let displayed: Vec<_> = shortened.iter().take(max_display).cloned().collect();
        format!("{}, +{} more", displayed.join(", "), shortened.len() - max_display)
    }
}

/// Simple table with box-drawing characters
struct SimpleTable {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    header_style: Option<fn(&str) -> colored::ColoredString>,
    footer: Option<Vec<String>>,
}

impl SimpleTable {
    fn new(headers: Vec<&str>) -> Self {
        Self {
            headers: headers.into_iter().map(String::from).collect(),
            rows: Vec::new(),
            header_style: None,
            footer: None,
        }
    }

    fn header_style(mut self, f: fn(&str) -> colored::ColoredString) -> Self {
        self.header_style = Some(f);
        self
    }

    fn add_row(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }

    fn set_footer(&mut self, footer: Vec<String>) {
        self.footer = Some(footer);
    }

    fn print(&self) {
        let num_cols = self.headers.len();
        let mut widths: Vec<usize> = self.headers.iter().map(|h| h.chars().count()).collect();

        // Compute max widths from rows (strip ANSI for proper width calc)
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if i < num_cols {
                    let stripped = strip_ansi(cell);
                    widths[i] = widths[i].max(stripped.chars().count());
                }
            }
        }
        if let Some(ref footer) = self.footer {
            for (i, cell) in footer.iter().enumerate() {
                if i < num_cols {
                    let stripped = strip_ansi(cell);
                    widths[i] = widths[i].max(stripped.chars().count());
                }
            }
        }

        // Top border: ┌─────┬─────┐
        print!("┌");
        for (i, w) in widths.iter().enumerate() {
            print!("{}", "─".repeat(*w + 2));
            if i < num_cols - 1 { print!("┬"); }
        }
        println!("┐");

        // Header row
        print!("│");
        for (i, header) in self.headers.iter().enumerate() {
            let padded = format!("{:width$}", header, width = widths[i]);
            if let Some(style) = self.header_style {
                print!(" {} │", style(&padded));
            } else {
                print!(" {} │", padded);
            }
        }
        println!();

        // Header separator: ├─────┼─────┤
        print!("├");
        for (i, w) in widths.iter().enumerate() {
            print!("{}", "─".repeat(*w + 2));
            if i < num_cols - 1 { print!("┼"); }
        }
        println!("┤");

        // Data rows
        for row in &self.rows {
            print!("│");
            for (i, cell) in row.iter().enumerate() {
                if i < num_cols {
                    let stripped = strip_ansi(cell);
                    let pad = widths[i].saturating_sub(stripped.chars().count());
                    print!(" {}{} │", cell, " ".repeat(pad));
                }
            }
            println!();
        }

        // Footer separator if present
        if let Some(ref footer) = self.footer {
            print!("├");
            for (i, w) in widths.iter().enumerate() {
                print!("{}", "─".repeat(*w + 2));
                if i < num_cols - 1 { print!("┼"); }
            }
            println!("┤");

            print!("│");
            for (i, cell) in footer.iter().enumerate() {
                if i < num_cols {
                    let stripped = strip_ansi(cell);
                    let pad = widths[i].saturating_sub(stripped.chars().count());
                    print!(" {}{} │", cell, " ".repeat(pad));
                }
            }
            println!();
        }

        // Bottom border: └─────┴─────┘
        print!("└");
        for (i, w) in widths.iter().enumerate() {
            print!("{}", "─".repeat(*w + 2));
            if i < num_cols - 1 { print!("┴"); }
        }
        println!("┘");
    }
}

fn strip_ansi(s: &str) -> String {
    let mut result = String::new();
    let mut in_escape = false;
    for c in s.chars() {
        if c == '\x1b' {
            in_escape = true;
        } else if in_escape {
            if c == 'm' {
                in_escape = false;
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn extract_project_name(path: &Path) -> String {
    let components = path
        .components()
        .map(|c| c.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    if let Some(idx) = components.iter().position(|c| c == PROJECTS_DIR) {
        if let Some(name) = components.get(idx + 1) {
            return name.clone();
        }
    }
    "unknown".into()
}

fn display_paths(paths: &[PathBuf]) -> String {
    let parts: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
    parts.join(", ")
}

fn sort_rows(rows: &mut [Row], order: Order) {
    rows.sort_by(|a, b| {
        let ord = a.key.cmp(&b.key);
        match order {
            Order::Asc => ord,
            Order::Desc => ord.reverse(),
        }
    });
}

fn aggregate_models_from_rows(rows: &[Row]) -> Vec<ModelBreakdown> {
    let mut map: BTreeMap<String, ModelAccumulator> = BTreeMap::new();
    for row in rows {
        for mb in &row.model_breakdowns {
            map.entry(mb.model.clone())
                .or_insert_with(ModelAccumulator::default)
                .add_from_breakdown(mb);
        }
    }
    map.into_iter()
        .map(|(model, acc)| acc.finish(model))
        .collect()
}

fn aggregate_models_from_session_rows(rows: &[SessionRow]) -> Vec<ModelBreakdown> {
    let mut map: BTreeMap<String, ModelAccumulator> = BTreeMap::new();
    for row in rows {
        for mb in &row.model_breakdowns {
            map.entry(mb.model.clone())
                .or_insert_with(ModelAccumulator::default)
                .add_from_breakdown(mb);
        }
    }
    map.into_iter()
        .map(|(model, acc)| acc.finish(model))
        .collect()
}

fn aggregate_models_from_blocks(rows: &[BlockRow]) -> Vec<ModelBreakdown> {
    let mut map: BTreeMap<String, ModelAccumulator> = BTreeMap::new();
    for row in rows {
        for mb in &row.model_breakdowns {
            map.entry(mb.model.clone())
                .or_insert_with(ModelAccumulator::default)
                .add_from_breakdown(mb);
        }
    }
    map.into_iter()
        .map(|(model, acc)| acc.finish(model))
        .collect()
}

impl ModelAccumulator {
    fn add_from_breakdown(&mut self, mb: &ModelBreakdown) {
        self.input_tokens += mb.input_tokens;
        self.output_tokens += mb.output_tokens;
        self.cache_creation_tokens += mb.cache_creation_tokens;
        self.cache_read_tokens += mb.cache_read_tokens;
        self.cost_usd += mb.cost_usd;
    }
}

fn locale_to_string(locale: &Locale) -> String {
    match locale {
        Locale::en => "en".to_string(),
        Locale::fr => "fr".to_string(),
        Locale::de => "de".to_string(),
        Locale::es => "es".to_string(),
        Locale::it => "it".to_string(),
        Locale::ja => "ja".to_string(),
        _ => "en".to_string(),
    }
}

fn format_block_range(start: DateTime<Tz>, hours: u32) -> String {
    let end = start + ChronoDuration::hours(hours as i64);
    format!(
        "{} - {}",
        start.format("%Y-%m-%d %H:%M"),
        end.format("%Y-%m-%d %H:%M")
    )
}
