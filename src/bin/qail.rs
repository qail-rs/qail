//! qail — The QAIL CLI
//!
//! A blazing fast CLI for executing QAIL queries.
//!
//! # Usage
//!
//! ```bash
//! # Execute a query
//! qail "get::users•@*[active=true][lim=10]"
//!
//! # Dry run (show SQL only)
//! qail "get::users•@*" --dry-run
//!
//! # With parameters
//! qail "get::users•@*[id=$1]" --bind 42
//! ```

use clap::{Parser, Subcommand};
use colored::*;
use qail::prelude::*;

#[derive(Parser)]
#[command(name = "qail")]
#[command(author = "QAIL Contributors")]
#[command(version = "0.1.0")]
#[command(about = "🪝 The Horizontal Query Language CLI", long_about = None)]
#[command(after_help = "EXAMPLES:
    qail \"get::users•@*[active=true]\"
    qail \"get::orders•@id@total[user_id=$1][lim=10]\" --bind 42
    qail \"set::users•[verified=true][id=$1]\" --bind 7 --dry-run")]
struct Cli {
    /// The QAIL query to execute
    query: Option<String>,

    /// Don't execute, just show the generated SQL
    #[arg(short, long)]
    dry_run: bool,

    /// Parameter bindings ($1, $2, etc.)
    #[arg(short, long, value_delimiter = ',')]
    bind: Vec<String>,

    /// Output format (table, json, csv)
    #[arg(short, long, default_value = "table")]
    format: String,

    /// Database connection URL
    #[arg(long, env = "QAIL_DATABASE_URL")]
    database_url: Option<String>,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Parse and explain a QAIL query
    Explain {
        /// The QAIL query to explain
        query: String,
    },
    /// Interactive REPL mode
    Repl,
    /// Show the symbol reference
    Symbols,
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Explain { query }) => explain_query(query),
        Some(Commands::Repl) => run_repl(),
        Some(Commands::Symbols) => show_symbols(),
        None => {
            if let Some(query) = &cli.query {
                execute_query(query, &cli);
            } else {
                println!("{}", "🪝 QAIL — The Horizontal Query Language".cyan().bold());
                println!();
                println!("Usage: kq <QUERY> [OPTIONS]");
                println!();
                println!("Try: kq --help");
            }
        }
    }
}

fn execute_query(query: &str, cli: &Cli) {
    if cli.verbose {
        println!("{} {}", "Input:".dimmed(), query.yellow());
    }

    match qail::parse(query) {
        Ok(cmd) => {
            let sql = cmd.to_sql();

            if cli.dry_run || cli.database_url.is_none() {
                println!("{}", "Generated SQL:".green().bold());
                println!("{}", sql.white());

                if !cli.bind.is_empty() {
                    println!();
                    println!("{}", "Bindings:".cyan());
                    for (i, b) in cli.bind.iter().enumerate() {
                        println!("  ${} = {}", i + 1, b.yellow());
                    }
                }

                if cli.database_url.is_none() && !cli.dry_run {
                    println!();
                    println!(
                        "{}",
                        "⚠ No database URL. Use --database-url or set QAIL_DATABASE_URL"
                            .yellow()
                    );
                }
            } else {
                // TODO: Execute with sqlx
                println!("{}", "Execution not yet implemented.".yellow());
                println!("{}", "Generated SQL:".green().bold());
                println!("{}", sql.white());
            }
        }
        Err(e) => {
            eprintln!("{} {}", "Error:".red().bold(), e);
            std::process::exit(1);
        }
    }
}

fn explain_query(query: &str) {
    println!("{}", "🪝 QAIL Query Explanation".cyan().bold());
    println!();
    println!("{} {}", "Query:".dimmed(), query.yellow());
    println!();

    match qail::parse(query) {
        Ok(cmd) => {
            println!("{}", "Parsed Structure:".green().bold());
            println!("  {} {}", "Action:".dimmed(), format!("{}", cmd.action).cyan());
            println!("  {} {}", "Table:".dimmed(), cmd.table.white());

            if !cmd.columns.is_empty() {
                println!("  {}", "Columns:".dimmed());
                for col in &cmd.columns {
                    println!("    • {}", col.to_string().white());
                }
            }

            if !cmd.cages.is_empty() {
                println!("  {}", "Cages:".dimmed());
                for cage in &cmd.cages {
                    let kind = match &cage.kind {
                        CageKind::Filter => "Filter".to_string(),
                        CageKind::Payload => "Payload".to_string(),
                        CageKind::Sort(SortOrder::Asc) => "Sort ↑".to_string(),
                        CageKind::Sort(SortOrder::Desc) => "Sort ↓".to_string(),
                        CageKind::Limit(n) => format!("Limit({})", n),
                        CageKind::Offset(n) => format!("Offset({})", n),
                    };
                    println!("    [{}]", kind.cyan());
                    for cond in &cage.conditions {
                        println!(
                            "      {} {:?} {}",
                            cond.column.white(),
                            cond.op,
                            cond.value.to_string().yellow()
                        );
                    }
                }
            }

            println!();
            println!("{}", "Generated SQL:".green().bold());
            println!("  {}", cmd.to_sql().white());
        }
        Err(e) => {
            eprintln!("{} {}", "Parse Error:".red().bold(), e);
        }
    }
}

fn run_repl() {
    println!("{}", "🪝 QAIL REPL — Interactive Mode".cyan().bold());
    println!("{}", "Type 'exit' or Ctrl+C to quit.".dimmed());
    println!();

    // TODO: Implement actual REPL with rustyline
    println!("{}", "REPL mode not yet implemented.".yellow());
}

fn show_symbols() {
    println!("{}", "🪝 QAIL Symbol Reference".cyan().bold());
    println!();

    let symbols = [
        ("::", "The Gate", "Defines the action", "SELECT/UPDATE/DELETE"),
        ("•", "The Pivot", "Connects action to table", "FROM table"),
        ("@", "The Hook", "Selects columns", "col1, col2"),
        ("[]", "The Cage", "Filters & constraints", "WHERE, SET, LIMIT"),
        ("~", "The Fuse", "Fuzzy match", "ILIKE '%val%'"),
        ("|", "The Split", "Logical OR", "OR"),
        ("&", "The Bind", "Logical AND", "AND"),
        ("^!", "The Peak", "Sort descending", "ORDER BY ... DESC"),
        ("^", "The Rise", "Sort ascending", "ORDER BY ... ASC"),
        ("*", "The Star", "Wildcard/All", "*"),
        ("[*]", "The Deep", "Array unnest", "UNNEST(arr)"),
        ("$", "The Var", "Parameter", "$1, $2"),
    ];

    println!(
        "{:8} {:15} {:30} {}",
        "Symbol".white().bold(),
        "Name".white().bold(),
        "Function".white().bold(),
        "SQL Equivalent".white().bold()
    );
    println!("{}", "─".repeat(80).dimmed());

    for (symbol, name, function, sql) in symbols {
        println!(
            "{:8} {:15} {:30} {}",
            symbol.cyan().bold(),
            name.yellow(),
            function.white(),
            sql.dimmed()
        );
    }
}
