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

use clap::{Parser, Subcommand, ValueEnum};
use colored::*;
use qail::prelude::*;
use std::collections::HashMap;

#[derive(Parser)]
#[command(name = "qail")]
#[command(author = "QAIL Contributors")]
#[command(version = "0.1.0")]
#[command(about = "🪝 The Horizontal Query Language CLI", long_about = None)]
#[command(after_help = "EXAMPLES:
    qail 'get::users•@*[active=true]'
    qail 'get::orders•@id@total[user_id=$1][lim=10]' --bind 42
    qail 'set::users•[verified=true][id=$1]' --bind 7 --dry-run")]
struct Cli {
    /// The QAIL query to execute
    query: Option<String>,

    /// Don't execute, just show the generated SQL
    #[arg(short, long)]
    dry_run: bool,

    /// Parameter bindings ($1, $2, etc.)
    #[arg(short, long, value_delimiter = ',')]
    bind: Vec<String>,

    /// Output format
    #[arg(short, long, value_enum, default_value = "table")]
    format: OutputFormat,

    /// Database connection URL
    #[arg(long, env = "QAIL_DATABASE_URL")]
    database_url: Option<String>,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Output file path (for gen command)
    #[arg(short, long)]
    output: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Explain { query }) => explain_query(query),
        Some(Commands::Repl) => run_repl(),
        Some(Commands::Symbols) => show_symbols(),
        None => {
            if let Some(query) = &cli.query {
                if let Err(e) = execute_query(query, &cli).await {
                    eprintln!("{} {}", "Error:".red().bold(), e);
                    std::process::exit(1);
                }
            } else {
                println!("{}", "🪝 QAIL — The Horizontal Query Language".cyan().bold());
                println!();
                println!("Usage: qail <QUERY> [OPTIONS]");
                println!();
                println!("Try: qail --help");
            }
        }
    }
}

async fn execute_query(query: &str, cli: &Cli) -> Result<(), QailError> {
    if cli.verbose {
        println!("{} {}", "Input:".dimmed(), query.yellow());
    }

    // Parse the query
    let cmd = qail::parse(query)?;
    let sql = cmd.to_sql();

    // Dry run or no database URL - just show SQL
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
        return Ok(());
    }

    // Connect and execute
    let db_url = cli.database_url.as_ref().unwrap();
    if cli.verbose {
        println!("{} {}", "Connecting to:".dimmed(), db_url);
    }

    let db = QailDB::connect(db_url).await?;
    
    // Handle Gen action separately
    if cmd.action == Action::Gen {
        let columns = qail::schema::get_table_schema(db.pool(), &cmd.table).await?;
        let struct_code = qail::schema::generate_struct(&cmd.table, &columns);
        
        if let Some(ref path) = cli.output {
            std::fs::write(path, &struct_code)
                .map_err(|e| QailError::Execution(format!("Failed to write file: {}", e)))?;
            println!("{} Wrote struct to {}", "✓".green(), path.cyan());
        } else {
            println!("{}", struct_code);
        }
        return Ok(());
    }
    
    let mut qry = db.query(query);

    // Bind parameters
    for binding in &cli.bind {
        // Try to parse as number, otherwise use as string
        if let Ok(n) = binding.parse::<i64>() {
            qry = qry.bind(n);
        } else if let Ok(f) = binding.parse::<f64>() {
            qry = qry.bind(f);
        } else if binding == "true" {
            qry = qry.bind(true);
        } else if binding == "false" {
            qry = qry.bind(false);
        } else {
            qry = qry.bind(binding.as_str());
        }
    }

    // Execute based on action type
    match cmd.action {
        Action::Get => {
            let results = qry.fetch_all().await?;
            format_output(&results, &cli.format);
        }
        Action::Set | Action::Del | Action::Add => {
            let affected = qry.execute().await?;
            println!("{} {} rows affected", "✓".green(), affected);
        }
        Action::Gen => unreachable!(), // Handled above
    }

    Ok(())
}

fn format_output(results: &[HashMap<String, serde_json::Value>], format: &OutputFormat) {
    if results.is_empty() {
        println!("{}", "(no results)".dimmed());
        return;
    }

    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(results).unwrap_or_default());
        }
        OutputFormat::Table => {
            // Get column names from first row
            let columns: Vec<&String> = results[0].keys().collect();
            
            // Calculate column widths
            let mut widths: HashMap<&String, usize> = columns.iter().map(|c| (*c, c.len())).collect();
            for row in results {
                for (col, val) in row {
                    let len = val_to_string(val).len();
                    if let Some(w) = widths.get_mut(col) {
                        *w = (*w).max(len);
                    }
                }
            }

            // Print header
            let header: Vec<String> = columns
                .iter()
                .map(|c| format!("{:width$}", c, width = widths[*c]))
                .collect();
            println!("{}", header.join(" │ ").white().bold());
            
            // Print separator
            let sep: Vec<String> = columns
                .iter()
                .map(|c| "─".repeat(widths[*c]))
                .collect();
            println!("{}", sep.join("─┼─").dimmed());

            // Print rows
            for row in results {
                let cells: Vec<String> = columns
                    .iter()
                    .map(|c| {
                        let val = row.get(*c).map(val_to_string).unwrap_or_default();
                        format!("{:width$}", val, width = widths[*c])
                    })
                    .collect();
                println!("{}", cells.join(" │ "));
            }

            println!();
            println!("{} row(s) returned", results.len().to_string().cyan());
        }
    }
}

fn val_to_string(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        _ => val.to_string(),
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

