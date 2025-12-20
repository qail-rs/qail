//! # QAIL — The Horizontal Query Language
//!
//! > **Stop writing strings. Hook your data.**
//!
//! QAIL transforms dense, symbolic query syntax into executable SQL.
//!
//! ## Quick Example
//!
//! ```rust,ignore
//! use qail::prelude::*;
//!
//! // Parse a QAIL query
//! let cmd = qail::parse("get::users•@id@email[active=true][lim=10]")?;
//!
//! // Transpile to SQL
//! let sql = cmd.to_sql();
//! // => "SELECT id, email FROM users WHERE active = true LIMIT 10"
//! ```
//!
//! ## Symbology
//!
//! | Symbol | Name     | Function              |
//! |--------|----------|-----------------------|
//! | `::`   | Gate     | Defines action        |
//! | `•`    | Pivot    | Connects to table     |
//! | `@`    | Hook     | Selects columns       |
//! | `[]`   | Cage     | Filters/constraints   |
//! | `~`    | Fuse     | Fuzzy match           |
//! | `$`    | Var      | Parameter binding     |

pub mod ast;
pub mod engine;
pub mod error;
pub mod parser;
pub mod schema;
pub mod transpiler;

pub mod prelude {
    pub use crate::ast::*;
    pub use crate::engine::{QailDB, QailQuery, QailValue};
    pub use crate::error::*;
    pub use crate::parser::parse;
    pub use crate::schema::{generate_struct, get_table_schema};
    pub use crate::transpiler::ToSql;
}

/// Parse a QAIL query string into a command AST.
///
/// # Example
///
/// ```
/// use qail::parse;
///
/// let cmd = parse("get::users•@*[active=true]").unwrap();
/// assert_eq!(cmd.table, "users");
/// ```
pub fn parse(input: &str) -> Result<ast::QailCmd, error::QailError> {
    parser::parse(input)
}
