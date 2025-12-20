//! Schema introspection and Rust struct generation.
//!
//! This module queries database schema and generates Rust structs.

use crate::error::QailError;
use sqlx::postgres::PgPool;
use sqlx::Row;

/// Column information from database schema.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub column_default: Option<String>,
}

/// Query the schema for a table and return column information.
pub async fn get_table_schema(pool: &PgPool, table_name: &str) -> Result<Vec<ColumnInfo>, QailError> {
    let rows = sqlx::query(
        r#"
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_name = $1 
        ORDER BY ordinal_position
        "#,
    )
    .bind(table_name)
    .fetch_all(pool)
    .await
    .map_err(|e| QailError::Execution(e.to_string()))?;

    let columns: Vec<ColumnInfo> = rows
        .iter()
        .map(|row| ColumnInfo {
            name: row.get::<String, _>("column_name"),
            data_type: row.get::<String, _>("data_type"),
            is_nullable: row.get::<String, _>("is_nullable") == "YES",
            column_default: row.try_get::<String, _>("column_default").ok(),
        })
        .collect();

    if columns.is_empty() {
        return Err(QailError::Execution(format!(
            "Table '{}' not found or has no columns",
            table_name
        )));
    }

    Ok(columns)
}

/// Map PostgreSQL type to Rust type.
fn pg_to_rust_type(pg_type: &str, nullable: bool) -> String {
    let base_type = match pg_type.to_lowercase().as_str() {
        "uuid" => "Uuid",
        "text" | "varchar" | "character varying" | "char" | "character" | "name" => "String",
        "int2" | "smallint" => "i16",
        "int4" | "integer" | "int" => "i32",
        "int8" | "bigint" => "i64",
        "float4" | "real" => "f32",
        "float8" | "double precision" => "f64",
        "numeric" | "decimal" => "rust_decimal::Decimal",
        "bool" | "boolean" => "bool",
        "timestamp without time zone" | "timestamp" => "chrono::NaiveDateTime",
        "timestamp with time zone" | "timestamptz" => "chrono::DateTime<chrono::Utc>",
        "date" => "chrono::NaiveDate",
        "time" | "time without time zone" => "chrono::NaiveTime",
        "jsonb" | "json" => "serde_json::Value",
        "bytea" => "Vec<u8>",
        t if t.ends_with("[]") => {
            let inner = pg_to_rust_type(&t[..t.len() - 2], false);
            return format!("Vec<{}>", inner);
        }
        _ => "String", // Fallback for unknown types
    };

    if nullable {
        format!("Option<{}>", base_type)
    } else {
        base_type.to_string()
    }
}

/// Convert snake_case to PascalCase for struct name.
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect()
}

/// Generate a Rust struct from table schema.
pub fn generate_struct(table_name: &str, columns: &[ColumnInfo]) -> String {
    let struct_name = to_pascal_case(table_name);
    
    let mut output = String::new();
    
    // Add derives
    output.push_str("use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};\n");
    output.push_str("use serde::{Deserialize, Serialize};\n");
    output.push_str("use sqlx::FromRow;\n");
    output.push_str("use uuid::Uuid;\n");
    output.push_str("\n");
    output.push_str("#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]\n");
    output.push_str(&format!("pub struct {} {{\n", struct_name));
    
    for col in columns {
        let rust_type = pg_to_rust_type(&col.data_type, col.is_nullable);
        output.push_str(&format!("    pub {}: {},\n", col.name, rust_type));
    }
    
    output.push_str("}\n");
    
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_pascal_case() {
        assert_eq!(to_pascal_case("users"), "Users");
        assert_eq!(to_pascal_case("ai_knowledge_base"), "AiKnowledgeBase");
        assert_eq!(to_pascal_case("order_items"), "OrderItems");
    }

    #[test]
    fn test_pg_to_rust_type() {
        assert_eq!(pg_to_rust_type("uuid", false), "Uuid");
        assert_eq!(pg_to_rust_type("text", false), "String");
        assert_eq!(pg_to_rust_type("integer", true), "Option<i32>");
        assert_eq!(pg_to_rust_type("timestamp with time zone", false), "chrono::DateTime<chrono::Utc>");
        assert_eq!(pg_to_rust_type("text[]", false), "Vec<String>");
    }
}
