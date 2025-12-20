//! Database execution engine for QAIL.
//!
//! This module provides the runtime for executing parsed QAIL queries
//! against PostgreSQL, MySQL, or SQLite databases using sqlx.

use crate::error::QailError;
use crate::parser;
use crate::transpiler::ToSql;

use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{AnyPool, Column, Row, TypeInfo};
use std::collections::HashMap;

/// A database connection for executing QAIL queries.
#[derive(Clone)]
pub struct QailDB {
    pool: AnyPool,
}

impl QailDB {
    /// Connect to a database using a connection URL.
    ///
    /// Supported URL formats:
    /// - `postgres://user:pass@host/db`
    /// - `mysql://user:pass@host/db`
    /// - `sqlite://path/to/db.sqlite` or `sqlite::memory:`
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let db = QailDB::connect("postgres://localhost/mydb").await?;
    /// ```
    pub async fn connect(url: &str) -> Result<Self, QailError> {
        // Install default drivers
        sqlx::any::install_default_drivers();
        
        let pool = AnyPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .map_err(|e| QailError::Connection(e.to_string()))?;

        Ok(Self { pool })
    }

    /// Create a new query from a QAIL string.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let users = db
    ///     .query("get::users•@*[active=$1]")
    ///     .bind(true)
    ///     .fetch_all()
    ///     .await?;
    /// ```
    pub fn query(&self, qail: &str) -> QailQuery {
        QailQuery::new(self.pool.clone(), qail.to_string())
    }

    /// Execute a raw SQL query (escape hatch).
    pub fn raw(&self, sql: &str) -> QailQuery {
        QailQuery::raw(self.pool.clone(), sql.to_string())
    }

    /// Get a reference to the underlying connection pool.
    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }
}

/// A QAIL query builder with parameter bindings.
pub struct QailQuery {
    pool: AnyPool,
    qail: String,
    sql: Option<String>,
    bindings: Vec<QailValue>,
    is_raw: bool,
}

/// Dynamic value type for query bindings.
#[derive(Debug, Clone)]
pub enum QailValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl QailQuery {
    fn new(pool: AnyPool, qail: String) -> Self {
        Self {
            pool,
            qail,
            sql: None,
            bindings: Vec::new(),
            is_raw: false,
        }
    }

    fn raw(pool: AnyPool, sql: String) -> Self {
        Self {
            pool,
            qail: String::new(),
            sql: Some(sql),
            bindings: Vec::new(),
            is_raw: true,
        }
    }

    /// Bind a boolean value.
    pub fn bind_bool(mut self, value: bool) -> Self {
        self.bindings.push(QailValue::Bool(value));
        self
    }

    /// Bind an integer value.
    pub fn bind_int(mut self, value: i64) -> Self {
        self.bindings.push(QailValue::Int(value));
        self
    }

    /// Bind a float value.
    pub fn bind_float(mut self, value: f64) -> Self {
        self.bindings.push(QailValue::Float(value));
        self
    }

    /// Bind a string value.
    pub fn bind_str(mut self, value: &str) -> Self {
        self.bindings.push(QailValue::String(value.to_string()));
        self
    }

    /// Bind a value (auto-detect type from common types).
    pub fn bind<T: Into<QailValue>>(mut self, value: T) -> Self {
        self.bindings.push(value.into());
        self
    }

    /// Get the generated SQL without executing.
    pub fn sql(&self) -> Result<String, QailError> {
        if self.is_raw {
            return Ok(self.sql.clone().unwrap_or_default());
        }
        let cmd = parser::parse(&self.qail)?;
        Ok(cmd.to_sql())
    }

    /// Fetch all rows as JSON-like maps.
    pub async fn fetch_all(&self) -> Result<Vec<HashMap<String, serde_json::Value>>, QailError> {
        let sql = self.sql()?;
        let mut query = sqlx::query(&sql);

        // Bind parameters
        for binding in &self.bindings {
            query = match binding {
                QailValue::Null => query,
                QailValue::Bool(v) => query.bind(*v),
                QailValue::Int(v) => query.bind(*v),
                QailValue::Float(v) => query.bind(*v),
                QailValue::String(v) => query.bind(v.as_str()),
            };
        }

        let rows: Vec<AnyRow> = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| QailError::Execution(e.to_string()))?;

        // Convert rows to HashMaps
        let results: Vec<HashMap<String, serde_json::Value>> = rows
            .iter()
            .map(|row| row_to_map(row))
            .collect();

        Ok(results)
    }

    /// Fetch a single row as a JSON-like map.
    pub async fn fetch_one(&self) -> Result<HashMap<String, serde_json::Value>, QailError> {
        let sql = self.sql()?;
        let mut query = sqlx::query(&sql);

        for binding in &self.bindings {
            query = match binding {
                QailValue::Null => query,
                QailValue::Bool(v) => query.bind(*v),
                QailValue::Int(v) => query.bind(*v),
                QailValue::Float(v) => query.bind(*v),
                QailValue::String(v) => query.bind(v.as_str()),
            };
        }

        let row: AnyRow = query
            .fetch_one(&self.pool)
            .await
            .map_err(|e| QailError::Execution(e.to_string()))?;

        Ok(row_to_map(&row))
    }

    /// Execute a mutation query (INSERT, UPDATE, DELETE).
    /// Returns the number of affected rows.
    pub async fn execute(&self) -> Result<u64, QailError> {
        let sql = self.sql()?;
        let mut query = sqlx::query(&sql);

        for binding in &self.bindings {
            query = match binding {
                QailValue::Null => query,
                QailValue::Bool(v) => query.bind(*v),
                QailValue::Int(v) => query.bind(*v),
                QailValue::Float(v) => query.bind(*v),
                QailValue::String(v) => query.bind(v.as_str()),
            };
        }

        let result = query
            .execute(&self.pool)
            .await
            .map_err(|e| QailError::Execution(e.to_string()))?;

        Ok(result.rows_affected())
    }
}

/// Convert an AnyRow to a HashMap.
fn row_to_map(row: &AnyRow) -> HashMap<String, serde_json::Value> {
    let mut map = HashMap::new();

    for (i, column) in row.columns().iter().enumerate() {
        let name = column.name().to_string();
        let type_name = column.type_info().name();

        let value: serde_json::Value = match type_name {
            "BOOL" | "BOOLEAN" => row
                .try_get::<bool, _>(i)
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null),
            "INT2" | "INT4" | "INT8" | "INTEGER" | "BIGINT" | "SMALLINT" => row
                .try_get::<i64, _>(i)
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null),
            "FLOAT4" | "FLOAT8" | "REAL" | "DOUBLE" => row
                .try_get::<f64, _>(i)
                .ok()
                .and_then(|v| serde_json::Number::from_f64(v))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            _ => row
                .try_get::<String, _>(i)
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
        };

        map.insert(name, value);
    }

    map
}

// Implement From traits for QailValue
impl From<bool> for QailValue {
    fn from(v: bool) -> Self {
        QailValue::Bool(v)
    }
}

impl From<i32> for QailValue {
    fn from(v: i32) -> Self {
        QailValue::Int(v as i64)
    }
}

impl From<i64> for QailValue {
    fn from(v: i64) -> Self {
        QailValue::Int(v)
    }
}

impl From<f64> for QailValue {
    fn from(v: f64) -> Self {
        QailValue::Float(v)
    }
}

impl From<&str> for QailValue {
    fn from(v: &str) -> Self {
        QailValue::String(v.to_string())
    }
}

impl From<String> for QailValue {
    fn from(v: String) -> Self {
        QailValue::String(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qail_value_from() {
        let _b: QailValue = true.into();
        let _i: QailValue = 42i32.into();
        let _f: QailValue = 3.14f64.into();
        let _s: QailValue = "hello".into();
    }
}
