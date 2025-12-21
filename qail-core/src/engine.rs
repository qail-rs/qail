//! Database execution engine for QAIL.
//!
//! This module provides the runtime for executing parsed QAIL queries
//! against PostgreSQL, MySQL, or SQLite databases using sqlx.

use crate::error::QailError;
use crate::parser;
use crate::transpiler::ToSql;

use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Column, Row, TypeInfo};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Cache for prepared statements (QAIL -> SQL mapping).
pub type StatementCache = Arc<RwLock<HashMap<String, String>>>;

/// A database connection for executing QAIL queries.
#[derive(Clone)]
pub struct QailDB {
    pool: PgPool,
    /// Cache for QAIL -> SQL mappings to avoid reparsing.
    cache: StatementCache,
    /// Whether caching is enabled.
    cache_enabled: bool,
}

impl QailDB {
    /// Connect to a database using a connection URL.
    ///
    /// Supported URL formats:
    /// - `postgres://user:pass@host/db`
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let db = QailDB::connect("postgres://localhost/mydb").await?;
    /// ```
    pub async fn connect(url: &str) -> Result<Self, QailError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .map_err(|e| QailError::Connection(e.to_string()))?;

        Ok(Self {
            pool,
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_enabled: true,
        })
    }

    /// Enable or disable statement caching.
    pub fn with_cache(mut self, enabled: bool) -> Self {
        self.cache_enabled = enabled;
        self
    }

    /// Clear the statement cache.
    pub fn clear_cache(&self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.clear();
        }
    }

    /// Get cache statistics.
    pub fn cache_stats(&self) -> (usize, bool) {
        let size = self.cache.read().map(|c| c.len()).unwrap_or(0);
        (size, self.cache_enabled)
    }

    /// Get cached SQL for a QAIL query, or parse and cache it.
    fn get_or_parse(&self, qail: &str) -> Result<String, QailError> {
        // Check cache first (read lock)
        if self.cache_enabled {
            if let Ok(cache) = self.cache.read() {
                if let Some(sql) = cache.get(qail) {
                    return Ok(sql.clone());
                }
            }
        }

        // Parse and cache (write lock)
        let cmd = parser::parse(qail)?;
        let sql = cmd.to_sql();

        if self.cache_enabled {
            if let Ok(mut cache) = self.cache.write() {
                cache.insert(qail.to_string(), sql.clone());
            }
        }

        Ok(sql)
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
        QailQuery::new(
            self.pool.clone(),
            qail.to_string(),
            Some(self.cache.clone()),
            self.cache_enabled,
        )
    }

    /// Execute a raw SQL query (escape hatch).
    pub fn raw(&self, sql: &str) -> QailQuery {
        QailQuery::raw(self.pool.clone(), sql.to_string())
    }

    /// Get a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Begin a new transaction.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let tx = db.begin().await?;
    /// tx.query("set::users•[balance=100][id=$1]").bind(1).execute().await?;
    /// tx.query("add::logs•@user_id@action[user_id=$1][action='deposit']").bind(1).execute().await?;
    /// tx.commit().await?;
    /// ```
    pub async fn begin(&self) -> Result<QailTransaction, QailError> {
        let tx = self.pool.begin().await
            .map_err(|e| QailError::Connection(format!("Failed to begin transaction: {}", e)))?;
        Ok(QailTransaction { tx: Some(tx) })
    }
}

/// A database transaction for executing multiple QAIL queries atomically.
pub struct QailTransaction {
    tx: Option<sqlx::Transaction<'static, sqlx::Postgres>>,
}

impl QailTransaction {
    /// Create a query within this transaction.
    pub fn query(&mut self, qail: &str) -> QailTxQuery<'_> {
        QailTxQuery::new(self.tx.as_mut().unwrap(), qail.to_string())
    }

    /// Execute raw SQL within this transaction.
    pub fn raw(&mut self, sql: &str) -> QailTxQuery<'_> {
        QailTxQuery::raw(self.tx.as_mut().unwrap(), sql.to_string())
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<(), QailError> {
        if let Some(tx) = self.tx.take() {
            tx.commit().await
                .map_err(|e| QailError::Execution(format!("Failed to commit: {}", e)))?;
        }
        Ok(())
    }

    /// Rollback the transaction.
    pub async fn rollback(mut self) -> Result<(), QailError> {
        if let Some(tx) = self.tx.take() {
            tx.rollback().await
                .map_err(|e| QailError::Execution(format!("Failed to rollback: {}", e)))?;
        }
        Ok(())
    }
}

/// A QAIL query within a transaction.
pub struct QailTxQuery<'a> {
    tx: &'a mut sqlx::Transaction<'static, sqlx::Postgres>,
    qail: String,
    sql: Option<String>,
    bindings: Vec<QailValue>,
    is_raw: bool,
}

impl<'a> QailTxQuery<'a> {
    fn new(tx: &'a mut sqlx::Transaction<'static, sqlx::Postgres>, qail: String) -> Self {
        Self {
            tx,
            qail,
            sql: None,
            bindings: Vec::new(),
            is_raw: false,
        }
    }

    fn raw(tx: &'a mut sqlx::Transaction<'static, sqlx::Postgres>, sql: String) -> Self {
        Self {
            tx,
            qail: String::new(),
            sql: Some(sql),
            bindings: Vec::new(),
            is_raw: true,
        }
    }

    /// Bind a value.
    pub fn bind<T: Into<QailValue>>(mut self, value: T) -> Self {
        self.bindings.push(value.into());
        self
    }

    /// Get the generated SQL.
    pub fn sql(&self) -> Result<String, QailError> {
        if self.is_raw {
            return Ok(self.sql.clone().unwrap_or_default());
        }
        let cmd = parser::parse(&self.qail)?;
        Ok(cmd.to_sql())
    }

    /// Execute within the transaction.
    pub async fn execute(self) -> Result<u64, QailError> {
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
            .execute(&mut **self.tx)
            .await
            .map_err(|e| QailError::Execution(e.to_string()))?;

        Ok(result.rows_affected())
    }

    /// Fetch all rows within the transaction.
    pub async fn fetch_all(self) -> Result<Vec<HashMap<String, serde_json::Value>>, QailError> {
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

        let rows: Vec<PgRow> = query
            .fetch_all(&mut **self.tx)
            .await
            .map_err(|e| QailError::Execution(e.to_string()))?;

        Ok(rows.iter().map(row_to_map).collect())
    }
}

/// A QAIL query builder with parameter bindings.
pub struct QailQuery {
    pool: PgPool,
    qail: String,
    sql: Option<String>,
    bindings: Vec<QailValue>,
    is_raw: bool,
    cache: Option<StatementCache>,
    cache_enabled: bool,
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
    fn new(pool: PgPool, qail: String, cache: Option<StatementCache>, cache_enabled: bool) -> Self {
        Self {
            pool,
            qail,
            sql: None,
            bindings: Vec::new(),
            is_raw: false,
            cache,
            cache_enabled,
        }
    }

    fn raw(pool: PgPool, sql: String) -> Self {
        Self {
            pool,
            qail: String::new(),
            sql: Some(sql),
            bindings: Vec::new(),
            is_raw: true,
            cache: None,
            cache_enabled: false,
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

        // Check cache first
        if self.cache_enabled {
            if let Some(ref cache) = self.cache {
                if let Ok(c) = cache.read() {
                    if let Some(sql) = c.get(&self.qail) {
                        return Ok(sql.clone());
                    }
                }
            }
        }

        // Parse and cache
        let cmd = parser::parse(&self.qail)?;
        let sql = cmd.to_sql();

        if self.cache_enabled {
            if let Some(ref cache) = self.cache {
                if let Ok(mut c) = cache.write() {
                    c.insert(self.qail.clone(), sql.clone());
                }
            }
        }

        Ok(sql)
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

        let rows: Vec<PgRow> = query
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

        let row: PgRow = query
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

/// Convert a PgRow to a HashMap, handling Postgres-specific types.
fn row_to_map(row: &PgRow) -> HashMap<String, serde_json::Value> {
    use sqlx::ValueRef;
    
    let mut map = HashMap::new();

    for (i, column) in row.columns().iter().enumerate() {
        let name = column.name().to_string();
        let type_name = column.type_info().name();

        // Try to get the raw value first to check for NULL
        let value_ref = row.try_get_raw(i);
        if value_ref.is_err() || value_ref.as_ref().map(|v| v.is_null()).unwrap_or(true) {
            map.insert(name, serde_json::Value::Null);
            continue;
        }

        let value: serde_json::Value = match type_name {
            "BOOL" => row
                .try_get::<bool, _>(i)
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null),
            "INT2" | "INT4" => row
                .try_get::<i32, _>(i)
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null),
            "INT8" => row
                .try_get::<i64, _>(i)
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null),
            "FLOAT4" => row
                .try_get::<f32, _>(i)
                .ok()
                .and_then(|v| serde_json::Number::from_f64(v as f64))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            "FLOAT8" => row
                .try_get::<f64, _>(i)
                .ok()
                .and_then(|v| serde_json::Number::from_f64(v))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            "UUID" => row
                .try_get::<sqlx::types::Uuid, _>(i)
                .map(|v| serde_json::Value::String(v.to_string()))
                .unwrap_or(serde_json::Value::Null),
            "TIMESTAMPTZ" | "TIMESTAMP" => row
                .try_get::<chrono::DateTime<chrono::Utc>, _>(i)
                .map(|v| serde_json::Value::String(v.to_rfc3339()))
                .or_else(|_| {
                    row.try_get::<chrono::NaiveDateTime, _>(i)
                        .map(|v| serde_json::Value::String(v.to_string()))
                })
                .unwrap_or(serde_json::Value::Null),
            "DATE" => row
                .try_get::<chrono::NaiveDate, _>(i)
                .map(|v| serde_json::Value::String(v.to_string()))
                .unwrap_or(serde_json::Value::Null),
            "TEXT" | "VARCHAR" | "CHAR" | "NAME" => row
                .try_get::<String, _>(i)
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
            "JSONB" | "JSON" => row
                .try_get::<serde_json::Value, _>(i)
                .unwrap_or(serde_json::Value::Null),
            _ => {
                // Fallback: try to get as string
                row.try_get::<String, _>(i)
                    .map(serde_json::Value::String)
                    .unwrap_or_else(|_| serde_json::Value::String(format!("<{}>", type_name)))
            }
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
