//! SQL Transpiler for QAIL AST.
//!
//! Converts parsed QAIL commands into executable SQL strings.

use crate::ast::*;

/// Trait for converting AST nodes to SQL.
pub trait ToSql {
    /// Convert this node to a SQL string.
    fn to_sql(&self) -> String;
}

impl ToSql for QailCmd {
    fn to_sql(&self) -> String {
        match self.action {
            Action::Get => self.to_select_sql(),
            Action::Set => self.to_update_sql(),
            Action::Del => self.to_delete_sql(),
            Action::Add => self.to_insert_sql(),
        }
    }
}

impl QailCmd {
    /// Generate SELECT SQL.
    fn to_select_sql(&self) -> String {
        let mut sql = String::from("SELECT ");

        // Columns
        if self.columns.is_empty() {
            sql.push('*');
        } else {
            let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
            sql.push_str(&cols.join(", "));
        }

        // FROM
        sql.push_str(" FROM ");
        sql.push_str(&self.table);

        // Process cages
        let mut where_groups: Vec<String> = Vec::new();
        let mut order_by: Option<String> = None;
        let mut limit: Option<usize> = None;
        let mut offset: Option<usize> = None;

        for cage in &self.cages {
            match &cage.kind {
                CageKind::Filter => {
                    if !cage.conditions.is_empty() {
                        let joiner = match cage.logical_op {
                            LogicalOp::And => " AND ",
                            LogicalOp::Or => " OR ",
                        };
                        let conditions: Vec<String> = cage.conditions.iter().map(|c| c.to_sql()).collect();
                        let group = conditions.join(joiner);
                        // Wrap OR groups in parentheses for correct precedence
                        if cage.logical_op == LogicalOp::Or && cage.conditions.len() > 1 {
                            where_groups.push(format!("({})", group));
                        } else {
                            where_groups.push(group);
                        }
                    }
                }
                CageKind::Sort(order) => {
                    if let Some(cond) = cage.conditions.first() {
                        let dir = match order {
                            SortOrder::Asc => "ASC",
                            SortOrder::Desc => "DESC",
                        };
                        order_by = Some(format!("{} {}", cond.column, dir));
                    }
                }
                CageKind::Limit(n) => {
                    limit = Some(*n);
                }
                CageKind::Offset(n) => {
                    offset = Some(*n);
                }
                CageKind::Payload => {
                    // Not used in SELECT
                }
            }
        }

        // WHERE - each cage group is joined with AND
        if !where_groups.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_groups.join(" AND "));
        }

        // ORDER BY
        if let Some(order) = order_by {
            sql.push_str(" ORDER BY ");
            sql.push_str(&order);
        }

        // LIMIT
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {}", n));
        }

        // OFFSET
        if let Some(n) = offset {
            sql.push_str(&format!(" OFFSET {}", n));
        }

        sql
    }

    /// Generate UPDATE SQL.
    fn to_update_sql(&self) -> String {
        let mut sql = String::from("UPDATE ");
        sql.push_str(&self.table);

        // For SET queries, first cage is payload, rest are filters
        let mut set_clauses: Vec<String> = Vec::new();
        let mut where_clauses: Vec<String> = Vec::new();
        let mut is_first_filter = true;

        for cage in &self.cages {
            if let CageKind::Filter = cage.kind {
                if is_first_filter {
                    // First filter cage is the SET payload
                    for cond in &cage.conditions {
                        set_clauses.push(format!("{} = {}", cond.column, cond.value));
                    }
                    is_first_filter = false;
                } else {
                    // Subsequent filter cages are WHERE conditions
                    for cond in &cage.conditions {
                        where_clauses.push(cond.to_sql());
                    }
                }
            }
        }

        // SET clause
        if !set_clauses.is_empty() {
            sql.push_str(" SET ");
            sql.push_str(&set_clauses.join(", "));
        }

        // WHERE clause
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        sql
    }

    /// Generate DELETE SQL.
    fn to_delete_sql(&self) -> String {
        let mut sql = String::from("DELETE FROM ");
        sql.push_str(&self.table);

        // Process WHERE clauses
        let mut where_clauses: Vec<String> = Vec::new();

        for cage in &self.cages {
            if let CageKind::Filter = cage.kind {
                for cond in &cage.conditions {
                    where_clauses.push(cond.to_sql());
                }
            }
        }

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        sql
    }

    /// Generate INSERT SQL.
    fn to_insert_sql(&self) -> String {
        let mut sql = String::from("INSERT INTO ");
        sql.push_str(&self.table);

        // For ADD queries, we use columns and first cage contains values
        let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        
        if !cols.is_empty() {
            sql.push_str(" (");
            sql.push_str(&cols.join(", "));
            sql.push(')');
        }

        // Values from first payload/filter cage
        if let Some(cage) = self.cages.first() {
            let values: Vec<String> = cage.conditions.iter().map(|c| c.value.to_string()).collect();
            
            if !values.is_empty() {
                sql.push_str(" VALUES (");
                sql.push_str(&values.join(", "));
                sql.push(')');
            }
        }

        sql
    }
}

impl Condition {
    /// Convert condition to SQL string.
    fn to_sql(&self) -> String {
        // Handle array unnest conditions: EXISTS (SELECT 1 FROM unnest(col) _el WHERE _el OP val)
        if self.is_array_unnest {
            let inner_condition = match self.op {
                Operator::Eq => format!("_el = {}", self.value),
                Operator::Ne => format!("_el != {}", self.value),
                Operator::Gt => format!("_el > {}", self.value),
                Operator::Gte => format!("_el >= {}", self.value),
                Operator::Lt => format!("_el < {}", self.value),
                Operator::Lte => format!("_el <= {}", self.value),
                Operator::Fuzzy => {
                    let val = match &self.value {
                        Value::String(s) => format!("'%{}%'", s),
                        Value::Param(n) => format!("'%' || ${} || '%'", n),
                        v => format!("'%{}%'", v),
                    };
                    format!("_el ILIKE {}", val)
                }
                _ => format!("_el = {}", self.value),
            };
            return format!(
                "EXISTS (SELECT 1 FROM unnest({}) _el WHERE {})",
                self.column, inner_condition
            );
        }
        
        // Normal conditions
        match self.op {
            Operator::Eq => format!("{} = {}", self.column, self.value),
            Operator::Ne => format!("{} != {}", self.column, self.value),
            Operator::Gt => format!("{} > {}", self.column, self.value),
            Operator::Gte => format!("{} >= {}", self.column, self.value),
            Operator::Lt => format!("{} < {}", self.column, self.value),
            Operator::Lte => format!("{} <= {}", self.column, self.value),
            Operator::Fuzzy => {
                // For fuzzy match, wrap the value in %...%
                let val = match &self.value {
                    Value::String(s) => format!("'%{}%'", s),
                    Value::Param(n) => format!("'%' || ${} || '%'", n),
                    v => format!("'%{}%'", v),
                };
                format!("{} ILIKE {}", self.column, val)
            }
            Operator::In => format!("{} = ANY({})", self.column, self.value),
            Operator::NotIn => format!("{} != ALL({})", self.column, self.value),
            Operator::IsNull => format!("{} IS NULL", self.column),
            Operator::IsNotNull => format!("{} IS NOT NULL", self.column),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;

    #[test]
    fn test_simple_select() {
        let cmd = parse("get::users•@*").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users");
    }

    #[test]
    fn test_select_columns() {
        let cmd = parse("get::users•@id@email@role").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT id, email, role FROM users");
    }

    #[test]
    fn test_select_with_where() {
        let cmd = parse("get::users•@*[active=true]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users WHERE active = true");
    }

    #[test]
    fn test_select_with_limit() {
        let cmd = parse("get::users•@*[lim=10]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users LIMIT 10");
    }

    #[test]
    fn test_select_with_order() {
        let cmd = parse("get::users•@*[^!created_at]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users ORDER BY created_at DESC");
    }

    #[test]
    fn test_select_complex() {
        let cmd = parse("get::users•@id@email[active=true][^!created_at][lim=10]").unwrap();
        assert_eq!(
            cmd.to_sql(),
            "SELECT id, email FROM users WHERE active = true ORDER BY created_at DESC LIMIT 10"
        );
    }

    #[test]
    fn test_update() {
        let cmd = parse("set::users•[verified=true][id=$1]").unwrap();
        assert_eq!(cmd.to_sql(), "UPDATE users SET verified = true WHERE id = $1");
    }

    #[test]
    fn test_delete() {
        let cmd = parse("del::sessions•[expired_at<now]").unwrap();
        // Note: 'now' is parsed as a function
        assert_eq!(cmd.to_sql(), "DELETE FROM sessions WHERE expired_at < now()");
    }

    #[test]
    fn test_fuzzy_match() {
        let cmd = parse("get::users•@*[name~$1]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users WHERE name ILIKE '%' || $1 || '%'");
    }

    #[test]
    fn test_or_conditions() {
        let cmd = parse("get::users•@*[role=admin|role=mod]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users WHERE (role = 'admin' OR role = 'mod')");
    }

    #[test]
    fn test_mixed_and_or() {
        let cmd = parse("get::users•@*[active=true][role=admin|role=mod]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM users WHERE active = true AND (role = 'admin' OR role = 'mod')");
    }

    #[test]
    fn test_array_unnest() {
        let cmd = parse("get::posts•@*[tags[*]~$1]").unwrap();
        assert_eq!(cmd.to_sql(), "SELECT * FROM posts WHERE EXISTS (SELECT 1 FROM unnest(tags) _el WHERE _el ILIKE '%' || $1 || '%')");
    }

    #[test]
    fn test_complex_array_search() {
        let cmd = parse("get::kb•@*[active=true][keywords[*]~$1|question~$1]").unwrap();
        assert_eq!(
            cmd.to_sql(),
            "SELECT * FROM kb WHERE active = true AND (EXISTS (SELECT 1 FROM unnest(keywords) _el WHERE _el ILIKE '%' || $1 || '%') OR question ILIKE '%' || $1 || '%')"
        );
    }
}
