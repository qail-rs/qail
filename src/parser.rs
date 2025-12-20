//! QAIL Parser using nom.
//!
//! Parses QAIL syntax into an AST.
//!
//! # Syntax Overview
//!
//! ```text
//! get::users•@id@email[active=true][lim=10]
//! ─┬─ ─┬─  ┬ ─────┬───── ─────┬──────────
//!  │   │   │      │           │
//!  │   │   │      │           └── Cages (filters, limits)
//!  │   │   │      └── Hooks (columns)
//!  │   │   └── Pivot (connects to table)
//!  │   └── Table name
//!  └── Gate (action)
//! ```

use nom::{
    branch::alt,
    bytes::complete::{tag, take_while, take_while1},
    character::complete::{char, digit1, multispace0},
    combinator::{map, opt, recognize, value},
    multi::many0,
    sequence::{pair, preceded, tuple},
    IResult,
};

use crate::ast::*;
use crate::error::{QailError, QailResult};

/// Parse a complete QAIL query string.
pub fn parse(input: &str) -> QailResult<QailCmd> {
    let input = input.trim();
    
    match parse_qail_cmd(input) {
        Ok(("", cmd)) => Ok(cmd),
        Ok((remaining, _)) => Err(QailError::parse(
            input.len() - remaining.len(),
            format!("Unexpected trailing content: '{}'", remaining),
        )),
        Err(e) => Err(QailError::parse(0, format!("Parse failed: {:?}", e))),
    }
}

/// Parse the complete QAIL command.
fn parse_qail_cmd(input: &str) -> IResult<&str, QailCmd> {
    let (input, action) = parse_action(input)?;
    let (input, _) = tag("::")(input)?;
    let (input, table) = parse_identifier(input)?;
    let (input, _) = opt(tag("•"))(input)?; // Pivot is optional if no columns
    let (input, _) = multispace0(input)?;
    let (input, columns) = parse_columns(input)?;
    let (input, _) = multispace0(input)?;
    let (input, cages) = parse_cages(input)?;

    Ok((
        input,
        QailCmd {
            action,
            table: table.to_string(),
            columns,
            cages,
        },
    ))
}

/// Parse the action (get, set, del, add, gen).
fn parse_action(input: &str) -> IResult<&str, Action> {
    alt((
        value(Action::Get, tag("get")),
        value(Action::Set, tag("set")),
        value(Action::Del, tag("del")),
        value(Action::Add, tag("add")),
        value(Action::Gen, tag("gen")),
    ))(input)
}

/// Parse an identifier (table name, column name).
fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

/// Parse columns (hooks).
fn parse_columns(input: &str) -> IResult<&str, Vec<Column>> {
    many0(preceded(
        char('@'),
        alt((
            value(Column::Star, char('*')),
            map(parse_identifier, |s| Column::Named(s.to_string())),
        )),
    ))(input)
}

/// Parse all cages.
fn parse_cages(input: &str) -> IResult<&str, Vec<Cage>> {
    many0(preceded(multispace0, parse_cage))(input)
}

/// Parse a single cage [...].
fn parse_cage(input: &str) -> IResult<&str, Cage> {
    let (input, _) = char('[')(input)?;
    let (input, _) = multispace0(input)?;
    
    // Check for special cage types
    if let Ok((remaining, cage)) = parse_limit_cage(input) {
        let (remaining, _) = multispace0(remaining)?;
        let (remaining, _) = char(']')(remaining)?;
        return Ok((remaining, cage));
    }
    
    if let Ok((remaining, cage)) = parse_sort_cage(input) {
        let (remaining, _) = multispace0(remaining)?;
        let (remaining, _) = char(']')(remaining)?;
        return Ok((remaining, cage));
    }
    
    // Otherwise, parse as filter conditions
    let (input, (conditions, logical_op)) = parse_conditions(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(']')(input)?;
    
    Ok((
        input,
        Cage {
            kind: CageKind::Filter,
            conditions,
            logical_op,
        },
    ))
}

/// Parse limit cage [lim=N].
fn parse_limit_cage(input: &str) -> IResult<&str, Cage> {
    let (input, _) = tag("lim")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, n) = digit1(input)?;
    
    Ok((
        input,
        Cage {
            kind: CageKind::Limit(n.parse().unwrap_or(10)),
            conditions: vec![],
            logical_op: LogicalOp::And,
        },
    ))
}

/// Parse sort cage [^col] or [^!col].
fn parse_sort_cage(input: &str) -> IResult<&str, Cage> {
    let (input, _) = char('^')(input)?;
    let (input, desc) = opt(char('!'))(input)?;
    let (input, col) = parse_identifier(input)?;
    
    let order = if desc.is_some() {
        SortOrder::Desc
    } else {
        SortOrder::Asc
    };
    
    Ok((
        input,
        Cage {
            kind: CageKind::Sort(order),
            conditions: vec![Condition {
                column: col.to_string(),
                op: Operator::Eq,
                value: Value::Null,
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        },
    ))
}

/// Parse conditions within a cage, returning both conditions and the logical operator.
fn parse_conditions(input: &str) -> IResult<&str, (Vec<Condition>, LogicalOp)> {
    // Parse first condition
    let (input, first) = parse_condition(input)?;
    let mut conditions = vec![first];
    let mut logical_op = LogicalOp::And;
    
    // Parse remaining conditions with their operators
    let mut remaining = input;
    loop {
        // Skip whitespace
        let (input, _) = multispace0(remaining)?;
        
        // Check for operator character
        let first_char = input.chars().next();
        match first_char {
            Some('|') => {
                logical_op = LogicalOp::Or;
                let input = &input[1..]; // consume '|'
                let (input, _) = multispace0(input)?;
                let (input, cond) = parse_condition(input)?;
                conditions.push(cond);
                remaining = input;
            }
            Some('&') => {
                let input = &input[1..]; // consume '&'
                let (input, _) = multispace0(input)?;
                let (input, cond) = parse_condition(input)?;
                conditions.push(cond);
                remaining = input;
            }
            _ => break,
        }
    }
    
    Ok((remaining, (conditions, logical_op)))
}

/// Parse a single condition.
fn parse_condition(input: &str) -> IResult<&str, Condition> {
    let (input, column) = parse_identifier(input)?;
    
    // Check for array unnest syntax: column[*]
    let (input, is_array_unnest) = if input.starts_with("[*]") {
        (&input[3..], true) // consume "[*]"
    } else {
        (input, false)
    };
    
    let (input, _) = multispace0(input)?;
    let (input, (op, value)) = parse_operator_and_value(input)?;
    
    Ok((
        input,
        Condition {
            column: column.to_string(),
            op,
            value,
            is_array_unnest,
        },
    ))
}

/// Parse operator and value together.
fn parse_operator_and_value(input: &str) -> IResult<&str, (Operator, Value)> {
    alt((
        // Fuzzy match: ~value
        map(preceded(char('~'), parse_value), |v| (Operator::Fuzzy, v)),
        // Greater than or equal: >=value
        map(preceded(tag(">="), parse_value), |v| (Operator::Gte, v)),
        // Less than or equal: <=value
        map(preceded(tag("<="), parse_value), |v| (Operator::Lte, v)),
        // Not equal: !=value
        map(preceded(tag("!="), parse_value), |v| (Operator::Ne, v)),
        // Greater than: >value
        map(preceded(char('>'), parse_value), |v| (Operator::Gt, v)),
        // Less than: <value
        map(preceded(char('<'), parse_value), |v| (Operator::Lt, v)),
        // Equal: =value
        map(preceded(char('='), parse_value), |v| (Operator::Eq, v)),
    ))(input)
}

/// Parse a value.
fn parse_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = multispace0(input)?;
    
    alt((
        // Parameter: $1, $2, etc.
        map(preceded(char('$'), digit1), |n: &str| {
            Value::Param(n.parse().unwrap_or(1))
        }),
        // Boolean: true/false
        value(Value::Bool(true), tag("true")),
        value(Value::Bool(false), tag("false")),
        // Function: name()
        map(
            recognize(pair(parse_identifier, tag("()"))),
            |s: &str| Value::Function(s.trim_end_matches("()").to_string()),
        ),
        // Function without parens: now, etc.
        map(tag("now"), |_| Value::Function("now".to_string())),
        // Number (float or int)
        parse_number,
        // Quoted string
        parse_quoted_string,
        // Bare identifier (treated as string)
        map(parse_identifier, |s| Value::String(s.to_string())),
    ))(input)
}

/// Parse a number (integer or float).
fn parse_number(input: &str) -> IResult<&str, Value> {
    let (input, num_str) = recognize(tuple((
        opt(char('-')),
        digit1,
        opt(pair(char('.'), digit1)),
    )))(input)?;
    
    if num_str.contains('.') {
        Ok((input, Value::Float(num_str.parse().unwrap_or(0.0))))
    } else {
        Ok((input, Value::Int(num_str.parse().unwrap_or(0))))
    }
}

/// Parse a quoted string.
fn parse_quoted_string(input: &str) -> IResult<&str, Value> {
    let (input, _) = char('\'')(input)?;
    let (input, content) = take_while(|c| c != '\'')(input)?;
    let (input, _) = char('\'')(input)?;
    
    Ok((input, Value::String(content.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_get() {
        let cmd = parse("get::users•@*").unwrap();
        assert_eq!(cmd.action, Action::Get);
        assert_eq!(cmd.table, "users");
        assert_eq!(cmd.columns, vec![Column::Star]);
    }

    #[test]
    fn test_get_with_columns() {
        let cmd = parse("get::users•@id@email@role").unwrap();
        assert_eq!(cmd.action, Action::Get);
        assert_eq!(cmd.table, "users");
        assert_eq!(
            cmd.columns,
            vec![
                Column::Named("id".to_string()),
                Column::Named("email".to_string()),
                Column::Named("role".to_string()),
            ]
        );
    }

    #[test]
    fn test_get_with_filter() {
        let cmd = parse("get::users•@*[active=true]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].kind, CageKind::Filter);
        assert_eq!(cmd.cages[0].conditions.len(), 1);
        assert_eq!(cmd.cages[0].conditions[0].column, "active");
        assert_eq!(cmd.cages[0].conditions[0].op, Operator::Eq);
        assert_eq!(cmd.cages[0].conditions[0].value, Value::Bool(true));
    }

    #[test]
    fn test_get_with_limit() {
        let cmd = parse("get::users•@*[lim=10]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].kind, CageKind::Limit(10));
    }

    #[test]
    fn test_get_with_sort_desc() {
        let cmd = parse("get::users•@*[^!created_at]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].kind, CageKind::Sort(SortOrder::Desc));
    }

    #[test]
    fn test_set_command() {
        let cmd = parse("set::users•[verified=true][id=$1]").unwrap();
        assert_eq!(cmd.action, Action::Set);
        assert_eq!(cmd.table, "users");
        assert_eq!(cmd.cages.len(), 2);
    }

    #[test]
    fn test_del_command() {
        let cmd = parse("del::sessions•[expired_at<now]").unwrap();
        assert_eq!(cmd.action, Action::Del);
        assert_eq!(cmd.table, "sessions");
    }

    #[test]
    fn test_fuzzy_match() {
        let cmd = parse("get::users•@*[name~$1]").unwrap();
        assert_eq!(cmd.cages[0].conditions[0].op, Operator::Fuzzy);
    }

    #[test]
    fn test_complex_query() {
        let cmd = parse("get::users•@id@email@role[active=true][lim=10]").unwrap();
        assert_eq!(cmd.action, Action::Get);
        assert_eq!(cmd.table, "users");
        assert_eq!(cmd.columns.len(), 3);
        assert_eq!(cmd.cages.len(), 2);
    }

    #[test]
    fn test_param_in_filter() {
        let cmd = parse("get::users•@*[id=$1]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].conditions[0].value, Value::Param(1));
    }

    #[test]
    fn test_param_in_update() {
        let cmd = parse("set::users•[verified=true][id=$1]").unwrap();
        assert_eq!(cmd.action, Action::Set);
        assert_eq!(cmd.cages.len(), 2);
        // Second cage should have the $1 param
        assert_eq!(cmd.cages[1].conditions[0].value, Value::Param(1));
    }
}

