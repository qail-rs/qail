# 🪝 QAIL — The Horizontal Query Language

> **Stop writing strings. Hook your data.**

[![Crates.io](https://img.shields.io/badge/crates.io-qail-orange)](https://crates.io/crates/qail)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75+-blueviolet)](https://www.rust-lang.org/)

---

## The Manifesto

SQL is **vertical**, verbose, and clunky inside modern codebases.

QAIL is **horizontal**, dense, and composable. It treats database queries like a pipeline, using symbols to **hook** data and pull it into your application.

```sql
-- The Old Way (SQL)
SELECT id, email, role FROM users WHERE active = true LIMIT 1;
```

```bash
# The QAIL Way
get::users•@id@email@role[active=true][lim=1]
```

One line. Zero ceremony. **Maximum velocity.**

### The Philosophy

1.  **Constraint**: Vertical space is precious. SQL blocks interrupt the flow of code reading. QAIL flows *with* your logic.
2.  **Density**: Symbols (`@`, `•`, `[]`) convey more information per pixel than keywords (`SELECT`, `FROM`, `WHERE`).
3.  **The Star Rule**: If you need 50 columns, fetch the struct (`@*`). If you need 3, list them. Listing 20 columns manually is an anti-pattern. QAIL encourages "all or nothing" density.

**Is it still Horizontal?**
Yes. The *language* itself is horizontal because it uses symbols instead of keywords. But we give you the **Vertical Escape Hatch** (tabs/newlines) so you can organize complex logic however you see fit, without fighting the parser. Horizontal is the *identity*; Vertical is the *layout*.

---

## 📖 Quick Reference

| Symbol | Name       | Function                | SQL Equivalent           |
|--------|------------|-------------------------|--------------------------|
| `::`   | The Gate   | Defines the action      | `SELECT`, `INSERT`, `UPDATE` |
| `!`    | The Unique | Distinct modifier       | `SELECT DISTINCT`        |
| `•`    | The Pivot  | Connects action to table| `FROM table`             |
| `@`    | The Hook   | Selects specific columns| `col1, col2`             |
| `[]`   | The Cage   | Constraints & Filters   | `WHERE`, `LIMIT`, `SET`  |
| `->`   | The Link   | Inner Join              | `INNER JOIN`             |
| `<-`   | The Left   | Left Join               | `LEFT JOIN`              |
| `->>`  | The Right  | Right Join              | `RIGHT JOIN`             |
| `~`    | The Fuse   | Fuzzy / Partial Match   | `ILIKE '%val%'`          |
| `\|`   | The Split  | Logical OR              | `OR`                     |
| `&`    | The Bind   | Logical AND             | `AND`                    |
| `^!`   | The Peak   | Sort Descending         | `ORDER BY ... DESC`      |
| `^`    | The Rise   | Sort Ascending          | `ORDER BY ... ASC`       |
| `*`    | The Star   | All / Wildcard          | `*`                      |
| `[*]`  | The Deep   | Array Unnest            | `UNNEST(arr)`            |
| `$`    | The Var    | Parameter Injection     | `$1`, `$2`               |
| `lim=` | The Limit  | Row limit               | `LIMIT n`                |
| `off=` | The Skip   | Offset for pagination   | `OFFSET n`               |

---

## 🚀 Installation

### CLI (Recommended)

```bash
cargo install qail
```

### As a Library

```toml
# Cargo.toml
[dependencies]
qail = "0.5.0-alpha"
```

---

## 💡 Usage

### CLI — The `qail` Command

```bash
# Fetch all users
qail 'get::users•@*'

# Get specific columns with filter
qail 'get::orders•@id@total@status[user_id=$1][lim=10]' --bind 42

# Update a record
qail 'set::users•[verified=true][id=$1]' --bind 7

# Delete with condition
qail 'del::sessions•[expired_at<now]'

# Transpile only (don't execute)
qail 'get::users•@*[active=true]' --dry-run
```

### As a Library

```rust
use qail::prelude::*;

#[tokio::main]
async fn main() -> Result<(), QailError> {
    let db = QailDB::connect("postgres://localhost/mydb").await?;

    // Parse and execute
    let users: Vec<User> = db
        .query("get::users•@id@email@role[active=true][lim=10]")
        .fetch_all()
        .await?;

    // Or use the builder for type-safe composition
    let query = qail::get("users")
        .hook(&["id", "email", "role"])
        .cage("active", true)
        .limit(10);

    let users: Vec<User> = db.run(query).fetch_all().await?;

    Ok(())
}
```

---

## 📚 Syntax Deep Dive

### A. Simple Fetch (`get::`)

```sql
-- SQL
SELECT id, email, role FROM users WHERE active = true LIMIT 1;
```

```bash
# QAIL
get::users•@id@email@role[active=true][lim=1]
```

---

### B. Mutation (`set::`)

```sql
-- SQL
UPDATE user_verifications SET consumed_at = now() WHERE id = $1;
```

```bash
# QAIL
set::user_verifications•[consumed_at=now][id=$1]
```

> **Note:** In `set::` mode, the **first `[]`** is the payload (SET), the **second `[]`** is the filter (WHERE).

---

### C. Deletion (`del::`)

```sql
-- SQL
DELETE FROM sessions WHERE expired_at < now();
```

```bash
# QAIL
del::sessions•[expired_at<now]
```

---

### D. Complex Search with Fuzzy Match

```sql
-- SQL
SELECT * FROM ai_knowledge_base 
WHERE active = true 
AND (topic ILIKE $1 OR question ILIKE $1 OR EXISTS (SELECT 1 FROM unnest(keywords) k WHERE k ILIKE $1))
ORDER BY created_at DESC
LIMIT 5;
```

```bash
# QAIL
get::ai_knowledge_base•@*[active=true][topic~$1|question~$1|keywords[*]~$1][^!created_at][lim=5]

# Or multi-line for readability:
get::ai_knowledge_base•@*
  [active=true]
  [topic~$1 | question~$1 | keywords[*]~$1]
  [^!created_at]
  [lim=5]
```

---

### E. Joins

```bash
# Inner join (default)
get::users->orders•@name@total
# → SELECT name, total FROM users INNER JOIN orders ON orders.user_id = users.id

# Left join (include users without orders)
get::users<-orders•@name@total
# → SELECT name, total FROM users LEFT JOIN orders ON orders.user_id = users.id

# Right join
get::orders->>customers•@*
# → SELECT * FROM orders RIGHT JOIN customers ON customers.order_id = orders.id
```

---

### F. DISTINCT Queries (v0.5+)

```bash
# Get unique roles
get!::users•@role
# → SELECT DISTINCT role FROM users

# Distinct with filter
get!::orders•@status[created_at>'2024-01-01']
# → SELECT DISTINCT status FROM orders WHERE created_at > '2024-01-01'
```

---

### G. Pagination (OFFSET)

```bash
# Page 3 (20 items per page)
get::products•@*[lim=20][off=40]
# → SELECT * FROM products LIMIT 20 OFFSET 40
```

---

## ⚙️ Configuration

Create a `.qailrc` or `qail.toml` in your project root:

```toml
[connection]
driver = "postgres"           # postgres | mysql | sqlite
url = "postgres://localhost/mydb"

[output]
format = "table"              # table | json | csv
color = true

[safety]
confirm_mutations = true      # Prompt before UPDATE/DELETE
dry_run_default = false
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      QAIL Pipeline                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   "get::users•@*[active=true]"                              │
│              │                                              │
│              ▼                                              │
│   ┌─────────────────────┐                                   │
│   │   Parser (nom)      │  → Tokenize symbols               │
│   └─────────┬───────────┘                                   │
│             │                                               │
│             ▼                                               │
│   ┌─────────────────────┐                                   │
│   │   AST (QailCmd)     │  → Structured representation      │
│   └─────────┬───────────┘                                   │
│             │                                               │
│             ▼                                               │
│   ┌─────────────────────┐                                   │
│   │ Transpiler (SQL)    │  → Generate valid SQL             │
│   └─────────┬───────────┘                                   │
│             │                                               │
│             ▼                                               │
│   ┌─────────────────────┐                                   │
│   │ Engine (sqlx)       │  → Execute against DB             │
│   └─────────────────────┘                                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Core Structs

```rust
pub struct QailCmd {
    pub action: Action,         // GET, SET, DEL, ADD
    pub table: String,
    pub columns: Vec<Column>,
    pub cages: Vec<Cage>,       // Filters, limits, sorts
    pub bindings: Vec<Value>,
}

pub enum Action {
    Get,    // SELECT
    Set,    // UPDATE
    Del,    // DELETE
    Add,    // INSERT
}

pub struct Cage {
    pub kind: CageKind,         // Filter, Limit, Sort, Payload
    pub conditions: Vec<Condition>,
}

pub struct Condition {
    pub column: String,
    pub op: Operator,           // Eq, Ne, Gt, Lt, Fuzzy, In
    pub value: Value,
}
```

---

## 🗺️ Roadmap

### Phase 1: Parser ✅
- [x] Lexer for QAIL symbols
- [x] `nom` parser combinators
- [x] AST generation

### Phase 2: Transpiler ✅
- [x] PostgreSQL codegen
- [x] MySQL codegen
- [x] SQLite codegen
- [x] JOINs (INNER, LEFT, RIGHT)
- [x] DISTINCT, OFFSET, RETURNING

### Phase 3: Engine ✅
- [x] Async execution (sqlx)
- [x] Connection pooling
- [x] Multi-driver support (Postgres/MySQL/SQLite)
- [x] Transaction support (`begin()`, `commit()`, `rollback()`)
- [x] Prepared statement caching (`StatementCache`)

### Phase 4: Ecosystem ✅
- [x] VS Code extension (syntax highlighting)
- [x] `qail!` compile-time macro
- [x] Struct generation (`gen::`)
- [x] REPL mode (`qail repl`)
- [x] Language server (`qail-lsp`)

### E. The Flagship Comparison (Complex Joins)

**Scenario**: Find verified users who joined after 2024 and booked under the 'SUMMER' campaign.

```sql
-- SQL (7 lines, cognitive load high)
SELECT u.* 
FROM users u
JOIN bookings b ON b.user_id = u.id
WHERE u.created_at >= '2024-01-01'
  AND u.email_verified = true
  AND b.campaign_code ILIKE '%SUMMER%'
ORDER BY u.created_at DESC
LIMIT 50;
```

```bash
# QAIL (1 line, cognitive load low)
get::users->bookings•@*[created_at>='2024-01-01'][email_verified=true][bookings.campaign_code~'SUMMER'][^!created_at][lim=50]
```

---

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

```bash
# Clone the repo
git clone https://github.com/your-username/qail.git
cd qail

# Run tests
cargo test

# Run with example
cargo run -- "get::users•@*[lim=5]" --dry-run
```

---

## 📄 License

MIT © 2025 QAIL Contributors

---

<p align="center">
  <strong>Built with 🦀 Rust and ☕ caffeine</strong><br>
  <a href="https://qail.rs">qail.rs</a>
</p>
