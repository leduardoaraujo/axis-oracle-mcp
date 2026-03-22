# axis-oracle-mcp

MCP server for Oracle Database, based on the architecture of [axis-postgres-mcp](https://github.com/leduardoaraujo/axis-postgres-mcp).

Allows LLMs (Claude, Cursor, etc.) to safely query and inspect Oracle databases — read-only.

---

## Available Tools

### Query
| Tool | Description |
|---|---|
| `ora_execute_query` | Executes a SELECT and returns results in Markdown or JSON |

### Schema / Metadata
| Tool | Description |
|---|---|
| `ora_list_tables` | Lists accessible tables with owner, status, and row count estimate |
| `ora_describe_table` | Describes columns, FKs, and indexes of a table |
| `ora_list_procedures` | Lists procedures, functions, packages, and triggers |
| `ora_search_objects` | Searches objects by name pattern (LIKE) in ALL_OBJECTS |

### DDL / Source Code
| Tool | Description |
|---|---|
| `ora_get_ddl` | Returns the DDL (CREATE ...) via DBMS_METADATA |
| `ora_get_source` | Returns the PL/SQL source code of a procedure/function/package via ALL_SOURCE |

### Diagnostics
| Tool | Description |
|---|---|
| `ora_explain_plan` | Generates and displays the execution plan for a SELECT |
| `ora_list_sessions` | Lists active sessions in V$SESSION |
| `ora_table_stats` | Optimizer statistics: rows, blocks, columns, histograms |

---

## Installation

```bash
git clone https://github.com/your-username/axis-oracle-mcp.git
cd axis-oracle-mcp

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

---

## Configuration

Copy the example file and fill in your credentials:

```bash
cp envexample.txt .env
```

Required variables:

```env
ORACLE_DSN=localhost:1521/XEPDB1
ORACLE_USER=readonly_user
ORACLE_PASSWORD=secret
```

### Thick Mode (Oracle Instant Client)

Required for Oracle databases < 12.2 or to use advanced types (BLOB, AQ, etc.):

```env
ORACLE_THICK_MODE=true
ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_21_9
```

---

## Running

```bash
python server.py
```

Or with HTTP (for remote use):

```python
# In server.py, replace the last line with:
mcp.run(transport="streamable_http", port=8000)
```

---

## Configure in Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "oracle": {
      "command": "python",
      "args": ["/path/to/axis-oracle-mcp/server.py"],
      "env": {
        "ORACLE_DSN": "localhost:1521/XEPDB1",
        "ORACLE_USER": "readonly_user",
        "ORACLE_PASSWORD": "secret"
      }
    }
  }
}
```

---

## Configure in Cursor / VS Code

In the project's `mcp.json`:

```json
{
  "mcpServers": {
    "oracle": {
      "command": "python",
      "args": ["server.py"],
      "cwd": "/path/to/axis-oracle-mcp"
    }
  }
}
```

---

## Security

- All queries run inside a `SET TRANSACTION READ ONLY` transaction
- Write keywords (INSERT, UPDATE, DELETE, DDL...) are blocked before execution
- It is recommended to connect with an Oracle user that has only `SELECT` privilege on the desired tables

Read-only user example:

```sql
CREATE USER readonly_user IDENTIFIED BY secret;
GRANT CREATE SESSION TO readonly_user;
GRANT SELECT ANY TABLE TO readonly_user;
-- Or, more restrictive:
GRANT SELECT ON hr.employees TO readonly_user;
```

---

## Differences from axis-postgres-mcp

| Aspect | PostgreSQL | Oracle |
|---|---|---|
| Driver | `asyncpg` | `oracledb` |
| DSN | URI (`postgresql://...`) | Easy Connect / TNS |
| Metadata | `information_schema` | `ALL_TABLES`, `ALL_TAB_COLUMNS`, `ALL_CONSTRAINTS` |
| Row limit | `LIMIT n` | `FETCH FIRST n ROWS ONLY` |
| Read-only TX | `SET TRANSACTION READ ONLY` | `SET TRANSACTION READ ONLY` |
| Schema | `schema_name` (namespace) | `owner` (Oracle user) |
| Extras | — | `ora_list_procedures` for PL/SQL objects |
