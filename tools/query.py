import logging
from enum import Enum

from pydantic import BaseModel, Field, ConfigDict
from mcp.server.fastmcp import FastMCP

from core.connection import get_pool
from core.formatters import records_to_dict, format_as_markdown_table, format_as_json

logger = logging.getLogger(__name__)

# Keywords that indicate a write operation — used as a safety net
_WRITE_KEYWORDS = frozenset([
    "INSERT", "UPDATE", "DELETE", "MERGE", "DROP", "CREATE",
    "ALTER", "TRUNCATE", "RENAME", "GRANT", "REVOKE", "COMMIT", "ROLLBACK",
])


class ResponseFormat(str, Enum):
    MARKDOWN = "markdown"
    JSON = "json"


class ExecuteQueryInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    sql: str = Field(
        ...,
        description="SQL SELECT to execute. Only read queries are allowed.",
        min_length=1,
        max_length=10_000,
    )
    limit: int = Field(
        default=100,
        description="Maximum number of rows returned (1–5000)",
        ge=1,
        le=5000,
    )
    format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN,
        description="'markdown' for human-readable output, 'json' for programmatic use",
    )


def _is_write_query(sql: str) -> bool:
    first_token = sql.strip().split()[0].upper() if sql.strip() else ""
    return first_token in _WRITE_KEYWORDS


def register_query_tools(mcp: FastMCP):

    @mcp.tool(
        name="ora_execute_query",
        annotations={
            "title": "Execute SQL Query",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def ora_execute_query(params: ExecuteQueryInput) -> str:
        """Executes a SELECT query on Oracle Database and returns the results.

        Only SELECT queries are accepted. Write operations (INSERT, UPDATE, DELETE,
        DDL, etc.) are rejected both by keyword check and by running the query
        inside a read-only transaction.

        For Oracle-specific syntax reminders:
        - Use FETCH FIRST n ROWS ONLY or ROWNUM for row limiting (this tool wraps
          your query automatically, so you don't need to add it yourself).
        - Date literals: DATE '2024-01-01' or TO_DATE('2024-01-01', 'YYYY-MM-DD')
        - String concat: || operator
        - NVL(col, default) instead of COALESCE (both work on 12c+)

        Args:
            params.sql: Valid SELECT query
            params.limit: Row limit (default 100, max 5000)
            params.format: Output format ('markdown' or 'json')

        Returns:
            str: Results formatted as a Markdown table or JSON
        """
        sql = params.sql.strip()

        if _is_write_query(sql):
            raise ValueError(
                f"Write operations are not allowed. "
                f"Only SELECT queries are accepted. Got: {sql[:80]!r}"
            )

        # Wrap user query to enforce row cap using Oracle 12c+ FETCH syntax
        wrapped_sql = (
            f"SELECT * FROM ({sql}) "
            f"FETCH FIRST {params.limit} ROWS ONLY"
        )

        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                # Disable autocommit and set the transaction to read-only
                conn.autocommit = False
                cursor = await conn.cursor()
                # Oracle read-only transaction
                await cursor.execute("SET TRANSACTION READ ONLY")
                await cursor.execute(wrapped_sql)
                columns = [col[0] for col in cursor.description]
                rows = await cursor.fetchall()
        except Exception as e:
            logger.error("ora_execute_query failed: %s", e)
            raise

        data = records_to_dict(columns, rows)

        if params.format == ResponseFormat.JSON:
            return format_as_json(data)

        result = format_as_markdown_table(data)
        return f"**{len(data)} row(s) returned**\n\n{result}"
