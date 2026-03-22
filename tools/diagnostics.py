import logging
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict
from mcp.server.fastmcp import FastMCP

from core.connection import get_pool
from core.formatters import records_to_dict, format_as_markdown_table, format_as_json

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Input models
# ─────────────────────────────────────────────────────────────────────────────

class ExplainPlanInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    sql: str = Field(
        ...,
        description="SELECT query to explain. Only SELECT is accepted.",
        min_length=1,
        max_length=10_000,
    )
    statement_id: str = Field(
        default="MCP_EXPLAIN",
        description="Identifier tag for the plan entry in PLAN_TABLE.",
        max_length=30,
    )


class ListSessionsInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    status_filter: Optional[str] = Field(
        default=None,
        description="Filter by session status: ACTIVE, INACTIVE, KILLED. If omitted, returns all.",
    )
    username_filter: Optional[str] = Field(
        default=None,
        description="Filter by Oracle username (case-insensitive LIKE pattern, e.g. 'APP%').",
    )
    limit: int = Field(default=50, ge=1, le=500, description="Maximum rows returned.")


class TableStatsInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    table_name: str = Field(..., description="Table name (case-insensitive).", min_length=1)
    owner: Optional[str] = Field(
        default=None,
        description="Schema owner. Defaults to connected user.",
    )


class SearchObjectsInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    pattern: str = Field(
        ...,
        description="LIKE pattern to search across ALL_OBJECTS (e.g. 'SALES%', '%CUSTOMER%').",
        min_length=1,
    )
    object_types: Optional[str] = Field(
        default=None,
        description=(
            "Comma-separated list of object types to include. "
            "E.g. 'TABLE,VIEW,PROCEDURE'. If omitted, searches all types."
        ),
    )
    owner: Optional[str] = Field(
        default=None,
        description="Restrict search to a specific schema owner.",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Registration
# ─────────────────────────────────────────────────────────────────────────────

def register_diagnostic_tools(mcp: FastMCP):

    # ── EXPLAIN PLAN ──────────────────────────────────────────────────────────

    @mcp.tool(
        name="ora_explain_plan",
        annotations={
            "title": "Explain Query Plan",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": False,
        },
    )
    async def ora_explain_plan(params: ExplainPlanInput) -> str:
        """Generates and returns the Oracle execution plan for a SELECT query.

        Uses EXPLAIN PLAN FOR ... and then reads from PLAN_TABLE (or the session
        plan table). Useful for understanding query performance without running it.

        Args:
            params.sql: SELECT query to explain
            params.statement_id: Tag for this plan entry (default: MCP_EXPLAIN)

        Returns:
            str: Markdown table with the execution plan steps
        """
        sql = params.sql.strip()
        first_token = sql.split()[0].upper() if sql.split() else ""
        if first_token != "SELECT":
            raise ValueError("ora_explain_plan only accepts SELECT queries.")

        sid = params.statement_id.upper()

        explain_sql = f"EXPLAIN PLAN SET STATEMENT_ID = '{sid}' FOR {sql}"

        read_plan_sql = """
            SELECT
                ID,
                PARENT_ID,
                LPAD(' ', DEPTH * 2) || OPERATION
                    || CASE WHEN OPTIONS IS NOT NULL THEN ' (' || OPTIONS || ')' ELSE '' END
                    AS OPERATION,
                OBJECT_OWNER || CASE WHEN OBJECT_NAME IS NOT NULL THEN '.' || OBJECT_NAME ELSE '' END
                    AS OBJECT,
                COST,
                CARDINALITY   AS ROWS_EST,
                BYTES,
                ACCESS_PREDICATES,
                FILTER_PREDICATES
            FROM PLAN_TABLE
            WHERE STATEMENT_ID = :sid
            ORDER BY ID
        """

        delete_sql = "DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = :sid"

        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                cur = await conn.cursor()
                # Clean previous entry with same ID
                await cur.execute(delete_sql, {"sid": sid})
                # Generate the plan (this does NOT execute the query)
                await cur.execute(explain_sql)
                # Read the plan
                await cur.execute(read_plan_sql, {"sid": sid})
                columns = [c[0] for c in cur.description]
                rows = await cur.fetchall()
                # Clean up
                await cur.execute(delete_sql, {"sid": sid})
                await conn.commit()
        except Exception as e:
            logger.error("ora_explain_plan failed: %s", e)
            raise

        if not rows:
            return "No execution plan generated. Check if PLAN_TABLE exists and the query is valid."

        data = records_to_dict(columns, rows)
        table = format_as_markdown_table(data)
        return f"## Execution Plan: `{sid}`\n\n{table}\n\n> Tip: high COST or FULL TABLE SCAN on large tables usually signals a missing index."

    # ── ACTIVE SESSIONS ───────────────────────────────────────────────────────

    @mcp.tool(
        name="ora_list_sessions",
        annotations={
            "title": "List Active Sessions",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": False,
        },
    )
    async def ora_list_sessions(params: ListSessionsInput) -> str:
        """Lists current Oracle sessions from V$SESSION.

        Requires SELECT privilege on V$SESSION (usually granted to DBA or via
        a dedicated monitoring role).

        Args:
            params.status_filter: ACTIVE | INACTIVE | KILLED (optional)
            params.username_filter: LIKE pattern for Oracle username (optional)
            params.limit: Max rows (default 50)

        Returns:
            str: Markdown table with session details
        """
        conditions = ["TYPE = 'USER'"]
        bind_vars: dict = {}

        if params.status_filter:
            conditions.append("STATUS = :status")
            bind_vars["status"] = params.status_filter.upper()

        if params.username_filter:
            conditions.append("USERNAME LIKE :uname")
            bind_vars["uname"] = params.username_filter.upper()

        where = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                SID,
                SERIAL#                         AS SERIAL,
                USERNAME,
                STATUS,
                MACHINE,
                PROGRAM,
                MODULE,
                SQL_ID,
                TO_CHAR(LOGON_TIME, 'YYYY-MM-DD HH24:MI:SS') AS LOGON_TIME,
                LAST_CALL_ET                    AS IDLE_SECS
            FROM V$SESSION
            {where}
            ORDER BY LAST_CALL_ET DESC
            FETCH FIRST :lim ROWS ONLY
        """
        bind_vars["lim"] = params.limit

        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                cur = await conn.cursor()
                await cur.execute(sql, bind_vars)
                columns = [c[0] for c in cur.description]
                rows = await cur.fetchall()
        except Exception as e:
            logger.error("ora_list_sessions failed: %s", e)
            raise

        if not rows:
            return "No sessions found matching the given criteria."

        data = records_to_dict(columns, rows)
        table = format_as_markdown_table(data)
        return f"**{len(data)} session(s)**\n\n{table}"

    # ── TABLE STATISTICS ──────────────────────────────────────────────────────

    @mcp.tool(
        name="ora_table_stats",
        annotations={
            "title": "Table Statistics",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def ora_table_stats(params: TableStatsInput) -> str:
        """Returns optimizer statistics and storage details for an Oracle table.

        Includes row count, block count, average row size, last analyzed date,
        partitioning info, and column-level stats (nulls, distinct values, histogram).

        Args:
            params.table_name: Table name
            params.owner: Schema owner (defaults to connected user)

        Returns:
            str: Markdown report with table and column statistics
        """
        table_name = params.table_name.upper()

        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                cur = await conn.cursor()

                if params.owner is None:
                    await cur.execute("SELECT USER FROM DUAL")
                    row = await cur.fetchone()
                    owner = row[0] if row else "UNKNOWN"
                else:
                    owner = params.owner.upper()

                # Table-level stats
                await cur.execute("""
                    SELECT
                        NUM_ROWS,
                        BLOCKS,
                        AVG_ROW_LEN,
                        CHAIN_CNT,
                        PARTITIONED,
                        ROW_MOVEMENT,
                        COMPRESSION,
                        COMPRESS_FOR,
                        TO_CHAR(LAST_ANALYZED, 'YYYY-MM-DD HH24:MI:SS') AS LAST_ANALYZED,
                        MONITORING,
                        DEGREE
                    FROM ALL_TABLES
                    WHERE TABLE_NAME = :tname AND OWNER = :owner
                """, {"tname": table_name, "owner": owner})
                trow = await cur.fetchone()

                # Column-level stats
                await cur.execute("""
                    SELECT
                        COLUMN_NAME,
                        DATA_TYPE,
                        NUM_DISTINCT,
                        NUM_NULLS,
                        DENSITY,
                        NUM_BUCKETS,
                        HISTOGRAM,
                        LOW_VALUE,
                        HIGH_VALUE,
                        AVG_COL_LEN
                    FROM ALL_TAB_COL_STATISTICS
                    WHERE TABLE_NAME = :tname AND OWNER = :owner
                    ORDER BY COLUMN_NAME
                """, {"tname": table_name, "owner": owner})
                col_cols = [c[0] for c in cur.description]
                col_rows = await cur.fetchall()

        except Exception as e:
            logger.error("ora_table_stats failed: %s", e)
            raise

        if trow is None:
            return f"Table `{owner}.{table_name}` not found or not accessible."

        (num_rows, blocks, avg_row_len, chain_cnt, partitioned,
         row_movement, compression, compress_for, last_analyzed,
         monitoring, degree) = trow

        stale_warning = ""
        if last_analyzed is None:
            stale_warning = "\n> ⚠️  Statistics have **never** been collected. Run `DBMS_STATS.GATHER_TABLE_STATS` for accurate plans."

        lines = [
            f"## Table Statistics: `{owner}.{table_name}`\n",
            f"| Property | Value |",
            f"|----------|-------|",
            f"| Rows (estimate) | {num_rows:,} |" if num_rows else "| Rows (estimate) | — |",
            f"| Blocks | {blocks:,} |" if blocks else "| Blocks | — |",
            f"| Avg row length | {avg_row_len} bytes |" if avg_row_len else "| Avg row length | — |",
            f"| Chained rows | {chain_cnt} |" if chain_cnt else "| Chained rows | 0 |",
            f"| Partitioned | {partitioned} |",
            f"| Compression | {compression or 'DISABLED'}{' (' + compress_for + ')' if compress_for else ''} |",
            f"| Last analyzed | {last_analyzed or 'Never'} |",
            f"| Parallel degree | {degree} |",
            stale_warning,
        ]

        if col_rows:
            lines.append("\n### Column Statistics\n")
            lines.append("| Column | Type | Distinct | Nulls | Histogram | Avg Len |")
            lines.append("|--------|------|----------|-------|-----------|---------|")
            for row in col_rows:
                col_name, dtype, ndistinct, nnulls, density, nbuckets, histogram, low, high, avg_len = row
                lines.append(
                    f"| {col_name} | {dtype} | {ndistinct or '—'} | {nnulls or 0} "
                    f"| {histogram or 'NONE'} ({nbuckets or 0} buckets) | {avg_len or '—'} |"
                )

        return "\n".join(lines)

    # ── SEARCH OBJECTS ────────────────────────────────────────────────────────

    @mcp.tool(
        name="ora_search_objects",
        annotations={
            "title": "Search Database Objects",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def ora_search_objects(params: SearchObjectsInput) -> str:
        """Searches for database objects by name pattern across ALL_OBJECTS.

        Useful for discovering tables, views, procedures, sequences, synonyms, etc.
        by partial name match.

        Args:
            params.pattern: LIKE pattern (e.g. 'SALES%', '%CUSTOMER%')
            params.object_types: Comma-separated types (TABLE, VIEW, PROCEDURE, ...)
            params.owner: Restrict to a specific schema owner

        Returns:
            str: Markdown table of matching objects
        """
        conditions = ["OBJECT_NAME LIKE :pattern", "STATUS = 'VALID'"]
        bind_vars: dict = {"pattern": params.pattern.upper()}

        if params.owner:
            conditions.append("OWNER = :owner")
            bind_vars["owner"] = params.owner.upper()

        type_list = None
        if params.object_types:
            type_list = [t.strip().upper() for t in params.object_types.split(",") if t.strip()]

        where = "WHERE " + " AND ".join(conditions)

        # Build IN clause for types if needed (bind vars for IN lists in oracledb)
        type_clause = ""
        if type_list:
            placeholders = ", ".join(f":t{i}" for i in range(len(type_list)))
            type_clause = f"AND OBJECT_TYPE IN ({placeholders})"
            for i, t in enumerate(type_list):
                bind_vars[f"t{i}"] = t

        sql = f"""
            SELECT
                OWNER,
                OBJECT_TYPE,
                OBJECT_NAME,
                TO_CHAR(LAST_DDL_TIME, 'YYYY-MM-DD') AS LAST_MODIFIED
            FROM ALL_OBJECTS
            {where}
            {type_clause}
            ORDER BY OWNER, OBJECT_TYPE, OBJECT_NAME
            FETCH FIRST 200 ROWS ONLY
        """

        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                cur = await conn.cursor()
                await cur.execute(sql, bind_vars)
                columns = [c[0] for c in cur.description]
                rows = await cur.fetchall()
        except Exception as e:
            logger.error("ora_search_objects failed: %s", e)
            raise

        if not rows:
            return f"No objects found matching pattern `{params.pattern}`."

        data = records_to_dict(columns, rows)
        table = format_as_markdown_table(data)
        return f"**{len(data)} object(s) matching `{params.pattern}`**\n\n{table}"
