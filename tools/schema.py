import logging
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict
from mcp.server.fastmcp import FastMCP

from core.connection import get_pool

logger = logging.getLogger(__name__)


class ListTablesInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    owner: Optional[str] = Field(
        default=None,
        description=(
            "Filter by Oracle schema owner (e.g. 'HR'). "
            "If omitted, lists tables accessible to the connected user (ALL_TABLES). "
            "Pass 'CURRENT' to list only tables owned by the connected user."
        ),
    )
    table_name_filter: Optional[str] = Field(
        default=None,
        description="Optional LIKE pattern to filter table names (e.g. 'SALES%').",
    )


class DescribeTableInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    table_name: str = Field(..., description="Table name (case-insensitive)", min_length=1)
    owner: Optional[str] = Field(
        default=None,
        description=(
            "Schema owner of the table. "
            "If omitted, the connected user's schema is assumed."
        ),
    )


def register_schema_tools(mcp: FastMCP):

    @mcp.tool(
        name="ora_list_tables",
        annotations={
            "title": "List Tables",
            "readOnlyHint": True,
            "destructiveHint": False,
        },
    )
    async def ora_list_tables(params: ListTablesInput) -> str:
        """Lists tables accessible to the connected user, with owner, status and row count estimate.

        Uses Oracle's ALL_TABLES (all accessible) or USER_TABLES (current user only).

        Args:
            params.owner: Optional schema owner filter (use 'CURRENT' for current user)
            params.table_name_filter: Optional LIKE pattern (e.g. 'SALES%')

        Returns:
            str: Markdown-formatted list of tables
        """
        conditions = []
        bind_vars: dict = {}

        use_user_tables = params.owner and params.owner.upper() == "CURRENT"

        if use_user_tables:
            base_view = "USER_TABLES"
            owner_col = "USER AS OWNER"
        else:
            base_view = "ALL_TABLES"
            owner_col = "OWNER"
            if params.owner:
                conditions.append("OWNER = :owner")
                bind_vars["owner"] = params.owner.upper()

        if params.table_name_filter:
            conditions.append("TABLE_NAME LIKE :tname")
            bind_vars["tname"] = params.table_name_filter.upper()

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        sql = f"""
            SELECT {owner_col}, TABLE_NAME, NUM_ROWS, STATUS
            FROM {base_view}
            {where_clause}
            ORDER BY OWNER, TABLE_NAME
            FETCH FIRST 500 ROWS ONLY
        """

        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                cursor = await conn.cursor()
                await cursor.execute(sql, bind_vars)
                rows = await cursor.fetchall()
        except Exception as e:
            logger.error("ora_list_tables failed: %s", e)
            raise

        if not rows:
            return "No tables found matching the given criteria."

        lines = ["## Available tables\n"]
        for owner, table_name, num_rows, status in rows:
            row_info = f"~{num_rows:,} rows" if num_rows is not None else "row count unavailable"
            status_icon = "✅" if status == "VALID" else "⚠️"
            lines.append(f"- {status_icon} **{owner}.{table_name}** — {row_info}")

        lines.append(f"\n_{len(rows)} table(s) found_")
        return "\n".join(lines)

    @mcp.tool(
        name="ora_describe_table",
        annotations={
            "title": "Describe Table",
            "readOnlyHint": True,
            "destructiveHint": False,
        },
    )
    async def ora_describe_table(params: DescribeTableInput) -> str:
        """Returns the full structure of an Oracle table: columns, types, constraints,
        foreign keys, and indexes.

        Args:
            params.table_name: Name of the table (case-insensitive)
            params.owner: Schema owner (defaults to connected user)

        Returns:
            str: Markdown-formatted table structure
        """
        table_name = params.table_name.upper()
        owner = params.owner.upper() if params.owner else None

        # Resolve owner — if not provided, use current connected user
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:

                if owner is None:
                    cur = await conn.cursor()
                    await cur.execute("SELECT USER FROM DUAL")
                    row = await cur.fetchone()
                    owner = row[0] if row else "UNKNOWN"

                # ── Columns ──────────────────────────────────────────────
                columns_sql = """
                    SELECT
                        c.COLUMN_NAME,
                        c.DATA_TYPE
                            || CASE
                                WHEN c.DATA_TYPE IN ('VARCHAR2','NVARCHAR2','CHAR','NCHAR')
                                    THEN '(' || c.CHAR_LENGTH || ')'
                                WHEN c.DATA_TYPE = 'NUMBER' AND c.DATA_PRECISION IS NOT NULL
                                    THEN '(' || c.DATA_PRECISION
                                         || CASE WHEN c.DATA_SCALE > 0
                                                 THEN ',' || c.DATA_SCALE END || ')'
                                ELSE ''
                               END                         AS DATA_TYPE_FULL,
                        c.NULLABLE,
                        c.DATA_DEFAULT,
                        CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PK' ELSE '' END AS KEY_FLAG
                    FROM ALL_TAB_COLUMNS c
                    LEFT JOIN (
                        SELECT cc.COLUMN_NAME
                        FROM ALL_CONSTRAINTS  con
                        JOIN ALL_CONS_COLUMNS cc
                          ON con.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
                         AND con.OWNER           = cc.OWNER
                        WHERE con.CONSTRAINT_TYPE = 'P'
                          AND con.TABLE_NAME = :tname
                          AND con.OWNER      = :owner
                    ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
                    WHERE c.TABLE_NAME = :tname
                      AND c.OWNER      = :owner
                    ORDER BY c.COLUMN_ID
                """

                # ── Foreign Keys ──────────────────────────────────────────
                fk_sql = """
                    SELECT
                        cc.COLUMN_NAME,
                        rc.OWNER        AS REF_OWNER,
                        rc.TABLE_NAME   AS REF_TABLE,
                        rcc.COLUMN_NAME AS REF_COLUMN
                    FROM ALL_CONSTRAINTS  con
                    JOIN ALL_CONS_COLUMNS cc
                      ON con.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
                     AND con.OWNER           = cc.OWNER
                    JOIN ALL_CONSTRAINTS  rc
                      ON con.R_CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                     AND con.R_OWNER           = rc.OWNER
                    JOIN ALL_CONS_COLUMNS rcc
                      ON rc.CONSTRAINT_NAME = rcc.CONSTRAINT_NAME
                     AND rc.OWNER           = rcc.OWNER
                     AND cc.POSITION        = rcc.POSITION
                    WHERE con.CONSTRAINT_TYPE = 'R'
                      AND con.TABLE_NAME = :tname
                      AND con.OWNER      = :owner
                    ORDER BY cc.POSITION
                """

                # ── Indexes ───────────────────────────────────────────────
                indexes_sql = """
                    SELECT
                        i.INDEX_NAME,
                        i.INDEX_TYPE,
                        i.UNIQUENESS,
                        LISTAGG(ic.COLUMN_NAME, ', ')
                            WITHIN GROUP (ORDER BY ic.COLUMN_POSITION) AS COLUMNS
                    FROM ALL_INDEXES    i
                    JOIN ALL_IND_COLUMNS ic
                      ON i.INDEX_NAME = ic.INDEX_NAME
                     AND i.OWNER      = ic.INDEX_OWNER
                    WHERE i.TABLE_NAME  = :tname
                      AND i.TABLE_OWNER = :owner
                    GROUP BY i.INDEX_NAME, i.INDEX_TYPE, i.UNIQUENESS
                    ORDER BY i.INDEX_NAME
                """

                bind = {"tname": table_name, "owner": owner}

                cur = await conn.cursor()

                await cur.execute(columns_sql, bind)
                columns = await cur.fetchall()

                await cur.execute(fk_sql, bind)
                fks = await cur.fetchall()

                await cur.execute(indexes_sql, bind)
                indexes = await cur.fetchall()

        except Exception as e:
            logger.error("ora_describe_table failed: %s", e)
            raise

        if not columns:
            return f"Table `{owner}.{table_name}` not found or not accessible."

        lines = [
            f"## Structure: `{owner}.{table_name}`\n",
            "| Column | Type | Nullable | Default | Key |",
            "|--------|------|----------|---------|-----|",
        ]
        for col_name, data_type, nullable, default, key_flag in columns:
            lines.append(
                f"| {col_name} | {data_type} | {nullable} "
                f"| {(default or '').strip()} | {key_flag} |"
            )

        if fks:
            lines.append("\n### Foreign Keys\n")
            for col, ref_owner, ref_table, ref_col in fks:
                lines.append(f"- `{col}` → `{ref_owner}.{ref_table}.{ref_col}`")

        if indexes:
            lines.append("\n### Indexes\n")
            for idx_name, idx_type, uniqueness, idx_cols in indexes:
                flag = " (UNIQUE)" if uniqueness == "UNIQUE" else ""
                lines.append(f"- **{idx_name}** [{idx_type}] on ({idx_cols}){flag}")

        return "\n".join(lines)

    @mcp.tool(
        name="ora_list_procedures",
        annotations={
            "title": "List Stored Procedures and Functions",
            "readOnlyHint": True,
            "destructiveHint": False,
        },
    )
    async def ora_list_procedures(
        owner: Optional[str] = None,
        object_type_filter: Optional[str] = None,
    ) -> str:
        """Lists stored procedures, functions, packages and triggers accessible to the user.

        Args:
            owner: Optional schema owner (defaults to all accessible objects)
            object_type_filter: Filter by type: PROCEDURE, FUNCTION, PACKAGE, TRIGGER

        Returns:
            str: Markdown-formatted list of objects
        """
        conditions = ["STATUS = 'VALID'"]
        bind_vars: dict = {}

        if owner:
            conditions.append("OWNER = :owner")
            bind_vars["owner"] = owner.upper()

        if object_type_filter:
            conditions.append("OBJECT_TYPE = :otype")
            bind_vars["otype"] = object_type_filter.upper()
        else:
            bind_vars_types = "PROCEDURE, FUNCTION, PACKAGE, TRIGGER"
            conditions.append(
                "OBJECT_TYPE IN ('PROCEDURE', 'FUNCTION', 'PACKAGE', 'TRIGGER')"
            )

        where = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT OWNER, OBJECT_TYPE, OBJECT_NAME, LAST_DDL_TIME
            FROM ALL_OBJECTS
            {where}
            ORDER BY OWNER, OBJECT_TYPE, OBJECT_NAME
            FETCH FIRST 500 ROWS ONLY
        """

        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                cursor = await conn.cursor()
                await cursor.execute(sql, bind_vars)
                rows = await cursor.fetchall()
        except Exception as e:
            logger.error("ora_list_procedures failed: %s", e)
            raise

        if not rows:
            return "No objects found matching the given criteria."

        lines = ["## Stored Objects\n"]
        current_type = None
        for obj_owner, obj_type, obj_name, last_ddl in rows:
            if obj_type != current_type:
                lines.append(f"\n### {obj_type}s\n")
                current_type = obj_type
            ddl_str = f" _(last modified: {last_ddl.strftime('%Y-%m-%d')})_" if last_ddl else ""
            lines.append(f"- **{obj_owner}.{obj_name}**{ddl_str}")

        lines.append(f"\n_{len(rows)} object(s) found_")
        return "\n".join(lines)
