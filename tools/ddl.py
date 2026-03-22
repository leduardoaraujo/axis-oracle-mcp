import logging
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict
from mcp.server.fastmcp import FastMCP

from core.connection import get_pool

logger = logging.getLogger(__name__)

_SUPPORTED_TYPES = frozenset([
    "TABLE", "VIEW", "PROCEDURE", "FUNCTION", "PACKAGE",
    "PACKAGE BODY", "TRIGGER", "TYPE", "SEQUENCE", "SYNONYM", "INDEX",
])


class GetDDLInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    object_name: str = Field(..., description="Object name (case-insensitive).", min_length=1)
    object_type: str = Field(
        ...,
        description=(
            "Object type. Supported: TABLE, VIEW, PROCEDURE, FUNCTION, PACKAGE, "
            "PACKAGE BODY, TRIGGER, TYPE, SEQUENCE, SYNONYM, INDEX."
        ),
    )
    owner: Optional[str] = Field(
        default=None,
        description="Schema owner. Defaults to connected user.",
    )


class GetSourceInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    object_name: str = Field(..., description="PL/SQL object name (case-insensitive).", min_length=1)
    object_type: str = Field(
        ...,
        description="PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TRIGGER, or TYPE.",
    )
    owner: Optional[str] = Field(
        default=None,
        description="Schema owner. Defaults to connected user.",
    )


def register_ddl_tools(mcp: FastMCP):

    @mcp.tool(
        name="ora_get_ddl",
        annotations={
            "title": "Get Object DDL",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def ora_get_ddl(params: GetDDLInput) -> str:
        """Returns the DDL (CREATE statement) for a database object using DBMS_METADATA.

        Works for TABLE, VIEW, PROCEDURE, FUNCTION, PACKAGE, TRIGGER, TYPE,
        SEQUENCE, SYNONYM, and INDEX.

        Requires EXECUTE privilege on DBMS_METADATA or SELECT_CATALOG_ROLE.

        Args:
            params.object_name: Name of the object
            params.object_type: Type of the object (e.g. TABLE, VIEW, PROCEDURE)
            params.owner: Schema owner (defaults to connected user)

        Returns:
            str: DDL as a fenced SQL code block
        """
        obj_name = params.object_name.upper()
        obj_type = params.object_type.upper()

        if obj_type not in _SUPPORTED_TYPES:
            raise ValueError(
                f"Unsupported object type: {obj_type!r}. "
                f"Supported types: {', '.join(sorted(_SUPPORTED_TYPES))}"
            )

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

                # DBMS_METADATA.GET_DDL returns a CLOB
                await cur.execute(
                    """
                    SELECT DBMS_METADATA.GET_DDL(
                        :obj_type,
                        :obj_name,
                        :owner
                    ) FROM DUAL
                    """,
                    {"obj_type": obj_type, "obj_name": obj_name, "owner": owner},
                )
                row = await cur.fetchone()

        except Exception as e:
            logger.error("ora_get_ddl failed: %s", e)
            raise

        if row is None or row[0] is None:
            return f"No DDL found for `{owner}.{obj_name}` (type: {obj_type})."

        ddl_text = row[0].read() if hasattr(row[0], "read") else str(row[0])
        ddl_text = ddl_text.strip()

        return f"## DDL: `{owner}.{obj_name}` ({obj_type})\n\n```sql\n{ddl_text}\n```"

    @mcp.tool(
        name="ora_get_source",
        annotations={
            "title": "Get PL/SQL Source",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def ora_get_source(params: GetSourceInput) -> str:
        """Returns the PL/SQL source code of a stored object from ALL_SOURCE.

        Unlike ora_get_ddl, this reads directly from ALL_SOURCE — no special
        privileges required beyond access to the object itself.

        Useful for inspecting procedure/function/package logic.

        Args:
            params.object_name: Name of the PL/SQL object
            params.object_type: PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TRIGGER, TYPE
            params.owner: Schema owner (defaults to connected user)

        Returns:
            str: Source code as a fenced PL/SQL code block
        """
        obj_name = params.object_name.upper()
        obj_type = params.object_type.upper()

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

                await cur.execute(
                    """
                    SELECT TEXT
                    FROM ALL_SOURCE
                    WHERE OWNER       = :owner
                      AND NAME        = :obj_name
                      AND TYPE        = :obj_type
                    ORDER BY LINE
                    """,
                    {"owner": owner, "obj_name": obj_name, "obj_type": obj_type},
                )
                rows = await cur.fetchall()

        except Exception as e:
            logger.error("ora_get_source failed: %s", e)
            raise

        if not rows:
            return (
                f"No source found for `{owner}.{obj_name}` (type: {obj_type}). "
                f"Check that the object exists and the owner is correct."
            )

        source = "".join(r[0] for r in rows)
        return f"## Source: `{owner}.{obj_name}` ({obj_type})\n\n```plsql\n{source.rstrip()}\n```"
