import logging
import sys
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from core.connection import get_pool, close_pool
from tools.query import register_query_tools
from tools.schema import register_schema_tools
from tools.diagnostics import register_diagnostic_tools
from tools.ddl import register_ddl_tools

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("oracle_mcp")


@asynccontextmanager
async def lifespan(app):
    logger.info("Starting Oracle MCP server...")
    await get_pool()
    logger.info("Oracle MCP server ready")
    yield
    logger.info("Shutting down Oracle MCP server...")
    await close_pool()
    logger.info("Oracle MCP server stopped")


mcp = FastMCP("oracle_mcp", lifespan=lifespan)

register_query_tools(mcp)
register_schema_tools(mcp)
register_diagnostic_tools(mcp)
register_ddl_tools(mcp)

if __name__ == "__main__":
    mcp.run()
    # mcp.run(transport="streamable_http", port=8000)
