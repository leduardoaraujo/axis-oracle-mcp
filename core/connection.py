import logging
import os
from typing import Optional

import oracledb

logger = logging.getLogger(__name__)

_pool: Optional[oracledb.AsyncConnectionPool] = None


async def get_pool() -> oracledb.AsyncConnectionPool:
    global _pool
    if _pool is None:
        dsn = os.getenv("ORACLE_DSN")
        user = os.getenv("ORACLE_USER")
        password = os.getenv("ORACLE_PASSWORD")

        if not all([dsn, user, password]):
            raise ValueError(
                "ORACLE_DSN, ORACLE_USER and ORACLE_PASSWORD environment variables must be set.\n"
                "Examples:\n"
                "  ORACLE_DSN=localhost:1521/XEPDB1\n"
                "  ORACLE_USER=myuser\n"
                "  ORACLE_PASSWORD=mypassword\n"
                "\n"
                "Alternatively, set ORACLE_DSN as a full connection string:\n"
                "  ORACLE_DSN=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=host)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=svc)))"
            )

        thick_mode = os.getenv("ORACLE_THICK_MODE", "false").lower() == "true"
        if thick_mode:
            lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR")
            oracledb.init_oracle_client(lib_dir=lib_dir)
            logger.info("Oracle thick mode enabled (Instant Client)")

        min_size = int(os.getenv("ORACLE_POOL_MIN", "1"))
        max_size = int(os.getenv("ORACLE_POOL_MAX", "3"))

        logger.info("Connecting to Oracle (%s)...", dsn)
        _pool = oracledb.create_pool_async(
            user=user,
            password=password,
            dsn=dsn,
            min=min_size,
            max=max_size,
            increment=1,
            getmode=oracledb.POOL_GETMODE_WAIT,
        )
        logger.info("Oracle connection pool created (min=%d, max=%d)", min_size, max_size)
    return _pool


async def close_pool():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Oracle connection pool closed")
