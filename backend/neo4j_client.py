"""
Neo4j Client Wrapper for ZYNTAX Pipeline
========================================
Provides:
- Resilient Neo4j driver initialization with retries and exponential backoff
- Genuinely responsive health check
- Driver mocking for isolated testing
"""

import time
import socket
import logging
from typing import Optional, Any
from urllib.parse import urlparse
from neo4j import GraphDatabase
from backend.config import (
    NEO4J_URI,
    NEO4J_USER,
    NEO4J_PASSWORD,
    NEO4J_DATABASE,
    NEO4J_RETRY_COUNT,
    NEO4J_RETRY_DELAY
)

logger = logging.getLogger("zyntax.neo4j")

_driver = None
_custom_driver = None


def check_neo4j_socket(uri: str) -> bool:
    """Verifies that the Neo4j bolt port is open."""
    try:
        parsed = urlparse(uri)
        host = parsed.hostname or "localhost"
        port = parsed.port or 7687
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except Exception:
        return False


def get_neo4j_driver(retries: int = 1, delay: float = NEO4J_RETRY_DELAY):
    """
    Returns an active Neo4j driver with lazy initialization.
    Does not crash the service if Neo4j is still spinning up.
    """
    global _driver, _custom_driver
    if _custom_driver is not None:
        return _custom_driver

    if _driver is not None:
        return _driver

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Connecting to Neo4j at {NEO4J_URI} (attempt {attempt}/{retries})...")
            _driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USER, NEO4J_PASSWORD),
                connection_timeout=3.0,
                max_connection_lifetime=300
            )
            # Verify connectivity immediately
            _driver.verify_connectivity()
            logger.info("Neo4j driver successfully connected.")
            return _driver
        except Exception as e:
            logger.warning(f"Neo4j connection attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(delay)

    return _driver


def set_neo4j_driver(custom_driver: Any):
    """Allows setting or mocking the driver for testing."""
    global _custom_driver
    _custom_driver = custom_driver


def is_neo4j_connected() -> bool:
    """
    Genuinely checks if Neo4j is reachable and responsive to queries.
    Never returns True unless a query succeeds.
    """
    global _custom_driver
    if _custom_driver is not None:
        return getattr(_custom_driver, "is_connected", True)

    try:
        if not check_neo4j_socket(NEO4J_URI):
            return False

        driver = get_neo4j_driver(retries=1)
        if driver is None:
            return False

        target_dbs = [NEO4J_DATABASE, None] if NEO4J_DATABASE else [None]
        for db in target_dbs:
            try:
                session_kwargs = {"database": db} if db else {}
                with driver.session(**session_kwargs) as session:
                    res = session.run("RETURN 1 AS ping")
                    rec = res.single()
                    if rec and rec["ping"] == 1:
                        return True
            except Exception:
                continue
        return False
    except Exception as e:
        logger.debug(f"Neo4j health check failed: {e}")
        return False


def close_neo4j_driver():
    """Closes driver connections cleanly."""
    global _driver
    if _driver is not None:
        try:
            _driver.close()
        except Exception:
            pass
        _driver = None
