"""
Neo4j Dynamic Row Loader
========================
Implements idempotent graph ingestion matching the official handout model:
(:Dataset {id, filename, uploaded_at})-[:HAS_ROW]->(:Row {dataset_id, row_index, ...})

Guarantees:
- Every row is written using MERGE with stable identifier (dataset_id + row_index).
- Never uses CREATE for row ingestion.
- The same CSV loaded twice does NOT create duplicate nodes.
"""

import time
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("zyntax.neo4j_loader")

# Cypher statement enforcing strict idempotency and schema independence
ROW_MERGE_CYPHER = """
MERGE (d:Dataset {id: $dataset_id})
ON CREATE SET d.filename = $filename, d.uploaded_at = $uploaded_at
WITH d
MERGE (r:Row {dataset_id: $dataset_id, row_index: $row_index})
SET r += $row_data
MERGE (d)-[:HAS_ROW]->(r)
RETURN count(r) AS rows_merged
"""


def merge_row_into_graph(
    driver: Any,
    dataset_id: str,
    filename: str,
    row_index: int,
    row_data: Dict[str, Any],
    database: Optional[str] = "CSV_Graph_DB"
) -> bool:
    """
    Executes an idempotent MERGE operation for a single row into Neo4j.
    Handles dynamic properties from unknown CSV headers.
    """
    if driver is None:
        logger.error("Cannot merge row: Neo4j driver is None.")
        return False

    params = {
        "dataset_id": str(dataset_id),
        "filename": str(filename),
        "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "row_index": int(row_index),
        "row_data": row_data
    }

    target_dbs = [database, None] if database else [None]
    last_err = None

    for db in target_dbs:
        try:
            session_kwargs = {"database": db} if db else {}
            with driver.session(**session_kwargs) as session:
                res = session.run(ROW_MERGE_CYPHER, **params)
                summary = res.consume() if hasattr(res, "consume") else None
                return True
        except Exception as e:
            last_err = e
            logger.debug(f"Attempt to write to db '{db}' encountered: {e}")
            continue

    logger.error(f"Failed to merge row ({dataset_id}, row {row_index}) into Neo4j: {last_err}")
    return False
