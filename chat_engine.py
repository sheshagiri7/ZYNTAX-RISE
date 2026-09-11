"""
ZYNTAX Chat Engine (Member 3 - AI / Chatbot / Integration)
===========================================================
Official Hackathon Implementation for RISE @ RST #5.

Architecture & Responsibilities:
--------------------------------
1. Question -> Cypher Mapping:
   - Inspects live graph schema (Row properties & Datasets) from Neo4j.
   - Maps user questions to deterministic, parameter-safe Cypher queries
     (counts, column filters, distinct listings, aggregations, schema summaries).
   - Case-insensitive fuzzy matching for dynamic CSV column names.

2. Grounding & Anti-Hallucination:
   - Validates that requested properties actually exist in the graph before querying.
   - Rejects general knowledge / irrelevant questions (returns grounded=False).
   - Detects empty graph state (pre-upload) safely without crashing.
   - Strictly reports facts directly from the query result; never fabricates numbers.

3. Read-Only Safety:
   - Strict blocklist rejects mutating keywords (CREATE, MERGE, DELETE, SET,
     DROP, ALTER, LOAD CSV, APOC write procedures).
   - Only permits read-only MATCH / WHERE / RETURN / ORDER BY / LIMIT queries.

4. Contract Specification (Part 4 of Handout):
   POST /chat
   Request:  {"question": "How many rows belong to the Billing group?"}
   Response: {
     "answer": "There are 128 rows where group = 'Billing'.",
     "cypher": "MATCH (r:Row {group: 'Billing'}) RETURN count(r)",
     "result": [{"count(r)": 128}],
     "grounded": true
   }
"""

import re
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("zyntax.chat_engine")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------------
# 1. READ-ONLY SAFETY GUARDRAILS
# -----------------------------------------------------------------------------

FORBIDDEN_CYPHER_TERMS = {
    "CREATE", "MERGE", "DELETE", "DETACH", "SET", "REMOVE", "DROP",
    "ALTER", "TRUNCATE", "LOAD CSV", "PERIODIC COMMIT", "CALL APOC.EXPORT",
    "CALL APOC.PERIODIC", "APOC.CYPHER.DOIT", "DBMS.", "GRANT", "REVOKE"
}

def is_safe_read_only_cypher(cypher: str) -> bool:
    """
    Ensures Cypher query is strictly read-only and does not contain mutating commands
    or multi-statement injection attempts.
    """
    if not cypher or not isinstance(cypher, str):
        return False
    
    # Disallow multiple statements
    cleaned = cypher.strip().rstrip(";")
    if ";" in cleaned:
        return False
    
    # Tokenize and check against forbidden keywords
    upper_query = cleaned.upper()
    for forbidden in FORBIDDEN_CYPHER_TERMS:
        # Match as whole word or phrase
        pattern = r"\b" + re.escape(forbidden) + r"\b"
        if re.search(pattern, upper_query):
            logger.warning(f"Blocked unsafe Cypher query containing: {forbidden}")
            return False
            
    # Must start with safe read-only operations
    safe_starts = ("MATCH", "OPTIONAL MATCH", "CYPHER", "RETURN", "WITH", "CALL DB.")
    return any(upper_query.startswith(prefix) for prefix in safe_starts)


# -----------------------------------------------------------------------------
# 2. SCHEMA & GRAPH INTROSPECTION
# -----------------------------------------------------------------------------

def get_graph_schema(driver: Any, database: Optional[str] = "CSV_Graph_DB") -> Dict[str, Any]:
    """
    Queries Neo4j to inspect:
    - Total row count
    - Unique property keys present on (:Row) nodes
    - Available (:Dataset) nodes
    """
    schema = {
        "empty": True,
        "total_rows": 0,
        "properties": [],
        "property_map": {},  # lowercase -> actual property name
        "datasets": []
    }
    
    if driver is None:
        return schema
        
    try:
        # Session parameters (fallback if database is not configured/custom)
        session_kwargs = {}
        if database:
            session_kwargs["database"] = database
            
        with driver.session(**session_kwargs) as session:
            # 1. Count total rows
            row_count_res = session.run("MATCH (r:Row) RETURN count(r) AS total")
            record = row_count_res.single()
            total_rows = record["total"] if record else 0
            schema["total_rows"] = total_rows
            
            if total_rows == 0:
                schema["empty"] = True
                return schema
                
            schema["empty"] = False
            
            # 2. Extract property keys from sample rows
            keys_res = session.run("MATCH (r:Row) RETURN keys(r) AS keys LIMIT 50")
            prop_set = set()
            for rec in keys_res:
                prop_set.update(rec["keys"])
                
            # Filter internal properties if any, but keep dynamic CSV properties
            props = sorted(list(prop_set))
            schema["properties"] = props
            schema["property_map"] = {p.lower(): p for p in props}
            
            # 3. Extract datasets
            dataset_res = session.run("MATCH (d:Dataset) RETURN d.id AS id, d.filename AS filename LIMIT 10")
            schema["datasets"] = [d.data() for d in dataset_res]
            
    except Exception as e:
        logger.warning(f"Failed to inspect Neo4j schema (graph may be initializing): {e}")
        # Try without database parameter if custom database fails
        if database:
            try:
                return get_graph_schema(driver, database=None)
            except Exception:
                pass
                
    return schema


# -----------------------------------------------------------------------------
# 3. QUESTION -> CYPHER DETERMINISTIC TEMPLATE ENGINE
# -----------------------------------------------------------------------------

def find_column_in_question(question: str, property_map: Dict[str, str]) -> Optional[str]:
    """
    Identifies if a question references any known column in property_map.
    Prefers longer column name matches to avoid substring collisions.
    """
    q_lower = question.lower()
    # Sort by length descending
    for prop_lower in sorted(property_map.keys(), key=len, reverse=True):
        if prop_lower in ("row_index", "dataset_id"):
            continue
        # Match as whole word
        pattern = r"\b" + re.escape(prop_lower) + r"\b"
        if re.search(pattern, q_lower):
            return property_map[prop_lower]
    return None


def extract_filter_value(question: str, col_name: str) -> Optional[str]:
    """
    Extracts candidate target filter value for a specified column from user question.
    Examples:
      - 'belong to the Billing group' -> 'Billing'
      - 'where group is Billing' -> 'Billing'
      - 'with status = "active"' -> 'active'
    """
    col_esc = re.escape(col_name)
    patterns = [
        # "belong to the Billing group" / "belong to Billing group"
        rf"(?:belong(?:s)? to(?: the)?|in(?: the)?)\s+['\"]?([^'\"]+?)['\"]?\s+{col_esc}\b",
        # "where group is 'Billing'" / "where group = Billing"
        rf"\b{col_esc}\s*(?:=|is|equals|are|of)\s*['\"]?([^'\"?.,]+)['\"]?",
        # "group 'Billing'" / "group of Billing"
        rf"\b{col_esc}\s*(?:of|:)\s*['\"]?([^'\"?.,]+)['\"]?",
        # "Billing group"
        rf"\b([a-zA-Z0-9_-]+)\s+{col_esc}\b"
    ]
    for pat in patterns:
        m = re.search(pat, question, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            # Exclude common stop words if captured
            if val.lower() not in ("the", "a", "an", "each", "every", "all", "what", "which", "how", "many"):
                return val
    return None


def generate_cypher_template(question: str, schema: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Maps user question to a safe Cypher query and metadata.
    Returns: (cypher_query, query_type, is_grounded)
    """
    if not question or not question.strip():
        return None, "empty_question", False

    q_clean = question.strip()
    q_lower = q_clean.lower()
    prop_map = schema.get("property_map", {})

    # 1. Check for dynamic column mention first
    col = find_column_in_question(q_clean, prop_map)

    # 2. Filtered count query (Matches Handout Part 4)
    # e.g., "How many rows belong to the Billing group?"
    # e.g., "How many rows have status active?"
    if re.search(r"\b(how\s+many\s+rows|count\s+rows|number\s+of\s+rows)\b", q_lower) and col:
        val = extract_filter_value(q_clean, col)
        if val:
            # Format clean Cypher matching Handout Part 4:
            # MATCH (r:Row {group: 'Billing'}) RETURN count(r)
            safe_val = val.replace("'", "\\'")
            cypher = f"MATCH (r:Row {{{col}: '{safe_val}'}}) RETURN count(r)"
            return cypher, "filtered_count", True
        else:
            # Question asked "how many rows for each <col>" / breakdown
            cypher = f"MATCH (r:Row) WHERE r.{col} IS NOT NULL RETURN r.{col} AS {col}, count(r) AS count ORDER BY count DESC LIMIT 20"
            return cypher, "group_breakdown", True

    # 3. Detect filtered or conditional questions referencing unknown columns/properties
    # If the user tries to filter ("where", "with", "belong to", "have", "equals") without a valid column in schema,
    # reject as ungrounded to prevent fabricated or mismatched answers.
    if re.search(r"\b(belong(?:s)?\s+to|where|with|have|has|equals?|\bfor\s+each\b|\bper\b)\b", q_lower):
        return None, "unknown_property", False

    # 4. Total row count query (only for genuine total count questions)
    # e.g., "how many rows are there", "total rows in dataset", "count rows", "total number of records"
    if re.search(r"\b(total\s+(?:number\s+of\s+)?rows|how\s+many\s+rows(?:\s+are\s+there|\s+in\s+total|\s+in\s+(?:the\s+)?(?:data|dataset|csv|file))?|count\s+rows|total\s+records|how\s+many\s+records(?:\s+are\s+there)?)\b", q_lower):
        cypher = "MATCH (r:Row) RETURN count(r)"
        return cypher, "total_count", True

    # 4. Distinct values / categories
    # e.g., "What groups exist?", "List distinct departments", "Show unique status"
    if col and re.search(r"\b(distinct|unique|values|what\s+[a-z]+\s+exist|list\s+all|show\s+all|categories)\b", q_lower):
        cypher = f"MATCH (r:Row) WHERE r.{col} IS NOT NULL RETURN DISTINCT r.{col} AS {col} ORDER BY {col} LIMIT 25"
        return cypher, "distinct_values", True

    # 5. Filtered rows retrieval
    # e.g., "Show rows where group is Billing", "List records with status active"
    if col:
        val = extract_filter_value(q_clean, col)
        if val:
            safe_val = val.replace("'", "\\'")
            cypher = f"MATCH (r:Row {{{col}: '{safe_val}'}}) RETURN r LIMIT 10"
            return cypher, "filtered_rows", True

    # 6. Breakdown / Aggregation by column
    # e.g., "Breakdown by department", "Rows per group"
    if col and re.search(r"\b(breakdown|per|by|distribution|summary)\b", q_lower):
        cypher = f"MATCH (r:Row) WHERE r.{col} IS NOT NULL RETURN r.{col} AS {col}, count(r) AS count ORDER BY count DESC LIMIT 20"
        return cypher, "group_breakdown", True

    # 7. List / Preview generic rows
    # e.g., "Show rows", "Preview data", "Show first 5 records"
    if re.search(r"\b(preview|show\s+rows|list\s+rows|sample\s+rows|display\s+data)\b", q_lower):
        cypher = "MATCH (r:Row) RETURN r LIMIT 5"
        return cypher, "preview_rows", True

    # 8. Schema / Column listing
    # e.g., "What columns are there?", "What fields exist?", "Show schema"
    if re.search(r"\b(columns|fields|schema|properties|headers)\b", q_lower):
        cypher = "MATCH (r:Row) RETURN keys(r) AS columns LIMIT 1"
        return cypher, "schema_info", True

    # 9. Dataset / Uploaded files listing
    if re.search(r"\b(datasets|files|uploaded|filename)\b", q_lower):
        cypher = "MATCH (d:Dataset) RETURN d.id AS id, d.filename AS filename, d.uploaded_at AS uploaded_at"
        return cypher, "dataset_info", True

    # Fallback: Unsupported question
    return None, "unsupported", False


# -----------------------------------------------------------------------------
# 4. ANSWER SYNTHESIS (STRICT GROUNDING ON NEO4J RESULTS)
# -----------------------------------------------------------------------------

def format_answer_from_result(
    question: str,
    query_type: str,
    cypher: str,
    result: List[Dict[str, Any]],
    schema: Dict[str, Any]
) -> Tuple[str, bool]:
    """
    Synthesizes a truthful, concise human-readable answer strictly from the query result.
    Never fabricates missing details.
    Returns: (answer_text, is_grounded)
    """
    if not result and query_type not in ("filtered_count", "total_count"):
        return "I don't have that information in the uploaded data.", False

    # 1. Total row count
    if query_type == "total_count":
        cnt = result[0].get("count(r)") if result else None
        if cnt is None and result and "total" in result[0]:
            cnt = result[0]["total"]
        if cnt is not None:
            return f"There are {cnt:,} total rows in the dataset.", True
        return "Unable to determine row count.", False

    # 2. Filtered count (Matches Handout Part 4)
    # Handout: "There are 128 rows where group = 'Billing'."
    if query_type == "filtered_count":
        cnt = result[0].get("count(r)", 0) if result else 0
        # Extract col and val from cypher pattern: {col: 'val'}
        m = re.search(r"\{(\w+):\s*'([^']+)'\}", cypher)
        if m:
            col, val = m.group(1), m.group(2)
            if cnt == 0:
                return f"There are 0 rows where {col} = '{val}'.", True
            return f"There are {cnt} rows where {col} = '{val}'.", True
        return f"Found {cnt} matching rows.", True

    # 3. Distinct values
    if query_type == "distinct_values":
        # Extract returned values
        keys = list(result[0].keys()) if result else []
        if keys:
            col = keys[0]
            vals = [str(r[col]) for r in result if r.get(col) is not None]
            if not vals:
                return f"No values found for {col}.", False
            sample = ", ".join(vals[:15])
            more = f" (and {len(vals)-15} more)" if len(vals) > 15 else ""
            return f"Found {len(vals)} distinct values for {col}: {sample}{more}.", True
        return "No distinct values found.", False

    # 4. Group breakdown
    if query_type == "group_breakdown":
        lines = []
        col_name = "group"
        for r in result[:10]:
            keys = [k for k in r.keys() if k != "count"]
            col_name = keys[0] if keys else "group"
            val = r.get(col_name, "Unknown")
            cnt = r.get("count", 0)
            lines.append(f"{val}: {cnt}")
        summary = "; ".join(lines)
        return f"Breakdown by {col_name}: {summary}.", True

    # 5. Filtered rows or Preview
    if query_type in ("filtered_rows", "preview_rows"):
        count = len(result)
        if count == 0:
            return "No matching rows found in the dataset.", False
        return f"Found {count} matching row(s). Showing properties in result.", True

    # 6. Schema info
    if query_type == "schema_info":
        if result and "columns" in result[0]:
            cols = [c for c in result[0]["columns"] if c not in ("row_index", "dataset_id")]
            return f"The dataset contains the following columns: {', '.join(cols)}.", True
        props = [p for p in schema.get("properties", []) if p not in ("row_index", "dataset_id")]
        if props:
            return f"The dataset contains columns: {', '.join(props)}.", True
        return "No column schema could be extracted.", False

    # 7. Dataset info
    if query_type == "dataset_info":
        if result:
            filenames = [d.get("filename", "unknown") for d in result]
            return f"Uploaded datasets: {', '.join(filenames)}.", True
        return "No datasets have been uploaded yet.", False

    return "Result retrieved from graph.", True


# -----------------------------------------------------------------------------
# 5. PUBLIC CHAT ENTRYPOINT
# -----------------------------------------------------------------------------

def handle_chat(
    question: str,
    driver: Any,
    database: Optional[str] = "CSV_Graph_DB"
) -> Dict[str, Any]:
    """
    Main Chat API handler complying with the hackathon handout specification.

    Parameters:
      - question: User's English natural language question.
      - driver: Neo4j Python GraphDatabase driver instance (or compatible mock).
      - database: Target Neo4j database name (defaults to 'CSV_Graph_DB').

    Returns:
      {
        "answer": str,
        "cypher": str,
        "result": list,
        "grounded": bool
      }
    """
    # 1. Validate question existence
    if not question or not str(question).strip():
        return {
            "answer": "Question was empty. Please ask a question about the uploaded data.",
            "cypher": "",
            "result": [],
            "grounded": False
        }

    # 2. Inspect Neo4j Schema / State
    try:
        schema = get_graph_schema(driver, database=database)
    except Exception as e:
        logger.error(f"Error accessing Neo4j database: {e}")
        return {
            "answer": "Unable to connect to Neo4j graph database. Please verify the service is running.",
            "cypher": "",
            "result": [],
            "grounded": False
        }

    # 3. Handle pre-upload / empty database state gracefully
    if schema.get("empty", True) or schema.get("total_rows", 0) == 0:
        return {
            "answer": "No uploaded data is currently available in the graph. Please upload a CSV first.",
            "cypher": "MATCH (r:Row) RETURN count(r)",
            "result": [{"count(r)": 0}],
            "grounded": False
        }

    # 4. Map question to Cypher query template
    cypher, query_type, supported = generate_cypher_template(question, schema)

    if not supported or not cypher:
        return {
            "answer": "I don't have that information in the uploaded data.",
            "cypher": "",
            "result": [],
            "grounded": False
        }

    # 5. Read-only safety verification
    if not is_safe_read_only_cypher(cypher):
        logger.warning(f"Safety check rejected query: {cypher}")
        return {
            "answer": "Invalid or restricted query operation.",
            "cypher": "",
            "result": [],
            "grounded": False
        }

    # 6. Execute Cypher against Neo4j
    raw_results = []
    try:
        session_kwargs = {}
        if database:
            session_kwargs["database"] = database

        with driver.session(**session_kwargs) as session:
            db_res = session.run(cypher)
            raw_results = [record.data() for record in db_res]
    except Exception as e:
        logger.warning(f"Neo4j execution failed for query '{cypher}': {e}")
        # If database name was wrong, attempt fallback to default session
        if database:
            try:
                with driver.session() as session:
                    db_res = session.run(cypher)
                    raw_results = [record.data() for record in db_res]
            except Exception as e2:
                logger.error(f"Fallback Neo4j execution also failed: {e2}")
                return {
                    "answer": "Query execution failed on the graph.",
                    "cypher": cypher,
                    "result": [],
                    "grounded": False
                }
        else:
            return {
                "answer": "Query execution failed on the graph.",
                "cypher": cypher,
                "result": [],
                "grounded": False
            }

    # 7. Generate grounded answer strictly from result
    answer_text, is_grounded = format_answer_from_result(
        question=question,
        query_type=query_type,
        cypher=cypher,
        result=raw_results,
        schema=schema
    )

    return {
        "answer": answer_text,
        "cypher": cypher,
        "result": raw_results,
        "grounded": is_grounded
    }
