"""
ZYNTAX Chat Engine (Member 3 - AI / Chatbot / Integration)
===========================================================
Official Hackathon Implementation for RISE @ RST #5.

Architecture & Responsibilities:
--------------------------------
1. Question -> Cypher Mapping:
   - Dynamic schema discovery: introspects arbitrary CSV properties from Neo4j (:Row) nodes.
   - Supports natural language variations: handles plural/singular variants, underscores,
     case-insensitivity, and filter expressions.
   - Generates deterministic, parameter-safe read-only Cypher queries.

2. Grounding & Anti-Hallucination:
   - Validates that referenced properties exist in the graph before querying.
   - Questions with unknown properties (e.g., 'blue_hair') or off-topic general knowledge
     (e.g., 'capital of France') are strictly marked grounded=False with cypher="" and result=[].
   - If the database is empty or no CSV has been ingested, returns grounded=False safely.
   - Never fabricates numbers or facts; answers are derived 100% from actual Neo4j query results.

3. Read-Only Safety:
   - Strict blocklist rejects mutating keywords: CREATE, MERGE, DELETE, DETACH, SET, REMOVE,
     DROP, ALTER, TRUNCATE, LOAD CSV, APOC write procedures, and multi-statement semicolons.
   - Only permits safe read-only queries (MATCH / WHERE / RETURN / ORDER BY / LIMIT).

4. Official Contract Specification (Handout Part 4):
   POST /chat
   Request:  {"question": "How many rows belong to the Billing group?"}
   Response (200 OK):
   {
     "answer": "There are 128 rows where group = 'Billing'.",
     "cypher": "MATCH (r:Row {group: 'Billing'}) RETURN count(r)",
     "result": [{"count(r)": 128}],
     "grounded": true
   }

   Ungrounded / Unsupported Response:
   {
     "answer": "I don't have that information in the uploaded data.",
     "cypher": "",
     "result": [],
     "grounded": false
   }
"""

import re
import logging
from typing import Any, Dict, List, Optional, Tuple, Set

logger = logging.getLogger("zyntax.chat_engine")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------------
# 1. READ-ONLY SAFETY GUARDRAILS
# -----------------------------------------------------------------------------

FORBIDDEN_CYPHER_TERMS = {
    "CREATE", "MERGE", "DELETE", "DETACH", "SET", "REMOVE", "DROP",
    "ALTER", "TRUNCATE", "LOAD CSV", "PERIODIC COMMIT", "CALL APOC.EXPORT",
    "CALL APOC.PERIODIC", "APOC.CYPHER.DOIT", "DBMS.", "GRANT", "REVOKE",
    "SYSTEM", "DATABASE"
}

def is_safe_read_only_cypher(cypher: str) -> bool:
    """
    Ensures Cypher query is strictly read-only and does not contain mutating commands
    or multi-statement injection attempts.
    """
    if not cypher or not isinstance(cypher, str):
        return False
    
    # Disallow multiple statements separated by semicolon
    cleaned = cypher.strip().rstrip(";")
    if ";" in cleaned:
        return False
    
    # Tokenize and check against forbidden keywords
    upper_query = cleaned.upper()
    for forbidden in FORBIDDEN_CYPHER_TERMS:
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
    Handles arbitrary dynamic CSV columns. Differentiates connection errors from empty graph.
    """
    schema = {
        "empty": True,
        "total_rows": 0,
        "properties": [],
        "property_map": {},  # lowercase -> actual property name
        "datasets": [],
        "error": None
    }
    
    if driver is None:
        schema["error"] = "No Neo4j driver provided"
        return schema
        
    last_err = None
    target_dbs = [database, None] if database else [None]
    
    for db in target_dbs:
        try:
            session_kwargs = {"database": db} if db else {}
            with driver.session(**session_kwargs) as session:
                # 1. Count total rows
                row_count_res = session.run("MATCH (r:Row) RETURN count(r) AS total")
                record = row_count_res.single()
                total_rows = record["total"] if record else 0
                schema["total_rows"] = total_rows
                
                if total_rows == 0:
                    schema["empty"] = True
                    schema["error"] = None
                    return schema
                    
                schema["empty"] = False
                
                # 2. Extract property keys from sample rows
                keys_res = session.run("MATCH (r:Row) RETURN keys(r) AS keys LIMIT 50")
                prop_set = set()
                for rec in keys_res:
                    if rec and rec["keys"]:
                        prop_set.update(rec["keys"])

                props = sorted(list(prop_set))
                schema["properties"] = props
                
                # Map lowercase property names, including UTF-8 BOM stripped versions
                prop_map = {}
                for p in props:
                    prop_map[p.lower()] = p
                    clean_p = p.lstrip("\ufeff").strip()
                    if clean_p:
                        prop_map[clean_p.lower()] = p
                schema["property_map"] = prop_map
                
                # 3. Extract datasets
                dataset_res = session.run("MATCH (d:Dataset) RETURN d.id AS id, d.filename AS filename ORDER BY d.uploaded_at DESC LIMIT 10")
                schema["datasets"] = [d.data() for d in dataset_res]
                schema["error"] = None
                return schema
        except Exception as e:
            last_err = e
            continue
            
    logger.warning(f"Failed to inspect Neo4j schema: {last_err}")
    schema["error"] = str(last_err)
    return schema


# -----------------------------------------------------------------------------
# 3. DYNAMIC COLUMN MATCHING & QUESTION -> CYPHER TEMPLATES
# -----------------------------------------------------------------------------

def escape_prop(col: str) -> str:
    """Format property name for Cypher: simple alphanumeric identifiers stay unquoted, otherwise wrap in backticks."""
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", col):
        return col
    return f"`{col}`"


def get_column_variants(prop: str) -> List[str]:
    """
    Generates likely natural language variations of a column name.
    e.g. 'group' -> ['group', 'groups']
         'first_name' -> ['first_name', 'first name', 'firstname']
         'category' -> ['category', 'categories']
         'status' -> ['status', 'statuses']
    """
    p = prop.lstrip("\ufeff").lower().strip()
    variants = [p]
    if "_" in p:
        variants.append(p.replace("_", " "))
        variants.append(p.replace("_", ""))

    # Plural and singular variations
    if p.endswith("ies") and len(p) > 3:
        variants.append(p[:-3] + "y")
    elif p.endswith("es") and len(p) > 3:
        variants.append(p[:-2])
        variants.append(p[:-1])
    elif p.endswith("s") and len(p) > 2:
        variants.append(p[:-1])
    else:
        if p.endswith("y") and len(p) > 1 and p[-2] not in "aeiou":
            variants.append(p[:-1] + "ies")
        elif p.endswith(("s", "x", "z", "ch", "sh")):
            variants.append(p + "es")
        else:
            variants.append(p + "s")

    # Remove duplicates preserving order
    return list(dict.fromkeys(variants))


def find_column_in_question(question: str, property_map: Dict[str, str]) -> Optional[str]:
    """
    Identifies if a question references any known column in property_map.
    Supports arbitrary dynamic CSV column names, singular/plural, and space-separated variants.
    """
    q_lower = question.lower()
    # Sort actual properties by length descending to prevent partial substring matches
    sorted_props = sorted(property_map.items(), key=lambda x: len(x[0]), reverse=True)
    for prop_key, actual_name in sorted_props:
        if prop_key in ("row_index", "dataset_id"):
            continue
        variants = get_column_variants(prop_key)
        for var in variants:
            pattern = r"\b" + re.escape(var) + r"\b"
            if re.search(pattern, q_lower):
                return actual_name
    return None


def extract_filter_value(question: str, col_name: str) -> Optional[str]:
    """
    Extracts candidate target filter value for a specified column from user question.
    Examples:
      - 'belong to the Billing group' -> 'Billing'
      - 'where group is Billing' -> 'Billing'
      - 'where group = Billing.' -> 'Billing'
      - 'with status = "active"' -> 'active'
    """
    col_esc = re.escape(col_name)
    variants = get_column_variants(col_name)
    col_group = "(?:" + "|".join(re.escape(v) for v in variants) + ")"

    patterns = [
        # "belong to the Billing group" / "belong to Billing group"
        rf"(?:belong(?:s)?\s+to(?: the)?|in(?: the)?)\s+['\"]?([^'\"]+?)['\"]?\s+{col_group}\b",
        # "where group equal to Billing" / "where group = Billing" / "group is active"
        rf"\b{col_group}\s*(?:equal\s+to|equals?|=|is|are|of|:)\s*['\"]?([^'\"?.,;]+)[\'\"]?",
        # "status active" (e.g. "with status active")
        rf"\b{col_group}\s+['\"]?([a-zA-Z0-9_-]+)[\'\"]?",
        # "group 'Billing'"
        rf"\b{col_group}\s+['\"]([^'\"]+)['\"]",
        # "Billing group"
        rf"\b([a-zA-Z0-9_-]+)\s+{col_group}\b"
    ]
    
    stop_words = {
        "the", "a", "an", "each", "every", "all", "what", "which",
        "how", "many", "rows", "records", "is", "are", "present",
        "there", "exist", "available", "distinct", "unique", "values",
        "have", "has", "with", "where", "belong", "belongs", "to", "for"
    }

    for pat in patterns:
        m = re.search(pat, question, re.IGNORECASE)
        if m:
            val = m.group(1).strip().strip(".,;:?!'\"")
            if val and val.lower() not in stop_words:
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

    # 0. Identify dynamic column from question if present
    col = find_column_in_question(q_clean, prop_map)

    # 1. Column count intent
    # e.g., "How many columns?", "How many columns are there?", "What is the column count?", "Tell me the number of columns", "Number of columns", "Count columns"
    if re.search(r"\b(how\s+many\s+(?:total\s+)?columns?|column\s+count|number\s+of\s+columns?|count\s+columns?|total\s+columns?)\b", q_lower):
        cypher = "OPTIONAL MATCH (d:Dataset) WITH d ORDER BY d.uploaded_at DESC LIMIT 1 MATCH (r:Row) WHERE (d IS NULL) OR (r.dataset_id = d.id) WITH d, collect(r) AS all_rows UNWIND all_rows AS row_item UNWIND keys(row_item) AS key_item WITH d, collect(DISTINCT key_item) AS all_keys WITH d, [k IN all_keys WHERE NOT k IN ['dataset_id', 'row_index']] AS columns RETURN size(columns) AS column_count, columns, coalesce(d.filename, 'uploaded dataset') AS filename"
        return cypher, "column_count", True

    # 2. Schema info / column listing
    # e.g., "What columns are available?", "List the columns.", "Show me the column names.", "List available columns.", "What are the columns?", "Show schema", "What fields exist?"
    if re.search(r"\b(what\s+columns|list\s+(?:the\s+)?columns?|available\s+columns|show\s+(?:me\s+)?(?:the\s+)?column\s+names?|column\s+names?|what\s+are\s+the\s+columns|columns?\s+available|show\s+schema|what\s+fields)\b", q_lower):
        cypher = "OPTIONAL MATCH (d:Dataset) WITH d ORDER BY d.uploaded_at DESC LIMIT 1 MATCH (r:Row) WHERE (d IS NULL) OR (r.dataset_id = d.id) WITH d, collect(r) AS all_rows UNWIND all_rows AS row_item UNWIND keys(row_item) AS key_item WITH d, collect(DISTINCT key_item) AS all_keys WITH d, [k IN all_keys WHERE NOT k IN ['dataset_id', 'row_index']] AS columns RETURN size(columns) AS column_count, columns, coalesce(d.filename, 'uploaded dataset') AS filename"
        return cypher, "schema_info", True

    # 3. Dataset summary / overview
    # e.g., "What is the content?", "What does this dataset contain?", "Tell me about this file.", "Tell me about the uploaded dataset.", "Give me a summary of the uploaded data."
    if re.search(r"\b(content|contain(?:s)?|about this file|about the (?:uploaded )?dataset|about the data|what\s+is\s+in\s+this\s+dataset|what\s+is\s+in\s+the\s+dataset|summary of the (?:uploaded )?data|summar(?:y|ize)|overview|describe the (?:data|dataset))\b", q_lower):
        cypher = "OPTIONAL MATCH (d:Dataset) WITH d ORDER BY d.uploaded_at DESC LIMIT 1 MATCH (r:Row) WHERE (d IS NULL) OR (r.dataset_id = d.id) WITH d, count(r) AS total_rows, collect(r) AS all_rows UNWIND all_rows AS row_item UNWIND keys(row_item) AS key_item WITH d, total_rows, all_rows, collect(DISTINCT key_item) AS all_keys WITH d, total_rows, [k IN all_keys WHERE NOT k IN ['dataset_id', 'row_index']] AS columns, all_rows[0..3] AS sample_rows RETURN coalesce(d.filename, 'uploaded dataset') AS filename, total_rows, size(columns) AS column_count, columns, sample_rows"
        return cypher, "dataset_summary", True

    # 4. Unrecognized property filter check:
    # If col was NOT found, but the question attempts to filter/condition ("where", "with", "have", "blue_hair", "green_eyes", etc.),
    # reject immediately to prevent ungrounded queries from falling through to general row count.
    if not col and re.search(r"\b(belong(?:s)?\s+to|where|with|have|has|equals?|equal\s+to|\bfor\s+each\b|\bper\b)\b", q_lower):
        return None, "unknown_property", False

    # 5. Breakdown by column (e.g., "Give me a breakdown by group.", "How many rows are in each group?", "Breakdown by department")
    if col and re.search(r"\b(breakdown|per|by|distribution|each)\b", q_lower):
        prop = escape_prop(col)
        cypher = f"MATCH (r:Row) WHERE r.{prop} IS NOT NULL RETURN r.{prop} AS {prop}, count(r) AS count ORDER BY count DESC LIMIT 20"
        return cypher, "group_breakdown", True

    # 6. Distinct values / categories query
    # e.g., "What values are in group?", "List unique group values.", "What groups are present?"
    if col and re.search(r"\b(distinct|unique|values|present|exist|available|what\s+[a-z_0-9-]+\s+(?:are|exist|is)|list\s+all|show\s+all|categories)\b", q_lower):
        if not re.search(r"\b(how\s+many\s+rows|count\s+rows|number\s+of\s+rows)\b", q_lower):
            prop = escape_prop(col)
            cypher = f"MATCH (r:Row) WHERE r.{prop} IS NOT NULL RETURN DISTINCT r.{prop} AS {prop} ORDER BY {prop} LIMIT 25"
            return cypher, "distinct_values", True

    # 7. Row listing / preview of specific rows
    # e.g., "Show the rows where group is Billing.", "Show rows with status active"
    if col and re.search(r"\b(show|list|display|find|get|see|view|preview)\b.*\b(rows?|records?)\b", q_lower):
        val = extract_filter_value(q_clean, col)
        prop = escape_prop(col)
        if val:
            safe_val = val.replace("'", "\\'")
            cypher = f"MATCH (r:Row {{{prop}: '{safe_val}'}}) RETURN r LIMIT 10"
            return cypher, "filtered_rows", True
        else:
            cypher = f"MATCH (r:Row) WHERE r.{prop} IS NOT NULL RETURN r LIMIT 10"
            return cypher, "filtered_rows", True

    # 8. Filtered count queries
    # e.g., "How many rows have group equal to Billing?", "Count rows where group = Billing.", "How many rows belong to the Billing group?"
    if col and re.search(r"\b(how\s+many\s+rows|count\s+rows|number\s+of\s+rows|how\s+many\s+records)\b", q_lower):
        val = extract_filter_value(q_clean, col)
        prop = escape_prop(col)
        if val:
            safe_val = val.replace("'", "\\'")
            cypher = f"MATCH (r:Row {{{prop}: '{safe_val}'}}) RETURN count(r)"
            return cypher, "filtered_count", True
        else:
            cypher = f"MATCH (r:Row) WHERE r.{prop} IS NOT NULL RETURN r.{prop} AS {prop}, count(r) AS count ORDER BY count DESC LIMIT 20"
            return cypher, "group_breakdown", True

    # 9. Total row count query (genuine total count across whole dataset)
    # e.g., "How many rows are there?", "How many rows?", "What is the number of rows?", "What is the row count?", "Tell me how many records are there"
    if re.search(r"\b(how\s+many\s+(?:total\s+)?(?:rows|records)|what\s+is\s+the\s+(?:number\s+of\s+rows|row\s+count)|number\s+of\s+rows|row\s+count|total\s+(?:number\s+of\s+)?rows|count\s+(?:total\s+)?rows|total\s+records)\b", q_lower):
        cypher = "MATCH (r:Row) RETURN count(r)"
        return cypher, "total_count", True

    # 10. Generic preview / list rows (when no column is specified)
    # e.g., "Show me some rows.", "Show first 5 rows.", "Give me a sample of the data.", "Preview data"
    if re.search(r"\b(preview|show|list|display|sample|view)\b.*\b(rows?|records?|data)\b", q_lower):
        cypher = "OPTIONAL MATCH (d:Dataset) WITH d ORDER BY d.uploaded_at DESC LIMIT 1 MATCH (r:Row) WHERE (d IS NULL) OR (r.dataset_id = d.id) RETURN r LIMIT 5"
        return cypher, "preview_rows", True

    # 11. General columns fallback
    if re.search(r"\b(columns?|fields?|schema|properties|headers?)\b", q_lower):
        cypher = "OPTIONAL MATCH (d:Dataset) WITH d ORDER BY d.uploaded_at DESC LIMIT 1 MATCH (r:Row) WHERE (d IS NULL) OR (r.dataset_id = d.id) WITH d, collect(r) AS all_rows UNWIND all_rows AS row_item UNWIND keys(row_item) AS key_item WITH d, collect(DISTINCT key_item) AS all_keys WITH d, [k IN all_keys WHERE NOT k IN ['dataset_id', 'row_index']] AS columns RETURN size(columns) AS column_count, columns, coalesce(d.filename, 'uploaded dataset') AS filename"
        return cypher, "schema_info", True

    # 12. Dataset listing
    if re.search(r"\b(datasets|files|uploaded|filename)\b", q_lower):
        cypher = "MATCH (d:Dataset) RETURN d.id AS id, d.filename AS filename, d.uploaded_at AS uploaded_at"
        return cypher, "dataset_info", True

    # Fallback: Unsupported / General knowledge question
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
        return "No matching rows found in the uploaded data.", False

    # 1. Total row count
    if query_type == "total_count":
        cnt = None
        fn = None
        if result:
            rec = result[0]
            cnt = rec.get("total_rows")
            if cnt is None:
                cnt = rec.get("count(r)")
            if cnt is None and "total" in rec:
                cnt = rec["total"]
            fn = rec.get("filename")
        if cnt is not None:
            if fn and fn != "uploaded dataset":
                return f"There are {cnt:,} total rows in the dataset '{fn}'.", True
            return f"There are {cnt:,} total rows in the dataset.", True
        return "Unable to determine row count.", False

    # 2. Filtered count (Matches Handout Part 4)
    # Handout: "There are 128 rows where group = 'Billing'."
    if query_type == "filtered_count":
        cnt = result[0].get("count(r)", 0) if result else 0
        m = re.search(r"\{`?([^`:]+)`?:\s*'([^']+)'\}", cypher)
        if m:
            col, val = m.group(1).lstrip("\ufeff"), m.group(2)
            return f"There are {cnt} rows where {col} = '{val}'.", True
        return f"Found {cnt} matching rows.", True

    # 3. Distinct values
    if query_type == "distinct_values":
        keys = list(result[0].keys()) if result else []
        if keys:
            raw_col = keys[0]
            col_disp = raw_col.lstrip("\ufeff")
            vals = [str(r[raw_col]) for r in result if r.get(raw_col) is not None and str(r.get(raw_col)).strip()]
            if not vals:
                return f"No values found for {col_disp}.", False
            sample = ", ".join(vals[:15])
            more = f" (and {len(vals)-15} more)" if len(vals) > 15 else ""
            return f"Found {len(vals)} distinct values for {col_disp}: {sample}{more}.", True
        return "No distinct values found.", False

    # 4. Filtered rows
    if query_type == "filtered_rows":
        count = len(result)
        if count == 0:
            return "No matching rows found in the uploaded data.", False
        m = re.search(r"\{`?([^`:]+)`?:\s*'([^']+)'\}", cypher)
        if m:
            col, val = m.group(1).lstrip("\ufeff"), m.group(2)
            return f"Found {count} matching row(s) where {col} = '{val}'. Showing properties in result.", True
        return f"Found {count} matching row(s). Showing properties in result.", True

    # 5. Generic preview rows
    if query_type == "preview_rows":
        count = len(result)
        if count == 0:
            return "No rows found in the uploaded data.", False
        return f"Showing top {count} rows from the dataset.", True

    # 6. Group breakdown
    if query_type == "group_breakdown":
        lines = []
        col_name = "group"
        for r in result[:10]:
            keys = [k for k in r.keys() if k != "count"]
            raw_col = keys[0] if keys else "group"
            col_name = raw_col.lstrip("\ufeff")
            val = r.get(raw_col, "Unknown")
            cnt = r.get("count", 0)
            val_disp = str(val) if str(val).strip() else "(empty)"
            lines.append(f"{val_disp}: {cnt}")
        summary = "; ".join(lines)
        return f"Breakdown by {col_name}: {summary}.", True

    # 7. Column count
    if query_type == "column_count":
        if result:
            rec = result[0]
            cnt = rec.get("column_count")
            cols = rec.get("columns") or []
            cols_clean = [c.lstrip("\ufeff") for c in cols if c not in ("dataset_id", "row_index")]
            if cnt is None:
                cnt = len(cols_clean)
            fn = rec.get("filename")
            if cols_clean:
                cols_str = ", ".join(cols_clean)
                if fn and fn != "uploaded dataset":
                    return f"There are {cnt} columns in the uploaded dataset '{fn}': {cols_str}.", True
                return f"There are {cnt} columns in the dataset: {cols_str}.", True
            return f"There are {cnt} columns in the dataset.", True
        return "No column schema could be extracted.", False

    # 8. Schema info / column listing
    if query_type == "schema_info":
        if result:
            rec = result[0]
            cols = rec.get("columns") or []
            cols_clean = [c.lstrip("\ufeff") for c in cols if c not in ("dataset_id", "row_index")]
            cnt = rec.get("column_count", len(cols_clean))
            fn = rec.get("filename")
            if cols_clean:
                cols_str = ", ".join(cols_clean)
                if fn and fn != "uploaded dataset":
                    return f"The available columns in '{fn}' are: {cols_str} ({cnt} columns total).", True
                return f"The available columns are: {cols_str} ({cnt} columns total).", True
        props = [p.lstrip("\ufeff") for p in schema.get("properties", []) if p not in ("row_index", "dataset_id")]
        if props:
            return f"The available columns are: {', '.join(props)} ({len(props)} columns total).", True
        return "No column schema could be extracted.", False

    # 9. Dataset summary (Broad dataset overview questions)
    if query_type == "dataset_summary":
        if result:
            rec = result[0]
            filename = rec.get("filename", "uploaded dataset")
            total = rec.get("total_rows", 0)
            cols = rec.get("columns") or []
            cols_clean = [c.lstrip("\ufeff") for c in cols if c not in ("dataset_id", "row_index")]
            cnt = rec.get("column_count", len(cols_clean))
            if cols_clean:
                cols_str = ", ".join(cols_clean)
                return f"The uploaded dataset '{filename}' contains {total:,} rows across {cnt} columns. The available columns are: {cols_str}.", True
            return f"The uploaded dataset '{filename}' contains {total:,} rows across {cnt} columns.", True
        return "No dataset information found in graph.", False

    # 10. Dataset info
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
            "answer": "Question was empty. Please ask a specific question about the uploaded data.",
            "cypher": "",
            "result": [],
            "grounded": False
        }

    # 2. Inspect Neo4j Schema / State
    try:
        schema = get_graph_schema(driver, database=database)
        if schema.get("error"):
            return {
                "answer": "Database error: unable to execute query on Neo4j. Please verify the graph service is running.",
                "cypher": "",
                "result": [],
                "grounded": False
            }
    except Exception as e:
        logger.error(f"Error accessing Neo4j database: {e}")
        return {
            "answer": "Database error: unable to execute query on Neo4j. Please verify the graph service is running.",
            "cypher": "",
            "result": [],
            "grounded": False
        }

    # 3. Handle pre-upload / empty database state gracefully
    if schema.get("empty", True) or schema.get("total_rows", 0) == 0:
        return {
            "answer": "No uploaded data is currently available in the graph. Please upload a CSV first.",
            "cypher": "",
            "result": [],
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
                    "answer": "Database error: unable to execute query on Neo4j. Please verify the graph service is running.",
                    "cypher": "",
                    "result": [],
                    "grounded": False
                }
        else:
            return {
                "answer": "Database error: unable to execute query on Neo4j. Please verify the graph service is running.",
                "cypher": "",
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

    if not is_grounded:
        return {
            "answer": answer_text,
            "cypher": "",
            "result": [],
            "grounded": False
        }

    return {
        "answer": answer_text,
        "cypher": cypher,
        "result": raw_results,
        "grounded": True
    }
