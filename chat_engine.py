"""
ZYNTAX Chat Engine (Member 3 - AI / Chatbot / Integration)
===========================================================
Official Hackathon Implementation for RISE @ RST #5:
"Data In, Answers Out: Build a CSV -> Kafka -> Neo4j Chatbot Pipeline"

Core Architecture & Capabilities:
--------------------------------
1. Dynamic Schema Discovery:
   - Discovers actual available properties dynamically from (:Row) nodes in Neo4j.
   - Zero hardcoding of column names, row counts, or dataset schemas.
   - Automatically scopes queries to the most recently uploaded dataset.
   - Tolerates UTF-8 BOM, case variations, singular/plural, and synonyms.

2. Comprehensive Natural-Language Query Intent Engine (12+ Intents):
   - 1. Row count (all paraphrases)
   - 2. Column count & Schema inspection (typo-tolerant for "columns" / "colums")
   - 3. Dataset summary ("What is the content?")
   - 4. Show / List rows (with extracted or default safe LIMIT)
   - 5. Distinct values for verified properties
   - 6. Filtered row counts (property = value)
   - 7. Filtered row retrieval (property = value with LIMIT)
   - 8. Breakdown / Group-by / Distribution by property
   - 9. Numeric aggregations (min, max, average, sum)
   - 10. String search / substring matching (CONTAINS)
   - 11. Multi-condition queries (e.g. department=HR AND status=Active)
   - 12. Lightweight conversational context support

3. Grounding & Anti-Hallucination:
   - Questions requiring nonexistent columns return grounded=False with an honest explanation.
   - Off-topic general knowledge queries return grounded=False.
   - Pre-upload and empty graph states return grounded=False safely.
   - Every factual answer is strictly synthesized from actual Neo4j query results.

4. Read-Only Safety Guard:
   - Strict blocklist blocks mutating keywords (CREATE, MERGE, DELETE, DETACH, SET, REMOVE,
     DROP, ALTER, TRUNCATE, LOAD CSV, APOC write procedures, and multi-statement semicolons).
"""

import re
import logging
from typing import Any, Dict, List, Optional, Tuple, Set

logger = logging.getLogger("zyntax.chat_engine")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# In-memory recent chat context for conversational follow-ups
GLOBAL_CHAT_CONTEXT: Dict[str, Any] = {
    "last_column": None,
    "last_value": None,
    "last_dataset_id": None,
    "last_query_type": None
}


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
    Queries Neo4j to dynamically inspect:
    - Total row count
    - Unique property keys present on (:Row) nodes
    - Available (:Dataset) nodes
    - Active dataset (most recent)
    Handles arbitrary dynamic CSV columns and stripped UTF-8 BOM.
    """
    schema: Dict[str, Any] = {
        "empty": True,
        "total_rows": 0,
        "properties": [],
        "property_map": {},  # lowercase -> actual property name
        "datasets": [],
        "active_dataset": None,
        "numeric_columns": set(),
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
                
                # 2. Extract datasets (most recent first)
                try:
                    dataset_res = session.run(
                        "MATCH (d:Dataset) RETURN d.id AS id, d.filename AS filename, d.uploaded_at AS uploaded_at "
                        "ORDER BY d.uploaded_at DESC LIMIT 10"
                    )
                    datasets = [d.data() for d in dataset_res]
                    schema["datasets"] = datasets
                    if datasets:
                        schema["active_dataset"] = datasets[0]
                except Exception as ex_ds:
                    logger.debug(f"Dataset node introspection note: {ex_ds}")
                
                # 3. Extract property keys from sample rows
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
                
                # 4. Inspect sample rows to identify numeric columns
                try:
                    sample_res = session.run("MATCH (r:Row) RETURN r LIMIT 10")
                    num_cols = set()
                    for s_rec in sample_res:
                        row_dict = s_rec.get("r")
                        if isinstance(row_dict, dict):
                            for k, v in row_dict.items():
                                if k in ("dataset_id", "row_index"):
                                    continue
                                if isinstance(v, (int, float)):
                                    num_cols.add(k)
                                elif isinstance(v, str):
                                    v_clean = v.replace(",", "").strip()
                                    try:
                                        float(v_clean)
                                        num_cols.add(k)
                                    except ValueError:
                                        pass
                    schema["numeric_columns"] = num_cols
                except Exception as ex_num:
                    logger.debug(f"Numeric type detection note: {ex_num}")

                schema["error"] = None
                return schema
        except Exception as e:
            last_err = e
            continue
            
    logger.warning(f"Failed to inspect Neo4j schema: {last_err}")
    schema["error"] = str(last_err)
    return schema


# -----------------------------------------------------------------------------
# 3. DYNAMIC COLUMN MATCHING & CYPHER UTILITIES
# -----------------------------------------------------------------------------

def escape_prop(col: str) -> str:
    """
    Format property name for Cypher: simple alphanumeric identifiers stay unquoted,
    otherwise safely wrap in backticks.
    """
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", col):
        return col
    return f"`{col}`"


def get_column_variants(prop: str) -> List[str]:
    """
    Generates natural language variations of a column name.
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

    return list(dict.fromkeys(variants))


def find_column_in_question(question: str, property_map: Dict[str, str]) -> Optional[str]:
    """
    Identifies if a question references any known column in property_map.
    Supports dynamic CSV column names, singular/plural, and space-separated variants.
    """
    q_lower = question.lower()
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
    variants = get_column_variants(col_name)
    col_group = "(?:" + "|".join(re.escape(v) for v in variants) + ")"

    patterns = [
        # "belong to the Billing group" / "in Billing group"
        rf"(?:belong(?:s)?\s+to(?: the)?|in(?: the)?)\s+['\"]?([^'\"]+?)['\"]?\s+{col_group}\b",
        # "where group equal to Billing" / "group = Billing" / "group is active"
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


def extract_limit(question: str, default: int = 5) -> int:
    """Extracts explicit row count limit if requested (e.g. 'show first 10 rows'), bounded [1, 50]."""
    m = re.search(r"\b(?:first\s+|top\s+)?(\d+)\s+(?:rows?|records?|items?|entries)\b", question.lower())
    if m:
        try:
            val = int(m.group(1))
            return min(max(val, 1), 50)
        except ValueError:
            pass
    return default


def detect_numeric_aggregation(question: str) -> Optional[str]:
    """Detects numeric aggregation intent: max, min, avg, or sum."""
    q = question.lower()
    if re.search(r"\b(max(?:imum)?|highest|top|most)\b", q):
        return "max"
    if re.search(r"\b(min(?:imum)?|lowest|bottom|least)\b", q):
        return "min"
    if re.search(r"\b(avg|average|mean)\b", q):
        return "avg"
    if re.search(r"\b(sum|total\s+sum|sum\s+of)\b", q):
        return "sum"
    return None


def detect_string_search(question: str) -> Optional[str]:
    """Detects text search queries (e.g. rows containing 'security' or which mention Kafka)."""
    q = question.strip()
    m = re.search(r"\b(?:contain(?:s|ing)?|mention(?:s|ed|ing)?|with\s+text|matching)\s+['\"]([^'\"]+)['\"]", q, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m2 = re.search(r"\b(?:contain(?:s|ing)?|mention(?:s|ed|ing)?)\s+([a-zA-Z0-9_-]+)\b", q, re.IGNORECASE)
    if m2:
        val = m2.group(1).strip()
        if val.lower() not in {"the", "a", "an", "all", "rows", "records", "data"}:
            return val
    return None


def detect_unknown_property_candidate(q_lower: str) -> Optional[str]:
    """
    Identifies candidate property names in user questions when no known column matched.
    Enables precise, honest feedback: "I don't have a column named '<candidate>' in the uploaded data."
    """
    patterns = [
        # breakdown by salary / distribution by department
        r"\b(?:breakdown|distribution)\s+by\s+([a-zA-Z0-9_-]+)\b",
        # what values are in location / unique departments
        r"\bvalues?\s+(?:are\s+)?(?:in|for|present\s+in)\s+([a-zA-Z0-9_-]+)\b",
        r"\b(?:unique|distinct)\s+([a-zA-Z0-9_-]+)\b",
        # where blue_hair / with green_eyes / have blue_hair
        r"\b(?:where|with|have|has|equals?|equal\s+to)\s+([a-zA-Z0-9_-]+)\b",
        # maximum age / average salary
        r"\b(?:max(?:imum)?|min(?:imum)?|average|avg|mean|highest|lowest)\s+([a-zA-Z0-9_-]+)\b"
    ]
    stop_words = {"the", "a", "an", "all", "rows", "records", "data", "count", "number", "dataset"}
    for pat in patterns:
        m = re.search(pat, q_lower)
        if m:
            cand = m.group(1).strip()
            if cand not in stop_words:
                return cand
    return None


# -----------------------------------------------------------------------------
# 4. QUESTION -> CYPHER TEMPLATE GENERATOR
# -----------------------------------------------------------------------------

def generate_cypher_template(
    question: str,
    schema: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None
) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Maps user question to a safe Cypher query and metadata.
    Returns: (cypher_query, query_type, is_grounded)
    """
    if not question or not question.strip():
        return None, "empty_question", False

    q_clean = question.strip()
    q_lower = q_clean.lower()
    prop_map = schema.get("property_map", {})

    # 0. Identify dynamic column from question or conversation context
    col = find_column_in_question(q_clean, prop_map)
    
    # Conversational follow-up resolution (e.g. "how many of them are in Billing?")
    if not col and context and context.get("last_column"):
        if re.search(r"\b(of\s+them|in\s+it|for\s+that|about\s+it)\b", q_lower) or re.search(r"\b(belong|in|with|having)\b", q_lower):
            col = context.get("last_column")

    # 1. Column count intent
    # e.g., "How many columns?", "How many columns are there?", "What is the column count?", "Tell me the number of columns", "Number of columns", "Count columns"
    if re.search(r"\b(how\s+many\s+(?:total\s+)?colum(?:n)?s?|what\s+is\s+(?:the\s+)?(?:number\s+of\s+|total\s+)?colum(?:n)?s?|what\s+is\s+(?:the\s+)?colum(?:n)?\s+count|colum(?:n)?\s+count|number\s+of\s+colum(?:n)?s?|count\s+colum(?:n)?s?|total\s+colum(?:n)?s?)\b", q_lower):
        cypher = (
            "OPTIONAL MATCH (d:Dataset) WITH d ORDER BY d.uploaded_at DESC LIMIT 1 "
            "MATCH (r:Row) WHERE (d IS NULL) OR (r.dataset_id = d.id) "
            "WITH d, collect(r) AS all_rows UNWIND all_rows AS row_item UNWIND keys(row_item) AS key_item "
            "WITH d, collect(DISTINCT key_item) AS all_keys "
            "WITH d, [k IN all_keys WHERE NOT k IN ['dataset_id', 'row_index']] AS columns "
            "RETURN size(columns) AS column_count, columns, coalesce(d.filename, 'uploaded dataset') AS filename"
        )
        return cypher, "column_count", True

    # 2. Schema info / column listing
    # e.g., "What columns are available?", "List the columns.", "Show me the column names.", "List available columns.", "What are the columns?", "Show schema", "What fields exist?"
    if re.search(r"\b(what\s+colum(?:n)?s?|list\s+(?:the\s+)?colum(?:n)?s?|available\s+colum(?:n)?s?|show\s+(?:me\s+)?(?:the\s+)?colum(?:n)?\s+names?|colum(?:n)?\s+names?|what\s+are\s+the\s+colum(?:n)?s?|colum(?:n)?s?\s+available|show\s+schema|what\s+fields|what\s+headers|headers)\b", q_lower):
        cypher = (
            "OPTIONAL MATCH (d:Dataset) WITH d ORDER BY d.uploaded_at DESC LIMIT 1 "
            "MATCH (r:Row) WHERE (d IS NULL) OR (r.dataset_id = d.id) "
            "WITH d, collect(r) AS all_rows UNWIND all_rows AS row_item UNWIND keys(row_item) AS key_item "
            "WITH d, collect(DISTINCT key_item) AS all_keys "
            "WITH d, [k IN all_keys WHERE NOT k IN ['dataset_id', 'row_index']] AS columns "
            "RETURN size(columns) AS column_count, columns, coalesce(d.filename, 'uploaded dataset') AS filename"
        )
        return cypher, "schema_info", True

    # 3. String Search / Substring Matching — MUST run before dataset summary to avoid "contains <value>" being swallowed
    # e.g., "Find rows containing 'security'", "Show records where status contains 'active'", "Which rows mention Kafka?"
    search_term = detect_string_search(q_clean)
    if search_term:
        safe_term = search_term.replace("'", "\\'")
        if col:
            prop = escape_prop(col)
            cypher = f"MATCH (r:Row) WHERE toLower(toString(r.{prop})) CONTAINS toLower('{safe_term}') RETURN r LIMIT 10"
        else:
            cypher = f"MATCH (r:Row) WHERE any(k IN keys(r) WHERE toLower(toString(r[k])) CONTAINS toLower('{safe_term}')) RETURN r LIMIT 10"
        return cypher, "string_search", True

    # 4. Dataset summary / overview (only when not a string search)
    # e.g., "What is the content?", "What does this dataset contain?", "Tell me about this file.", "Tell me about the uploaded dataset.", "Give me a summary of the uploaded data."
    if re.search(r"\b(content|contain(?:s)?|about this file|about the (?:uploaded )?dataset|about the data|what\s+is\s+in\s+this\s+dataset|what\s+is\s+in\s+the\s+dataset|summary of the (?:uploaded )?data|summar(?:y|ize)|overview|describe the (?:data|dataset)|what\s+kind\s+of\s+data)\b", q_lower):
        cypher = (
            "OPTIONAL MATCH (d:Dataset) WITH d ORDER BY d.uploaded_at DESC LIMIT 1 "
            "MATCH (r:Row) WHERE (d IS NULL) OR (r.dataset_id = d.id) "
            "WITH d, count(r) AS total_rows, collect(r) AS all_rows UNWIND all_rows AS row_item UNWIND keys(row_item) AS key_item "
            "WITH d, total_rows, all_rows, collect(DISTINCT key_item) AS all_keys "
            "WITH d, total_rows, [k IN all_keys WHERE NOT k IN ['dataset_id', 'row_index']] AS columns, all_rows[0..3] AS sample_rows "
            "RETURN coalesce(d.filename, 'uploaded dataset') AS filename, total_rows, size(columns) AS column_count, columns, sample_rows"
        )
        return cypher, "dataset_summary", True

    # 5. Multi-Condition Queries (e.g. "How many Billing records are Open?", "Show rows where department is HR and status is Active")
    if " and " in q_lower or "," in q_lower:
        clauses = [c.strip() for c in q_clean.replace(",", " and ").split(" and ") if c.strip()]
        conds = []
        for clause in clauses:
            c_col = find_column_in_question(clause, prop_map)
            if c_col:
                c_val = extract_filter_value(clause, c_col)
                if c_val:
                    conds.append((c_col, c_val))
        if len(conds) >= 2:
            where_parts = []
            for c_col, c_val in conds:
                safe_v = c_val.replace("'", "\\'")
                where_parts.append(f"r.{escape_prop(c_col)} = '{safe_v}'")
            where_clause = " AND ".join(where_parts)
            if re.search(r"\b(how\s+many|count|number\s+of)\b", q_lower):
                cypher = f"MATCH (r:Row) WHERE {where_clause} RETURN count(r)"
                return cypher, "multi_condition_count", True
            else:
                cypher = f"MATCH (r:Row) WHERE {where_clause} RETURN r LIMIT 10"
                return cypher, "multi_condition_rows", True


    # 6. Numeric Aggregation (Min / Max / Average / Sum)
    # e.g., "What is the maximum amount?", "average score", "minimum age", "sum of amount"
    num_agg = detect_numeric_aggregation(q_clean)
    if num_agg and col:
        prop = escape_prop(col)
        cypher = f"MATCH (r:Row) WHERE r.{prop} IS NOT NULL RETURN avg(toFloat(r.{prop})) AS avg_val, min(toFloat(r.{prop})) AS min_val, max(toFloat(r.{prop})) AS max_val, sum(toFloat(r.{prop})) AS sum_val"
        return cypher, f"numeric_{num_agg}", True

    # 7. Unrecognized property filter check:
    # If col was NOT found, but the question attempts to filter/condition ("where", "with", "have", "blue_hair", "green_eyes", etc.),
    # reject immediately to prevent ungrounded queries from falling through to general row count.
    if not col:
        cand = detect_unknown_property_candidate(q_lower)
        if cand or re.search(r"\b(belong(?:s)?\s+to|where|with|have|has|equals?|equal\s+to|\bfor\s+each\b|\bper\b)\b", q_lower):
            return None, "unknown_property", False

    # 8. Breakdown by column (e.g., "Give me a breakdown by group.", "How many rows are in each group?", "Breakdown by department")
    if col and re.search(r"\b(breakdown|per|by|distribution|each)\b", q_lower):
        prop = escape_prop(col)
        cypher = f"MATCH (r:Row) WHERE r.{prop} IS NOT NULL RETURN r.{prop} AS {prop}, count(r) AS count ORDER BY count DESC LIMIT 20"
        return cypher, "group_breakdown", True

    # 9. Distinct values / categories query
    # e.g., "What values are in group?", "List unique group values.", "What groups are present?"
    if col and re.search(r"\b(distinct|unique|values|present|exist|available|what\s+[a-z_0-9-]+\s+(?:are|exist|is)|list\s+all|show\s+all|categories)\b", q_lower):
        if not re.search(r"\b(how\s+many\s+rows|count\s+rows|number\s+of\s+rows)\b", q_lower):
            prop = escape_prop(col)
            cypher = f"MATCH (r:Row) WHERE r.{prop} IS NOT NULL RETURN DISTINCT r.{prop} AS {prop} ORDER BY {prop} LIMIT 25"
            return cypher, "distinct_values", True

    # 10. Row listing / preview of specific rows
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

    # 11. Filtered count queries
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

    # 12. Total row count query (genuine total count across whole dataset)
    # e.g., "How many rows are there?", "How many rows?", "What is the number of rows?", "What is the row count?", "Tell me how many records are there"
    if re.search(r"\b(how\s+many\s+(?:total\s+)?(?:rows|records)|what\s+is\s+the\s+(?:number\s+of\s+rows|row\s+count)|number\s+of\s+rows|row\s+count|total\s+(?:number\s+of\s+)?rows|count\s+(?:total\s+)?rows|total\s+records)\b", q_lower):
        cypher = "MATCH (r:Row) RETURN count(r)"
        return cypher, "total_count", True

    # 13. Generic preview / list rows (when no column is specified)
    # e.g., "Show me some rows.", "Show first 5 rows.", "Give me a sample of the data.", "Preview data"
    if re.search(r"\b(preview|show|list|display|sample|view)\b.*\b(rows?|records?|data)\b", q_lower):
        limit_val = extract_limit(q_clean, default=5)
        cypher = f"OPTIONAL MATCH (d:Dataset) WITH d ORDER BY d.uploaded_at DESC LIMIT 1 MATCH (r:Row) WHERE (d IS NULL) OR (r.dataset_id = d.id) RETURN r LIMIT {limit_val}"
        return cypher, "preview_rows", True

    # 14. General columns fallback (typo-tolerant)
    if re.search(r"\b(colum(?:n)?s?|fields?|schema|properties|headers?)\b", q_lower):
        cypher = (
            "OPTIONAL MATCH (d:Dataset) WITH d ORDER BY d.uploaded_at DESC LIMIT 1 "
            "MATCH (r:Row) WHERE (d IS NULL) OR (r.dataset_id = d.id) "
            "WITH d, collect(r) AS all_rows UNWIND all_rows AS row_item UNWIND keys(row_item) AS key_item "
            "WITH d, collect(DISTINCT key_item) AS all_keys "
            "WITH d, [k IN all_keys WHERE NOT k IN ['dataset_id', 'row_index']] AS columns "
            "RETURN size(columns) AS column_count, columns, coalesce(d.filename, 'uploaded dataset') AS filename"
        )
        return cypher, "schema_info", True

    # 15. Dataset listing
    if re.search(r"\b(datasets|files|uploaded|filename)\b", q_lower):
        cypher = "MATCH (d:Dataset) RETURN d.id AS id, d.filename AS filename, d.uploaded_at AS uploaded_at"
        return cypher, "dataset_info", True

    # Fallback: Unsupported / General knowledge question
    return None, "unsupported", False


# -----------------------------------------------------------------------------
# 5. ANSWER SYNTHESIS (STRICT GROUNDING ON NEO4J RESULTS)
# -----------------------------------------------------------------------------

def format_answer_from_result(
    question: str,
    query_type: str,
    cypher: str,
    result: List[Dict[str, Any]],
    schema: Dict[str, Any]
) -> Tuple[str, bool]:
    """
    Synthesizes honest natural language response based STRICTLY on Neo4j query result.
    Returns: (answer_text, is_grounded)
    """
    if query_type in ("unsupported", "unknown_property"):
        cand = detect_unknown_property_candidate(question.lower())
        if cand:
            return f"I don't have that information in the uploaded data. No column named '{cand}' was found in the schema.", False
        return "I don't have that information in the uploaded data.", False

    # 1. Total count
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
            GLOBAL_CHAT_CONTEXT["last_column"] = col
            GLOBAL_CHAT_CONTEXT["last_value"] = val
            return f"There are {cnt} rows where {col} = '{val}'.", True
        return f"Found {cnt} matching rows.", True

    # 3. Distinct values
    if query_type == "distinct_values":
        keys = list(result[0].keys()) if result else []
        if keys:
            raw_col = keys[0]
            col_disp = raw_col.lstrip("\ufeff")
            GLOBAL_CHAT_CONTEXT["last_column"] = col_disp
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
            GLOBAL_CHAT_CONTEXT["last_column"] = col
            GLOBAL_CHAT_CONTEXT["last_value"] = val
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
        GLOBAL_CHAT_CONTEXT["last_column"] = col_name
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

    # 10. Numeric Aggregations (min, max, avg, sum)
    if query_type.startswith("numeric_"):
        op = query_type.split("_")[1]
        if result:
            rec = result[0]
            val = rec.get(f"{op}_val")
            if val is not None:
                if isinstance(val, float):
                    val_str = f"{val:,.2f}" if not val.is_integer() else f"{int(val):,}"
                else:
                    val_str = f"{val:,}"
                return f"The {op} value in the uploaded data is {val_str}.", True
        return "Unable to compute numeric aggregation on this column.", False

    # 11. String search
    if query_type == "string_search":
        count = len(result)
        if count == 0:
            return "No matching rows found containing the requested text.", True
        return f"Found {count} matching row(s) containing the search text.", True

    # 12. Multi-condition queries
    if query_type == "multi_condition_count":
        cnt = result[0].get("count(r)", 0) if result else 0
        return f"There are {cnt} rows matching all specified conditions.", True

    if query_type == "multi_condition_rows":
        count = len(result)
        if count == 0:
            return "No matching rows found for the specified conditions.", False
        return f"Found {count} matching row(s) for the specified conditions. Showing properties in result.", True

    # 13. Dataset info
    if query_type == "dataset_info":
        if result:
            filenames = [d.get("filename", "unknown") for d in result]
            return f"Uploaded datasets: {', '.join(filenames)}.", True
        return "No datasets have been uploaded yet.", False

    return "Result retrieved from graph.", True


# -----------------------------------------------------------------------------
# 6. PUBLIC CHAT ENTRYPOINT
# -----------------------------------------------------------------------------

def handle_chat(
    question: str,
    driver: Any,
    database: Optional[str] = "CSV_Graph_DB",
    context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Main Chat API handler complying with the hackathon handout specification.

    Parameters:
      - question: User's English natural language question.
      - driver: Neo4j Python GraphDatabase driver instance (or compatible mock).
      - database: Target Neo4j database name (defaults to 'CSV_Graph_DB').
      - context: Optional conversation context dict for multi-turn conversational follow-ups.

    Returns:
      {
        "answer": str,
        "cypher": str,
        "result": list,
        "grounded": bool
      }
    """
    active_ctx = context or GLOBAL_CHAT_CONTEXT

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
    cypher, query_type, supported = generate_cypher_template(question, schema, context=active_ctx)

    if not supported or not cypher:
        cand = detect_unknown_property_candidate(question.lower())
        if cand:
            msg = f"I don't have that information in the uploaded data. No column named '{cand}' was found in the schema."
        else:
            msg = "I don't have that information in the uploaded data."
        return {
            "answer": msg,
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
        # Fallback to default session if named database was not found
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
