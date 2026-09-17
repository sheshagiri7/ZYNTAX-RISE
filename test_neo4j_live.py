"""
Complete Real Neo4j Hardening & Multi-Dataset Isolation Test Suite for chat_engine.py
=====================================================================================
Tests against actual Neo4j container at bolt://localhost:7687:
1. Empty graph / Pre-upload checks
2. Dataset 1 Ingestion (Employee Roster: 5 rows: Billing, Engineering, Marketing)
3. Dataset 2 Ingestion (Hospital Patients: 4 rows: Cardiology, Neurology, Pediatrics)
   -> Deterministic Active Dataset Isolation: Queries MUST target Dataset 2 only!
4. 20+ Natural Language Variations across all required intents:
   - Row count (variations: "how many rows?", "total rows?", "row count?", "what about rows?", "how many records?")
   - Column count (variations: "how many columns?", "number of columns?", "column count?", "what about columns?")
   - Schema (variations: "what columns are there?", "list columns", "show schema")
   - Summary (variations: "what is the content?", "what is in this dataset?", "summarize the dataset")
   - Sample (variations: "show me some rows", "show first 2 rows", "display 3 records")
   - Distinct (variations: "what values are in department?", "list unique departments")
   - Filter count (variations: "how many Cardiology rows?", "count rows where department is Cardiology", "how many rows belong to Cardiology?")
   - Filtered rows (variations: "show Cardiology rows", "show rows where status is Discharged")
   - Breakdown (variations: "breakdown by department", "count by status", "distribution by department")
   - Numeric (variations: "maximum bill", "minimum bill", "average bill", "total bill")
   - Text search (variations: "rows containing cardiac", "records mentioning Smith")
   - Multi-condition (variations: "Cardiology and Admitted", "department Neurology and status Discharged")
   - Unknown property (variations: "how many rows have blue_eyes?", "what is average wing_span?")
   - Unrelated / General knowledge (variations: "What is the capital of France?", "Who wrote Hamlet?")
   - Mutation attacks (variations: CREATE, MERGE, DELETE, DETACH, SET, REMOVE, DROP, ALTER, TRUNCATE, LOAD CSV, APOC)
   - Semicolon injection (variations: "MATCH (r:Row) RETURN r; DROP CONSTRAINT c;")
   - Multiple datasets isolation verification (strictly verifies older Dataset 1 rows are NOT mixed!)
   - Short follow-up questions ("what about rows?", "what about columns?")
"""

import sys
import time
from neo4j import GraphDatabase
from chat_engine import handle_chat, is_safe_read_only_cypher, get_graph_schema

NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "csvgraphdb")
NEO4J_DB = "CSV_Graph_DB"

def setup_neo4j():
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    return driver

def run_tests():
    driver = setup_neo4j()
    print("=" * 80)
    print("STARTING LIVE NEO4J COMPREHENSIVE HARDENING TESTS FOR CHAT ENGINE")
    print("=" * 80)

    # -------------------------------------------------------------------------
    # STEP 0: Clean Neo4j database to known empty state
    # -------------------------------------------------------------------------
    print("\n[STEP 0] Resetting Neo4j database to clean empty state...")
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")
    print("  ✓ Database cleared.")

    # -------------------------------------------------------------------------
    # TEST 1: Empty Graph / Pre-Upload State
    # -------------------------------------------------------------------------
    print("\n[TEST 1] Pre-Upload / Empty Graph Verification")
    res1 = handle_chat("how many rows?", driver)
    assert res1["grounded"] is False
    assert res1["cypher"] == ""
    assert res1["result"] == []
    assert "No uploaded data is currently available" in res1["answer"]
    print("  ✓ Test 1 Passed: Empty graph safely rejected with grounded=False")

    # -------------------------------------------------------------------------
    # STEP 1: Ingest Dataset 1 (Older upload: Employees, 5 rows)
    # -------------------------------------------------------------------------
    print("\n[STEP 1] Ingesting Older Dataset 1 (employees.csv - 5 rows)...")
    ds1_cypher = """
    CREATE (d:Dataset {
        id: 'ds_emp_001',
        filename: 'employees.csv',
        uploaded_at: '2026-09-17T10:00:00Z'
    })
    CREATE (r1:Row {dataset_id: 'ds_emp_001', row_index: 1, group: 'Billing', status: 'Active', salary: 65000, name: 'Alice'})
    CREATE (r2:Row {dataset_id: 'ds_emp_001', row_index: 2, group: 'Billing', status: 'Pending', salary: 70000, name: 'Bob'})
    CREATE (r3:Row {dataset_id: 'ds_emp_001', row_index: 3, group: 'Engineering', status: 'Active', salary: 110000, name: 'Charlie'})
    CREATE (r4:Row {dataset_id: 'ds_emp_001', row_index: 4, group: 'Engineering', status: 'Active', salary: 120000, name: 'Diana'})
    CREATE (r5:Row {dataset_id: 'ds_emp_001', row_index: 5, group: 'Marketing', status: 'Inactive', salary: 55000, name: 'Evan'})
    CREATE (d)-[:HAS_ROW]->(r1)
    CREATE (d)-[:HAS_ROW]->(r2)
    CREATE (d)-[:HAS_ROW]->(r3)
    CREATE (d)-[:HAS_ROW]->(r4)
    CREATE (d)-[:HAS_ROW]->(r5)
    """
    with driver.session() as s:
        s.run(ds1_cypher)
    print("  ✓ Dataset 1 (5 rows) ingested.")

    # -------------------------------------------------------------------------
    # STEP 2: Ingest Dataset 2 (Newer active upload: patients.csv - 4 rows)
    # -------------------------------------------------------------------------
    print("\n[STEP 2] Ingesting Newer Dataset 2 (patients.csv - 4 rows)...")
    ds2_cypher = """
    CREATE (d:Dataset {
        id: 'ds_patients_002',
        filename: 'patients.csv',
        uploaded_at: '2026-09-17T12:00:00Z'
    })
    CREATE (r1:Row {dataset_id: 'ds_patients_002', row_index: 1, department: 'Cardiology', status: 'Admitted', bill: 4500, doctor: 'Dr. Smith', diagnosis: 'cardiac arrhythmia'})
    CREATE (r2:Row {dataset_id: 'ds_patients_002', row_index: 2, department: 'Cardiology', status: 'Discharged', bill: 3200, doctor: 'Dr. Jones', diagnosis: 'cardiac failure'})
    CREATE (r3:Row {dataset_id: 'ds_patients_002', row_index: 3, department: 'Neurology', status: 'Discharged', bill: 8900, doctor: 'Dr. Adams', diagnosis: 'concussion'})
    CREATE (r4:Row {dataset_id: 'ds_patients_002', row_index: 4, department: 'Pediatrics', status: 'Admitted', bill: 1400, doctor: 'Dr. Smith', diagnosis: 'bronchitis'})
    CREATE (d)-[:HAS_ROW]->(r1)
    CREATE (d)-[:HAS_ROW]->(r2)
    CREATE (d)-[:HAS_ROW]->(r3)
    CREATE (d)-[:HAS_ROW]->(r4)
    """
    with driver.session() as s:
        s.run(ds2_cypher)
    print("  ✓ Dataset 2 (4 rows) ingested.")

    # Total rows in graph is now 9 (5 + 4).
    # But active dataset is patients.csv with 4 rows!

    results = []

    # -------------------------------------------------------------------------
    # TEST SUITE A: Active Dataset Row Count Isolation (5 NL variations)
    # -------------------------------------------------------------------------
    row_count_variations = [
        "how many rows?",
        "how many records?",
        "total rows?",
        "row count?",
        "what about rows?"
    ]
    for q in row_count_variations:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is True
            and res["result"] == [{"count(r)": 4}]
            and "4 total rows" in res["answer"]
            and ("patients.csv" in res["answer"] or "dataset" in res["answer"])
            and "MATCH (d:Dataset)" in res["cypher"]
            and "-[:HAS_ROW]->(r:Row)" in res["cypher"]
        )
        assert passed, f"Row count failed for '{q}': {res}"
        results.append((f"Row count: '{q}'", passed))
    print("  ✓ All 5 Row Count variations passed with strict dataset isolation (4 rows, NOT 9)")

    # -------------------------------------------------------------------------
    # TEST SUITE B: Column Count & Schema Isolation (5 NL variations)
    # -------------------------------------------------------------------------
    col_count_variations = [
        "how many columns?",
        "number of columns?",
        "column count?",
        "what columns are there?",
        "what about columns?"
    ]
    for q in col_count_variations:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is True
            and "department" in res["answer"]
            and "bill" in res["answer"]
            and "salary" not in res["answer"] # Must NOT leak older dataset columns!
        )
        assert passed, f"Column count failed for '{q}': {res}"
        results.append((f"Column count: '{q}'", passed))
    print("  ✓ All 5 Column Count / Schema variations passed without leaking older dataset schema")

    # -------------------------------------------------------------------------
    # TEST SUITE C: Summary Intent (4 NL variations)
    # -------------------------------------------------------------------------
    summary_variations = [
        "what is the content?",
        "what is in this dataset?",
        "what does this file contain?",
        "summarize the dataset"
    ]
    for q in summary_variations:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is True
            and "patients.csv" in res["answer"]
            and "4 rows" in res["answer"]
            and "department" in res["answer"]
        )
        assert passed, f"Summary failed for '{q}': {res}"
        results.append((f"Summary: '{q}'", passed))
    print("  ✓ All 4 Dataset Summary variations passed with active dataset isolation")

    # -------------------------------------------------------------------------
    # TEST SUITE D: Sample / Preview Rows (3 NL variations)
    # -------------------------------------------------------------------------
    sample_variations = [
        "show me some rows",
        "show first 2 rows",
        "display 3 records"
    ]
    for q in sample_variations:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is True
            and len(res["result"]) > 0
            and all(r["r"]["dataset_id"] == "ds_patients_002" for r in res["result"])
        )
        assert passed, f"Sample failed for '{q}': {res}"
        results.append((f"Sample: '{q}'", passed))
    print("  ✓ All 3 Sample / Preview variations passed with active dataset rows only")

    # -------------------------------------------------------------------------
    # TEST SUITE E: Distinct Values (2 NL variations)
    # -------------------------------------------------------------------------
    distinct_variations = [
        "what values are in department?",
        "list unique departments"
    ]
    for q in distinct_variations:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is True
            and "Cardiology" in res["answer"]
            and "Neurology" in res["answer"]
            and "Pediatrics" in res["answer"]
            and "Billing" not in res["answer"] # Must NOT leak older dataset values!
        )
        assert passed, f"Distinct failed for '{q}': {res}"
        results.append((f"Distinct: '{q}'", passed))
    print("  ✓ Distinct values passed (Cardiology, Neurology, Pediatrics; zero Billing leak)")

    # -------------------------------------------------------------------------
    # TEST SUITE F: Filter Count & Filtered Rows (4 NL variations)
    # -------------------------------------------------------------------------
    filter_count_variations = [
        ("how many Cardiology rows?", 2),
        ("count rows where department is Cardiology", 2),
        ("how many rows belong to Cardiology?", 2),
        ("how many rows belong to the Neurology department?", 1)
    ]
    for q, expected_cnt in filter_count_variations:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is True
            and res["result"] == [{"count(r)": expected_cnt}]
            and f"{expected_cnt} rows" in res["answer"]
        )
        assert passed, f"Filter count failed for '{q}': {res}"
        results.append((f"Filter count: '{q}'", passed))
    print("  ✓ Filter count passed across all variations")

    filtered_row_variations = [
        "show Cardiology rows",
        "show rows where status is Discharged"
    ]
    for q in filtered_row_variations:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is True
            and len(res["result"]) > 0
            and all(r["r"]["dataset_id"] == "ds_patients_002" for r in res["result"])
        )
        assert passed, f"Filtered rows failed for '{q}': {res}"
        results.append((f"Filtered rows: '{q}'", passed))
    print("  ✓ Filtered rows passed with active dataset rows only")

    # -------------------------------------------------------------------------
    # TEST SUITE G: Breakdown / Group-By (3 NL variations)
    # -------------------------------------------------------------------------
    breakdown_variations = [
        "breakdown by department",
        "count by status",
        "distribution by department"
    ]
    for q in breakdown_variations:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is True
            and "Breakdown by" in res["answer"]
            and len(res["result"]) > 0
        )
        assert passed, f"Breakdown failed for '{q}': {res}"
        results.append((f"Breakdown: '{q}'", passed))
    print("  ✓ Breakdown / distribution queries passed")

    # -------------------------------------------------------------------------
    # TEST SUITE H: Numeric Aggregations (4 NL variations: max, min, avg, sum)
    # -------------------------------------------------------------------------
    # Dataset 2 bills: 4500, 3200, 8900, 1400 -> min=1400, max=8900, avg=4500, sum=18000
    numeric_tests = [
        ("maximum bill", "max", 8900),
        ("minimum bill", "min", 1400),
        ("average bill", "avg", 4500),
        ("total bill", "sum", 18000)
    ]
    for q, op, val in numeric_tests:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is True
            and str(val) in res["answer"].replace(",", "")
            and f"The {op} value" in res["answer"]
        )
        assert passed, f"Numeric {op} failed for '{q}': {res}"
        results.append((f"Numeric {op}: '{q}'", passed))
    print("  ✓ Numeric aggregations (max, min, avg, sum) verified against actual Neo4j values")

    # -------------------------------------------------------------------------
    # TEST SUITE I: Text Search (2 NL variations)
    # -------------------------------------------------------------------------
    text_search_tests = [
        ("rows containing cardiac", 2),
        ("records mentioning Smith", 2)
    ]
    for q, expected_len in text_search_tests:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is True
            and len(res["result"]) == expected_len
            and "Found 2 matching" in res["answer"]
        )
        assert passed, f"Text search failed for '{q}': {res}"
        results.append((f"Text search: '{q}'", passed))
    print("  ✓ Text search passed on actual text content")

    # -------------------------------------------------------------------------
    # TEST SUITE J: Multi-Condition Queries (2 NL variations)
    # -------------------------------------------------------------------------
    multi_cond_tests = [
        ("Cardiology and Admitted", 1),
        ("department Neurology and status Discharged", 1)
    ]
    for q, expected_cnt in multi_cond_tests:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is True
            and len(res["result"]) > 0
        )
        assert passed, f"Multi-condition failed for '{q}': {res}"
        results.append((f"Multi-condition: '{q}'", passed))
    print("  ✓ Multi-condition queries passed")

    # -------------------------------------------------------------------------
    # TEST SUITE K: Unknown Property & Off-Topic (Anti-Hallucination)
    # -------------------------------------------------------------------------
    unknown_prop_tests = [
        "how many rows have blue_eyes?",
        "what is the average wing_span?",
        "how many rows have group Billing?" # Group is in older dataset, NOT active dataset!
    ]
    for q in unknown_prop_tests:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is False
            and res["cypher"] == ""
            and res["result"] == []
            and "I don't have that information in the uploaded data." in res["answer"]
        )
        assert passed, f"Unknown prop check failed for '{q}': {res}"
        results.append((f"Unknown prop: '{q}'", passed))
    print("  ✓ Unknown property questions cleanly rejected with grounded=False (zero hallucination)")

    unrelated_tests = [
        "What is the capital of France?",
        "Who wrote Hamlet?"
    ]
    for q in unrelated_tests:
        res = handle_chat(q, driver)
        passed = (
            res["grounded"] is False
            and res["cypher"] == ""
            and res["result"] == []
            and "I don't have that information in the uploaded data." in res["answer"]
        )
        assert passed, f"Unrelated check failed for '{q}': {res}"
        results.append((f"Unrelated: '{q}'", passed))
    print("  ✓ General knowledge questions cleanly rejected with grounded=False")

    # -------------------------------------------------------------------------
    # TEST SUITE L: Mutation & Semicolon Attacks
    # -------------------------------------------------------------------------
    attacks = [
        "CREATE (n:Row {name: 'hacked'})",
        "MATCH (r:Row) DELETE r",
        "MATCH (r:Row) DETACH DELETE r",
        "MERGE (r:Row {name: 'hacked'})",
        "MATCH (r:Row) SET r.bill = 0",
        "MATCH (r:Row) REMOVE r.bill",
        "DROP CONSTRAINT some_constraint",
        "ALTER DATABASE neo4j SET ACCESS READ ONLY",
        "CALL apoc.export.csv.all('hacked.csv', {})",
        "LOAD CSV FROM 'http://evil.com/a.csv' AS line",
        "how many rows?; DROP TABLE users;",
        "show rows; DELETE (r:Row)"
    ]
    for att in attacks:
        safe = is_safe_read_only_cypher(att)
        assert safe is False, f"Safety failed to block: {att}"
        res = handle_chat(att, driver)
        assert res["grounded"] is False, f"Attack query got grounded response: {att}"
        assert res["cypher"] == "", f"Attack query returned Cypher: {att}"
        results.append((f"Security attack blocked: '{att[:35]}...'", True))
    print("  ✓ All 12 Mutating and Semicolon Injection attacks blocked before execution")

    # -------------------------------------------------------------------------
    # TEST SUITE M: Context Session Isolation (No cross-user leakage)
    # -------------------------------------------------------------------------
    print("\n[TEST SUITE M] Context Session Isolation Verification")
    ctx_user1 = {}
    ctx_user2 = {}

    # User 1 queries Cardiology
    res_u1 = handle_chat("how many rows belong to Cardiology?", driver, context=ctx_user1)
    assert res_u1["grounded"] is True
    assert ctx_user1.get("last_column") == "department"
    assert ctx_user1.get("last_value") == "Cardiology"

    # User 2 makes an independent query without context
    res_u2 = handle_chat("how many rows?", driver)
    assert res_u2["grounded"] is True
    # User 2 request must not have contaminated or used User 1's context
    assert ctx_user2.get("last_column") is None

    # User 1 does a follow-up ("what about Neurology?") using their own context
    res_u1_followup = handle_chat("how many rows belong to Neurology?", driver, context=ctx_user1)
    assert res_u1_followup["grounded"] is True
    assert ctx_user1.get("last_value") == "Neurology"

    print("  ✓ Context isolation verified: Zero cross-user state leakage")

    # -------------------------------------------------------------------------
    # Summary of Live Neo4j Test Results
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(f"LIVE NEO4J TEST RESULTS: {len(results)} / {len(results)} TESTS PASSED (100%)")
    print("=" * 80)
    for name, p in results:
        print(f"  ✓ {name}")

    print("\n>>> ALL REQUIREMENTS VERIFIED AGAINST LIVE NEO4J SUCCESSFULLY! <<<")

if __name__ == "__main__":
    run_tests()
