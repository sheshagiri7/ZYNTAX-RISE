"""
Comprehensive Verification & Hardening Tests for ZYNTAX Chat Engine (Member 3)
=============================================================================
Tests all 9 specific scenarios required by Hackathon Handout & Master Prompt:

A. Valid count question: "How many rows are there?"
B. Filter question: "How many rows belong to the Billing group?"
C. Distinct-value question: "What groups are present?"
D. Row listing question: "Show the rows where group is Billing."
E. Unknown property: "How many employees have blue_hair?"
F. Unsupported/general knowledge question: "What is the capital of France?"
G. Empty question: ""
H. Empty graph / no uploaded dataset
I. Neo4j execution error simulation
+ Read-only Cypher safety check (blocks CREATE, MERGE, DELETE, DETACH, SET, DROP, etc.)
+ Dynamic CSV schema support (arbitrary headers: customer_id, department, salary)
"""

import sys
from typing import Any, Dict, List
from chat_engine import handle_chat, is_safe_read_only_cypher, get_graph_schema


class MockRecord:
    def __init__(self, data_dict: Dict[str, Any]):
        self._data = data_dict

    def data(self) -> Dict[str, Any]:
        return self._data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)


class MockResult:
    def __init__(self, records: List[Dict[str, Any]]):
        self.records = [MockRecord(r) for r in records]

    def single(self):
        return self.records[0] if self.records else None

    def __iter__(self):
        return iter(self.records)


class MockSession:
    def __init__(self, graph_data: Dict[str, Any], should_fail: bool = False):
        self.graph_data = graph_data
        self.should_fail = should_fail

    def __enter__(self):
        if self.should_fail:
            raise RuntimeError("Simulated Neo4j connection failure")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def run(self, cypher: str, parameters: Dict[str, Any] = None):
        if self.should_fail:
            raise RuntimeError("Simulated Neo4j query error")

        cypher_stripped = cypher.strip()

        # Row count
        if "AS total" in cypher_stripped:
            total = len(self.graph_data.get("rows", []))
            return MockResult([{"total": total}])

        # Total row count
        if "total_rows" in cypher_stripped and "size(columns)" not in cypher_stripped:
            total = len(self.graph_data.get("rows", []))
            datasets = self.graph_data.get("datasets", [])
            fn = datasets[0].get("filename", "test_data.csv") if datasets else "test_data.csv"
            return MockResult([{"total_rows": total, "filename": fn}])

        if "RETURN count(r)" in cypher_stripped and "{" not in cypher_stripped:
            total = len(self.graph_data.get("rows", []))
            return MockResult([{"count(r)": total}])

        # Column count & Schema info
        if "size(columns)" in cypher_stripped or "column_count" in cypher_stripped:
            rows = self.graph_data.get("rows", [])
            cols = [k for k in rows[0].keys() if k not in ("dataset_id", "row_index")] if rows else []
            datasets = self.graph_data.get("datasets", [])
            fn = datasets[0].get("filename", "test_data.csv") if datasets else "test_data.csv"
            return MockResult([{"column_count": len(cols), "columns": cols, "filename": fn}])

        # Keys
        if "RETURN keys(r) AS keys" in cypher_stripped:
            if not self.graph_data.get("rows"):
                return MockResult([])
            keys = list(self.graph_data["rows"][0].keys())
            return MockResult([{"keys": keys}])

        # Dataset summary: MATCH (d:Dataset) ... total_rows ...
        if "sample_rows" in cypher_stripped or ("MATCH (d:Dataset)" in cypher_stripped and "total_rows" in cypher_stripped):
            datasets = self.graph_data.get("datasets", [{"filename": "test_data.csv"}])
            fn = datasets[0].get("filename", "test_data.csv") if datasets else "test_data.csv"
            rows = self.graph_data.get("rows", [])
            cols = [k for k in rows[0].keys() if k not in ("dataset_id", "row_index")] if rows else []
            return MockResult([{"filename": fn, "total_rows": len(rows), "column_count": len(cols), "columns": cols, "sample_rows": rows[:3]}])

        # Datasets
        if "MATCH (d:Dataset)" in cypher_stripped:
            return MockResult(self.graph_data.get("datasets", []))

        # Filtered count: MATCH (r:Row {group: 'Billing'}) RETURN count(r)
        if "MATCH (r:Row {" in cypher_stripped and "RETURN count(r)" in cypher_stripped:
            import re
            m = re.search(r"\{(\w+):\s*'([^']+)'\}", cypher_stripped)
            if m:
                col, val = m.group(1), m.group(2)
                matched = [r for r in self.graph_data.get("rows", []) if str(r.get(col)) == val]
                return MockResult([{"count(r)": len(matched)}])
            return MockResult([{"count(r)": 0}])

        # Filtered rows: MATCH (r:Row {group: 'Billing'}) RETURN r LIMIT 10
        if "MATCH (r:Row {" in cypher_stripped and "RETURN r" in cypher_stripped:
            import re
            m = re.search(r"\{(\w+):\s*'([^']+)'\}", cypher_stripped)
            if m:
                col, val = m.group(1), m.group(2)
                matched = [r for r in self.graph_data.get("rows", []) if str(r.get(col)) == val]
                return MockResult([{"r": r} for r in matched[:10]])
            return MockResult([])

        # Generic preview: MATCH (r:Row) RETURN r LIMIT 5
        if "MATCH (r:Row) RETURN r" in cypher_stripped:
            rows = self.graph_data.get("rows", [])
            return MockResult([{"r": r} for r in rows[:5]])

        # Distinct: MATCH (r:Row) WHERE r.group IS NOT NULL RETURN DISTINCT r.group AS group
        if "RETURN DISTINCT" in cypher_stripped:
            import re
            m = re.search(r"AS\s+(\w+)", cypher_stripped)
            col = m.group(1) if m else "group"
            vals = sorted(list({str(r[col]) for r in self.graph_data.get("rows", []) if col in r}))
            return MockResult([{col: v} for v in vals])

        # Breakdown
        if "count(r) AS count" in cypher_stripped:
            import re
            m = re.search(r"RETURN\s+r\.(\w+)\s+AS\s+(\w+)", cypher_stripped)
            col = m.group(1) if m else "group"
            counts = {}
            for r in self.graph_data.get("rows", []):
                val = r.get(col, "Unknown")
                counts[val] = counts.get(val, 0) + 1
            return MockResult([{col: k, "count": v} for k, v in counts.items()])

        # Numeric aggregation: RETURN avg(...) AS avg_val, min(...) AS min_val, max(...) AS max_val, sum(...) AS sum_val
        if "sum_val" in cypher_stripped and "avg_val" in cypher_stripped:
            import re
            m = re.search(r"r\.(\w+)\s+IS\s+NOT\s+NULL", cypher_stripped)
            col = m.group(1) if m else None
            if col:
                vals = []
                for r in self.graph_data.get("rows", []):
                    v = r.get(col)
                    if v is not None:
                        try:
                            vals.append(float(str(v).replace(",", "").strip()))
                        except (ValueError, TypeError):
                            pass
                if vals:
                    return MockResult([{
                        "avg_val": sum(vals) / len(vals),
                        "min_val": min(vals),
                        "max_val": max(vals),
                        "sum_val": sum(vals),
                    }])
            return MockResult([])

        return MockResult([])


class MockDriver:
    def __init__(self, graph_data: Dict[str, Any], should_fail: bool = False):
        self.graph_data = graph_data
        self.should_fail = should_fail

    def session(self, **kwargs):
        return MockSession(self.graph_data, should_fail=self.should_fail)


def run_all_tests():
    print("=" * 70)
    print("STARTING COMPLETE VERIFICATION & HARDENING TESTS FOR ZYNTAX CHAT ENGINE")
    print("=" * 70)

    # Sample dataset 1 (Standard Handout dataset with group, status, amount)
    sample_rows = [
        {"row_index": 1, "dataset_id": "ds1", "group": "Billing", "status": "active", "amount": 100},
        {"row_index": 2, "dataset_id": "ds1", "group": "Billing", "status": "pending", "amount": 200},
        {"row_index": 3, "dataset_id": "ds1", "group": "Engineering", "status": "active", "amount": 300},
        {"row_index": 4, "dataset_id": "ds1", "group": "Marketing", "status": "inactive", "amount": 150},
    ]
    sample_datasets = [{"id": "ds1", "filename": "test_data.csv", "uploaded_at": "2026-09-11T10:00:00Z"}]
    active_driver = MockDriver({"rows": sample_rows, "datasets": sample_datasets})

    results = []

    # -------------------------------------------------------------
    # Test A: Valid count question
    # -------------------------------------------------------------
    q_a = "How many rows are there?"
    res_a = handle_chat(q_a, active_driver)
    pass_a = (
        res_a["grounded"] is True
        and res_a["cypher"] == "MATCH (r:Row) RETURN count(r)"
        and res_a["result"] == [{"count(r)": 4}]
        and "4 total rows" in res_a["answer"]
    )
    results.append(("A. Valid count question", q_a, pass_a, res_a))
    assert pass_a, f"Test A Failed: {res_a}"

    # -------------------------------------------------------------
    # Test B: Filter question (Matches Handout Part 4)
    # -------------------------------------------------------------
    q_b = "How many rows belong to the Billing group?"
    res_b = handle_chat(q_b, active_driver)
    pass_b = (
        res_b["grounded"] is True
        and res_b["cypher"] == "MATCH (r:Row {group: 'Billing'}) RETURN count(r)"
        and res_b["result"] == [{"count(r)": 2}]
        and "2 rows where group = 'Billing'" in res_b["answer"]
    )
    results.append(("B. Filter question (Handout)", q_b, pass_b, res_b))
    assert pass_b, f"Test B Failed: {res_b}"

    # -------------------------------------------------------------
    # Test C: Distinct-value question
    # -------------------------------------------------------------
    q_c = "What groups are present?"
    res_c = handle_chat(q_c, active_driver)
    pass_c = (
        res_c["grounded"] is True
        and "RETURN DISTINCT r.group AS group" in res_c["cypher"]
        and len(res_c["result"]) == 3
        and "Billing" in res_c["answer"]
        and "Engineering" in res_c["answer"]
    )
    results.append(("C. Distinct-value question", q_c, pass_c, res_c))
    assert pass_c, f"Test C Failed: {res_c}"

    # -------------------------------------------------------------
    # Test D: Row listing question
    # -------------------------------------------------------------
    q_d = "Show the rows where group is Billing."
    res_d = handle_chat(q_d, active_driver)
    pass_d = (
        res_d["grounded"] is True
        and res_d["cypher"] == "MATCH (r:Row {group: 'Billing'}) RETURN r LIMIT 10"
        and len(res_d["result"]) == 2
        and "Found 2 matching row(s)" in res_d["answer"]
    )
    results.append(("D. Row listing question", q_d, pass_d, res_d))
    assert pass_d, f"Test D Failed: {res_d}"

    # -------------------------------------------------------------
    # Test E: Unknown property
    # -------------------------------------------------------------
    q_e = "How many employees have blue_hair?"
    res_e = handle_chat(q_e, active_driver)
    pass_e = (
        res_e["grounded"] is False
        and res_e["cypher"] == ""
        and res_e["result"] == []
        and "I don't have that information in the uploaded data." in res_e["answer"]
    )
    results.append(("E. Unknown property", q_e, pass_e, res_e))
    assert pass_e, f"Test E Failed: {res_e}"

    # -------------------------------------------------------------
    # Test F: Unsupported / General knowledge question
    # -------------------------------------------------------------
    q_f = "What is the capital of France?"
    res_f = handle_chat(q_f, active_driver)
    pass_f = (
        res_f["grounded"] is False
        and res_f["cypher"] == ""
        and res_f["result"] == []
        and "I don't have that information in the uploaded data." in res_f["answer"]
    )
    results.append(("F. General knowledge / off-topic", q_f, pass_f, res_f))
    assert pass_f, f"Test F Failed: {res_f}"

    # -------------------------------------------------------------
    # Test G: Empty question
    # -------------------------------------------------------------
    q_g = "   "
    res_g = handle_chat(q_g, active_driver)
    pass_g = (
        res_g["grounded"] is False
        and res_g["cypher"] == ""
        and res_g["result"] == []
        and "empty" in res_g["answer"].lower()
    )
    results.append(("G. Empty question", "''", pass_g, res_g))
    assert pass_g, f"Test G Failed: {res_g}"

    # -------------------------------------------------------------
    # Test H: Empty graph / No uploaded dataset
    # -------------------------------------------------------------
    empty_driver = MockDriver({"rows": [], "datasets": []})
    q_h = "How many rows belong to the Billing group?"
    res_h = handle_chat(q_h, empty_driver)
    pass_h = (
        res_h["grounded"] is False
        and res_h["cypher"] == ""
        and res_h["result"] == []
        and "No uploaded data is currently available" in res_h["answer"]
    )
    results.append(("H. Empty graph / Pre-upload", q_h, pass_h, res_h))
    assert pass_h, f"Test H Failed: {res_h}"

    # -------------------------------------------------------------
    # Test I: Neo4j execution error
    # -------------------------------------------------------------
    failing_driver = MockDriver({"rows": sample_rows, "datasets": sample_datasets}, should_fail=True)
    q_i = "How many rows are there?"
    res_i = handle_chat(q_i, failing_driver)
    pass_i = (
        res_i["grounded"] is False
        and res_i["cypher"] == ""
        and res_i["result"] == []
        and "database error" in res_i["answer"].lower()
    )
    results.append(("I. Neo4j execution error", q_i, pass_i, res_i))
    assert pass_i, f"Test I Failed: {res_i}"

    # -------------------------------------------------------------
    # Test J: Read-Only Safety Validation
    # -------------------------------------------------------------
    mutating_queries = [
        "CREATE (n:Row {name: 'hacked'})",
        "MATCH (r:Row) DELETE r",
        "MATCH (r:Row) DETACH DELETE r",
        "MERGE (r:Row {name: 'dup'})",
        "MATCH (r:Row) SET r.amount = 9999",
        "MATCH (r:Row) REMOVE r.amount",
        "DROP CONSTRAINT some_constraint",
        "LOAD CSV FROM 'file:///evil.csv' AS row",
        "MATCH (r:Row) RETURN r; DROP TABLE users;",
        "CALL apoc.export.csv.all('bad.csv', {})",
    ]
    pass_j = True
    for q in mutating_queries:
        if is_safe_read_only_cypher(q):
            pass_j = False
            print(f"FAILED TO BLOCK UNSAFE CYPHER: {q}")
    assert pass_j, "Test J Failed: Read-only safety guard failed to block unsafe query"
    results.append(("J. Read-Only Safety Guard", "10 destructive queries", pass_j, "All 10 blocked"))

    # -------------------------------------------------------------
    # Test K: Dynamic CSV Support (arbitrary headers: department, salary)
    # -------------------------------------------------------------
    dynamic_csv_rows = [
        {"row_index": 1, "department": "Cardiology", "salary": 120000, "city": "Boston"},
        {"row_index": 2, "department": "Neurology", "salary": 140000, "city": "Boston"},
        {"row_index": 3, "department": "Cardiology", "salary": 130000, "city": "Seattle"},
    ]
    dynamic_driver = MockDriver({"rows": dynamic_csv_rows, "datasets": [{"id": "d2", "filename": "hospital.csv"}]})
    q_k1 = "How many rows belong to the Cardiology department?"
    res_k1 = handle_chat(q_k1, dynamic_driver)
    pass_k1 = (
        res_k1["grounded"] is True
        and res_k1["cypher"] == "MATCH (r:Row {department: 'Cardiology'}) RETURN count(r)"
        and "2 rows where department = 'Cardiology'" in res_k1["answer"]
    )
    
    q_k2 = "What departments are present?"
    res_k2 = handle_chat(q_k2, dynamic_driver)
    pass_k2 = (
        res_k2["grounded"] is True
        and "Cardiology" in res_k2["answer"] and "Neurology" in res_k2["answer"]
    )
    pass_k = pass_k1 and pass_k2
    results.append(("K. Dynamic CSV Support (hospital schema)", f"{q_k1} & {q_k2}", pass_k, "Both queries grounded"))
    assert pass_k, f"Test K Failed: k1={res_k1}, k2={res_k2}"

    # -------------------------------------------------------------
    # Test L: Broad dataset summary ("What is the content?")
    # -------------------------------------------------------------
    q_l = "What is the content?"
    res_l = handle_chat(q_l, active_driver)
    pass_l = (
        res_l["grounded"] is True
        and ("Dataset" in res_l["cypher"] or "Row" in res_l["cypher"])
        and ("test_data.csv" in res_l["answer"] or "dataset" in res_l["answer"])
        and "rows" in res_l["answer"]
    )
    results.append(("L. Broad dataset question", q_l, pass_l, res_l["answer"]))
    assert pass_l, f"Test L Failed: {res_l}"

    # -------------------------------------------------------------
    # Test M: Column count ("How many columns?")
    # -------------------------------------------------------------
    q_m = "How many columns?"
    res_m = handle_chat(q_m, active_driver)
    pass_m = (
        res_m["grounded"] is True
        and "column" in res_m["answer"]
        and "columns" in res_m["cypher"]
        and res_m["result"]
    )
    results.append(("M. Column count question", q_m, pass_m, res_m["answer"]))
    assert pass_m, f"Test M Failed: {res_m}"

    # -------------------------------------------------------------
    # Test N: Column schema listing ("What columns are available?")
    # -------------------------------------------------------------
    q_n = "What columns are available?"
    res_n = handle_chat(q_n, active_driver)
    pass_n = (
        res_n["grounded"] is True
        and "columns" in res_n["answer"]
        and "columns" in res_n["cypher"]
        and res_n["result"]
    )
    results.append(("N. Column listing question", q_n, pass_n, res_n["answer"]))
    assert pass_n, f"Test N Failed: {res_n}"

    # -----------------------------------------------------------------------
    # Test O: Row-Count Synonyms (Bug 1 fix — adversarial judge failures)
    # All of these must resolve to total_count with grounded=True
    # -----------------------------------------------------------------------
    row_count_synonyms = [
        "how many rows?",
        "how many records?",
        "how many entries?",
        "total rows?",
        "total records?",
        "What is the total number of records?",
        "what's the total number of records?",
        "number of records?",
        "record count?",
        "What is the row count?",
        "what is the number of records?",
    ]
    pass_o = True
    failed_o = []
    for q_syn in row_count_synonyms:
        res_syn = handle_chat(q_syn, active_driver)
        ok = (
            res_syn["grounded"] is True
            and "4" in res_syn["answer"]           # 4 rows in sample_rows
            and "MATCH (r:Row) RETURN count(r)" in res_syn["cypher"]
        )
        if not ok:
            pass_o = False
            failed_o.append((q_syn, res_syn))
    results.append(("O. Row-count synonyms (11 variants)", "adversarial row count set", pass_o,
                    f"All {len(row_count_synonyms)} passed" if pass_o else f"FAILED: {failed_o}"))
    assert pass_o, f"Test O Failed — some row-count synonyms returned wrong result: {failed_o}"

    # -----------------------------------------------------------------------
    # Test P: SUM Aggregation Synonyms (Bug 2 fix — "total <numeric col>")
    # All of these must resolve to numeric_sum with grounded=True
    # -----------------------------------------------------------------------
    # active_driver has 'amount' as a numeric column (values 100, 200, 300, 150)
    sum_synonyms = [
        ("What is the total amount?", "amount", 750.0),
        ("total of amount?", "amount", 750.0),
        ("sum of amount?", "amount", 750.0),
        ("amount total?", "amount", 750.0),
        ("what is the total amount?", "amount", 750.0),
    ]
    pass_p = True
    failed_p = []
    for q_sum, col_name, expected_sum in sum_synonyms:
        res_sum = handle_chat(q_sum, active_driver)
        ok = (
            res_sum["grounded"] is True
            and "sum_val" in res_sum["cypher"]
            and str(int(expected_sum)) in res_sum["answer"]
        )
        if not ok:
            pass_p = False
            failed_p.append((q_sum, res_sum))
    results.append(("P. SUM aggregation synonyms (5 variants)", "adversarial sum set", pass_p,
                    f"All {len(sum_synonyms)} passed" if pass_p else f"FAILED: {failed_p}"))
    assert pass_p, f"Test P Failed — some SUM synonyms returned wrong result: {failed_p}"

    # -----------------------------------------------------------------------
    # Test Q: Ambiguous "What is the total?" → grounded=False
    # "total" alone without a column name must NOT resolve to row-count or sum
    # -----------------------------------------------------------------------
    q_q = "What is the total?"
    res_q = handle_chat(q_q, active_driver)
    pass_q = res_q["grounded"] is False
    results.append(("Q. Ambiguous 'total' → grounded=False", q_q, pass_q, res_q["answer"]))
    assert pass_q, f"Test Q Failed: ambiguous 'total' should be grounded=False, got: {res_q}"

    print("\nSUMMARY OF VERIFICATION RESULTS:")
    print(f"{'Scenario':<40} | {'Status':<6} | {'Details'}")
    print("-" * 75)
    for name, inp, passed, out in results:
        status_str = "PASS" if passed else "FAIL"
        print(f"{name:<36} | {status_str:<6} | Input: {inp[:28]}")

    print("\n>>> ALL TESTS PASSED WITH 100% SPEC COMPLIANCE! <<<")


if __name__ == "__main__":
    run_all_tests()
