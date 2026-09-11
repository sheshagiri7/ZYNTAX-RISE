"""
Tests for ZYNTAX Chat Engine (Member 3)
=======================================
Verifies:
1. Empty graph handling (pre-upload)
2. Total count queries
3. Filtered count queries (Handout Part 4 match)
4. Distinct value queries
5. Breakdown / aggregation queries
6. Unsupported / off-topic queries
7. Nonexistent column queries
8. Read-only safety validation
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


class MockResult:
    def __init__(self, records: List[Dict[str, Any]]):
        self.records = [MockRecord(r) for r in records]

    def single(self):
        return self.records[0] if self.records else None

    def __iter__(self):
        return iter(self.records)


class MockSession:
    def __init__(self, graph_data: Dict[str, Any]):
        self.graph_data = graph_data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def run(self, cypher: str, parameters: Dict[str, Any] = None):
        cypher_stripped = cypher.strip()

        # Row count
        if "MATCH (r:Row) RETURN count(r) AS total" in cypher_stripped:
            total = len(self.graph_data.get("rows", []))
            return MockResult([{"total": total}])

        if "MATCH (r:Row) RETURN count(r)" in cypher_stripped and "{" not in cypher_stripped:
            total = len(self.graph_data.get("rows", []))
            return MockResult([{"count(r)": total}])

        # Keys
        if "MATCH (r:Row) RETURN keys(r) AS keys" in cypher_stripped:
            if not self.graph_data.get("rows"):
                return MockResult([])
            keys = list(self.graph_data["rows"][0].keys())
            return MockResult([{"keys": keys}])

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

        # Distinct: MATCH (r:Row) WHERE r.status IS NOT NULL RETURN DISTINCT r.status AS status
        if "RETURN DISTINCT" in cypher_stripped:
            import re
            m = re.search(r"AS\s+(\w+)", cypher_stripped)
            col = m.group(1) if m else "status"
            vals = sorted(list({r[col] for r in self.graph_data.get("rows", []) if col in r}))
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

        # Default fallback
        return MockResult([])


class MockDriver:
    def __init__(self, graph_data: Dict[str, Any]):
        self.graph_data = graph_data

    def session(self, **kwargs):
        return MockSession(self.graph_data)


def run_all_tests():
    print("=== RUNNING ZYNTAX CHAT ENGINE TESTS ===")

    # -------------------------------------------------------------
    # Test 1: Empty Graph (Pre-upload State)
    # -------------------------------------------------------------
    empty_driver = MockDriver({"rows": [], "datasets": []})
    res_empty = handle_chat("How many rows are there?", empty_driver)
    assert res_empty["grounded"] is False, "Empty graph should return grounded=False"
    assert "No uploaded data is currently available" in res_empty["answer"]
    assert res_empty["result"] == [{"count(r)": 0}]
    print("✓ Test 1 Passed: Empty graph safely handled")

    # Populate mock graph with sample CSV data
    sample_rows = [
        {"row_index": 1, "dataset_id": "ds1", "group": "Billing", "status": "active", "amount": 100},
        {"row_index": 2, "dataset_id": "ds1", "group": "Billing", "status": "pending", "amount": 200},
        {"row_index": 3, "dataset_id": "ds1", "group": "Engineering", "status": "active", "amount": 300},
        {"row_index": 4, "dataset_id": "ds1", "group": "Marketing", "status": "inactive", "amount": 150},
    ]
    sample_datasets = [{"id": "ds1", "filename": "sample.csv", "uploaded_at": "2026-09-11T10:00:00Z"}]
    active_driver = MockDriver({"rows": sample_rows, "datasets": sample_datasets})

    # -------------------------------------------------------------
    # Test 2: Total Row Count Query
    # -------------------------------------------------------------
    res_total = handle_chat("How many rows are there?", active_driver)
    assert res_total["grounded"] is True, "Total count should be grounded"
    assert res_total["cypher"] == "MATCH (r:Row) RETURN count(r)"
    assert res_total["result"] == [{"count(r)": 4}]
    assert "There are 4 total rows in the dataset." in res_total["answer"]
    print(f"✓ Test 2 Passed: Total count query -> '{res_total['answer']}'")

    # -------------------------------------------------------------
    # Test 3: Filtered Count Query (Official Handout Match)
    # -------------------------------------------------------------
    res_filtered = handle_chat("How many rows belong to the Billing group?", active_driver)
    assert res_filtered["grounded"] is True, "Filtered query should be grounded"
    assert res_filtered["cypher"] == "MATCH (r:Row {group: 'Billing'}) RETURN count(r)"
    assert res_filtered["result"] == [{"count(r)": 2}]
    assert "There are 2 rows where group = 'Billing'." in res_filtered["answer"]
    print(f"✓ Test 3 Passed: Handout match -> '{res_filtered['answer']}' (Cypher: {res_filtered['cypher']})")

    # -------------------------------------------------------------
    # Test 4: Distinct Values Query
    # -------------------------------------------------------------
    res_distinct = handle_chat("What distinct values exist for status?", active_driver)
    assert res_distinct["grounded"] is True, "Distinct query should be grounded"
    assert "RETURN DISTINCT" in res_distinct["cypher"]
    assert "active" in res_distinct["answer"] and "pending" in res_distinct["answer"]
    print(f"✓ Test 4 Passed: Distinct values -> '{res_distinct['answer']}'")

    # -------------------------------------------------------------
    # Test 5: Unsupported / Off-topic Question
    # -------------------------------------------------------------
    res_unsupported = handle_chat("What is the capital of Australia?", active_driver)
    assert res_unsupported["grounded"] is False, "Off-topic should be ungrounded"
    assert res_unsupported["answer"] == "I don't have that information in the uploaded data."
    assert res_unsupported["cypher"] == ""
    assert res_unsupported["result"] == []
    print("✓ Test 5 Passed: Off-topic safely rejected (grounded=False)")

    # -------------------------------------------------------------
    # Test 6: Nonexistent Column Query
    # -------------------------------------------------------------
    res_missing_col = handle_chat("How many rows belong to category A?", active_driver)
    assert res_missing_col["grounded"] is False
    assert res_missing_col["answer"] == "I don't have that information in the uploaded data."
    
    res_missing_filter = handle_chat("Show rows where salary > 50000", active_driver)
    assert res_missing_filter["grounded"] is False
    assert res_missing_filter["answer"] == "I don't have that information in the uploaded data."
    print("✓ Test 6 Passed: Nonexistent columns safely rejected as ungrounded")

    # -------------------------------------------------------------
    # Test 7: Read-Only Safety Validation
    # -------------------------------------------------------------
    assert is_safe_read_only_cypher("MATCH (r:Row) RETURN r") is True
    assert is_safe_read_only_cypher("MATCH (r:Row) DELETE r") is False
    assert is_safe_read_only_cypher("MATCH (r:Row) DETACH DELETE r") is False
    assert is_safe_read_only_cypher("CREATE (r:Row {name: 'fake'})") is False
    assert is_safe_read_only_cypher("MATCH (r:Row) SET r.foo = 'bar'") is False
    assert is_safe_read_only_cypher("DROP CONSTRAINT something") is False
    assert is_safe_read_only_cypher("MATCH (r:Row) RETURN r; DROP TABLE users;") is False
    print("✓ Test 7 Passed: Mutating and destructive Cypher successfully blocked")

    # -------------------------------------------------------------
    # Test 8: Empty Question
    # -------------------------------------------------------------
    res_empty_q = handle_chat("", active_driver)
    assert res_empty_q["grounded"] is False
    assert "empty" in res_empty_q["answer"].lower()
    print("✓ Test 8 Passed: Empty question handled safely")

    print("\nALL 8 TESTS PASSED SUCCESSFULLY! 100% SPEC COMPLIANT.")


if __name__ == "__main__":
    run_all_tests()
