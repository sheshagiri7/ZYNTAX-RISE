"""
test_multi_schema.py
====================
ZYNTAX Multi-Schema Chatbot Test Suite — RISE @ RST #5

Tests chat_engine.py with 3 completely different CSV schemas to verify
the chatbot correctly adapts to any CSV structure (dynamic column discovery).

CSV Schema A: Simple employee roster — name, group, status
CSV Schema B: E-commerce orders — customer_id, order_id, amount, region
CSV Schema C: Hackathon problems — problem_statement, technology, team, status, priority
"""

import re
import sys
import logging
from collections import Counter

logging.disable(logging.CRITICAL)

try:
    from chat_engine import handle_chat
except ImportError as e:
    print(f"FATAL: Cannot import chat_engine: {e}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Mock Neo4j Infrastructure
# ---------------------------------------------------------------------------

class MockRecord:
    def __init__(self, data):
        self._data = data
    def __getitem__(self, key): return self._data[key]
    def get(self, key, default=None): return self._data.get(key, default)
    def keys(self): return self._data.keys()
    def data(self): return self._data
    def single(self): return self
    def values(self): return list(self._data.values())


class MockResult:
    def __init__(self, records):
        self._records = records or []
    def single(self):
        return self._records[0] if self._records else None
    def __iter__(self):
        return iter(self._records)


class MockSession:
    def __init__(self, dataset):
        self.dataset = dataset
    def __enter__(self): return self
    def __exit__(self, *args): pass

    def run(self, query, **params):
        q = query.strip()
        props = self.dataset["props"]
        rows  = self.dataset["rows"]
        fn    = self.dataset["filename"]
        ds_id = self.dataset["id"]
        internal = ["dataset_id", "row_index"]

        # Schema introspection
        if "RETURN count(r) AS total" in q:
            return MockResult([MockRecord({"total": len(rows)})])

        if ("MATCH (d:Dataset)" in q and "uploaded_at" in q
                and "ORDER BY" in q and "WHERE" not in q and "count" not in q.lower()):
            return MockResult([MockRecord({"id": ds_id, "filename": fn, "uploaded_at": "2026-09-01T10:00:00Z"})])

        if "keys(r) AS keys" in q:
            return MockResult([MockRecord({"keys": list(props) + internal}) for _ in range(min(3, len(rows)))])

        if "MATCH (r:Row) RETURN r LIMIT 10" in q and "WHERE" not in q:
            return MockResult([MockRecord({"r": {**r, "dataset_id": ds_id, "row_index": i}}) for i, r in enumerate(rows[:10])])

        # Column count / schema info
        if "size(columns) AS column_count" in q:
            return MockResult([MockRecord({"column_count": len(props), "columns": list(props), "filename": fn})])

        # Dataset summary
        if "total_rows" in q and "sample_rows" in q:
            sample = [{**r, "dataset_id": ds_id, "row_index": i} for i, r in enumerate(rows[:3])]
            return MockResult([MockRecord({"filename": fn, "total_rows": len(rows), "column_count": len(props),
                                           "columns": list(props), "sample_rows": sample})])

        # Total row count (exact)
        if q == "MATCH (r:Row) RETURN count(r)":
            return MockResult([MockRecord({"count(r)": len(rows)})])

        # Distinct values
        if "RETURN DISTINCT" in q:
            m = re.search(r"r\.`?(\w+)`?\s+AS\s+`?(\w+)`?", q)
            if m:
                col, alias = m.group(1), m.group(2)
                vals = list({str(r.get(col,"")) for r in rows if r.get(col) is not None})
                return MockResult([MockRecord({alias: v}) for v in vals[:25]])
            return MockResult([])

        # Filtered count {col: 'val'}
        if "RETURN count(r)" in q and "{" in q:
            m = re.search(r"\{`?(\w+)`?:\s*'([^']+)'\}", q)
            if m:
                col, val = m.group(1), m.group(2)
                cnt = sum(1 for r in rows if str(r.get(col,"")).lower() == val.lower())
                return MockResult([MockRecord({"count(r)": cnt})])
            return MockResult([MockRecord({"count(r)": 0})])

        # Filtered rows {col: 'val'}
        if "RETURN r LIMIT" in q and "{" in q:
            m = re.search(r"\{`?(\w+)`?:\s*'([^']+)'\}", q)
            if m:
                col, val = m.group(1), m.group(2)
                filtered = [r for r in rows if str(r.get(col,"")).lower() == val.lower()]
                return MockResult([MockRecord({"r": {**r, "dataset_id": ds_id}}) for r in filtered[:10]])
            return MockResult([])

        # Scoped preview rows (dataset_id filter)
        if "r.dataset_id" in q and "RETURN r LIMIT" in q:
            lim = int(q.split("LIMIT")[-1].strip()) if "LIMIT" in q else 5
            return MockResult([MockRecord({"r": {**r, "dataset_id": ds_id, "row_index": i}}) for i, r in enumerate(rows[:lim])])

        # Generic preview (no WHERE)
        if "RETURN r LIMIT" in q and "WHERE" not in q:
            lim = int(q.split("LIMIT")[-1].strip()) if "LIMIT" in q else 5
            return MockResult([MockRecord({"r": {**r, "dataset_id": ds_id, "row_index": i}}) for i, r in enumerate(rows[:lim])])

        # Group breakdown
        if "count(r) AS count" in q and "ORDER BY count" in q:
            m = re.search(r"r\.`?(\w+)`?\s+AS\s+`?(\w+)`?", q)
            if m:
                col, alias = m.group(1), m.group(2)
                counts = Counter(str(r.get(col,"")) for r in rows if r.get(col) is not None)
                return MockResult([MockRecord({alias: v, "count": c}) for v, c in counts.most_common(20)])
            return MockResult([])

        # Numeric aggregation
        if "avg(toFloat" in q or "min(toFloat" in q or "max(toFloat" in q:
            m = re.search(r"WHERE r\.`?(\w+)`? IS NOT NULL", q)
            col = m.group(1) if m else None
            if col:
                vals = []
                for r in rows:
                    try: vals.append(float(str(r.get(col,"")).replace(",","")))
                    except (ValueError, TypeError): pass
                if vals:
                    return MockResult([MockRecord({
                        "avg_val": sum(vals)/len(vals),
                        "min_val": min(vals),
                        "max_val": max(vals),
                        "sum_val": sum(vals)
                    })])
            return MockResult([])

        # String search CONTAINS
        if "CONTAINS" in q.upper():
            m = re.search(r"toLower\('([^']+)'\)", q)
            search = m.group(1).lower() if m else ""
            filtered = [r for r in rows if any(search in str(v).lower() for v in r.values())]
            return MockResult([MockRecord({"r": {**r, "dataset_id": ds_id}}) for r in filtered[:10]])

        # Multi-condition WHERE ... AND
        if "WHERE" in q and " AND " in q:
            conds = re.findall(r"r\.`?(\w+)`?\s*=\s*'([^']+)'", q)
            if "RETURN count(r)" in q:
                cnt = sum(1 for r in rows if all(str(r.get(c,"")).lower()==v.lower() for c,v in conds))
                return MockResult([MockRecord({"count(r)": cnt})])
            if "RETURN r LIMIT" in q:
                filtered = [r for r in rows if all(str(r.get(c,"")).lower()==v.lower() for c,v in conds)]
                return MockResult([MockRecord({"r": {**r, "dataset_id": ds_id}}) for r in filtered[:10]])

        return MockResult([])


class MockDriver:
    def __init__(self, dataset): self.dataset = dataset
    def session(self, **kwargs): return MockSession(self.dataset)


# ---------------------------------------------------------------------------
# Test Datasets
# ---------------------------------------------------------------------------

SCHEMA_A = {
    "name": "Employee Roster",
    "filename": "employees.csv",
    "id": "schema_a_001",
    "props": ["name", "group", "status"],
    "rows": [
        {"name": "Alice",  "group": "Engineering", "status": "active"},
        {"name": "Bob",    "group": "Billing",     "status": "pending"},
        {"name": "Carol",  "group": "Billing",     "status": "active"},
        {"name": "Dave",   "group": "Marketing",   "status": "inactive"},
        {"name": "Eve",    "group": "Engineering", "status": "active"},
    ]
}

SCHEMA_B = {
    "name": "E-Commerce Orders",
    "filename": "orders.csv",
    "id": "schema_b_001",
    "props": ["customer_id", "order_id", "amount", "region"],
    "rows": [
        {"customer_id": "C001", "order_id": "O001", "amount": 150.0,  "region": "North"},
        {"customer_id": "C002", "order_id": "O002", "amount": 320.5,  "region": "South"},
        {"customer_id": "C003", "order_id": "O003", "amount": 75.0,   "region": "North"},
        {"customer_id": "C001", "order_id": "O004", "amount": 480.0,  "region": "East"},
        {"customer_id": "C004", "order_id": "O005", "amount": 200.0,  "region": "West"},
        {"customer_id": "C002", "order_id": "O006", "amount": 900.0,  "region": "South"},
    ]
}

SCHEMA_C = {
    "name": "Hackathon Problems",
    "filename": "problems.csv",
    "id": "schema_c_001",
    "props": ["problem_statement", "technology", "team", "status", "priority"],
    "rows": [
        {"problem_statement": "Automate soil testing",    "technology": "IoT", "team": "AgriTech",  "status": "Open",        "priority": "High"},
        {"problem_statement": "Real-time fraud detection","technology": "ML",  "team": "FinTech",   "status": "In Progress", "priority": "Critical"},
        {"problem_statement": "Traffic flow optimization","technology": "AI",  "team": "SmartCity", "status": "Open",        "priority": "Medium"},
        {"problem_statement": "Healthcare chatbot",       "technology": "NLP", "team": "MedTech",   "status": "Complete",    "priority": "High"},
        {"problem_statement": "Carbon footprint tracker", "technology": "IoT", "team": "GreenTech", "status": "Open",        "priority": "Low"},
    ]
}


# ---------------------------------------------------------------------------
# Test Runner
# ---------------------------------------------------------------------------

PASS_COUNT = 0
FAIL_COUNT = 0
RESULTS = []


def test(tid, dataset, question, expected_grounded, desc):
    global PASS_COUNT, FAIL_COUNT
    driver = MockDriver(dataset)
    try:
        resp = handle_chat(question, driver, database=None)
        grounded = resp.get("grounded", False)
        answer = resp.get("answer", "")
        ok = grounded == expected_grounded
        if ok: PASS_COUNT += 1
        else:  FAIL_COUNT += 1
        RESULTS.append((tid, dataset["name"], "PASS" if ok else "FAIL",
                        desc, expected_grounded, grounded, answer[:70]))
    except Exception as e:
        FAIL_COUNT += 1
        RESULTS.append((tid, dataset["name"], "FAIL", desc,
                        expected_grounded, "ERROR", str(e)[:70]))


# ── Schema A ──────────────────────────────────────────────────────────────────
test("A1",  SCHEMA_A, "how many rows?",                       True,  "Total row count")
test("A2",  SCHEMA_A, "how many columns?",                    True,  "Column count")
test("A3",  SCHEMA_A, "what columns are available?",          True,  "Column listing")
test("A4",  SCHEMA_A, "what is the content?",                 True,  "Dataset summary")
test("A5",  SCHEMA_A, "how many rows belong to the Billing group?", True, "Filtered count")
test("A6",  SCHEMA_A, "what are the distinct values of group?",     True, "Distinct values")
test("A7",  SCHEMA_A, "show the rows where status is active", True,  "Filtered row listing")
test("A8",  SCHEMA_A, "how many employees have blue_hair?",   False, "Unknown property rejection")
test("A9",  SCHEMA_A, "what is the capital of France?",       False, "Off-topic general knowledge")
test("A10", SCHEMA_A, "show me the first 3 rows",             True,  "Preview with limit")

# ── Schema B ──────────────────────────────────────────────────────────────────
test("B1",  SCHEMA_B, "how many rows?",                        True,  "Total row count")
test("B2",  SCHEMA_B, "how many columns?",                     True,  "Column count")
test("B3",  SCHEMA_B, "what is the maximum amount?",           True,  "Numeric max")
test("B4",  SCHEMA_B, "what is the average amount?",           True,  "Numeric average")
test("B5",  SCHEMA_B, "what is the minimum amount?",           True,  "Numeric minimum")
test("B6",  SCHEMA_B, "how many rows have region equal to North?", True, "Filtered count")
test("B7",  SCHEMA_B, "what regions are present?",             True,  "Distinct values")
test("B8",  SCHEMA_B, "breakdown by region",                   True,  "Group breakdown")
test("B9",  SCHEMA_B, "show rows where region contains South", True,  "String search CONTAINS")
test("B10", SCHEMA_B, "how many rows have stock_level above 100?", False, "Unknown property")

# ── Schema C ──────────────────────────────────────────────────────────────────
test("C1",  SCHEMA_C, "how many rows?",                             True,  "Total row count")
test("C2",  SCHEMA_C, "how many columns?",                          True,  "Column count")
test("C3",  SCHEMA_C, "what technologies are used?",                True,  "Distinct values")
test("C4",  SCHEMA_C, "how many rows where priority is High?",      True,  "Filtered count")
test("C5",  SCHEMA_C, "show me rows where status is Open",          True,  "Filtered row listing")
test("C6",  SCHEMA_C, "breakdown by technology",                    True,  "Group breakdown")
test("C7",  SCHEMA_C, "show rows containing IoT",                   True,  "String search CONTAINS")
test("C8",  SCHEMA_C, "give me a summary of the uploaded data",     True,  "Dataset summary")
test("C9",  SCHEMA_C, "how many rows are there?",                   True,  "Row count alt phrasing")
test("C10", SCHEMA_C, "what columns are there in the file?",        True,  "Column listing alt")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

print("\n" + "=" * 115)
print("ZYNTAX MULTI-SCHEMA CHATBOT TEST SUITE")
print(f"Verifying dynamic column discovery across 3 CSV schemas (A=Employee, B=Orders, C=Hackathon)")
print("=" * 115)
print(f"{'ID':>3} | {'Schema':<22} | {'Status':>4} | {'Exp':>5} | {'Got':>5} | {'Description':<35} | Answer")
print("-" * 115)

all_pass = True
for (tid, schema, status, desc, exp, got, ans) in RESULTS:
    flag = "✓" if status == "PASS" else "✗"
    print(f"{flag} {tid:>3} | {schema:<22} | {status:>4} | {str(exp):>5} | {str(got):>5} | {desc:<35} | {ans}")
    if status != "PASS":
        all_pass = False

print("=" * 115)
total = PASS_COUNT + FAIL_COUNT
print(f"\nRESULT: {PASS_COUNT}/{total} tests PASSED  |  FAILED: {FAIL_COUNT}")
print()

if all_pass:
    print(">>> ALL MULTI-SCHEMA TESTS PASSED — Dynamic CSV schema adaptation VERIFIED <<<")
else:
    print(f">>> {FAIL_COUNT} TEST(S) FAILED — Review above for details. <<<")
    sys.exit(1)
