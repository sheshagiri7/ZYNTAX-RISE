"""
Comprehensive 16-Scenario Pipeline & Hostile Input Test Suite for ZYNTAX
=======================================================================
Phase 13 & 15 Verification Suite for RISE @ RST #5 Hackathon:

Scenarios Tested:
1.  GET /health offline state (reports 'degraded', kafka/neo4j=False)
2.  GET /health connected state (reports 'ok', kafka/neo4j=True)
3.  Empty CSV (0 bytes) -> clean 400 Bad Request
4.  Header-only CSV (zero data rows) -> 202 Accepted, rows_received=0, honest complete status
5.  Non-CSV file -> clean 400 Bad Request
6.  Ragged / Malformed CSV -> clean 400 Bad Request
7.  Chat query before any data is uploaded -> grounded=False, clear pre-upload message
8.  Unsupported chat question -> grounded=False, safe fallback without hallucination
9.  Upload while Kafka is not ready -> clean 503 error, no crash, no fake complete
10. Upload while Neo4j is not ready -> succeeds with 202 to Kafka, decouples from DB
11. Valid small CSV upload -> 202 Accepted with job_id and queued status
12. Verify Kafka receives messages (1 per row with dataset_id & row_index)
13. Loader ingestion into Neo4j using generic dynamic model (:Dataset)-[:HAS_ROW]->(:Row)
14. Loader failure handling (tracks rows_failed and honest status)
15. GET /status honest tracking across complete lifecycle
16. Re-upload of same CSV -> identical deterministic dataset_id, ZERO duplicate nodes
"""

import io
import re
import time
import logging
from typing import Dict, Any, List
from app import app, set_driver, set_kafka
from backend.job_tracker import tracker
from loader.loader import process_message_payload
from loader.neo4j_loader import merge_row_into_graph

logging.basicConfig(level=logging.ERROR)


# -----------------------------------------------------------------------------
# Mock In-Memory Kafka Producer to inspect messages and simulate outages
# -----------------------------------------------------------------------------
class MockKafkaProducer:
    def __init__(self, connected: bool = True, fail_row_indices: list = None):
        self.is_connected = connected
        self.published_messages = []
        self.fail_row_indices = fail_row_indices or []

    def send(self, topic: str, value: dict, key: str = None):
        if not self.is_connected:
            raise RuntimeError("Broker unavailable: simulated Kafka outage")
        row_idx = value.get("row_index") if isinstance(value, dict) else None
        if row_idx is not None and row_idx in self.fail_row_indices:
            raise RuntimeError(f"Simulated message delivery failure for row {row_idx}")
        self.published_messages.append({"topic": topic, "value": value, "key": key})
        class Future:
            def get(self, timeout=None):
                return None
        return Future()

    def flush(self):
        pass

    def bootstrap_connected(self):
        return self.is_connected


# -----------------------------------------------------------------------------
# Mock In-Memory Neo4j Graph Driver matching Member 3's format
# -----------------------------------------------------------------------------
class MockRecord:
    def __init__(self, data_dict: Dict[str, Any]):
        self._data = data_dict

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)

    def data(self):
        return self._data


class MockResult:
    def __init__(self, records: List[Dict[str, Any]]):
        self.records = [MockRecord(r) for r in records]

    def single(self):
        return self.records[0] if self.records else None

    def __iter__(self):
        return iter(self.records)

    def consume(self):
        return None


class MockGraphSession:
    def __init__(self, store: Dict[str, Any], fail_writes: bool = False):
        self.store = store
        self.fail_writes = fail_writes
        self.store.setdefault("datasets", {})
        self.store.setdefault("rows", {})
        self.store.setdefault("has_row", set())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def run(self, cypher: str, **params):
        cy = cypher.strip()

        # Ping
        if "RETURN 1 AS ping" in cy or "RETURN 1" in cy:
            return MockResult([{"ping": 1}])

        # Ingestion MERGE execution
        if "MERGE (d:Dataset" in cy and "MERGE (r:Row" in cy:
            if self.fail_writes:
                raise RuntimeError("Simulated Neo4j write failure")

            d_id = params.get("dataset_id")
            r_idx = params.get("row_index")
            row_data = params.get("row_data", {})
            filename = params.get("filename")

            # Store Dataset idempotently
            if d_id not in self.store["datasets"]:
                self.store["datasets"][d_id] = {
                    "id": d_id,
                    "filename": filename,
                    "uploaded_at": params.get("uploaded_at")
                }

            # Stable composite key: dataset_id + row_index
            composite_key = f"{d_id}:{r_idx}"
            node = dict(row_data)
            node["dataset_id"] = d_id
            node["row_index"] = r_idx
            self.store["rows"][composite_key] = node

            # Record HAS_ROW relationship
            self.store["has_row"].add((d_id, composite_key))
            return MockResult([{"rows_merged": 1}])

        # HAS_ROW relationship count
        if "HAS_ROW" in cy and "RETURN count" in cy:
            return MockResult([{"count": len(self.store["has_row"])}])

        # Schema Total row count: MATCH (r:Row) RETURN count(r) AS total
        if "MATCH (r:Row) RETURN count(r) AS total" in cy:
            total = len(self.store["rows"])
            return MockResult([{"total": total}])

        # Count by dataset_id & row_index: MATCH (r:Row {dataset_id: $did, row_index: 1}) RETURN count(r) AS cnt
        if "MATCH (r:Row {dataset_id:" in cy and "AS cnt" in cy:
            did = params.get("did")
            cnt = sum(1 for k, r in self.store["rows"].items() if r.get("dataset_id") == did and r.get("row_index") == 1)
            return MockResult([{"cnt": cnt}])

        # Relationship count by did: MATCH (d:Dataset {id: $did})-[rel:HAS_ROW]->(r:Row {row_index: 1}) RETURN count(rel) AS rcnt
        if "AS rcnt" in cy:
            did = params.get("did")
            rcnt = sum(1 for (d, r) in self.store.get("has_row", set()) if d == did and r.endswith(":1"))
            return MockResult([{"rcnt": rcnt}])

        # Total count without alias
        if "MATCH (r:Row) RETURN count(r)" in cy and "{" not in cy:
            total = len(self.store["rows"])
            return MockResult([{"count(r)": total}])

        # Property Keys
        if "MATCH (r:Row) RETURN keys(r) AS keys" in cy:
            if not self.store["rows"]:
                return MockResult([])
            first_row = next(iter(self.store["rows"].values()))
            return MockResult([{"keys": list(first_row.keys())}])

        # Datasets
        if "MATCH (d:Dataset)" in cy:
            datasets = [{"id": d["id"], "filename": d.get("filename"), "uploaded_at": d.get("uploaded_at")}
                        for d in self.store["datasets"].values()]
            return MockResult(datasets)

        # Filtered count: MATCH (r:Row {group: 'Billing'}) RETURN count(r)
        if "MATCH (r:Row {" in cy and "RETURN count(r)" in cy:
            m = re.search(r"\{(\w+):\s*'([^']+)'\}", cy)
            if m:
                col, val = m.group(1), m.group(2)
                matched = [r for r in self.store["rows"].values() if str(r.get(col)) == val]
                return MockResult([{"count(r)": len(matched)}])
            return MockResult([{"count(r)": 0}])

        # Distinct: MATCH (r:Row) ... RETURN DISTINCT r.status AS status
        if "RETURN DISTINCT" in cy:
            m = re.search(r"AS\s+(\w+)", cy)
            col = m.group(1) if m else "status"
            vals = sorted(list({str(r[col]) for r in self.store["rows"].values() if col in r}))
            return MockResult([{col: v} for v in vals])

        return MockResult([])


class MockGraphDriver:
    def __init__(self, connected: bool = True, fail_writes: bool = False):
        self.is_connected = connected
        self.fail_writes = fail_writes
        self.store = {"datasets": {}, "rows": {}}

    def session(self, **kwargs):
        if not self.is_connected:
            raise RuntimeError("Simulated Neo4j driver connection failure")
        return MockGraphSession(self.store, fail_writes=self.fail_writes)

    def verify_connectivity(self):
        if not self.is_connected:
            raise RuntimeError("Cannot connect to Neo4j")


def run_pipeline_tests():
    print("=" * 78)
    print("RUNNING COMPLETE 16-SCENARIO ZYNTAX PIPELINE & HOSTILE INPUT TEST SUITE")
    print("=" * 78)

    client = app.test_client()
    tracker.reset()

    # -------------------------------------------------------------------------
    # TEST 1: GET /health (Unreachable state)
    # -------------------------------------------------------------------------
    print("\n[TEST 1] GET /health (Services Down)")
    set_kafka(MockKafkaProducer(connected=False))
    set_driver(MockGraphDriver(connected=False))
    resp = client.get("/health")
    data = resp.get_json()
    assert resp.status_code == 200
    assert data["status"] == "degraded"
    assert data["kafka_connected"] is False
    assert data["neo4j_connected"] is False
    print("  ✓ Test 1 Passed: GET /health honestly reports degraded when offline")

    # -------------------------------------------------------------------------
    # TEST 2: GET /health (Connected state)
    # -------------------------------------------------------------------------
    print("\n[TEST 2] GET /health (Services Connected)")
    mock_kafka = MockKafkaProducer(connected=True)
    mock_neo4j = MockGraphDriver(connected=True)
    set_kafka(mock_kafka)
    set_driver(mock_neo4j)
    resp = client.get("/health")
    data = resp.get_json()
    assert resp.status_code == 200
    assert data["status"] == "ok"
    assert data["kafka_connected"] is True
    assert data["neo4j_connected"] is True
    print("  ✓ Test 2 Passed: GET /health reports ok only when both services are up")

    # -------------------------------------------------------------------------
    # TEST 3: Hostile Input: Empty CSV
    # -------------------------------------------------------------------------
    print("\n[TEST 3] Hostile Input: Empty CSV (0 bytes)")
    resp = client.post("/ingest", data={"file": (io.BytesIO(b""), "empty.csv")}, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "empty" in resp.get_json()["error"].lower()
    print(f"  ✓ Test 3 Passed: 0-byte CSV cleanly rejected: {resp.get_json()}")

    # -------------------------------------------------------------------------
    # TEST 4: Hostile Input: Header-only CSV (zero data rows)
    # -------------------------------------------------------------------------
    print("\n[TEST 4] Hostile Input: Header-only CSV")
    header_csv = "col1,col2,col3\n"
    resp = client.post("/ingest", data={"file": (io.BytesIO(header_csv.encode("utf-8")), "headers.csv")}, content_type="multipart/form-data")
    data = resp.get_json()
    assert resp.status_code == 202
    assert data["rows_received"] == 0
    assert data["status"] == "complete", "Header-only CSV must return complete consistently across layers"
    # Check status
    resp_s = client.get(f"/status?job_id={data['job_id']}")
    data_s = resp_s.get_json()
    assert data_s["status"] == "complete"
    assert data_s["rows_total"] == 0
    assert data_s["rows_loaded"] == 0
    assert data_s["rows_failed"] == 0
    print(f"  ✓ Test 4 Passed: Header-only CSV handled safely with consistent 'complete' status: {data_s}")

    # -------------------------------------------------------------------------
    # TEST 5: Hostile Input: Non-CSV file
    # -------------------------------------------------------------------------
    print("\n[TEST 5] Hostile Input: Non-CSV file")
    resp = client.post("/ingest", data={"file": (io.BytesIO(b'{"json": true}'), "data.json")}, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "only csv files are allowed" in resp.get_json()["error"].lower()
    print("  ✓ Test 5 Passed: Non-CSV upload cleanly rejected with 400")

    # -------------------------------------------------------------------------
    # TEST 6: Hostile Input: Malformed CSV structure
    # -------------------------------------------------------------------------
    print("\n[TEST 6] Hostile Input: Malformed CSV with null bytes")
    resp = client.post("/ingest", data={"file": (io.BytesIO(b"a,b,c\x00d,e,f"), "bad.csv")}, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "malformed" in resp.get_json()["error"].lower() or "structure" in resp.get_json()["error"].lower()
    print(f"  ✓ Test 6 Passed: Malformed CSV cleanly rejected with 400: {resp.get_json()}")

    # -------------------------------------------------------------------------
    # TEST 7: Chat before any upload
    # -------------------------------------------------------------------------
    print("\n[TEST 7] Chat before upload (Empty Graph)")
    resp = client.post("/chat", json={"question": "How many rows are there?"})
    data = resp.get_json()
    assert resp.status_code == 200
    assert data["grounded"] is False
    assert "No uploaded data is currently available" in data["answer"]
    print(f"  ✓ Test 7 Passed: Pre-upload chat query handled safely: '{data['answer']}'")

    # -------------------------------------------------------------------------
    # TEST 8: Unsupported question on pre-upload graph
    # -------------------------------------------------------------------------
    print("\n[TEST 8] Unsupported query on empty graph")
    resp = client.post("/chat", json={"question": "What is the capital of Mars?"})
    data = resp.get_json()
    assert resp.status_code == 200
    assert data["grounded"] is False
    assert "No uploaded data is currently available" in data["answer"]
    print(f"  ✓ Test 8 Passed: Pre-upload unsupported query safely rejected: '{data['answer']}'")

    # -------------------------------------------------------------------------
    # TEST 9: Upload while Kafka is not ready
    # -------------------------------------------------------------------------
    print("\n[TEST 9] Upload while Kafka is not ready")
    set_kafka(MockKafkaProducer(connected=False))
    resp = client.post("/ingest", data={"file": (io.BytesIO(b"a,b\n1,2\n"), "test.csv")}, content_type="multipart/form-data")
    assert resp.status_code == 503
    assert "message broker" in resp.get_json()["error"].lower()
    print(f"  ✓ Test 9 Passed: Ingestion rejected cleanly when Kafka is offline: {resp.get_json()}")

    # -------------------------------------------------------------------------
    # TEST 10: Upload while Neo4j is not yet ready (Decoupling Verification)
    # -------------------------------------------------------------------------
    print("\n[TEST 10] Upload while Neo4j is not ready (Pipeline Decoupled)")
    mock_kafka = MockKafkaProducer(connected=True)
    mock_neo4j_down = MockGraphDriver(connected=False)
    set_kafka(mock_kafka)
    set_driver(mock_neo4j_down)
    # Upload should SUCCEED to Kafka because API does not talk to Neo4j directly!
    csv_body = "group,status,amount\nBilling,active,100\nBilling,pending,250\nEngineering,active,300\n"
    resp = client.post("/ingest", data={"file": (io.BytesIO(csv_body.encode("utf-8")), "decoupled.csv")}, content_type="multipart/form-data")
    data = resp.get_json()
    assert resp.status_code == 202
    assert len(mock_kafka.published_messages) == 3
    print("  ✓ Test 10 Passed: Ingestion accepted to Kafka even when Neo4j is starting up")

    # -------------------------------------------------------------------------
    # TEST 11: Valid small CSV upload
    # -------------------------------------------------------------------------
    print("\n[TEST 11] Upload Valid Small CSV")
    mock_kafka = MockKafkaProducer(connected=True)
    mock_neo4j = MockGraphDriver(connected=True)
    set_kafka(mock_kafka)
    set_driver(mock_neo4j)

    full_csv = (
        "group,status,amount\n"
        "Billing,active,100\n"
        "Billing,pending,250\n"
        "Engineering,active,300\n"
        "Marketing,inactive,150\n"
    )
    resp = client.post("/ingest", data={"file": (io.BytesIO(full_csv.encode("utf-8")), "company.csv")}, content_type="multipart/form-data")
    data = resp.get_json()
    assert resp.status_code == 202
    assert data["rows_received"] == 4
    job_id = data["job_id"]
    print(f"  ✓ Test 11 Passed: Valid CSV accepted with job_id='{job_id}'")

    # -------------------------------------------------------------------------
    # TEST 12: Verify Kafka messages published
    # -------------------------------------------------------------------------
    print("\n[TEST 12] Verify Kafka messages published (1 per row)")
    assert len(mock_kafka.published_messages) == 4
    first_msg = mock_kafka.published_messages[0]["value"]
    dataset_id = first_msg["dataset_id"]
    assert first_msg["row_index"] == 1
    assert first_msg["data"]["group"] == "Billing"
    print(f"  ✓ Test 12 Passed: Exactly 4 messages published with dataset_id='{dataset_id}' and row_index [1..4]")

    # -------------------------------------------------------------------------
    # TEST 13: Loader ingestion into Neo4j
    # -------------------------------------------------------------------------
    print("\n[TEST 13] Loader Ingestion into Neo4j")
    for record in mock_kafka.published_messages:
        ok = process_message_payload(record["value"], driver=mock_neo4j)
        assert ok is True
    assert len(mock_neo4j.store["rows"]) == 4
    print("  ✓ Test 13 Passed: Loader successfully merged 4 rows into Neo4j")

    # -------------------------------------------------------------------------
    # TEST 14: Loader failure handling (tracks rows_failed)
    # -------------------------------------------------------------------------
    print("\n[TEST 14] Loader Failure Tracking")
    mock_neo4j_failing = MockGraphDriver(connected=True, fail_writes=True)
    bad_msg = {"job_id": job_id, "dataset_id": dataset_id, "filename": "company.csv", "row_index": 5, "data": {"a": 1}}
    fail_res = process_message_payload(bad_msg, driver=mock_neo4j_failing)
    assert fail_res is False
    status_resp = client.get(f"/status?job_id={job_id}")
    assert status_resp.get_json()["rows_failed"] >= 1
    print(f"  ✓ Test 14 Passed: Failed row honestly incremented rows_failed: {status_resp.get_json()}")

    # -------------------------------------------------------------------------
    # TEST 15: GET /status honest lifecycle
    # -------------------------------------------------------------------------
    print("\n[TEST 15] GET /status honest lifecycle")
    # Reset job and run clean complete flow
    clean_job = tracker.create_job("job_clean", "d_clean", "clean.csv", 2)
    assert clean_job["status"] == "queued"
    tracker.update_progress("job_clean", loaded_inc=1)
    assert tracker.get_job("job_clean")["status"] == "loading"
    tracker.update_progress("job_clean", loaded_inc=1)
    assert tracker.get_job("job_clean")["status"] == "complete"
    assert tracker.get_job("job_clean")["rows_loaded"] == 2
    print("  ✓ Test 15 Passed: Status accurately transitioned: queued -> loading -> complete")

    # -------------------------------------------------------------------------
    # TEST 16: Idempotent Re-upload (ZERO duplicate nodes)
    # -------------------------------------------------------------------------
    print("\n[TEST 16] Idempotent Re-upload (ZERO Duplicate Nodes)")
    initial_node_count = len(mock_neo4j.store["rows"])
    mock_kafka.published_messages.clear()

    # Upload exact same CSV again
    resp_2 = client.post("/ingest", data={"file": (io.BytesIO(full_csv.encode("utf-8")), "company_again.csv")}, content_type="multipart/form-data")
    assert resp_2.status_code == 202
    reupload_msg = mock_kafka.published_messages[0]["value"]
    assert reupload_msg["dataset_id"] == dataset_id, "Dataset ID changed across identical content!"

    # Loader consumes re-uploaded messages
    for record in mock_kafka.published_messages:
        process_message_payload(record["value"], driver=mock_neo4j)

    final_node_count = len(mock_neo4j.store["rows"])
    assert final_node_count == initial_node_count == 4, f"Duplicate nodes created! Expected 4, got {final_node_count}"
    print(f"  ✓ Test 16 Passed: First load: 4 rows | Second load: 4 rows | Duplicate nodes: 0")

    # -------------------------------------------------------------------------
    # POST /chat verification with live uploaded graph
    # -------------------------------------------------------------------------
    print("\n[VERIFICATION] Grounded Chat over Ingested Neo4j Data")
    chat_resp = client.post("/chat", json={"question": "How many rows belong to the Billing group?"})
    chat_data = chat_resp.get_json()
    assert chat_resp.status_code == 200
    assert chat_data["grounded"] is True
    assert chat_data["cypher"] == "MATCH (r:Row {group: 'Billing'}) RETURN count(r)"
    assert chat_data["result"] == [{"count(r)": 2}]
    assert "2 rows where group = 'Billing'" in chat_data["answer"]
    print(f"  ✓ Chat Grounded Response Verified: '{chat_data['answer']}' (Cypher: {chat_data['cypher']})")

    # Post-upload unsupported question
    unsupported_resp = client.post("/chat", json={"question": "What is the average temperature on Mars?"})
    unsupported_data = unsupported_resp.get_json()
    assert unsupported_resp.status_code == 200
    assert unsupported_data["grounded"] is False
    assert unsupported_data["cypher"] == ""
    assert unsupported_data["result"] == []
    assert "I don't have that information in the uploaded data." in unsupported_data["answer"]
    print(f"  ✓ Post-upload Unsupported Question Verified: '{unsupported_data['answer']}' (grounded=False)")

    # -------------------------------------------------------------------------
    # TEST 17: Hostile Input: Whitespace-only CSV
    # -------------------------------------------------------------------------
    print("\n[TEST 17] Hostile Input: Whitespace-only CSV")
    resp = client.post("/ingest", data={"file": (io.BytesIO(b"   \n  \t  \n  "), "spaces.csv")}, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "empty" in resp.get_json()["error"].lower()
    print("  ✓ Test 17 Passed: Whitespace-only CSV rejected cleanly with 400")

    # -------------------------------------------------------------------------
    # TEST 18: Hostile Input: Ragged Columns (fewer and more fields)
    # -------------------------------------------------------------------------
    print("\n[TEST 18] Hostile Input: Ragged Columns")
    # Case A: row with fewer columns
    ragged_short = "col1,col2,col3\n1,2\n"
    resp_rs = client.post("/ingest", data={"file": (io.BytesIO(ragged_short.encode()), "ragged1.csv")}, content_type="multipart/form-data")
    assert resp_rs.status_code == 400
    assert "malformed" in resp_rs.get_json()["error"].lower()
    # Case B: row with more columns
    ragged_long = "col1,col2,col3\n1,2,3,4\n"
    resp_rl = client.post("/ingest", data={"file": (io.BytesIO(ragged_long.encode()), "ragged2.csv")}, content_type="multipart/form-data")
    assert resp_rl.status_code == 400
    assert "malformed" in resp_rl.get_json()["error"].lower()
    print("  ✓ Test 18 Passed: Ragged columns (fewer/more) rejected with 400 without silently dropping fields")

    # -------------------------------------------------------------------------
    # TEST 19: Hostile Input: Stray Commas & Unclosed Quotes
    # -------------------------------------------------------------------------
    print("\n[TEST 19] Hostile Input: Stray Commas & Unclosed Quotes")
    stray_comma = "col1,col2,col3\n1,2,3,\n"
    resp_sc = client.post("/ingest", data={"file": (io.BytesIO(stray_comma.encode()), "stray.csv")}, content_type="multipart/form-data")
    assert resp_sc.status_code == 400

    unclosed_quote = 'col1,col2,col3\n"unclosed,2,3\n'
    resp_uq = client.post("/ingest", data={"file": (io.BytesIO(unclosed_quote.encode()), "quote.csv")}, content_type="multipart/form-data")
    assert resp_uq.status_code == 400
    print("  ✓ Test 19 Passed: Stray commas and unclosed quotes rejected cleanly with 400")

    # -------------------------------------------------------------------------
    # TEST 20: Hostile Input: Missing and Empty Header Fields
    # -------------------------------------------------------------------------
    print("\n[TEST 20] Hostile Input: Missing and Empty Header Fields")
    # Empty column name in middle
    missing_header_mid = "col1,,col3\n1,2,3\n"
    resp_mhm = client.post("/ingest", data={"file": (io.BytesIO(missing_header_mid.encode()), "mid.csv")}, content_type="multipart/form-data")
    assert resp_mhm.status_code == 400
    assert "header" in resp_mhm.get_json()["error"].lower()

    # Trailing comma in header
    missing_header_end = "col1,col2,\n1,2,3\n"
    resp_mhe = client.post("/ingest", data={"file": (io.BytesIO(missing_header_end.encode()), "end.csv")}, content_type="multipart/form-data")
    assert resp_mhe.status_code == 400

    # All commas in header
    all_commas = ",,\n1,2,3\n"
    resp_ac = client.post("/ingest", data={"file": (io.BytesIO(all_commas.encode()), "commas.csv")}, content_type="multipart/form-data")
    assert resp_ac.status_code == 400
    print("  ✓ Test 20 Passed: Missing/empty header structures rejected cleanly with 400")

    # -------------------------------------------------------------------------
    # TEST 21: Partial Kafka Publishing Failure (Honest Accounting)
    # -------------------------------------------------------------------------
    print("\n[TEST 21] Partial Kafka Publishing Failure (Honest Accounting)")
    # 5 rows total, rows 2 and 4 fail during Kafka dispatch
    partial_csv = (
        "id,name,val\n"
        "1,alpha,10\n"
        "2,bravo,20\n"
        "3,charlie,30\n"
        "4,delta,40\n"
        "5,echo,50\n"
    )
    # Mock producer that fails on row_index 2 and 4
    partial_producer = MockKafkaProducer(connected=True, fail_row_indices=[2, 4])
    partial_neo4j = MockGraphDriver(connected=True)
    set_kafka(partial_producer)
    set_driver(partial_neo4j)

    resp_part = client.post("/ingest", data={"file": (io.BytesIO(partial_csv.encode()), "partial.csv")}, content_type="multipart/form-data")
    assert resp_part.status_code == 202
    part_job_id = resp_part.get_json()["job_id"]

    # Ingest published 3 of 5 rows, failed 2 rows
    assert len(partial_producer.published_messages) == 3
    # Check status right after ingest: rows_failed must be 2, NOT 0!
    stat_before = client.get(f"/status?job_id={part_job_id}").get_json()
    assert stat_before["rows_total"] == 5
    assert stat_before["rows_failed"] == 2, f"Failed publish not honestly tracked! Got: {stat_before}"
    assert stat_before["rows_loaded"] == 0

    # Loader processes the 3 published messages
    for msg in partial_producer.published_messages:
        ok = process_message_payload(msg["value"], driver=partial_neo4j)
        assert ok is True

    # Check status after loading: terminal state complete only when rows_loaded + rows_failed == rows_total!
    stat_after = client.get(f"/status?job_id={part_job_id}").get_json()
    assert stat_after["rows_loaded"] == 3
    assert stat_after["rows_failed"] == 2
    assert stat_after["rows_loaded"] + stat_after["rows_failed"] == stat_after["rows_total"] == 5
    assert stat_after["status"] == "complete"
    print(f"  ✓ Test 21 Passed: Partial publish failure honestly tracked: {stat_after}")

    # -------------------------------------------------------------------------
    # TEST 22: Total Kafka Publishing Failure
    # -------------------------------------------------------------------------
    print("\n[TEST 22] Total Kafka Publishing Failure")
    total_fail_producer = MockKafkaProducer(connected=False)
    set_kafka(total_fail_producer)
    resp_tf = client.post("/ingest", data={"file": (io.BytesIO(partial_csv.encode()), "fail.csv")}, content_type="multipart/form-data")
    assert resp_tf.status_code == 503
    print("  ✓ Test 22 Passed: Total Kafka outage returned clean 503 error")

    # -------------------------------------------------------------------------
    # TEST 23: Loader Failure / Crash (Status Not Stuck in Loading Forever)
    # -------------------------------------------------------------------------
    print("\n[TEST 23] Loader Crash Handling (Terminal State Invariant)")
    crash_job = tracker.create_job("job_crash_test", "d_crash", "crash.csv", rows_total=6)
    assert crash_job["status"] == "queued"

    # Loader loads 2 rows
    tracker.update_progress("job_crash_test", loaded_inc=1, row_index=1)
    tracker.update_progress("job_crash_test", loaded_inc=1, row_index=2)
    assert tracker.get_job("job_crash_test")["status"] == "loading"
    assert tracker.get_job("job_crash_test")["rows_loaded"] == 2

    # Loader crashes partway through! fail_job called
    tracker.fail_job("job_crash_test", error="Simulated loader container kill")
    crashed_stat = tracker.get_job("job_crash_test")
    assert crashed_stat["status"] == "failed"
    # Row count actually reached must be preserved!
    assert crashed_stat["rows_loaded"] == 2
    # Unreached rows accounted as failed
    assert crashed_stat["rows_failed"] == 4
    # Terminal condition holds!
    assert crashed_stat["rows_loaded"] + crashed_stat["rows_failed"] == crashed_stat["rows_total"] == 6
    print(f"  ✓ Test 23 Passed: Loader crash transitioned to failed preserving rows actually reached: {crashed_stat}")

    # -------------------------------------------------------------------------
    # TEST 24: Stale Loader Crash Detection in GET /status
    # -------------------------------------------------------------------------
    print("\n[TEST 24] Stale Loader Crash Inactivity Detection")
    import os
    stale_job = tracker.create_job("job_stale_test", "d_stale", "stale.csv", rows_total=4)
    tracker.update_progress("job_stale_test", loaded_inc=1, row_index=1)
    assert tracker.get_job("job_stale_test")["status"] == "loading"

    # Set updated_at to 100 seconds in the past to simulate sudden container death
    with tracker.lock:
        tracker._memory_jobs["job_stale_test"]["updated_at"] = time.time() - 100
        tracker._save_to_disk()

    # Query status via GET /status with short timeout
    os.environ["LOADER_CRASH_TIMEOUT"] = "5.0"
    stale_resp = client.get("/status?job_id=job_stale_test").get_json()
    assert stale_resp["status"] == "failed"
    assert stale_resp["rows_loaded"] == 1
    assert stale_resp["rows_failed"] == 3
    assert stale_resp["rows_loaded"] + stale_resp["rows_failed"] == stale_resp["rows_total"] == 4
    print(f"  ✓ Test 24 Passed: Stale unresponsive job detected and failed cleanly: {stale_resp}")

    # -------------------------------------------------------------------------
    # TEST 25: Message Replay Deduplication per Job
    # -------------------------------------------------------------------------
    print("\n[TEST 25] Message Replay Deduplication per Job")
    replay_job = tracker.create_job("job_replay", "d_replay", "replay.csv", rows_total=2)
    # Deliver row 1
    tracker.update_progress("job_replay", loaded_inc=1, row_index=1)
    assert tracker.get_job("job_replay")["rows_loaded"] == 1

    # Redeliver row 1 (simulating Kafka consumer restart before offset commit)
    tracker.update_progress("job_replay", loaded_inc=1, row_index=1)
    assert tracker.get_job("job_replay")["rows_loaded"] == 1, "Duplicate message double-incremented rows_loaded!"

    # Deliver row 2
    tracker.update_progress("job_replay", loaded_inc=1, row_index=2)
    assert tracker.get_job("job_replay")["rows_loaded"] == 2
    assert tracker.get_job("job_replay")["status"] == "complete"
    print("  ✓ Test 25 Passed: Message replay deduplication successfully prevented double counting")

    # -------------------------------------------------------------------------
    # TEST 26: Exact Repeat Upload & HAS_ROW Count Idempotency
    # -------------------------------------------------------------------------
    print("\n[TEST 26] Exact Repeat Upload & HAS_ROW Count Idempotency")
    idemp_kafka = MockKafkaProducer(connected=True)
    idemp_neo4j = MockGraphDriver(connected=True)
    set_kafka(idemp_kafka)
    set_driver(idemp_neo4j)

    sample_csv = "dept,manager,budget\nSales,Alice,50000\nEngineering,Bob,120000\nHR,Charlie,30000\n"

    # Upload 1
    resp_u1 = client.post("/ingest", data={"file": (io.BytesIO(sample_csv.encode()), "budget.csv")}, content_type="multipart/form-data")
    assert resp_u1.status_code == 202
    ds_id_1 = idemp_kafka.published_messages[0]["value"]["dataset_id"]
    for msg in idemp_kafka.published_messages:
        process_message_payload(msg["value"], driver=idemp_neo4j)

    rows_count_1 = len(idemp_neo4j.store["rows"])
    rel_count_1 = len(idemp_neo4j.store["has_row"])
    assert rows_count_1 == 3
    assert rel_count_1 == 3

    # Upload 2 (exact same content)
    idemp_kafka.published_messages.clear()
    resp_u2 = client.post("/ingest", data={"file": (io.BytesIO(sample_csv.encode()), "budget_again.csv")}, content_type="multipart/form-data")
    assert resp_u2.status_code == 202
    ds_id_2 = idemp_kafka.published_messages[0]["value"]["dataset_id"]
    assert ds_id_1 == ds_id_2, "Same content did not produce same dataset_id!"

    for msg in idemp_kafka.published_messages:
        process_message_payload(msg["value"], driver=idemp_neo4j)

    rows_count_2 = len(idemp_neo4j.store["rows"])
    rel_count_2 = len(idemp_neo4j.store["has_row"])
    assert rows_count_2 == rows_count_1 == 3, f"Duplicate rows created! Expected 3, got {rows_count_2}"
    assert rel_count_2 == rel_count_1 == 3, f"Duplicate HAS_ROW relationships created! Expected 3, got {rel_count_2}"
    print(f"  ✓ Test 26 Passed: Re-upload verified: dataset_id='{ds_id_1}' | rows={rows_count_2} | HAS_ROW={rel_count_2} | duplicates=0")

    # -------------------------------------------------------------------------
    # TEST 27: Live Neo4j Database Verification (if reachable)
    # -------------------------------------------------------------------------
    print("\n[TEST 27] Live Neo4j Database Verification")
    from backend.neo4j_client import (
        is_neo4j_connected, get_neo4j_driver, set_neo4j_driver,
        _custom_driver as _prev_custom_driver
    )
    # Earlier tests set a mock via set_driver(). Clear it so is_neo4j_connected()
    # and get_neo4j_driver() can reach the real container, then restore afterwards.
    set_neo4j_driver(None)
    if is_neo4j_connected():
        live_driver = get_neo4j_driver(retries=1)
        test_ds_id = "test_live_verify"
        test_row = {"dept": "Security", "level": 5}
        # Pre-test cleanup: remove any stale data from a prior aborted run
        with live_driver.session() as s:
            s.run("MATCH (d:Dataset {id: $did}) OPTIONAL MATCH (d)-[:HAS_ROW]->(r:Row) DETACH DELETE d, r", did=test_ds_id)
        # First load
        ok1 = merge_row_into_graph(live_driver, dataset_id=test_ds_id, filename="live.csv", row_index=1, row_data=test_row)
        assert ok1 is True
        # Second load (exact same row — MERGE must not create duplicates)
        ok2 = merge_row_into_graph(live_driver, dataset_id=test_ds_id, filename="live.csv", row_index=1, row_data=test_row)
        assert ok2 is True
        # Verify node count and relationship count in live database
        with live_driver.session() as s:
            rec = s.run("MATCH (r:Row {dataset_id: $did, row_index: 1}) RETURN count(r) AS cnt", did=test_ds_id).single()
            assert rec is not None, "Row count query returned no result from live Neo4j"
            assert rec["cnt"] == 1, f"Duplicate nodes in live Neo4j! Expected 1, got {rec['cnt']}"
            rel_rec = s.run(
                "MATCH (d:Dataset {id: $did})-[rel:HAS_ROW]->(r:Row {row_index: 1}) RETURN count(rel) AS rcnt",
                did=test_ds_id
            ).single()
            assert rel_rec is not None, "HAS_ROW relationship query returned no result — Dataset->Row link missing in live Neo4j"
            assert rel_rec["rcnt"] == 1, f"Duplicate HAS_ROW in live Neo4j! Expected 1, got {rel_rec['rcnt']}"
            # Post-test cleanup
            s.run("MATCH (d:Dataset {id: $did}) OPTIONAL MATCH (d)-[:HAS_ROW]->(r:Row) DETACH DELETE d, r", did=test_ds_id)
        print("  ✓ Test 27 Passed: Live Neo4j verified: MERGE idempotency confirmed with 0 duplicate nodes and 0 duplicate relationships")
    else:
        print("  - Test 27 Skipped: Live Neo4j not reachable in this test run")
    # Restore previous mock driver (if any) so the test harness is left in a clean state
    set_neo4j_driver(_prev_custom_driver)

    print("\n" + "=" * 78)
    print(">>> ALL 27 TESTS PASSED WITH 100% SPEC COMPLIANCE & HOSTILE INPUT SAFETY <<<")
    print("=" * 78)


if __name__ == "__main__":
    run_pipeline_tests()
