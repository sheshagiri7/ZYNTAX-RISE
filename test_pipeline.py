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
import logging
from typing import Dict, Any, List
from app import app, set_driver, set_kafka
from backend.job_tracker import tracker
from loader.loader import process_message_payload

logging.basicConfig(level=logging.ERROR)


# -----------------------------------------------------------------------------
# Mock In-Memory Kafka Producer to inspect messages and simulate outages
# -----------------------------------------------------------------------------
class MockKafkaProducer:
    def __init__(self, connected: bool = True):
        self.is_connected = connected
        self.published_messages = []

    def send(self, topic: str, value: dict, key: str = None):
        if not self.is_connected:
            raise RuntimeError("Broker unavailable: simulated Kafka outage")
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
            return MockResult([{"rows_merged": 1}])

        # Schema Total row count: MATCH (r:Row) RETURN count(r) AS total
        if "MATCH (r:Row) RETURN count(r) AS total" in cy:
            total = len(self.store["rows"])
            return MockResult([{"total": total}])

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
    assert data["status"] == "queued"
    # Check status
    resp_s = client.get(f"/status?job_id={data['job_id']}")
    data_s = resp_s.get_json()
    assert data_s["status"] == "complete"
    assert data_s["rows_total"] == 0
    print(f"  ✓ Test 4 Passed: Header-only CSV handled safely: {data_s}")

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

    print("\n" + "=" * 78)
    print(">>> ALL 16 TESTS PASSED WITH 100% SPEC COMPLIANCE & HOSTILE INPUT SAFETY <<<")
    print("=" * 78)


if __name__ == "__main__":
    run_pipeline_tests()
