"""
Comprehensive Pipeline Test Suite for ZYNTAX Pipeline
======================================================
Tests all 11 required test cases matching the hackathon specifications:

1.  GET /health (both offline and connected states)
2.  POST /ingest (upload a valid small CSV)
3.  Verify Kafka receives messages (one message per row with dataset_id & row_index)
4.  Verify loader writes rows to Neo4j (idempotent graph model)
5.  GET /status?job_id=... (honest progression & row counts)
6.  POST /chat with a question supported by the CSV (grounded=True)
7.  POST /chat with a question not supported by the data (grounded=False)
8.  Upload the same CSV again (deterministic dataset_id)
9.  Verify duplicate nodes are NOT created (MERGE idempotency verification)
10. Test empty CSV (clean 400 Bad Request)
11. Test non-CSV file (clean 400 Bad Request)
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
# Mock In-Memory Kafka Producer to inspect messages
# -----------------------------------------------------------------------------
class MockKafkaProducer:
    def __init__(self, connected: bool = True):
        self.is_connected = connected
        self.published_messages = []

    def send(self, topic: str, value: dict, key: str = None):
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
    def __init__(self, store: Dict[str, Any]):
        self.store = store

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
    def __init__(self, connected: bool = True):
        self.is_connected = connected
        self.store = {"datasets": {}, "rows": {}}

    def session(self, **kwargs):
        return MockGraphSession(self.store)

    def verify_connectivity(self):
        if not self.is_connected:
            raise Exception("Cannot connect to Neo4j")


def run_pipeline_tests():
    print("=" * 75)
    print("RUNNING 11-POINT ZYNTAX PIPELINE INTEGRATION TEST SUITE")
    print("=" * 75)

    client = app.test_client()
    tracker.reset()

    # -------------------------------------------------------------------------
    # TEST 1: GET /health (Both Unreachable and Reachable States)
    # -------------------------------------------------------------------------
    print("\n[TEST 1] GET /health Verification")
    # Sub-test 1A: Offline state
    set_kafka(MockKafkaProducer(connected=False))
    set_driver(MockGraphDriver(connected=False))
    resp_health_down = client.get("/health")
    data_health_down = resp_health_down.get_json()
    assert resp_health_down.status_code == 200
    assert data_health_down["status"] == "degraded", f"Expected degraded, got: {data_health_down}"
    assert data_health_down["kafka_connected"] is False
    assert data_health_down["neo4j_connected"] is False
    print("  ✓ 1A Passed: GET /health honestly reports 'degraded' when services are disconnected")

    # Sub-test 1B: Connected state
    mock_kafka = MockKafkaProducer(connected=True)
    mock_neo4j = MockGraphDriver(connected=True)
    set_kafka(mock_kafka)
    set_driver(mock_neo4j)

    resp_health_ok = client.get("/health")
    data_health_ok = resp_health_ok.get_json()
    assert resp_health_ok.status_code == 200
    assert data_health_ok["status"] == "ok", f"Expected ok, got: {data_health_ok}"
    assert data_health_ok["kafka_connected"] is True
    assert data_health_ok["neo4j_connected"] is True
    print("  ✓ 1B Passed: GET /health reports 'ok' when Kafka & Neo4j are connected")

    # -------------------------------------------------------------------------
    # TEST 2: Upload a Valid Small CSV (POST /ingest)
    # -------------------------------------------------------------------------
    print("\n[TEST 2] Upload Valid Small CSV (POST /ingest)")
    csv_content = (
        "group,status,amount\n"
        "Billing,active,100\n"
        "Billing,pending,250\n"
        "Engineering,active,300\n"
        "Marketing,inactive,150\n"
    )
    data = {
        "file": (io.BytesIO(csv_content.encode("utf-8")), "transactions.csv")
    }
    resp_ingest = client.post("/ingest", data=data, content_type="multipart/form-data")
    data_ingest = resp_ingest.get_json()
    assert resp_ingest.status_code == 202, f"Expected 202 Accepted, got: {resp_ingest.status_code}"
    assert "job_id" in data_ingest
    assert data_ingest["rows_received"] == 4
    assert data_ingest["status"] == "queued"
    job_id = data_ingest["job_id"]
    print(f"  ✓ 2 Passed: Upload accepted with job_id='{job_id}', rows_received=4, status='queued'")

    # -------------------------------------------------------------------------
    # TEST 3: Verify Kafka Receives Messages (One per row, with dataset_id & row_index)
    # -------------------------------------------------------------------------
    print("\n[TEST 3] Verify Kafka Messages Published")
    assert len(mock_kafka.published_messages) == 4, f"Expected 4 Kafka messages, got {len(mock_kafka.published_messages)}"
    first_msg = mock_kafka.published_messages[0]["value"]
    assert first_msg["job_id"] == job_id
    assert "dataset_id" in first_msg
    assert first_msg["row_index"] == 1
    assert first_msg["data"]["group"] == "Billing"
    assert first_msg["data"]["amount"] == 100
    dataset_id = first_msg["dataset_id"]
    print(f"  ✓ 3 Passed: 4 Kafka messages published with dataset_id='{dataset_id}' and row_index [1..4]")

    # -------------------------------------------------------------------------
    # TEST 4: Verify Loader Writes Rows to Neo4j (MERGE)
    # -------------------------------------------------------------------------
    print("\n[TEST 4] Verify Loader Writes Rows to Neo4j")
    # Simulate loader consuming the 4 messages
    for record in mock_kafka.published_messages:
        msg = record["value"]
        success = process_message_payload(msg, driver=mock_neo4j)
        assert success is True, "Failed to merge row in Neo4j"

    assert len(mock_neo4j.store["rows"]) == 4, f"Expected 4 rows in Neo4j store, got {len(mock_neo4j.store['rows'])}"
    assert dataset_id in mock_neo4j.store["datasets"]
    print("  ✓ 4 Passed: Loader consumed Kafka messages and merged 4 rows into Neo4j")

    # -------------------------------------------------------------------------
    # TEST 5: GET /status?job_id=... Verification
    # -------------------------------------------------------------------------
    print("\n[TEST 5] GET /status Verification")
    resp_status = client.get(f"/status?job_id={job_id}")
    data_status = resp_status.get_json()
    assert resp_status.status_code == 200
    assert data_status["job_id"] == job_id
    assert data_status["status"] == "complete"
    assert data_status["rows_total"] == 4
    assert data_status["rows_loaded"] == 4
    assert data_status["rows_failed"] == 0
    print(f"  ✓ 5 Passed: GET /status returned honest progression: {data_status}")

    # -------------------------------------------------------------------------
    # TEST 6: POST /chat Supported Question (Grounded=True)
    # -------------------------------------------------------------------------
    print("\n[TEST 6] POST /chat Supported Question")
    resp_chat_supported = client.post("/chat", json={"question": "How many rows belong to the Billing group?"})
    data_chat_supported = resp_chat_supported.get_json()
    assert resp_chat_supported.status_code == 200
    assert data_chat_supported["grounded"] is True
    assert data_chat_supported["cypher"] == "MATCH (r:Row {group: 'Billing'}) RETURN count(r)"
    assert data_chat_supported["result"] == [{"count(r)": 2}]
    assert "2 rows where group = 'Billing'" in data_chat_supported["answer"]
    print(f"  ✓ 6 Passed: Grounded chat response: '{data_chat_supported['answer']}'")

    # -------------------------------------------------------------------------
    # TEST 7: POST /chat Unsupported Question (Grounded=False, No Hallucination)
    # -------------------------------------------------------------------------
    print("\n[TEST 7] POST /chat Unsupported Question")
    resp_chat_unsupported = client.post("/chat", json={"question": "What is the average temperature on Mars?"})
    data_chat_unsupported = resp_chat_unsupported.get_json()
    assert resp_chat_unsupported.status_code == 200
    assert data_chat_unsupported["grounded"] is False
    assert data_chat_unsupported["cypher"] == ""
    assert data_chat_unsupported["result"] == []
    assert "I don't have that information in the uploaded data." in data_chat_unsupported["answer"]
    print(f"  ✓ 7 Passed: Ungrounded query safely rejected: '{data_chat_unsupported['answer']}'")

    # -------------------------------------------------------------------------
    # TEST 8: Upload the Same CSV Again (Deterministic Dataset ID)
    # -------------------------------------------------------------------------
    print("\n[TEST 8] Re-upload Same CSV")
    data_duplicate = {
        "file": (io.BytesIO(csv_content.encode("utf-8")), "transactions_copy.csv")
    }
    resp_ingest_2 = client.post("/ingest", data=data_duplicate, content_type="multipart/form-data")
    data_ingest_2 = resp_ingest_2.get_json()
    assert resp_ingest_2.status_code == 202
    # Verify published message has the IDENTICAL dataset_id
    latest_msg = mock_kafka.published_messages[-1]["value"]
    assert latest_msg["dataset_id"] == dataset_id, "Dataset ID was not deterministic for identical content!"
    print(f"  ✓ 8 Passed: Re-uploaded CSV produced identical deterministic dataset_id='{dataset_id}'")

    # -------------------------------------------------------------------------
    # TEST 9: Verify Duplicate Nodes are NOT Created (Idempotent MERGE)
    # -------------------------------------------------------------------------
    print("\n[TEST 9] Idempotent MERGE Verification")
    # Simulate loader processing the duplicate upload messages
    for record in mock_kafka.published_messages[4:]:
        process_message_payload(record["value"], driver=mock_neo4j)

    # Graph must STILL have exactly 4 row nodes, not 8!
    assert len(mock_neo4j.store["rows"]) == 4, f"Duplication error: Expected 4 nodes, got {len(mock_neo4j.store['rows'])}"
    assert len(mock_neo4j.store["datasets"]) == 1
    print(f"  ✓ 9 Passed: Zero duplicate nodes created. Total rows in graph remain exactly 4.")

    # -------------------------------------------------------------------------
    # TEST 10: Test Empty CSV (Reject with 400 Bad Request)
    # -------------------------------------------------------------------------
    print("\n[TEST 10] Empty CSV Validation")
    empty_data = {
        "file": (io.BytesIO(b""), "empty.csv")
    }
    resp_empty = client.post("/ingest", data=empty_data, content_type="multipart/form-data")
    assert resp_empty.status_code == 400
    assert "empty" in resp_empty.get_json()["error"].lower()
    print(f"  ✓ 10 Passed: Empty CSV cleanly rejected with 400 Bad Request: {resp_empty.get_json()}")

    # -------------------------------------------------------------------------
    # TEST 11: Test Non-CSV File (Reject with 400 Bad Request)
    # -------------------------------------------------------------------------
    print("\n[TEST 11] Non-CSV File Validation")
    non_csv_data = {
        "file": (io.BytesIO(b'{"key": "value"}'), "payload.json")
    }
    resp_non_csv = client.post("/ingest", data=non_csv_data, content_type="multipart/form-data")
    assert resp_non_csv.status_code == 400
    assert "only csv files are allowed" in resp_non_csv.get_json()["error"].lower()
    print(f"  ✓ 11 Passed: Non-CSV file cleanly rejected with 400 Bad Request: {resp_non_csv.get_json()}")

    print("\n" + "=" * 75)
    print(">>> ALL 11 REQUIRED PIPELINE TESTS COMPLETED AND PASSED (100%) <<<")
    print("=" * 75)


if __name__ == "__main__":
    run_pipeline_tests()
