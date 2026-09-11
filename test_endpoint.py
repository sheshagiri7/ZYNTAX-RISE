"""
Integration Tests for POST /chat Endpoint
=========================================
Tests the live Flask application endpoint /chat using Flask's test_client.

Scenarios tested:
A. Valid count question: {"question": "How many rows are there?"}
B. Answerable filter question: {"question": "How many rows belong to the Billing group?"}
C. Unanswerable question: {"question": "How many rows have green_eyes?"}
D. Empty question: {"question": ""}
E. Request before any data exists (empty database)
F. General knowledge / off-topic question: {"question": "What is the capital of France?"}
G. Safe JSON error response on malformed input
"""

import json
from app import app, set_driver
from test_chat_engine import MockDriver


def run_endpoint_tests():
    print("=" * 70)
    print("TESTING LIVE POST /chat ENDPOINT VIA FLASK TEST CLIENT")
    print("=" * 70)

    client = app.test_client()

    sample_rows = [
        {"row_index": 1, "dataset_id": "ds1", "group": "Billing", "status": "active"},
        {"row_index": 2, "dataset_id": "ds1", "group": "Billing", "status": "pending"},
        {"row_index": 3, "dataset_id": "ds1", "group": "Engineering", "status": "active"},
    ]
    sample_datasets = [{"id": "ds1", "filename": "sample.csv"}]
    active_driver = MockDriver({"rows": sample_rows, "datasets": sample_datasets})

    results = []

    # -------------------------------------------------------------------------
    # TEST A: Valid count question
    # -------------------------------------------------------------------------
    set_driver(active_driver)
    resp_a = client.post("/chat", json={"question": "How many rows are there?"})
    data_a = resp_a.get_json()
    pass_a = (
        resp_a.status_code == 200
        and data_a["grounded"] is True
        and data_a["cypher"] == "MATCH (r:Row) RETURN count(r)"
        and data_a["result"] == [{"count(r)": 3}]
        and "3 total rows" in data_a["answer"]
    )
    results.append(("A. Total count question", resp_a.status_code, pass_a, data_a))
    assert pass_a, f"Test A Failed: {data_a}"

    # -------------------------------------------------------------------------
    # TEST B: Answerable filter question
    # -------------------------------------------------------------------------
    resp_b = client.post("/chat", json={"question": "How many rows belong to the Billing group?"})
    data_b = resp_b.get_json()
    pass_b = (
        resp_b.status_code == 200
        and data_b["grounded"] is True
        and data_b["cypher"] == "MATCH (r:Row {group: 'Billing'}) RETURN count(r)"
        and data_b["result"] == [{"count(r)": 2}]
        and "2 rows where group = 'Billing'" in data_b["answer"]
    )
    results.append(("B. Answerable filter question", resp_b.status_code, pass_b, data_b))
    assert pass_b, f"Test B Failed: {data_b}"

    # -------------------------------------------------------------------------
    # TEST C: Question that cannot be answered from the graph
    # -------------------------------------------------------------------------
    resp_c = client.post("/chat", json={"question": "How many rows have green_eyes?"})
    data_c = resp_c.get_json()
    pass_c = (
        resp_c.status_code == 200
        and data_c["grounded"] is False
        and data_c["cypher"] == ""
        and data_c["result"] == []
        and "I don't have that information in the uploaded data." in data_c["answer"]
    )
    results.append(("C. Unanswerable property question", resp_c.status_code, pass_c, data_c))
    assert pass_c, f"Test C Failed: {data_c}"

    # -------------------------------------------------------------------------
    # TEST D: Empty question
    # -------------------------------------------------------------------------
    resp_d = client.post("/chat", json={"question": ""})
    data_d = resp_d.get_json()
    pass_d = (
        resp_d.status_code == 200
        and data_d["grounded"] is False
        and data_d["cypher"] == ""
        and data_d["result"] == []
        and "empty" in data_d["answer"].lower()
    )
    results.append(("D. Empty question", resp_d.status_code, pass_d, data_d))
    assert pass_d, f"Test D Failed: {data_d}"

    # -------------------------------------------------------------------------
    # TEST E: Request made before any data exists (empty database)
    # -------------------------------------------------------------------------
    empty_driver = MockDriver({"rows": [], "datasets": []})
    set_driver(empty_driver)
    resp_e = client.post("/chat", json={"question": "How many rows are there?"})
    data_e = resp_e.get_json()
    pass_e = (
        resp_e.status_code == 200
        and data_e["grounded"] is False
        and data_e["cypher"] == ""
        and data_e["result"] == []
        and "No uploaded data is currently available" in data_e["answer"]
    )
    results.append(("E. Pre-upload / empty database", resp_e.status_code, pass_e, data_e))
    assert pass_e, f"Test E Failed: {data_e}"

    # -------------------------------------------------------------------------
    # TEST F: Off-topic / General knowledge question
    # -------------------------------------------------------------------------
    set_driver(active_driver)
    resp_f = client.post("/chat", json={"question": "What is the capital of France?"})
    data_f = resp_f.get_json()
    pass_f = (
        resp_f.status_code == 200
        and data_f["grounded"] is False
        and data_f["cypher"] == ""
        and data_f["result"] == []
        and "I don't have that information in the uploaded data." in data_f["answer"]
    )
    results.append(("F. General knowledge question", resp_f.status_code, pass_f, data_f))
    assert pass_f, f"Test F Failed: {data_f}"

    # -------------------------------------------------------------------------
    # TEST G: Malformed body / Empty JSON
    # -------------------------------------------------------------------------
    resp_g = client.post("/chat", data="not json", content_type="text/plain")
    data_g = resp_g.get_json()
    pass_g = (
        resp_g.status_code == 200
        and data_g["grounded"] is False
        and "empty" in data_g["answer"].lower()
    )
    results.append(("G. Non-JSON body fallback", resp_g.status_code, pass_g, data_g))
    assert pass_g, f"Test G Failed: {data_g}"

    # Print summary
    print("\nENDPOINT TEST SUMMARY:")
    print(f"{'Test':<36} | {'HTTP':<4} | {'Status':<6} | {'Grounded'}")
    print("-" * 70)
    for name, code, passed, payload in results:
        status_str = "PASS" if passed else "FAIL"
        print(f"{name:<36} | {code:<4} | {status_str:<6} | {payload.get('grounded')}")

    print("\n>>> ALL 7 ENDPOINT TESTS PASSED SUCCESSFULLY! <<<")


if __name__ == "__main__":
    run_endpoint_tests()
