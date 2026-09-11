"""
ZYNTAX Backend API Service
==========================
RISE @ RST #5 Hackathon API Entrypoint.

API Contracts & Architecture:
-----------------------------
- POST /ingest -> Validates CSV, publishes one message per row to Kafka, returns 202 Accepted.
- GET /status  -> Returns real-time ingestion status and row counts honestly.
- GET /health  -> Accurately verifies Kafka and Neo4j connectivity before reporting 'ok'.
- POST /chat   -> Grounded Q&A over Neo4j dynamic graph model via Member 3's chat_engine.
"""

import os
import logging
from flask import Flask, request, jsonify
from backend.config import (
    PORT,
    HOST,
    NEO4J_DATABASE
)
from backend.kafka_client import is_kafka_connected, set_producer
from backend.neo4j_client import (
    get_neo4j_driver,
    set_neo4j_driver,
    is_neo4j_connected
)
from backend.ingest import process_csv_upload
from backend.status import get_job_status
from chat_engine import handle_chat

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("zyntax.api")

app = Flask(__name__)

# Lazy driver access and test injection compatibility for Member 3
def get_driver():
    return get_neo4j_driver()

def set_driver(custom_driver):
    set_neo4j_driver(custom_driver)

def set_kafka(custom_producer):
    set_producer(custom_producer)


# -----------------------------------------------------------------------------
# CORS HEADERS (For seamless frontend integration)
# -----------------------------------------------------------------------------
@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return response


# -----------------------------------------------------------------------------
# 1. HEALTH CHECK ENDPOINT (Official Contract)
# -----------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    """
    GET /health
    Expected structure:
    {
      "status": "ok" | "degraded",
      "kafka_connected": bool,
      "neo4j_connected": bool
    }
    The API must NOT report "ok" until Kafka and Neo4j are genuinely reachable.
    """
    kafka_ok = is_kafka_connected()
    neo4j_ok = is_neo4j_connected()

    # Status must be "ok" ONLY when both services are connected
    status_str = "ok" if (kafka_ok and neo4j_ok) else "degraded"

    return jsonify({
        "status": status_str,
        "kafka_connected": kafka_ok,
        "neo4j_connected": neo4j_ok
    }), 200


# -----------------------------------------------------------------------------
# 2. INGESTION ENDPOINT (Official Contract)
# -----------------------------------------------------------------------------
@app.route("/ingest", methods=["POST", "OPTIONS"])
def ingest():
    """
    POST /ingest
    Content-Type: multipart/form-data
    Field: file

    Expected response (202 Accepted):
    {
      "job_id": "b3f1",
      "rows_received": 1000,
      "status": "queued"
    }
    """
    if request.method == "OPTIONS":
        return "", 204

    if "file" not in request.files:
        return jsonify({"error": "Missing 'file' in multipart form data."}), 400

    file_storage = request.files["file"]
    response_payload, status_code = process_csv_upload(file_storage)
    return jsonify(response_payload), status_code


# -----------------------------------------------------------------------------
# 3. STATUS ENDPOINT (Official Contract)
# -----------------------------------------------------------------------------
@app.route("/status", methods=["GET"])
def status():
    """
    GET /status?job_id=b3f1
    Expected structure:
    {
      "job_id": "b3f1",
      "status": "loading",
      "rows_total": 1000,
      "rows_loaded": 640,
      "rows_failed": 3
    }
    Status must honestly be one of: queued, loading, complete, failed.
    """
    job_id = request.args.get("job_id")
    response_payload, status_code = get_job_status(job_id)
    return jsonify(response_payload), status_code


# -----------------------------------------------------------------------------
# 4. CHATBOT ENDPOINT (Member 3 - RISE @ RST #5 Contract)
# -----------------------------------------------------------------------------
@app.route("/chat", methods=["POST", "OPTIONS"])
def chat():
    """
    POST /chat
    Request:  {"question": "How many rows belong to the Billing group?"}
    Response: {
      "answer": "There are 128 rows where group = 'Billing'.",
      "cypher": "MATCH (r:Row {group: 'Billing'}) RETURN count(r)",
      "result": [{"count(r)": 128}],
      "grounded": true
    }
    """
    if request.method == "OPTIONS":
        return "", 204

    try:
        data = request.get_json(silent=True) or {}
    except Exception:
        data = {}

    question = data.get("question", "")

    # Retrieve Neo4j driver and execute grounded chat handler
    driver = get_driver()
    response_payload = handle_chat(
        question=question,
        driver=driver,
        database=NEO4J_DATABASE
    )

    return jsonify(response_payload), 200


# -----------------------------------------------------------------------------
# ERROR HANDLERS (Always return JSON)
# -----------------------------------------------------------------------------
@app.errorhandler(400)
def bad_request(e):
    return jsonify({"error": str(e), "grounded": False}), 400

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Resource not found", "grounded": False}), 404

@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({"error": "Method not allowed", "grounded": False}), 405

@app.errorhandler(Exception)
def internal_error(e):
    logger.error(f"Unhandled API error: {e}", exc_info=True)
    return jsonify({
        "answer": "Internal server error processing request.",
        "cypher": "",
        "result": [],
        "grounded": False
    }), 500


if __name__ == "__main__":
    logger.info(f"Starting ZYNTAX API on {HOST}:{PORT}")
    app.run(host=HOST, port=PORT, debug=False)
