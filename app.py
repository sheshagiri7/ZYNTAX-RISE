"""
ZYNTAX Backend API Service
==========================
RISE @ RST #5 Hackathon API entrypoint.
Decoupled architecture:
- POST /chat   -> Powered by Member 3 (chat_engine.py)
- POST /ingest -> Decoupled Kafka producer (Member 2)
- GET /status  -> Progress reporter (Member 2)
- GET /health  -> Service healthcheck (Member 2)
"""

import os
import logging
from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from chat_engine import handle_chat

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("zyntax.api")

app = Flask(__name__)

# -----------------------------------------------------------------------------
# NEO4J CONFIGURATION (Official Handout Part 2.4)
# -----------------------------------------------------------------------------
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "csvgraphdb")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "CSV_Graph_DB")

_driver = None

def get_driver():
    """
    Returns the active Neo4j driver with lazy initialization.
    Prevents API container crash if Neo4j is still starting up.
    """
    global _driver
    if _driver is None:
        try:
            _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            logger.info(f"Connected to Neo4j at {NEO4J_URI} (db: {NEO4J_DATABASE})")
        except Exception as e:
            logger.warning(f"Neo4j driver initialization deferred/failed: {e}")
    return _driver

def set_driver(custom_driver):
    """Allows setting or mocking the driver for testing."""
    global _driver
    _driver = custom_driver


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
# CHATBOT ENDPOINT (Member 3 - RISE @ RST #5 Contract)
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
# SKELETON PLACEHOLDERS FOR MEMBER 2 (INGEST, STATUS, HEALTH)
# -----------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    """Health check verifying Kafka and Neo4j connectivity."""
    neo4j_ok = False
    driver = get_driver()
    if driver:
        try:
            with driver.session(database=NEO4J_DATABASE) as s:
                s.run("RETURN 1").single()
            neo4j_ok = True
        except Exception:
            neo4j_ok = False

    status_str = "ok" if neo4j_ok else "degraded"
    return jsonify({
        "status": status_str,
        "kafka_connected": False,  # Managed by Member 2
        "neo4j_connected": neo4j_ok
    }), 200


@app.route("/status", methods=["GET"])
def status():
    """Progress reporter for CSV ingestion jobs (Member 2)."""
    job_id = request.args.get("job_id", "default")
    return jsonify({
        "job_id": job_id,
        "status": "queued",
        "rows_total": 0,
        "rows_loaded": 0,
        "rows_failed": 0
    }), 200


@app.route("/ingest", methods=["POST"])
def ingest():
    """CSV upload handler (Member 2)."""
    return jsonify({
        "job_id": "job-pending",
        "rows_received": 0,
        "status": "queued"
    }), 202


# -----------------------------------------------------------------------------
# ERROR HANDLERS (Always return JSON)
# -----------------------------------------------------------------------------
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
    port = int(os.getenv("PORT", 5000))
    host = os.getenv("HOST", "0.0.0.0")
    logger.info(f"Starting ZYNTAX API on {host}:{port}")
    app.run(host=host, port=port, debug=False)
