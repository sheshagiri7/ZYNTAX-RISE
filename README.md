# ZYNTAX — RISE @ RST #5
> **Challenge:** "Data In, Answers Out: Build a CSV → Kafka → Neo4j Chatbot Pipeline"

Team ZYNTAX provides a decoupled data ingestion pipeline and a 100% grounded, read-only chatbot graph engine.

```
CSV Upload -> Flask REST API -> Kafka (csv-rows) -> Loader Daemon -> Neo4j -> Grounded Chatbot
```

---

## Quick Start (Judge / Evaluator)

### 1. Configure Environment
```bash
cp .env.example .env
```
*(Default settings match the challenge specification: Neo4j password `csvgraphdb`, Kafka KRaft on port 9092).*

### 2. Start the Application
Start the entire 5-service architecture with a single command:
```bash
docker compose up --build -d
```

### 3. Verify Health
Wait ~20 seconds for Neo4j and Kafka KRaft to initialize, then verify:
```bash
curl http://localhost:3000/health
```
Expected response:
```json
{"kafka_connected":true,"neo4j_connected":true,"status":"ok"}
```

### 4. Open the Web Dashboard
Navigate your browser to:
👉 **[http://localhost:3000](http://localhost:3000)**

---

## Services & Ports

| Service | Docker Container | Internal Port | Host Port | Role | User |
|---|---|---|---|---|---|
| **UI** | `zyntax-ui` | 8080 | **3000** | Reverse proxy & web dashboard | Non-root (`nginx` 101) |
| **API** | `zyntax-api` | 5000 | **8000** | REST API (`/ingest`, `/status`, `/chat`) | Non-root (`appuser` 1001) |
| **Kafka** | `zyntax-kafka` | 9092 | **9092** | KRaft broker (`csv-rows` topic) | Non-root (`appuser` 1000) |
| **Loader** | `zyntax-loader` | N/A | N/A | Kafka consumer -> Neo4j MERGE | Non-root (`appuser` 1001) |
| **Neo4j** | `zyntax-neo4j` | 7474 / 7687 | **7474 / 7687** | Graph Database (`bolt://localhost:7687`) | Process drops to `neo4j` (7474) |

---

## API Reference & Testing via CLI

### 1. Ingest a CSV (`POST /ingest`)
Upload any CSV with arbitrary columns:
```bash
curl -X POST -F "file=@test_data.csv" http://localhost:3000/ingest
```
Response (`202 Accepted`):
```json
{"job_id":"1b82e843","rows_received":5,"status":"queued"}
```

### 2. Poll Ingestion Status (`GET /status`)
```bash
curl "http://localhost:3000/status?job_id=1b82e843"
```
Response:
```json
{"job_id":"1b82e843","rows_failed":0,"rows_loaded":5,"rows_total":5,"status":"complete"}
```

### 3. Grounded Chatbot (`POST /chat`)

**A. Broad Dataset Summary:**
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"question": "What is the content?"}' \
  http://localhost:3000/chat
```
Response:
```json
{
  "answer": "The dataset 'test_data.csv' contains 5 rows with columns: name, status, amount, group, id.",
  "cypher": "MATCH (d:Dataset) OPTIONAL MATCH (d)-[:HAS_ROW]->(r:Row) WITH d, count(r) AS total_rows, collect(r)[0..3] AS sample_rows RETURN d.filename AS filename, total_rows, [k IN keys(sample_rows[0]) WHERE NOT k IN ['dataset_id', 'row_index']] AS columns LIMIT 1",
  "grounded": true,
  "result": [{"columns":["name","status","amount","group","id"],"filename":"test_data.csv","total_rows":5}]
}
```

**B. Filtered Count:**
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"question": "How many rows have group = Billing?"}' \
  http://localhost:3000/chat
```
Response:
```json
{
  "answer": "There are 3 rows where group = 'Billing'.",
  "cypher": "MATCH (r:Row {group: 'Billing'}) RETURN count(r)",
  "grounded": true,
  "result": [{"count(r)": 3}]
}
```

**C. Unanswerable / Off-topic Question (Anti-Hallucination):**
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"question": "What is the capital of France?"}' \
  http://localhost:3000/chat
```
Response:
```json
{
  "answer": "I don't have that information in the uploaded data.",
  "cypher": "",
  "grounded": false,
  "result": []
}
```

---

## Automated Test Suites

Run the complete test suite locally:
```bash
# 1. Pipeline and hostile inputs test suite (16 tests)
python3 test_pipeline.py

# 2. Chatbot verification and read-only safety guardrails (14 scenarios)
python3 test_chat_engine.py

# 3. Live Flask endpoint integration tests (7 tests)
python3 test_endpoint.py
```

---

## Idempotency Guarantee
The loader uses Cypher `MERGE` on `(dataset_id + row_index)`:
- Uploading the same CSV multiple times will **never create duplicate nodes or relationships**.
- Verified by automated tests and live integration runs.

---

## Teardown
```bash
docker compose down -v
```
