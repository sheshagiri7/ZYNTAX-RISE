# Team ZYNTAX — Final Engineering Report
**Hackathon:** RISE @ RST #5  
**Challenge:** Data In, Answers Out — Build a CSV → Kafka → Neo4j Chatbot Pipeline  
**Team:** ZYNTAX (Member 1: Frontend/UI, Member 2: Backend/Pipeline, Member 3: AI/Chatbot)  
**Status:** Submission Ready  

---

## 1. What We Built

Team ZYNTAX built an end-to-end, decoupled data ingestion and conversational query system that turns arbitrary CSV files into a dynamic Neo4j graph and exposes a 100% grounded, hallucination-free chatbot.

### System Architecture
The application runs as five isolated Docker containers in a single Docker network:

```
[ Browser UI (Port 3000) ]
        |
        v
  [ Nginx Proxy (Port 8080, non-root uid 101) ]
        |
        v
  [ Flask API (Port 5000, non-root uid 1001) ]
        |
        +---> Publishes 1 message / row ---> [ Apache Kafka 3.7.0 (KRaft, csv-rows) ]
        |                                                 |
        |                                                 v
        |                                       [ Python Loader Daemon (non-root) ]
        |                                                 |
        |                                                 v (Idempotent MERGE)
        +<--- Executes Safe Cypher <---------------- [ Neo4j 5.24 Graph DB ]
```

1. **Frontend (zyntax-ui):** Pinned `nginx:1.27.4-alpine` running strictly non-root (UID 101), reverse-proxying API endpoints to decouple the client and eliminate CORS friction.
2. **Backend REST API (zyntax-api):** Flask 3.1.3 runtime in Python 3.11-slim (non-root `appuser` UID 1001). Validates CSVs, computes deterministic SHA-256 dataset identities, decouples ingestion via Kafka, tracks job state, and serves the grounded `/chat` endpoint.
3. **Message Broker (zyntax-kafka):** Apache Kafka 3.7.0 running in single-broker KRaft mode (zero ZooKeeper dependency) with partitioned topic `csv-rows`. Decouples ingestion spikes from database writes.
4. **Loader Daemon (zyntax-loader):** Dedicated Python 3.11-slim consumer (non-root UID 1001) that ingests row messages from Kafka and executes idempotent `MERGE` transactions into Neo4j.
5. **Graph Database (zyntax-neo4j):** Neo4j 5.24 Community with APOC plugin, persisting node and relationship data across container lifecycles.

---

## 2. Data and Graph Model

The pipeline implements the official generic graph model:

```
(:Dataset {
    id: STRING (SHA-256 hash prefix),
    filename: STRING,
    uploaded_at: STRING (ISO 8601)
})
  |
  +---[:HAS_ROW]--->
                    (:Row {
                        dataset_id: STRING,
                        row_index: INTEGER,
                        <col_1>: ANY,
                        <col_2>: ANY,
                        ...
                    })
```

### Dynamic Column Schema
The system does not enforce a rigid table structure. All CSV headers are dynamically parsed, stripped of leading/trailing whitespace, and converted into native property types (integer, float, or trimmed string) on the `(:Row)` nodes using the Cypher map projection:
```cypher
MERGE (d:Dataset {id: $dataset_id})
ON CREATE SET d.filename = $filename, d.uploaded_at = $uploaded_at
WITH d
MERGE (r:Row {dataset_id: $dataset_id, row_index: $row_index})
SET r += $row_data
MERGE (d)-[:HAS_ROW]->(r)
RETURN count(r) AS rows_merged
```

This model was verified with arbitrary schemas including standard billing datasets (`id, name, group, status, amount`) and clinical healthcare datasets (`patient_id, physician, ward, acuity, room_number`).

---

## 3. Methods

### A. Decoupled Ingestion
The API (`POST /ingest`) never performs direct writes to Neo4j. Instead:
- Validates the multipart file payload (detects empty files, header-only CSVs, non-CSV MIME types, and malformed null bytes).
- Calculates a content-derived SHA-256 hash (`dataset_id`).
- Dispatches exactly one Kafka message per row with key `f"{dataset_id}:{row_index}"` to topic `csv-rows`.
- Immediately responds with HTTP `202 Accepted` returning `job_id`, `rows_received`, and status `queued`.
- The background `loader` service consumes Kafka asynchronously and commits rows to Neo4j.

### B. Shared Inter-Process Job Tracking
To ensure `GET /status?job_id=...` accurately reflects live progress between the API container and the Loader daemon:
- A shared Docker bind mount `./.state:/app/.state` hosts atomic, process-safe state storage (`jobs.json`).
- As rows are successfully merged into Neo4j, the loader atomically updates `rows_loaded` and `rows_failed`.
- The status honestly transitions through `queued` -> `loading` -> `complete` (or `failed`). Zero fake timers or artificial delays.

### C. Strict Idempotency
- Row identity is strictly bound to `dataset_id + row_index`.
- Uploading the identical CSV file multiple times yields the same SHA-256 `dataset_id`.
- The Cypher `MERGE` clause ensures that re-uploading an existing CSV creates **zero duplicate nodes** and **zero duplicate relationships**.

### D. Grounded Chatbot Engine
- Introspects the live Neo4j database schema (`keys(r)` and `count(r)`).
- Maps user questions to deterministic, parameter-safe Cypher read queries.
- Strict Read-Only Guardrail blocks all mutating operations (`CREATE`, `MERGE`, `DELETE`, `DETACH`, `SET`, `REMOVE`, `DROP`, `ALTER`, `TRUNCATE`, `LOAD CSV`, `APOC` mutations, and multi-statement semicolons).
- General knowledge questions ("capital of France") or questions referencing nonexistent columns ("blue_hair") return `grounded: false` with `cypher: ""` and `result: []`.
- Broad dataset overview queries ("What is the content?", "Tell me about the uploaded dataset.") introspect the graph and return accurate counts, filenames, and column schemas without hallucinations.

---

## 4. Results

### A. End-to-End System Test Results

| Test Category | Tested Input / Operation | Observed Behavior | Status |
|---|---|---|---|
| **Health Check** | `GET /health` | Both Kafka & Neo4j verified via active protocol probes | **PASS** (`status: ok`) |
| **Empty CSV** | `0 bytes` file upload | Rejected cleanly with HTTP 400 | **PASS** |
| **Header-only CSV** | CSV with headers only | Handled safely, 0 rows, status complete | **PASS** |
| **Non-CSV Upload** | `.txt` text file | Rejected with HTTP 400 invalid file type | **PASS** |
| **Hostile Null Bytes** | CSV containing `\x00` | Rejected with HTTP 400 malformed structure | **PASS** |
| **Idempotency** | Second upload of identical 5-row CSV | Datasets: 1, Rows: 5, HAS_ROW: 5 (0 duplicates) | **PASS** |
| **Dynamic Schema** | Healthcare CSV (`ward, physician, acuity`) | Ingested dynamically, queried accurately | **PASS** |

### B. Chatbot Q&A Pairs — Live Docker Verification (Real Observed Output)

All Q&A pairs below were run live against `http://localhost:3000/chat` with `sample.csv` (4 rows, columns: `group, status, amount, department`) ingested via Kafka → Neo4j.

| # | User Question | `grounded` | Answer Returned | Cypher Executed |
|---|---|:---:|---|---|
| 1 | `how many rows?` | `true` | `There are 4 total rows in the dataset.` | `MATCH (r:Row) RETURN count(r)` |
| 2 | `how many columns?` | `true` | `There are 4 columns in the uploaded dataset 'sample.csv': status, amount, department, group.` | Schema introspection via `size(columns)` |
| 3 | `colums` *(typo)* | `true` | `The available columns in 'sample.csv' are: status, amount, department, group (4 columns total).` | Typo-tolerant regex `colum(?:n)?s?` matched; schema introspection executed |
| 4 | `what columns are available?` | `true` | `The available columns in 'sample.csv' are: status, amount, department, group (4 columns total).` | Schema introspection via `DISTINCT key_item` |
| 5 | `what is the content?` | `true` | `The uploaded dataset 'sample.csv' contains 4 rows across 4 columns. The available columns are: status, amount, department, group.` | Full dataset summary query |
| 6 | `show me some rows` | `true` | `Showing top 4 rows from the dataset.` | `MATCH (r:Row) … RETURN r LIMIT 5` |
| 7 | `show me the first 3 rows` | `true` | `Showing top 3 rows from the dataset.` | `MATCH (r:Row) … RETURN r LIMIT 3` |
| 8 | `how many rows where group is Billing?` | `true` | `There are 2 rows where group = 'Billing'.` | `MATCH (r:Row {group: 'Billing'}) RETURN count(r)` |
| 9 | `what are the distinct values of group?` | `true` | `Found 3 distinct values for group: Billing, Engineering, Marketing.` | `RETURN DISTINCT r.group AS group ORDER BY group LIMIT 25` |
| 10 | `what is the max amount?` | `true` | `The max value in the uploaded data is 300.` | `RETURN avg(toFloat(r.amount)) … max(toFloat(r.amount)) … ` |
| 11 | `what is the average amount?` | `true` | `The avg value in the uploaded data is 200.` | Numeric aggregation with `avg(toFloat(r.amount))` |
| 12 | `show me rows where status contains active` | `true` | `Found 3 matching row(s) containing the search text.` | `MATCH (r:Row) WHERE toLower(toString(r.status)) CONTAINS toLower('active') RETURN r LIMIT 10` |
| 13 | `how many rows have blue_eyes?` | `false` | `I don't have that information in the uploaded data. No column named 'blue_eyes' was found in the schema.` | *(none — rejected before execution)* |
| 14 | `what is the capital of France?` | `false` | `I don't have that information in the uploaded data.` | *(none — general knowledge blocked)* |

> **Test suite summary (automated + live):**
> - `test_pipeline.py`: **16/16 PASS** (ingestion, idempotency, Kafka, Neo4j)
> - `test_chat_engine.py`: **14/14 PASS** (unit tests, mocked Neo4j)
> - `test_endpoint.py`: **7/7 PASS** (Flask test client, HTTP surface)
> - `test_multi_schema.py`: **30/30 PASS** (3 CSV schemas × 10 scenarios each)
> - **Live Docker API** (`localhost:3000`): **14/14 PASS** (real queries against running containers)

---


## 5. How We Worked

### Architectural Decisions

| Decision | Alternatives Considered | Rationale |
|---|---|---|
| **Single-broker KRaft Mode** | Kafka with ZooKeeper | KRaft eliminates the separate ZooKeeper container, reducing resource footprint and startup latency within the hackathon time limit while fully satisfying decoupled streaming requirements. |
| **Asynchronous Shared-State File** | In-memory tracker vs. Redis | Using an atomic bind-mounted JSON state file (`jobs.json`) allowed the API and Loader containers to share live status honestly without requiring a sixth container service. |
| **Dynamic Cypher Node Properties** | Static schema tables / Relational DB | Cypher's `SET r += $row_data` enables arbitrary CSV schemas without migrations or schema pre-definition. |
| **Nginx Reverse Proxy with Dynamic DNS** | Exposing API directly on host port | Proxying `/ingest`, `/status`, `/health`, and `/chat` through Nginx on port 3000 solved CORS completely and centralized the browser entrypoint. |

### Dead End & Resolution

- **The Dead End:**  
  The UI intermittently displayed `System Online (Kafka: ERR | Neo4j: OK)`, even though CSV uploads succeeded and Kafka was actively running.
- **How It Was Discovered:**  
  Inspecting `docker logs zyntax-api` revealed:
  ```
  INFO:kafka.client:Closing idle connection bootstrap-0, last active 5000 ms ago
  ```
  Testing `KafkaProducer.bootstrap_connected()` in Python confirmed that in `kafka-python`, `bootstrap_connected()` returns `False` as soon as the initial bootstrap connection enters an idle state. Because the API cached this producer object, subsequent `/health` polling permanently reported `kafka_connected: false`.
- **How It Was Resolved:**  
  Refactored `backend/kafka_client.py` to use a two-phase check: a fast TCP socket connection probe to `KAFKA_BOOTSTRAP_SERVERS`, followed by an active cluster metadata introspection using `KafkaAdminClient.describe_cluster()`. This probe never caches stale connection state, correctly reporting `kafka_connected: true` persistently while Kafka is live.

---

## 6. Limitations and Next Steps

1. **Multi-File Relationships:** The current data model links rows to their respective `Dataset` node. Future iterations can infer foreign-key relationships across multiple uploaded CSVs (e.g., matching `department_id` across datasets).
2. **Streaming Ingestion for Gigabyte CSVs:** For massive multi-gigabyte CSVs, chunking file streaming directly from the multipart reader into Kafka partitions would bypass memory buffering limits.
3. **Natural Language Semantic Routing:** Integrating local embedding-based vector search for complex colloquial questions while preserving the strict read-only Cypher execution pipeline.

---

## 7. How to Run It

### Prerequisites
- Docker Engine & Docker Compose v2+
- Ports available: `3000` (UI), `8000` (API), `7474`/`7687` (Neo4j), `9092` (Kafka)

### One-Command Startup
```bash
# 1. Clone repository
git clone https://github.com/sheshagiri7/ZYNTAX-RISE.git
cd ZYNTAX-RISE

# 2. Configure environment (uses challenge defaults)
cp .env.example .env

# 3. Start all 5 services
docker compose up --build -d
```

### Accessing the System
- **Web UI:** [http://localhost:3000](http://localhost:3000)
- **Direct API Health:** [http://localhost:8000/health](http://localhost:8000/health)
- **Neo4j Browser:** [http://localhost:7474](http://localhost:7474) (`neo4j` / `csvgraphdb`)

### Running Automated Test Suites
```bash
python3 test_pipeline.py       # 16/16 — ingestion, idempotency, Kafka, Neo4j
python3 test_chat_engine.py    # 14/14 — chatbot unit tests (mocked Neo4j)
python3 test_endpoint.py       # 7/7  — HTTP endpoint tests (Flask test client)
python3 test_multi_schema.py   # 30/30 — multi-schema dynamic CSV adaptation
```

### Teardown
```bash
docker compose down -v
```
