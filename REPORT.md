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

### B. Chatbot Test Matrix (Actual Observed Runs)

The table below documents 11 actual questions executed against the live system:

| # | Question | Expected Behavior | Actual Observed Result | Grounded | Correct | Explanation |
|---|---|---|---|---|---|---|
| 1 | *How many rows are in the dataset?* | Return total row count from graph | `The dataset 'test_data.csv' contains 5 rows with columns: name, status, amount, group, id.` | **True** | **Yes** | Introspected total rows and schema accurately |
| 2 | *What columns are available?* | List properties on `:Row` nodes | `The dataset contains the following columns: status, name, amount, group, id.` | **True** | **Yes** | Retrieved column keys via `keys(r)` |
| 3 | *What is the content?* | Return grounded dataset summary | `The dataset 'test_data.csv' contains 5 rows with columns: name, status, amount, group, id.` | **True** | **Yes** | Grounded overview synthesized from Neo4j |
| 4 | *Tell me about the uploaded dataset.* | Overview of dataset | `The dataset 'test_data.csv' contains 5 rows with columns: name, status, amount, group, id.` | **True** | **Yes** | Grounded overview synthesized from Neo4j |
| 5 | *Show me some rows.* | Preview first 5 rows | `Showing top 5 rows from the dataset.` with full row properties | **True** | **Yes** | Cypher `MATCH (r:Row) RETURN r LIMIT 5` |
| 6 | *What values are present in group?* | List distinct groups | `Found 3 distinct values for group: Billing, Engineering, Support.` | **True** | **Yes** | Cypher `RETURN DISTINCT r.group` |
| 7 | *How many rows have group = Billing?* | Filtered count | `There are 3 rows where group = 'Billing'.` | **True** | **Yes** | Count query returned exact count of 3 |
| 8 | *What is the breakdown by status?* | Group distribution | `Breakdown by status: active: 3; inactive: 2.` | **True** | **Yes** | Aggregation query returned count per status |
| 9 | *What is the capital of France?* | Reject off-topic query | `I don't have that information in the uploaded data.` (Cypher: `""`, Result: `[]`) | **False** | **Yes** | Successfully prevented hallucination |
| 10 | *How many employees have blue_hair?* | Reject nonexistent property | `I don't have that information in the uploaded data.` (Cypher: `""`, Result: `[]`) | **False** | **Yes** | Property validation caught unknown column |
| 11 | *MATCH (r:Row) DELETE r* | Block destructive mutation | Blocked before execution; returned honest no-information response | **False** | **Yes** | Blocklist intercepted `DELETE` keyword |

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
python3 test_pipeline.py
python3 test_chat_engine.py
python3 test_endpoint.py
```

### Teardown
```bash
docker compose down -v
```
