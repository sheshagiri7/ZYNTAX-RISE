"""
CSV Ingestion & Validation Module for ZYNTAX Pipeline
=====================================================
Handles:
- File validation (rejects non-CSV, rejects empty files, handles headers-only, ragged lines)
- Deterministic dataset_id calculation based on SHA256 of CSV content
- Publishing one Kafka message per row with dataset_id and row_index
- Decoupled from Neo4j (never writes directly from upload handler to Neo4j)
- Fails cleanly with HTTP 503 if Kafka is not ready (no fake 'complete' or stack traces)
"""

import io
import csv
import time
import hashlib
import logging
from typing import Tuple, Dict, Any
from werkzeug.datastructures import FileStorage
from backend.config import KAFKA_TOPIC
from backend.kafka_client import publish_row_message, flush_producer, is_kafka_connected, get_producer
from backend.job_tracker import tracker

logger = logging.getLogger("zyntax.ingest")


def clean_row_values(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Cleans row dictionary keys and parses numbers where appropriate.
    Preserves raw data integrity while standardizing keys.
    """
    cleaned = {}
    for k, v in row.items():
        if k is None:
            continue
        key_str = str(k).strip()
        if not key_str:
            continue

        val_str = str(v).strip() if v is not None else ""

        # Try parsing integer
        try:
            val = int(val_str)
            cleaned[key_str] = val
            continue
        except (ValueError, TypeError):
            pass

        # Try parsing float
        try:
            val = float(val_str)
            cleaned[key_str] = val
            continue
        except (ValueError, TypeError):
            pass

        # String value
        cleaned[key_str] = val_str

    return cleaned


def calculate_deterministic_dataset_id(content_bytes: bytes) -> str:
    """
    Produces a stable, deterministic dataset ID based on the CSV content.
    Re-uploading the exact same CSV will yield the identical dataset_id,
    preventing duplicate node creation.
    """
    return hashlib.sha256(content_bytes).hexdigest()[:12]


def process_csv_upload(file_storage: FileStorage) -> Tuple[Dict[str, Any], int]:
    """
    Processes the uploaded file from POST /ingest.
    Returns: (response_payload, http_status_code)
    """
    # 1. Validate file presence
    if not file_storage or not file_storage.filename:
        return {"error": "No file uploaded. Please provide a CSV file in the 'file' form field."}, 400

    filename = file_storage.filename
    # 2. Validate file extension (must be .csv)
    if not filename.lower().endswith(".csv"):
        return {"error": f"Invalid file type for '{filename}'. Only CSV files are allowed."}, 400

    # 3. Read content
    try:
        content_bytes = file_storage.read()
    except Exception as e:
        logger.error(f"Failed to read upload payload: {e}")
        return {"error": "Failed to read uploaded file payload."}, 400

    # 4. Check for empty file
    if not content_bytes or not content_bytes.strip():
        return {"error": "Uploaded CSV file is empty."}, 400

    # 5. Decode text
    try:
        content_text = content_bytes.decode("utf-8")
    except UnicodeDecodeError:
        try:
            content_text = content_bytes.decode("latin-1")
        except Exception:
            return {"error": "Unable to decode CSV text. Ensure file is UTF-8 encoded."}, 400

    # 6. Parse CSV rows
    try:
        stream = io.StringIO(content_text.strip())
        reader = csv.DictReader(stream)
        if not reader.fieldnames:
            return {"error": "CSV file does not contain valid headers."}, 400

        rows = list(reader)
    except Exception as e:
        logger.error(f"CSV parse error: {e}")
        return {"error": f"Malformed CSV structure: {str(e)}"}, 400

    # Calculate deterministic dataset identifier
    dataset_id = calculate_deterministic_dataset_id(content_bytes)
    rows_total = len(rows)

    # Generate a unique job ID for this ingestion run
    job_id = hashlib.sha256(f"{dataset_id}_{time.time()}_{rows_total}".encode()).hexdigest()[:8]

    # Handle headers-only CSV (zero data rows)
    if rows_total == 0:
        tracker.create_job(
            job_id=job_id,
            dataset_id=dataset_id,
            filename=filename,
            rows_total=0
        )
        return {
            "job_id": job_id,
            "rows_received": 0,
            "status": "queued"
        }, 202

    # 7. Check Kafka readiness before accepting job
    # Prevents false 'complete' or broken promises if Kafka is down
    if not is_kafka_connected():
        p = get_producer(retries=2, delay=0.5)
        if p is None:
            logger.error("Rejecting CSV upload: Kafka is unreachable.")
            return {
                "error": "Message broker (Kafka) is not ready. Ingestion rejected cleanly without data loss."
            }, 503

    # 8. Create job in tracker
    tracker.create_job(
        job_id=job_id,
        dataset_id=dataset_id,
        filename=filename,
        rows_total=rows_total
    )

    # 9. Publish one Kafka message per row
    published_count = 0
    for idx, raw_row in enumerate(rows):
        cleaned = clean_row_values(raw_row)
        message = {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "filename": filename,
            "row_index": idx + 1,
            "data": cleaned,
            "timestamp": time.time()
        }
        ok = publish_row_message(
            topic=KAFKA_TOPIC,
            message=message,
            key=f"{dataset_id}:{idx + 1}"
        )
        if ok:
            published_count += 1

    # Flush Kafka producer buffer
    flush_producer()

    if published_count == 0:
        tracker.set_status(job_id, "failed")
        return {
            "error": "Message broker (Kafka) unavailable: failed to dispatch row messages."
        }, 503

    logger.info(f"Published {published_count}/{rows_total} rows for job {job_id} (dataset: {dataset_id}) to Kafka topic '{KAFKA_TOPIC}'")

    # Contract requirement: 202 Accepted
    return {
        "job_id": job_id,
        "rows_received": rows_total,
        "status": "queued"
    }, 202
