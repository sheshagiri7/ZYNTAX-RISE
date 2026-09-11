"""
Job Status Handler for ZYNTAX Pipeline
======================================
Implements GET /status?job_id=...
Honest tracking of job status: queued, loading, complete, failed.
"""

from typing import Tuple, Dict, Any
from backend.job_tracker import tracker


def get_job_status(job_id: str) -> Tuple[Dict[str, Any], int]:
    """
    Returns the real-time status of an ingestion job.
    Structure:
    {
      "job_id": str,
      "status": "queued" | "loading" | "complete" | "failed",
      "rows_total": int,
      "rows_loaded": int,
      "rows_failed": int
    }
    """
    if not job_id or not job_id.strip():
        return {
            "error": "Missing required query parameter 'job_id'"
        }, 400

    job = tracker.get_job(job_id.strip())
    if not job:
        return {
            "job_id": job_id,
            "status": "failed",
            "rows_total": 0,
            "rows_loaded": 0,
            "rows_failed": 0,
            "error": f"Job '{job_id}' not found."
        }, 404

    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "rows_total": job["rows_total"],
        "rows_loaded": job["rows_loaded"],
        "rows_failed": job["rows_failed"]
    }, 200
