"""
Job Status Tracker for ZYNTAX Pipeline
======================================
Provides thread-safe and process-safe tracking for CSV ingestion jobs.
Tracks:
- job_id
- status ('queued' | 'loading' | 'complete' | 'failed')
- rows_total
- rows_loaded
- rows_failed
"""

import os
import json
import time
import threading
import logging

logger = logging.getLogger("zyntax.job_tracker")

# Default state file location for inter-process synchronization
STATE_FILE = os.getenv("JOB_STATE_FILE", os.path.join(os.path.dirname(os.path.dirname(__file__)), ".job_state.json"))

class JobTracker:
    def __init__(self, state_file: str = STATE_FILE):
        self.state_file = state_file
        self.lock = threading.Lock()
        self._memory_jobs = {}
        self._load_from_disk()

    def _load_from_disk(self):
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, "r", encoding="utf-8") as f:
                    self._memory_jobs = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load state file: {e}")

    def _save_to_disk(self):
        try:
            temp_path = f"{self.state_file}.tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(self._memory_jobs, f, indent=2)
            os.replace(temp_path, self.state_file)
        except Exception as e:
            logger.warning(f"Could not persist state file: {e}")

    def create_job(self, job_id: str, dataset_id: str, filename: str, rows_total: int) -> dict:
        with self.lock:
            self._load_from_disk()
            status = "complete" if rows_total == 0 else "queued"
            job = {
                "job_id": job_id,
                "dataset_id": dataset_id,
                "filename": filename,
                "status": status,
                "rows_total": rows_total,
                "rows_loaded": 0,
                "rows_failed": 0,
                "created_at": time.time(),
                "updated_at": time.time()
            }
            self._memory_jobs[job_id] = job
            self._save_to_disk()
            return job

    def get_job(self, job_id: str):
        with self.lock:
            self._load_from_disk()
            return self._memory_jobs.get(job_id)

    def set_status(self, job_id: str, status: str):
        with self.lock:
            self._load_from_disk()
            if job_id in self._memory_jobs:
                self._memory_jobs[job_id]["status"] = status
                self._memory_jobs[job_id]["updated_at"] = time.time()
                self._save_to_disk()

    def update_progress(self, job_id: str, loaded_inc: int = 0, failed_inc: int = 0, status: str = None):
        with self.lock:
            self._load_from_disk()
            if job_id in self._memory_jobs:
                job = self._memory_jobs[job_id]
                job["rows_loaded"] += loaded_inc
                job["rows_failed"] += failed_inc
                job["updated_at"] = time.time()

                if status:
                    job["status"] = status
                else:
                    # Transition to 'loading' if not already complete/failed
                    if job["rows_loaded"] + job["rows_failed"] < job["rows_total"]:
                        job["status"] = "loading"
                    elif job["rows_total"] > 0 and (job["rows_loaded"] + job["rows_failed"] >= job["rows_total"]):
                        job["status"] = "complete" if job["rows_loaded"] > 0 or job["rows_failed"] == 0 else "failed"

                self._save_to_disk()
                return job
            return None

    def reset(self):
        """Clears all jobs (useful for testing)."""
        with self.lock:
            self._memory_jobs.clear()
            if os.path.exists(self.state_file):
                try:
                    os.remove(self.state_file)
                except Exception:
                    pass

# Global shared instance
tracker = JobTracker()
