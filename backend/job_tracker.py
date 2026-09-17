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

Invariants:
- Terminal state ('complete' | 'failed') only when: rows_loaded + rows_failed == rows_total
- Header-only CSV (rows_total == 0) initialized as 'complete' (0 loaded, 0 failed)
- Crash resilience: fail_job preserves rows_loaded (actually reached) and accounts remaining as rows_failed
- Loader offsets / message replay deduplication per row_index within each job
"""

import os
import json
import time
import threading
import logging
from typing import Optional, Dict, Any

try:
    import fcntl
except ImportError:
    fcntl = None

logger = logging.getLogger("zyntax.job_tracker")

# Default state file location for inter-process synchronization
STATE_FILE = os.getenv("JOB_STATE_FILE", os.path.join(os.path.dirname(os.path.dirname(__file__)), ".job_state.json"))
LOADER_CRASH_TIMEOUT = float(os.getenv("LOADER_CRASH_TIMEOUT", "60.0"))


class JobTracker:
    def __init__(self, state_file: str = STATE_FILE):
        self.state_file = state_file
        self.lock = threading.Lock()
        self._memory_jobs: Dict[str, Any] = {}
        self._load_from_disk()

    def _get_lock_file(self):
        lock_path = f"{self.state_file}.lock"
        dir_name = os.path.dirname(os.path.abspath(self.state_file))
        os.makedirs(dir_name, exist_ok=True)
        return open(lock_path, "a")

    def _load_from_disk(self):
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, "r", encoding="utf-8") as f:
                    self._memory_jobs = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load state file: {e}")

    def _save_to_disk(self):
        try:
            dir_name = os.path.dirname(os.path.abspath(self.state_file))
            os.makedirs(dir_name, exist_ok=True)
            temp_path = f"{self.state_file}.tmp.{os.getpid()}.{threading.get_ident()}"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(self._memory_jobs, f, indent=2)
            os.replace(temp_path, self.state_file)
        except Exception as e:
            logger.warning(f"Could not persist state file: {e}")

    def create_job(self, job_id: str, dataset_id: str, filename: str, rows_total: int) -> dict:
        with self.lock:
            lf = None
            try:
                if fcntl:
                    lf = self._get_lock_file()
                    fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
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
                    "loaded_indices": [],
                    "failed_indices": [],
                    "created_at": time.time(),
                    "updated_at": time.time()
                }
                self._memory_jobs[job_id] = job
                self._save_to_disk()
                return job
            finally:
                if lf and fcntl:
                    try:
                        fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
                        lf.close()
                    except Exception:
                        pass

    def get_job(self, job_id: str):
        with self.lock:
            lf = None
            try:
                if fcntl:
                    lf = self._get_lock_file()
                    fcntl.flock(lf.fileno(), fcntl.LOCK_SH)
                self._load_from_disk()
                job = self._memory_jobs.get(job_id)
                if not job:
                    return None

                # Detect loader crash: if job was loading and hasn't progressed in LOADER_CRASH_TIMEOUT
                crash_timeout = float(os.getenv("LOADER_CRASH_TIMEOUT", str(LOADER_CRASH_TIMEOUT)))
                if job["status"] == "loading" and (time.time() - job.get("updated_at", 0)) > crash_timeout:
                    logger.warning(
                        f"Job '{job_id}' timed out after {crash_timeout}s without progress. Marking as failed."
                    )
                    if fcntl and lf:
                        fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                    job["status"] = "failed"
                    if job["rows_loaded"] + job["rows_failed"] < job["rows_total"]:
                        job["rows_failed"] = job["rows_total"] - job["rows_loaded"]
                    job["error"] = f"Loader crashed or timed out after {crash_timeout}s of inactivity."
                    job["updated_at"] = time.time()
                    self._save_to_disk()

                return job
            finally:
                if lf and fcntl:
                    try:
                        fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
                        lf.close()
                    except Exception:
                        pass

    def set_status(self, job_id: str, status: str, error: Optional[str] = None):
        with self.lock:
            lf = None
            try:
                if fcntl:
                    lf = self._get_lock_file()
                    fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                self._load_from_disk()
                if job_id in self._memory_jobs:
                    job = self._memory_jobs[job_id]
                    job["status"] = status
                    job["updated_at"] = time.time()
                    if error:
                        job["error"] = str(error)

                    if status == "failed":
                        if job["rows_loaded"] + job["rows_failed"] < job["rows_total"]:
                            job["rows_failed"] = job["rows_total"] - job["rows_loaded"]
                    elif status == "complete":
                        if job["rows_loaded"] + job["rows_failed"] < job["rows_total"]:
                            job["rows_loaded"] = job["rows_total"] - job["rows_failed"]

                    self._save_to_disk()
            finally:
                if lf and fcntl:
                    try:
                        fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
                        lf.close()
                    except Exception:
                        pass

    def fail_job(self, job_id: str, error: Optional[str] = None) -> Optional[dict]:
        with self.lock:
            lf = None
            try:
                if fcntl:
                    lf = self._get_lock_file()
                    fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                self._load_from_disk()
                if job_id in self._memory_jobs:
                    job = self._memory_jobs[job_id]
                    job["status"] = "failed"
                    if job["rows_loaded"] + job["rows_failed"] < job["rows_total"]:
                        job["rows_failed"] = job["rows_total"] - job["rows_loaded"]
                    if error:
                        job["error"] = str(error)
                    job["updated_at"] = time.time()
                    self._save_to_disk()
                    return job
                return None
            finally:
                if lf and fcntl:
                    try:
                        fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
                        lf.close()
                    except Exception:
                        pass

    def fail_in_progress_jobs(self, error: str = "Loader process terminated unexpectedly"):
        with self.lock:
            lf = None
            try:
                if fcntl:
                    lf = self._get_lock_file()
                    fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                self._load_from_disk()
                for job_id, job in self._memory_jobs.items():
                    if job.get("status") == "loading":
                        job["status"] = "failed"
                        if job["rows_loaded"] + job["rows_failed"] < job["rows_total"]:
                            job["rows_failed"] = job["rows_total"] - job["rows_loaded"]
                        job["error"] = error
                        job["updated_at"] = time.time()
                self._save_to_disk()
            finally:
                if lf and fcntl:
                    try:
                        fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
                        lf.close()
                    except Exception:
                        pass

    def update_progress(
        self,
        job_id: str,
        loaded_inc: int = 0,
        failed_inc: int = 0,
        row_index: Optional[int] = None,
        status: Optional[str] = None
    ) -> Optional[dict]:
        with self.lock:
            lf = None
            try:
                if fcntl:
                    lf = self._get_lock_file()
                    fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                self._load_from_disk()
                if job_id not in self._memory_jobs:
                    return None

                job = self._memory_jobs[job_id]

                if "loaded_indices" not in job:
                    job["loaded_indices"] = []
                if "failed_indices" not in job:
                    job["failed_indices"] = []

                if row_index is not None:
                    if loaded_inc > 0:
                        if row_index in job["loaded_indices"]:
                            pass
                        else:
                            job["loaded_indices"].append(row_index)
                            if row_index in job["failed_indices"]:
                                job["failed_indices"].remove(row_index)
                                job["rows_failed"] = max(0, job["rows_failed"] - 1)
                            job["rows_loaded"] = len(job["loaded_indices"])
                    elif failed_inc > 0:
                        if row_index not in job["failed_indices"] and row_index not in job["loaded_indices"]:
                            job["failed_indices"].append(row_index)
                            job["rows_failed"] += failed_inc
                else:
                    job["rows_loaded"] += loaded_inc
                    job["rows_failed"] += failed_inc

                job["updated_at"] = time.time()

                if status:
                    job["status"] = status
                else:
                    total_accounted = job["rows_loaded"] + job["rows_failed"]
                    if total_accounted >= job["rows_total"]:
                        if job["rows_total"] == 0:
                            job["status"] = "complete"
                        elif job["rows_loaded"] > 0:
                            job["status"] = "complete"
                        else:
                            job["status"] = "failed"
                    elif job["rows_loaded"] > 0:
                        job["status"] = "loading"

                self._save_to_disk()
                return job
            finally:
                if lf and fcntl:
                    try:
                        fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
                        lf.close()
                    except Exception:
                        pass

    def reset(self):
        """Clears all jobs (useful for testing)."""
        with self.lock:
            lf = None
            try:
                if fcntl:
                    lf = self._get_lock_file()
                    fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                self._memory_jobs.clear()
                if os.path.exists(self.state_file):
                    try:
                        os.remove(self.state_file)
                    except Exception:
                        pass
            finally:
                if lf and fcntl:
                    try:
                        fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
                        lf.close()
                    except Exception:
                        pass

# Global shared instance
tracker = JobTracker()
