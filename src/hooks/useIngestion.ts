import { useState, useEffect, useCallback, useRef } from 'react';
import { api, type StatusResponse } from '../services/api';

export function useIngestion() {
  const [jobId, setJobId] = useState<string | null>(null);
  const [status, setStatus] = useState<StatusResponse['status'] | null>(null);
  const [progress, setProgress] = useState<number>(0);
  const [metrics, setMetrics] = useState({ total: 0, loaded: 0, failed: 0 });
  const [error, setError] = useState<string | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const pollIntervalRef = useRef<number | null>(null);

  const startIngestion = async (file: File) => {
    try {
      setIsUploading(true);
      setError(null);
      const res = await api.ingest(file);
      setJobId(res.job_id);
      setStatus(res.status);
    } catch (err: any) {
      setError(err.message || "Failed to upload file");
    } finally {
      setIsUploading(false);
    }
  };

  const pollStatus = useCallback(async () => {
    if (!jobId) return;
    try {
      const res = await api.getStatus(jobId);
      setStatus(res.status);
      setMetrics({
        total: res.rows_total,
        loaded: res.rows_loaded,
        failed: res.rows_failed,
      });
      if (res.rows_total > 0) {
        setProgress(Math.round((res.rows_loaded / res.rows_total) * 100));
      }

      if (res.status === 'complete' || res.status === 'failed') {
        if (pollIntervalRef.current) {
          clearInterval(pollIntervalRef.current);
          pollIntervalRef.current = null;
        }
      }
    } catch (err: any) {
      setError(err.message || "Failed to fetch status");
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }
    }
  }, [jobId]);

  useEffect(() => {
    if (status === 'queued' || status === 'loading') {
      pollIntervalRef.current = window.setInterval(pollStatus, 1000);
    }
    return () => {
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
      }
    };
  }, [status, pollStatus]);

  const reset = () => {
    setJobId(null);
    setStatus(null);
    setProgress(0);
    setMetrics({ total: 0, loaded: 0, failed: 0 });
    setError(null);
    if (pollIntervalRef.current) {
      clearInterval(pollIntervalRef.current);
      pollIntervalRef.current = null;
    }
  };

  return {
    startIngestion,
    jobId,
    status,
    progress,
    metrics,
    error,
    isUploading,
    reset
  };
}
