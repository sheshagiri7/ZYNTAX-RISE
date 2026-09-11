import { useState, useEffect } from 'react';
import { api, type HealthResponse } from '../services/api';

export function useHealth() {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    
    const checkHealth = async () => {
      try {
        const res = await api.getHealth();
        if (mounted) {
          setHealth(res);
          setError(null);
        }
      } catch (err: any) {
        if (mounted) {
          setError(err.message || "Backend offline");
          setHealth(null);
        }
      } finally {
        if (mounted) {
          setIsLoading(false);
        }
      }
    };

    checkHealth();
    
    // Optional: poll every 10s
    const intervalId = setInterval(checkHealth, 10000);
    
    return () => {
      mounted = false;
      clearInterval(intervalId);
    };
  }, []);

  return { health, error, isLoading };
}
