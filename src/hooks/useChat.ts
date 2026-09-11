import { useState } from 'react';
import { api, type ChatResponse } from '../services/api';

export function useChat() {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [response, setResponse] = useState<ChatResponse | null>(null);

  const sendMessage = async (question: string) => {
    if (!question.trim()) return;
    
    setIsLoading(true);
    setError(null);
    setResponse(null);
    
    try {
      const res = await api.chat(question);
      setResponse(res);
    } catch (err: any) {
      setError(err.message || "Failed to send message");
    } finally {
      setIsLoading(false);
    }
  };

  return {
    sendMessage,
    isLoading,
    error,
    response,
    clearResponse: () => setResponse(null)
  };
}
