const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '';

export type IngestResponse = {
  job_id: string;
  rows_received: number;
  status: "queued";
};

export type StatusResponse = {
  job_id: string;
  status: "queued" | "loading" | "complete" | "failed";
  rows_total: number;
  rows_loaded: number;
  rows_failed: number;
};

export type HealthResponse = {
  status: string;
  kafka_connected: boolean;
  neo4j_connected: boolean;
};

export type ChatResponse = {
  answer: string;
  cypher: string;
  result: unknown[];
  grounded: boolean;
};

export const api = {
  async ingest(file: File): Promise<IngestResponse> {
    const formData = new FormData();
    formData.append("file", file);

    const res = await fetch(`${API_BASE_URL}/ingest`, {
      method: "POST",
      body: formData,
    });
    
    if (!res.ok) {
      throw new Error(`Ingest failed with status ${res.status}`);
    }
    
    return res.json();
  },

  async getStatus(jobId: string): Promise<StatusResponse> {
    const res = await fetch(`${API_BASE_URL}/status?job_id=${encodeURIComponent(jobId)}`);
    if (!res.ok) {
      throw new Error(`Status fetch failed with status ${res.status}`);
    }
    return res.json();
  },

  async getHealth(): Promise<HealthResponse> {
    const res = await fetch(`${API_BASE_URL}/health`);
    if (!res.ok) {
      throw new Error(`Health fetch failed with status ${res.status}`);
    }
    return res.json();
  },

  async chat(question: string): Promise<ChatResponse> {
    const res = await fetch(`${API_BASE_URL}/chat`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ question }),
    });

    if (!res.ok) {
      throw new Error(`Chat failed with status ${res.status}`);
    }
    
    return res.json();
  }
};
