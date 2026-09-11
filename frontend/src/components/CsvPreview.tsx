import React, { useState, useEffect } from 'react';
import Papa from 'papaparse';

interface CsvPreviewProps {
  file: File;
}

export const CsvPreview: React.FC<CsvPreviewProps> = ({ file }) => {
  const [headers, setHeaders] = useState<string[]>([]);
  const [rows, setRows] = useState<any[][]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    setIsLoading(true);
    setError(null);
    
    Papa.parse(file, {
      preview: 5, // Just preview first 5 rows
      complete: (results) => {
        if (results.errors.length > 0 && results.data.length === 0) {
          setError("Failed to parse CSV");
        } else {
          const data = results.data as any[][];
          if (data.length > 0) {
            setHeaders(data[0] || []);
            setRows(data.slice(1));
          } else {
            setError("Empty CSV file");
          }
        }
        setIsLoading(false);
      },
      error: (err) => {
        setError(err.message);
        setIsLoading(false);
      }
    });
  }, [file]);

  if (isLoading) return <div style={{marginTop: '1rem'}}>Loading preview...</div>;
  if (error) return <div style={{marginTop: '1rem', color: '#ff4d4f'}}>Error: {error}</div>;
  if (headers.length === 0) return null;

  return (
    <div className="csv-preview-container" style={{ marginTop: '1.5rem', overflowX: 'auto', background: 'rgba(0,0,0,0.3)', borderRadius: '8px', padding: '1rem' }}>
      <h3 style={{ marginTop: 0, marginBottom: '1rem', fontSize: '1rem', color: '#a0a0a0' }}>CSV Preview (First few rows)</h3>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>
        <thead>
          <tr>
            {headers.map((h, i) => (
              <th key={i} style={{ textAlign: 'left', padding: '8px', borderBottom: '1px solid #333', color: '#e0e0e0' }}>
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              {row.map((cell, j) => (
                <td key={j} style={{ padding: '8px', borderBottom: '1px solid #222', color: '#b0b0b0' }}>
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
