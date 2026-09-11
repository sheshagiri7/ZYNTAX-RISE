import React, { useState } from 'react';
import { useChat } from '../hooks/useChat';
import { Send, AlertTriangle, Code, Terminal } from 'lucide-react';

export const ChatInterface: React.FC = () => {
  const [question, setQuestion] = useState("");
  const { sendMessage, isLoading, error, response } = useChat();

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    sendMessage(question);
  };

  return (
    <div className="chat-interface" style={{ marginTop: '2rem', display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
      <form onSubmit={handleSubmit} style={{ display: 'flex', gap: '0.5rem' }}>
        <input 
          type="text" 
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="Ask a question about your data..."
          disabled={isLoading}
          style={{
            flex: 1,
            padding: '12px 16px',
            borderRadius: '8px',
            border: '1px solid #333',
            background: 'rgba(0,0,0,0.5)',
            color: 'white',
            outline: 'none'
          }}
        />
        <button 
          type="submit" 
          disabled={isLoading || !question.trim()}
          style={{
            padding: '12px 24px',
            borderRadius: '8px',
            background: '#6366f1',
            color: 'white',
            border: 'none',
            cursor: isLoading || !question.trim() ? 'not-allowed' : 'pointer',
            opacity: isLoading || !question.trim() ? 0.7 : 1,
            display: 'flex',
            alignItems: 'center',
            gap: '8px'
          }}
        >
          {isLoading ? 'Thinking...' : <><Send size={18} /> Send</>}
        </button>
      </form>

      {error && (
        <div style={{ background: 'rgba(255, 77, 79, 0.1)', color: '#ff4d4f', padding: '12px', borderRadius: '8px', display: 'flex', alignItems: 'center', gap: '8px' }}>
          <AlertTriangle size={20} /> {error}
        </div>
      )}

      {response && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
          <div style={{ background: 'rgba(255,255,255,0.05)', padding: '1.5rem', borderRadius: '12px' }}>
            <h4 style={{ margin: '0 0 12px 0', color: '#a0a0a0', fontSize: '0.9rem', textTransform: 'uppercase', letterSpacing: '1px' }}>Answer</h4>
            <p style={{ margin: 0, fontSize: '1.1rem', lineHeight: '1.5' }}>{response.answer}</p>
          </div>

          {!response.grounded && (
            <div style={{ background: 'rgba(255, 170, 0, 0.1)', color: '#ffaa00', padding: '12px', borderRadius: '8px', display: 'flex', alignItems: 'center', gap: '8px', fontWeight: 'bold' }}>
              <AlertTriangle size={20} /> NOT GROUNDED IN UPLOADED DATA
            </div>
          )}

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
            <div style={{ background: 'rgba(0,0,0,0.4)', padding: '1rem', borderRadius: '8px', border: '1px solid #333' }}>
              <h4 style={{ margin: '0 0 12px 0', color: '#a0a0a0', fontSize: '0.85rem', display: 'flex', alignItems: 'center', gap: '6px' }}>
                <Code size={16} /> CYPHER
              </h4>
              <pre style={{ margin: 0, whiteSpace: 'pre-wrap', fontSize: '0.85rem', color: '#a78bfa', fontFamily: 'monospace' }}>
                {response.cypher}
              </pre>
            </div>

            <div style={{ background: 'rgba(0,0,0,0.4)', padding: '1rem', borderRadius: '8px', border: '1px solid #333' }}>
              <h4 style={{ margin: '0 0 12px 0', color: '#a0a0a0', fontSize: '0.85rem', display: 'flex', alignItems: 'center', gap: '6px' }}>
                <Terminal size={16} /> RAW RESULT
              </h4>
              <pre style={{ margin: 0, whiteSpace: 'pre-wrap', fontSize: '0.85rem', color: '#34d399', fontFamily: 'monospace', overflow: 'auto', maxHeight: '300px' }}>
                {JSON.stringify(response.result, null, 2)}
              </pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
