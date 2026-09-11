import { useState, useRef, type DragEvent, type ChangeEvent } from 'react';
import { UploadCloud, FileSpreadsheet, X, ArrowRight, CheckCircle2, AlertTriangle, Activity, Sparkles, Database, BarChart3 } from 'lucide-react';
import StarsBackground from './StarsBackground';
import { useIngestion } from './hooks/useIngestion';
import { useHealth } from './hooks/useHealth';
import { CsvPreview } from './components/CsvPreview';
import { ChatInterface } from './components/ChatInterface';
import './App.css';

function App() {
  const [dragActive, setDragActive] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  
  const { startIngestion, status, progress, metrics, error: ingestionError, isUploading, reset } = useIngestion();
  const { health, error: healthError } = useHealth();
  
  const inputRef = useRef<HTMLInputElement>(null);

  const handleDrag = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const validateAndSetFile = (selectedFile: File) => {
    if (selectedFile && (selectedFile.type === "text/csv" || selectedFile.name.endsWith('.csv'))) {
      setFile(selectedFile);
      reset();
    } else {
      alert("Please upload a valid CSV file.");
    }
  };

  const handleDrop = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      validateAndSetFile(e.dataTransfer.files[0]);
    }
  };

  const handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    e.preventDefault();
    if (e.target.files && e.target.files[0]) {
      validateAndSetFile(e.target.files[0]);
    }
  };

  const onButtonClick = () => {
    inputRef.current?.click();
  };

  const removeFile = () => {
    setFile(null);
    reset();
    if (inputRef.current) {
      inputRef.current.value = '';
    }
  };

  const handleUpload = () => {
    if (!file) return;
    startIngestion(file);
  };

  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const isComplete = status === 'complete';
  const isFailed = status === 'failed';
  const isProcessing = status === 'queued' || status === 'loading' || isUploading;

  return (
    <>
      <StarsBackground />
      
      {/* System Status Banner */}
      <div style={{ position: 'fixed', top: 0, left: 0, right: 0, padding: '8px', display: 'flex', justifyContent: 'center', zIndex: 1000, background: health?.status === 'ok' ? 'rgba(16, 185, 129, 0.2)' : 'rgba(239, 68, 68, 0.2)', backdropFilter: 'blur(4px)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '0.85rem' }}>
          <Activity size={16} /> 
          {healthError ? (
            <span style={{ color: '#ef4444' }}>Backend unavailable</span>
          ) : health ? (
            <span style={{ color: '#10b981' }}>System Online (Kafka: {health.kafka_connected ? 'OK' : 'ERR'} | Neo4j: {health.neo4j_connected ? 'OK' : 'ERR'})</span>
          ) : (
            <span>Connecting...</span>
          )}
        </div>
      </div>

      <div className="app-container" style={{ paddingTop: '40px', paddingBottom: '40px', minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div className="upload-card" style={{ width: '100%', maxWidth: isComplete ? '800px' : '600px', transition: 'max-width 0.3s ease' }}>
          
          <div className="decorative-icon icon-top-right">
            <Sparkles size={48} />
          </div>
          <div className="decorative-icon icon-bottom-left">
            <Database size={48} />
          </div>
          <div className="decorative-icon icon-center-right">
            <BarChart3 size={48} />
          </div>

          <div className="header" style={{ position: 'relative', zIndex: 1 }}>
            <h1 className="title">Data Portal</h1>
            <p className="subtitle">Upload your CSV dataset to get started</p>
          </div>

          {ingestionError && (
             <div style={{ background: 'rgba(239, 68, 68, 0.1)', color: '#ef4444', padding: '12px', borderRadius: '8px', marginBottom: '16px', display: 'flex', alignItems: 'center', gap: '8px' }}>
               <AlertTriangle size={20} /> {ingestionError}
             </div>
          )}

          {!file ? (
            <div 
              className={`drop-zone ${dragActive ? "drag-active" : ""}`}
              onDragEnter={handleDrag}
              onDragLeave={handleDrag}
              onDragOver={handleDrag}
              onDrop={handleDrop}
              onClick={onButtonClick}
            >
              <div className="icon-container">
                <UploadCloud size={40} />
              </div>
              <div>
                <p>Drag & drop your file here, or <button className="browse-btn">browse</button></p>
                <p style={{ fontSize: '0.85rem', marginTop: '0.5rem', opacity: 0.7 }}>Supports .csv files</p>
              </div>
              <input
                ref={inputRef}
                type="file"
                className="file-input"
                accept=".csv,text/csv"
                onChange={handleChange}
              />
            </div>
          ) : (
            <div className="file-info">
              <div className="file-details">
                <div className="file-icon">
                  <FileSpreadsheet size={32} />
                </div>
                <div>
                  <div className="file-name" title={file.name}>{file.name}</div>
                  <div className="file-size">{formatFileSize(file.size)}</div>
                </div>
              </div>
              {!isProcessing && !isComplete && (
                <button className="remove-btn" onClick={removeFile} aria-label="Remove file">
                  <X size={20} />
                </button>
              )}
            </div>
          )}

          {file && !status && !isUploading && (
             <CsvPreview file={file} />
          )}

          {isProcessing && (
            <div style={{ marginTop: '1.5rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px', fontSize: '0.9rem' }}>
                <span style={{ textTransform: 'uppercase', color: '#a0a0a0', fontWeight: 'bold' }}>
                  {status || 'Uploading'}
                </span>
                <span>{progress}%</span>
              </div>
              <div style={{ width: '100%', height: '8px', background: '#333', borderRadius: '4px', overflow: 'hidden' }}>
                <div style={{ height: '100%', width: `${progress}%`, background: '#6366f1', transition: 'width 0.3s ease' }} />
              </div>
              {metrics.total > 0 && (
                <div style={{ marginTop: '8px', fontSize: '0.85rem', color: '#888', display: 'flex', justifyContent: 'space-between' }}>
                  <span>{metrics.loaded} / {metrics.total} rows loaded</span>
                  {metrics.failed > 0 && <span style={{ color: '#ef4444' }}>{metrics.failed} failed</span>}
                </div>
              )}
            </div>
          )}

          {isComplete && (
            <div style={{ marginTop: '1rem', background: 'rgba(16, 185, 129, 0.1)', padding: '12px', borderRadius: '8px', display: 'flex', alignItems: 'center', gap: '8px', color: '#10b981' }}>
              <CheckCircle2 size={20} />
              <span>Successfully ingested {metrics.loaded} rows!</span>
            </div>
          )}

          {isFailed && (
            <div style={{ marginTop: '1rem', background: 'rgba(239, 68, 68, 0.1)', padding: '12px', borderRadius: '8px', display: 'flex', alignItems: 'center', gap: '8px', color: '#ef4444' }}>
              <AlertTriangle size={20} />
              <span>Ingestion failed.</span>
            </div>
          )}

          <button 
            className="action-btn"
            style={{ marginTop: '1.5rem', display: (!file || isProcessing || isComplete || isFailed) ? 'none' : 'block' }}
            disabled={!file}
            onClick={handleUpload}
          >
            Start Ingestion <ArrowRight size={20} style={{ verticalAlign: 'middle', marginLeft: '8px' }} />
          </button>
          
          {isComplete && (
            <div style={{ borderTop: '1px solid rgba(255,255,255,0.1)', marginTop: '2rem', paddingTop: '1rem' }}>
              <ChatInterface />
            </div>
          )}
          
        </div>
      </div>
    </>
  );
}

export default App;
