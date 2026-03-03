import { useState, useRef, useEffect } from "react";
import "./App.css";

const API_BASE_URL = "http://localhost:8000";

// Hardcoded scraped data
const SCRAPED_DATA = [
  { id: "5038294000", name: "2019 Tracker bass buggy", image: "https://cdn-media.tilabs.io/v1/media/6940ec0a2d456029260b3ef7.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294032", name: "2019 Suntracker 24DLX", image: "https://cdn-media.tilabs.io/v1/media/6940ebd364364e86670b41ef.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294041", name: "2016 Premier 310 BOUNDRY WATER", image: "https://cdn-media.tilabs.io/v1/media/6940ec2f9c4ed21b07078252.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294045", name: "2014 Scout 210XSF", image: "https://cdn-media.tilabs.io/v1/media/6940ec8b1110062f6c06df3a.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294048", name: "1995 Mariah 220 Talari - it runs!", image: "https://cdn-media.tilabs.io/v1/media/6940eb5ef82da339ed07a3b6.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294069", name: "2006 Boston Whaler Ventura 180", image: "https://cdn-media.tilabs.io/v1/media/6940eb03aedd8dc0210cb60f.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294100", name: "2002 Sea Swirl 2301 Striper", image: "https://cdn-media.tilabs.io/v1/media/6940eba1d9b2ab79b309947d.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294104", name: "2022 Sea-Doo GTI 170", image: "https://cdn-media.tilabs.io/v1/media/6940eb3d28832b27a80368ef.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294114", name: "2016 Tracker Tahoe 400 TS", image: "https://cdn-media.tilabs.io/v1/media/6940ebfbc51fce7e5e0fe821.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294140", name: "2017 Correct Craft RI237", image: "https://cdn-media.tilabs.io/v1/media/6940ebb0d61b055a110c8ace.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294192", name: "2025 Boston Whaler 130 SUPERSPORT", image: "https://cdn-media.tilabs.io/v1/media/6940ea608a62ba96f50474fb.webp?width=1328&quality=70&upsize=true" },
  { id: "5038293956", name: "2008 Sunchaser 8520 CRS", image: "https://cdn-media.tilabs.io/v1/media/6940eb539b172457cb0dfd7d.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294251", name: "2026 Sylvan Mirage X3 CLZ Platinum", image: "https://cdn-media.tilabs.io/v1/media/6940ec0348b0ac5c31071009.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294356", name: "2026 Scarab 210LX", image: "https://cdn-media.tilabs.io/v1/media/690267d190fb9946b309d182.webp?width=1328&quality=70&upsize=true" },
  { id: "5038294378", name: "2003 Boston Whaler 180 VENTURE", image: "https://cdn-media.tilabs.io/v1/media/6940eae9d9f7a4c69d031396.webp?width=1328&quality=70&upsize=true" },
  { id: "5038298437", name: "2008 Suncatcher LX 325C", image: "https://cdn-media.tilabs.io/v1/media/6940e99ed159e4249400bd2b.webp?width=1328&quality=70&upsize=true" },
  { id: "5038298506", name: "2025 Montara SURF BOSS EVO SL 25'", image: "https://cdn-media.tilabs.io/v1/media/6902e88d431cede691030aa1.webp?width=1328&quality=70&upsize=true" },
  { id: "5038298508", name: "2026 Tiara Yachts 39LS", image: "https://cdn-media.tilabs.io/v1/media/6940e9e5c974b1ed7502454b.webp?width=1328&quality=70&upsize=true" },
  { id: "5038298525", name: "2026 Starcraft Marine VX 22 R DH", image: "https://cdn-media.tilabs.io/v1/media/6940e9f49408df034b0381ae.webp?width=1328&quality=70&upsize=true" },
  { id: "5038298531", name: "2026 Twin Vee Powercats 240 Center Console GFX2", image: "https://cdn-media.tilabs.io/v1/media/6940ea50d23d94c0070191ed.webp?width=1328&quality=70&upsize=true" },
  { id: "5038298534", name: "2020 Godfrey Pontoons SW 2086 BF", image: "https://cdn-media.tilabs.io/v1/media/6940e9ab07bfbf9aaf0d434a.webp?width=1328&quality=70&upsize=true" },
  { id: "5038298544", name: "2007 Hurricane 202 FUN DECK", image: "https://cdn-media.tilabs.io/v1/media/6940ea86269220bd94035704.webp?width=1328&quality=70&upsize=true" },
  { id: "5038298546", name: "2021 Craig Cat E2 ELITE", image: "https://cdn-media.tilabs.io/v1/media/6940e9d616d494271a01ae9e.webp?width=1328&quality=70&upsize=true" },
  { id: "5038298634", name: "2022 Massimo Marine P-23 Max Limited", image: "https://cdn-media.tilabs.io/v1/media/6940eae78e62472a1608350b.webp?width=1328&quality=70&upsize=true" },
  { id: "5038298643", name: "1990 Kingfisher XL 179", image: "https://cdn-media.tilabs.io/v1/media/6940ea018bab04ddf00139df.webp?width=1328&quality=70&upsize=true" },
  { id: "5038283191", name: "2025 Gator Tail Extreme Series 48\" x 18'", image: "https://cdn-media.tilabs.io/v1/media/6940ea089cfad48c550a931a.webp?width=1328&quality=70&upsize=true" },
  { id: "5038283777", name: "2016 Sealine C330", image: "https://cdn-media.tilabs.io/v1/media/6940ee2551d29e6472054c25.webp?width=1328&quality=70&upsize=true" },
];

// Phases
const PHASES = {
  idle: { label: "Ready", color: "#64748b" },
  annotating: { label: "Annotating", color: "#8b5cf6" },
  complete: { label: "Complete", color: "#10b981" },
  error: { label: "Error", color: "#ef4444" },
};

export default function App() {
  const [currentPhase, setCurrentPhase] = useState("idle");
  const [progress, setProgress] = useState({ current: 0, total: 0 });
  const [logs, setLogs] = useState([]);
  const [annotations, setAnnotations] = useState({});
  const [isProcessing, setIsProcessing] = useState(false);
  const [selectedListing, setSelectedListing] = useState(null);
  const [view, setView] = useState("grid"); // "grid" or "table"
  const [outputFileName, setOutputFileName] = useState(null);
  const logsEndRef = useRef(null);

  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logs]);

  const addLog = (message, type = "info") => {
    const timestamp = new Date().toLocaleTimeString();
    setLogs((prev) => [...prev, { timestamp, message, type }]);
  };

  const handleStartAnnotation = async () => {
    setIsProcessing(true);
    setCurrentPhase("annotating");
    setAnnotations({});
    setOutputFileName(null);
    setProgress({ current: 0, total: SCRAPED_DATA.length });
    addLog("🚀 Starting AI annotation pipeline...", "info");

    try {
      const eventSource = new EventSource(`${API_BASE_URL}/annotate/stream`);

      eventSource.onmessage = (event) => {
        const data = JSON.parse(event.data);

        if (data.status === "processing") {
          setProgress({ current: data.current, total: data.total });
          addLog(`Processing [${data.current}/${data.total}]: ${data.listing}`, "info");
        } else if (data.status === "completed") {
          const r = data.result;
          setAnnotations((prev) => ({
            ...prev,
            [data.id]: r,
          }));
          addLog(
            `✓ [${data.current}/${data.total}] ${r.Year} ${r.Brand} ${r.Full_Model_Name} | ${r.Trim_Series_Name} (${(r.Confidence_Score * 100).toFixed(0)}%)`,
            "success"
          );
          setProgress({ current: data.current, total: data.total });
        } else if (data.status === "error") {
          addLog(`✗ Error: ${data.message}`, "error");
        } else if (data.status === "finished") {
          addLog(`🎉 Annotation complete! ${data.total} listings processed`, "success");
          setCurrentPhase("complete");
          setIsProcessing(false);
          if (data.output_file) {
            setOutputFileName(data.output_file);
          }
          eventSource.close();
        }
      };

      eventSource.onerror = () => {
        addLog("Connection lost. Please check if backend is running.", "error");
        setCurrentPhase("error");
        setIsProcessing(false);
        eventSource.close();
      };
    } catch (error) {
      addLog(`Error: ${error.message}`, "error");
      setCurrentPhase("error");
      setIsProcessing(false);
    }
  };

  const handleReset = () => {
    setCurrentPhase("idle");
    setProgress({ current: 0, total: 0 });
    setLogs([]);
    setAnnotations({});
    setSelectedListing(null);
    setOutputFileName(null);
  };

  const handleDownload = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/download/latest`);
      
      if (!response.ok) {
        addLog("⚠️ Failed to download file. Please try again.", "error");
        return;
      }
      
      // Get the filename from the Content-Disposition header or use a default
      const contentDisposition = response.headers.get('content-disposition');
      let filename = outputFileName || 'annotated_output.xlsx';
      
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="?(.+)"?/);
        if (filenameMatch) {
          filename = filenameMatch[1];
        }
      }
      
      // Create a blob from the response and trigger download
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      
      addLog(`📥 Downloaded: ${filename}`, "success");
    } catch (error) {
      addLog(`❌ Download error: ${error.message}`, "error");
    }
  };

  const getConfidenceColor = (score) => {
    if (score >= 0.8) return "#10b981";
    if (score >= 0.5) return "#f59e0b";
    return "#ef4444";
  };

  const completedCount = Object.keys(annotations).length;

  return (
    <div className="app">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="logo">
          <span className="logo-icon">⚓</span>
          MMT Pipeline
        </div>
        <nav className="nav">
          <div className={`nav-item ${view === "grid" ? "active" : ""}`} onClick={() => setView("grid")}>
            <span className="nav-icon">🖼️</span>
            Gallery View
          </div>
          <div className={`nav-item ${view === "table" ? "active" : ""}`} onClick={() => setView("table")}>
            <span className="nav-icon">📊</span>
            Table View
          </div>
        </nav>
        
        <div className="sidebar-stats">
          <div className="stat-mini">
            <span className="stat-mini-value">{SCRAPED_DATA.length}</span>
            <span className="stat-mini-label">Listings</span>
          </div>
          <div className="stat-mini">
            <span className="stat-mini-value">{completedCount}</span>
            <span className="stat-mini-label">Annotated</span>
          </div>
        </div>

        <div className="sidebar-footer">
          <div className="version">v2.0.0</div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="main">
        <header className="header">
          <div>
            <h1 className="page-title">Marine Listing Annotator</h1>
            <p className="page-subtitle">
              {SCRAPED_DATA.length} listings ready for AI annotation
            </p>
          </div>
          <div className="header-actions">
            <div className="phase-badge" style={{ background: PHASES[currentPhase]?.color }}>
              {PHASES[currentPhase]?.label}
            </div>
            <button
              className="btn btn-primary"
              onClick={handleStartAnnotation}
              disabled={isProcessing}
            >
              {isProcessing ? (
                <>
                  <span className="spinner"></span>
                  Annotating... {progress.current}/{progress.total}
                </>
              ) : (
                <>🤖 Start Annotation</>
              )}
            </button>
            <button className="btn btn-secondary" onClick={handleReset} disabled={isProcessing}>
              🔄 Reset
            </button>
            {currentPhase === "complete" && (
              <button className="btn btn-download" onClick={handleDownload}>
                📥 Download Excel
              </button>
            )}
          </div>
        </header>

        {/* Progress Bar */}
        {isProcessing && (
          <div className="progress-strip">
            <div
              className="progress-strip-bar"
              style={{ width: `${(progress.current / progress.total) * 100}%` }}
            ></div>
          </div>
        )}

        <div className="content-grid">
          {/* Left - Listings */}
          <div className="listings-panel">
            {view === "grid" ? (
              <div className="listings-grid">
                {SCRAPED_DATA.map((item) => {
                  const annotation = annotations[item.id];
                  const isSelected = selectedListing?.id === item.id;
                  
                  return (
                    <div
                      key={item.id}
                      className={`listing-card ${annotation ? "annotated" : ""} ${isSelected ? "selected" : ""}`}
                      onClick={() => setSelectedListing(item)}
                    >
                      <div className="listing-image-container">
                        <img src={item.image} alt={item.name} className="listing-image" />
                        {annotation && (
                          <div
                            className="confidence-overlay"
                            style={{ background: getConfidenceColor(annotation.Confidence_Score) }}
                          >
                            {(annotation.Confidence_Score * 100).toFixed(0)}%
                          </div>
                        )}
                      </div>
                      <div className="listing-info">
                        <div className="listing-id">#{item.id}</div>
                        <div className="listing-name">{item.name}</div>
                        {annotation && (
                          <div className="listing-annotation">
                            <span className="anno-brand">{annotation.Brand}</span>
                            <span className="anno-model">{annotation.Full_Model_Name}</span>
                            {annotation.Trim_Series_Name && (
                              <span className="anno-trim">{annotation.Trim_Series_Name}</span>
                            )}
                          </div>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <div className="listings-table-container">
                <table className="listings-table">
                  <thead>
                    <tr>
                      <th>Image</th>
                      <th>Scraped Name</th>
                      <th>Year</th>
                      <th>Brand</th>
                      <th>Model</th>
                      <th>Trim</th>
                      <th>Conf.</th>
                    </tr>
                  </thead>
                  <tbody>
                    {SCRAPED_DATA.map((item) => {
                      const annotation = annotations[item.id];
                      return (
                        <tr
                          key={item.id}
                          className={selectedListing?.id === item.id ? "selected" : ""}
                          onClick={() => setSelectedListing(item)}
                        >
                          <td>
                            <img src={item.image} alt="" className="table-thumb" />
                          </td>
                          <td className="scraped-name-cell">{item.name}</td>
                          <td>{annotation?.Year || "-"}</td>
                          <td>{annotation?.Brand || "-"}</td>
                          <td className="model-cell">{annotation?.Full_Model_Name || "-"}</td>
                          <td>{annotation?.Trim_Series_Name || "-"}</td>
                          <td>
                            {annotation ? (
                              <span
                                className="confidence-badge"
                                style={{ background: getConfidenceColor(annotation.Confidence_Score) }}
                              >
                                {(annotation.Confidence_Score * 100).toFixed(0)}%
                              </span>
                            ) : (
                              "-"
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* Right - Details & Logs */}
          <div className="details-panel">
            {/* Selected Listing Detail */}
            {selectedListing && (
              <div className="card detail-card">
                <img src={selectedListing.image} alt="" className="detail-image" />
                <div className="detail-content">
                  <div className="detail-id">Ad ID: {selectedListing.id}</div>
                  <div className="detail-scraped">
                    <label>Scraped Name</label>
                    <div className="detail-scraped-value">{selectedListing.name}</div>
                  </div>
                  
                  {annotations[selectedListing.id] && (
                    <div className="detail-annotation">
                      <label>AI Annotation</label>
                      <div className="detail-fields">
                        <div className="detail-field">
                          <span className="field-label">Year</span>
                          <span className="field-value">{annotations[selectedListing.id].Year}</span>
                        </div>
                        <div className="detail-field">
                          <span className="field-label">Brand</span>
                          <span className="field-value">{annotations[selectedListing.id].Brand}</span>
                        </div>
                        <div className="detail-field">
                          <span className="field-label">Model</span>
                          <span className="field-value">{annotations[selectedListing.id].Full_Model_Name}</span>
                        </div>
                        <div className="detail-field">
                          <span className="field-label">Trim</span>
                          <span className="field-value">{annotations[selectedListing.id].Trim_Series_Name || "N/A"}</span>
                        </div>
                        <div className="detail-field">
                          <span className="field-label">Confidence</span>
                          <span
                            className="field-value confidence-badge"
                            style={{ background: getConfidenceColor(annotations[selectedListing.id].Confidence_Score) }}
                          >
                            {(annotations[selectedListing.id].Confidence_Score * 100).toFixed(0)}%
                          </span>
                        </div>
                      </div>
                      <div className="detail-reasoning">
                        <label>Reasoning</label>
                        <p>{annotations[selectedListing.id].Reasoning}</p>
                      </div>
                      {annotations[selectedListing.id].Suggested_Trims?.length > 0 && (
                        <div className="detail-suggested">
                          <label>Suggested Trims</label>
                          <div className="suggested-tags">
                            {annotations[selectedListing.id].Suggested_Trims.map((trim, i) => (
                              <span key={i} className="suggested-tag">{trim}</span>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Activity Log */}
            <div className="card logs-card">
              <h2 className="card-title">📋 Activity Log</h2>
              <div className="logs-container">
                {logs.length === 0 ? (
                  <div className="logs-empty">Click "Start Annotation" to begin processing...</div>
                ) : (
                  logs.map((log, i) => (
                    <div key={i} className={`log-item log-${log.type}`}>
                      <span className="log-time">{log.timestamp}</span>
                      <span className="log-message">{log.message}</span>
                    </div>
                  ))
                )}
                <div ref={logsEndRef} />
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
