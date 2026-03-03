import "./LiveAnnotation.css";
import { useEffect, useRef } from "react";

export default function LiveAnnotation({ annotations }) {
  const endRef = useRef(null);

  // Auto-scroll to bottom when new annotations are added
  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [annotations]);

  return (
    <div className="status-box">
      {annotations.length === 0 ? (
        <div className="status-empty">
          System idle. Awaiting input…
        </div>
      ) : (
        <>
          {annotations.map((item, i) => (
            <div key={i} className="status-item">
              <span className="status-time">
                {new Date().toLocaleTimeString()}
              </span>
              {" "}{item}
            </div>
          ))}
          <div ref={endRef} />
        </>
      )}
    </div>
  );
}
