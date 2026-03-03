from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import shutil
import os
import uuid
import json
import asyncio
from queue import Queue
from threading import Thread

from Scrapper import Scrapper

app = FastAPI(title="Boatmart Scraper API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store active scraping sessions
active_sessions = {}


@app.post("/scrape")
async def scrape_boatmart(file: UploadFile = File(...)):
    """Upload file and start scraping with real-time progress"""
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only Excel files allowed")

    session_id = str(uuid.uuid4())
    input_path = f"input_{session_id}.xlsx"
    output_path = "output_with_scrapped.xlsx"

    # Save uploaded file
    with open(input_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Create a queue for progress updates
    progress_queue = Queue()
    active_sessions[session_id] = {
        "queue": progress_queue,
        "input_path": input_path,
        "output_path": output_path
    }

    return {"session_id": session_id, "message": "File uploaded successfully"}


@app.get("/scrape/{session_id}/stream")
async def stream_progress(session_id: str):
    """Stream scraping progress in real-time using Server-Sent Events"""
    if session_id not in active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    session = active_sessions[session_id]
    progress_queue = session["queue"]
    input_path = session["input_path"]
    output_path = session["output_path"]

    async def event_generator():
        # Start scraping in a separate thread
        def run_scraper():
            def progress_callback(data):
                progress_queue.put(data)
            
            try:
                Scrapper(input_path, output_path, progress_callback)
            except Exception as e:
                progress_queue.put({"status": "error", "message": str(e)})
            finally:
                progress_queue.put(None)  # Signal completion

        Thread(target=run_scraper, daemon=True).start()

        # Stream progress updates
        while True:
            await asyncio.sleep(0.1)  # Small delay to prevent CPU spinning
            
            if not progress_queue.empty():
                data = progress_queue.get()
                
                if data is None:  # Scraping finished
                    yield f"data: {json.dumps({'status': 'done'})}\n\n"
                    # Cleanup
                    if os.path.exists(input_path):
                        os.remove(input_path)
                    del active_sessions[session_id]
                    break
                
                yield f"data: {json.dumps(data)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/download")
async def download_output():
    """Download the scraped output file"""
    output_path = "output_with_scrapped.xlsx"
    
    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="Output file not found")
    
    return FileResponse(
        output_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="output_with_scrapped.xlsx",
    )


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}