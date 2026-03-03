# MMT Pipeline - Marine Listing Processor

A full-stack application for scraping, annotating, and normalizing marine vessel listings using AI.

## Features

- **Web Scraping**: Automatically scrape boat listings from BoatMart.com
- **AI Annotation**: Use Google Gemini with grounded search to normalize listing data
- **Real-time Progress**: Live SSE streaming for both scraping and annotation progress
- **Beautiful UI**: Modern React dashboard with live activity logs
- **Export Results**: Download annotated Excel files

## Architecture

```
React Frontend (Port 3000) -> FastAPI Backend (Port 8000) -> Gemini AI + Grounding
                                      |
                                      v
                                BoatMart.com (Scraping)
```

## Prerequisites

- Python 3.10+
- Node.js 18+
- Google Gemini API Key (Get one at https://aistudio.google.com/apikey)

## Quick Start

### 1. Clone and Setup

```bash
cd MMT-Demo
```

### 2. Configure Environment

Edit the `.env` file in `Gemini-GS/`:

```bash
# Edit this file
nano Gemini-GS/.env
```

Add your Gemini API key:

```env
GEMINI_API_KEY=your_actual_api_key_here
GEMINI_RPM_PER_KEY=20
```

### 3. Start the Backend

```bash
cd Gemini-GS
python3 backend.py
```

The backend will start on `http://localhost:8000`

**Note**: Make sure you have installed all dependencies first:
```bash
cd Gemini-GS
pip install -r requirements.txt
```

### 4. Start the Frontend

In a new terminal:

```bash
cd frontend/myapp
npm start
```

The frontend will start on `http://localhost:3000`

**Note**: If this is your first time, install dependencies first:
```bash
cd frontend/myapp
npm install
```

### 5. Access the Application

- **Frontend**: http://localhost:3000
- **API Docs**: http://localhost:8000/docs

## Usage Workflow

1. **Upload**: Select an Excel file with a `Listing_id` column
2. **Scrape**: Click "Start Pipeline" to begin scraping boat details
3. **Annotate**: AI automatically processes scraped data
4. **Download**: Get the annotated Excel file with normalized fields

## Expected Input Format

Your Excel file should have at minimum a `Listing_id` column containing BoatMart listing IDs:

| Listing_id |
|------------|
| 5036910391 |
| 5036963575 |
| 5036963702 |

## Output Fields

The pipeline adds these AI-annotated columns:

| Field | Description |
|-------|-------------|
| `AI_Year` | Extracted/verified year |
| `AI_Make` | Normalized brand name |
| `AI_Model` | Full model name |
| `AI_Trim` | Size/layout designation |
| `AI_Suggested_Trims` | Alternative trims if uncertain |
| `Confidence_Score` | AI confidence (0-1) |
| `AI_Reasoning` | Explanation of normalization |

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/upload` | Upload Excel file |
| `GET` | `/process/{session_id}/stream` | SSE stream for pipeline progress |
| `GET` | `/download/{session_id}` | Download annotated file |
| `GET` | `/preview/{session_id}` | Preview results as JSON |
| `DELETE` | `/session/{session_id}` | Cleanup session |

## Tech Stack

**Frontend:**
- React 18
- CSS3 with custom properties
- Server-Sent Events (SSE)

**Backend:**
- FastAPI
- Google Gemini AI (gemini-2.5-flash)
- BeautifulSoup4 for scraping
- Pandas + OpenPyXL for Excel processing

## Project Structure

```
MMT-Demo/
├── Gemini-GS/
│   ├── backend.py          # Unified FastAPI backend
│   ├── script.py           # Original annotation script
│   ├── requirements.txt    # Python dependencies
│   ├── .env               # Environment variables
│   ├── uploads/           # Uploaded files
│   ├── outputs/           # Processed files
│   └── downloaded_images/ # Scraped images
├── frontend/
│   └── myapp/
│       ├── src/
│       │   ├── App.js     # Main React component
│       │   └── App.css    # Styling
│       └── package.json
├── start_backend.sh       # Backend startup script
├── start_frontend.sh      # Frontend startup script
└── README.md
```

## Troubleshooting

### "No Gemini API keys found"
- Ensure `.env` file exists in `Gemini-GS/` directory
- Check that `GEMINI_API_KEY` is set correctly

### Rate Limiting (429 errors)
- Reduce `GEMINI_RPM_PER_KEY` in `.env`
- Add multiple API keys for higher throughput

### CORS Errors
- Ensure backend is running on port 8000
- Check that frontend is on port 3000

## License

MIT License
