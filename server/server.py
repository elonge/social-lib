from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import uuid
import time
from book_extractor import process_image_bytes
from book_enricher import BookEnricher
from deduplicator import BookDeduplicator
from session_manager import get_session_store

app = FastAPI(title="Book spine extractor API")

# Initialize session store (using memory for now, can be switched to redis via env)
session_store = get_session_store("memory")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

class CompleteUploadRequest(BaseModel):
    results: Optional[List[Dict[str, Any]]] = None
    sessionId: Optional[str] = None
    enrich: Optional[bool] = False
    metadata: Dict[str, Any] = {}

@app.get("/init_upload")
@app.post("/init_upload")
async def init_upload():
    """
    Initializes a new upload session.
    """
    session_id = str(uuid.uuid4())
    session_store.createSession(session_id, {}, ttl_seconds=3600)
    return {"status": "success", "sessionId": session_id}

@app.post("/upload_frame")
async def upload_frame(file: UploadFile = File(...), sessionId: Optional[str] = Form(None), frame_id: Optional[int] = Form(None)):
    """
    Receives an image file, extracts books, enriches them, and returns them with a count.
    Saves results to session using books_<frame_id> if sessionId is provided.
    If frame_id is not provided, current millisecond timestamp is used.
    """
    # Read image bytes
    content = await file.read()
    
    # 0. Handle frame_id fallback
    if frame_id is None:
        frame_id = time.time_ns() // 1_000_000
    
    # 1. Process the image using Gemini to extract raw books
    result = process_image_bytes(image_bytes=content)
    raw_books = result.get("books", [])
    
    # 2. Immediately enrich and deduplicate (counting mode)
    if raw_books:
        enricher = BookEnricher()
        enriched_books, stats = enricher.batch_enrich(raw_books, dedupe_mode="counting")
        
        response_data = {
            "status": "success",
            "books": enriched_books,
            "enrichment_stats": stats,
            "usage": result.get("usage")
        }
        
        # 3. Save to session if sessionId is provided
        if sessionId:
            print(f"Saving to session: {sessionId}, frame_id: {frame_id}")
            session_store.put(sessionId, f"books_{frame_id}", response_data)
            response_data["sessionId"] = sessionId
            response_data["frame_id"] = frame_id
            
        return response_data
    
    return result

@app.post("/complete_upload")
async def complete_upload(request: CompleteUploadRequest):
    """
    Receives a list of JSON results from upload_next_frame.
    Deduplicates books by proximity (already enriched).
    """
    results = request.results
    
    # If results are not provided, try to fetch from session
    if not results and request.sessionId:
        full_session = session_store.getSession(request.sessionId)
        if not full_session:
            return {"status": "error", "message": "Session not found"}
        
        # Aggregate all frame results (keys starting with books_)
        # Sort keys numerically based on the frame_id suffix
        frame_keys = [k for k in full_session.keys() if k.startswith("books_")]
        
        def get_frame_num(k):
            try:
                return int(k.split("_")[1])
            except (IndexError, ValueError):
                return 0
                
        frame_keys.sort(key=get_frame_num)
        results = [full_session[k] for k in frame_keys]
        
        if not results:
            return {"status": "error", "message": "Session empty (no frames found)"}
            
    if not results:
        return {"status": "error", "message": "No results provided and no session found"}
        
    all_books = []
    
    # Collect all books in their original sequence
    for res in results:
        books = res.get("books", [])
        all_books.extend(books)
    
    # 1. Apply proximity-based deduplication
    # dedupe_window defaults to 5
    final_books = BookDeduplicator.deduplicate_proximity(all_books)
    dedupe_count = len(all_books) - len(final_books)
    
    # 2. Cleanup session if it was used
    if request.sessionId:
        print(f"Deleting session: {request.sessionId}")
        session_store.deleteSession(request.sessionId)
    
    return {
        "status": "success",
        "total_books_found": len(final_books),
        "books": final_books,
        "deduplication_stats": {
            "proximity_deduped": dedupe_count
        }
    }

@app.post("/enrich_book")
async def enrich_book(book: Dict[str, Any]):
    """
    Receives a single book's metadata and returns enriched metadata and diagnostics.
    """
    enricher = BookEnricher()
    enriched, diagnostics = enricher.enrich_book(book)
    return {
        "book": enriched,
        "diagnostics": diagnostics
    }

@app.post("/enrich_books")
async def enrich_books(books: List[Dict[str, Any]]):
    """
    Receives a list of book metadata and returns enriched results and batch diagnostics.
    """
    enricher = BookEnricher()
    enriched, stats = enricher.batch_enrich(books)
    return {
        "books": enriched,
        "batch_stats": stats
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
