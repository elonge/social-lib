from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Annotated
import uuid
import time
from book_extractor import process_image_bytes
from book_enricher import BookEnricher
from deduplicator import BookDeduplicator
from session_manager import get_session_store
from image_storage import get_image_storage
from document_store import MongoDocumentStore, PartitionKey, SortKey, default_to_dict as to_dict

@dataclass
class RawLibraryEntry:
    user_id: Annotated[str, PartitionKey]
    frame_id: Annotated[int, SortKey]
    name: Optional[str] = None
    books: List[Dict[str, Any]] = field(default_factory=list)

@dataclass
class LibraryBook:
    user_id: Annotated[str, PartitionKey]
    book_id: Annotated[str, SortKey] # ISBN or title|author
    title: str
    author: Optional[str] = None
    isbn: Optional[str] = None
    frame_ids: List[int] = field(default_factory=list)

app = FastAPI(title="Book spine extractor API")

# Initialize session store
session_store = get_session_store("memory")

# Initialize image storage
image_storage = get_image_storage("file")

# Initialize document stores
# Use environment variables if available
mongo_conn = "mongodb://localhost:27017/"
raw_library_store = MongoDocumentStore[RawLibraryEntry, RawLibraryEntry](
    connection_string=mongo_conn,
    database_name="social_lib",
    collection_name="user_raw_library"
)

user_library_store = MongoDocumentStore[LibraryBook, LibraryBook](
    connection_string=mongo_conn,
    database_name="social_lib",
    collection_name="user_library"
)
raw_library_store.create_table()
user_library_store.create_table()

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
    session_id: Optional[str] = None
    enrich: Optional[bool] = False
    metadata: Dict[str, Any] = {}
    user_id: str = "default_user"

@app.get("/init_upload")
@app.post("/init_upload")
async def init_upload():
    """
    Initializes a new upload session.
    """
    session_id = str(uuid.uuid4())
    session_store.create_session(session_id, {}, ttl_seconds=3600)
    return {"status": "success", "session_id": session_id}

@app.post("/upload_frame")
async def upload_frame(
    file: UploadFile = File(...), 
    session_id: Optional[str] = Form(None), 
    frame_id: Optional[int] = Form(None),
    user_id: Optional[str] = Form("default_user"),
    name: Optional[str] = Form(None)
):
    """
    Receives an image file, extracts books, enriches them, and returns them with a count.
    Saves results to session using books_<frame_id> if session_id is provided.
    Saves raw library entry to MongoDB.
    """
    # Read image bytes
    content = await file.read()
    
    # 0. Handle frame_id fallback
    if frame_id is None:
        frame_id = time.time_ns() // 1_000_000
    
    # 0.5 Save the frame image
    image_path = image_storage.save_image(user_id, str(frame_id), content)
    
    # 1. Process the image using Gemini to extract raw books
    result = process_image_bytes(image_bytes=content)
    raw_books = result.get("books", [])
    
    # 2. Immediately enrich and deduplicate (counting mode)
    if raw_books:
        enricher = BookEnricher()
        enriched_books, stats = enricher.batch_enrich(raw_books, dedupe_mode="counting")
        
        # 2.5 Save to raw library document store
        entry = RawLibraryEntry(user_id=user_id, frame_id=frame_id, name=name, books=enriched_books)
        raw_library_store.put(entry, entry)

        response_data = {
            "status": "success",
            "books": enriched_books,
            "enrichment_stats": stats,
            "usage": result.get("usage"),
            "image_path": image_path,
            "frame_id": frame_id,
            "name": name
        }
        
        # 3. Save to session if session_id is provided
        if session_id:
            print(f"Saving to session: {session_id}, frame_id: {frame_id}")
            session_store.put(session_id, f"books_{frame_id}", response_data)
            response_data["session_id"] = session_id
            
        return response_data
    
    return result

@app.post("/complete_upload")
async def complete_upload(request: CompleteUploadRequest):
    """
    Receives a list of JSON results from upload_frame.
    Deduplicates books by proximity (already enriched).
    Saves aggregated library to MongoDB.
    """
    results = request.results
    user_id = request.user_id
    
    # If results are not provided, try to fetch from session
    if not results and request.session_id:
        full_session = session_store.get_session(request.session_id)
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
        
    all_books_to_dedupe = []
    
    # Collect all books and attach their frame_id for deduplication tracking
    for res in results:
        frame_id = res.get("frame_id")
        books = res.get("books", [])
        for book in books:
            book_copy = book.copy()
            book_copy["frame_id"] = frame_id
            all_books_to_dedupe.append(book_copy)
    
    # 1. Apply proximity-based deduplication
    final_books = BookDeduplicator.deduplicate_proximity(all_books_to_dedupe)
    dedupe_count = len(all_books_to_dedupe) - len(final_books)
    
    # 2. Save aggregated books to user_library document store
    for book in final_books:
        book_id = book.get("isbn")
        if not book_id:
            title = book.get("title", "").lower().strip()
            author = book.get("author", "").lower().strip()
            book_id = f"{title}|{author}"
        
        lib_book = LibraryBook(
            user_id=user_id,
            book_id=book_id,
            title=book.get("title", "Unknown"),
            author=book.get("author"),
            isbn=book.get("isbn"),
            frame_ids=book.get("frame_ids", [])
        )
        user_library_store.put(lib_book, lib_book)

    # 3. Cleanup session if it was used
    if request.session_id:
        print(f"Deleting session: {request.session_id}")
        session_store.delete_session(request.session_id)
    
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
