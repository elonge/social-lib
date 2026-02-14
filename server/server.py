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


@dataclass(frozen=True)
class RawLibraryKey:
    user_id: Annotated[str, PartitionKey]
    frame_id: Annotated[int, SortKey]

@dataclass
class RawLibraryEntry:
    user_id: str
    frame_id: int
    name: Optional[str] = None
    books: List[Dict[str, Any]] = field(default_factory=list)

@dataclass(frozen=True)
class LibraryBookKey:
    user_id: Annotated[str, PartitionKey]
    shelf_name: Annotated[str, SortKey(order=1)]
    book_id: Annotated[str, SortKey(order=2)] # ISBN or title|author

@dataclass
class LibraryBook:
    user_id: str
    shelf_name: str
    book_id: str
    title: str
    author: Optional[str] = None
    isbn: Optional[str] = None
    frame_ids: List[int] = field(default_factory=list)

@dataclass(frozen=True)
class ShelfFrameMetadataKey:
    user_id: Annotated[str, PartitionKey]
    frame_id: Annotated[int, SortKey]

@dataclass
class ShelfFrameMetadata:
    user_id: str
    frame_id: int
    shelf_name: Optional[str]
    book_count: int
    uploaded_at: float

app = FastAPI(title="Book spine extractor API")

# Initialize session store
session_store = get_session_store("memory")

# Initialize image storage
image_storage = get_image_storage("file")

# Initialize document stores
# Use environment variables if available
mongo_conn = "mongodb://localhost:27017/"
raw_library_store = MongoDocumentStore[RawLibraryKey, RawLibraryEntry](
    connection_string=mongo_conn,
    database_name="social_lib",
    collection_name="user_raw_library",
    key_type=RawLibraryKey,
    data_type=RawLibraryEntry
)

user_library_store = MongoDocumentStore[LibraryBookKey, LibraryBook](
    connection_string=mongo_conn,
    database_name="social_lib",
    collection_name="user_library",
    key_type=LibraryBookKey,
    data_type=LibraryBook
)

shelf_metadata_store = MongoDocumentStore[ShelfFrameMetadataKey, ShelfFrameMetadata](
    connection_string=mongo_conn,
    database_name="social_lib",
    collection_name="shelf_metadata",
    key_type=ShelfFrameMetadataKey,
    data_type=ShelfFrameMetadata
)

raw_library_store.create_table()
user_library_store.create_table()
shelf_metadata_store.create_table()

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
    result = await process_image_bytes(image_bytes=content)
    raw_books = result.get("books", [])
    
    # 2. Immediately enrich and deduplicate (counting mode)
    if raw_books:
        enricher = BookEnricher()
        enriched_books, stats = await enricher.batch_enrich(raw_books, dedupe_mode="counting")
        
        # 2.5 Save to raw library document store
        # 2.5 Save to raw library document store
        entry_key = RawLibraryKey(user_id=user_id, frame_id=frame_id)
        entry = RawLibraryEntry(user_id=user_id, frame_id=frame_id, name=name, books=enriched_books)
        raw_library_store.put(entry_key, entry)
        
        # 2.6 Save shelf frame metadata
        # 2.6 Save shelf frame metadata
        meta_key = ShelfFrameMetadataKey(user_id=user_id, frame_id=frame_id)
        metadata = ShelfFrameMetadata(
            user_id=user_id,
            frame_id=frame_id,
            shelf_name=name,
            book_count=len(enriched_books),
            uploaded_at=time.time()
        )
        shelf_metadata_store.put(meta_key, metadata)

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

    frame_to_shelf = {res.get("frame_id"): res.get("name") for res in results}

    deduped_books = BookDeduplicator.deduplicate_proximity(all_books_to_dedupe)

    deduped_count = len(all_books_to_dedupe) - len(deduped_books)

    books_by_shelf: Dict[str, List[Dict[str, Any]]] = {}
    unshelved_books: List[Dict[str, Any]] = []

    # Group books by shelf
    for book in deduped_books:
        # Determine shelf for this book
        # Current logic: Book has `frame_ids` list.
        # We'll create a LibraryBook entry for EACH unique shelf it belongs to.
        
        book_frame_ids = book.get("frame_ids", [])
        if not book_frame_ids:
            unshelved_books.append(book)
            continue
            
        shelves_for_book = set()
        for fid in book_frame_ids:
            s_name = frame_to_shelf.get(fid)
            if s_name:
                shelves_for_book.add(s_name)
        
        if not shelves_for_book:
            unshelved_books.append(book)
        else:
            for s_name in shelves_for_book:
                if s_name not in books_by_shelf:
                    books_by_shelf[s_name] = []
                books_by_shelf[s_name].append(book)

    # For each affected shelf: Delete old content and Insert new
    for s_name, shelf_books in books_by_shelf.items():
        # Delete range for (user_id, shelf_name)
        # We need a way to specify partial sort key for range delete?
        # DocumentStore.delete_range(start, end)
        # start = (user_id, shelf_name, "")
        # end = (user_id, shelf_name, "\uffff")
        
        del_start = LibraryBookKey(user_id=user_id, shelf_name=s_name, book_id="")
        del_end = LibraryBookKey(user_id=user_id, shelf_name=s_name, book_id="\uffff")
        user_library_store.delete_range(del_start, del_end)
        
        # Insert new books
        for book in shelf_books:
            book_id = book.get("isbn")
            if not book_id:
                title = book.get("title", "").lower().strip()
                author = book.get("author", "").lower().strip()
                book_id = f"{title}|{author}"
            
            lib_key = LibraryBookKey(user_id=user_id, shelf_name=s_name, book_id=book_id)
            lib_book = LibraryBook(
                user_id=user_id,
                shelf_name=s_name,
                book_id=book_id,
                title=book.get("title", "Unknown"),
                author=book.get("author"),
                isbn=book.get("isbn"),
                frame_ids=book.get("frame_ids", [])
            )
            user_library_store.put(lib_key, lib_book)
            
    if unshelved_books:
        s_name = "Unshelved" 
        
        del_start = LibraryBookKey(user_id=user_id, shelf_name=s_name, book_id="")
        del_end = LibraryBookKey(user_id=user_id, shelf_name=s_name, book_id="\uffff")
        user_library_store.delete_range(del_start, del_end)

        for book in unshelved_books:
            book_id = book.get("isbn")
            if not book_id:
                title = book.get("title", "").lower().strip()
                author = book.get("author", "").lower().strip()
                book_id = f"{title}|{author}"

            lib_key = LibraryBookKey(user_id=user_id, shelf_name=s_name, book_id=book_id)
            lib_book = LibraryBook(
                user_id=user_id,
                shelf_name=s_name,
                book_id=book_id,
                title=book.get("title", "Unknown"),
                author=book.get("author"),
                isbn=book.get("isbn"),
                frame_ids=book.get("frame_ids", [])
            )
            user_library_store.put(lib_key, lib_book)

    # 4. Cleanup session if it was used
    if request.session_id:
        print(f"Deleting session: {request.session_id}")
        session_store.delete_session(request.session_id)
    
    return {
        "status": "success",
        "total_books_found": len(deduped_books),
        "books": deduped_books,
        "deduplication_stats": {
            "proximity_deduped": deduped_count
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

@app.get("/user_library")
async def get_user_library(user_id: str = "default_user"):
    """
    Returns the user's library organized by shelf.
    """
    # 1. Fetch all shelf metadata for user
    # Range query from (user_id, 0) to (user_id, max_int)
    # We use tuples directly since DocumentStore supports it
    # Fetch all books for user
    # Range query from (user_id, "", "") to (user_id, "\uffff", "\uffff")
    # Because sort key is now (shelf_name, book_id)
    b_start = LibraryBookKey(user_id=user_id, shelf_name="", book_id="")
    b_end = LibraryBookKey(user_id=user_id, shelf_name="\uffff", book_id="\uffff")
    
    book_rows = user_library_store.get_range(b_start, b_end)
    
    shelves: Dict[str, List[LibraryBook]] = {}
    unshelved: List[LibraryBook] = []
    
    for _, book in book_rows:
        s_name = book.shelf_name
        if s_name == "Unshelved":
            unshelved.append(book)
        else:
            if s_name not in shelves:
                shelves[s_name] = []
            shelves[s_name].append(book)

    return {
        "status": "success",
        "shelves": shelves,
        "unshelved": unshelved
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
