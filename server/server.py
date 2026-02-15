from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Annotated
import os
import uuid
import time
import os
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, auth
from db_model import FrameUploadKey, FrameUploadEntry, LibraryBookKey, LibraryBook, ShelfFrameMetadataKey, ShelfFrameMetadata, LibraryKey, Library, Shelf, ShelfKey

from book_extractor import process_image_bytes
from book_enricher import BookEnricher
from deduplicator import BookDeduplicator
from session_manager import get_session_store
from image_storage import get_image_storage
from document_store import MongoDocumentStore, InMemoryDocumentStore, PartitionKey, SortKey, default_to_dict as to_dict
from pymongo import MongoClient
from google.cloud import secretmanager

load_dotenv()

project_id = os.getenv("GOOGLE_CLOUD_PROJECT")

def get_config_value(env_var_name: str, secret_name: str) -> Optional[str]:
    """
    Get configuration value from environment variable or GCP Secret Manager.
    If fetched from Secret Manager, caches the value in the environment variable.
    
    Args:
        env_var_name: Name of the environment variable to check first
        secret_name: Name of the secret in GCP Secret Manager
        project_id: GCP project ID (defaults to GOOGLE_CLOUD_PROJECT env var)
    
    Returns:
        The configuration value or None if not found
    """
    # First, try to get from environment variable
    value = os.getenv(env_var_name)
    if value:
        return value
    
    # Fall back to GCP Secret Manager
    try:
        if not project_id:
            print(f"Warning: Cannot fetch secret '{secret_name}' - no project_id provided and GOOGLE_CLOUD_PROJECT not set")
            return None

        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": secret_path})
        value = response.payload.data.decode("UTF-8")
        
        # Cache the value in environment variable for future calls
        os.environ[env_var_name] = value
        
        print("Using secret manager for ", secret_name)
        
        return value
    except Exception as e:
        print(f"Warning: Failed to fetch secret '{secret_name}' from GCP Secret Manager: {e}")
        return None

test_user = os.environ.get("TEST_USER")

# Initialize Firebase Admin
if not firebase_admin._apps and not test_user:
    try:
        # 1. Try to load from FIREBASE_SERVICE_ACCOUNT_JSON env var (for Cloud Run)
        service_account_json = get_config_value("FIREBASE_SERVICE_ACCOUNT_JSON", "firebase-service-account")
        if service_account_json:
            import json
            cert = json.loads(service_account_json)
            cred = credentials.Certificate(cert)
            firebase_admin.initialize_app(cred)
            print("Initialized Firebase Admin using FIREBASE_SERVICE_ACCOUNT_JSON env var")
        else:
            # 2. Fallback to implicit credentials (GOOGLE_APPLICATION_CREDENTIALS) or default
            cred = credentials.ApplicationDefault()
            firebase_admin.initialize_app(cred)
            print("Initialized Firebase Admin using Application Default Credentials")
    except Exception as e:
        print(f"Warning: Firebase Admin initialization failed: {e}")
        print("Auth verification may fail if credentials are not set.")

# Get the API key for gemini
get_config_value("GOOGLE_API_KEY", "google-api-key")

app = FastAPI(title="Book spine extractor API")

# Initialize session store
session_store = get_session_store("memory" if os.getenv("GOOGLE_API_KEY") else "redis")

# Initialize image storage
image_storage = get_image_storage("file" if os.getenv("GOOGLE_API_KEY") else "gcs")

# Initialize document stores
# Use environment variables if available
mongo_conn = get_config_value("MONGODB_CONNECTION", "mongodb-connection")
mongo_client = MongoClient(mongo_conn)

user_uploads_store = MongoDocumentStore[FrameUploadKey, FrameUploadEntry](
    client=mongo_client,
    database_name="social_lib",
    collection_name="user_uploads",
    key_type=FrameUploadKey,
    data_type=FrameUploadEntry
)

user_books_store = MongoDocumentStore[LibraryBookKey, LibraryBook](
    client=mongo_client,
    database_name="social_lib",
    collection_name="user_books",
    key_type=LibraryBookKey,
    data_type=LibraryBook
)

user_shelf_frame_metadata_store = MongoDocumentStore[ShelfFrameMetadataKey, ShelfFrameMetadata](
    client=mongo_client,
    database_name="social_lib",
    collection_name="user_shelf_frame_metadata",
    key_type=ShelfFrameMetadataKey,
    data_type=ShelfFrameMetadata
)

user_library_store = MongoDocumentStore[LibraryKey, Library](client=mongo_client, database_name="social_lib", collection_name="user_library", key_type=LibraryKey, data_type=Library)
user_shelf_store = MongoDocumentStore[ShelfKey, Shelf](client=mongo_client, database_name="social_lib", collection_name="user_shelf", key_type=ShelfKey, data_type=Shelf)

user_uploads_store.create_table()
user_library_store.create_table()
user_shelf_frame_metadata_store.create_table()
user_library_store.create_table()
user_shelf_store.create_table()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if test_user:
        return {"uid": test_user}

    print("ZRZRZR", test_user)

    token = credentials.credentials
    try:
        # Verify the Firebase token
        decoded_token = auth.verify_id_token(token)
        return decoded_token
    except Exception as e:
        print(f"Auth Error: {e}") # DEBUG LOG
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid authentication credentials: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )

@dataclass(frozen=True)
class UserKey:
    uid: Annotated[str, PartitionKey]

@dataclass
class User:
    uid: str
    email: Optional[str] = None
    display_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    picture: Optional[str] = None
    created_at: float = field(default_factory=time.time)

# Initialize user store
# Fallback to InMemoryDocumentStore if Mongo is not available or for simpler dev setup
# To use Mongo: Ensure MongoDB is running and MONGO_CONNECTION_STRING is set
user_store = InMemoryDocumentStore[UserKey, User](
    key_type=UserKey,
    data_type=User
)
# user_store.create_table() # InMemory doesn't need create_table usually, but abstract method exists
user_store.create_table()


class CompleteUploadRequest(BaseModel):
    results: Optional[List[Dict[str, Any]]] = None
    session_id: Optional[str] = None
    metadata: Dict[str, Any] = {}

class UserResponse(BaseModel):
    uid: str
    email: Optional[str] = None
    display_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    picture: Optional[str] = None
    created_at: Optional[float] = None

@app.get("/users/me", response_model=UserResponse)
async def read_users_me(current_user: Dict[str, Any] = Depends(get_current_user)):
    uid = current_user.get("uid")
    if not uid:
        raise HTTPException(status_code=400, detail="User ID not found in token")
    
    # Check if user exists in store
    key = UserKey(uid=uid)
    user = user_store.get(key)
    
    # Extract latest info from token
    email = current_user.get("email")
    display_name = current_user.get("name")
    picture = current_user.get("picture")
    
    # Split name logic
    first_name = None
    last_name = None
    if display_name:
        parts = display_name.strip().split(" ", 1)
        first_name = parts[0]
        if len(parts) > 1:
            last_name = parts[1]

    if not user:
        # Create new user
        user = User(
            uid=uid,
            email=email,
            display_name=display_name,
            first_name=first_name,
            last_name=last_name,
            picture=picture,
            created_at=time.time()
        )
        user_store.put(key, user)
    else:
        # Update existing user metadata if changed
        # This keeps the profile fresh if they update it in Google/Firebase
        updated = False
        if user.display_name != display_name:
            user.display_name = display_name
            # Re-split name if display name changed
            user.first_name = first_name
            user.last_name = last_name
            updated = True
        
        if user.picture != picture:
            user.picture = picture
            updated = True
            
        if user.email != email:
            user.email = email
            updated = True
            
        if updated:
            user_store.put(key, user)
    
    return UserResponse(
        uid=user.uid,
        email=user.email,
        display_name=user.display_name,
        first_name=user.first_name,
        last_name=user.last_name,
        picture=user.picture,
        created_at=user.created_at
    )

@app.get("/init_upload")
@app.post("/init_upload")
async def init_upload(current_user: Dict[str, Any] = Depends(get_current_user), library: Optional[str] = "My Library"):
    """ 
    First find the library or create a new one
    """
    libraries = user_library_store.get_range(LibraryKey(current_user["uid"], ""), LibraryKey(current_user["uid"], "\uffff"))
    library_id = None
    for lib in libraries:
        if lib[1].name == library:
            library_id = lib[0].library_id
            break
    if not library_id:
        id = ""
        if library == "My Library":
            id = "my_library"
        else:
            id = str(uuid.uuid4())
        key = LibraryKey(current_user["uid"], id)
        user_library_store.put(key, Library(key=key, name=library, created_at=time.time()))
        library_id = id

    """
    Initializes a new upload session.
    """
    session_id = str(uuid.uuid4())
    session_store.create_session(session_id, {"library_id": library_id}, ttl_seconds=3600)
    return {"status": "success", "session_id": session_id, "library_id": library_id}

@app.post("/upload_frame")
async def upload_frame(
    file: UploadFile = File(...), 
    session_id: Optional[str] = Form(None), 
    frame_id: Optional[int] = Form(None),
    library_id: Optional[str] = Form(None),
    shelf: Optional[str] = Form(None),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Receives an image file, extracts books, enriches them, and returns them with a count.
    Saves results to session using books_<frame_id> if session_id is provided.
    Saves upload entry to MongoDB.
    """
    user_id = current_user["uid"]

    session = session_store.get_session(session_id)
    if session:
        library_id = session["library_id"]

    # Read image bytes
    content = await file.read()
    
    # 0. Handle frame_id fallback
    if frame_id is None:
        frame_id = time.time_ns() // 1_000_000
    
    # 0.5 Save the frame image
    image_path = image_storage.save_image(user_id, f"{session_id}_{frame_id}", content)
    
    # 1. Process the image using Gemini to extract raw books
    result = await process_image_bytes(image_bytes=content, vertexai=True, project=project_id)
    raw_books = result.get("books", [])

    # 2. Immediately enrich and deduplicate (counting mode)
    if raw_books:
        enricher = BookEnricher()
        enriched_books, stats = await enricher.batch_enrich(raw_books, dedupe_mode="counting")
        
        # 2.5 Save to raw library document store
        entry_key = FrameUploadKey(user_id, session_id, frame_id)
        entry = FrameUploadEntry(entry_key, shelf, library_id, enriched_books)
        user_uploads_store.put(entry_key, entry)
        
        # 2.6 Save shelf frame metadata
        meta_key = ShelfFrameMetadataKey(user_id, library_id, frame_id)
        metadata = ShelfFrameMetadata(
            key=meta_key,
            shelf=shelf,
            book_count=len(enriched_books),
            uploaded_at=time.time()
        )
        user_shelf_frame_metadata_store.put(meta_key, metadata)

        response_data = {
            "status": "success",
            "books": enriched_books,
            "enrichment_stats": stats,
            "usage": result.get("usage"),
            "image_path": image_path,
            "frame_id": frame_id,
            "shelf": shelf
        }
        
        # 3. Save to session if session_id is provided
        if session:
            print(f"Saving to session: {session_id}, frame_id: {frame_id}")
            session_store.put(session_id, f"books_{frame_id}", response_data)
            response_data["session_id"] = session_id
            
        return response_data
    
    return result

@app.post("/complete_upload")
async def complete_upload(
    request: CompleteUploadRequest,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Receives a list of JSON results from upload_frame.
    Deduplicates books by proximity (already enriched).
    Saves aggregated library to MongoDB.
    """
    results = request.results
    user_id = current_user["uid"]

    # If results are not provided, try to fetch from session
    if not results and request.session_id:
        full_session = session_store.get_session(request.session_id)
        if full_session:        
            library_id = full_session.get("library_id")
            
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
        
        # Reconstruct library from uploads table
        else:
            frames = user_uploads_store.get_range(
                FrameUploadKey(user_id, request.session_id, 0),
                FrameUploadKey(user_id, request.session_id, time.time_ns() // 1_000_000)
            )
            results = []
            if frames and len(frames) > 0:
                library_id = frames[0][1].library_id
                for frame in frames:
                    f = frame[1]
                    f.frame_id = frame[0].frame_id
                    results.append(to_dict(f))

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

    frame_to_shelf = {res.get("frame_id"): res.get("shelf") for res in results}

    deduped_books = BookDeduplicator.deduplicate_proximity(all_books_to_dedupe)
    for book in deduped_books:
        shelves = list({frame_to_shelf.get(frame_id) for frame_id in book["frame_ids"]})
        book["shelves"] = shelves

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
            shelf = frame_to_shelf.get(fid)
            if shelf:
                shelves_for_book.add(shelf)
        
        if not shelves_for_book:
            unshelved_books.append(book)
        else:
            for shelf in shelves_for_book:
                if shelf not in books_by_shelf:
                    books_by_shelf[shelf] = []
                books_by_shelf[shelf].append(book)

    # For each affected shelf: Delete old content and Insert new
    for shelf, shelf_books in books_by_shelf.items():
        # Delete range for (user_id, library, shelf)
        # We need a way to specify partial sort key for range delete?
        # DocumentStore.delete_range(start, end)
        # start = (user_id, library, shelf, "")
        # end = (user_id, library, shelf, "\uffff")
        
        del_start = LibraryBookKey(user_id, library_id, shelf, "")
        del_end = LibraryBookKey(user_id, library_id, shelf, "\uffff")
        user_books_store.delete_range(del_start, del_end)
        
        # Insert new books
        for book in shelf_books:
            book_id = book.get("isbn")
            if not book_id:
                title = book.get("title", "").lower().strip()
                author = book.get("author", "").lower().strip()
                book_id = f"{title}|{author}"
            
            lib_key = LibraryBookKey(user_id, library_id, shelf, book_id)
            lib_book = LibraryBook(
                key=lib_key,
                title=book.get("title", "Unknown"),
                author=book.get("author"),
                isbn=book.get("isbn"),
                frame_ids=book.get("frame_ids", []),
                copies=book.get("count")
            )
            user_books_store.put(lib_key, lib_book)
            
    if unshelved_books:
        shelf = "Unshelved" 
        
        del_start = LibraryBookKey(user_id, library_id, shelf, "")
        del_end = LibraryBookKey(user_id, library_id, shelf, "\uffff")
        user_books_store.delete_range(del_start, del_end)

        for book in unshelved_books:
            book_id = book.get("isbn")
            if not book_id:
                title = book.get("title", "").lower().strip()
                author = book.get("author", "").lower().strip()
                book_id = f"{title}|{author}"

            lib_key = LibraryBookKey(user_id, library_id, shelf, book_id)
            lib_book = LibraryBook(
                key=lib_key,
                title=book.get("title", "Unknown"),
                author=book.get("author"),
                isbn=book.get("isbn"),
                frame_ids=book.get("frame_ids", []),
                copies=book.get("count")
            )
            user_books_store.put(lib_key, lib_book)

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
    enriched, diagnostics = await enricher.enrich_book(book)
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
    enriched, stats = await enricher.batch_enrich(books)
    return {
        "books": enriched,
        "batch_stats": stats
    }


@app.get("/user_libraries")
async def get_user_libraries(current_user: Dict[str, Any] = Depends(get_current_user)):
    """
    Returns the user's libraries organized by library and shelf.
    """
    user_id = current_user["uid"]
    # 1. Fetch all shelf metadata for user
    lib_rows = user_library_store.get_range(LibraryKey(user_id, ""), LibraryKey(user_id, "\uffff"))
    
    # 2. Group books by library_id
    libraries = {}
    for _, lib in lib_rows:
        libraries[lib.key.library_id] = {
            "name": lib.name,
        }
    return libraries

@app.get("/user_library")
async def get_user_library(current_user: Dict[str, Any] = Depends(get_current_user), library_id: Optional[str] = None):
    """
    Returns the user's libraries organized by library and shelf.
    """
    user_id = current_user["uid"]
    # 1. Fetch all shelf metadata for user
    from_library = library_id if library_id else ""
    to_library = library_id if library_id else "\uffff"
    b_start = LibraryBookKey(user_id, from_library, "", "")
    b_end = LibraryBookKey(user_id, to_library, "\uffff", "\uffff")
    
    book_rows = user_books_store.get_range(b_start, b_end)
    
    shelves: Dict[str, List[LibraryBook]] = {}
    unshelved: List[LibraryBook] = []
    
    for id, book in book_rows:
        s_name = id.library_id + ":" + id.shelf
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
