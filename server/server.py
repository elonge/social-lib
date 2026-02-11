from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from book_extractor import process_image_bytes
from book_enricher import BookEnricher

app = FastAPI(title="Book spine extractor API")

class CompleteUploadRequest(BaseModel):
    results: List[Dict[str, Any]]
    enrich: Optional[bool] = False
    metadata: Dict[str, Any] = {}

@app.post("/upload_next_frame")
async def upload_next_frame(file: UploadFile = File(...)):
    """
    Receives an image file and returns a JSON response.
    """
    # Read image bytes
    content = await file.read()
    
    # Process the image using Gemini
    result = process_image_bytes(image_bytes=content)
    
    return result

@app.post("/complete_upload")
async def complete_upload(request: CompleteUploadRequest):
    """
    Receives a list of JSON results from upload_next_frame.
    Deduplicates books by title, picking the one with most metadata/text.
    """
    best_books = {}  # title -> best_book_entry
    
    for result in request.results:
        books = result.get("books", [])
        for book in books:
            title = book.get("title")
            if not title:
                continue
            
            # Metadata fields to check
            meta_fields = ["author", "publisher", "year", "other_text"]
            
            # Calculate metadata count (non-None, non-empty)
            meta_count = sum(1 for f in meta_fields if book.get(f))
            
            # Calculate total text length (including title)
            total_text = sum(len(str(v)) for v in book.values() if v)
            
            if title not in best_books:
                best_books[title] = {
                    "entry": book,
                    "meta_count": meta_count,
                    "total_text": total_text
                }
            else:
                current_best = best_books[title]
                # Compare metadata count
                if meta_count > current_best["meta_count"]:
                    better = True
                elif meta_count == current_best["meta_count"]:
                    # Tie-breaker: total text length
                    better = total_text > current_best["total_text"]
                else:
                    better = False
                
                if better:
                    best_books[title] = {
                        "entry": book,
                        "meta_count": meta_count,
                        "total_text": total_text
                    }
    
    final_books = [item["entry"] for item in best_books.values()]
    
    # Optional enrichment in batches
    if request.enrich:
        batch_size = 10
        print(f"Enriching {len(final_books)} books in batches of {batch_size}...")
        enricher = BookEnricher()
        
        enriched_results = []
        total_stats = {
            "google_books_hits": 0,
            "open_library_hits": 0,
            "gemini_calls": 0,
            "total_duration_seconds": 0.0
        }
        
        # Process in chunks
        for i in range(0, len(final_books), batch_size):
            chunk = final_books[i:i + batch_size]
            print(f"  Processing batch {i//batch_size + 1} ({len(chunk)} books)...")
            enriched_chunk, batch_stats = enricher.batch_enrich(chunk)
            enriched_results.extend(enriched_chunk)
            
            # Aggregate stats
            total_stats["google_books_hits"] += batch_stats["google_books_hits"]
            total_stats["open_library_hits"] += batch_stats["open_library_hits"]
            total_stats["gemini_calls"] += batch_stats["gemini_calls"]
            total_stats["total_duration_seconds"] += batch_stats["total_duration_seconds"]
            
        final_books = enriched_results
        
        return {
            "status": "success",
            "total_books_found": len(final_books),
            "books": final_books,
            "enrichment_stats": total_stats
        }
    
    return {
        "status": "success",
        "total_books_found": len(final_books),
        "books": final_books
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
