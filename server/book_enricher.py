import os
import json
import time
import httpx
import asyncio
from typing import List, Dict, Any, Optional
from google import genai
from deduplicator import BookDeduplicator

class BookEnricher:
    """Enrich book metadata using external APIs and Gemini."""
    
    def __init__(self, api_key: str = None, model_name: str = "gemini-3-flash-preview"):
        self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        self.client = genai.Client(api_key=self.api_key) if self.api_key else genai.Client(vertexai=True, project=self.project_id)
        self.model_name = model_name

    async def enrich_book(self, book_data: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Enrich a book's metadata using Google Books, Open Library, and Gemini.
        Returns (enriched_data, diagnostics).
        """
        start_time = time.perf_counter()
        title = book_data.get("title")
        author = book_data.get("author")
        
        diagnostics = {
            "google_books_success": False,
            "open_library_success": False,
            "gemini_correction_used": False,
            "duration_seconds": 0.0
        }

        if not title:
            diagnostics["duration_seconds"] = time.perf_counter() - start_time
            return book_data, diagnostics

        # 1. Try Open Library first, then Google Books
        ol_res = await self._fetch_open_library_raw(title, author)
        gb_res = await self._fetch_google_books_raw(title, author)
        
        diagnostics["google_books_success"] = gb_res is not None
        if not gb_res:
            print(f"[Miss] Google Books did not find '{title}'")
            
        diagnostics["open_library_success"] = ol_res is not None
        if not ol_res:
             print(f"[Miss] Open Library did not find '{title}'")
        
        external_data = self._combine_raw_data(gb_res, ol_res)
        
        # 2. If not found, try Gemini for fuzzy correction
        if not external_data:
            diagnostics["gemini_correction_used"] = True
            corrected = await self._gemini_fuzzy_correction(book_data)
            if corrected:
                title = corrected.get("title", title)
                author = corrected.get("author", author)
                
                ol_res = await self._fetch_open_library_raw(title, author)
                gb_res = await self._fetch_google_books_raw(title, author)
                
                diagnostics["google_books_success"] = diagnostics["google_books_success"] or (gb_res is not None)
                diagnostics["open_library_success"] = diagnostics["open_library_success"] or (ol_res is not None)
                
                external_data = self._combine_raw_data(gb_res, ol_res)
                if not external_data:
                    external_data = corrected

        # 3. Combine results
        enriched = book_data.copy()
        if external_data:
            for key, value in external_data.items():
                if not enriched.get(key) or (key == "title" and value):
                    enriched[key] = value
                    
        diagnostics["duration_seconds"] = time.perf_counter() - start_time
        return enriched, diagnostics

    async def batch_enrich(self, books: List[Dict[str, Any]], max_workers: int = 5, dedupe_mode: Optional[str] = "counting", dedupe_window: int = 20) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Enrich multiple books in parallel, deduplicate results, and return results plus aggregated diagnostics.
        dedupe_mode: "proximity", "counting", or None
        """
        start_time = time.perf_counter()
        
        # Filter out books that do not have a title
        books = [b for b in books if b.get("title", "").strip()]
        
        if not books:
             return [], {
                "total_books": 0,
                "google_books_hits": 0,
                "open_library_hits": 0,
                "gemini_calls": 0,
                "total_duration_seconds": 0.0,
                "average_book_duration": 0.0,
                "deduplicated_count": 0
            }

        results = await asyncio.gather(*(self.enrich_book(book) for book in books))
            
        enriched_books = [r[0] for r in results]
        individual_diagnostics = [r[1] for r in results]
        
        # Deduplicate enriched books
        dedupe_count = 0
        if dedupe_mode == "proximity":
            deduplicated_books = BookDeduplicator.deduplicate_proximity(enriched_books, window_size=dedupe_window)
            dedupe_count = len(enriched_books) - len(deduplicated_books)
        elif dedupe_mode == "counting":
            deduplicated_books = BookDeduplicator.deduplicate_counting(enriched_books)
            dedupe_count = len(enriched_books) - len(deduplicated_books)
        else:
            deduplicated_books = enriched_books
        
        total_duration = time.perf_counter() - start_time
        
        # Aggregate stats
        aggregated = {
            "total_books": len(books),
            "google_books_hits": sum(1 for d in individual_diagnostics if d["google_books_success"]),
            "open_library_hits": sum(1 for d in individual_diagnostics if d["open_library_success"]),
            "gemini_calls": sum(1 for d in individual_diagnostics if d["gemini_correction_used"]),
            "total_duration_seconds": total_duration,
            "average_book_duration": sum(d["duration_seconds"] for d in individual_diagnostics) / len(books) if books else 0,
            "deduplicated_count": dedupe_count
        }
        
        print(f"\n--- Batch Enrichment Diagnostics ({dedupe_mode or 'no dedupe'}) ---")
        print(f"Total Books: {aggregated['total_books']}")
        print(f"Google Books Success: {aggregated['google_books_hits']}/{aggregated['total_books']}")
        print(f"Open Library Success: {aggregated['open_library_hits']}/{aggregated['total_books']}")
        print(f"Gemini Corrections: {aggregated['gemini_calls']}")
        print(f"Books Deduplicated: {aggregated['deduplicated_count']}")
        print(f"Total Elapsed Time: {aggregated['total_duration_seconds']:.2f}s")
        print(f"Average Book Time: {aggregated['average_book_duration']:.2f}s")
        print(f"-------------------------------------\n")
        
        return deduplicated_books, aggregated

    async def _fetch_external_data(self, title: str, author: Optional[str]) -> Optional[Dict[str, Any]]:
        """DEPRECATED: Use raw fetch and combine for diagnostics."""
        gb_data = await self._fetch_google_books(title, author)
        ol_data = await self._fetch_open_library(title, author)
        return self._combine_raw_data(gb_data, ol_data)

    def _combine_raw_data(self, gb_data: Optional[Dict[str, Any]], ol_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not gb_data and not ol_data:
            return None
        combined = {}
        source_data = ol_data or gb_data
        combined.update(source_data)
        if gb_data and ol_data:
            combined["publisher"] = ol_data.get("publisher") or gb_data.get("publisher")
            combined["year"] = ol_data.get("year") or gb_data.get("year")
            combined["author"] = ol_data.get("author") or gb_data.get("author")
            combined["language"] = ol_data.get("language") or gb_data.get("language")
            combined["isbn"] = ol_data.get("isbn") or gb_data.get("isbn")
            combined["cover_link"] = ol_data.get("cover_link") or gb_data.get("cover_link")
        if combined.get("cover_link"):
            combined["cover_link"] = self._normalize_cover_link(combined.get("cover_link"))
        return combined

    async def _fetch_google_books_raw(self, title: str, author: Optional[str]) -> Optional[Dict[str, Any]]:
        return await self._fetch_google_books(title, author)

    async def _fetch_open_library_raw(self, title: str, author: Optional[str]) -> Optional[Dict[str, Any]]:
        return await self._fetch_open_library(title, author)

    async def _fetch_google_books(self, title: str, author: Optional[str]) -> Optional[Dict[str, Any]]:
        """Fetch metadata from Google Books API."""
        start = time.perf_counter()
        query = f"intitle:{title}"
        if author:
            query += f"+inauthor:{author}"
            
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=10)
            duration = time.perf_counter() - start
            if duration > 2.0:
                print(f"[Slow API] Google Books for '{title}': {duration:.2f}s")
                
            if response.status_code == 200:
                data = response.json()
                if "items" in data:
                    item = data["items"][0]
                    volume_info = item["volumeInfo"]
                    
                    # Extract ISBN
                    isbn = None
                    for identifier in volume_info.get("industryIdentifiers", []):
                        if identifier.get("type") in ["ISBN_13", "ISBN_10"]:
                            isbn = identifier.get("identifier")
                            if identifier.get("type") == "ISBN_13":
                                break  # Prefer ISBN_13
                                
                    return {
                        "title": volume_info.get("title"),
                        "author": ", ".join(volume_info.get("authors", [])),
                        "publisher": volume_info.get("publisher"),
                        "year": volume_info.get("publishedDate", "")[:4],
                        "language": volume_info.get("language"),
                        "description": volume_info.get("description"),
                        "isbn": isbn,
                        "cover_link": self._normalize_cover_link(volume_info.get("imageLinks", {}).get("thumbnail"))
                    }
        except Exception as e:
            print(f"Error fetching from Google Books: {e}")
            if time.perf_counter() - start > 2.0:
                 print(f"[Slow API] Google Books ERROR for '{title}': {time.perf_counter() - start:.2f}s")
        return None

    async def _fetch_open_library(self, title: str, author: Optional[str]) -> Optional[Dict[str, Any]]:
        """Fetch metadata from Open Library API."""
        start = time.perf_counter()
        query = f"title={title}"
        if author:
            query += f"&author={author}"
            
        url = f"https://openlibrary.org/search.json?{query}&limit=1"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=10)
            duration = time.perf_counter() - start
            if duration > 2.0:
                 print(f"[Slow API] Open Library for '{title}': {duration:.2f}s")
            
            if response.status_code == 200:
                data = response.json()
                if data.get("docs"):
                    doc = data["docs"][0]
                    
                    # Extract ISBN (Open Library usually returns a list)
                    isbns = doc.get("isbn", [])
                    isbn = isbns[0] if isbns else None
                    
                    return {
                        "title": doc.get("title"),
                        "author": ", ".join(doc.get("author_name", [])),
                        "publisher": ", ".join(doc.get("publisher", [])[:1]),
                        "year": str(doc.get("first_publish_year", "")),
                        "language": ", ".join(doc.get("language", [])[:1]),
                        "isbn": isbn,
                        "cover_link": f"https://covers.openlibrary.org/b/id/{doc.get('cover_i')}-L.jpg" if doc.get("cover_i") else None
                    }
        except Exception as e:
            print(f"Error fetching from Open Library: {e}")
            if time.perf_counter() - start > 2.0:
                 print(f"[Slow API] Open Library ERROR for '{title}': {time.perf_counter() - start:.2f}s")
        return None

    async def _gemini_fuzzy_correction(self, book_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Use Gemini to correct book metadata fuzzy-style."""
        start = time.perf_counter()
        if not self.client:
            return None
            
        prompt = f"""Given the following potentially noisy or partial book metadata, identify the most likely actual book.
Return the corrected information as JSON.

Data:
Title: {book_data.get('title')}
Author: {book_data.get('author')}
Publisher: {book_data.get('publisher')}
Year: {book_data.get('year')}

Rules:
1. If you are very sure you know the book, return corrected fields.
2. If you are not sure, return the original fields.
3. Return only valid JSON.
4. Fields: "title", "author", "publisher", "year", "language"

JSON Format:
{{
  "title": "...",
  "author": "...",
  "publisher": "...",
  "year": "...",
  "language": "..."
}}
"""
        try:
            response = await self.client.aio.models.generate_content(
                model=self.model_name,
                contents=[prompt],
                config={'temperature': 0.1, 'thinking_level': 'LOW'}
            )
            duration = time.perf_counter() - start
            if duration > 2.0:
                 print(f"[Slow API] Gemini Correction for '{book_data.get('title')}': {duration:.2f}s")

            text = response.text.strip()
            # Basic JSON cleanup
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].strip()
                
            return json.loads(text)
        except Exception as e:
            print(f"Error in Gemini fuzzy correction: {e}")
            if time.perf_counter() - start > 2.0:
                 print(f"[Slow API] Gemini Correction ERROR for '{book_data.get('title')}': {time.perf_counter() - start:.2f}s")
        return None

    def _normalize_cover_link(self, link: Optional[str]) -> Optional[str]:
        if not link:
            return None
        link = link.strip()
        if link.startswith("http://"):
            return "https://" + link[len("http://"):]
        return link
