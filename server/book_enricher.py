import os
import json
import time
import requests
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from google import genai

class BookEnricher:
    """Enrich book metadata using external APIs and Gemini."""
    
    def __init__(self, api_key: str = None, model_name: str = "gemini-3-flash-preview"):
        self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")
        self.client = genai.Client(api_key=self.api_key) if self.api_key else None
        self.model_name = model_name

    def enrich_book(self, book_data: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Enrich a book's metadata using Google Books, Open Library, and Gemini.
        Returns (enriched_data, diagnostics).
        """
        start_time = time.time()
        title = book_data.get("title")
        author = book_data.get("author")
        
        diagnostics = {
            "google_books_success": False,
            "open_library_success": False,
            "gemini_correction_used": False,
            "duration_seconds": 0.0
        }

        if not title:
            diagnostics["duration_seconds"] = time.time() - start_time
            return book_data, diagnostics

        # 1. Try Google Books and Open Library
        gb_res = self._fetch_google_books_raw(title, author)
        ol_res = self._fetch_open_library_raw(title, author)
        
        diagnostics["google_books_success"] = gb_res is not None
        diagnostics["open_library_success"] = ol_res is not None
        
        external_data = self._combine_raw_data(gb_res, ol_res)
        
        # 2. If not found, try Gemini for fuzzy correction
        if not external_data:
            diagnostics["gemini_correction_used"] = True
            corrected = self._gemini_fuzzy_correction(book_data)
            if corrected:
                title = corrected.get("title", title)
                author = corrected.get("author", author)
                
                gb_res = self._fetch_google_books_raw(title, author)
                ol_res = self._fetch_open_library_raw(title, author)
                
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
                    
        diagnostics["duration_seconds"] = time.time() - start_time
        return enriched, diagnostics

    def batch_enrich(self, books: List[Dict[str, Any]], max_workers: int = 5) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Enrich multiple books in parallel and return results plus aggregated diagnostics.
        """
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # zip results to separate books and diagnostics
            results = list(executor.map(self.enrich_book, books))
            
        enriched_books = [r[0] for r in results]
        individual_diagnostics = [r[1] for r in results]
        
        total_duration = time.time() - start_time
        
        # Aggregate stats
        aggregated = {
            "total_books": len(books),
            "google_books_hits": sum(1 for d in individual_diagnostics if d["google_books_success"]),
            "open_library_hits": sum(1 for d in individual_diagnostics if d["open_library_success"]),
            "gemini_calls": sum(1 for d in individual_diagnostics if d["gemini_correction_used"]),
            "total_duration_seconds": total_duration,
            "average_book_duration": sum(d["duration_seconds"] for d in individual_diagnostics) / len(books) if books else 0
        }
        
        print(f"\n--- Batch Enrichment Diagnostics ---")
        print(f"Total Books: {aggregated['total_books']}")
        print(f"Google Books Success: {aggregated['google_books_hits']}/{aggregated['total_books']}")
        print(f"Open Library Success: {aggregated['open_library_hits']}/{aggregated['total_books']}")
        print(f"Gemini Corrections: {aggregated['gemini_calls']}")
        print(f"Total Elapsed Time: {aggregated['total_duration_seconds']:.2f}s")
        print(f"Average Book Time: {aggregated['average_book_duration']:.2f}s")
        print(f"-------------------------------------\n")
        
        return enriched_books, aggregated

    def _fetch_external_data(self, title: str, author: Optional[str]) -> Optional[Dict[str, Any]]:
        """DEPRECATED: Use raw fetch and combine for diagnostics."""
        gb_data = self._fetch_google_books(title, author)
        ol_data = self._fetch_open_library(title, author)
        return self._combine_raw_data(gb_data, ol_data)

    def _combine_raw_data(self, gb_data: Optional[Dict[str, Any]], ol_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not gb_data and not ol_data:
            return None
        combined = {}
        source_data = gb_data or ol_data
        combined.update(source_data)
        if gb_data and ol_data:
            combined["publisher"] = gb_data.get("publisher") or ol_data.get("publisher")
            combined["year"] = gb_data.get("year") or ol_data.get("year")
            combined["author"] = gb_data.get("author") or ol_data.get("author")
            combined["language"] = gb_data.get("language") or ol_data.get("language")
            combined["cover_link"] = gb_data.get("cover_link") or ol_data.get("cover_link")
        return combined

    def _fetch_google_books_raw(self, title: str, author: Optional[str]) -> Optional[Dict[str, Any]]:
        return self._fetch_google_books(title, author)

    def _fetch_open_library_raw(self, title: str, author: Optional[str]) -> Optional[Dict[str, Any]]:
        return self._fetch_open_library(title, author)

    def _fetch_google_books(self, title: str, author: Optional[str]) -> Optional[Dict[str, Any]]:
        """Fetch metadata from Google Books API."""
        query = f"intitle:{title}"
        if author:
            query += f"+inauthor:{author}"
            
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if "items" in data:
                    volume_info = data["items"][0]["volumeInfo"]
                    return {
                        "title": volume_info.get("title"),
                        "author": ", ".join(volume_info.get("authors", [])),
                        "publisher": volume_info.get("publisher"),
                        "year": volume_info.get("publishedDate", "")[:4],
                        "language": volume_info.get("language"),
                        "description": volume_info.get("description"),
                        "cover_link": volume_info.get("imageLinks", {}).get("thumbnail")
                    }
        except Exception as e:
            print(f"Error fetching from Google Books: {e}")
        return None

    def _fetch_open_library(self, title: str, author: Optional[str]) -> Optional[Dict[str, Any]]:
        """Fetch metadata from Open Library API."""
        query = f"title={title}"
        if author:
            query += f"&author={author}"
            
        url = f"https://openlibrary.org/search.json?{query}&limit=1"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("docs"):
                    doc = data["docs"][0]
                    return {
                        "title": doc.get("title"),
                        "author": ", ".join(doc.get("author_name", [])),
                        "publisher": ", ".join(doc.get("publisher", [])[:1]),
                        "year": str(doc.get("first_publish_year", "")),
                        "language": ", ".join(doc.get("language", [])[:1]),
                        "cover_link": f"https://covers.openlibrary.org/b/id/{doc.get('cover_i')}-L.jpg" if doc.get("cover_i") else None
                    }
        except Exception as e:
            print(f"Error fetching from Open Library: {e}")
        return None

    def _gemini_fuzzy_correction(self, book_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Use Gemini to correct book metadata fuzzy-style."""
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
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=[prompt],
                config={'temperature': 0.1}
            )
            text = response.text.strip()
            # Basic JSON cleanup
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].strip()
                
            return json.loads(text)
        except Exception as e:
            print(f"Error in Gemini fuzzy correction: {e}")
        return None
