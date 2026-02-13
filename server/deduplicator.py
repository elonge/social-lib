from typing import List, Dict, Any, Optional

class BookDeduplicator:
    """Handles deduplication of book metadata."""

    @staticmethod
    def deduplicate_proximity(books: List[Dict[str, Any]], window_size: int = 20) -> List[Dict[str, Any]]:
        """
        Removes books that appear twice in close proximity based on ISBN and count.
        Aggregates frame_ids for duplicates.
        """
        if not books:
            return []
            
        deduplicated = []
        # isbn_count_key -> index in deduplicated list
        recent_entries = {}
        
        for book in books:
            isbn = book.get("isbn")
            count = book.get("count", 1)
            frame_id = book.get("frame_id")
            
            key = (isbn, count) if isbn else None
            is_dupe = False
            
            if key and key in recent_entries:
                idx = recent_entries[key]
                is_dupe = True
                # Aggregate frame_id
                if frame_id:
                    if "frame_ids" not in deduplicated[idx]:
                        deduplicated[idx]["frame_ids"] = []
                    if frame_id not in deduplicated[idx]["frame_ids"]:
                        deduplicated[idx]["frame_ids"].append(frame_id)
            
            if not is_dupe:
                book_copy = book.copy()
                if frame_id:
                    book_copy["frame_ids"] = [frame_id]
                deduplicated.append(book_copy)
                if key:
                    recent_entries[key] = len(deduplicated) - 1
                    if len(recent_entries) > window_size:
                        # Remove oldest from map
                        oldest_key = list(recent_entries.keys())[0]
                        del recent_entries[oldest_key]
                        
        return deduplicated

    @staticmethod
    def deduplicate_counting(books: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Groups books by ISBN (or title+author) and adds a 'count' field.
        Used for single frame deduplication.
        """
        if not books:
            return []
            
        unique_books = {} 
        
        for book in books:
            isbn = book.get("isbn")
            if isbn:
                key = f"isbn:{isbn}"
            else:
                title = book.get("title", "").lower().strip()
                author = book.get("author", "").lower().strip()
                key = f"ta:{title}|{author}"
            
            if key in unique_books:
                unique_books[key]["count"] = unique_books[key].get("count", 1) + 1
            else:
                book_copy = book.copy()
                book_copy["count"] = 1
                unique_books[key] = book_copy
                
        return list(unique_books.values())

    @staticmethod
    def deduplicate_richness(books: List[Dict[str, Any]], window_size: int = 5) -> List[Dict[str, Any]]:
        """
        Groups books that appear twice in close proximity based on TITLE.
        Significant for extraction stage where ISBN is missing.
        Picks the candidate with the most metadata fields (highest richness).
        If multiple have same richness, adds a 'count' field.
        """
        if not books:
            return []

        def get_richness(book: Dict[str, Any]) -> int:
            # Count non-empty values
            return sum(1 for v in book.values() if v and str(v).strip() and str(v).lower() != "null")

        def get_key(book: Dict[str, Any]) -> Optional[str]:
            title = book.get("title")
            if not title or str(title).strip().lower() == "null":
                return None
            return str(title).lower().strip()

        deduplicated = []
        # Store (key, book_index_in_deduplicated) for recent books
        recent_keys = {} # key -> index

        for book in books:
            key = get_key(book)
            if not key:
                continue # Ignore books without a title
            
            # Check if we saw this book recently
            if key in recent_keys:
                idx = recent_keys[key]
                existing_book = deduplicated[idx]
                
                # Compare richness
                if get_richness(book) > get_richness(existing_book):
                    # Current book is better, swap but keep the count if it was aggregated
                    new_count = existing_book.get("count", 1) + book.get("count", 1)
                    book_copy = book.copy()
                    book_copy["count"] = new_count
                    deduplicated[idx] = book_copy
                else:
                    # Existing book is better or equal richness, just increment count
                    existing_book["count"] = existing_book.get("count", 1) + book.get("count", 1)
            else:
                # New book
                book_copy = book.copy()
                if "count" not in book_copy:
                    book_copy["count"] = 1
                
                # Add to deduplicated and track it
                deduplicated.append(book_copy)
                curr_idx = len(deduplicated) - 1
                recent_keys[key] = curr_idx
                
                # Maintain window: remove keys that are too far back in deduplicated list
                if len(recent_keys) > window_size:
                    # Remove the oldest key (the one with the smallest index)
                    oldest_key = min(recent_keys, key=lambda k: recent_keys[k])
                    if recent_keys[oldest_key] < curr_idx - window_size:
                        del recent_keys[oldest_key]

        return deduplicated
