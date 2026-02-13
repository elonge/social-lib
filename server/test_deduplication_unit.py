import sys
import os

# Add the server directory to the path so we can import BookEnricher
sys.path.append(os.path.join(os.getcwd(), "server"))

from deduplicator import BookDeduplicator

def test_deduplication_proximity():
    print("Testing BookDeduplicator.deduplicate_proximity...")
    
    books = [
        {"title": "Book A", "isbn": "123", "count": 1},
        {"title": "Book B", "isbn": "456", "count": 1},
        {"title": "Book A Duplicate", "isbn": "123", "count": 1}, # Dupe (same ISBN, same count)
        {"title": "Book A Diff Count", "isbn": "123", "count": 2}, # NOT a dupe (same ISBN, diff count)
        {"title": "Book C", "isbn": "789", "count": 1},
    ]
    
    # Test window size 5
    deduplicated = BookDeduplicator.deduplicate_proximity(books, window_size=5)
    print(f"Deduplicated count: {len(deduplicated)} (expected 4)")
    assert len(deduplicated) == 4
    # The entries should be Book A(c=1), Book B(c=1), Book A Diff Count(c=2), Book C(c=1)
    isbns_counts = [(b["isbn"], b.get("count")) for b in deduplicated]
    assert ("123", 1) in isbns_counts
    assert ("456", 1) in isbns_counts
    assert ("123", 2) in isbns_counts
    assert ("789", 1) in isbns_counts
    
    print("Count-aware proximity deduplication verified!")

def test_deduplication_counting():
    print("\nTesting BookDeduplicator.deduplicate_counting...")
    
    books = [
        {"title": "Book A", "isbn": "123"},
        {"title": "Book A", "isbn": "123"}, # Exact same
        {"title": "Book B", "author": "Author B"},
        {"title": "Book B", "author": "Author B"}, # Match by title/author
        {"title": "Book C", "isbn": "789"},
    ]
    
    deduplicated = BookDeduplicator.deduplicate_counting(books)
    print(f"Counting dedupe: {len(deduplicated)} books (expected 3)")
    assert len(deduplicated) == 3
    
    # Check counts
    counts = {b["title"]: b["count"] for b in deduplicated}
    assert counts["Book A"] == 2
    assert counts["Book B"] == 2
    assert counts["Book C"] == 1
    
    print("Counting deduplication logic verified!")

def test_deduplication_richness():
    print("\nTesting BookDeduplicator.deduplicate_richness (title-based)...")
    
    books = [
        {"title": "Gatsby", "author": "Fitzgerald"}, # Richness 2
        {"title": "Gatsby", "author": "Fitzgerald", "publisher": "S&S"}, # Richness 3 (Better)
        {"title": "1984", "author": "Orwell", "year": "1949"}, # Richness 3
        {"title": "1984", "author": "Orwell"}, # Richness 2 (Worse)
        {"title": "Gatsby", "author": "Fitzgerald", "publisher": "S&S", "language": "en"}, # Richness 4 (Best)
        {"title": "Hobbit", "author": "Tolkien"}, # New book
        {"title": "null", "author": "Unknown"}, # Should be ignored
        {"author": "No Title"}, # Should be ignored
    ]
    
    # Test window size 10 (global in this case)
    deduplicated = BookDeduplicator.deduplicate_richness(books, window_size=10)
    print(f"Richness dedupe: {len(deduplicated)} books (expected 3)")
    
    # Check counts and richness
    for b in deduplicated:
        if b["title"] == "Gatsby":
            print(f"  Gatsby count: {b.get('count')} (expected 3)")
            assert b.get("count") == 3
            assert b.get("language") == "en" # Should be the richest one
        elif b["title"] == "1984":
            print(f"  1984 count: {b.get('count')} (expected 2)")
            assert b.get("count") == 2
            assert b.get("year") == "1949" # Should be the richest one
            
    assert len(deduplicated) == 3
    # Verify no title-less books
    assert all(b.get("title") and b.get("title").lower() != "null" for b in deduplicated)
    
    print("Title-based richness deduplication verified!")

if __name__ == "__main__":
    try:
        test_deduplication_proximity()
        test_deduplication_counting()
        test_deduplication_richness()
        print("\nAll unit tests passed!")
    except AssertionError as e:
        print(f"\nTest failed: {e}")
        sys.exit(1)
