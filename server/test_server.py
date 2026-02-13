import requests
import json
import os
from PIL import Image
import io

# Constants
BASE_URL = "http://127.0.0.1:8000"

def test_upload_next_frame():
    print("Testing /upload_next_frame with enrichment/counting...")
    # Create a dummy image
    img = Image.new('RGB', (100, 100), color = 'red')
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='JPEG')
    img_byte_arr = img_byte_arr.getvalue()
    
    files = {'file': ('test.jpg', img_byte_arr, 'image/jpeg')}
    response = requests.post(f"{BASE_URL}/upload_next_frame", files=files)
    
    print(f"Status Code: {response.status_code}")
    res_json = response.json()
    print(f"Response: {res_json}")
    
    assert response.status_code == 200
    assert res_json["status"] == "success"
    # Even if no books found, structure should be there or handled
    if "books" in res_json:
        print(f"Found {len(res_json['books'])} enriched books")
        assert "enrichment_stats" in res_json

def test_complete_upload_proximity():
    print("\nTesting /complete_upload with proximity deduplication...")
    results = [
        {
            "books": [
                {"title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "isbn": "123"},
                {"title": "1984", "author": "George Orwell", "isbn": "456"}
            ]
        },
        {
            "books": [
                {"title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "isbn": "123"}, # Dupe
                {"title": "Animal Farm", "author": "George Orwell", "isbn": "789"}
            ]
        }
    ]
    
    data = {"results": results}
    response = requests.post(f"{BASE_URL}/complete_upload", json=data)
    
    print(f"Status Code: {response.status_code}")
    res_json = response.json()
    print(f"Response: {res_json}")
    
    assert response.status_code == 200
    books = res_json["books"]
    stats = res_json["deduplication_stats"]
    
    print(f"Total Books after dedupe: {len(books)}")
    print(f"Stats: {stats}")
    
    assert len(books) == 3
    assert stats["proximity_deduped"] == 1
    print("Proximity deduplication verified!")

def test_enrich_book():
    print("\nTesting /enrich_book with diagnostics...")
    book = {"title": "The Great Gatsby", "author": "Fitzgerald"}
    response = requests.post(f"{BASE_URL}/enrich_book", json=book)
    
    print(f"Status Code: {response.status_code}")
    res_json = response.json()
    print(f"Response: {res_json}")
    
    assert response.status_code == 200
    assert res_json["book"].get("title") == "The Great Gatsby"
    assert "diagnostics" in res_json
    print("Enrich_book endpoint with diagnostics verified!")

def test_enrich_books():
    print("\nTesting /enrich_books (batch parallel with diagnostics)...")
    books = [
        {"title": "The Great Gatsby", "author": "Fitzgerald"},
        {"title": "1984", "author": "Orwell"}
    ]
    response = requests.post(f"{BASE_URL}/enrich_books", json=books)
    
    print(f"Status Code: {response.status_code}")
    res_json = response.json()
    print(f"Response count: {len(res_json['books'])}")
    print(f"Batch stats: {res_json['batch_stats']}")
    
    assert response.status_code == 200
    assert len(res_json["books"]) == 2
    assert "batch_stats" in res_json
    print("Enrich_books (batch) endpoint with diagnostics verified!")

def test_complete_upload_chunked():
    print("\nTesting /complete_upload with proximity deduplication stats...")
    # Generate 12 dummy books
    books = [{"title": f"Book {i}", "author": f"Author {i}"} for i in range(1, 13)]
    results = [{"books": books}]
    
    data = {"results": results}
    response = requests.post(f"{BASE_URL}/complete_upload", json=data)
    
    print(f"Status Code: {response.status_code}")
    res_json = response.json()
    print(f"Response count: {len(res_json['books'])}")
    print(f"Deduplication stats: {res_json.get('deduplication_stats')}")
    
    assert response.status_code == 200
    assert len(res_json["books"]) == 12
    assert "deduplication_stats" in res_json
    print("Complete upload logic with deduplication stats verified!")

if __name__ == "__main__":
    try:
        # test_upload_next_frame() # This requires Gemini key and real image processing, might be slow/expensive
        test_complete_upload_proximity()
        test_enrich_book()
        test_enrich_books()
        test_complete_upload_chunked()
        print("\nAll tests passed!")
    except Exception as e:
        print(f"\nTest failed: {e}")
        exit(1)
