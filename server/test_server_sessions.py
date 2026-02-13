import requests
import json
import time
import io
from PIL import Image

BASE_URL = "http://localhost:8000"

def create_test_image():
    file = io.BytesIO()
    image = Image.new('RGB', (100, 100), color=(73, 109, 137))
    image.save(file, 'PNG')
    file.seek(0)
    return file

def test_session_flow():
    print("Testing session-based upload flow...")
    
    # 1. Init Session
    print("\n1. Initializing session...")
    resp = requests.get(f"{BASE_URL}/init_upload")
    assert resp.status_code == 200
    session_id = resp.json()["session_id"]
    print(f"   Session ID: {session_id}")
    
    # 2. Upload frame with session_id and frame_id
    print("\n2. Uploading frame with session_id and frame_id...")
    img_file = create_test_image()
    files = {"file": ("test.png", img_file, "image/png")}
    data = {"session_id": session_id, "frame_id": 1, "user_id": "test_user"}
    resp = requests.post(f"{BASE_URL}/upload_frame", files=files, data=data)
    print(f"   Status: {resp.status_code}")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert "session_id" in resp_json
    assert "frame_id" in resp_json
    print(f"   Books found: {len(resp_json.get('books', []))}")

    # Upload another frame
    print("\n2b. Uploading second frame...")
    img_file2 = create_test_image()
    files2 = {"file": ("test2.png", img_file2, "image/png")}
    data2 = {"session_id": session_id, "frame_id": 2, "user_id": "test_user"}
    resp2 = requests.post(f"{BASE_URL}/upload_frame", files=files2, data=data2)
    assert resp2.status_code == 200
    print(f"   Frame 2 status: {resp2.status_code}")

    # 3. Complete Upload using session_id
    print("\n3. Completing upload using session_id...")
    # Results is None, so it should fetch from session
    payload = {
        "session_id": session_id,
        "enrich": False
    }
    resp = requests.post(f"{BASE_URL}/complete_upload", json=payload)
    print(f"   Status: {resp.status_code}")
    assert resp.status_code == 200
    final_json = resp.json()
    print(f"   Total books in final result: {final_json.get('total_books_found')}")
    assert "total_books_found" in final_json

    # 4. Verify session is deleted
    print("\n4. Verifying session is deleted...")
    # Trying to complete again with the same session_id should fail
    resp = requests.post(f"{BASE_URL}/complete_upload", json=payload)
    print(f"   Second attempt status: {resp.status_code}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "error"
    assert resp.json()["message"] == "Session not found"
    print("   Session deletion confirmed!")

    # 5. Cleanup
    print("\n5. Verification successful!")

if __name__ == "__main__":
    try:
        test_session_flow()
    except Exception as e:
        print(f"Test failed: {e}")
        import sys
        sys.exit(1)
