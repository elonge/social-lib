import os
from abc import ABC, abstractmethod
from typing import Optional
import time

class ShelfImageStorage(ABC):
    @abstractmethod
    def save_image(self, user_id: str, frame_id: str, image_bytes: bytes) -> str:
        """
        Saves the image and returns a path or URL.
        """
        pass

class FileShelfImageStorage(ShelfImageStorage):
    def __init__(self, base_path: str = "shelf_images"):
        self.base_path = base_path
        if not os.path.exists(base_path):
            os.makedirs(base_path)

    def save_image(self, user_id: str, frame_id: str, image_bytes: bytes) -> str:
        user_dir = os.path.join(self.base_path, user_id)
        if not os.path.exists(user_dir):
            os.makedirs(user_dir)
        
        filename = f"{frame_id}.jpg"
        file_path = os.path.join(user_dir, filename)
        
        with open(file_path, "wb") as f:
            f.write(image_bytes)
            
        return file_path

class GCSShelfImageStorage(ShelfImageStorage):
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        try:
            from google.cloud import storage
            self.client = storage.Client()
            self.bucket = self.client.bucket(bucket_name)
        except ImportError:
            print("Warning: google-cloud-storage not installed. GCSShelfImageStorage will not work.")
            self.client = None
            self.bucket = None

    def save_image(self, user_id: str, frame_id: str, image_bytes: bytes) -> str:
        if not self.bucket:
            raise RuntimeError("GCS Storage not initialized (missing dependencies or bucket)")

        blob_path = f"{user_id}/{frame_id}.jpg"
        blob = self.bucket.blob(blob_path)
        blob.upload_from_string(image_bytes, content_type="image/jpeg")
        
        return f"gs://{self.bucket_name}/{blob_path}"

def get_image_storage(storage_type: str = "file", **kwargs) -> ShelfImageStorage:
    print("Storage type: ", storage_type)
    if storage_type == "file":
        return FileShelfImageStorage(**kwargs)
    elif storage_type == "gcs":
        bucket_name = kwargs.get("bucket_name") or os.environ.get("SHELF_IMAGES_BUCKET") or "user-shelf-images"
        if not bucket_name:
            raise ValueError("bucket_name is required for GCS storage")
        return GCSShelfImageStorage(bucket_name)
    else:
        raise ValueError(f"Unknown image storage type: {storage_type}")
