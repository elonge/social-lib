import unittest
import os
import shutil
from image_storage import FileShelfImageStorage, get_image_storage

class TestImageStorage(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_shelf_images"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        self.storage = FileShelfImageStorage(base_path=self.test_dir)

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_file_storage_save(self):
        user_id = "user123"
        frame_id = "frame456"
        image_bytes = b"fake-image-data"
        
        path = self.storage.save_image(user_id, frame_id, image_bytes)
        
        self.assertTrue(os.path.exists(path))
        self.assertIn(user_id, path)
        self.assertIn(frame_id, path)
        
        with open(path, "rb") as f:
            self.assertEqual(f.read(), image_bytes)

    def test_factory(self):
        storage = get_image_storage("file", base_path=self.test_dir)
        self.assertIsInstance(storage, FileShelfImageStorage)

if __name__ == "__main__":
    unittest.main()
