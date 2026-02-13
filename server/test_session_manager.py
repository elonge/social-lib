import time
import unittest
from session_manager import InMemorySessionStore, RedisSessionStore

class TestSessionManager(unittest.TestCase):
    def test_in_memory_session(self):
        print("Testing InMemorySessionStore...")
        store = InMemorySessionStore()
        session_id = "test-123"
        data = {"user": "alice", "items": [1, 2, 3]}
        
        # Test Create
        store.create_session(session_id, data, ttl_seconds=2)
        self.assertEqual(store.get_session(session_id), data)
        
        # Test Update
        new_data = {"user": "alice", "items": [1, 2, 3, 4]}
        store.update_session(session_id, new_data)
        self.assertEqual(store.get_session(session_id), new_data)
        
        # Test TTL Expiry (Sliding Window)
        time.sleep(1.5)
        # Accessing should refresh the TTL
        self.assertEqual(store.get_session(session_id), new_data)
        
        time.sleep(1.5)
        self.assertEqual(store.get_session(session_id), new_data)
        
        # Now wait longer than TTL
        print("  Waiting for TTL expiry...")
        time.sleep(2.5)
        self.assertIsNone(store.get_session(session_id))
        
        # Test Delete
        store.create_session("to-delete", {"test": 1}, 10)
        self.assertIsNotNone(store.get_session("to-delete"))
        store.delete_session("to-delete")
        self.assertIsNone(store.get_session("to-delete"))

        # Test Granular Put and Get
        store.create_session("granular", {"existing": 1}, 10)
        store.put("granular", "new_item", "new_value")
        self.assertEqual(store.get_session("granular", "new_item"), "new_value")
        self.assertEqual(store.get_session("granular", "existing"), 1)
        
        # Test put_array_item
        store.put_array_item("granular", "my_list", -1, "first")
        self.assertEqual(store.get_session("granular", "my_list"), ["first"])
        store.put_array_item("granular", "my_list", -1, "second")
        self.assertEqual(store.get_session("granular", "my_list"), ["first", "second"])
        store.put_array_item("granular", "my_list", 0, "fixed_first")
        self.assertEqual(store.get_session("granular", "my_list"), ["fixed_first", "second"])
        store.put_array_item("granular", "my_list", 2, "third") # Append via index
        self.assertEqual(store.get_session("granular", "my_list"), ["fixed_first", "second", "third"])

        print("InMemorySessionStore granular methods verified!")

    def test_redis_session(self):
        print("\nChecking for local Redis for testing...")
        try:
            import redis
            client = redis.Redis(host='localhost', port=6379)
            client.ping()
        except Exception as e:
            print(f"  Redis not available locally, skipping RedisSessionStore test. Error: {e}")
            return

        print("Testing RedisSessionStore...")
        store = RedisSessionStore(host='localhost', port=6379)
        session_id = "redis-test"
        data = {"user": "bob", "auth": True}
        
        # Test Create
        store.create_session(session_id, data, ttl_seconds=2)
        self.assertEqual(store.get_session(session_id), data)
        
        # Test Update
        new_data = {"user": "bob", "auth": False}
        store.update_session(session_id, new_data)
        self.assertEqual(store.get_session(session_id), new_data)
        
        # Test TTL
        time.sleep(1.5)
        self.assertEqual(store.get_session(session_id), new_data) # Refresh
        
        print("  Waiting for TTL expiry...")
        time.sleep(2.5)
        self.assertIsNone(store.get_session(session_id))
        
        # Test Delete
        store.create_session("del", 123, 10)
        store.delete_session("del")
        self.assertIsNone(store.get_session("del"))
        print("RedisSessionStore verified!")

if __name__ == "__main__":
    unittest.main()
