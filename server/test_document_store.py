import unittest
from dataclasses import dataclass
from typing import Optional, Annotated, List, Tuple, Any
from document_store import InMemoryDocumentStore, MongoDocumentStore, PartitionKey, SortKey, GetParams
from pymongo import MongoClient

@dataclass(frozen=True)
class MyKey:
    user_id: Annotated[str, PartitionKey]
    note_id: Annotated[int, SortKey]

@dataclass
class MyNote:
    title: str
    content: str
    category: str

@dataclass(frozen=True)
class ComplexKey:
    org_id: Annotated[str, PartitionKey]
    dept_id: Annotated[int, PartitionKey]
    category: Annotated[str, SortKey]
    timestamp: Annotated[str, SortKey]

@dataclass(frozen=True)
class CategoryIndex:
    category: Annotated[str, PartitionKey]

@dataclass(frozen=True)
class SimpleKey:
    partition: Annotated[str, PartitionKey]
    value: Annotated[int, SortKey]

# ============================================================================
# Base Test Classes - One per Schema
# ============================================================================

class MyKeyMyNoteTests:
    """Tests using MyKey -> MyNote schema"""
    
    def test_basic_put_get(self):
        key = MyKey(user_id="alice", note_id=123)
        note = MyNote(title="Plan", content="Buy milk", category="personal")
        self.store.put(key, note)
        retrieved = self.store.get(key)
        self.assertIsNotNone(retrieved)
        self.assertIsInstance(retrieved, MyNote)
        self.assertEqual(retrieved.title, "Plan")
        self.assertEqual(retrieved.content, "Buy milk")
    
    def test_batch_operations(self):
        k1 = MyKey("batch", 1)
        k2 = MyKey("batch", 2)
        k3 = MyKey("batch", 3)
        
        n1 = MyNote("B1", "Content1", "cat1")
        n2 = MyNote("B2", "Content2", "cat2")
        n3 = MyNote("B3", "Content3", "cat3")
        
        self.store.batch_put({k1: n1, k2: n2, k3: n3})
        
        results = self.store.batch_get({k1, k2, k3})
        self.assertEqual(len(results), 3)
        for v in results.values():
            self.assertIsInstance(v, MyNote)
        titles = sorted([v.title for v in results.values()])
        self.assertEqual(titles, ["B1", "B2", "B3"])
    
    def test_update_overwrite(self):
        key = MyKey("u_update", 1)
        n1 = MyNote("Original", "Original content", "personal")
        n2 = MyNote("Updated", "Updated content", "work")
        
        self.store.put(key, n1)
        retrieved = self.store.get(key)
        self.assertIsInstance(retrieved, MyNote)
        self.assertEqual(retrieved.title, "Original")
        
        self.store.put(key, n2)
        retrieved = self.store.get(key)
        self.assertIsInstance(retrieved, MyNote)
        self.assertEqual(retrieved.title, "Updated")
        self.assertEqual(retrieved.category, "work")
    
    def test_secondary_index(self):
        k1 = MyKey("u1", 1)
        n1 = MyNote("T1", "C1", "personal")
        k2 = MyKey("u1", 2)
        n2 = MyNote("T2", "C2", "work")
        k3 = MyKey("u2", 1)
        n3 = MyNote("T3", "C3", "personal")
        
        self.store.batch_put({k1: n1, k2: n2, k3: n3})
        
        idx = CategoryIndex(category="personal")
        items = self.store.get_by_index(idx)
        
        self.assertEqual(len(items), 2)
        for item in items:
            self.assertIsInstance(item, MyNote)
        titles = sorted([item.title for item in items])
        self.assertEqual(titles, ["T1", "T3"])
    
    def test_index_range(self):
        k1 = MyKey("u1", 1)
        n1 = MyNote("T1", "C1", "A")
        k2 = MyKey("u1", 2)
        n2 = MyNote("T2", "C2", "B")
        k3 = MyKey("u2", 1)
        n3 = MyNote("T3", "C3", "C")
        
        self.store.batch_put({k1: n1, k2: n2, k3: n3})
        
        idx_start = CategoryIndex(category="A")
        idx_end = CategoryIndex(category="B")
        items = self.store.get_by_index_range(idx_start, idx_end)
        
        self.assertEqual(len(items), 2)
        for item in items:
            self.assertIsInstance(item, MyNote)
        titles = sorted([item.title for item in items])
        self.assertEqual(titles, ["T1", "T2"])

class ComplexKeyMyNoteTests:
    """Tests using ComplexKey -> MyNote schema"""
    
    def test_complex_keys_crud(self):
        k1 = ComplexKey(org_id="g", dept_id=1, category="A", timestamp="2026-01-01")
        k2 = ComplexKey(org_id="g", dept_id=1, category="A", timestamp="2026-01-02")
        k3 = ComplexKey(org_id="g", dept_id=1, category="B", timestamp="2026-01-01")
        
        n1 = MyNote(title="Note1", content="Content1", category="A")
        n2 = MyNote(title="Note2", content="Content2", category="A")
        n3 = MyNote(title="Note3", content="Content3", category="B")
        
        self.store.put(k1, n1)
        retrieved = self.store.get(k1)
        self.assertIsInstance(retrieved, MyNote)
        self.assertEqual(retrieved.title, "Note1")
        
        self.store.batch_put({k2: n2, k3: n3})
        batch_res = self.store.batch_get({k1, k2, k3})
        self.assertEqual(len(batch_res), 3)
        for v in batch_res.values():
            self.assertIsInstance(v, MyNote)
        
        rng = self.store.get_range(k1, k3)
        self.assertEqual(len(rng), 3)
        self.assertIsInstance(rng[0][1], MyNote)
        self.assertEqual(rng[0][1].title, "Note1")
        self.assertIsInstance(rng[2][1], MyNote)
        self.assertEqual(rng[2][1].title, "Note3")
        
        self.store.delete_range(k1, k2)
        self.assertIsNone(self.store.get(k1))
        self.assertIsNone(self.store.get(k2))
        self.assertIsNotNone(self.store.get(k3))

class SimpleKeyMyNoteTests:
    """Tests using SimpleKey -> MyNote schema"""
    
    def test_range_and_iterators(self):
        k1 = SimpleKey("p", 5)
        k2 = SimpleKey("p", 10)
        k3 = SimpleKey("p", 15)
        
        n1 = MyNote("T1", "C1", "A")
        n2 = MyNote("T2", "C2", "B")
        n3 = MyNote("T3", "C3", "C")
        
        self.store.batch_put({k1: n1, k2: n2, k3: n3})
        
        items = list(self.store.get_range_iterator(k1, k3, GetParams(reverse=False)))
        self.assertEqual(len(items), 3)
        for _, v in items:
            self.assertIsInstance(v, MyNote)
        self.assertEqual(items[0][1].title, "T1")
        self.assertEqual(items[2][1].title, "T3")
        
        items_rev = list(self.store.get_range_iterator(k1, k3, GetParams(reverse=True)))
        self.assertEqual(len(items_rev), 3)
        for _, v in items_rev:
            self.assertIsInstance(v, MyNote)
        self.assertEqual(items_rev[0][1].title, "T3")
        self.assertEqual(items_rev[2][1].title, "T1")
    
    def test_delete_range(self):
        k1 = SimpleKey("p", 1)
        k2 = SimpleKey("p", 5)
        k3 = SimpleKey("p", 10)
        
        n1 = MyNote("T1", "C1", "A")
        n2 = MyNote("T2", "C2", "B")
        n3 = MyNote("T3", "C3", "C")
        
        self.store.batch_put({k1: n1, k2: n2, k3: n3})
        
        self.store.delete_range(k1, k2)
        
        self.assertIsNone(self.store.get(k1))
        self.assertIsNone(self.store.get(k2))
        retrieved = self.store.get(k3)
        self.assertIsNotNone(retrieved)
        self.assertIsInstance(retrieved, MyNote)
        self.assertEqual(retrieved.title, "T3")

# ============================================================================
# Concrete Test Classes for InMemoryDocumentStore
# ============================================================================

class TestInMemoryMyKeyMyNote(MyKeyMyNoteTests, unittest.TestCase):
    def setUp(self):
        self.store = InMemoryDocumentStore[MyKey, MyNote](
            key_type=MyKey,
            data_type=MyNote
        )

class TestInMemoryComplexKey(ComplexKeyMyNoteTests, unittest.TestCase):
    def setUp(self):
        self.store = InMemoryDocumentStore[ComplexKey, MyNote](
            key_type=ComplexKey,
            data_type=MyNote
        )

class TestInMemorySimpleKey(SimpleKeyMyNoteTests, unittest.TestCase):
    def setUp(self):
        self.store = InMemoryDocumentStore[SimpleKey, MyNote](
            key_type=SimpleKey,
            data_type=MyNote
        )

# ============================================================================
# Concrete Test Classes for MongoDocumentStore
# ============================================================================

class TestMongoMyKeyMyNote(MyKeyMyNoteTests, unittest.TestCase):
    def setUp(self):
        self.store = MongoDocumentStore[MyKey, MyNote](
            client=MongoClient("mongodb://localhost:27017/"),
            database_name="test_db",
            collection_name="test_mykey_mynote",
            key_type=MyKey,
            data_type=MyNote
        )
        self.store.drop_table()
        self.store.create_table()
    
    def tearDown(self):
        if hasattr(self, 'store'):
            self.store.drop_table()
            self.store.close()

class TestMongoComplexKey(ComplexKeyMyNoteTests, unittest.TestCase):
    def setUp(self):
        self.store = MongoDocumentStore[ComplexKey, MyNote](
            client=MongoClient("mongodb://localhost:27017/"),
            database_name="test_db",
            collection_name="test_complexkey_mynote",
            key_type=ComplexKey,
            data_type=MyNote
        )
        self.store.drop_table()
        self.store.create_table()
    
    def tearDown(self):
        if hasattr(self, 'store'):
            self.store.drop_table()
            self.store.close()

class TestMongoSimpleKey(SimpleKeyMyNoteTests, unittest.TestCase):
    def setUp(self):
        self.store = MongoDocumentStore[SimpleKey, MyNote](
            client=MongoClient("mongodb://localhost:27017/"),
            database_name="test_db",
            collection_name="test_simplekey_mynote",
            key_type=SimpleKey,
            data_type=MyNote
        )
        self.store.drop_table()
        self.store.create_table()
    
    def tearDown(self):
        if hasattr(self, 'store'):
            self.store.drop_table()
            self.store.close()

# ============================================================================
# Concrete Test Classes for MongoEmbeddedDocumentStore
# ============================================================================

from document_store import MongoEmbeddedDocumentStore

class TestEmbeddedMyKeyMyNote(MyKeyMyNoteTests, unittest.TestCase):
    def setUp(self):
        self.store = MongoEmbeddedDocumentStore[MyKey, MyNote](
            client=MongoClient("mongodb://localhost:27017/"),
            database_name="test_db",
            collection_name="test_embedded_mykey",
            key_type=MyKey,
            data_type=MyNote
        )
        self.store.drop_table()
        self.store.create_table()
    
    def tearDown(self):
        if hasattr(self, 'store'):
            self.store.drop_table()
            self.store.close()

class TestEmbeddedComplexKey(ComplexKeyMyNoteTests, unittest.TestCase):
    def setUp(self):
        self.store = MongoEmbeddedDocumentStore[ComplexKey, MyNote](
            client=MongoClient("mongodb://localhost:27017/"),
            database_name="test_db",
            collection_name="test_embedded_complex",
            key_type=ComplexKey,
            data_type=MyNote
        )
        self.store.drop_table()
        self.store.create_table()
    
    def tearDown(self):
        if hasattr(self, 'store'):
            self.store.drop_table()
            self.store.close()

class TestEmbeddedSimpleKey(SimpleKeyMyNoteTests, unittest.TestCase):
    def setUp(self):
        self.store = MongoEmbeddedDocumentStore[SimpleKey, MyNote](
            client=MongoClient("mongodb://localhost:27017/"),
            database_name="test_db",
            collection_name="test_embedded_simple",
            key_type=SimpleKey,
            data_type=MyNote
        )
        self.store.drop_table()
        self.store.create_table()
    
    def tearDown(self):
        if hasattr(self, 'store'):
            self.store.drop_table()
            self.store.close()

if __name__ == "__main__":
    unittest.main()


@dataclass(frozen=True)
class MyKey:
    user_id: Annotated[str, PartitionKey]
    note_id: Annotated[int, SortKey]

@dataclass
class MyNote:
    title: str
    content: str
    category: str

@dataclass(frozen=True)
class ComplexKey:
    org_id: Annotated[str, PartitionKey]
    dept_id: Annotated[int, PartitionKey]
    category: Annotated[str, SortKey]
    timestamp: Annotated[str, SortKey]

@dataclass(frozen=True)
class CategoryIndex:
    category: Annotated[str, PartitionKey]

@dataclass(frozen=True)
class OrderedKey:
    # Defined first, but order=2 so it's the second part of the sort key
    suffix: Annotated[str, SortKey(order=2)]
    # Defined second, but order=1 so it's the first part of the sort key
    prefix: Annotated[int, SortKey(order=1)]
    # Make OrderedKey partition-aware for embedded store
    partition: Annotated[str, PartitionKey] = "p"

class DocumentStoreTestMixin:
    """Shared test logic for DocumentStore implementations."""
    store = None # To be set by subclasses

    def test_put_get_with_annotations(self):
        key = MyKey(user_id="alice", note_id=123)
        note = MyNote(title="Plan", content="Buy milk", category="personal")
        self.store.put(key, note)
        retrieved = self.store.get(key)
        self.assertIsNotNone(retrieved)
        self.assertIsInstance(retrieved, MyNote)
        self.assertEqual(retrieved.title, "Plan")
        self.assertEqual(retrieved.content, "Buy milk")
        
        # Now key is a single flat tuple: ("alice", 123)
        key_tuple = self.store._get_key_tuple(key)
        self.assertEqual(key_tuple, ("alice", 123))

    def test_complex_keys_crud(self):
        # Multi-field PK: org_id, dept_id
        # Multi-field SK: category, timestamp
        k1 = ComplexKey(org_id="g", dept_id=1, category="A", timestamp="2026-01-01")
        k2 = ComplexKey(org_id="g", dept_id=1, category="A", timestamp="2026-01-02")
        k3 = ComplexKey(org_id="g", dept_id=1, category="B", timestamp="2026-01-01")
        
        # Put and Get with MyNote objects
        n1 = MyNote(title="Note1", content="Content1", category="A")
        n2 = MyNote(title="Note2", content="Content2", category="A")
        n3 = MyNote(title="Note3", content="Content3", category="B")
        
        self.store.put(k1, n1)
        retrieved = self.store.get(k1)
        self.assertIsInstance(retrieved, MyNote)
        self.assertEqual(retrieved.title, "Note1")
        
        # Batch Put and Get
        self.store.batch_put({k2: n2, k3: n3})
        batch_res = self.store.batch_get({k1, k2, k3})
        self.assertEqual(len(batch_res), 3)
        for v in batch_res.values():
            self.assertIsInstance(v, MyNote)
        
        # Range across boundaries
        rng = self.store.get_range(k1, k3)
        self.assertEqual(len(rng), 3)
        self.assertIsInstance(rng[0][1], MyNote)
        self.assertEqual(rng[0][1].title, "Note1")
        self.assertIsInstance(rng[2][1], MyNote)
        self.assertEqual(rng[2][1].title, "Note3")
        
        # Delete Range
        self.store.delete_range(k1, k2)
        self.assertIsNone(self.store.get(k1))
        self.assertIsNone(self.store.get(k2))
        self.assertIsNotNone(self.store.get(k3))

    def test_index_range_comprehensive(self):
        # Use MyNote with category as index
        k1 = MyKey("u1", 1)
        n1 = MyNote("T1", "C1", "A") # category A
        k2 = MyKey("u1", 2)
        n2 = MyNote("T2", "C2", "B") # category B
        k3 = MyKey("u2", 1)
        n3 = MyNote("T3", "C3", "C") # category C
        
        self.store.batch_put({k1: n1, k2: n2, k3: n3})
        
        # Index range A to B
        idx_start = CategoryIndex(category="A")
        idx_end = CategoryIndex(category="B")
        
        items = self.store.get_by_index_range(idx_start, idx_end)
        
        # Should return n1 and n2
        self.assertEqual(len(items), 2)
        for item in items:
            self.assertIsInstance(item, MyNote)
        titles = sorted([item.title for item in items])
        self.assertEqual(titles, ["T1", "T2"])
        
        # Iterator
        it = self.store.get_index_range_iterator(idx_start, idx_end)
        it_results = list(it)
        self.assertEqual(len(it_results), 2)
        
        # Reverse Index Range
        rev_results = self.store.get_by_index_range(idx_start, idx_end, params=GetParams(reverse=True))
        self.assertEqual(len(rev_results), 2)
        self.assertEqual(rev_results[0].category, "B")
        self.assertEqual(rev_results[1].category, "A")

    def test_secondary_index(self):
        # Use MyNote with category as index
        k1 = MyKey("u1", 1)
        n1 = MyNote("T1", "C1", "personal")
        k2 = MyKey("u1", 2)
        n2 = MyNote("T2", "C2", "work")
        k3 = MyKey("u2", 1)
        n3 = MyNote("T3", "C3", "personal")
        
        self.store.batch_put({k1: n1, k2: n2, k3: n3})
        
        # Query by category index
        idx = CategoryIndex(category="personal")
        items = self.store.get_by_index(idx)
        
        self.assertEqual(len(items), 2)
        for item in items:
            self.assertIsInstance(item, MyNote)
        titles = sorted([item.title for item in items])
        self.assertEqual(titles, ["T1", "T3"])

    def test_reverse_and_iterators(self):
        @dataclass(frozen=True)
        class PIntKey:
            partition: Annotated[str, PartitionKey]
            value: Annotated[int, SortKey]
        
        k1 = PIntKey("p", 5)
        k2 = PIntKey("p", 10)
        k3 = PIntKey("p", 15)
        
        n1 = MyNote("T1", "C1", "A")
        n2 = MyNote("T2", "C2", "B")
        n3 = MyNote("T3", "C3", "C")
        
        self.store.batch_put({k1: n1, k2: n2, k3: n3})
        
        # Forward
        from document_store import GetParams
        items = list(self.store.get_range_iterator(k1, k3, GetParams(reverse=False)))
        self.assertEqual(len(items), 3)
        for _, v in items:
            self.assertIsInstance(v, MyNote)
        self.assertEqual(items[0][1].title, "T1")
        self.assertEqual(items[2][1].title, "T3")
        
        # Reverse
        items_rev = list(self.store.get_range_iterator(k1, k3, GetParams(reverse=True)))
        self.assertEqual(len(items_rev), 3)
        for _, v in items_rev:
            self.assertIsInstance(v, MyNote)
        self.assertEqual(items_rev[0][1].title, "T3")
        self.assertEqual(items_rev[2][1].title, "T1")

    def test_delete_range(self):
        @dataclass(frozen=True)
        class PIntKey:
            partition: Annotated[str, PartitionKey]
            value: Annotated[int, SortKey]
        
        k1 = PIntKey("p", 1)
        k2 = PIntKey("p", 5)
        k3 = PIntKey("p", 10)
        
        n1 = MyNote("T1", "C1", "A")
        n2 = MyNote("T2", "C2", "B")
        n3 = MyNote("T3", "C3", "C")
        
        self.store.batch_put({k1: n1, k2: n2, k3: n3})
        
        # Delete range [k1, k2]
        self.store.delete_range(k1, k2)
        
        self.assertIsNone(self.store.get(k1))
        self.assertIsNone(self.store.get(k2))
        retrieved = self.store.get(k3)
        self.assertIsNotNone(retrieved)
        self.assertIsInstance(retrieved, MyNote)
        self.assertEqual(retrieved.title, "T3")

    def test_ordered_keys(self):
        # Test that order parameter works correctly
        k1 = OrderedKey(suffix="z", prefix=5)
        k2 = OrderedKey(suffix="b", prefix=10)
        
        n1 = MyNote("T1", "C1", "A")
        n2 = MyNote("T2", "C2", "B")
        
        self.store.batch_put({k1: n1, k2: n2})
        
        # Get range - should be sorted by prefix first (5, 10), then suffix
        results = self.store.get_range(k1, k2)
        
        # results[i] is (key, value)
        # key is a tuple because key_type is not set.
        # Check components
        self.assertEqual(results[0][0], ("p", (5, "z")))
        self.assertEqual(results[1][0], ("p", (10, "b")))
        
        # Check values are MyNote
        for _, v in results:
            self.assertIsInstance(v, MyNote)

    def test_update_overwrite(self):
        key = MyKey("u_update", 1)
        n1 = MyNote("Original", "Original content", "personal")
        n2 = MyNote("Updated", "Updated content", "work")
        
        self.store.put(key, n1)
        retrieved = self.store.get(key)
        self.assertIsInstance(retrieved, MyNote)
        self.assertEqual(retrieved.title, "Original")
        
        # Overwrite
        self.store.put(key, n2)
        retrieved = self.store.get(key)
        self.assertIsInstance(retrieved, MyNote)
        self.assertEqual(retrieved.title, "Updated")
        self.assertEqual(retrieved.category, "work")

    def test_delete_single(self):
        key = MyKey("u_del", 1)
        self.store.put(key, MyNote("T", "C", "A"))
        self.store.delete(key)
        self.assertIsNone(self.store.get(key))

    def test_multiple_items_in_partition(self):
        # Using MyKey which has (user_id, note_id)
        user = "u_multi"
        for i in range(5):
            self.store.put(MyKey(user, i), MyNote(f"T{i}", "C", "A"))
            
        for i in range(5):
            retrieved = self.store.get(MyKey(user, i))
            self.assertIsNotNone(retrieved)
            self.assertEqual(retrieved.title, f"T{i}")
            
        # Range query within partition
        start = MyKey(user, 1)
        end = MyKey(user, 3)
        items = self.store.get_range(start, end)
        self.assertEqual(len(items), 3)
        self.assertEqual(items[0][1].title, "T1")
        self.assertEqual(items[2][1].title, "T3")

class TestInMemoryDocumentStore(DocumentStoreTestMixin, unittest.TestCase):
    def setUp(self):
        self.store = InMemoryDocumentStore[Any, MyNote](
            from_dict_fn=lambda d: MyNote(**d) if d and "title" in d else d # type: ignore
        )

class TestMongoDocumentStore(DocumentStoreTestMixin, unittest.TestCase):
    from pymongo import MongoClient
    def setUp(self):
        # We no longer skip if Mongo is not reachable, to help with debugging.
        self.collection_name = "test_collection_unique"
        self.store = MongoDocumentStore[Any, MyNote](
            client=MongoClient("mongodb://localhost:27017/"),
            database_name="test_db",
            collection_name=self.collection_name,
            from_dict_fn=lambda d: MyNote(**d) if d and "title" in d else d # type: ignore
        )
        # Clear if exists
        self.store.drop_table()
        self.store.create_table()

    def tearDown(self):
        if hasattr(self, 'store'):
            self.store.drop_table()
            self.store.close()


from document_store import MongoEmbeddedDocumentStore

class TestMongoEmbeddedDocumentStore(DocumentStoreTestMixin, unittest.TestCase):
    def setUp(self):
        self.collection_name = "test_embedded_collection"
        self.store = MongoEmbeddedDocumentStore[Any, MyNote](
            client=MongoClient("mongodb://localhost:27017/"),
            database_name="test_social_lib",
            collection_name=self.collection_name,
            from_dict_fn=lambda d: MyNote(**d) if d and "title" in d else d
        )
        self.store.drop_table()
        self.store.create_table()

    def tearDown(self):
        if hasattr(self, 'store'):
            self.store.drop_table()
            self.store.close()

if __name__ == "__main__":
    unittest.main()
