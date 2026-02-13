import unittest
from dataclasses import dataclass
from typing import Optional, Annotated, List, Tuple, Any
from document_store import InMemoryDocumentStore, MongoDocumentStore, PartitionKey, SortKey, GetParams

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

class DocumentStoreTestMixin:
    """Shared test logic for DocumentStore implementations."""
    store = None # To be set by subclasses

    def test_put_get_with_annotations(self):
        key = MyKey(user_id="alice", note_id=123)
        note = MyNote(title="Plan", content="Buy milk", category="personal")
        self.store.put(key, note)
        retrieved = self.store.get(key)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.title, "Plan") # type: ignore
        
        self.assertEqual(retrieved.title, "Plan") # type: ignore
        
        # Now key is a single flat tuple: ("alice", 123)
        key_tuple = self.store._get_key_tuple(key)
        self.assertEqual(key_tuple, ("alice", 123))

    def test_complex_keys_crud(self):
        # Multi-field PK: org_id, dept_id
        # Multi-field SK: category, timestamp
        k1 = ComplexKey(org_id="g", dept_id=1, category="A", timestamp="2026-01-01")
        k2 = ComplexKey(org_id="g", dept_id=1, category="A", timestamp="2026-01-02")
        k3 = ComplexKey(org_id="g", dept_id=2, category="B", timestamp="2026-01-01")
        
        # Put and Get
        self.store.put(k1, {"v": 1})
        self.assertEqual(self.store.get(k1), {"v": 1})
        
        # Batch Put and Get
        self.store.batch_put({k2: {"v": 2}, k3: {"v": 3}})
        batch_res = self.store.batch_get({k1, k2, k3})
        # Note: batch_get returns keys as InternalKey if found by Mongo
        # We need to normalize or check values
        v_list = sorted([v["v"] for v in batch_res.values()])
        self.assertEqual(v_list, [1, 2, 3])
        
        # Range across boundaries
        rng = self.store.get_range(k1, k3)
        self.assertEqual(len(rng), 3)
        self.assertEqual(rng[0][1], {"v": 1})
        self.assertEqual(rng[2][1], {"v": 3})
        
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
        
        results = self.store.get_by_index_range(idx_start, idx_end)
        self.assertEqual(len(results), 2)
        cats = {r.category for r in results}
        self.assertEqual(cats, {"A", "B"})
        
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
        k1 = MyKey("u1", 1)
        n1 = MyNote("T1", "C1", "work")
        k2 = MyKey("u1", 2)
        n2 = MyNote("T2", "C2", "personal")
        k3 = MyKey("u2", 1)
        n3 = MyNote("T3", "C3", "work")
        
        self.store.batch_put({k1: n1, k2: n2, k3: n3})
        
        idx = CategoryIndex(category="work")
        results = self.store.get_by_index(idx)
        self.assertEqual(len(results), 2)
        titles = {r.title for r in results}
        self.assertEqual(titles, {"T1", "T3"})

    def test_reverse_and_iterators(self):
        for i in range(1, 4):
            # Using dict as value because default_to_dict wraps primitives
            self.store.put(i, {"val": i})
        
        it = self.store.get_range_iterator(1, 3)
        vals = [item[1]["val"] for item in it]
        self.assertEqual(vals, [1, 2, 3])
        
        it_rev = self.store.get_range_iterator(1, 3, params=GetParams(reverse=True))
        vals_rev = [item[1]["val"] for item in it_rev]
        self.assertEqual(vals_rev, [3, 2, 1])

    def test_delete_range(self):
        for i in range(1, 5):
            self.store.put(i, {"v": i})
        self.store.delete_range(2, 3)
        self.assertIsNone(self.store.get(2))
        self.assertIsNone(self.store.get(3))
        self.assertIsNotNone(self.store.get(1))
        self.assertIsNotNone(self.store.get(1))
        self.assertIsNotNone(self.store.get(4))

    def test_ordered_keys(self):
        # Create keys where lexicographical order depends on 'prefix' (order=1)
        # even though 'suffix' (order=2) is defined first.
        # k1: prefix=10, suffix="b" -> (10, "b")
        k1 = OrderedKey(suffix="b", prefix=10)
        # k2: prefix=5, suffix="z" -> (5, "z")
        k2 = OrderedKey(suffix="z", prefix=5)
        
        # Verify internal key structure
        kt1 = self.store._get_key_tuple(k1)
        kt2 = self.store._get_key_tuple(k2)
        
        # Expecting key tuple to be (prefix, suffix) -> (order=1, order=2)
        # k1: prefix=10, suffix="b" -> (10, "b")
        self.assertEqual(kt1, (10, "b"))
        self.assertEqual(kt2, (5, "z"))
        
        # Verify comparison: (5, "z") < (10, "b")
        self.assertLess(kt2, kt1)
        
        # Put and Get Range
        self.store.put(k1, {"v": 1})
        self.store.put(k2, {"v": 2})
        
        # Range from k2 to k1 (since k2 < k1)
        results = self.store.get_range(k2, k1)
        self.assertEqual(len(results), 2)
        
        # results[i] is (key, value)
        # key is a tuple because key_type is not set.
        # Check components
        self.assertEqual(results[0][0], (5, "z"))
        self.assertEqual(results[1][0], (10, "b"))

class TestInMemoryDocumentStore(DocumentStoreTestMixin, unittest.TestCase):
    def setUp(self):
        self.store = InMemoryDocumentStore[Any, Any](
            from_dict_fn=lambda d: MyNote(**d) if d and "title" in d else d # type: ignore
        )

class TestMongoDocumentStore(DocumentStoreTestMixin, unittest.TestCase):
    def setUp(self):
        # We no longer skip if Mongo is not reachable, to help with debugging.
        self.collection_name = "test_collection_unique"
        self.store = MongoDocumentStore[Any, Any](
            connection_string="mongodb://localhost:27017/",
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

if __name__ == "__main__":
    unittest.main()
