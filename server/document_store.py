import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Tuple, Union, TypeVar, Generic, Callable, Annotated, get_type_hints, get_args, get_origin
from dataclasses import dataclass, field, make_dataclass
import datetime


@dataclass
class OperationParams:
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class PutParams(OperationParams):
    overwrite: bool = True

@dataclass
class GetParams(OperationParams):
    consistent_read: bool = False
    reverse: bool = False
    batch_size: Optional[int] = None

@dataclass
class DeleteParams(OperationParams):
    pass

@dataclass
class PartitionKey:
    """Marker for partition key members with optional ordering."""
    order: int = 0

@dataclass
class SortKey:
    """Marker for sort key members with optional ordering."""
    order: int = 0

K = TypeVar('K')
V = TypeVar('V')

def default_to_dict(obj: Any) -> Dict[str, Any]:
    """
    Very simple reflection helper to convert an object to a dict.
    """
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    if isinstance(obj, dict):
        return obj
    return {"value": obj}

class DocumentStore(Generic[K, V], ABC):
    def __init__(
        self, 
        key_attrs: Optional[List[str]] = None,
        key_type: Optional[type] = None,
        to_dict_fn: Callable[[V], Dict[str, Any]] = default_to_dict,
        from_dict_fn: Optional[Callable[[Dict[str, Any]], V]] = None
    ):
        self._key_attrs = key_attrs
        self.key_type = key_type
        self._to_dict_fn = to_dict_fn
        self._from_dict_fn = from_dict_fn

    def _discover_attrs_for(self, cls: type) -> List[str]:
        """
        Discovers partition and sort attributes for any class, respecting specified order.
        """
        attrs = []
        try:
            hints = get_type_hints(cls, include_extras=True)
            for name, hint in hints.items():
                if get_origin(hint) is Annotated:
                    metadata = get_args(hint)[1:]
                    for m in metadata:
                        if m is PartitionKey or isinstance(m, PartitionKey):
                            order = m.order if isinstance(m, PartitionKey) else 0
                            attrs.append((order, name))
                        if m is SortKey or isinstance(m, SortKey):
                            order = m.order if isinstance(m, SortKey) else 0
                            attrs.append((order, name))
        except Exception as e:
            print(f"Warning: Failed to discover attributes for {cls}: {e}")
        
        # Sort by order, then by name (stable sort)
        attrs.sort(key=lambda x: x[0])
        
        return [a[1] for a in attrs]

    def _discover_attrs(self, cls: type):
        """
        Discovers key attributes by looking at class annotations for K.
        """
        if not self._key_attrs:
            self._key_attrs = self._discover_attrs_for(cls)

    def _get_key_tuple(self, obj: Any, attrs: Optional[List[str]] = None) -> Tuple[Any, ...]:
        # If attrs are not provided, use instance defaults
        key_attrs = attrs if attrs is not None else self._key_attrs

        # Discovery if needed
        if not key_attrs:
            # If we don't have attrs, and we can't discover from obj (e.g. primitive), we assume it's a simple key
            if isinstance(obj, (str, int, float, bool, datetime.date, tuple)):
                 return obj if isinstance(obj, tuple) else (obj,)
            
            # Try discovery
            discovered = self._discover_attrs_for(type(obj))
            key_attrs = key_attrs if key_attrs else discovered
            
        # Safe defaults
        key_attrs = key_attrs or []

        if isinstance(obj, (str, int, float, bool, datetime.date, tuple)):
            return obj if isinstance(obj, tuple) else (obj,)

        d = default_to_dict(obj) if not isinstance(obj, dict) else obj
        
        return tuple(d.get(attr) for attr in key_attrs)
    
    def _reconstruct_key(self, values: Tuple[Any, ...]) -> K:
        # If we have a key_type, try to populate it
        if self.key_type:
             if self.key_type in (str, int, float, bool, datetime.date):
                 return values[0] # type: ignore
             if self.key_type == tuple:
                 return values # type: ignore
                 
             # Assuming dataclass or user object with __init__
             if hasattr(self.key_type, "__dataclass_fields__") or isinstance(self.key_type, type):
                 if not self._key_attrs:
                     self._discover_attrs(self.key_type)
                 
                 kwargs = {}
                 for i, attr in enumerate(self._key_attrs):
                     if i < len(values):
                         kwargs[attr] = values[i]
                 try:
                     return self.key_type(**kwargs)
                 except Exception:
                     # Fallback if init fails
                     pass

        if len(values) == 1:
            return values[0] # type: ignore
        return values # type: ignore

    def _to_data_dict(self, obj: V) -> Dict[str, Any]:
        return self._to_dict_fn(obj)

    def _from_data_dict(self, data: Dict[str, Any]) -> V:
        if self._from_dict_fn:
            return self._from_dict_fn(data)
        return data  # type: ignore

    @abstractmethod
    def put(self, key: K, data: V, params: Optional[PutParams] = None):
        pass

    @abstractmethod
    def batch_put(self, items: Dict[K, V], params: Optional[PutParams] = None):
        pass

    @abstractmethod
    def get(self, key: K, params: Optional[GetParams] = None) -> Optional[V]:
        pass

    @abstractmethod
    def batch_get(self, keys: Set[K], params: Optional[GetParams] = None) -> Dict[K, V]:
        pass

    @abstractmethod
    def get_range(self, start_key: K, end_key: K, params: Optional[GetParams] = None) -> List[Tuple[K, V]]:
        pass

    @abstractmethod
    def get_range_iterator(self, start_key: K, end_key: K, params: Optional[GetParams] = None): # -> Iterator[Tuple[K, V]]
        pass

    @abstractmethod
    def get_by_index(self, index_key: Any, params: Optional[GetParams] = None) -> List[V]:
        pass

    @abstractmethod
    def get_by_index_range(self, start_index: Any, end_index: Any, params: Optional[GetParams] = None) -> List[V]:
        pass

    @abstractmethod
    def get_index_range_iterator(self, start_index: Any, end_index: Any, params: Optional[GetParams] = None): # -> Iterator[V]
        pass

    @abstractmethod
    def delete(self, key: K, params: Optional[DeleteParams] = None):
        pass

    @abstractmethod
    def delete_range(self, start_key: K, end_key: K, params: Optional[DeleteParams] = None):
        pass

    @abstractmethod
    def create_table(self):
        pass

    @abstractmethod
    def drop_table(self):
        pass

    @abstractmethod
    def close(self):
        pass

class InMemoryDocumentStore(DocumentStore[K, V]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Storage: key_tuple -> data_dict
        self._storage: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    def put(self, key: K, data: V, params: Optional[PutParams] = None):
        key_tuple = self._get_key_tuple(key)
        self._storage[key_tuple] = self._to_data_dict(data)

    def batch_put(self, items: Dict[K, V], params: Optional[PutParams] = None):
        for k, v in items.items():
            self.put(k, v, params)

    def get(self, key: K, params: Optional[GetParams] = None) -> Optional[V]:
        key_tuple = self._get_key_tuple(key)
        data = self._storage.get(key_tuple)
        return self._from_data_dict(data) if data is not None else None

    def batch_get(self, keys: Set[K], params: Optional[GetParams] = None) -> Dict[K, V]:
        results = {}
        for k in keys:
            val = self.get(k, params)
            if val is not None:
                results[k] = val
        return results

    def get_range(self, start_key: K, end_key: K, params: Optional[GetParams] = None) -> List[Tuple[K, V]]:
        s_tuple = self._get_key_tuple(start_key)
        e_tuple = self._get_key_tuple(end_key)
        
        results = []
        for k_tuple in sorted(self._storage.keys(), reverse=params.reverse if params else False):
            if s_tuple <= k_tuple <= e_tuple:
                k = self._reconstruct_key(k_tuple)
                results.append((k, self._from_data_dict(self._storage[k_tuple]))) # type: ignore
        return results

    def get_range_iterator(self, start_key: K, end_key: K, params: Optional[GetParams] = None):
        items = self.get_range(start_key, end_key, params)
        for item in items:
            yield item

    def _match_index(self, obj_data: Dict[str, Any], target_tuple: Tuple[Any, ...], idx_attrs: List[str]) -> bool:
        # Construct current object's index key values
        obj_vals = tuple(obj_data.get(a) for a in idx_attrs)
        return obj_vals == target_tuple

    def get_by_index(self, index_key: Any, params: Optional[GetParams] = None) -> List[V]:
        idx_attrs = self._discover_attrs_for(type(index_key))
        idx_tuple = self._get_key_tuple(index_key, idx_attrs)
        results = []
        for data_dict in self._storage.values():
            if self._match_index(data_dict, idx_tuple, idx_attrs):
                results.append(self._from_data_dict(data_dict))
        return results

    def get_by_index_range(self, start_index: Any, end_index: Any, params: Optional[GetParams] = None) -> List[V]:
        idx_attrs = self._discover_attrs_for(type(start_index))
        s_tuple = self._get_key_tuple(start_index, idx_attrs)
        e_tuple = self._get_key_tuple(end_index, idx_attrs)
        
        all_matches = []
        for data_dict in self._storage.values():
            obj_vals = tuple(data_dict.get(a) for a in idx_attrs)
            
            if s_tuple <= obj_vals <= e_tuple:
                all_matches.append((obj_vals, self._from_data_dict(data_dict)))
        
        # Sort by the index key structure
        all_matches.sort(key=lambda x: x[0], reverse=params.reverse if params else False)
        return [x[1] for x in all_matches]

    def get_index_range_iterator(self, start_index: Any, end_index: Any, params: Optional[GetParams] = None):
        items = self.get_by_index_range(start_index, end_index, params)
        for item in items:
            yield item

    def delete(self, key: K, params: Optional[DeleteParams] = None):
        key_tuple = self._get_key_tuple(key)
        if key_tuple in self._storage:
            del self._storage[key_tuple]

    def delete_range(self, start_key: K, end_key: K, params: Optional[DeleteParams] = None):
        s_tuple = self._get_key_tuple(start_key)
        e_tuple = self._get_key_tuple(end_key)
        
        keys_to_delete = [
            k for k in self._storage.keys() 
            if s_tuple <= k <= e_tuple
        ]
        for k in keys_to_delete:
            del self._storage[k]

    def create_table(self):
        pass

    def drop_table(self):
        self._storage.clear()

    def close(self):
        pass

class MongoDocumentStore(DocumentStore[K, V]):
    def __init__(self, connection_string: str, database_name: str, collection_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from pymongo import MongoClient
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        self.collection_name = collection_name
        self.collection = self.db[collection_name]

    def _key_to_mongo_query(self, key: K) -> Dict[str, Any]:
        # Ensure discovery
        if not self._key_attrs:
            self._discover_attrs(type(key))

        vals = self._get_key_tuple(key)
        
        # 1. Mapped attributes (Preferred for complex keys)
        if self._key_attrs and len(self._key_attrs) == len(vals):
             return {"_id": {attr: val for attr, val in zip(self._key_attrs, vals)}}
        
        # 2. Single generic value (primitive)
        if len(vals) == 1:
             return {"_id": vals[0]}

        # 3. Tuple/List generic (multiple values, no attrs)
        # Wrap in dict to avoid array _id error if strict
        # But for range queries, this wrapper might be annoying.
        # However, we must avoid top-level array _id.
        return {"_id": {"v": list(vals)}}

    def put(self, key: K, data: V, params: Optional[PutParams] = None):
        mongo_filter = self._key_to_mongo_query(key)
        doc = mongo_filter.copy()
        doc["data"] = self._to_data_dict(data)
        self.collection.replace_one(mongo_filter, doc, upsert=True)

    def batch_put(self, items: Dict[K, V], params: Optional[PutParams] = None):
        from pymongo import ReplaceOne
        operations = []
        for key, data in items.items():
            mongo_filter = self._key_to_mongo_query(key)
            doc = mongo_filter.copy()
            doc["data"] = self._to_data_dict(data)
            operations.append(ReplaceOne(mongo_filter, doc, upsert=True))
        if operations:
            self.collection.bulk_write(operations)

    def get(self, key: K, params: Optional[GetParams] = None) -> Optional[V]:
        query = self._key_to_mongo_query(key)
        doc = self.collection.find_one(query)
        return self._from_data_dict(doc["data"]) if doc else None

    def batch_get(self, keys: Set[K], params: Optional[GetParams] = None) -> Dict[K, V]:
        # Map key_tuple -> original key
        key_map = {}
        ids = []
        for k in keys:
             q = self._key_to_mongo_query(k)
             ids.append(q["_id"])
             # Store mapping by the immutable _id representation (for dicts we need tuple)
             # _id can be a dict (if attrs are known) or list.
             # We need a stable key for the map.
             
             # Let's use the tuple values as the stable key for mapping
             kt = self._get_key_tuple(k)
             key_map[kt] = k

        query = {"_id": {"$in": ids}}
        cursor = self.collection.find(query)
        
        results = {}
        for doc in cursor:
            _id = doc["_id"]
            if isinstance(_id, dict) and "v" in _id and len(_id) == 1 and isinstance(_id["v"], list):
                 vals = tuple(_id["v"])
            elif isinstance(_id, dict) and self._key_attrs:
                 vals = tuple(_id.get(attr) for attr in self._key_attrs)
            elif isinstance(_id, dict):
                 vals = tuple(_id.values())
            elif isinstance(_id, list):
                 vals = tuple(_id)
            else:
                 vals = (_id,)
                 
            original_k = key_map.get(vals)
            if original_k:
                results[original_k] = self._from_data_dict(doc["data"])
            else:
                k = self._reconstruct_key(vals)
                results[k] = self._from_data_dict(doc["data"])
        return results

    def get_range(self, start_key: K, end_key: K, params: Optional[GetParams] = None) -> List[Tuple[K, V]]:
        cursor = self._get_range_cursor(start_key, end_key, params)
        results = []
        for doc in cursor:
            _id = doc["_id"]
            if isinstance(_id, dict) and "v" in _id and len(_id) == 1 and isinstance(_id["v"], list):
                 # Case 3: Tuple wrapped in dict
                 vals = tuple(_id["v"])
            elif isinstance(_id, dict) and self._key_attrs:
                 # Case 1: Mapped attrs
                 vals = tuple(_id.get(attr) for attr in self._key_attrs)
            elif isinstance(_id, dict):
                 # Fallback for dict without known attrs (maybe usage of insertion order?)
                 vals = tuple(_id.values())
            elif isinstance(_id, list):
                 # Should not happen with new logic, but for robustness
                 vals = tuple(_id)
            else:
                 # Case 2: Primitive (int, str, etc)
                 vals = (_id,)

            k = self._reconstruct_key(vals)
            results.append((k, self._from_data_dict(doc["data"])))
        return results

    def _get_range_cursor(self, start_key: K, end_key: K, params: Optional[GetParams] = None):
        s_query = self._key_to_mongo_query(start_key)["_id"]
        e_query = self._key_to_mongo_query(end_key)["_id"]
        
        query = {
            "_id": {
                "$gte": s_query,
                "$lte": e_query
            }
        }
        sort_dir = -1 if params and params.reverse else 1
        cursor = self.collection.find(query).sort("_id", sort_dir)
        if params and params.batch_size:
            cursor.batch_size(params.batch_size)
        return cursor

    def get_range_iterator(self, start_key: K, end_key: K, params: Optional[GetParams] = None):
        cursor = self._get_range_cursor(start_key, end_key, params)
        for doc in cursor:
            _id = doc["_id"]
            if isinstance(_id, dict) and "v" in _id and len(_id) == 1 and isinstance(_id["v"], list):
                 vals = tuple(_id["v"])
            elif isinstance(_id, dict) and self._key_attrs:
                 vals = tuple(_id.get(attr) for attr in self._key_attrs)
            elif isinstance(_id, dict):
                 vals = tuple(_id.values())
            elif isinstance(_id, list):
                 vals = tuple(_id)
            else:
                 vals = (_id,)

            k = self._reconstruct_key(vals)
            yield (k, self._from_data_dict(doc["data"]))

    def _index_to_mongo_query(self, vals: Tuple[Any, ...], attrs: List[str]) -> Dict[str, Any]:
        query = {}
        for i, attr in enumerate(attrs):
            query[f"data.{attr}"] = vals[i]
        return query

    def get_by_index(self, index_key: Any, params: Optional[GetParams] = None) -> List[V]:
        attrs = self._discover_attrs_for(type(index_key))
        vals = self._get_key_tuple(index_key, attrs)
        query = self._index_to_mongo_query(vals, attrs)
        # In a real app we'd want an index for sorting too
        cursor = self.collection.find(query)
        return [self._from_data_dict(doc["data"]) for doc in cursor]

    def _get_index_range_cursor(self, start_index: Any, end_index: Any, params: Optional[GetParams] = None):
        attrs = self._discover_attrs_for(type(start_index))
        s_vals = self._get_key_tuple(start_index, attrs)
        e_vals = self._get_key_tuple(end_index, attrs)
        
        sort_dir = -1 if params and params.reverse else 1
        cursor = self.collection.find({}).sort([(f"data.{a}", sort_dir) for a in attrs])
        if params and params.batch_size:
            cursor.batch_size(params.batch_size)
        return cursor, attrs, s_vals, e_vals

    def get_by_index_range(self, start_index: Any, end_index: Any, params: Optional[GetParams] = None) -> List[V]:
        cursor, attrs, s_vals, e_vals = self._get_index_range_cursor(start_index, end_index, params)
        results = []
        for doc in cursor:
            obj_vals = tuple(doc["data"].get(a) for a in attrs)
            if s_vals <= obj_vals <= e_vals:
                results.append(self._from_data_dict(doc["data"]))
        return results

    def get_index_range_iterator(self, start_index: Any, end_index: Any, params: Optional[GetParams] = None):
        cursor, attrs, s_vals, e_vals = self._get_index_range_cursor(start_index, end_index, params)
        for doc in cursor:
            obj_vals = tuple(doc["data"].get(a) for a in attrs)
            if s_vals <= obj_vals <= e_vals:
                yield self._from_data_dict(doc["data"])

    def delete(self, key: K, params: Optional[DeleteParams] = None):
        query = self._key_to_mongo_query(key)
        self.collection.delete_one(query)

    def delete_range(self, start_key: K, end_key: K, params: Optional[DeleteParams] = None):
        s_query = self._key_to_mongo_query(start_key)["_id"]
        e_query = self._key_to_mongo_query(end_key)["_id"]
        
        query = {
            "_id": {
                "$gte": s_query,
                "$lte": e_query
            }
        }
        self.collection.delete_many(query)

    def create_table(self):
        # In MongoDB, collection is implicitly created on first insert,
        # but we can explicitly create it if needed.
        if self.collection_name not in self.db.list_collection_names():
            self.db.create_collection(self.collection_name)

    def drop_table(self):
        self.collection.drop()

    def close(self):
        if hasattr(self, 'client'):
            self.client.close()
