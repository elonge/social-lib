from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Tuple, Union, TypeVar, Generic, Callable, Annotated, get_type_hints, get_args, get_origin
from dataclasses import dataclass, field, make_dataclass
import datetime
from pymongo import MongoClient

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
    limit: Optional[int] = None
    include_from: Optional[int] = None
    include_to: Optional[int] = None

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

@dataclass
class CopyOfKey:
    """Marker for fields that are copies of the key and should not be serialized."""
    pass

K = TypeVar('K')
V = TypeVar('V')

def default_to_dict(obj: Any) -> Dict[str, Any]:
    """
    Very simple reflection helper to convert an object to a dict.
    Excludes fields annotated with CopyOfKey.
    """
    if hasattr(obj, "__dict__"):
        result = {}
        # Check for CopyOfKey annotations
        try:
            hints = get_type_hints(type(obj), include_extras=True)
            copy_of_key_fields = set()
            for name, hint in hints.items():
                if get_origin(hint) is Annotated:
                    metadata = get_args(hint)[1:]
                    for m in metadata:
                        if m is CopyOfKey or isinstance(m, CopyOfKey):
                            copy_of_key_fields.add(name)
                            break
            
            # Build dict excluding CopyOfKey fields and private fields
            for k, v in obj.__dict__.items():
                if not v:
                    continue
                if not k.startswith("_") and k not in copy_of_key_fields:
                    result[k] = v
            return result
        except Exception:
            # Fallback to simple filtering if type hints fail
            return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    if isinstance(obj, dict):
        return obj
    return {"value": obj}

class DocumentStore(Generic[K, V], ABC):
    def __init__(
        self, 
        key_attrs: Optional[List[str]] = None,
        partition_attrs: Optional[List[str]] = None,
        sort_attrs: Optional[List[str]] = None,
        key_type: Optional[type] = None,
        data_type: Optional[type] = None,
        to_dict_fn: Callable[[V], Dict[str, Any]] = default_to_dict,
        from_dict_fn: Optional[Callable[[Dict[str, Any]], V]] = None
    ):
        self._key_attrs = key_attrs
        self._partition_attrs = partition_attrs
        self._sort_attrs = sort_attrs
        self.key_type = key_type
        self.data_type = data_type
        self._to_dict_fn = to_dict_fn
        self._from_dict_fn = from_dict_fn

    def _discover_attrs_for(self, cls: type) -> Tuple[List[str], List[str], List[str]]:
        """
        Discovers partition and sort attributes for any class, respecting specified order.
        Returns (all_attrs, partition_attrs, sort_attrs)
        """
        p_attrs = []
        s_attrs = []
        try:
            hints = get_type_hints(cls, include_extras=True)
            for name, hint in hints.items():
                if get_origin(hint) is Annotated:
                    metadata = get_args(hint)[1:]
                    for m in metadata:
                        if m is PartitionKey or isinstance(m, PartitionKey):
                            order = m.order if isinstance(m, PartitionKey) else 0
                            p_attrs.append((order, name))
                        if m is SortKey or isinstance(m, SortKey):
                            order = m.order if isinstance(m, SortKey) else 0
                            s_attrs.append((order, name))
        except Exception as e:
            print(f"Warning: Failed to discover attributes for {cls}: {e}")
        
        # Sort by order, then by name (stable sort)
        p_attrs.sort(key=lambda x: x[0])
        s_attrs.sort(key=lambda x: x[0])
        
        partition_names = [a[1] for a in p_attrs]
        sort_names = [a[1] for a in s_attrs]
        all_names = partition_names + sort_names
        
        return all_names, partition_names, sort_names

    def _discover_attrs(self, cls: type):
        """
        Discovers key attributes by looking at class annotations for K.
        """
        if not self._key_attrs:
            all_a, p_a, s_a = self._discover_attrs_for(cls)
            self._key_attrs = all_a
            self._partition_attrs = p_a
            self._sort_attrs = s_a

    def _get_key_tuple(self, obj: Any, attrs: Optional[List[str]] = None) -> Tuple[Any, ...]:
        # If attrs are not provided, use instance defaults
        key_attrs = attrs if attrs is not None else self._key_attrs

        # Discovery if needed
        if not key_attrs:
            # If we don't have attrs, and we can't discover from obj (e.g. primitive), we assume it's a simple key
            if isinstance(obj, (str, int, float, bool, datetime.date, tuple)):
                 return obj if isinstance(obj, tuple) else (obj,)
            
            # Try discovery
            all_a, p_a, s_a = self._discover_attrs_for(type(obj))
            key_attrs = key_attrs if key_attrs else all_a
            
        # Safe defaults
        key_attrs = key_attrs or []

        if isinstance(obj, (str, int, float, bool, datetime.date, tuple)):
            return obj if isinstance(obj, tuple) else (obj,)

        d = default_to_dict(obj) if not isinstance(obj, dict) else obj
        
        return tuple(d.get(attr) for attr in key_attrs)

    def _get_partition_key_tuple(self, obj: Any) -> Tuple[Any, ...]:
        if not self._partition_attrs:
            self._discover_attrs(type(obj))
            
        if self._partition_attrs:
             return self._get_key_tuple(obj, self._partition_attrs)
        
        # Fallback if no specific partition attrs: treat whole key as partition?
        # Or if generic tuple, first element? 
        # For MongoEmbedded, we really expect partition attrs definition or usage of Key class.
        return self._get_key_tuple(obj)

    def _get_sort_key_tuple(self, obj: Any) -> Tuple[Any, ...]:
        if not self._sort_attrs:
            self._discover_attrs(type(obj)) # Ensure discovered
            
        if self._sort_attrs:
             return self._get_key_tuple(obj, self._sort_attrs)
        return ()
    
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
    
    def _get_copy_of_key_field(self) -> Optional[str]:
        """Returns the name of the field annotated with CopyOfKey, if any."""
        if not self.data_type:
            return None
        
        try:
            hints = get_type_hints(self.data_type, include_extras=True)
            for name, hint in hints.items():
                if get_origin(hint) is Annotated:
                    metadata = get_args(hint)[1:]
                    for m in metadata:
                        if m is CopyOfKey or isinstance(m, CopyOfKey):
                            return name
        except Exception:
            pass
        return None
    
    def _populate_copy_of_key_field(self, obj: V, key: K) -> V:
        """Populates the CopyOfKey field on obj with the given key, if such a field exists."""
        copy_of_key_field = self._get_copy_of_key_field()
        if copy_of_key_field and hasattr(obj, copy_of_key_field):
            # Set the field - works for both dataclasses and regular classes
            try:
                object.__setattr__(obj, copy_of_key_field, key)
            except Exception:
                pass
        return obj

    def _from_data_dict(self, data: Dict[str, Any], key: Optional[K] = None) -> V:
        """
        Convert dict to V object. If key is provided and V has a CopyOfKey field,
        populate it with the key.
        """
        obj: V
        if self._from_dict_fn:
            obj = self._from_dict_fn(data)
        elif self.data_type:
             # If there's a CopyOfKey field, add it to data for instantiation
             copy_of_key_field = self._get_copy_of_key_field()
             data_for_init = data.copy()
             if copy_of_key_field and copy_of_key_field not in data_for_init:
                 data_for_init[copy_of_key_field] = None
             
             try:
                 obj = self.data_type(**data_for_init)
             except Exception as e:
                 # If instantiation fails, return the dict as fallback
                 obj = data  # type: ignore
        else:
            obj = data  # type: ignore
        
        # Populate CopyOfKey field with actual key if provided
        if key is not None and not isinstance(obj, dict):
            obj = self._populate_copy_of_key_field(obj, key)
        
        return obj

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
        return self._from_data_dict(data, key) if data is not None else None

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
                results.append((k, self._from_data_dict(self._storage[k_tuple], k))) # type: ignore
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
        idx_attrs, _, _ = self._discover_attrs_for(type(index_key))
        idx_tuple = self._get_key_tuple(index_key, idx_attrs)
        results = []
        for data_dict in self._storage.values():
            if self._match_index(data_dict, idx_tuple, idx_attrs):
                results.append(self._from_data_dict(data_dict, None))
        return results

    def get_by_index_range(self, start_index: Any, end_index: Any, params: Optional[GetParams] = None) -> List[V]:
        idx_attrs, _, _ = self._discover_attrs_for(type(start_index))
        s_tuple = self._get_key_tuple(start_index, idx_attrs)
        e_tuple = self._get_key_tuple(end_index, idx_attrs)
        
        all_matches = []
        for data_dict in self._storage.values():
            obj_vals = tuple(data_dict.get(a) for a in idx_attrs)
            
            if s_tuple <= obj_vals <= e_tuple:
                all_matches.append((obj_vals, self._from_data_dict(data_dict, None)))
        
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
    def __init__(self, client: MongoClient, database_name: str, collection_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
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
        return self._from_data_dict(doc["data"], key) if doc else None

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
                results[original_k] = self._from_data_dict(doc["data"], original_k)
            else:
                k = self._reconstruct_key(vals)
                results[k] = self._from_data_dict(doc["data"], k)
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
            results.append((k, self._from_data_dict(doc["data"], k)))
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
            yield (k, self._from_data_dict(doc["data"], k))

    def _index_to_mongo_query(self, vals: Tuple[Any, ...], attrs: List[str]) -> Dict[str, Any]:
        query = {}
        for i, attr in enumerate(attrs):
            query[f"data.{attr}"] = vals[i]
        return query

    def get_by_index(self, index_key: Any, params: Optional[GetParams] = None) -> List[V]:
        all_a, _, _ = self._discover_attrs_for(type(index_key))
        vals = self._get_key_tuple(index_key, all_a)
        query = self._index_to_mongo_query(vals, all_a)
        # In a real app we'd want an index for sorting too
        cursor = self.collection.find(query)
        return [self._from_data_dict(doc["data"], None) for doc in cursor]

    def _get_index_range_cursor(self, start_index: Any, end_index: Any, params: Optional[GetParams] = None):
        attrs, _, _ = self._discover_attrs_for(type(start_index))
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
                results.append(self._from_data_dict(doc["data"], None))
        return results

    def get_index_range_iterator(self, start_index: Any, end_index: Any, params: Optional[GetParams] = None):
        cursor, attrs, s_vals, e_vals = self._get_index_range_cursor(start_index, end_index, params)
        for doc in cursor:
            obj_vals = tuple(doc["data"].get(a) for a in attrs)
            if s_vals <= obj_vals <= e_vals:
                yield self._from_data_dict(doc["data"], None)

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


class MongoEmbeddedDocumentStore(DocumentStore[K, V]):
    """
    Stores data in a list 'items' within a document identified by partition key.
    Each item in the list has 'sk' (sort key) and 'd' (data).
    """
    def __init__(self, client: MongoClient, database_name: str, collection_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]
        self.collection_name = collection_name

    def _key_to_mongo_pk_query(self, key: K) -> Dict[str, Any]:
        # Uses only partition key attributes for the document _id
        if not self._partition_attrs:
            self._discover_attrs(type(key))

        vals = self._get_partition_key_tuple(key)
        
        # 1. Mapped attributes
        if self._partition_attrs and len(self._partition_attrs) == len(vals):
             return {"_id": {attr: val for attr, val in zip(self._partition_attrs, vals)}}
        
        # 2. Single generic value
        if len(vals) == 1:
             return {"_id": vals[0]}

        # 3. Tuple wrapper
        return {"_id": {"v": list(vals)}}

    def _get_sort_key_value(self, key: K) -> Any:
        vals = self._get_sort_key_tuple(key)
        # Sort key storage format
        if self._sort_attrs and len(self._sort_attrs) == len(vals):
             return {attr: val for attr, val in zip(self._sort_attrs, vals)}
        if len(vals) == 1:
             return vals[0]
        return {"v": list(vals)}

    def put(self, key: K, data: V, params: Optional[PutParams] = None):
        pk_query = self._key_to_mongo_pk_query(key)
        sk_val = self._get_sort_key_value(key)
        data_dict = self._to_data_dict(data)
        
        # Try to update existing item in list
        # We match document by PK and item by SK
        query = pk_query.copy()
        query["items.sk"] = sk_val
        
        update = {"$set": {"items.$.d": data_dict}}
        
        result = self.collection.update_one(query, update)
        
        if result.matched_count == 0:
            push_update = {
                "$push": {"items": {"sk": sk_val, "d": data_dict}}
            }
            self.collection.update_one(pk_query, push_update, upsert=True)

    def batch_put(self, items: Dict[K, V], params: Optional[PutParams] = None):
        # Naive implementation loop
        for k, v in items.items():
            self.put(k, v, params)

    def get(self, key: K, params: Optional[GetParams] = None) -> Optional[V]:
        pk_query = self._key_to_mongo_pk_query(key)
        sk_val = self._get_sort_key_value(key)
        
        query = pk_query.copy()
        query["items.sk"] = sk_val
        
        # Project only the matching item
        doc = self.collection.find_one(query, {"items.$": 1})
        
        if doc and "items" in doc and doc["items"]:
            return self._from_data_dict(doc["items"][0]["d"], key)
        return None

    def batch_get(self, keys: Set[K], params: Optional[GetParams] = None) -> Dict[K, V]:
        results = {}
        for k in keys:
            val = self.get(k)
            if val:
                results[k] = val
        return results

    def get_range(self, start_key: K, end_key: K, params: Optional[GetParams] = None) -> List[Tuple[K, V]]:
        pk_query = self._key_to_mongo_pk_query(start_key)
        
        doc = self.collection.find_one(pk_query)
        if not doc or "items" not in doc:
             return []
        
        start_sk = self._get_sort_key_value(start_key)
        end_sk = self._get_sort_key_value(end_key)
        
        items = []
        for item in doc["items"]:
            sk = item["sk"]
            
            # Simple comparison for now. 
            # Note: This assumes sk structure is comparable and consistent with range queries.
            # Dict comparison in Python is okay if keys match order.
            match = False
            try:
                if start_sk <= sk <= end_sk:
                    match = True
            except TypeError:
                # Fallback or error if types differ
                pass
                
            if match:
                 try:
                     # Reconstruct Full Key
                     # Assuming Key construction takes ALL fields in order (PK then SK)
                     
                     pk_tuple = self._get_partition_key_tuple(start_key)
                     
                     if isinstance(sk, dict) and "v" in sk and isinstance(sk["v"], list):
                         sk_tuple = tuple(sk["v"])
                     elif isinstance(sk, dict):
                         if self._sort_attrs:
                             sk_tuple = tuple(sk.get(a) for a in self._sort_attrs)
                         else:
                             sk_tuple = tuple(sk.values())
                     elif isinstance(sk, list):
                         sk_tuple = tuple(sk)
                     else:
                         sk_tuple = (sk,)
                         
                     full_tuple = pk_tuple + sk_tuple
                     
                     k = self._reconstruct_key(full_tuple)
                     items.append((k, self._from_data_dict(item["d"], k)))
                 except Exception:
                     continue
        
        # Sort in memory
        try:
             # Sort by the key object itself if comparable, or try to sort by SK value?
             # If K is a dataclass(frozen=True), it is orderable if order=True.
             # We assume K is orderable.
             items.sort(key=lambda x: x[0], reverse=params.reverse if params else False) # type: ignore
        except Exception:
             pass

        return items

    def get_range_iterator(self, start_key: K, end_key: K, params: Optional[GetParams] = None):
        items = self.get_range(start_key, end_key, params)
        for item in items:
            yield item

    def get_by_index(self, index_key: Any, params: Optional[GetParams] = None) -> List[V]:
        idx_attrs, _, _ = self._discover_attrs_for(type(index_key))
        vals = self._get_key_tuple(index_key, idx_attrs)
        
        # Query for documents containing matching items
        query = {}
        for i, attr in enumerate(idx_attrs):
            query[f"items.d.{attr}"] = vals[i]
            
        cursor = self.collection.find(query)
        
        results = []
        for doc in cursor:
            if "items" in doc:
                for item in doc["items"]:
                    data = item["d"]
                    # Check match in memory
                    match = True
                    for i, attr in enumerate(idx_attrs):
                        if data.get(attr) != vals[i]:
                            match = False
                            break
                    if match:
                        results.append(self._from_data_dict(data, None))
        return results

    def get_by_index_range(self, start_index: Any, end_index: Any, params: Optional[GetParams] = None) -> List[V]:
        idx_attrs, _, _ = self._discover_attrs_for(type(start_index))
        s_vals = self._get_key_tuple(start_index, idx_attrs)
        e_vals = self._get_key_tuple(end_index, idx_attrs)
        
        # MongoDB query to narrow down documents (optional optimization, but helps)
        # We can range query on the first attribute of the index
        first_attr = idx_attrs[0] if idx_attrs else None
        query = {}
        if first_attr:
             query[f"items.d.{first_attr}"] = {
                 "$gte": s_vals[0],
                 "$lte": e_vals[0]
             }
        
        cursor = self.collection.find(query)
        
        results = []
        for doc in cursor:
            if "items" in doc:
                for item in doc["items"]:
                    data = item["d"]
                    # Construct value tuple for comparison
                    obj_vals = tuple(data.get(a) for a in idx_attrs)
                    
                    if s_vals <= obj_vals <= e_vals:
                         results.append((obj_vals, self._from_data_dict(data, None)))

        # Sort by index tuple
        results.sort(key=lambda x: x[0], reverse=params.reverse if params else False) # type: ignore
        return [r[1] for r in results]

    def get_index_range_iterator(self, start_index: Any, end_index: Any, params: Optional[GetParams] = None):
        items = self.get_by_index_range(start_index, end_index, params)
        for item in items:
            yield item

    def batch_put(self, items: Dict[K, V], params: Optional[PutParams] = None):
        # Naive implementation loop
        for k, v in items.items():
            self.put(k, v, params)

    def get(self, key: K, params: Optional[GetParams] = None) -> Optional[V]:
        pk_query = self._key_to_mongo_pk_query(key)
        sk_val = self._get_sort_key_value(key)
        
        query = pk_query.copy()
        query["items.sk"] = sk_val
        
        # Project only the matching item
        doc = self.collection.find_one(query, {"items.$": 1})
        
        if doc and "items" in doc and doc["items"]:
            return self._from_data_dict(doc["items"][0]["d"], key)
        return None

    def batch_get(self, keys: Set[K], params: Optional[GetParams] = None) -> Dict[K, V]:
        results = {}
        for k in keys:
            val = self.get(k)
            if val:
                results[k] = val
        return results

    def get_range(self, start_key: K, end_key: K, params: Optional[GetParams] = None) -> List[Tuple[K, V]]:
        pk_query = self._key_to_mongo_pk_query(start_key)
        
        doc = self.collection.find_one(pk_query)
        if not doc or "items" not in doc:
             return []
        
        start_sk = self._get_sort_key_tuple(start_key)
        end_sk = self._get_sort_key_tuple(end_key)
        
        items = []
        for item in doc["items"]:
            sk = self._get_sort_key_tuple(item["sk"])
            
            # Simple comparison for now. 
            # Note: This assumes sk structure is comparable and consistent with range queries.
            # Dict comparison in Python is okay if keys match order.
            match = False
            try:
                if start_sk <= sk <= end_sk:
                    match = True
            except TypeError:
                # Fallback or error if types differ
                pass
                
            if match:
                 try:
                     # Reconstruct Full Key
                     # Assuming Key construction takes ALL fields in order (PK then SK)
                     
                     pk_tuple = self._get_partition_key_tuple(start_key)
                     
                     if isinstance(sk, dict) and "v" in sk and isinstance(sk["v"], list):
                         sk_tuple = tuple(sk["v"])
                     elif isinstance(sk, dict):
                         if self._sort_attrs:
                             sk_tuple = tuple(sk.get(a) for a in self._sort_attrs)
                         else:
                             sk_tuple = tuple(sk.values())
                     elif isinstance(sk, list):
                         sk_tuple = tuple(sk)
                     else:
                         sk_tuple = (sk,)
                         
                     full_tuple = pk_tuple + sk_tuple
                     
                     k = self._reconstruct_key(full_tuple)
                     items.append((k, self._from_data_dict(item["d"], k)))
                 except Exception:
                     continue
        
        # Sort in memory by sort key tuple (guaranteed comparable)
        try:
             items.sort(key=lambda x: self._get_sort_key_tuple(x[0]), reverse=params.reverse if params else False)
        except Exception as e:
             # Fallback: try sorting by the full key
             try:
                 items.sort(key=lambda x: x[0], reverse=params.reverse if params else False) # type: ignore
             except Exception:
                 pass

        return items

    def get_range_iterator(self, start_key: K, end_key: K, params: Optional[GetParams] = None):
        items = self.get_range(start_key, end_key, params)
        for item in items:
            yield item

    def delete(self, key: K, params: Optional[DeleteParams] = None):
        pk_query = self._key_to_mongo_pk_query(key)
        sk_val = self._get_sort_key_value(key)
        
        update = {"$pull": {"items": {"sk": sk_val}}}
        self.collection.update_one(pk_query, update)

    def delete_range(self, start_key: K, end_key: K, params: Optional[DeleteParams] = None):
        pk_query = self._key_to_mongo_pk_query(start_key)
        start_sk = self._get_sort_key_value(start_key)
        end_sk = self._get_sort_key_value(end_key)
        
        condition = {
            "sk": {
                "$gte": start_sk,
                "$lte": end_sk
            }
        }
        
        update = {
            "$pull": {
                "items": condition
            }
        }
        self.collection.update_one(pk_query, update)

    def create_table(self):
        if self.collection_name not in self.db.list_collection_names():
            self.db.create_collection(self.collection_name)

    def drop_table(self):
        self.collection.drop()

    def close(self):
        if hasattr(self, 'client'):
            self.client.close()
