import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

class SessionStore(ABC):
    @abstractmethod
    def create_session(self, session_id: str, obj: Any, ttl_seconds: int):
        pass

    @abstractmethod
    def update_session(self, session_id: str, obj: Any):
        pass

    @abstractmethod
    def get_session(self, session_id: str, item: Optional[str] = None) -> Optional[Any]:
        pass

    @abstractmethod
    def delete_session(self, session_id: str):
        pass

    @abstractmethod
    def put(self, session_id: str, item: str, value: Any):
        pass

    @abstractmethod
    def put_array_item(self, session_id: str, array_item: str, index: int, value: Any):
        pass

class InMemorySessionStore(SessionStore):
    def __init__(self):
        self._sessions: Dict[str, Dict[str, Any]] = {}

    def _is_expired(self, session_id: str) -> bool:
        session = self._sessions.get(session_id)
        if not session:
            return True
        
        elapsed = time.time() - session["last_access"]
        if elapsed > session["ttl"]:
            del self._sessions[session_id]
            return True
        return False

    def create_session(self, session_id: str, obj: Any, ttl_seconds: int):
        self._sessions[session_id] = {
            "object": obj,
            "ttl": ttl_seconds,
            "last_access": time.time()
        }

    def update_session(self, session_id: str, obj: Any):
        if not self._is_expired(session_id):
            self._sessions[session_id]["object"] = obj
            self._sessions[session_id]["last_access"] = time.time()

    def get_session(self, session_id: str, item: Optional[str] = None) -> Optional[Any]:
        if self._is_expired(session_id):
            return None
        
        self._sessions[session_id]["last_access"] = time.time()
        obj = self._sessions[session_id]["object"]
        
        if item:
            if isinstance(obj, dict):
                return obj.get(item)
            return None
        return obj

    def delete_session(self, session_id: str):
        if session_id in self._sessions:
            del self._sessions[session_id]

    def put(self, session_id: str, item: str, value: Any):
        if not self._is_expired(session_id):
            obj = self._sessions[session_id]["object"]
            if not isinstance(obj, dict):
                self._sessions[session_id]["object"] = {}
                obj = self._sessions[session_id]["object"]
            
            obj[item] = value
            self._sessions[session_id]["last_access"] = time.time()

    def put_array_item(self, session_id: str, array_item: str, index: int, value: Any):
        if not self._is_expired(session_id):
            obj = self._sessions[session_id]["object"]
            if not isinstance(obj, dict):
                self._sessions[session_id]["object"] = {}
                obj = self._sessions[session_id]["object"]
                
            if array_item not in obj or not isinstance(obj[array_item], list):
                obj[array_item] = []
            
            arr = obj[array_item]
            if index == -1:
                arr.append(value)
            elif 0 <= index < len(arr):
                arr[index] = value
            elif index == len(arr):
                arr.append(value)
            else:
                # Basic handling for out of bounds if not appending
                pass
                
            self._sessions[session_id]["last_access"] = time.time()

class RedisSessionStore(SessionStore):
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, **kwargs):
        import redis
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=False, **kwargs)

    def create_session(self, session_id: str, obj: Any, ttl_seconds: int):
        import pickle
        self.client.delete(session_id)
        self.client.delete(f"ttl:{session_id}")
        
        if isinstance(obj, dict):
            if obj:
                mapping = {k: pickle.dumps(v) for k, v in obj.items()}
                self.client.hset(session_id, mapping=mapping)
        else:
            # If not a dict, wrap it in a default field
            self.client.hset(session_id, "data", pickle.dumps(obj))
            
        self.client.setex(f"ttl:{session_id}", ttl_seconds, str(ttl_seconds))
        self.client.expire(session_id, ttl_seconds)

    def update_session(self, session_id: str, obj: Any):
        # Refresh TTL and replace the whole hash
        ttl = self.client.get(f"ttl:{session_id}")
        if ttl:
            ttl_val = int(ttl)
            self.create_session(session_id, obj, ttl_val)

    def get_session(self, session_id: str, item: Optional[str] = None) -> Optional[Any]:
        import pickle
        # Refresh TTL
        ttl = self.client.get(f"ttl:{session_id}")
        if not ttl:
            return None
        
        ttl_val = int(ttl)
        self.client.expire(session_id, ttl_val)
        self.client.expire(f"ttl:{session_id}", ttl_val)
        
        if item:
            data = self.client.hget(session_id, item)
            return pickle.loads(data) if data else None
        else:
            data = self.client.hgetall(session_id)
            if not data:
                return {}
            return {k.decode() if isinstance(k, bytes) else k: pickle.loads(v) for k, v in data.items()}

    def delete_session(self, session_id: str):
        self.client.delete(session_id)
        self.client.delete(f"ttl:{session_id}")

    def put(self, session_id: str, item: str, value: Any):
        import pickle
        ttl = self.client.get(f"ttl:{session_id}")
        if ttl:
            ttl_val = int(ttl)
            self.client.hset(session_id, item, pickle.dumps(value))
            self.client.expire(session_id, ttl_val)
            self.client.expire(f"ttl:{session_id}", ttl_val)

    def put_array_item(self, session_id: str, array_item: str, index: int, value: Any):
        import pickle
        ttl = self.client.get(f"ttl:{session_id}")
        if ttl:
            ttl_val = int(ttl)
            data = self.client.hget(session_id, array_item)
            arr = pickle.loads(data) if data else []
            if not isinstance(arr, list):
                arr = []
            
            if index == -1:
                arr.append(value)
            elif 0 <= index < len(arr):
                arr[index] = value
            elif index == len(arr):
                arr.append(value)
                
            self.client.hset(session_id, array_item, pickle.dumps(arr))
            self.client.expire(session_id, ttl_val)
            self.client.expire(f"ttl:{session_id}", ttl_val)

def get_session_store(store_type: str = "memory", **kwargs) -> SessionStore:
    if store_type == "memory":
        return InMemorySessionStore()
    elif store_type == "redis":
        return RedisSessionStore(**kwargs)
    else:
        raise ValueError(f"Unknown store type: {store_type}")
