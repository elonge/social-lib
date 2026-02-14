from pydantic import BaseModel
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Annotated
from document_store import PartitionKey, SortKey, CopyOfKey

@dataclass(frozen=True)
class RawLibraryBookKey:
    user_id: Annotated[str, PartitionKey]
    frame_id: Annotated[int, SortKey]

@dataclass
class RawLibraryBookEntry:
    key: Annotated[RawLibraryBookKey, CopyOfKey]
    shelf: Optional[str] = None
    books: List[Dict[str, Any]] = field(default_factory=list)

@dataclass(frozen=True)
class LibraryBookKey:
    user_id: Annotated[str, PartitionKey(order=1)]
    library_id: Annotated[str, PartitionKey(order=2)]
    shelf: Annotated[str, SortKey(order=1)]
    book_id: Annotated[str, SortKey(order=2)] # ISBN or title|author

@dataclass
class LibraryBook:
    key: Annotated[LibraryBookKey, CopyOfKey]
    title: str
    author: Optional[str] = None
    isbn: Optional[str] = None
    frame_ids: List[int] = field(default_factory=list)
    copies: int = 1

@dataclass(frozen=True)
class ShelfFrameMetadataKey:
    user_id: Annotated[str, PartitionKey(order=1)]
    library_id: Annotated[str, SortKey(order=1)]
    frame_id: Annotated[int, SortKey(order=2)]

@dataclass
class ShelfFrameMetadata:
    key: Annotated[ShelfFrameMetadataKey, CopyOfKey]
    shelf: str
    book_count: int
    uploaded_at: float

@dataclass(frozen=True)
class LibraryKey:
    user_id: Annotated[str, PartitionKey]
    library_id: Annotated[str, SortKey]

@dataclass
class Library:
    key: Annotated[LibraryKey, CopyOfKey]
    name: str
    created_at: float
    
@dataclass(frozen=True)
class ShelfKey:
    user_id: Annotated[str, PartitionKey]
    library_id: Annotated[str, SortKey]
    shelf: Annotated[str, SortKey]

@dataclass
class Shelf:
    key: Annotated[ShelfKey, CopyOfKey]
    name: str
    created_at: float
    book_count: int = 0
    position: Optional[str] = None
    room: Optional[str] = None
    case: Optional[int] = None
    row: Optional[int] = None
    col: Optional[int] = None
