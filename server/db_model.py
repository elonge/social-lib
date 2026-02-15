from pydantic import BaseModel
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Annotated, Tuple
from document_store import PartitionKey, SortKey, CopyOfKey

@dataclass(frozen=True)
class UserFrameUploadKey:
    user_id: Annotated[str, PartitionKey]
    session_id: Annotated[str, SortKey]
    frame_id: Annotated[int, SortKey]

@dataclass
class UserBook:
    title: str
    author: Optional[str] = None
    publisher: Optional[str] = None
    year: Optional[str] = None
    language: Optional[str] = None
    other_text: Optional[str] = None
    pos: Optional[Tuple[int, int]] = None
    angle: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    isbn: Optional[str] = None
    subjects: List[str] = field(default_factory=list)
    description: Optional[str] = None
    cover_link: Optional[str] = None
    count: int = 1

@dataclass
class UserFrameUploadEntry:
    key: Annotated[UserFrameUploadKey, CopyOfKey]
    shelf: Optional[str] = None
    library_id: Optional[str] = None
    books: List[Dict[str, Any]] = field(default_factory=list)

@dataclass(frozen=True)
class UserLibraryBookKey:
    user_id: Annotated[str, PartitionKey(order=1)]
    library_id: Annotated[str, PartitionKey(order=2)]
    shelf: Annotated[str, SortKey(order=1)]
    book_id: Annotated[str, SortKey(order=2)] # ISBN or title|author

@dataclass
class UserLibraryBook(UserBook):
    key: Annotated[UserLibraryBookKey, CopyOfKey] = None
    frame_ids: List[int] = field(default_factory=list)
    copies: int = 1

@dataclass(frozen=True)
class UserShelfFrameMetadataKey:
    user_id: Annotated[str, PartitionKey(order=1)]
    library_id: Annotated[str, SortKey(order=1)]
    frame_id: Annotated[int, SortKey(order=2)]

@dataclass
class UserShelfFrameMetadata:
    key: Annotated[UserShelfFrameMetadataKey, CopyOfKey]
    shelf: str
    book_count: int
    uploaded_at: float

@dataclass(frozen=True)
class UserLibraryKey:
    user_id: Annotated[str, PartitionKey]
    library_id: Annotated[str, SortKey]

@dataclass
class UserLibrary:
    key: Annotated[UserLibraryKey, CopyOfKey]
    name: str
    created_at: float
    
@dataclass(frozen=True)
class UserShelfKey:
    user_id: Annotated[str, PartitionKey]
    library_id: Annotated[str, SortKey]
    shelf: Annotated[str, SortKey]

@dataclass
class UserShelf:
    key: Annotated[UserShelfKey, CopyOfKey]
    name: str
    created_at: float
    book_count: int = 0
    position: Optional[str] = None
    room: Optional[str] = None
    case: Optional[int] = None
    row: Optional[int] = None
    col: Optional[int] = None
