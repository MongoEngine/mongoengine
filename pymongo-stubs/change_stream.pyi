from typing import Any, Dict, List, Optional

from bson.timestamp import Timestamp
from typing_extensions import Literal, TypedDict

class _Document(TypedDict):
    _data: str

class _DocumentKey(TypedDict):
    _id: str

class _Ns(TypedDict):
    coll: str
    db: str

_FullDocument = Dict[str, Any]

class _UpdateDescription(TypedDict):
    updatedFields: Dict[str, Any]
    removedFields: List[str]

# see: https://github.com/mongodb/specifications/blob/6b46a43de5bec233d071a6ff8ebaa326f2b306a3/source/change-streams/change-streams.rst#response-format
class _Event(TypedDict):
    _id: _Document
    documentKey: _DocumentKey
    fullDocument: Optional[_FullDocument]
    ns: _Ns
    updateDescription: _UpdateDescription
    clusterTime: Timestamp
    operationType: Literal[
        "insert",
        "update",
        "replace",
        "delete",
        "invalidate",
        "drop",
        "dropDatabase",
        "rename",
    ]

class ChangeStream:
    def __iter__(self) -> ChangeStream: ...
    def next(self) -> _Event: ...
    def close(self) -> None: ...
    def __next__(self) -> _Event: ...
    def __enter__(self) -> ChangeStream: ...
    def __exit__(self, exc_type: Any, exc_value: Any, exc_tb: Any) -> None: ...

class CollectionChangeStream(ChangeStream): ...
