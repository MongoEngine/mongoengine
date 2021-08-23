from typing import (
    Any,
    Dict,
    Generic,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from bson import ObjectId
from mongoengine import Document
from mongoengine.queryset.visitor import Q
from pymongo.collation import Collation
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.read_preferences import _ServerMode
from typing_extensions import Literal, TypedDict

_T = TypeVar("_T")

_U = TypeVar("_U", bound="QuerySet[Any]")

_ReadWriteConcern = Mapping[str, Union[str, int, bool]]

class _ExecutionStats(TypedDict):
    allPlansExecution: List[Any]
    executionStages: Dict[str, Any]
    executionSuccess: bool
    executionTimeMillis: int
    nReturned: int
    totalDocsExamined: int
    totalKeysExamined: int

class _QueryPlanner(TypedDict):
    indexFilterSet: bool
    namespace: str
    parsedQuery: Dict[str, Any]
    plannerVersion: int
    rejectedPlans: List[Any]
    winningPlan: Dict[str, Any]

class _ServerInfo(TypedDict):
    gitVersion: str
    host: str
    port: int
    version: str

class _ExplainCursor(TypedDict):
    executionStats: _ExecutionStats
    ok: float
    queryPlanner: _QueryPlanner
    serverInfo: _ServerInfo

_Hint = Union[str, List[Tuple[str, Literal[-1, 1]]]]
_V = TypeVar("_V", bound="Document")

class QuerySet(Generic[_T]):
    _document: Type[_T]
    _collection: Collection

    _cursor: Cursor

    _query: Dict[str, Any]
    def __init__(self, document: Type[_T], collection: Collection) -> None: ...
    def _clone_into(self, new_qs: _U) -> _U: ...
    def first(self) -> Optional[_T]: ...
    def get(self, *q_objs: Q, **query: object) -> _T: ...
    @overload
    def insert(
        self: QuerySet[_V],
        doc_or_docs: _V,
        load_bulk: Literal[False],
        write_concern: Optional[_ReadWriteConcern] = ...,
        signal_kwargs: Optional[Any] = ...,
    ) -> ObjectId: ...
    @overload
    def insert(
        self: QuerySet[_V],
        doc_or_docs: _V,
        load_bulk: Literal[True] = ...,
        write_concern: Optional[_ReadWriteConcern] = ...,
        signal_kwargs: Optional[Any] = ...,
    ) -> _V: ...
    @overload
    def insert(
        self: QuerySet[_V],
        doc_or_docs: List[_V],
        load_bulk: Literal[False],
        write_concern: Optional[_ReadWriteConcern] = ...,
        signal_kwargs: Optional[Any] = ...,
    ) -> List[ObjectId]: ...
    @overload
    def insert(
        self: QuerySet[_V],
        doc_or_docs: List[_V],
        load_bulk: Literal[True] = ...,
        write_concern: Optional[_ReadWriteConcern] = ...,
        signal_kwargs: Optional[Any] = ...,
    ) -> List[_V]: ...
    def values(self, *args: str) -> Dict[str, Any]: ...
    def as_pymongo(self) -> QuerySet[Dict[str, Any]]: ...
    def scalar(self, *fields: str) -> List[Any]: ...
    def values_list(self, *args: str) -> List[Any]: ...
    def update(self, **update: Any) -> int: ...
    def update_one(self, **update: Any) -> int: ...
    def upsert_one(self, **update: Any) -> _T: ...
    def delete(self) -> Optional[int]: ...
    def filter(self: _U, *args: Q, **kwargs: object) -> _U: ...
    def search_text(self: _U, text: str, language: Optional[str] = ...) -> _U: ...
    def none(self: _U) -> _U: ...
    def read_preference(self: _U, read_preference: _ServerMode) -> _U: ...
    def rewind(self: _U) -> _U: ...
    def only(self: _U, *fields: str) -> _U: ...
    def exclude(self: _U, *args: str) -> _U: ...
    def exec_js(self: _U, code: str, *fields: str, **options: Any) -> _U: ...
    def explain(self) -> _ExplainCursor: ...
    def all_fields(self: _U) -> _U: ...
    def average(self: _U, field: str) -> _U: ...
    def batch_size(self: _U, size: int) -> _U: ...
    def clone(self: _U) -> _U: ...
    def collation(self: _U, collation: Optional[Collation] = ...) -> _U: ...
    def comment(self: _U, text: str) -> _U: ...
    def order_by(self: _U, *keys: str) -> _U: ...
    def skip(self: _U, n: Optional[int]) -> _U: ...
    def hint(self: _U, index: Optional[_Hint] = ...) -> _U: ...
    def sum(self, field: str) -> float: ...
    def limit(self: _U, n: int) -> _U: ...
    def count(self, with_limit_and_skip: bool = ...) -> int: ...
    def __iter__(self) -> Iterator[_T]: ...
    def __next__(self) -> _T: ...
    def all(self: _U) -> _U: ...
    def timeout(self: _U, enabled: bool) -> _U: ...
    def aggregate(
        self,
        *args: object,
        allowDiskUse: bool = ...,
        hint: Optional[Dict[str, int]] = ...,
    ) -> CommandCursor: ...
    def max_time_ms(self: _U, ms: Optional[int]) -> _U: ...
    def to_json(self) -> str: ...
    def from_json(self: _U, json_data: str) -> List[_U]: ...
    def modify(
        self: _U,
        upsert: bool = ...,
        full_response: bool = ...,
        remove: bool = ...,
        new: bool = ...,
        **update: object,
    ) -> _U: ...
    def no_cache(self: _U) -> _U: ...
    def no_dereference(self: _U) -> _U: ...
    def create(self, **kwargs: object) -> _T: ...
    def distinct(self, field: str) -> List[Any]: ...
    @overload
    def __getitem__(self: _U, key: slice) -> _U: ...
    @overload
    def __getitem__(self, key: int) -> _T: ...
    def fields(self: _U, _only_called: bool = ..., **kwargs: int) -> _U: ...
    def __len__(self) -> int: ...
    def __call__(self: _U, *args: Q, **kwargs: object) -> _U: ...
