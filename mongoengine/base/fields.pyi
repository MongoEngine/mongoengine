# pyright: reportIncompatibleMethodOverride=warning
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Literal,
    NoReturn,
    Optional,
    Sequence,
    TypedDict,
    TypeVar,
    Union,
    overload,
)

from bson import ObjectId
from typing_extensions import Self, TypeAlias, Unpack

from mongoengine.document import Document

__all__ = [
    "BaseField",
    "ComplexBaseField",
    "ObjectIdField",
    "GeoJsonBaseField",
    "_no_dereference_for_fields",
]
_ST = TypeVar("_ST")
_GT = TypeVar("_GT")
_F = TypeVar("_F", bound=BaseField)
_Choice: TypeAlias = str | tuple[str, str]
_no_dereference_for_fields: Any

class _BaseFieldOptions(TypedDict, total=False):
    db_field: str
    name: str
    unique: bool
    unique_with: str | Iterable[str]
    primary_key: bool
    choices: Iterable[_Choice]
    null: bool
    verbose_name: str
    help_text: str

class BaseField(Generic[_ST, _GT]):
    name: str
    creation_counter: int
    auto_creation_counter: int
    db_field: str
    required: bool
    default: bool
    unique: bool
    unique_with: str | Iterable[str] | None
    primary_key: bool
    validation: Callable[[Any], None] | None
    choices: Any
    null: bool
    sparse: bool

    _auto_gen: bool

    def __set__(self, instance: Any, value: _ST) -> None: ...
    @overload
    def __get__(self, instance: None, owner: Any) -> Self: ...
    @overload
    def __get__(self, instance: Any, owner: Any) -> _GT: ...
    def __init___(
        self,
        db_field: str | None = None,
        required: bool = False,
        default: Any | None | Callable[[], Any] = None,
        unique: bool = False,
        unique_with: str | Iterable[str] | None = None,
        primary_key: bool = False,
        validation: Callable[[Any], None] | None = None,
        choices: Any = None,
        null: bool = False,
        sparse: bool = False,
        **kwargs: Any,
    ) -> None: ...
    def error(
        self,
        message: str = "",
        errors: dict[str, Any] | None = None,
        field_name: str | None = None,
    ) -> NoReturn: ...
    def to_python(self, value: Any) -> Any: ...
    def to_mongo(self, value: Any) -> Any: ...
    def prepare_query_value(self, op: str, value: Any) -> Any: ...
    def validate(self, value: Any, clean: bool = True) -> None: ...
    @property
    def owner_document(self) -> type[Document]: ...
    @owner_document.setter
    def owner_document(self, owner_document: type[Document]) -> None: ...

class ComplexBaseField(Generic[_F, _ST, _GT], BaseField[_ST, _GT]):
    field: _F
    def to_python(self, value): ...
    def to_mongo(
        self, value, use_db_field: bool = True, fields: Sequence[str] | None = None
    ): ...
    def validate(self, value: Any) -> None: ...  # type: ignore[override]
    def prepare_query_value(self, op, value): ...
    def lookup_member(self, member_name): ...

class ObjectIdField(BaseField[_ST, _GT]):
    # ObjectIdField()
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ObjectIdField[Optional[ObjectId], Optional[ObjectId]]: ...
    # ObjectIdField(default=ObjectId)
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: Union[ObjectId, Callable[[], ObjectId]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ObjectIdField[Optional[ObjectId], ObjectId]: ...
    # ObjectIdField(required=True)
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ObjectIdField[ObjectId, ObjectId]: ...
    # ObjectIdField(required=True, default=ObjectId)
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: Union[ObjectId, Callable[[], ObjectId]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ObjectIdField[Optional[ObjectId], ObjectId]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

class GeoJsonBaseField(BaseField[dict[str, Any], dict[str, Any]]):
    def __init__(
        self, auto_index: bool = True, *args: Any, **kwargs: Unpack[_BaseFieldOptions]
    ) -> None: ...
