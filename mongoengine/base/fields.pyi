# pyright: reportIncompatibleMethodOverride=warning
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    NoReturn,
    Sequence,
    TypeVar,
)

from bson import ObjectId
from typing_extensions import TypeAlias

from mongoengine.document import Document

__all__ = ["BaseField", "ComplexBaseField", "ObjectIdField", "GeoJsonBaseField"]
_ST = TypeVar("_ST")
_GT = TypeVar("_GT")
_F = TypeVar("_F", bound=BaseField)
_Choice: TypeAlias = str | tuple[str, str]

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

    # BaseField(required=True)
    def __set__(self, instance: Any, value: _ST) -> None: ...
    def __get__(self, instance: Any, owner: Any) -> _GT: ...
    def __init__(
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

class ComplexBaseField(Generic[_F], BaseField[_F, _F]):
    field: _F
    def __init__(self, field: _F | None = None, **kwargs) -> None: ...
    def __set__(self, instance, value): ...
    def __get__(self, instance, owner): ...
    def to_python(self, value): ...
    def to_mongo(
        self, value, use_db_field: bool = True, fields: Sequence[str] | None = None
    ): ...
    def validate(self, value: Any) -> None: ...  # type: ignore[override]
    def prepare_query_value(self, op, value): ...
    def lookup_member(self, member_name): ...

class ObjectIdField(BaseField[ObjectId | str, ObjectId]): ...

class GeoJsonBaseField(BaseField[_ST, _GT]):
    def __init__(self, auto_index: bool = True, *args: Any, **kwargs: Any) -> None: ...
