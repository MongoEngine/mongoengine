# pyright: reportIncompatibleMethodOverride=false
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import (
    Any,
    Callable,
    Container,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Pattern,
    Tuple,
    Type,
    TypedDict,
    TypeVar,
    Union,
    overload,
)
from uuid import UUID

from bson.objectid import ObjectId
from typing_extensions import Literal, TypeAlias, Unpack

from mongoengine.base import BaseField, ComplexBaseField
from mongoengine.base.fields import _F, _GT, _ST
from mongoengine.document import Document

_T = TypeVar("_T")
_Choice: TypeAlias = str | tuple[str, str]
__all__ = (
    "StringField",
    "URLField",
    "EmailField",
    "IntField",
    "LongField",
    "FloatField",
    "DecimalField",
    "BooleanField",
    "DateTimeField",
    "DateField",
    "ComplexDateTimeField",
    "EmbeddedDocumentField",
    "ObjectIdField",
    "GenericEmbeddedDocumentField",
    "DynamicField",
    "ListField",
    "SortedListField",
    "EmbeddedDocumentListField",
    "DictField",
    "MapField",
    "ReferenceField",
    "CachedReferenceField",
    "LazyReferenceField",
    "GenericLazyReferenceField",
    "GenericReferenceField",
    "BinaryField",
    "GridFSError",
    "GridFSProxy",
    "FileField",
    "ImageGridFsProxy",
    "ImproperlyConfigured",
    "ImageField",
    "GeoPointField",
    "PointField",
    "LineStringField",
    "PolygonField",
    "SequenceField",
    "UUIDField",
    "EnumField",
    "MultiPointField",
    "MultiLineStringField",
    "MultiPolygonField",
    "GeoJsonBaseField",
    "Decimal128Field",
)

class _BaseFieldOptions(TypedDict, total=False):
    db_field: str
    name: str
    unique: bool
    unique_with: Union[str, Iterable[str]]
    primary_key: bool
    choices: Iterable[_Choice]
    null: bool
    verbose_name: str
    help_text: str


class StringField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> StringField[Optional[str], Optional[str]]: ...
    # StringField(default="foo")
    @overload
    def __new__(
        cls,
        *,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        required: Literal[False] = ...,
        default: Union[str, Callable[[], str]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> StringField[Optional[str], str]: ...
    # StringField(required=True)
    @overload
    def __new__(
        cls,
        *,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> StringField[str, str]: ...
    # StringField(required=True, default="foo")
    @overload
    def __new__(
        cls,
        *,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        required: Literal[True],
        default: Union[str, Callable[[], str]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> StringField[Optional[str], str]: ...

class URLField(StringField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        url_regex: Optional[Pattern[str]] = ...,
        schemas: Optional[Container[str]] = ...,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> URLField[Optional[str], Optional[str]]: ...
    # URLField(default="foo")
    @overload
    def __new__(
        cls,
        *,
        url_regex: Optional[Pattern[str]] = ...,
        schemas: Optional[Container[str]] = ...,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        required: Literal[False] = ...,
        default: Union[str, Callable[[], str]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> URLField[Optional[str], str]: ...
    # URLField(required=True)
    @overload
    def __new__(
        cls,
        *,
        url_regex: Optional[Pattern[str]] = ...,
        schemas: Optional[Container[str]] = ...,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> URLField[str, str]: ...
    # URLField(required=True, default="foo")
    @overload
    def __new__(
        cls,
        *,
        url_regex: Optional[Pattern[str]] = ...,
        schemas: Optional[Container[str]] = ...,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        required: Literal[True],
        default: Union[str, Callable[[], str]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> URLField[Optional[str], str]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

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

class EmailField(StringField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        domain_whitelist: Optional[List[str]] = ...,
        allow_utf8_user: bool = ...,
        allow_ip_domain: bool = ...,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> EmailField[Optional[str], Optional[str]]: ...
    @overload
    def __new__(
        cls,
        *,
        domain_whitelist: Optional[List[str]] = ...,
        allow_utf8_user: bool = ...,
        allow_ip_domain: bool = ...,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        required: Literal[False] = ...,
        default: Union[str, Callable[[], str]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> EmailField[Optional[str], str]: ...
    @overload
    def __new__(
        cls,
        *,
        domain_whitelist: Optional[List[str]] = ...,
        allow_utf8_user: bool = ...,
        allow_ip_domain: bool = ...,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> EmailField[str, str]: ...
    @overload
    def __new__(
        cls,
        *,
        domain_whitelist: Optional[List[str]] = ...,
        allow_utf8_user: bool = ...,
        allow_ip_domain: bool = ...,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        required: Literal[True],
        default: Union[str, Callable[[], str]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> EmailField[Optional[str], str]: ...

class IntField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        min_value: int = ...,
        max_value: int = ...,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> IntField[Optional[int], Optional[int]]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: int = ...,
        max_value: int = ...,
        required: Literal[False] = ...,
        default: Union[int, Callable[[], int]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> IntField[Optional[int], int]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: int = ...,
        max_value: int = ...,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> IntField[int, int]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: int = ...,
        max_value: int = ...,
        required: Literal[True],
        default: Union[int, Callable[[], int]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> IntField[Optional[int], int]: ...

class FloatField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        min_value: float = ...,
        max_value: float = ...,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> FloatField[Optional[float], Optional[float]]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: float = ...,
        max_value: float = ...,
        required: Literal[False] = ...,
        default: Union[float, Callable[[], float]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> FloatField[Optional[float], float]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: float = ...,
        max_value: float = ...,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> FloatField[float, float]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: float = ...,
        max_value: float = ...,
        required: Literal[True],
        default: Union[float, Callable[[], float]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> FloatField[Optional[float], float]: ...

class DecimalField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        min_value: Decimal = ...,
        max_value: Decimal = ...,
        force_string: bool = ...,
        precision: int = ...,
        rounding: str = ...,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DecimalField[Optional[Decimal], Optional[Decimal]]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: Decimal = ...,
        max_value: Decimal = ...,
        force_string: bool = ...,
        precision: int = ...,
        rounding: str = ...,
        required: Literal[False] = ...,
        default: Union[Decimal, Callable[[], Decimal]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DecimalField[Optional[Decimal], Decimal]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: Decimal = ...,
        max_value: Decimal = ...,
        force_string: bool = ...,
        precision: int = ...,
        rounding: str = ...,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DecimalField[Decimal, Decimal]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: Decimal = ...,
        max_value: Decimal = ...,
        force_string: bool = ...,
        precision: int = ...,
        rounding: str = ...,
        required: Literal[True],
        default: Union[Decimal, Callable[[], Decimal]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DecimalField[Optional[Decimal], Decimal]: ...

class BooleanField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> BooleanField[Optional[bool], Optional[bool]]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: Union[bool, Callable[[], bool]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> BooleanField[Optional[bool], bool]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> BooleanField[bool, bool]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: Union[bool, Callable[[], bool]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> BooleanField[Optional[bool], bool]: ...

class DateTimeField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DateTimeField[Optional[datetime], Optional[datetime]]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: Union[datetime, Callable[[], datetime]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DateTimeField[Optional[datetime], datetime]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DateTimeField[datetime, datetime]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: Union[datetime, Callable[[], datetime]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DateTimeField[Optional[datetime], datetime]: ...

class EmbeddedDocumentField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        document_type: Type[_T],
        required: Literal[False] = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> EmbeddedDocumentField[Optional[_T], Optional[_T]]: ...
    @overload
    def __new__(
        cls,
        document_type: Type[_T],
        *,
        required: Literal[True],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> EmbeddedDocumentField[_T, _T]: ...

class DynamicField(BaseField): ...

class ListField(ComplexBaseField[_F, _ST, _GT]):
    # see: https://github.com/python/mypy/issues/4236#issuecomment-521628880
    # and probably this:
    #  * https://github.com/python/typing/issues/548
    # With Higher-Kinded TypeVars this could be simplfied, but it's not there yet.
    @overload
    def __new__(
        cls,
        field: StringField[_ST, _GT],
        required: bool = ...,
        default: Optional[Union[List[Any], Callable[[], List[Any]]]] = ...,
    ) -> ListField[StringField[_ST, _GT], list[_ST], list[_GT]]: ...
    @overload
    def __new__(
        cls,
        field: DictField[Any, Any, Any],
        required: bool = ...,
        default: Optional[Union[List[Any], Callable[[], List[Any]]]] = ...,
    ) -> ListField[DictField[Any, Any, Any], dict[str, Any], dict[str, Any]]: ...
    @overload
    def __new__(
        cls,
        field: Any | None,
        required: bool = ...,
        default: Optional[Union[List[Any], Callable[[], List[Any]]]] = ...,
    ) -> ListField[Any, Any, Any]: ...
    def __getitem__(self, arg: Any) -> _F: ...
    def __iter__(self) -> Iterator[_T]: ...

class DictField(ComplexBaseField[_F, _ST, _GT]):
    def __new__(
        cls,
        field: BaseField[_ST, _GT] = ...,
        required: bool = ...,
        default: Union[Dict[str, Any], None, Callable[[], Dict[str, Any]]] = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DictField[BaseField[_ST, _GT], dict[str, _ST], dict[str, _GT]]: ...
    def __getitem__(self, arg: Any) -> _GT: ...

class EmbeddedDocumentListField(ListField[_F, _ST, _GT]):
    def __new__(
        cls,
        document_type: Type[_T] | str,
        required: bool = ...,
        default: Optional[Any] = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> EmbeddedDocumentListField[EmbeddedDocumentField[_T, _T], list[_T], list[_T]]: ...


class LazyReference(Generic[_T], BaseField[_T, _T]):
    def __getitem__(self, arg: Any) -> LazyReference[_T]: ...

class LazyReferenceField(BaseField):
    def __new__(
        cls,
        document_type: Union[str, Type[Document]],
        required: bool = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> LazyReferenceField: ...
    def __getitem__(self, arg: Any) -> LazyReference[Any]: ...

class UUIDField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        binary: bool = ...,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> UUIDField[Optional[UUID], Optional[UUID]]: ...
    @overload
    def __new__(
        cls,
        *,
        binary: bool = ...,
        required: Literal[False] = ...,
        default: Union[UUID, Callable[[], UUID]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> UUIDField[Optional[UUID], UUID]: ...
    @overload
    def __new__(
        cls,
        *,
        binary: bool = ...,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> UUIDField[UUID, UUID]: ...
    @overload
    def __new__(
        cls,
        *,
        binary: bool = ...,
        required: Literal[True],
        default: Union[UUID, Callable[[], UUID]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> UUIDField[Optional[UUID], UUID]: ...

_Tuple2Like = Union[Tuple[Union[float, int], Union[float, int]], List[float], List[int]]

class GeoPointField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> GeoPointField[_Tuple2Like | None, list[float] | None]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: Union[_Tuple2Like, Callable[[], _Tuple2Like]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> GeoPointField[_Tuple2Like | None, list[float]]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> GeoPointField[_Tuple2Like, list[float]]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: Union[_Tuple2Like, Callable[[], _Tuple2Like]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> GeoPointField[_Tuple2Like | None, list[float]]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

class MapField(DictField):
    pass

class ReferenceField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        document_type: Type[_T] | str,
        dbref: bool = ...,
        reverse_delete_rule: int = ...,
        required: Literal[True] = ...,
        blank: bool = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ReferenceField[_T, _T]: ...
    @overload
    def __new__(
        cls,
        document_type: Type[_T] | str,
        dbref: bool = ...,
        reverse_delete_rule: int = ...,
        required: Literal[False] = ...,
        blank: bool = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ReferenceField[_T | None, _T | None]: ...
    def __getitem__(self, arg: Any) -> Any: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

_T_ENUM = TypeVar("_T_ENUM", bound=Enum)

class EnumField(BaseField[_ST, _GT]):
    # EnumField(Foo)
    @overload
    def __new__(
        cls,
        enum: Type[_T_ENUM],
        *,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> EnumField[Optional[_T_ENUM], Optional[_T_ENUM]]: ...
    # EnumField(Foo, default=Foo.Bar)
    @overload
    def __new__(
        cls,
        enum: Type[_T_ENUM],
        *,
        required: Literal[False] = ...,
        default: Union[_T_ENUM, Callable[[], _T_ENUM]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> EnumField[Optional[_T_ENUM], _T_ENUM]: ...
    # EnumField(Foo, required=True)
    @overload
    def __new__(
        cls,
        enum: Type[_T_ENUM],
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> EnumField[_T_ENUM, _T_ENUM]: ...
    # EnumField(Foo, required=True, default=Foo.Bar)
    @overload
    def __new__(
        cls,
        enum: Type[_T_ENUM],
        *,
        required: Literal[True],
        default: Union[_T_ENUM, Callable[[], _T_ENUM]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> EnumField[Optional[_T_ENUM], _T_ENUM]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

class LongField(BaseField[_ST, _GT]): ...
class DateField(BaseField[_ST, _GT]): ...
class ComplexDateTimeField(StringField[_ST, _GT]): ...
class GenericEmbeddedDocumentField(BaseField[_ST, _GT]): ...
class SortedListField(BaseField[_ST, _GT]): ...
class CachedReferenceField(BaseField[_ST, _GT]): ...
class GenericLazyReferenceField(BaseField[_ST, _GT]): ...
class BinaryField(BaseField[_ST, _GT]): ...
class GridFSError(Exception): ...
class GridFSProxy: ...

class FileField(BaseField[_ST, _GT]):
    def __new__(cls, *args, **kwargs) -> FileField[Any, Any]: ...

class ImageGridFsProxy(GridFSProxy):
    def put(self, file_obj, **kwargs): ...
    def delete(self, *args, **kwargs): ...
    @property
    def size(self): ...
    @property
    def format(self): ...
    @property
    def thumbnail(self): ...
    def write(self, *args, **kwargs) -> None: ...
    def writelines(self, *args, **kwargs) -> None: ...

class ImproperlyConfigured(Exception): ...
class PointField(GeoJsonBaseField[_ST, _GT]): ...
class LineStringField(GeoJsonBaseField[_ST, _GT]): ...
class PolygonField(GeoJsonBaseField[_ST, _GT]): ...
class SequenceField(BaseField[_ST, _GT]): ...
class MultiPointField(GeoJsonBaseField[_ST, _GT]): ...
class MultiLineStringField(GeoJsonBaseField[_ST, _GT]): ...
class MultiPolygonField(GeoJsonBaseField[_ST, _GT]): ...
class GeoJsonBaseField(BaseField[_ST, _GT]): ...
class Decimal128Field(BaseField[_ST, _GT]): ...
class ImageField(FileField[_ST, _GT]): ...
class GenericReferenceField(BaseField[Any, Any]): ...
