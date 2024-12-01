# pyright: reportIncompatibleMethodOverride=false
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import (
    Any,
    Callable,
    Container,
    Dict,
    Iterator,
    List,
    Optional,
    Pattern,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)
from uuid import UUID

from typing_extensions import Literal, TypeAlias, Unpack

from mongoengine.base import BaseField, ComplexBaseField
from mongoengine.base.datastructures import LazyReference
from mongoengine.base.fields import (
    _F,
    _GT,
    _ST,
    GeoJsonBaseField,
    ObjectIdField,
    _BaseFieldOptions,
)
from mongoengine.document import Document

_T = TypeVar("_T")
_DT = TypeVar("_DT", bound=Document)
_Choice: TypeAlias = str | tuple[str, str]
__all__ = (
    "StringField",
    "URLField",
    "EmailField",
    "IntField",
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

class DynamicField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DynamicField[Optional[Any], Optional[Any]]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: Union[Any, Callable[[], Any]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DynamicField[Optional[Any], Any]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DynamicField[Any, Any]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: Union[Any, Callable[[], Any]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DynamicField[Optional[Any], Any]: ...

class ListField(ComplexBaseField[_F, _ST, _GT]):
    # see: https://github.com/python/mypy/issues/4236#issuecomment-521628880
    # and probably this:
    #  * https://github.com/python/typing/issues/548
    # With Higher-Kinded TypeVars this could be simplfied, but it's not there yet.
    @overload
    def __new__(
        cls,
        field: BaseField[_ST, _GT],
        required: bool = ...,
        default: Optional[Union[List[Any], Callable[[], List[Any]]]] = ...,
    ) -> ListField[BaseField[_ST, _GT], list[_ST], list[_GT]]: ...
    @overload
    def __new__(
        cls,
        field: Any | None,
        required: bool = ...,
        default: Optional[Union[List[Any], Callable[[], List[Any]]]] = ...,
    ) -> ListField[Any, Any, Any]: ...
    def __getitem__(self, arg: Any) -> _GT: ...
    def __iter__(self) -> Iterator[_GT]: ...

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
    ) -> EmbeddedDocumentListField[
        EmbeddedDocumentField[_T, _T], list[_T], list[_T]
    ]: ...

class LazyReferenceField(BaseField[_ST, _GT]):
    def __new__(
        cls,
        document_type: type[_T] | str,
        passthrough: bool = ...,
        dbref: bool = ...,
        reverse_delete_rule: int = ...,
        required: bool = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> LazyReferenceField[_T, _T]: ...

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

class MapField(DictField[_F, _ST, _GT]):
    def __new__(
        cls,
        field: BaseField[_ST, _GT] = ...,
        required: bool = ...,
        default: Union[Dict[str, Any], None, Callable[[], Dict[str, Any]]] = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> MapField[BaseField[_ST, _GT], dict[str, _ST], dict[str, _GT]]: ...
    def __getitem__(self, arg: Any) -> _GT: ...

class ReferenceField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        document_type: Type[_T] | str,
        dbref: bool = ...,
        reverse_delete_rule: int = ...,
        required: Literal[False] = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ReferenceField[_T | None, _T | None]: ...
    @overload
    def __new__(
        cls,
        document_type: Type[_T] | str,
        dbref: bool = ...,
        reverse_delete_rule: int = ...,
        required: Literal[True] = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ReferenceField[_T, _T]: ...
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

class DateField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DateField[Optional[date], Optional[date]]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: Union[date, Callable[[], date]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DateField[Optional[date], date]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DateField[date, date]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: Union[date, Callable[[], date]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> DateField[Optional[date], date]: ...

class ComplexDateTimeField(StringField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        separator: str = ...,
        *,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ComplexDateTimeField[Optional[datetime], Optional[datetime]]: ...
    @overload
    def __new__(
        cls,
        separator: str = ...,
        *,
        required: Literal[False] = ...,
        default: Union[datetime, Callable[[], datetime]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ComplexDateTimeField[Optional[datetime], datetime]: ...
    @overload
    def __new__(
        cls,
        separator: str = ...,
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ComplexDateTimeField[datetime, datetime]: ...
    @overload
    def __new__(
        cls,
        separator: str = ...,
        *,
        required: Literal[True],
        default: Union[datetime, Callable[[], datetime]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ComplexDateTimeField[Optional[datetime], datetime]: ...

class GenericEmbeddedDocumentField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> GenericEmbeddedDocumentField[Optional[Any], Optional[Any]]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[False] = ...,
        default: Union[Any, Callable[[], Any]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> GenericEmbeddedDocumentField[Optional[Any], Any]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> GenericEmbeddedDocumentField[Any, Any]: ...
    @overload
    def __new__(
        cls,
        *,
        required: Literal[True],
        default: Union[Any, Callable[[], Any]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> GenericEmbeddedDocumentField[Optional[Any], Any]: ...

class SortedListField(ListField[_F, _ST, _GT]):
    @overload
    def __new__(
        cls,
        field: BaseField[_ST, _GT],
        required: bool = ...,
        default: Optional[Union[List[Any], Callable[[], List[Any]]]] = ...,
    ) -> SortedListField[BaseField[_ST, _GT], list[_ST], list[_GT]]: ...
    @overload
    def __new__(
        cls,
        field: Any | None,
        required: bool = ...,
        default: Optional[Union[List[Any], Callable[[], List[Any]]]] = ...,
    ) -> SortedListField[Any, Any, Any]: ...

class CachedReferenceField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        document_type: Type[_T] | str,
        required: Literal[False] = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> CachedReferenceField[_T | None, _T | None]: ...
    @overload
    def __new__(
        cls,
        document_type: Type[_T] | str,
        required: Literal[True] = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> CachedReferenceField[_T, _T]: ...

class GenericLazyReferenceField(BaseField[LazyReference[Any], LazyReference[Any]]):
    def __init__(
        self, *args: Any, passthrough: bool = False, **kwargs: Unpack[_BaseFieldOptions]
    ): ...

class BinaryField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        max_bytes: int | None = ...,
        *,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> BinaryField[Optional[bytes], Optional[bytes]]: ...
    @overload
    def __new__(
        cls,
        max_bytes: int | None = ...,
        *,
        required: Literal[False] = ...,
        default: Union[bytes, Callable[[], bytes]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> BinaryField[Optional[bytes], bytes]: ...
    @overload
    def __new__(
        cls,
        max_bytes: int | None = ...,
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> BinaryField[bytes, bytes]: ...
    @overload
    def __new__(
        cls,
        max_bytes: int | None = ...,
        *,
        required: Literal[True],
        default: Union[bytes, Callable[[], bytes]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> BinaryField[Optional[bytes], bytes]: ...

class GridFSError(Exception): ...
class GridFSProxy: ...

class FileField(BaseField[_ST, _GT]):
    def __new__(
        cls,
        db_alias: str = ...,
        collection_name: str = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> FileField[Any, Any]: ...

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
class PointField(GeoJsonBaseField): ...
class LineStringField(GeoJsonBaseField): ...
class PolygonField(GeoJsonBaseField): ...

class SequenceField(BaseField[Any, Any]):
    def __init__(
        self,
        collection_name: str | None = ...,
        db_alias: str | None = ...,
        sequence_name: str | None = ...,
        value_decorator: Any | None = ...,
        *args: Any,
        **kwargs: Unpack[_BaseFieldOptions],
    ): ...

class MultiPointField(GeoJsonBaseField): ...
class MultiLineStringField(GeoJsonBaseField): ...
class MultiPolygonField(GeoJsonBaseField): ...

class Decimal128Field(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        min_value: int | None = ...,
        max_value: int | None = ...,
        *,
        required: Literal[False] = ...,
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> Decimal128Field[Optional[Decimal], Optional[Decimal]]: ...
    @overload
    def __new__(
        cls,
        min_value: int | None = ...,
        max_value: int | None = ...,
        *,
        required: Literal[False] = ...,
        default: Union[Decimal, Callable[[], Decimal]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> Decimal128Field[Optional[Decimal], Decimal]: ...
    @overload
    def __new__(
        cls,
        min_value: int | None = ...,
        max_value: int | None = ...,
        *,
        required: Literal[True],
        default: None = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> Decimal128Field[Decimal, Decimal]: ...
    @overload
    def __new__(
        cls,
        min_value: int | None = ...,
        max_value: int | None = ...,
        *,
        required: Literal[True],
        default: Union[Decimal, Callable[[], Decimal]],
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> Decimal128Field[Optional[Decimal], Decimal]: ...

class ImageField(FileField[_ST, _GT]):
    def __new__(
        cls,
        size: tuple[int, int, bool] | None = ...,
        thumbnail_size: tuple[int, int, bool] | None = ...,
        collection_name: str = ...,
        db_alias: str = ...,
        **kwargs: Unpack[_BaseFieldOptions],
    ) -> ImageField[Any, Any]: ...

class GenericReferenceField(BaseField[Any, Any]):
    def __init__(self, *args: Any, **kwargs: Unpack[_BaseFieldOptions]): ...
