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

class _FieldOptions(TypedDict, total=False):
    db_field: str
    name: str
    required: bool
    default: Union[Any, Callable[[], Any]]
    unique: bool
    unique_with: Union[str, Iterable[str]]
    primary_key: bool
    choices: Iterable[Any]
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> StringField[Optional[str], Optional[str]]: ...
    # StringField(default="foo")
    @overload
    def __new__(
        cls,
        *,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[str, Callable[[], str]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> StringField[Optional[str], str]: ...
    # StringField(required=True)
    @overload
    def __new__(
        cls,
        *,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> StringField[str, str]: ...
    # StringField(required=True, default="foo")
    @overload
    def __new__(
        cls,
        *,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: Union[str, Callable[[], str]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> StringField[Optional[str], str]: ...
    # StringField(primary_key=True)
    @overload
    def __new__(
        cls,
        *,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: bool = ...,
        default: Union[str, Callable[[], str], None] = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[True],
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> StringField[str, str]: ...

class URLField(StringField[_ST, _GT]):
    def __init__(
        self,
        url_regex: str | None = None,
        schemes: Iterable[str] | None = None,
        **kwargs: Any,
    ) -> None: ...
    @overload
    def __new__(
        cls,
        *,
        url_regex: Optional[Pattern[str]] = ...,
        schemas: Optional[Container[str]] = ...,
        regex: Optional[str] = ...,
        max_length: Optional[int] = ...,
        min_length: Optional[int] = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[str, Callable[[], str]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: Union[str, Callable[[], str]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> URLField[Optional[str], str]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

class ObjectIdField(BaseField[_ST, _GT]):
    # ObjectIdField()
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[ObjectId]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> ObjectIdField[Optional[ObjectId], Optional[ObjectId]]: ...
    # ObjectIdField(default=ObjectId)
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[ObjectId, Callable[[], ObjectId]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[ObjectId]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> ObjectIdField[Optional[ObjectId], ObjectId]: ...
    # ObjectIdField(required=True)
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[ObjectId]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> ObjectIdField[ObjectId, ObjectId]: ...
    # ObjectIdField(required=True, default=ObjectId)
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: Union[ObjectId, Callable[[], ObjectId]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[ObjectId]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> ObjectIdField[Optional[ObjectId], ObjectId]: ...
    # ObjectIdField(primiary_key=True)
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: bool = ...,
        default: Union[ObjectId, None, Callable[[], ObjectId]] = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[True],
        choices: Optional[Iterable[ObjectId]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> ObjectIdField[ObjectId, ObjectId]: ...
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[str, Callable[[], str]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: Union[str, Callable[[], str]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: bool = ...,
        default: Union[str, Callable[[], str], None] = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[True],
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> EmailField[str, str]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

class IntField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        min_value: int = ...,
        max_value: int = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[int]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> IntField[Optional[int], Optional[int]]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: int = ...,
        max_value: int = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[int, Callable[[], int]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[int]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> IntField[Optional[int], int]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: int = ...,
        max_value: int = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[int]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> IntField[int, int]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: int = ...,
        max_value: int = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: Union[int, Callable[[], int]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[int]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> IntField[Optional[int], int]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: int = ...,
        max_value: int = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: bool = ...,
        default: Union[int, Callable[[], int], None] = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[True],
        choices: Optional[Iterable[int]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> IntField[int, int]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

class FloatField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        min_value: float = ...,
        max_value: float = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[float]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> FloatField[Optional[float], Optional[float]]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: float = ...,
        max_value: float = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[float, Callable[[], float]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[float]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> FloatField[Optional[float], float]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: float = ...,
        max_value: float = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[float]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> FloatField[float, float]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: float = ...,
        max_value: float = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: Union[float, Callable[[], float]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[float]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> FloatField[Optional[float], float]: ...
    @overload
    def __new__(
        cls,
        *,
        min_value: float = ...,
        max_value: float = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: bool = ...,
        default: Union[float, Callable[[], float], None] = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[True],
        choices: Optional[Iterable[float]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> FloatField[float, float]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[Decimal]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[Decimal, Callable[[], Decimal]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[Decimal]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[Decimal]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: Union[Decimal, Callable[[], Decimal]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[Decimal]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
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
        db_field: str = ...,
        name: Optional[str] = ...,
        required: bool = ...,
        default: Union[Decimal, Callable[[], Decimal], None] = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[True],
        choices: Optional[Iterable[Decimal]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> DecimalField[Decimal, Decimal]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

class BooleanField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[bool]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> BooleanField[Optional[bool], Optional[bool]]: ...
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[bool, Callable[[], bool]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[bool]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> BooleanField[Optional[bool], bool]: ...
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[bool]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> BooleanField[bool, bool]: ...
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: Union[bool, Callable[[], bool]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[bool]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> BooleanField[Optional[bool], bool]: ...
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: bool = ...,
        default: Union[bool, None, Callable[[], bool]] = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[True],
        choices: Optional[Iterable[bool]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> BooleanField[bool, bool]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

class DateTimeField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[datetime]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> DateTimeField[Optional[datetime], Optional[datetime]]: ...
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[datetime, Callable[[], datetime]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[datetime]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> DateTimeField[Optional[datetime], datetime]: ...
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[datetime]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> DateTimeField[datetime, datetime]: ...
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: Union[datetime, Callable[[], datetime]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[datetime]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> DateTimeField[Optional[datetime], datetime]: ...
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: bool = ...,
        default: Union[datetime, None, Callable[[], datetime]] = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[True],
        choices: Optional[Iterable[datetime]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> DateTimeField[datetime, datetime]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

class EmbeddedDocumentField(Generic[_T], BaseField[_T, _T]):
    @overload
    def __new__(
        cls,
        document_type: Type[_T],
        required: Literal[False] = ...,
    ) -> EmbeddedDocumentField[Optional[_T]]: ...
    @overload
    def __new__(
        cls,
        document_type: Type[_T],
        *,
        required: Literal[True],
    ) -> EmbeddedDocumentField[_T]: ...
    def __init__(self, document_type: Type[_T] | str, **kwargs: Any) -> None: ...

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
        verbose_name: str = ...,
        help_text: str = ...,
        null: bool = ...,
    ) -> ListField[StringField[_ST, _GT], list[_ST], list[_GT]]: ...
    @overload
    def __new__(
        cls,
        field: DictField[Any, Any, Any],
        required: bool = ...,
        default: Optional[Union[List[Any], Callable[[], List[Any]]]] = ...,
        verbose_name: str = ...,
        help_text: str = ...,
        null: bool = ...,
    ) -> ListField[DictField[Any, Any, Any], dict[str, Any], dict[str, Any]]: ...
    @overload
    def __new__(
        cls,
        field: Any | None,
        required: bool = ...,
        default: Optional[Union[List[Any], Callable[[], List[Any]]]] = ...,
        verbose_name: str = ...,
        help_text: str = ...,
        null: bool = ...,
    ) -> ListField[Any, Any, Any]: ...
    def __getitem__(self, arg: Any) -> _F: ...
    def __iter__(self) -> Iterator[_T]: ...

class DictField(ComplexBaseField[_F, _ST, _GT]):
    def __new__(
        cls,
        field: BaseField[_ST, _GT] = ...,
        required: bool = ...,
        name: Optional[str] = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: bool = ...,
        help_text: Optional[str] = ...,
        default: Union[Dict[str, Any], None, Callable[[], Dict[str, Any]]] = ...,
        choices: Optional[Iterable[Dict[str, Any]]] = ...,
        verbose_name: Optional[str] = ...,
        db_field: str = ...,
        **kwargs: Any,
    ) -> DictField[BaseField[_ST, _GT], dict[str, _ST], dict[str, _GT]]: ...
    def __getitem__(self, arg: Any) -> _GT: ...

class EmbeddedDocumentListField(ListField[_F, _ST, _GT]):
    def __new__(
        cls,
        document_type: Type[_T],
        required: bool = ...,
        default: Optional[Any] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> EmbeddedDocumentListField[EmbeddedDocumentField[_ST, _GT], _ST, _GT]: ...

class LazyReference(Generic[_T], BaseField[_T, _T]):
    def __getitem__(self, arg: Any) -> LazyReference[_T]: ...

class LazyReferenceField(BaseField):
    def __new__(
        cls,
        name: Union[str, Type[Document]],
        unique: bool = ...,
        required: bool = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> LazyReferenceField: ...
    def __getitem__(self, arg: Any) -> LazyReference[Any]: ...

class UUIDField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        binary: bool = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[UUID]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> UUIDField[Optional[UUID], Optional[UUID]]: ...
    @overload
    def __new__(
        cls,
        *,
        binary: bool = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[UUID, Callable[[], UUID]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[UUID]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> UUIDField[Optional[UUID], UUID]: ...
    @overload
    def __new__(
        cls,
        *,
        binary: bool = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[UUID]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> UUIDField[UUID, UUID]: ...
    @overload
    def __new__(
        cls,
        *,
        binary: bool = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: Union[UUID, Callable[[], UUID]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[UUID]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> UUIDField[Optional[UUID], UUID]: ...
    @overload
    def __new__(
        cls,
        *,
        binary: bool = ...,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[UUID, None, Callable[[], UUID]] = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[True],
        choices: Optional[Iterable[UUID]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> UUIDField[UUID | None, UUID | None]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

_Tuple2Like = Union[Tuple[Union[float, int], Union[float, int]], List[float], List[int]]

class GeoPointField(BaseField[_ST, _GT]):
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> GeoPointField[_Tuple2Like | None, list[float] | None]: ...
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[_Tuple2Like, Callable[[], _Tuple2Like]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> GeoPointField[_Tuple2Like | None, list[float]]: ...
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> GeoPointField[_Tuple2Like, list[float]]: ...
    @overload
    def __new__(
        cls,
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: Union[_Tuple2Like, Callable[[], _Tuple2Like]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_Choice]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> GeoPointField[_Tuple2Like | None, list[float]]: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

class MapField(DictField):
    pass

class ReferenceField(BaseField[_ST, _GT]):
    def __init__(
        self,
        document_type: Type[_T],
        dbref: bool = False,
        reverse_delete_rule=...,
        **kwargs: Any,
    ) -> None: ...
    @overload
    def __new__(
        cls,
        model: Union[str, Type[_T]],
        required: Literal[True],
        name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        blank: bool = ...,
        **kwargs: Any,
    ) -> ReferenceField[_T, _T]: ...
    @overload
    def __new__(
        cls,
        model: Union[str, Type[_T]],
        required: Literal[False] = ...,
        name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        blank: bool = ...,
        **kwargs: Any,
    ) -> ReferenceField[_T | None, _T | None]: ...
    def __getitem__(self, arg: Any) -> Any: ...
    def __set__(self, instance: Any, value: _ST) -> None: ...

_T_ENUM = TypeVar("_T_ENUM", bound=Enum)

class EnumField(BaseField[_ST, _GT]):
    def __init__(self, enum: type[Enum], **kwargs: Unpack[_FieldOptions]): ...
    # EnumField(Foo)
    @overload
    def __new__(
        cls,
        enum: Type[_T_ENUM],
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_T_ENUM]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> EnumField[Optional[_T_ENUM], Optional[_T_ENUM]]: ...
    # EnumField(Foo, default=Foo.Bar)
    @overload
    def __new__(
        cls,
        enum: Type[_T_ENUM],
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[False] = ...,
        default: Union[_T_ENUM, Callable[[], _T_ENUM]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_T_ENUM]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> EnumField[Optional[_T_ENUM], _T_ENUM]: ...
    # EnumField(Foo, required=True)
    @overload
    def __new__(
        cls,
        enum: Type[_T_ENUM],
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: None = ...,
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_T_ENUM]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
    ) -> EnumField[_T_ENUM, _T_ENUM]: ...
    # EnumField(Foo, required=True, default=Foo.Bar)
    @overload
    def __new__(
        cls,
        enum: Type[_T_ENUM],
        *,
        db_field: str = ...,
        name: Optional[str] = ...,
        required: Literal[True],
        default: Union[_T_ENUM, Callable[[], _T_ENUM]],
        unique: bool = ...,
        unique_with: Union[str, Iterable[str]] = ...,
        primary_key: Literal[False] = ...,
        choices: Optional[Iterable[_T_ENUM]] = ...,
        null: bool = ...,
        verbose_name: Optional[str] = ...,
        help_text: Optional[str] = ...,
        **kwargs: Any,
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
