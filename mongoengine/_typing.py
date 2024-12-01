from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from mongoengine.queryset.queryset import QuerySet

QS = TypeVar("QS", bound="QuerySet")
