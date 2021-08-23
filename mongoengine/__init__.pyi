from typing import Optional

from mongoengine.document import Document, DynamicDocument, EmbeddedDocument
from mongoengine.errors import DoesNotExist, NotUniqueError, ValidationError
from mongoengine.queryset.queryset import QuerySet
from mongoengine.queryset.visitor import Q
from pymongo import MongoClient, ReadPreference

def connect(name: str, alias: str = ..., host: Optional[str] = ...) -> MongoClient: ...
def register_connection(
    alias: str,
    db: Optional[str] = ...,
    name: Optional[str] = ...,
    host: Optional[str] = ...,
    port: Optional[int] = ...,
    read_preference: ReadPreference = ...,
    username: Optional[str] = ...,
    password: Optional[str] = ...,
    authentication_source: Optional[str] = ...,
    authentication_mechanism: Optional[str] = ...,
) -> None: ...

__all__ = [
    "Q",
    "DoesNotExist",
    "QuerySet",
    "DynamicDocument",
    "EmbeddedDocument",
    "Document",
    "ValidationError",
    "NotUniqueError",
]
