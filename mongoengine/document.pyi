from typing import Any, Dict, Mapping, Optional, Type, TypeVar

import mongoengine.errors as errors
from mongoengine.base import BaseDocument
from mongoengine.fields import StringField
from pymongo.collection import Collection
from typing_extensions import TypedDict

_U = TypeVar("_U", bound="Document")

_MetaDict = Mapping[str, Any]

class _UnderMetaDict(TypedDict):
    strict: bool
    collection: str

class Document(BaseDocument):
    meta: _MetaDict
    _meta: _UnderMetaDict
    _fields: Dict[str, Any]

    pk = StringField(required=True)
    @classmethod
    def _get_collection(cls) -> Collection: ...
    # NOTE(sbdchd): if we are willing to change all Document.objects.filter()
    # to Document.objects().filter() then we can define this method and we
    # won't need to provide the `objects = ObjectManager[T]()` in each mongo model.
    #
    # @classmethod
    # def objects(cls: Type[U]) -> QuerySet[U]:
    #     """
    #     from:
    #     https://github.com/python/peps/blob/50a31d14b467aba0cc0168408369d2bb28e9b4e2/pep-0484.txt#L1260-L1270
    #     """
    #     ...
    def modify(self, query: Optional[object] = ..., **update: object) -> bool: ...
    def update(self, **update: object) -> int: ...
    def __contains__(self, key: str) -> bool: ...
    def delete(self, signal_kwargs: object = ..., **write_concern: object) -> None: ...
    @classmethod
    def from_json(cls: Type[_U], data: object, created: bool = ...) -> _U: ...
    def save(
        self: _U,
        force_insert: bool = ...,
        validate: bool = ...,
        clean: bool = ...,
        write_concern: Any = ...,
        cascade: Any = ...,
        cascade_kwargs: Any = ...,
        _refs: Any = ...,
        save_condition: Any = ...,
        signal_kwargs: Any = ...,
    ) -> _U: ...
    class DoesNotExist(errors.DoesNotExist): ...
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def reload(self: _U) -> _U:
        """
        from: https://github.com/python/peps/commit/ada7d3566e26edf5381d1339b61e48a82c51c566#diff-da7d638a3d189515209a80943cdc8eaf196b75d20ccc0d6a796393c025d1f975R1169
        """
        ...

class EmbeddedDocument(BaseDocument):
    _fields: Dict[str, Any]
    _meta: _UnderMetaDict
    def __new__(cls, *args: Any, **kwargs: Any) -> EmbeddedDocument: ...
    def save(self) -> None: ...
    def __contains__(self, key: str) -> bool: ...

class DynamicDocument(Document):
    def __getattr__(self, key: str) -> Any: ...
    def __setattr__(self, key: str, value: Any) -> None: ...

__all__ = [
    "DynamicDocument",
    "EmbeddedDocument",
    "Document",
    "BaseDocument",
]
