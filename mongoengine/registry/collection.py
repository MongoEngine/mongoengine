from __future__ import annotations

__all__ = ("_CollectionRegistry",)

import enum
import threading

from pymongo.asynchronous.collection import AsyncCollection
from pymongo.synchronous.collection import Collection

MongoCollection = AsyncCollection | Collection


class CollectionType(enum.IntEnum):
    DEFAULT = 1
    CAPPED = 2
    TIMESERIES = 3


class _CollectionRegistry:
    """
    Thread-safe registry for caching MongoDB Collection / AsyncCollection.

    Key is:
        (db_alias, collection_name, collection_type, fingerprint, is_async)
    """

    _store: dict[tuple[str, str, CollectionType, str, bool], MongoCollection] = {}
    _lock = threading.RLock()

    # ---------------------------------------------------------------
    # GET
    # ---------------------------------------------------------------
    @classmethod
    def get(
        cls,
        db_alias: str,
        name: str,
        *,
        type_: CollectionType,
        fingerprint: str,
        is_async: bool,
    ) -> MongoCollection | None:
        key = (db_alias, name, type_, fingerprint, is_async)
        with cls._lock:
            return cls._store.get(key)

    # ---------------------------------------------------------------
    # REGISTER
    # ---------------------------------------------------------------
    @classmethod
    def register(
        cls,
        db_alias: str,
        name: str,
        collection: MongoCollection,
        *,
        type_: CollectionType,
        fingerprint: str,
    ) -> tuple[MongoCollection, bool]:
        """
        Registers and returns the collection + flag: was_created?

        You *must* provide fingerprint externally:
        e.g., fingerprint = Group._collection_fingerprint()
        """
        is_async = isinstance(collection, AsyncCollection)
        key = (db_alias, name, type_, fingerprint, is_async)

        with cls._lock:
            if key in cls._store:
                return cls._store[key], False

            cls._store[key] = collection
            return collection, True

    # ---------------------------------------------------------------
    # UNREGISTER
    # ---------------------------------------------------------------
    @classmethod
    def unregister(
        cls,
        db_alias: str,
        name: str,
        *,
        type_: CollectionType,
        fingerprint: str,
        is_async: bool,
    ) -> bool:
        key = (db_alias, name, type_, fingerprint, is_async)

        with cls._lock:
            if key in cls._store:
                del cls._store[key]
                return True
            return False

    # ---------------------------------------------------------------
    # CLEAR
    # ---------------------------------------------------------------
    @classmethod
    def clear(cls, db_alias: str | None = None) -> None:
        """Clear the whole registry or just entries for one alias."""
        with cls._lock:
            if db_alias is None:
                cls._store.clear()
                return

            to_delete = [key for key in cls._store if key[0] == db_alias]
            for key in to_delete:
                del cls._store[key]
