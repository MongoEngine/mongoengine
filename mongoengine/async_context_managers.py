"""Async context managers for MongoEngine."""

import contextlib
import logging

from pymongo.errors import ConnectionFailure, OperationFailure

from mongoengine.async_utils import ensure_async_connection, _get_async_session, _set_async_session
from mongoengine.connection import DEFAULT_CONNECTION_NAME, get_connection


__all__ = (
    "async_switch_db",
    "async_switch_collection",
    "async_no_dereference",
    "async_run_in_transaction",
)


class async_switch_db:
    """Async version of switch_db context manager.
    
    Temporarily switch the database for a document class in async context.
    
    Example::
    
        # Register connections
        await connect_async('mongoenginetest', alias='default')
        await connect_async('mongoenginetest2', alias='testdb-1')
        
        class Group(Document):
            name = StringField()
        
        await Group(name='test').async_save()  # Saves in the default db
        
        async with async_switch_db(Group, 'testdb-1') as Group:
            await Group(name='hello testdb!').async_save()  # Saves in testdb-1
    """
    
    def __init__(self, cls, db_alias):
        """Construct the async_switch_db context manager.
        
        :param cls: the class to change the registered db
        :param db_alias: the name of the specific database to use
        """
        self.cls = cls
        self.collection = None
        self.async_collection = None
        self.db_alias = db_alias
        self.ori_db_alias = cls._meta.get("db_alias", DEFAULT_CONNECTION_NAME)
    
    async def __aenter__(self):
        """Change the db_alias and clear the cached collection."""
        # Ensure the target connection is async
        ensure_async_connection(self.db_alias)
        
        # Store the current collections (both sync and async)
        self.collection = getattr(self.cls, '_collection', None)
        self.async_collection = getattr(self.cls, '_async_collection', None)
        
        # Switch to new database
        self.cls._meta["db_alias"] = self.db_alias
        # Clear both sync and async collections
        self.cls._collection = None
        if hasattr(self.cls, '_async_collection'):
            self.cls._async_collection = None
        
        return self.cls
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Reset the db_alias and collection."""
        self.cls._meta["db_alias"] = self.ori_db_alias
        self.cls._collection = self.collection
        if hasattr(self.cls, '_async_collection'):
            self.cls._async_collection = self.async_collection


class async_switch_collection:
    """Async version of switch_collection context manager.
    
    Temporarily switch the collection for a document class in async context.
    
    Example::
    
        class Group(Document):
            name = StringField()
        
        await Group(name='test').async_save()  # Saves in default collection
        
        async with async_switch_collection(Group, 'group_backup') as Group:
            await Group(name='hello backup!').async_save()  # Saves in group_backup collection
    """
    
    def __init__(self, cls, collection_name):
        """Construct the async_switch_collection context manager.
        
        :param cls: the class to change the collection
        :param collection_name: the name of the collection to use
        """
        self.cls = cls
        self.ori_collection = None
        self.ori_get_collection_name = cls._get_collection_name
        self.collection_name = collection_name
    
    async def __aenter__(self):
        """Change the collection name."""
        # Ensure we're using an async connection
        ensure_async_connection(self.cls._get_db_alias())
        
        # Store the current collections (both sync and async)
        self.ori_collection = getattr(self.cls, '_collection', None)
        self.ori_async_collection = getattr(self.cls, '_async_collection', None)
        
        # Create new collection name getter
        @classmethod
        def _get_collection_name(cls):
            return self.collection_name
        
        # Switch to new collection
        self.cls._get_collection_name = _get_collection_name
        # Clear both sync and async collections
        self.cls._collection = None
        if hasattr(self.cls, '_async_collection'):
            self.cls._async_collection = None
        
        return self.cls
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Reset the collection."""
        self.cls._collection = self.ori_collection
        if hasattr(self.cls, '_async_collection'):
            self.cls._async_collection = self.ori_async_collection
        self.cls._get_collection_name = self.ori_get_collection_name


@contextlib.asynccontextmanager
async def async_no_dereference(cls):
    """Async version of no_dereference context manager.
    
    Turns off all dereferencing in Documents for the duration of the context
    manager in async operations.
    
    Example::
    
        async with async_no_dereference(Group):
            groups = await Group.objects.async_to_list()
            # All reference fields will return AsyncReferenceProxy or DBRef objects
    """
    from mongoengine.base.fields import _no_dereference_for_fields
    from mongoengine.common import _import_class
    from mongoengine.context_managers import (
        _register_no_dereferencing_for_class,
        _unregister_no_dereferencing_for_class,
    )
    
    try:
        # Ensure we're using an async connection
        ensure_async_connection(cls._get_db_alias())
        
        ReferenceField = _import_class("ReferenceField")
        GenericReferenceField = _import_class("GenericReferenceField")
        ComplexBaseField = _import_class("ComplexBaseField")
        
        deref_fields = [
            field
            for name, field in cls._fields.items()
            if isinstance(
                field, (ReferenceField, GenericReferenceField, ComplexBaseField)
            )
        ]
        
        _register_no_dereferencing_for_class(cls)
        
        with _no_dereference_for_fields(*deref_fields):
            yield None
    finally:
        _unregister_no_dereferencing_for_class(cls)


async def _async_commit_with_retry(session):
    """Retry commit operation for async transactions.
    
    :param session: The async client session to commit
    """
    while True:
        try:
            # Commit uses write concern set at transaction start
            await session.commit_transaction()
            break
        except (ConnectionFailure, OperationFailure) as exc:
            # Can retry commit
            if exc.has_error_label("UnknownTransactionCommitResult"):
                logging.warning(
                    "UnknownTransactionCommitResult, retrying commit operation ..."
                )
                continue
            else:
                # Error during commit
                raise


@contextlib.asynccontextmanager
async def async_run_in_transaction(
    alias=DEFAULT_CONNECTION_NAME, session_kwargs=None, transaction_kwargs=None
):
    """Execute async queries within a database transaction.
    
    Execute queries within the context in a database transaction.
    
    Usage:
    
    .. code-block:: python
    
        class A(Document):
            name = StringField()
        
        async with async_run_in_transaction():
            a_doc = await A.objects.async_create(name="a")
            await a_doc.async_update(name="b")
    
    Be aware that:
    - Mongo transactions run inside a session which is bound to a connection. If you attempt to
      execute a transaction across a different connection alias, pymongo will raise an exception. In
      other words: you cannot create a transaction that crosses different database connections. That
      said, multiple transaction can be nested within the same session for particular connection.
    
    For more information regarding pymongo transactions: https://pymongo.readthedocs.io/en/stable/api/pymongo/client_session.html#transactions
    
    :param alias: the database alias name to use for the transaction
    :param session_kwargs: keyword arguments to pass to start_session()
    :param transaction_kwargs: keyword arguments to pass to start_transaction()
    """
    # Ensure we're using an async connection
    ensure_async_connection(alias)
    
    conn = get_connection(alias)
    session_kwargs = session_kwargs or {}
    
    # Start async session
    async with conn.start_session(**session_kwargs) as session:
        transaction_kwargs = transaction_kwargs or {}
        # Start the transaction
        await session.start_transaction(**transaction_kwargs)
        try:
            # Set the async session for the duration of the transaction
            await _set_async_session(session)
            yield
            # Commit with retry logic
            await _async_commit_with_retry(session)
        except Exception:
            # Abort transaction on any exception
            await session.abort_transaction()
            raise
        finally:
            # Clear the async session
            await _set_async_session(None)