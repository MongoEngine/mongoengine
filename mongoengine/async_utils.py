"""Async utility functions for MongoEngine async support."""

import contextvars

from mongoengine.connection import (
    DEFAULT_CONNECTION_NAME,
    ConnectionFailure,
    _connection_settings,
    _connections,
    _dbs,
    get_async_db,
    is_async_connection,
)

# Context variable for async sessions
_async_session_context = contextvars.ContextVar('mongoengine_async_session', default=None)


async def get_async_collection(collection_name, alias=DEFAULT_CONNECTION_NAME):
    """Get an async collection for the given name and alias.
    
    :param collection_name: the name of the collection
    :param alias: the alias name for the connection
    :return: AsyncMongoClient collection instance
    :raises ConnectionFailure: if connection is not async
    """
    db = get_async_db(alias)
    return db[collection_name]


def ensure_async_connection(alias=DEFAULT_CONNECTION_NAME):
    """Ensure the connection is async, raise error if not.
    
    :param alias: the alias name for the connection
    :raises RuntimeError: if connection is not async
    """
    if not is_async_connection(alias):
        raise RuntimeError(
            f"Connection '{alias}' is not async. Use connect_async() to create "
            "an async connection. Current operation requires async connection."
        )


def ensure_sync_connection(alias=DEFAULT_CONNECTION_NAME):
    """Ensure the connection is sync, raise error if not.
    
    :param alias: the alias name for the connection
    :raises RuntimeError: if connection is async
    """
    if is_async_connection(alias):
        raise RuntimeError(
            f"Connection '{alias}' is async. Use connect() to create "
            "a sync connection. Current operation requires sync connection."
        )


async def _get_async_session():
    """Get the current async session if any.
    
    :return: Current async session or None
    """
    return _async_session_context.get()


async def _set_async_session(session):
    """Set the current async session.
    
    :param session: The async session to set
    """
    _async_session_context.set(session)


async def async_exec_js(code, *args, **kwargs):
    """Execute JavaScript code asynchronously in MongoDB.
    
    This is the async version of exec_js that works with async connections.
    
    :param code: the JavaScript code to execute
    :param args: arguments to pass to the JavaScript code
    :param kwargs: keyword arguments including 'alias' for connection
    :return: result of JavaScript execution
    """
    alias = kwargs.pop('alias', DEFAULT_CONNECTION_NAME)
    ensure_async_connection(alias)
    
    db = get_async_db(alias)
    
    # In newer MongoDB versions, server-side JavaScript is deprecated
    # This is kept for compatibility but may not work with all MongoDB versions
    try:
        result = await db.command('eval', code, args=args, **kwargs)
        return result.get('retval')
    except Exception as e:
        # Fallback or raise appropriate error
        raise RuntimeError(
            f"JavaScript execution failed. Note that server-side JavaScript "
            f"is deprecated in MongoDB 4.2+. Error: {e}"
        )


class AsyncContextManager:
    """Base class for async context managers in MongoEngine."""
    
    async def __aenter__(self):
        raise NotImplementedError
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError


# Re-export commonly used functions for convenience
__all__ = [
    'get_async_collection',
    'ensure_async_connection',
    'ensure_sync_connection',
    'async_exec_js',
    'AsyncContextManager',
    '_get_async_session',
    '_set_async_session',
]