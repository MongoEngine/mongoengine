import logging
from contextlib import contextmanager
from contextvars import ContextVar

from pymongo.asynchronous.database import AsyncDatabase
from pymongo.synchronous.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from mongoengine.asynchronous import async_get_db, async_get_connection
from mongoengine.synchronous.connection import (
    DEFAULT_CONNECTION_NAME,
    get_connection,
    get_db,
)

from mongoengine.session import _clear_session, _get_session, _set_session

from mongoengine.pymongo_support import count_documents, async_count_documents

__all__ = (
    "switch_db",
    "switch_collection",
    "no_sub_classes",
    "query_counter",
    "set_write_concern",
    "set_read_write_concern",
    "run_in_transaction",
)

CURRENT_DB_ALIAS = ContextVar("mongoengine_db_alias", default={})
CURRENT_COLLECTION = ContextVar("mongoengine_collection_overrides", default={})


class switch_db:
    def __init__(self, cls, db_alias=DEFAULT_CONNECTION_NAME):
        self.cls = cls
        self.db_alias = db_alias
        self.token = None

    def __enter__(self):
        cur = CURRENT_DB_ALIAS.get() or {}
        new = dict(cur)
        new[self.cls] = self.db_alias
        self.token = CURRENT_DB_ALIAS.set(new)
        return self.cls

    def __exit__(self, exc_type, exc, tb):
        CURRENT_DB_ALIAS.reset(self.token)

    async def __aenter__(self):
        cur = CURRENT_DB_ALIAS.get() or {}
        new = dict(cur)
        new[self.cls] = self.db_alias
        self.token = CURRENT_DB_ALIAS.set(new)
        return self.cls

    async def __aexit__(self, exc_type, exc, tb):
        CURRENT_DB_ALIAS.reset(self.token)


class switch_collection:
    """switch_collection alias context manager.

    Example ::

        class Group(Document):
            name = StringField()

        Group(name='test').save()  # Saves in the default db

        with switch_collection(Group, 'group1') as Group:
            Group(name='hello testdb!').save()  # Saves in group1 collection
    """

    def __init__(self, cls, collection_name):
        """Construct the switch_collection context manager.

        :param cls: the class to change the registered db
        :param collection_name: the name of the collection to use
        """
        self.cls = cls
        self.collection_name = collection_name
        self.token = None

    def __enter__(self):
        cur = CURRENT_COLLECTION.get() or {}
        new = dict(cur)
        new[self.cls] = self.collection_name
        self.token = CURRENT_COLLECTION.set(new)
        return self.cls

    def __exit__(self, exc_type, exc, tb):
        CURRENT_COLLECTION.reset(self.token)

    async def __aenter__(self):
        cur = CURRENT_COLLECTION.get() or {}
        new = dict(cur)
        new[self.cls] = self.collection_name
        self.token = CURRENT_COLLECTION.set(new)
        return self.cls

    async def __aexit__(self, exc_type, exc, tb):
        CURRENT_COLLECTION.reset(self.token)


class no_sub_classes:
    """no_sub_classes context manager.

    Only returns instances of this class and no sub (inherited) classes::

        with no_sub_classes(Group) as Group:
            Group.objects.find()
    """

    def __init__(self, cls):
        """Construct the no_sub_classes context manager.

        :param cls: the class to turn querying subclasses on
        """
        self.cls = cls
        self.cls_initial_subclasses = None

    def __enter__(self):
        """Change the objects default and _auto_dereference values."""
        self.cls_initial_subclasses = self.cls._subclasses
        self.cls._subclasses = (self.cls._class_name,)
        return self.cls

    def __exit__(self, t, value, traceback):
        """Reset the default and _auto_dereference values."""
        self.cls._subclasses = self.cls_initial_subclasses


class query_counter:
    """Query_counter context manager to get the number of queries.
    This works by updating the `profiling_level` of the database so that all queries get logged,
    resetting the db.system.profile collection at the beginning of the context and counting the new entries.

    This was designed for debugging purpose. In fact it is a global counter so queries issued by other threads/processes
    can interfere with it

    Usage:

    .. code-block:: python

        class User(Document):
            name = StringField()

        with query_counter() as q:
            user = User(name='Bob')
            assert q == 0       # no query fired yet
            user.save()
            assert q == 1       # 1 query was fired, an 'insert'
            user_bis = User.objects().first()
            assert q == 2       # a 2nd query was fired, a 'find_one'

    Be aware that:

    - Iterating over large amount of documents (>101) makes pymongo issue `getmore` queries to fetch the next batch of
    documents (https://www.mongodb.com/docs/manual/tutorial/iterate-a-cursor/#cursor-batches)
    - Some queries are ignored by default by the counter (killcursors, db.system.indexes)
    """

    def __init__(self, alias=DEFAULT_CONNECTION_NAME):
        self.alias = alias
        self._db = None
        self.initial_profiling_level = None
        self._ctx_query_counter = 0  # number of queries issued by the context
        self._ignored_query = None

    @property
    def db(self):
        if self._db is None:
            self._db = get_db(alias=self.alias)
            if not isinstance(self._db, Database):
                raise Exception("async_query_counter only support sync database")
            self._ignored_query = {
                "ns": {"$ne": "%s.system.indexes" % self._db.name},
                "op": {"$ne": "killcursors"},  # MONGODB < 3.2
                "command.killCursors": {"$exists": False},  # MONGODB >= 3.2
            }
        return self._db

    def _turn_on_profiling(self):
        profile_update_res = self.db.command({"profile": 0}, session=_get_session())
        self.initial_profiling_level = profile_update_res["was"]

        self.db.system.profile.drop()
        self.db.command({"profile": 2}, session=_get_session())

    def _resets_profiling(self):
        self.db.command({"profile": self.initial_profiling_level})

    def __enter__(self):
        self._turn_on_profiling()
        return self

    def __exit__(self, t, value, traceback):
        self._resets_profiling()

    def __eq__(self, value):
        counter = self._get_count()
        return value == counter

    def __ne__(self, value):
        return not self.__eq__(value)

    def __lt__(self, value):
        return self._get_count() < value

    def __le__(self, value):
        return self._get_count() <= value

    def __gt__(self, value):
        return self._get_count() > value

    def __ge__(self, value):
        return self._get_count() >= value

    def __int__(self):
        return self._get_count()

    def __repr__(self):
        """repr query_counter as the number of queries."""
        return "%s" % self._get_count()

    def _get_count(self):
        """Get the number of queries by counting the current number of entries in db.system.profile
        and substracting the queries issued by this context. In fact everytime this is called, 1 query is
        issued so we need to balance that
        """
        count = (
                count_documents(self.db.system.profile, self._ignored_query)
                - self._ctx_query_counter
        )
        self._ctx_query_counter += (
            1  # Account for the query we just issued to gather the information
        )
        return count


class async_query_counter:
    """Query_counter context manager to get the number of queries.
    This works by updating the `profiling_level` of the database so that all queries get logged,
    resetting the db.system.profile collection at the beginning of the context and counting the new entries.

    This was designed for debugging purpose. In fact it is a global counter so queries issued by other threads/processes
    can interfere with it

    Usage:

    .. code-block:: python

        class User(Document):
            name = StringField()

        with query_counter() as q:
            user = User(name='Bob')
            assert q == 0 # no query fired yet
            user.save()
            assert q == 1 # 1 query was fired, an 'insert'
            user_bis = User.objects().first()
            assert q == 2 # a 2nd query was fired, a 'find_one'

    Be aware that:

    - Iterating over large amount of documents (>101) makes pymongo issue `getmore` queries to fetch the next batch of
    documents (https://www.mongodb.com/docs/manual/tutorial/iterate-a-cursor/#cursor-batches)
    - Some queries are ignored by default by the counter (killcursors, db.system.indexes)
    """

    def __init__(self, alias=DEFAULT_CONNECTION_NAME):
        self.alias = alias
        self._db = None
        self.initial_profiling_level = None
        self._ctx_query_counter = 0  # number of queries issued by the context
        self._ignored_query = None

    @property
    async def db(self):
        if self._db is None:
            self._db = await async_get_db(alias=self.alias)
            if not isinstance(self._db, AsyncDatabase):
                raise Exception("async_query_counter only support async database")
            self._ignored_query = {
                "ns": {"$ne": "%s.system.indexes" % self._db.name},
                "op": {"$ne": "killcursors"},  # MONGODB < 3.2
                "command.killCursors": {"$exists": False},  # MONGODB >= 3.2
            }
        return self._db

    async def _turn_on_profiling(self):
        profile_update_res = await (await self.db).command({"profile": 0}, session=_get_session())
        self.initial_profiling_level = profile_update_res["was"]

        await (await self.db).system.profile.drop()
        await (await self.db).command({"profile": 2}, session=_get_session())

    async def _resets_profiling(self):
        await (await self.db).command({"profile": self.initial_profiling_level})

    def __enter__(self):
        raise NotImplementedError("Not supported for AsyncQuerySet.")

    async def __aenter__(self):
        await self._turn_on_profiling()
        return self

    async def __aexit__(self, t, value, traceback):
        await self._resets_profiling()

    def __exit__(self, t, value, traceback):
        raise NotImplementedError("Not supported for AsyncQuerySet.")

    def __eq__(self, value):
        raise NotImplementedError("Not supported for AsyncQuerySet.")

    async def eq(self, value):
        counter = await self._get_count()
        return value == counter

    def __ne__(self, value):
        raise NotImplementedError("Not supported for AsyncQuerySet.")

    async def ne(self, value):
        return not await self.eq(value)

    def __lt__(self, value):
        raise NotImplementedError("Not supported for AsyncQuerySet.")

    async def lt(self, value):
        return await self._get_count() < value

    def __le__(self, value):
        raise NotImplementedError("Not supported for AsyncQuerySet.")

    async def le(self, value):
        return await self._get_count() <= value

    def __gt__(self, value):
        raise NotImplementedError("Not supported for AsyncQuerySet.")

    async def gt(self, value):
        return await self._get_count() > value

    def __ge__(self, value):
        raise NotImplementedError("Not supported for AsyncQuerySet.")

    async def ge(self, value):
        return await self._get_count() >= value

    def __int__(self):
        raise NotImplementedError("Not supported for AsyncQuerySet.")

    async def int(self):
        value = await self._get_count()
        return value

    def __repr__(self):
        raise NotImplementedError("Not supported for AsyncQuerySet.")

    async def repr(self):
        """repr query_counter as the number of queries."""
        return "%s" % await self._get_count()

    async def _get_count(self):
        """Get the number of queries by counting the current number of entries in db.system.profile
        and substracting the queries issued by this context. In fact everytime this is called, 1 query is
        issued so we need to balance that
        """
        count = (
                await async_count_documents((await self.db).system.profile, self._ignored_query)
                - self._ctx_query_counter
        )
        self._ctx_query_counter += (
            1  # Account for the query we just issued to gather the information
        )
        return count


@contextmanager
def set_write_concern(collection, write_concerns):
    combined_concerns = dict(collection.write_concern.document.items())
    combined_concerns.update(write_concerns)
    yield collection.with_options(write_concern=WriteConcern(**combined_concerns))


@contextmanager
def set_read_write_concern(collection, write_concerns, read_concerns):
    combined_write_concerns = dict(collection.write_concern.document.items())

    if write_concerns is not None:
        combined_write_concerns.update(write_concerns)

    combined_read_concerns = dict(collection.read_concern.document.items())

    if read_concerns is not None:
        combined_read_concerns.update(read_concerns)

    yield collection.with_options(
        write_concern=WriteConcern(**combined_write_concerns),
        read_concern=ReadConcern(**combined_read_concerns),
    )


class run_in_transaction:
    """
    Unified sync + async transaction context manager.

    Sync:
        with run_in_transaction():
            ...

    Async:
        async with run_in_transaction():
            ...
    """

    def __init__(
            self,
            alias=DEFAULT_CONNECTION_NAME,
            session_kwargs=None,
            transaction_kwargs=None,
    ):
        self.alias = alias
        self.session_kwargs = session_kwargs or {}
        self.transaction_kwargs = transaction_kwargs or {}

        # sync state
        self._sync_session_cm = None
        self._sync_txn_cm = None
        self._sync_session = None

        # async state
        self._async_session_cm = None
        self._async_session = None

    # ------------------------------------------------------------------
    # Retry helpers (SYNC)
    # ------------------------------------------------------------------
    def _commit_with_retry(self, session):
        while True:
            try:
                session.commit_transaction()
                break
            except (ConnectionFailure, OperationFailure) as exc:
                if exc.has_error_label("UnknownTransactionCommitResult"):
                    logging.warning(
                        "UnknownTransactionCommitResult, retrying commit operation ..."
                    )
                    continue
                raise

    def _abort_with_retry(self, session):
        while True:
            try:
                session.abort_transaction()
                break
            except (ConnectionFailure, OperationFailure) as exc:
                if exc.has_error_label("TransientTransactionError"):
                    logging.warning(
                        "TransientTransactionError, retrying abort operation ..."
                    )
                    continue
                raise

    # ------------------------------------------------------------------
    # Retry helpers (ASYNC)
    # ------------------------------------------------------------------
    async def _async_commit_with_retry(self, session):
        while True:
            try:
                await session.commit_transaction()
                return
            except (ConnectionFailure, OperationFailure) as exc:
                if exc.has_error_label("UnknownTransactionCommitResult"):
                    logging.warning(
                        "UnknownTransactionCommitResult, retrying commit operation ..."
                    )
                    continue
                raise

    async def _async_abort_with_retry(self, session):
        while True:
            try:
                await session.abort_transaction()
                return
            except (ConnectionFailure, OperationFailure) as exc:
                if exc.has_error_label("TransientTransactionError"):
                    logging.warning(
                        "TransientTransactionError, retrying abort operation ..."
                    )
                    continue
                raise

    # ------------------------------------------------------------------
    # Sync context manager
    # ------------------------------------------------------------------
    def __enter__(self):
        conn = get_connection(self.alias)

        self._sync_session_cm = conn.start_session(**self.session_kwargs)
        self._sync_session = self._sync_session_cm.__enter__()

        self._sync_txn_cm = self._sync_session.start_transaction(
            **self.transaction_kwargs
        )
        self._sync_txn_cm.__enter__()

        _set_session(self._sync_session)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self._commit_with_retry(self._sync_session)
            else:
                self._abort_with_retry(self._sync_session)
        finally:
            _clear_session()

            try:
                if self._sync_txn_cm is not None:
                    self._sync_txn_cm.__exit__(exc_type, exc, tb)
            finally:
                if self._sync_session_cm is not None:
                    self._sync_session_cm.__exit__(exc_type, exc, tb)

        return False  # never swallow exceptions

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------
    async def __aenter__(self):
        conn = await async_get_connection(self.alias)

        self._async_session_cm = conn.start_session(**self.session_kwargs)
        self._async_session = await self._async_session_cm.__aenter__()

        # in your environment this is a coroutine
        await self._async_session.start_transaction(**self.transaction_kwargs)

        _set_session(self._async_session)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                await self._async_commit_with_retry(self._async_session)
            else:
                await self._async_abort_with_retry(self._async_session)
        finally:
            _clear_session()
            if self._async_session_cm is not None:
                await self._async_session_cm.__aexit__(exc_type, exc, tb)

        return False
