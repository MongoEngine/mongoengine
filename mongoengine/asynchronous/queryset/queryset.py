import inspect

from .base import AsyncBaseQuerySet
from mongoengine.errors import OperationError

__all__ = (
    "AsyncQuerySet",
    "AsyncQuerySetNoCache",
)

# The maximum number of items to fetch per chunk when caching
REPR_OUTPUT_SIZE = 20
ITER_CHUNK_SIZE = 100


class AsyncQuerySet(AsyncBaseQuerySet):
    """Fully asynchronous QuerySet that wraps an async MongoDB cursor.

    This QuerySet never performs synchronous operations. All DB access must
    be awaited explicitly.
    """

    _has_more = True
    _len = None
    _result_cache = None

    # -------------------------------------------------------------
    # Representation
    # -------------------------------------------------------------
    def __repr__(self):
        """Not supported for AsyncQuerySet.

        This queryset is fully asynchronous; __repr__ cannot access the DB.
        """
        return "<AsyncQuerySet (repr not supported; use async methods)>"

    # -------------------------------------------------------------
    # Async iteration
    # -------------------------------------------------------------
    def __aiter__(self):
        """Allow `async for doc in queryset`."""
        self._iter = True
        return self._iter_results()

    async def __anext__(self):
        if self._none or self._empty:
            raise StopAsyncIteration

        try:
            cursor = await self._cursor
            raw = await cursor.__anext__()
        except StopAsyncIteration:
            raise

        # RAW pymongo mode bypass
        if self._as_pymongo:
            return raw

        # ---- SCALAR MODE: return scalar from *raw* doc ----
        if self._scalar:
            return self._get_scalar(raw)

        # ---- Normal document creation ----
        return self._document._from_son(
            raw
        )

    # -------------------------------------------------------------
    # Async internal helpers
    # -------------------------------------------------------------

    async def _populate_cache(self):
        """Populate the cache with the next chunk of results."""
        if self._result_cache is None:
            self._result_cache = []

        if not self._has_more:
            return

        try:
            for _ in range(ITER_CHUNK_SIZE):
                value = await self.__anext__()

                # If scalar returned a coroutine, await it
                if inspect.isawaitable(value):
                    value = await value

                self._result_cache.append(value)

        except StopAsyncIteration:
            self._has_more = False

    async def _iter_results(self):
        """Async generator that yields cached docs and populates when needed."""
        if self._result_cache is None:
            self._result_cache = []

        pos = 0
        while True:
            # Yield from cache first
            while pos < len(self._result_cache):
                yield self._result_cache[pos]
                pos += 1

            # No more cached results
            if not self._has_more:
                return

            # Populate more docs from cursor
            if len(self._result_cache) <= pos:
                await self._populate_cache()

    # -------------------------------------------------------------
    # Async public API
    # -------------------------------------------------------------
    async def count(self, with_limit_and_skip: bool = False):
        """Count documents asynchronously."""
        if with_limit_and_skip is False:
            return await super().count(with_limit_and_skip)

        if self._len is None:
            self._len = await super().count(with_limit_and_skip)
        return self._len

    async def len(self):
        """Compute true length asynchronously (consumes cursor)."""
        if self._len is not None:
            return self._len

        if self._has_more:
            async for _ in self._iter_results():
                pass

        self._len = len(self._result_cache)
        return self._len

    async def no_cache(self):
        """Return a non-caching async queryset."""
        if self._result_cache is not None:
            raise OperationError("QuerySet already cached")

        return self._clone_into(
            AsyncQuerySetNoCache(self._document)
        )

    async def to_list(self):
        """Return all results as a list asynchronously, respecting cache."""
        if self._result_cache is None:
            self._result_cache = []

        # Fully populate cache if cursor still active
        while self._has_more:
            await self._populate_cache()

        # Return a copy of cached results
        return list(self._result_cache)

    async def set(self):
        """Return all results as a list asynchronously, respecting cache."""
        if self._result_cache is None:
            self._result_cache = []

        # Fully populate cache if cursor still active
        while self._has_more:
            await self._populate_cache()

        # Return a copy of cached results
        return set(self._result_cache)


# --------------f----------------------------------------------------
# Non-caching async queryset
# ------------------------------------------------------------------
class AsyncQuerySetNoCache(AsyncQuerySet):
    """A non-caching async queryset.
    Iteration always streams from MongoDB and never populates or reads
    `_result_cache`, `_has_more`, or `_len`.
    """

    def __repr__(self):
        return "<AsyncQuerySetNoCache (repr not supported; use async methods)>"

    async def cache(self):
        """Convert to a normal caching AsyncQuerySet."""
        return self._clone_into(AsyncQuerySet(self._document))

    # ------------------------------------------------------------------
    # Iteration (NO CACHE)
    # ------------------------------------------------------------------
    def __aiter__(self):
        """Always return a fresh raw iterator; never use cache."""
        self._cursor_obj = None  # force a new cursor every time
        return self._iter_raw()

    async def _iter_raw(self):
        """Yield documents directly from the live MongoDB cursor."""
        cursor = await self._cursor

        async for raw in cursor:
            # RAW pymongo mode
            if self._as_pymongo:
                yield raw
                continue

            # SCALAR mode
            if self._scalar:
                yield self._get_scalar(raw)
                continue

            # Full document
            yield self._document._from_son(
                raw,
            )

    # ------------------------------------------------------------------
    # list() (NO CACHE)
    # ------------------------------------------------------------------
    async def to_list(self):
        """Return all results by re-running the query every time."""
        self._cursor_obj = None  # ensure new DB execution
        return [doc async for doc in self]

    # ------------------------------------------------------------------
    # set() (NO CACHE)
    # ------------------------------------------------------------------
    async def set(self):
        """Return all results as a set, without caching."""
        self._cursor_obj = None
        return {doc async for doc in self}

    # ------------------------------------------------------------------
    # Disable caching methods from parent
    # ------------------------------------------------------------------
    async def _populate_cache(self):
        """Do nothing. No caching."""
        return

    async def len(self):
        """Compute length without cache by counting streamed results."""
        return len([1 async for _ in self])
