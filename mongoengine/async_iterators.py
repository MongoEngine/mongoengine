"""Async iterator wrappers for MongoDB operations."""


class AsyncAggregationIterator:
    """Wrapper to make async_aggregate work with async for directly."""

    def __init__(self, queryset, pipeline, kwargs):
        self.queryset = queryset
        self.pipeline = pipeline
        self.kwargs = kwargs
        self._cursor = None

    def __aiter__(self):
        """Return self as async iterator."""
        return self

    async def __anext__(self):
        """Get next item from the aggregation cursor."""
        if self._cursor is None:
            # Lazy initialization - execute the aggregation on first iteration
            self._cursor = await self._execute_aggregation()

        return await self._cursor.__anext__()

    async def _execute_aggregation(self):
        """Execute the actual aggregation and return the cursor."""
        from mongoengine.async_utils import (
            _get_async_session,
            ensure_async_connection,
        )
        from mongoengine.connection import DEFAULT_CONNECTION_NAME

        alias = self.queryset._document._meta.get("db_alias", DEFAULT_CONNECTION_NAME)
        ensure_async_connection(alias)

        if not isinstance(self.pipeline, (tuple, list)):
            raise TypeError(
                f"Starting from 1.0 release pipeline must be a list/tuple, received: {type(self.pipeline)}"
            )

        initial_pipeline = []
        if self.queryset._none or self.queryset._empty:
            initial_pipeline.append({"$limit": 1})
            initial_pipeline.append({"$match": {"$expr": False}})

        if self.queryset._query:
            initial_pipeline.append({"$match": self.queryset._query})

        if self.queryset._ordering:
            initial_pipeline.append({"$sort": dict(self.queryset._ordering)})

        if self.queryset._limit is not None:
            initial_pipeline.append(
                {"$limit": self.queryset._limit + (self.queryset._skip or 0)}
            )

        if self.queryset._skip is not None:
            initial_pipeline.append({"$skip": self.queryset._skip})

        # geoNear and collStats must be the first stages in the pipeline if present
        first_step = []
        new_user_pipeline = []
        for step in self.pipeline:
            if "$geoNear" in step:
                first_step.append(step)
            elif "$collStats" in step:
                first_step.append(step)
            else:
                new_user_pipeline.append(step)

        final_pipeline = first_step + initial_pipeline + new_user_pipeline

        collection = await self.queryset._async_get_collection()
        if (
            self.queryset._read_preference is not None
            or self.queryset._read_concern is not None
        ):
            collection = collection.with_options(
                read_preference=self.queryset._read_preference,
                read_concern=self.queryset._read_concern,
            )

        if self.queryset._hint not in (-1, None):
            self.kwargs.setdefault("hint", self.queryset._hint)
        if self.queryset._collation:
            self.kwargs.setdefault("collation", self.queryset._collation)
        if self.queryset._comment:
            self.kwargs.setdefault("comment", self.queryset._comment)

        # Get async session if available
        session = await _get_async_session()
        if session:
            self.kwargs["session"] = session

        return await collection.aggregate(
            final_pipeline,
            cursor={},
            **self.kwargs,
        )
