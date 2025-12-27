import abc
import copy
import itertools
import re
import typing
import warnings
from collections.abc import Mapping
from typing import Union, Type

import pymongo
import pymongo.errors

from bson import SON, json_util, ObjectId, Code
from pymongo import ReturnDocument
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.asynchronous.cursor import AsyncCursor
from pymongo.common import validate_read_preference
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode
from pymongo.synchronous.command_cursor import CommandCursor
from pymongo.synchronous.cursor import Cursor

from mongoengine import signals
from mongoengine.base import _DocumentRegistry
from mongoengine.base.queryset import DENY, CASCADE, NULLIFY, PULL, transform
from mongoengine.base.queryset.pipeline_builder import PipelineBuilder, needs_aggregation
from mongoengine.common import _import_class
from mongoengine.context_managers import (
    set_write_concern, set_read_write_concern,
)
from mongoengine.errors import (
    InvalidQueryError,
    LookUpError,
    OperationError, MultipleObjectsReturned, DoesNotExist, NotUniqueError, BulkWriteError,
)

from mongoengine.base.queryset.field_list import QueryFieldList
from mongoengine.base.queryset.visitor import Q, QNode

from mongoengine.pymongo_support import LEGACY_JSON_OPTIONS

from mongoengine.session import _get_session

if typing.TYPE_CHECKING:
    from mongoengine import Document

__all__ = ("BaseQuerySet",)


class BaseQuerySet(abc.ABC):
    """BaseQuerySet for MongoDB queries.

    A set of results returned from a query. Wraps a MongoDB cursor,
    providing: class:`~mongoengine.Document` objects as the results.

    Common Patterns:
    ===============
    # Filtering (chainable, non-blocking)
    qs = User.objects(active=True).filter(age__gte=18)

    # Get single document
    user = User.objects(email='test@example.com').get()

    # Get first document or None
    user = User.objects(active=True).first()

    # Count documents
    count = User.objects(active=True).count()

    # Iterate results
    async for user in User.aobjects(age__gte=18):
        print(user.name)

    # Bulk operations
    deleted = await User.aobjects(active=False).delete()
    updated = await User.aobjects(role='admin').update(set__active=True)

    # Aggregation
    cursor = await User.aobjects.aggregate([
        {"$group": {"_id": "$status", "count": {"$sum": 1}}}
    ])
    for a result in cursor:
        print(result)

    # Field projection
    users = User.objects.only('name', 'email')
    for user in users:
        print(user.name) # Only name and email are loaded

    # Pagination (use skip/limit instead of slicing)
    first_10 = User.objects.limit(10)
    next_10 = User.objects.skip(10).limit(10)

    Attributes:
    ==========
    _document: Document class this queryset operates on
    _query_obj: Q object representing the query filters
    _mongo_query: Cached MongoDB query dictionary
    _ordering: Sort order for results
    _limit/_skip: Pagination parameters
    _loaded_fields: Field projection configuration
    _scalar: Fields for scalar/values_list mode
    _as_pymongo: Return raw dicts instead of Documents
    """

    def __init__(self, document: Type['Document']):
        """Initialize an async queryset for the given document class.

        Args:
            document: The Document class this queryset operates on
        """
        self._document = document
        self._mongo_query: dict | None = None  # Cached MongoDB query dict
        self._query_obj: Q = Q()  # MongoEngine query object
        self._cls_query: dict = {}  # Query filter for inheritance (_cls field)
        self._where_clause: str | None = None  # JavaScript $where clause
        self._loaded_fields: QueryFieldList = QueryFieldList()  # Fields to load (projection)
        self._ordering: dict | None = None  # Sort order for results
        self._snapshot: bool = False  # Deprecated snapshot mode
        self._timeout: bool = True  # Enable MongoDB cursor timeout
        self._allow_disk_use: bool = False  # Allow disk usage for large sorts
        self._read_preference: _ServerMode | None = None  # MongoDB read preference
        self._read_concern: ReadConcern | None = None  # MongoDB read concern
        self._iter: bool = False  # Iteration state flag
        self._scalar: list[str] = []  # Fields for scalar/values_list mode
        self._none: bool = False  # Return empty results without querying DB
        self._using: tuple[str, str] | None = None
        self._as_pymongo: bool = False  # Return raw pymongo dicts instead of Documents
        self._search_text: str | None = None  # Text search query
        self._search_text_score: bool = False  # Include text search scores
        self.__auto_dereference = True  # Auto-dereference references

        # If inheritance is allowed, only return instances and instances of
        # subclasses of the class being used
        if document._meta.get("allow_inheritance") is True:
            if len(self._document._subclasses) == 1:
                self._cls_query = {"_cls": self._document._subclasses[0]}
            else:
                self._cls_query = {"_cls": {"$in": self._document._subclasses}}
            self._loaded_fields = QueryFieldList(always_include=["_cls"])

        self._cursor_obj: AsyncCursor | Cursor | AsyncCommandCursor | None = None
        self._limit: int | None = None
        self._select_related = None
        self._skip: int | None = None

        self._hint: str | int = -1  # Using -1 as None is a valid value for hint
        self._collation: str | None = None
        self._batch_size: int | None = None
        self._max_time_ms: int | None = None
        self._comment: str | None = None

        # Hack - As people expect cursor[5:5] to return
        # an empty result set. It's hard to do that right, though, because the
        # server uses limit(0) to mean 'no limit'. So we set _empty
        # in that case and check for it when iterating. We also unset
        # it anytime we change _limit. Inspired by how it is done in pymongo.Cursor
        self._empty: bool = False

    def __call__(self, q_obj: Union['BaseQuerySet', None] = None, **query: dict) -> 'BaseQuerySet':
        """Filter the selected documents by calling the: class:
        `~mongoengine.queryset.BaseQuerySet` with a query.

        :param q_obj: a: class:`~mongoengine.queryset.Q` object to be used in
            the query; the: class:`~mongoengine.queryset.AsyncQuerySet` is filtered
            multiple times with different: class:`~mongoengine.queryset.Q`
            objects, only the last one will be used.
        :param query: Django-style query keyword arguments.
        """
        query = Q(**query)
        if q_obj:
            # Make sure a proper query object is passed.
            if not isinstance(q_obj, QNode):
                msg = (
                        "Not a query object: %s. "
                        "Did you intend to use key=value?" % q_obj
                )
                raise InvalidQueryError(msg)
            query &= q_obj

        queryset = self.clone()
        queryset._query_obj &= query
        queryset._mongo_query = None
        queryset._cursor_obj = None

        return queryset

    def __getstate__(self) -> dict:
        """
        Need for pickling queryset

        See https://github.com/MongoEngine/mongoengine/issues/442
        """

        obj_dict = self.__dict__.copy()

        # don't pickle cursor
        obj_dict["_cursor_obj"] = None

        return obj_dict

    def __setstate__(self, obj_dict: dict) -> None:
        """
        Need for pickling queryset

        See https://github.com/MongoEngine/mongoengine/issues/442
        """

        # update attributes
        self.__dict__.update(obj_dict)

        # force load cursor
        # self._cursor

    def __getitem__(self, key: int | slice):
        """
        Slicing or indexing applied to a QuerySet.

        Supports:
            qs[:N] → limit(N)
            qs[M:] → skip(M)
            qs[M:N] → skip(M) + limit(N-M)
            qs[i] → returns the i-th result (equivalent to skip(i).limit(1))

        Behaviour:
            • Returns a *new cloned* QuerySet — original is never modified.
            • No cursor is created here — limit/skip are only applied at query execution.
            • Allows chaining:   qs[1:5].order_by("name").only(...)
            • Fully lazy: slicing does not hit the database until iteration.

        Notes:
            - Negative indexing is NOT supported.
            - stop < start always returns an empty QuerySet.
            - If limit resolves to zero, the query becomes empty immediately.
            - This matches Django ORM slicing semantics.

        Parameters
        ----------
        key : int | slice
            Integer index or slice definition.

        Returns
        -------
        QuerySet
            A cloned queryset with applied skip/limit rules,
            OR an actual value in scalar/indexed mode.

        Raises
        ------
        TypeError
            If key is neither int nor slice.
        IndexError
            If key is an integer index beyond the result range.
        """

        queryset = self.clone()
        queryset._empty = False

        # ------------------------------
        # slice handling: qs[a:b]
        # ------------------------------
        if isinstance(key, slice):
            start = key.start or 0
            stop = key.stop

            queryset._skip = start if start > 0 else None

            if stop is not None:
                queryset._limit = max(stop - start, 0)
                if queryset._limit == 0:  # quick empty result
                    queryset._empty = True
                    return queryset
            else:
                queryset._limit = None  # open-ended LIMIT

            queryset._cursor_obj = None  # 🔥 critical: force new cursor later
            return queryset

        # ------------------------------
        # integer index: qs[i]
        # ------------------------------
        if isinstance(key, int):
            if key < 0:
                raise IndexError("Negative indexing is not supported.")

            qs = queryset.limit(1)
            if key > 0:
                qs = qs.skip(key)

            try:
                return next(qs.__iter__())
            except StopIteration:
                raise IndexError("list index out of range")

        raise TypeError("Key must be int or slice.")

    @abc.abstractmethod
    def __iter__(self) -> list['Document'] | dict:
        """Must be implemented by subclasses"""

    def __next__(self):
        """Fetch next document in async iteration.

        Async equivalent of sync BaseQuerySet's __next__ method.
        Handles scalar mode, as_pymongo mode, and normal Document mode.

        Returns:
            Document or value: Next item based on queryset mode

        Raises:
            StopAsyncIteration: When no more documents available

        Note:
            - In scalar mode: returns field value(s)
            - In as_pymongo mode: returns raw pymongo dict
            - Normal mode: returns Document instance
        """
        if self._none or self._empty:
            raise StopIteration

        try:
            raw = self._cursor.__next__()
        except StopIteration:
            raise

        if self._as_pymongo:
            return raw

        # SCALAR MODE → return raw field values, not a Document instance
        if self._scalar:
            return self._get_scalar(raw)

        # Normal mode → return Document instance
        return self._document._from_son(raw)

    def _has_data(self) -> bool:
        """Check if the queryset has any matching documents.

          Internal method used for checking data existence.

          Returns:
              bool: True if at least one document matches the query
          """
        queryset = self.order_by()
        return False if queryset.first() is None else True

    def __bool__(self) -> bool:
        return self._has_data()

    def exists(self) -> bool:
        """ Returns:
            bool: True if at least one matching document exists"""
        return self._has_data()

    # Core functions

    def all(self) -> 'BaseQuerySet':
        """Returns a copy of the current BaseQuerySet."""
        return self.__call__()

    def filter(self, *q_objs: Union['BaseQuerySet', None], **query) -> 'BaseQuerySet':
        """An alias of :meth:`~mongoengine.queryset.QuerySet.__call__`"""
        return self.__call__(*q_objs, **query)

    def search_text(self, text: str, language: str = None, text_score: bool = True) -> 'BaseQuerySet':
        """
        Start a text search, using text indexes.
        Require: MongoDB server version 2.6+.

        :param text:
        :param language:  The language that determines the list of stop words
            for the search and the rules for the stemmer and tokenizer.
            If not specified, the search uses the default language of the index.
            For supported languages, see
            `Text Search Languages <https://docs.mongodb.org/manual/reference/text-search-languages
            /#text-search-languages>`.
        :param text_score:  True to have it return the text_score (available through get_text_score()),
        False to disable that
            Note that unless you order the results, leaving text_score=True may provide randomness
            in the returned documents
        """
        queryset = self.clone()
        if queryset._search_text:
            raise OperationError("It is not possible to use search_text two times.")

        query_kwargs = SON({"$search": text})
        if language:
            query_kwargs["$language"] = language

        queryset._query_obj &= Q(__raw__={"$text": query_kwargs})
        queryset._mongo_query = None
        queryset._cursor_obj = None
        queryset._search_text = text
        queryset._search_text_score = text_score

        return queryset

    def get(self, *q_objs, **query) -> 'Document':
        """ Retrieve exactly one document matching the query.

        Sync version of BaseQuerySet.get(). Efficiently checks for
        multiple results by limiting the query to 2 documents.

        Args:
            *q_objs: Q objects for complex queries
            **query: Django-style filter arguments

        Returns:
            Document: The matching document instance

        Raises:
            DoesNotExist: If no documents match the query
            MultipleObjectsReturned: If more than one document matches

        Example:
            user = await User.aobjects.get(email='test@example.com')
            user = await User.aobjects(active=True).get(id=user_id)
        """

        queryset = self.clone()
        queryset = queryset.order_by().limit(2)
        queryset = queryset.filter(*q_objs, **query)

        # Start an async iterator over the queryset
        cursor = queryset._cursor

        try:
            if queryset._as_pymongo:
                result = next(cursor)
            else:
                result = queryset._document._from_son(
                    next(cursor),
                )
        except StopIteration:
            msg = f"{queryset._document.__name__} matching query does not exist."
            raise DoesNotExist(msg)

        try:
            next(cursor)
        except StopIteration:
            return result

        raise MultipleObjectsReturned(
            "2 or more items returned, instead of 1"
        )

    def create(self, **kwargs) -> 'Document':
        """Create and save a new document instance.
        Args:
            **kwargs: Field values for the new document

        Returns:
            Document: The created and saved document instance

        Example:
            user = await User.aobjects.create(name='John', email='john@example.com')
        """
        return self._document(**kwargs).save(force_insert=True)

    def first(self) -> Union['Document', None]:
        """Retrieve the first document matching the query.

        Sync version of BaseQuerySet.first(). Returns None if no matches are found.

        Returns:
            Document or None: First matching document, or None if no results
        """
        queryset = self.clone()

        if queryset._none or queryset._empty:
            return None

        # DO NOT TOUCH SKIP
        queryset._limit = 1
        queryset._cursor_obj = None

        cursor = queryset._cursor
        docs = cursor.to_list(length=1)

        if not docs:
            return None

        raw = docs[0]

        if queryset._as_pymongo:
            return raw

        if queryset._scalar:
            return queryset._get_scalar(raw)

        return queryset._document._from_son(
            raw,
        )

    def insert(
            self, doc_or_docs: Union['Document', list['Document']], load_bulk: bool = True,
            write_concern: dict | None = None,
            signal_kwargs: dict | None = None
    ) -> Union['Document', list['Document']]:
        """Bulk insert documents into the database.

        BaseQuerySet.insert(). Supports single or multiple
        document insertion with optional bulk loading.

        Args:
            doc_or_docs: Single document or list of documents to insert
            load_bulk: If True, returns document instances; if False, returns ObjectIds
            write_concern: MongoDB writes concern options (e.g., {w: 2, fsync: True})
            signal_kwargs: Additional kwargs for pre/post bulk insert signals

        Returns:
            Document or list: Inserted document(s) if load_bulk=True, else ObjectId(s)

        Raises:
            NotUniqueError: If duplicate key constraint is violated
            BulkWriteError: If bulk write operation fails
            OperationError: If documents are invalid or have existing ObjectIds

        Example:
            # Insert single document
            user = await User.aobjects.insert(User(name='John'))

            # Bulk insert
            users = [User(name='Alice'), User(name='Bob')]
            inserted = await User.aobjects.insert(users)
        """
        Document = _import_class("Document")

        if write_concern is None:
            write_concern = {}

        docs = doc_or_docs
        return_one = False
        if isinstance(docs, Document) or issubclass(docs.__class__, Document):
            return_one = True
            docs = [docs]
        for doc in docs:
            if not isinstance(doc, self._document):
                msg = "Some documents inserted aren't instances of %s" % str(
                    self._document
                )
                raise OperationError(msg)
            if doc.pk and not doc._created:
                msg = "Some documents have ObjectIds, use doc.update() instead"
                raise OperationError(msg)

        signal_kwargs = signal_kwargs or {}
        signals.pre_bulk_insert.send(self._document, documents=docs, **signal_kwargs)

        raw = [doc.to_mongo() for doc in docs]

        with set_write_concern(self._collection, write_concern) as collection:
            insert_func = collection.insert_many
            if return_one:
                raw = raw[0]
                insert_func = collection.insert_one

        try:
            inserted_result = insert_func(raw, session=_get_session())
            ids = (
                [inserted_result.inserted_id]
                if return_one
                else inserted_result.inserted_ids
            )
        except pymongo.errors.DuplicateKeyError as err:
            message = "Could not save document (%s)"
            raise NotUniqueError(message % err)
        except pymongo.errors.BulkWriteError as err:
            # inserting documents that already have an _id field will
            # give huge performance debt or raise
            message = "Bulk write error: (%s)"
            raise BulkWriteError(message % err.details)
        except pymongo.errors.OperationFailure as err:
            message = "Could not save document (%s)"
            if re.match("^E1100[01] duplicate key", str(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = "Tried to save duplicate unique keys (%s)"
                raise NotUniqueError(message % err)
            raise OperationError(message % err)

        # Apply inserted_ids to documents
        for doc, doc_id in zip(docs, ids):
            doc.pk = doc_id
        if not load_bulk:
            signals.post_bulk_insert.send(
                self._document, documents=docs, loaded=False, **signal_kwargs
            )
            return ids[0] if return_one else ids

        documents = self.in_bulk(ids)
        results = [documents.get(obj_id) for obj_id in ids]
        signals.post_bulk_insert.send(
            self._document, documents=results, loaded=True, **signal_kwargs
        )
        return results[0] if return_one else results

    def count(self, with_limit_and_skip: bool = False) -> int:
        """Count documents matching the query.

        Async version of BaseQuerySet.count(). Returns count of documents
        without loading them into memory.

        Args:
            with_limit_and_skip: If True, respects any limit/skip applied to queryset

        Returns:
            int: Number of documents matching the query

        Example:
            total = await User.aobjects(active=True).count()
            first_10 = await User.aobjects.limit(10).count(with_limit_and_skip=True)
        """
        # mimic the fact that setting .limit(0) in pymongo sets no limit
        # https://www.mongodb.com/docs/manual/reference/method/cursor.limit/#zero-value
        if (
                (self._limit == 0 and not with_limit_and_skip)
                or self._none
                or self._empty
        ):
            return 0

        kwargs = {}
        if with_limit_and_skip:
            if self._skip is not None:
                kwargs["skip"] = int(self._skip)
            if self._limit not in (None, 0):
                kwargs["limit"] = int(self._limit)

        # .limit(0) means "no limit"
        if self._limit == 0:
            kwargs.pop("limit", None)

        if self._hint not in (-1, None):
            kwargs["hint"] = self._hint

        if self._collation is not None:
            kwargs["collation"] = self._collation

        # Ensure we await the async collection
        collection = self._collection
        try:
            count = collection.count_documents(self._query, **kwargs, session=_get_session())
        except pymongo.errors.OperationFailure as err:
            message = "Could not count documents (%s)"
            raise OperationError(message % err) from err
        # Reset cached cursor so future queries rebuild correctly
        self._cursor_obj = None
        return count

    def delete(self, write_concern: dict | None = None, _from_doc_delete: bool = False, cascade_refs: set[str] = None):
        """Delete documents matching the query.

        BaseQuerySet.delete(). Handles delete rules (CASCADE,
        NULLIFY, PULL, DENY) and signals if configured.

        Args:
            write_concern: MongoDB write concern options
            _from_doc_delete: Internal flag indicating call from document.delete()
            cascade_refs: Set of already-cascaded reference IDs (prevents infinite loops)

        Returns:
            int: Number of documents deleted (if write concern is acknowledged)

        Raises:
            OperationError: If DENY rule blocks deletion
         Example:
            deleted = await User.objects(active=False).delete()
            print(f"Deleted {deleted} inactive users")
        """
        queryset = self.clone()
        doc = queryset._document
        if write_concern is None:
            write_concern = {}

        # Handle deletes where skips or limits have been applied or
        # there is an untriggered delete signal
        has_delete_signal = signals.signals_available and (
                signals.pre_delete.has_receivers_for(doc)
                or signals.post_delete.has_receivers_for(doc)
        )

        call_document_delete = (
                                       queryset._skip or queryset._limit or has_delete_signal
                               ) and not _from_doc_delete

        if call_document_delete:
            cnt = 0
            for doc in queryset:
                doc.delete(**write_concern)
                cnt += 1
            return cnt

        delete_rules = doc._meta.get("delete_rules") or {}
        delete_rules = list(delete_rules.items())

        # Check for DENY rules before actually deleting/nullifying any other
        # references
        for rule_entry, rule in delete_rules:
            document_cls, field_name = rule_entry
            if document_cls._meta.get("abstract"):
                continue

            if rule == DENY:
                refs = document_cls.objects(**{field_name + "__in": self})
                if refs.limit(1).count() > 0:
                    raise OperationError(
                        "Could not delete document (%s.%s refers to it)"
                        % (document_cls.__name__, field_name)
                    )
        # Check all the other rules
        for rule_entry, rule in delete_rules:
            document_cls, field_name = rule_entry
            if document_cls._meta.get("abstract"):
                continue

            if rule == CASCADE:
                cascade_refs = set() if cascade_refs is None else cascade_refs
                # Handle recursive reference
                if doc._get_collection_name() == document_cls._get_collection_name():
                    for ref in queryset:
                        cascade_refs.add(ref.id)
                refs = document_cls.objects(
                    **{field_name + "__in": self, "pk__nin": cascade_refs}
                )
                if refs.count() > 0:
                    refs.delete(write_concern=write_concern, cascade_refs=cascade_refs)
            elif rule == NULLIFY:
                document_cls.objects(**{field_name + "__in": self}).update(
                    write_concern=write_concern, **{"unset__%s" % field_name: 1}
                )
            elif rule == PULL:
                document_cls.objects(**{field_name + "__in": self}).update(
                    write_concern=write_concern, **{"pull_all__%s" % field_name: self}
                )

        kwargs = {}
        if self._hint not in (-1, None):
            kwargs["hint"] = self._hint
        if self._collation:
            kwargs["collation"] = self._collation
        if self._comment:
            kwargs["comment"] = self._comment

        with set_write_concern(queryset._collection, write_concern) as collection:
            result = collection.delete_many(
                queryset._query,
                session=_get_session(),
                **kwargs,
            )

            # If we're using an unack'd write concern, we don't really know how
            # many items have been deleted at this point, hence we only return
            # the count for ack'd ops.
            if result.acknowledged:
                return result.deleted_count

    def update(
            self,
            upsert: bool = False,
            multi: bool = True,
            write_concern: dict | None = None,
            read_concern: ReadConcern | None = None,
            full_result: bool = False,
            array_filters: dict | None = None,
            **update: dict,
    ):
        """Perform atomic update on documents matching the query.

        Async version of BaseQuerySet.update(). Supports MongoDB update operators
        via Django-style syntax (set__, inc__, push__, etc.)

        Args:
            upsert: Insert a document if no match exists
            multi: Update multiple documents (False = update first match only)
            write_concern: MongoDB write concern options
            read_concern: MongoDB read concern for the operation
            full_result: Return UpdateResult object instead of count
            array_filters: Filters for updating array elements
            **update: Update operations (e.g., set__name='John', inc__age=1)

        Returns:
            int or UpdateResult: Number updated (or UpdateResult if full_result=True)

        Raises:
            NotUniqueError: If an update causes duplicate key violation,
            OperationError: If an update fails or no update params are provided

         Example:
            # Simple update
            count = User.objects(active=False).update(set__active=True)

            # Increment field
            Post.objects(id=post_id).update(inc__views=1)

            # Array operations
            User.objects(id=uid).update(push__tags='python')
        """
        if not update and not upsert:
            raise OperationError("No update parameters, would remove data")

        if write_concern is None:
            write_concern = {}
        if self._none or self._empty:
            return 0

        queryset = self.clone()
        query = queryset._query
        if "__raw__" in update and isinstance(
                update["__raw__"], list
        ):  # Case of Update with Aggregation Pipeline
            update = [
                transform.update(queryset._document, **{"__raw__": u})
                for u in update["__raw__"]
            ]
        else:
            update = transform.update(queryset._document, **update)
        # If doing an atomic upsert on an inheritable class
        # then ensure we add _cls to the update operation
        if upsert and "_cls" in query:
            if "$set" in update:
                update["$set"]["_cls"] = queryset._document.__name__
            else:
                update["$set"] = {"_cls": queryset._document.__name__}

        kwargs = {}
        if self._hint not in (-1, None):
            kwargs["hint"] = self._hint
        if self._collation:
            kwargs["collation"] = self._collation
        if self._comment:
            kwargs["comment"] = self._comment

        try:
            with set_read_write_concern(
                    queryset._collection, write_concern, read_concern
            ) as collection:
                update_func = collection.update_one
                if multi:
                    update_func = collection.update_many
                result = update_func(
                    query,
                    update,
                    upsert=upsert,
                    array_filters=array_filters,
                    session=_get_session(),
                    **kwargs,
                )
            if full_result:
                return result
            elif result.raw_result:
                return result.raw_result["n"]
        except pymongo.errors.DuplicateKeyError as err:
            raise NotUniqueError("Update failed (%s)" % err)
        except pymongo.errors.OperationFailure as err:
            if str(err) == "multi not coded yet":
                message = "update() method requires MongoDB 1.1.3+"
                raise OperationError(message)
            raise OperationError("Update failed (%s)" % err)

    def upsert_one(self, write_concern: dict | None = None, read_concern: ReadConcern | None = None, **update: dict):
        """Overwrite or add the first document matched by the query.

        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force a fsync on the primary server.
        :param read_concern: Override the read concern for the operation
        :param update: Django-style update keyword arguments

        :returns the new or overwritten document
        """
        atomic_update = self.update(
            multi=False,
            upsert=True,
            write_concern=write_concern,
            read_concern=read_concern,
            full_result=True,
            **update,
        )

        if atomic_update.raw_result["updatedExisting"]:
            document = self.get()
        else:
            document = self._document.objects.with_id(atomic_update.upserted_id)
        return document

    def update_one(
            self,
            upsert=False,
            write_concern=None,
            full_result=False,
            array_filters=None,
            **update,
    ):
        """Perform an atomic update on the fields of the first document
        matched by the query.

        :param upsert: Insert if a document doesn't exist (default ``False``)
        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param full_result: Return the associated ``pymongo.UpdateResult`` rather than just the number of
            updated items
        :param array_filters: A list of filters specifying which array elements an update should apply.
        :param update: Django-style update keyword arguments
            full_result
        :returns the number of updated documents (unless ``full_result`` is True)
        """
        return self.update(
            upsert=upsert,
            multi=False,
            write_concern=write_concern,
            full_result=full_result,
            array_filters=array_filters,
            **update,
        )

    def modify(
            self,
            upsert: bool = False,
            remove: bool = False,
            new: bool = False,
            array_filters: dict | None = None,
            **update: dict,
    ):
        """Update and return the updated document.

        Returns either the document before or after modification based on the ` new `
        parameter. If no documents match the query and `upsert` is false,
        returns ``None``. If upserting and `new` is false, returns ``None``.

        :param upsert: insert if a document doesn't exist (default ``False``)
        :param remove: remove rather than updating (default ``False``)
        :param new: return updated rather than the original document
            (default ``False``)
        :param array_filters: A list of filters specifying which array elements an update should apply.
        :param update: Django-style update keyword arguments
        """
        if remove and new:
            raise OperationError("Conflicting parameters: remove and new")

        if not update and not upsert and not remove:
            raise OperationError("No update parameters, must either update or remove")

        if self._none or self._empty:
            return None

        queryset = self.clone()
        query = queryset._query

        if self._where_clause:
            where_clause = self._sub_js_fields(self._where_clause)
            query["$where"] = where_clause

        if not remove:
            update = transform.update(queryset._document, **update)
        sort = queryset._ordering

        try:
            if remove:
                result = queryset._collection.find_one_and_delete(
                    query, sort=sort, session=_get_session(), **self._cursor_args
                )
            else:
                if new:
                    return_doc = ReturnDocument.AFTER
                else:
                    return_doc = ReturnDocument.BEFORE
                result = queryset._collection.find_one_and_update(
                    query,
                    update,
                    upsert=upsert,
                    sort=sort,
                    return_document=return_doc,
                    session=_get_session(),
                    array_filters=array_filters,
                    **self._cursor_args,
                )
        except pymongo.errors.DuplicateKeyError as err:
            raise NotUniqueError("Update failed (%s)" % err)
        except pymongo.errors.OperationFailure as err:
            raise OperationError("Update failed (%s)" % err)

        if result is not None:
            result = self._document._from_son(result)

        return result

    def with_id(self, object_id: ObjectId):
        """Retrieve the object matching the id provided.  Uses `object_id` only
        and raises InvalidQueryError if a filter has been applied. Returns
        `None` if no document exists with that id.

        :param object_id: the value for the id of the document to look up
        """
        queryset = self.clone()
        if queryset._query_obj:
            msg = "Cannot use a filter whilst using `with_id`"
            raise InvalidQueryError(msg)
        return queryset.filter(pk=object_id).first()

    def in_bulk(self, object_ids: list[ObjectId] | tuple[ObjectId]):
        """Retrieve multiple documents by their IDs in a single query.

        Async version of BaseQuerySet.in_bulk(). Efficient bulk loading
        by fetching all documents in one database round trip.

        Args:
            object_ids: List or tuple of ObjectIds to fetch

        Returns:
            dict: Mapping of ObjectId to Document instances

         Example:
            # Fetch multiple users by ID efficiently
            user_ids = [ObjectId(...), ObjectId(...)]
            users_dict = await User.objects.in_bulk(user_ids)

            for user_id, user in users_dict.items():
                print(f"{user_id}: {user.name}")

        Note:
            Respects scalar() and as_pymongo() modes if set
        """
        doc_map = {}

        collection = self._collection  # this part *is* awaitable

        cursor = collection.find(
            {"_id": {"$in": object_ids}},
            session=_get_session(),
            **self._cursor_args,
        )

        # Case 1: scalar mode
        if self._scalar:
            for raw in cursor:
                doc_map[raw["_id"]] = self._get_scalar(raw)
            return doc_map

        # Case 2: return raw pymongo documents
        if self._as_pymongo:
            for doc in cursor:
                doc_map[doc["_id"]] = doc
            return doc_map

        # Case 3: normal document return
        for doc in cursor:
            doc_map[doc["_id"]] = self._document._from_son(
                doc,
            )

        return doc_map

    def none(self) -> 'BaseQuerySet':
        """Returns a queryset that never returns any objects, and no query will be executed when accessing the results
        inspired by django none() https://docs.djangoproject.com/en/dev/ref/models/querysets/#none
        """
        queryset = self.clone()
        queryset._none = True
        return queryset

    def no_sub_classes(self) -> 'BaseQuerySet':
        """Filter for only the instances of this specific document.

        Do NOT return any inherited documents.
        """
        if self._document._meta.get("allow_inheritance") is True:
            self._cls_query = {"_cls": self._document._class_name}

        return self

    def using(self, alias: str | None = None, collection_name: str = None) -> 'BaseQuerySet':
        """This method is for controlling which database the QuerySet will be
        evaluated against if you are using more than one database.

        :param alias: The database alias
        :param collection_name:
        """
        queryset = self.clone()
        queryset._using = (alias, collection_name)
        return queryset

    def clone(self) -> 'BaseQuerySet':
        """Create a copy of the current queryset."""
        return self._clone_into(self.__class__(self._document))

    def _clone_into(self, new_qs: 'BaseQuerySet') -> 'BaseQuerySet':
        if not isinstance(new_qs, BaseQuerySet):
            raise OperationError(
                "%s is not a subclass of BaseQuerySet" % new_qs.__name__
            )

        copy_props = (
            "_mongo_query",
            "_cls_query",
            "_none",
            "_query_obj",
            "_where_clause",
            "_loaded_fields",
            "_ordering",
            "_snapshot",
            "_timeout",
            "_allow_disk_use",
            "_read_preference",
            "_read_concern",
            "_iter",
            "_scalar",
            "_as_pymongo",
            "_limit",
            "_skip",
            "_empty",
            "_hint",
            "_collation",
            "_search_text",
            "_search_text_score",
            "_max_time_ms",
            "_comment",
            "_batch_size",
            "_using",
            "_select_related",
        )

        for prop in copy_props:
            val = getattr(self, prop)

            if prop == "_loaded_fields":
                setattr(new_qs, prop, copy.deepcopy(val))
                continue

            setattr(new_qs, prop, copy.copy(val))

        new_qs.__auto_dereference = self._BaseQuerySet__auto_dereference

        if self._cursor_obj:
            new_qs._cursor_obj = self._cursor_obj.clone()

        return new_qs

    def select_related(self, *fields: str):
        """
        Enable eager-loading of reference fields using aggregation $lookup.

        Args:
            *fields: dotted paths of reference fields to preload.
                     Examples:
                        select_related("author")
                        select_related("author.country")
                        select_related("comments.user")

        Returns:
            QuerySet — clone with select_related instructions

        Behavior:
            Without select_related → LazyReference returned
            With select_related → referenced documents are $lookup joined

        Example:
            # N+1 queries avoided:
            books = Book.objects.select_related("author")
            for b in books:
                print(b.author.name) # does NOT trigger DB hit
        """
        qs = self.clone()
        qs._select_related = qs._select_related or set()
        for p in fields:
            parts = p.split("__")
            self._document._validate_related_chain(parts)
        qs._select_related = fields  # <---- only validation
        return qs

    def limit(self, n: int) -> 'BaseQuerySet':
        """Limit the number of returned documents to `n`. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[:5]``).

        :param n: The maximum number of objects to return if n is greater than 0.
        When 0 is passed, returns all the documents in the cursor
        """
        queryset = self.clone()
        queryset._limit = n
        queryset._empty = False  # cancels the effect of empty

        # If a cursor object has already been created, apply the limit to it.
        if queryset._cursor_obj:
            queryset._cursor_obj.limit(queryset._limit)

        # if queryset._limit == 0:
        #     queryset._empty = True

        return queryset

    def skip(self, n: int) -> 'BaseQuerySet':
        """Skip `n` documents before returning the results. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[5: ]``).

        :param n: The number of objects to skip before returning results
        """
        queryset = self.clone()
        queryset._skip = n

        # If a cursor object has already been created, apply the skip to it.
        if queryset._cursor_obj:
            queryset._cursor_obj.skip(queryset._skip)

        return queryset

    def hint(self, index: str | None = None) -> 'BaseQuerySet':
        """Added 'hint' support, telling Mongo the proper index to use for the
        query.

        Judicious use of hints can greatly improve query performance. When
        doing a query on multiple fields (at least one of which is indexed)
        pass the indexed field as a hint to the query.

        Hinting will not do anything if the corresponding index does not exist.
        The last hint applied to this cursor takes precedence over all others.
        """
        queryset = self.clone()
        queryset._hint = index

        # If a cursor object has already been created, apply the hint to it.
        if queryset._cursor_obj:
            queryset._cursor_obj.hint(queryset._hint)

        return queryset

    def collation(self, collation=None):
        """
        Collation allows users to specify language-specific rules for string
        comparison, such as rules for lettercase and accent marks.
        :param collation: `~pymongo.collation.Collation` or dict with
        the following fields:
            {
                locale: str,
                caseLevel: bool,
                caseFirst: str,
                strength: int,
                numericOrdering: bool,
                alternate: str,
                maxVariable: str,
                backwards: str
            }
        Collation should be added to indexes like in the test example
        """
        queryset = self.clone()
        queryset._collation = collation

        if queryset._cursor_obj:
            queryset._cursor_obj.collation(collation)

        return queryset

    def batch_size(self, size):
        """Limit the number of documents returned in a single batch (each
        batch requires a round trip to the server).

        See https://pymongo.readthedocs.io/en/stable/api/pymongo/cursor.html#pymongo.cursor.Cursor
        for details.

        :param size: Desired size of each batch.
        """
        queryset = self.clone()
        queryset._batch_size = size

        # If a cursor object has already been created, apply the batch size to it.
        if queryset._cursor_obj:
            queryset._cursor_obj.batch_size(queryset._batch_size)

        return queryset

    def distinct(self, field):
        queryset = self.clone()

        # normalize db field name
        try:
            field = self._fields_to_dbfields([field]).pop()
        except LookUpError:
            pass

        # --------------------------------------------------------------
        # CASE 1: simple distinct (no aggregation)
        # --------------------------------------------------------------
        if not needs_aggregation(queryset):
            cursor = queryset._cursor
            raw_values = cursor.distinct(field)

            # === Determine the correct doc_field ===
            parts = field.split(".")
            top = parts[0]
            doc_field = self._document._fields.get(top)

            from mongoengine.fields import (
                EmbeddedDocumentField,
                ListField,
                ReferenceField,
            )

            # Walk into nested fields
            instance = None
            if isinstance(doc_field, ListField):
                doc_field = doc_field.field  # unwrap ListField
            if isinstance(doc_field, EmbeddedDocumentField):
                instance = doc_field.document_type

            for part in parts[1:]:
                if instance and isinstance(doc_field, EmbeddedDocumentField):
                    doc_field = instance._fields.get(part)
                    instance = doc_field.document_type if isinstance(doc_field, EmbeddedDocumentField) else None
                elif isinstance(doc_field, EmbeddedDocumentField):
                    instance = doc_field.document_type
                    doc_field = instance._fields.get(part)
                    instance = doc_field.document_type if isinstance(doc_field, EmbeddedDocumentField) else None
                elif isinstance(doc_field, ListField):
                    doc_field = doc_field.field

            # === Now doc_field is correct ===

            # CASE: EmbeddedDocumentField → build from SON
            if isinstance(doc_field, EmbeddedDocumentField):
                model = doc_field.document_type
                return [model(**v) for v in raw_values if isinstance(v, dict)]

            # CASE: ListField(EmbeddedDocumentField)
            if isinstance(doc_field, ListField) and isinstance(doc_field.field, EmbeddedDocumentField):
                model = doc_field.field.document_type
                return [model(**v) for v in raw_values if isinstance(v, dict)]

            # CASE: ReferenceField → dereference or not
            if isinstance(doc_field, ReferenceField):
                if not self._auto_dereference:
                    return raw_values

                ids = raw_values
                objs = doc_field.document_type.objects.in_bulk(ids)
                return [objs[i] for i in ids if i in objs]

            # default: scalar values
            return raw_values

        # --------------------------------------------------------------
        # CASE 2: aggregation pipeline distinct
        # --------------------------------------------------------------
        pipeline_builder = PipelineBuilder(queryset=queryset, max_depth=2)
        pipeline = pipeline_builder.build()

        # Detect shape of field
        doc_field = self._document._fields.get(field)

        # --------------------------------------------------------------
        # SCALAR DISTINCT  → NO $unwind needed, safe
        # --------------------------------------------------------------
        from mongoengine.fields import ListField, EmbeddedDocumentField, ReferenceField

        if not isinstance(doc_field, ListField):
            # scalar distinct
            pipeline += [
                {"$group": {"_id": f"${field}"}},
                {"$replaceRoot": {"newRoot": {"value": "$_id"}}},
                {"$project": {"_id": 0}}
            ]

            coll = queryset._collection
            raw = coll.aggregate(pipeline).to_list(None)
            raw_vals = [d["value"] for d in raw]

            # EmbeddedDocument scalar
            if isinstance(doc_field, EmbeddedDocumentField):
                t = doc_field.document_type
                return [t._from_son(v) for v in raw_vals]

            # ReferenceField scalar
            if isinstance(doc_field, ReferenceField):
                t = doc_field.document_type
                if raw_vals and not isinstance(raw_vals[0], ObjectId):
                    return [t._from_son(v) for v in raw_vals]
                return [v["_id"] if isinstance(v, dict) else v for v in raw_vals]

            return raw_vals

        # --------------------------------------------------------------
        # LIST FIELD DISTINCT (correct unwinding)
        # --------------------------------------------------------------
        pipeline += [
            {"$unwind": f"${field}"},
            {"$group": {"_id": f"${field}"}},
            {"$replaceRoot": {"newRoot": {"value": "$_id"}}},
            {"$project": {"_id": 0}}
        ]

        coll = queryset._collection
        raw = coll.aggregate(pipeline).to_list(None)
        raw_vals = [d["value"] for d in raw]

        # list of embedded
        if isinstance(doc_field.field, EmbeddedDocumentField):
            t = doc_field.field.document_type
            return [t._from_son(v) for v in raw_vals]

        # list of references
        if isinstance(doc_field.field, ReferenceField):
            t = doc_field.field.document_type
            if raw_vals and not isinstance(raw_vals[0], ObjectId):
                return [t._from_son(v) for v in raw_vals]
            return [v["_id"] if isinstance(v, dict) else v for v in raw_vals]

        return raw_vals

    def only(self, *fields):
        """Load only a subset of this document's fields. ::

            Post = BlogPost.objects(...).only('title', 'author.name')

        . Note: `only()` is chainable and will perform a union ::
            So with the following it will fetch both: `title` and `author.name`::

                Post = BlogPost.objects.only('title').only('author.name')

        :func:`~mongoengine.queryset.QuerySet.all_fields` will reset any
        field filters.

        :param fields: Fields to include
        """
        fields = {f: QueryFieldList.ONLY for f in fields}
        return self.fields(True, **fields)

    def exclude(self, *fields):
        """Opposite to .only(), exclude some document's fields. ::

            Post = BlogPost.objects(...).exclude('comments')

        . Note: `exclude()` is chainable and will perform a union :
            So with the following it will exclude both: `title` and `author.name`::

                Post = BlogPost.objects.exclude('title').exclude('author.name')

        :func:`~mongoengine.queryset.QuerySet.all_fields` will reset any
        field filters.

        :param fields: Fields to exclude
        """
        fields = {f: QueryFieldList.EXCLUDE for f in fields}
        return self.fields(**fields)

    def fields(self, _only_called=False, **kwargs):
        """Manipulate how you load this document's fields. Used by `.only()`
        and `.exclude()` to manipulate which fields to retrieve. If called
        directly, use a set of kwargs similar to the MongoDB projection
        document. For example:

        Include only a subset of fields:

            posts = BlogPost.objects(...).fields(author=1, title=1)

        Exclude a specific field:

            posts = BlogPost.objects(...).fields(comments=0)

        To retrieve a subrange or sublist of array elements,
        support exists for both the `slice` and `elemMatch` projection operator:

            posts = BlogPost.objects(...).fields(slice__comments=5)
            posts = BlogPost.objects(...).fields(elemMatch__comments="test")

        :param kwargs: A set of keyword arguments identifying what to
            include, exclude, or slice.
        """

        # Check for an operator and transform to mongo-style if there is
        operators = ["slice", "elemMatch"]
        cleaned_fields = []
        for key, value in kwargs.items():
            parts = key.split("__")
            if parts[0] in operators:
                op = parts.pop(0)
                value = {"$" + op: value}
            key = ".".join(parts)
            cleaned_fields.append((key, value))

        # Sort fields by their values, explicitly excluded fields first, then
        # explicitly included, and then more complicated operators such as
        # $slice.
        def _sort_key(field_tuple):
            _, value = field_tuple
            if isinstance(value, int):
                return value  # 0 for exclusion, 1 for inclusion
            return 2  # so that complex values appear last

        fields = sorted(cleaned_fields, key=_sort_key)

        # Clone the queryset, group all fields by their value, convert
        # each of them to db_fields, and set the queryset's _loaded_fields
        queryset = self.clone()
        for value, group in itertools.groupby(fields, lambda x: x[1]):
            fields = [field for field, value in group]
            fields = queryset._fields_to_dbfields(fields)
            queryset._loaded_fields += QueryFieldList(
                fields, value=value, _only_called=_only_called
            )

        # ---- FIX: ensure `_id` is always included for ONLY(...) ----
        if _only_called:
            lf = queryset._loaded_fields

            # If a user explicitly excluded `_id`, keep it excluded
            if lf._id == QueryFieldList.EXCLUDE:
                return queryset.exclude("_id")

            # If `_id` already included, done
            if lf._id == QueryFieldList.ONLY:
                return queryset

        return queryset

    def all_fields(self):
        """Include all fields. Reset all previous calls of .only() or
        .exclude(). ::

            post = BlogPost.objects.exclude('comments').all_fields()
        """
        queryset = self.clone()
        queryset._loaded_fields = QueryFieldList(
            always_include=queryset._loaded_fields.always_include
        )
        return queryset

    def order_by(self, *keys, __raw__=None):
        """Order the :class:`~mongoengine.queryset.BaseQuerySet` by the given keys.

        The order may be specified by prepending each of the keys by a "+" or
        a "-". Ascending order is assumed if there's no prefix.

        If no keys are passed, existing ordering is cleared instead.

        :param keys: Fields to order the query results by; keys may be
            prefixed with "+" or a "-" to determine the ordering direction.
        :param __raw__: A raw pymongo "sort" argument (provided as a list of (key, direction))
            see 'key_or_list' in `pymongo.cursor.Cursor.sort doc
            <https://pymongo.readthedocs.io/en/stable/api/pymongo/cursor.html#pymongo.cursor.Cursor.sort>`.
            If both keys and __raw__ are provided, an exception is raised
        """
        if __raw__ and keys:
            raise OperationError("Can not use both keys and __raw__ with order_by() ")

        queryset = self.clone()
        old_ordering = queryset._ordering
        if __raw__:
            new_ordering = __raw__
        else:
            new_ordering = queryset._get_order_by(keys)

        if queryset._cursor_obj:
            # If a cursor object has already been created, apply the sort to it
            if new_ordering:
                queryset._cursor_obj.sort(new_ordering)

            # If we're trying to clear a previous explicit ordering, we need
            # to clear the cursor entirely (because PyMongo doesn't allow
            # clearing an existing sort on a cursor).
            elif old_ordering:
                queryset._cursor_obj = None

        queryset._ordering = new_ordering

        return queryset

    def clear_cls_query(self):
        """Clear the default "_cls" query.

        By default, all queries generated for documents that allow inheritance
        include an extra "_cls" clause. In most cases this is desirable, but
        sometimes you might achieve better performance if you clear that
        default query.

        Scan the code for `_cls_query` to get more details.
        """
        queryset = self.clone()
        queryset._cls_query = {}
        return queryset

    def comment(self, text):
        """Add a comment to the query.

        See https://www.mongodb.com/docs/manual/reference/method/cursor.comment/
        for details.
        """
        return self._chainable_method("comment", text)

    def explain(self):
        """Return an explain plan record for the: class:`~mongoengine.queryset.BaseQuerySet` cursor.
        """
        return self._cursor.explain()

    def allow_disk_use(self, enabled):
        """Enable or disable the use of temporary files on disk while processing a blocking sort operation.
         (To store data exceeding the 100-megabyte system memory limit)

        :param enabled: Whether temporary files on disk are used
        """
        queryset = self.clone()
        queryset._allow_disk_use = enabled
        return queryset

    def timeout(self, enabled):
        """Enable or disable the default mongod timeout when querying. (no_cursor_timeout option)

        :param enabled: whether the timeout is used
        """
        queryset = self.clone()
        queryset._timeout = enabled
        return queryset

    def read_preference(self, read_preference):
        """Change the read_preference when querying.

        :param read_preference: Override ReplicaSetConnection-level
            preference.
        """
        validate_read_preference("read_preference", read_preference)
        queryset = self.clone()
        queryset._read_preference = read_preference
        queryset._cursor_obj = None  # we need to re-create the cursor object whenever we apply read_preference # todo check this
        return queryset

    def read_concern(self, read_concern):
        """Change the read_concern when querying.

        :param read_concern: Override ReplicaSetConnection-level
            preference.
        """
        if read_concern is not None and not isinstance(read_concern, Mapping):
            raise TypeError(f"{read_concern!r} is not a valid read concern.")

        queryset = self.clone()
        queryset._read_concern = (
            ReadConcern(**read_concern) if read_concern is not None else None
        )
        queryset._cursor_obj = None  # todo we need to re-create the cursor object whenever we apply read_concern
        return queryset

    def scalar(self, *fields):
        """Instead of returning Document instances, return either a specific
        value or a tuple of values in order.

        Can be used along with: func:`~mongoengine.queryset.BaseQuerySet.no_dereference` to turn off
        dereferencing.

        . Note: This affects all results and can be unset by calling
                  ``scalar`` without arguments. Calls ``only`` automatically.

        :param fields: One or more fields to return instead of a Document.
        """
        queryset = self.clone()
        queryset._scalar = list(fields)

        if fields:
            queryset = queryset.only(*fields)
        else:
            queryset = queryset.all_fields()

        return queryset

    def values_list(self, *fields):
        """An alias for scalar"""
        return self.scalar(*fields)

    def as_pymongo(self):
        """Instead of returning Document instances, return raw values from
        pymongo.

        This method is particularly useful if you don't need dereferencing
        and care primarily about the speed of data retrieval.
        """
        queryset = self.clone()
        queryset._as_pymongo = True
        return queryset

    def max_time_ms(self, ms):
        """Wait `ms` milliseconds before killing the query on the server

        :param ms: the number of milliseconds before killing the query on the server
        """
        if ms is not None and not isinstance(ms, int):
            raise TypeError("max_time_ms() only accepts int or None")
        return self._chainable_method("max_time_ms", ms)

    # JSON Helpers

    def to_json(self, *args, **kwargs):
        """Converts a queryset to JSON"""
        if "json_options" not in kwargs:
            warnings.warn(
                "No 'json_options' are specified! Falling back to "
                "LEGACY_JSON_OPTIONS with uuid_representation=PYTHON_LEGACY. "
                "For use with other MongoDB drivers specify the UUID "
                "representation to use. This will be changed to "
                "uuid_representation=UNSPECIFIED in a future release.",
                DeprecationWarning,
                stacklevel=2,
            )
            kwargs["json_options"] = LEGACY_JSON_OPTIONS
        return json_util.dumps([a for a in self.as_pymongo()], *args, **kwargs)

    def from_json(self, json_data: str):
        """Converts json data to unsaved objects"""
        son_data = json_util.loads(json_data)
        return [self._document._from_son(data) for data in son_data]

    def aggregate(self, pipeline: list[dict], **kwargs):
        """Execute the MongoDB aggregation pipeline on the queryset.

        Async version of BaseQuerySet.aggregate(). Combines queryset filters
        with the provided aggregation pipeline.

        Important Notes:
        - Queryset filters are automatically prepended to your pipeline as $match
        - Ordering, limits, and skips are also prepended
        - For critical pipelines, use Document._collection.aggregate() directly
          for full control

        Args:
            pipeline: List of aggregation pipeline stages
            **kwargs: Additional options passed to pymongo's aggregate()

        Returns:
            CommandCursor: Async cursor over aggregation results

        Raises:
            TypeError: If a pipeline is not a list or tuple

        Note:
            geoNear and collStats must be first in the pipeline if used
        """
        if not isinstance(pipeline, (tuple, list)):
            raise TypeError(
                f"Starting from 1.0 release pipeline must be a list/tuple, received: {type(pipeline)}"
            )

        initial_pipeline = []
        if self._none or self._empty:
            initial_pipeline.append({"$limit": 1})
            initial_pipeline.append({"$match": {"$expr": False}})

        if self._query:
            initial_pipeline.append({"$match": self._query})

        if self._ordering:
            initial_pipeline.append({"$sort": dict(self._ordering)})

        if self._limit is not None:
            # As per MongoDB Documentation (https://www.mongodb.com/docs/manual/reference/operator/aggregation/limit/),
            # keeping limit stage right after sort stage is more efficient. But this leads to a wrong set of documents
            # for a skip stage that might succeed these. So we need to maintain more documents in memory in such a
            # case (https://stackoverflow.com/a/24161461).
            initial_pipeline.append({"$limit": self._limit + (self._skip or 0)})

        if self._skip is not None:
            initial_pipeline.append({"$skip": self._skip})

        # geoNear and collStats must be the first stages in the pipeline if present
        first_step = []
        new_user_pipeline = []
        for step_step in pipeline:
            if "$geoNear" in step_step:
                first_step.append(step_step)
            elif "$collStats" in step_step:
                first_step.append(step_step)
            else:
                new_user_pipeline.append(step_step)

        final_pipeline = first_step + initial_pipeline + new_user_pipeline

        collection = self._collection
        if self._read_preference is not None or self._read_concern is not None:
            collection = self._collection.with_options(
                read_preference=self._read_preference, read_concern=self._read_concern
            )

        if self._hint not in (-1, None):
            kwargs.setdefault("hint", self._hint)
        if self._collation:
            kwargs.setdefault("collation", self._collation)
        if self._comment:
            kwargs.setdefault("comment", self._comment)
        return collection.aggregate(
            final_pipeline,
            cursor={},
            session=_get_session(),
            **kwargs,
        )

    # JS functionality
    def map_reduce(
            self, map_f, reduce_f, output, finalize_f=None, limit=None, scope=None
    ):
        """Execute the map-reduce operation on the queryset."""
        queryset = self.clone()
        MapReduceDocument = _import_class("MapReduceDocument")
        collection_name = queryset._document._get_collection_name()

        # ------- Normalize JavaScript -------
        def _to_code(fn, scope=None):
            if isinstance(fn, Code):
                fn_scope = fn.scope or {}
                fn = str(fn)
            else:
                fn_scope = scope or {}
            return Code(queryset._sub_js_fields(fn), fn_scope or None)

        map_f = _to_code(map_f)
        reduce_f = _to_code(reduce_f)
        if finalize_f:
            finalize_f = _to_code(finalize_f)

        # ------- Build query -------
        query = queryset._query
        mr_args = {"query": query}
        if finalize_f:
            mr_args["finalize"] = finalize_f
        if scope:
            mr_args["scope"] = scope
        if limit:
            mr_args["limit"] = limit

        # ------- Determine OUTPUT DB -------
        if isinstance(output, dict) and "db_alias" in output:
            from mongoengine import get_db
            output_db = get_db(output["db_alias"])
        else:
            output_db = queryset._document._get_db()

        # ------- Build OUT spec -------
        if output == "inline" and not queryset._ordering:
            out_spec = {"inline": 1}
            inline = True
        else:
            inline = False
            if isinstance(output, str):
                # simple string name => replace
                out_spec = {"replace": output, "db": output_db.name}
            else:
                # dict form {"replace": "x", "db_alias": "test2"}
                out_spec = {}
                if "replace" in output:
                    out_spec["replace"] = output["replace"]
                elif "reduce" in output:
                    out_spec["reduce"] = output["reduce"]
                elif "merge" in output:
                    out_spec["merge"] = output["merge"]
                else:
                    raise OperationError("Invalid output spec")

                # MUST set db to output_db.name
                out_spec["db"] = output_db.name

        # ------- Execute mapReduce on SOURCE DB -------
        source_db = queryset._document._get_db()

        result = source_db.command(
            {
                "mapReduce": collection_name,
                "map": map_f,
                "reduce": reduce_f,
                "out": out_spec,
                **mr_args,
            },
            session=_get_session(),
        )

        # ------- Read results -------
        if inline:
            docs = result["results"]
        else:
            # Load from output DB
            if isinstance(result["result"], str):
                output_collection = output_db[result["result"]]
            else:
                info = result["result"]
                output_collection = output_db[info["collection"]]

            cursor = output_collection.find()
            if queryset._ordering:
                cursor = cursor.sort(queryset._ordering)

            docs = []
            for doc in cursor:
                docs.append(doc)

        # ------- Convert to MapReduceDocument -------
        results = []
        for doc in docs:
            results.append(
                MapReduceDocument(
                    queryset._document,
                    None,
                    doc["_id"],
                    doc["value"],
                )
            )

        return results

    def exec_js(self, code: Code, *fields, **options):
        """Execute a JavaScript function on the server. A list of fields may be
        provided, which will be translated to their correct names and supplied
        as the arguments to the function. A few extra variables are added to
        the function's scope: ``collection``, which is the name of the
        collection in use; ``query``, which is an object representing the
        current query; and ``options``, which is an object containing any
        options specified as keyword arguments.

        As fields in MongoEngine may use different names in the database (set
        using the: attr:`db_field` keyword argument to a: class:`Field`
        constructor), a mechanism exists for replacing MongoEngine field names
        with the database field names in JavaScript code. When accessing a
        field, use square-bracket notation and prefix the MongoEngine field
        name with a tilde (~).

        :param code: a string of JavaScript code to execute
        :param fields: fields that you will be using in your function, which
            will be passed in to your function as arguments
        :param options: options that you want available to the function
            (accessed in JavaScript through the ``options`` object)
        """
        queryset = self.clone()

        code = queryset._sub_js_fields(code)

        fields = [queryset._document._translate_field_name(f) for f in fields]
        collection = queryset._document._get_collection_name()

        scope = {"collection": collection, "options": options or {}}

        query = queryset._query
        if queryset._where_clause:
            query["$where"] = queryset._where_clause

        scope["query"] = query
        code = Code(code, scope=scope)

        db = queryset._document._get_db()
        return db.command("eval", code, args=fields).get("retval")

    def where(self, where_clause):
        """Filter ``BaseQuerySet`` results with a ``$where`` clause (a JavaScript
        expression). Performs automatic field name substitution like
        :meth:`mongoengine.queryset.Queryset.exec_js`.

        .. note:: When using this mode of query, the database will call your
                  function, or evaluate your predicate clause, for each object
                  in the collection.
        """
        queryset = self.clone()
        where_clause = queryset._sub_js_fields(where_clause)
        queryset._where_clause = where_clause
        return queryset

    def sum(self, field):
        """Calculate the sum of values for a field across matching documents.

        Async version of BaseQuerySet.sum(). Uses MongoDB aggregation
        pipeline for efficient server-side calculation.

        Args:
            field: Field name to sum (supports dot notation for nested fields)

        Returns:
            int or float: Sum of all values, or 0 if no documents match

        Example:
            # Sum all user ages
            total_age = await User.aobjects(active=True).sum('age')

            # Sum nested field
            total_price = await Order.aobjects.sum('items.price')

        Note:
            For ListField, automatically unwinds and sums all elements
        """
        db_field = self._fields_to_dbfields([field]).pop()
        pipeline = [
            {"$match": self._query},
            {"$group": {"_id": "sum", "total": {"$sum": "$" + db_field}}},
        ]

        # if we're performing a sum over a list field, we sum up all the
        # elements in the list, hence we need to $unwind the arrays first
        ListField = _import_class("ListField")
        field_parts = field.split(".")
        field_instances = self._document._lookup_field(field_parts)
        if isinstance(field_instances[-1], ListField):
            pipeline.insert(1, {"$unwind": "$" + field})

        result = [res for res in (
            self._document._get_collection(self._using)).aggregate(pipeline, session=_get_session()
                                                                   )]
        if result:
            return result[0]["total"]
        return 0

    def average(self, field):
        """Calculate the average of values for a field across matching documents.

        Async version of BaseQuerySet.average(). Uses MongoDB aggregation
        pipeline for efficient server-side calculation.

        Args:
            field: Field name to average (supports dot notation for nested fields)

        Returns:
            float: Average of all values, or 0 if no documents match

        Note:
            For ListField, automatically unwinds and averages all elements
        """
        db_field = self._fields_to_dbfields([field]).pop()
        pipeline = [
            {"$match": self._query},
            {"$group": {"_id": "avg", "total": {"$avg": "$" + db_field}}},
        ]

        # if we're performing an average over a list field, we average out
        # all the elements in the list, hence we need to $unwind the arrays
        # first
        ListField = _import_class("ListField")
        field_parts = field.split(".")
        field_instances = self._document._lookup_field(field_parts)
        if isinstance(field_instances[-1], ListField):
            pipeline.insert(1, {"$unwind": "$" + field})

        result = [res for res in (
            self._document._get_collection(self._using)).aggregate(pipeline, session=_get_session()
                                                                   )]
        if result:
            return result[0]["total"]
        return 0

    def item_frequencies(self, field, normalize=False, map_reduce=True):
        """Returns a dictionary of all items present in a field across
        the whole queried set of documents, and their corresponding frequency.
        This is useful for generating tag clouds or searching documents.

        . Note:

            Can only do direct simple mappings and cannot map across:
              class:`~mongoengine.fields.ReferenceField` or: class:`~mongoengine.fields.GenericReferenceField`
               for more complex counting a manual map reduce call is required.

        If the field is a: class:`~mongoengine.fields.ListField`, the items within
        each list will be counted individually.

        :param field: The field to use
        :param normalize: normalizes the results so they add to 1.0
        :param map_reduce: Use map_reduce over exec_js
        """
        """Fetch next document in async iteration.

        Async equivalent of sync BaseQuerySet's __next__ method.
        Handles scalar mode, as_pymongo mode, and normal Document mode.

        Returns:
            Document or value: Next item based on queryset mode

        Raises:
            StopAsyncIteration: When no more documents available

        Note:
            - In scalar mode: returns field value(s)
            - In as_pymongo mode: returns raw pymongo dict
            - Normal mode: returns Document instance
        """
        if map_reduce:
            return self._item_frequencies_map_reduce(field, normalize=normalize)
        return self._item_frequencies_exec_js(field, normalize=normalize)

    def rewind(self):
        """Rewind the cursor to its unevaluated state."""
        self._iter = False
        self._cursor.rewind()

    # Properties
    @property
    def _collection(self):
        """
        Return the Collection for this queryset, considering:
        - instance-level db/collection switch
        - queryset-level .using("alias","collection1")
        - document-class default alias
        """
        return self._document._get_collection(db_alias=self._using[0] if self._using else None,
                                              collection_name=self._using[1] if self._using else None)

    @property
    def _cursor_args(self):
        fields_name = "projection"
        cursor_args = {}
        if not self._timeout:
            cursor_args["no_cursor_timeout"] = True

        if self._allow_disk_use:
            cursor_args["allow_disk_use"] = True

        if self._loaded_fields:
            cursor_args[fields_name] = self._loaded_fields.as_dict()

        if self._search_text:
            if fields_name not in cursor_args:
                cursor_args[fields_name] = {}

            if self._search_text_score:
                cursor_args[fields_name]["_text_score"] = {"$meta": "textScore"}

        return cursor_args

    @property
    def _cursor(self):
        """Get or create the MongoDB cursor for this queryset.

        Sync equivalent of sync BaseQuerySet._cursor property.
        Lazily creates and configures the cursor with all query parameters.

        Key operations performed:
        1. Gets the async collection (awaited)
        2. Builds the query from _query_obj
        3. Applies projection (_loaded_fields)
        4. Applies ordering, limit, skip
        5. Applies hints, collation, batch_size
        6. Applies where clauses

        Returns:
            AsyncCursor or AsyncCommandCursor: Configured MongoDB cursor

        Note:
            Must be awaited: cursor = await queryset._cursor
        """
        # If _cursor_obj already exists, return it immediately.
        if self._cursor_obj is not None:
            return self._cursor_obj
        if needs_aggregation(self):
            pipeline = PipelineBuilder(queryset=self).build()
            if self._read_preference is not None or self._read_concern is not None:
                self._cursor_obj = self._collection.with_options(
                    read_preference=self._read_preference, read_concern=self._read_concern
                ).aggregate(pipeline=pipeline, session=_get_session(), batchSize=self._batch_size)
            else:
                self._cursor_obj = self._collection.aggregate(pipeline=pipeline,
                                                              session=_get_session(),
                                                              batchSize=self._batch_size)
        else:
            # Create a new PyMongo cursor.
            # XXX In PyMongo 3+, we define the read preference on a collection
            # level, not a cursor level. Thus, we need to get a cloned collection
            # object using `with_options` first.
            if self._read_preference is not None or self._read_concern is not None:
                self._cursor_obj = self._collection.with_options(
                    read_preference=self._read_preference, read_concern=self._read_concern
                ).find(self._query, session=_get_session(), **self._cursor_args)
            else:
                self._cursor_obj = self._collection.find(
                    self._query, session=_get_session(), **self._cursor_args
                )
                # Apply "where" clauses to the cursor
                if self._where_clause:
                    where_clause = self._sub_js_fields(self._where_clause)
                    self._cursor_obj.where(where_clause)

                # Apply ordering to the cursor.
                # XXX self._ordering can be equal to:
                # * None if we didn't explicitly call order_by on this queryset.
                # * A list of PyMongo-style sorting tuples.
                # * An empty list if we explicitly called order_by() without any
                #   arguments. This indicates that we want to clear the default
                #   ordering.
                if self._ordering:
                    # explicit ordering
                    self._cursor_obj.sort(self._ordering)
                elif self._ordering is None and self._document._meta["ordering"]:
                    # default ordering
                    order = self._get_order_by(self._document._meta["ordering"])
                    self._cursor_obj.sort(order)
                if self._limit is not None:
                    self._cursor_obj.limit(self._limit)

                if self._skip is not None:
                    self._cursor_obj.skip(self._skip)

                if self._hint != -1:
                    self._cursor_obj.hint(self._hint)

                if self._collation is not None:
                    self._cursor_obj.collation(self._collation)

                if self._batch_size is not None:
                    self._cursor_obj.batch_size(self._batch_size)

                if self._comment is not None:
                    self._cursor_obj.comment(self._comment)
        return self._cursor_obj

    def __deepcopy__(self, memo):
        """Essential for chained queries with ReferenceFields involved"""
        return self.clone()

    @property
    def _query(self):
        """Build and cache the MongoDB query dictionary.

        Async version that uses async_to_query() to handle async field
        transformations (e.g., for ReferenceFields).

        Key difference from the sync version:
        - Calls async_to_query() instead of to_query()
        - Must be awaited to get the query dict

        Returns:
            dict: MongoDB query document ready for collection.find()

        Note:
            Combines _query_obj filters with _cls_query for inheritance
        """
        if self._mongo_query is None:
            self._mongo_query = self._query_obj.to_query(self._document)
            if self._cls_query:
                if "_cls" in self._mongo_query:
                    self._mongo_query = {"$and": [self._cls_query, self._mongo_query]}
                else:
                    self._mongo_query.update(self._cls_query)
        return self._mongo_query

    @_query.setter
    def _query(self, query):
        self._mongo_query = query

    # Helper Functions

    def _item_frequencies_map_reduce(self, field, normalize=False):
        map_func = """
                    function() {{
                        var path = '{{{{~{field}}}}}'.split('.');
                        var field = this;

                        for (p in path) {{
                            if (typeof field != 'undefined')
                               field = field[path[p]];
                            else
                               break;
                        }}
                        if (field && field.constructor == Array) {{
                            field.forEach(function(item) {{
                                emit(item, 1);
                            }});
                        }} else if (typeof field != 'undefined') {{
                            emit(field, 1);
                        }} else {{
                            emit(null, 1);
                        }}
                    }}
                """.format(
            field=field
        )
        reduce_func = """
                    function(key, values) {
                        var total = 0;
                        var valuesSize = values.length;
                        for (var i=0; i < valuesSize; i++) {
                            total += parseInt(values[i], 10);
                        }
                        return total;
                    }
                """
        values = self.map_reduce(map_func, reduce_func, "inline")
        frequencies = {}
        for f in values:
            key = f.key
            if isinstance(key, float):
                if int(key) == key:
                    key = int(key)
            frequencies[key] = int(f.value)

        if normalize:
            count = sum(frequencies.values())
            frequencies = {k: float(v) / count for k, v in frequencies.items()}

        return frequencies

    def _item_frequencies_exec_js(self, field, normalize=False):
        """Uses exec_js to execute"""
        """Uses exec_js to execute"""
        freq_func = """
                    function(path) {
                        var path = path.split('.');

                        var total = 0.0;
                        db[collection].find(query).forEach(function(doc) {
                            var field = doc;
                            for (p in path) {
                                if (field)
                                    field = field[path[p]];
                                 else
                                    break;
                            }
                            if (field && field.constructor == Array) {
                               total += field.length;
                            } else {
                               total++;
                            }
                        });

                        var frequencies = {};
                        var types = {};
                        var inc = 1.0;

                        db[collection].find(query).forEach(function(doc) {
                            field = doc;
                            for (p in path) {
                                if (field)
                                    field = field[path[p]];
                                else
                                    break;
                            }
                            if (field && field.constructor == Array) {
                                field.forEach(function(item) {
                                    frequencies[item] = inc + (isNaN(frequencies[item]) ? 0: frequencies[item]);
                                });
                            } else {
                                var item = field;
                                types[item] = item;
                                frequencies[item] = inc + (isNaN(frequencies[item]) ? 0: frequencies[item]);
                            }
                        });
                        return [total, frequencies, types];
                    }
                """
        total, data, types = self.exec_js(freq_func, field)
        values = {types.get(k): int(v) for k, v in data.items()}

        if normalize:
            values = {k: float(v) / total for k, v in values.items()}

        frequencies = {}
        for k, v in values.items():
            if isinstance(k, float):
                if int(k) == k:
                    k = int(k)

            frequencies[k] = v

        return frequencies

    def _fields_to_dbfields(self, fields):
        """Translate fields' paths to their db equivalents.

        Supports both:
            - mongoengine style: profile.name
            - Django-style: profile__name
        """
        subclasses = []
        if self._document._meta["allow_inheritance"]:
            subclasses = [_DocumentRegistry.get(x) for x in self._document._subclasses][1:]

        db_field_paths = []

        for field in fields:

            # ---- SPECIAL CASES FOR ID / _ID ----
            if field == "id":
                db_field_paths.append("_id")
                continue

            if field == "_id":
                db_field_paths.append("_id")
                continue

            # NEW: accept Django-style embedded fields
            field_parts = (
                field.split("__") if "__" in field else field.split(".")
            )

            try:
                # lookup field chain
                lookup = self._document._lookup_field(field_parts)

                # build db-field path using db_field instead of attribute name
                db_path = ".".join(
                    part if isinstance(part, str) else part.db_field
                    for part in lookup
                )
                db_field_paths.append(db_path)
                continue

            except LookUpError as err:
                # try subclasses
                found = False
                for subdoc in subclasses:
                    try:
                        lookup = subdoc._lookup_field(field_parts)
                        db_path = ".".join(
                            part if isinstance(part, str) else part.db_field
                            for part in lookup
                        )
                        db_field_paths.append(db_path)
                        found = True
                        break
                    except LookUpError:
                        pass

                if not found:
                    raise err

        return db_field_paths

    def _get_order_by(self, keys):
        """Given a list of MongoEngine-style sort keys, return a list
        of sorting tuples that can be applied to a PyMongo cursor. For
        example:

        >>> qs._get_order_by(['-last_name', 'first_name'])
        [('last_name', -1), ('first_name', 1)]
        """
        key_list = []
        for key in keys:
            if not key:
                continue

            if key == "$text_score":
                key_list.append(("_text_score", {"$meta": "textScore"}))
                continue

            direction = pymongo.ASCENDING
            if key[0] == "-":
                direction = pymongo.DESCENDING

            if key[0] in ("-", "+"):
                key = key[1:]

            key = key.replace("__", ".")
            try:
                key = self._document._translate_field_name(key)
            except Exception:
                # TODO this exception should be more specific
                pass

            key_list.append((key, direction))

        return key_list

    def _get_scalar(self, raw_doc):
        doc = self._document._from_son(
            raw_doc,
        )

        def lookup(obj, name):
            if name in ("id", "pk"):
                return raw_doc["_id"]

            chunks = name.split("__")
            val = obj

            for chunk in chunks:
                val = getattr(val, chunk, None)

            return val

        results = [lookup(doc, f) for f in self._scalar]
        return results[0] if len(results) == 1 else tuple(results)

    def _sub_js_fields(self, code) -> str:
        """When fields are specified with [~fieldname] syntax, where
        *fieldname* is the Python name of a field, *fieldname* will be
        substituted for the MongoDB name of the field (specified using the
        :attr:`name` keyword argument in a field's constructor).
        """

        def field_sub(match):
            # Extract just the field name, and look up the field objects
            field_name = match.group(1).split(".")
            fields = self._document._lookup_field(field_name)
            # Substitute the correct name for the field into the javascript
            return '["%s"]' % fields[-1].db_field

        def field_path_sub(match):
            # Extract just the field name, and look up the field objects
            field_name = match.group(1).split(".")
            fields = self._document._lookup_field(field_name)
            # Substitute the correct name for the field into the javascript
            return ".".join([f.db_field for f in fields])

        code = re.sub(r"\[\s*~([A-z_][A-z_0-9.]+?)\s*\]", field_sub, code)
        code = re.sub(r"\{\{\s*~([A-z_][A-z_0-9.]+?)\s*\}\}", field_path_sub, code)
        return code

    def _chainable_method(self, method_name, val) -> 'BaseQuerySet':
        """Generic handler for chainable cursor configuration methods.

        Key difference from sync BaseQuerySet:
        - Version calls method on cursor immediately
        - Async version stores value and applies when cursor is created
        - This is necessary because cursor creation is async in this class

        Used by methods like comment(), max_time_ms(), etc.

        Args:
            method_name: Name of the cursor method to call later
            val: Value to pass to the cursor method

        Returns:
            AsyncBaseQuerySet: Cloned queryset with configuration stored

        Note:
            The value is stored in __{method_name} attribute and applied
            in the _cursor property when the async cursor is initialized.
        """
        queryset = self.clone()

        # Cache the parameter for a lazy application at execution time
        setattr(queryset, f"_{method_name}", val)

        return queryset
