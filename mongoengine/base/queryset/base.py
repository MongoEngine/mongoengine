import abc
import copy
import itertools
import re
from collections.abc import Mapping
from typing import Union, Type

import pymongo
import pymongo.errors

from bson import SON, json_util, ObjectId
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.asynchronous.cursor import AsyncCursor
from pymongo.common import validate_read_preference
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode
from pymongo.synchronous.cursor import Cursor

from mongoengine import Document
from mongoengine.base import _DocumentRegistry
from mongoengine.context_managers import (
    no_dereferencing_active_for_class,
)
from mongoengine.errors import (
    InvalidQueryError,
    LookUpError,
    OperationError,
)

from mongoengine.base.queryset.field_list import QueryFieldList
from mongoengine.base.queryset.visitor import Q, QNode

__all__ = ("BaseQuerySet",)


class BaseQuerySet(abc.ABC):
    """BaseQuerySet for MongoDB queries.

    This class provides the async/await API for querying MongoDB documents.
    It mirrors the BaseQuerySet API but requires `await` for database operations.

    Key Differences from Sync BaseQuerySet:
    ======================================
    1. Database operations are async (get, first, count, delete, update, etc.)
    2. Iteration uses `async for` instead of `for`
    3. Boolean evaluation disabled - use `await qs.exists()` instead
    4. Indexing and slicing disabled - use .skip()/.limit() methods
    5. Collection and cursor properties must be awaited
    6. Uses asyncio.Lock to prevent concurrent collection initialization

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
    for result in cursor:
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

    def __init__(self, document: Type[Document]):
        """Initialize an async queryset for the given document class.

        Args:
            document: The Document class this queryset operates on
        """
        self._document = document
        self._mongo_query: dict | None = None  # Cached MongoDB query dict
        self._query_obj: Q = Q()  # MongoEngine query object
        self._cls_query: dict = {}  # Query filter for inheritance (_cls field)
        self._where_clause: dict | None = None  # JavaScript $where clause
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

    @abc.abstractmethod
    def __getitem__(self, key: Union[int, slice]):
        """Disabled in async queryset - indexing and slicing not supported.

        Unlike sync BaseQuerySet, neither integer indexing nor slicing are
        supported because they cannot return data synchronously.

        User.objects[0]
        <User: User object>
        User.objects[1:3]
        [<User: User object>, <User: User object>]

        Examples of what DOESN'T work:
            qs[0] → OperationError (use: await qs.first())
            qs[1:5] → OperationError (use: qs.skip(1).limit(4))
            qs[:10] → OperationError (use: qs.limit(10))

        Use these async alternatives instead:
            # Get first document
            doc = await qs.first()

            # Get nth document
            doc = await qs.skip(n).first()

            # Limit results
            docs = qs.limit(10)
            async for doc in docs:
                ...

            # Skip and limit
            docs = qs.skip(5).limit(10)

        Args:
            key: int or slice (both will raise errors)

        Raises:
            OperationError: Always - indexing/slicing not supported in async

        Note:
            While slicing could theoretically work by returning a queryset
            with skip/limit, it's disabled to prevent confusion and maintain
            consistency with the async-only API design.
             Both slicing and integer indexing should be disabled in an async version
        """
        queryset = self.clone()
        queryset._empty = False

        # Handle a slice
        if isinstance(key, slice):
            queryset._cursor_obj = queryset._cursor[key]
            queryset._skip, queryset._limit = key.start, key.stop
            if key.start and key.stop:
                queryset._limit = key.stop - key.start
            if queryset._limit == 0:
                queryset._empty = True

            # Allow further QuerySet modifications to be performed
            return queryset

        # Handle an index
        elif isinstance(key, int):
            if queryset._scalar:
                return queryset._get_scalar(
                    queryset._document._from_son(
                        queryset._cursor[key],
                    )
                )

            if queryset._as_pymongo:
                return queryset._cursor[key]

            return queryset._document._from_son(
                queryset._cursor[key],
            )

    @abc.abstractmethod
    def __iter__(self) -> list[Document] | dict:
        """Must be implemented by subclasses"""

    @abc.abstractmethod
    def __aiter__(self) -> list[Document] | dict:
        """Must be implemented by subclasses"""

    @abc.abstractmethod
    def __anext__(self) -> Document | dict:
        """Must be implemented by subclasses"""

    @abc.abstractmethod
    def __next__(self) -> Document | dict:
        """Must be implemented by subclasses"""

    @abc.abstractmethod
    def _has_data(self) -> bool:
        """Must be implemented by subclasses. Check if the queryset has any matching documents."""

    @abc.abstractmethod
    def __bool__(self) -> bool:
        """Must be implemented by subclasses"""

    @abc.abstractmethod
    def exists(self) -> bool:
        """Must be implemented by subclasses"""

    # Core functions

    def all(self) -> 'BaseQuerySet':
        """Returns a copy of the current BaseQuerySet."""
        return self.__call__()

    def filter(self, *q_objs: Union['BaseQuerySet', None], **query: dict) -> 'BaseQuerySet':
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

    def get(self, *q_objs, **query) -> Document:
        """Must be implemented by subclasses"""

    @abc.abstractmethod
    def create(self, **kwargs) -> Document:
        """Create and save a new document instance.
        Args:
            **kwargs: Field values for the new document

        Returns:
            Document: The created and saved document instance

        Example:
            user = await User.aobjects.create(name='John', email='john@example.com')
        """

    @abc.abstractmethod
    def first(self) -> Document | None:
        """Retrieve the first document matching the query.

        Async version of BaseQuerySet.first(). Returns None if no matches are found.

        Returns:
            Document or None: First matching document, or None if no results
        """

    @abc.abstractmethod
    def insert(
            self, doc_or_docs: Document | list[Document], load_bulk: bool = True, write_concern: dict | None = None,
            signal_kwargs: dict | None = None
    ) -> Document | list[Document]:
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

    @abc.abstractmethod
    def count(self, with_limit_and_skip: bool = False) -> int:
        """Count documents matching the query.

        Async version of BaseQuerySet.count(). Returns count of documents
        without loading them into memory.

        Args:
            with_limit_and_skip: If True, respects any limit/skip applied to queryset

        Returns:
            int: Number of documents matching the query
        """

    @abc.abstractmethod
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
        """

    @abc.abstractmethod
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

        """

    @abc.abstractmethod
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

    @abc.abstractmethod
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

    @abc.abstractmethod
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

    @abc.abstractmethod
    def with_id(self, object_id: ObjectId):
        """Retrieve the object matching the id provided.  Uses `object_id` only
        and raises InvalidQueryError if a filter has been applied. Returns
        `None` if no document exists with that id.

        :param object_id: the value for the id of the document to look up
        """

    @abc.abstractmethod
    def in_bulk(self, object_ids: list[ObjectId] | tuple[ObjectId]):
        """Retrieve multiple documents by their IDs in a single query.

        Async version of BaseQuerySet.in_bulk(). Efficient bulk loading
        by fetching all documents in one database round trip.

        Args:
            object_ids: List or tuple of ObjectIds to fetch

        Returns:
            dict: Mapping of ObjectId to Document instances

        Note:
            Respects scalar() and as_pymongo() modes if set
        """

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

    def select_related(self, max_depth: int = 1) -> 'BaseQuerySet':
        """Pre-fetch related documents to reduce database queries.

        Version of BaseQuerySet.select_related(). Eagerly loads
        referenced documents up to a specified depth to avoid N+1 queries.

        Args:
            max_depth: Maximum depth for dereferencing nested references

        Returns:
            BaseQuerySet: Self for method chaining

        """
        queryset = self.clone()
        max_depth += 1
        return queryset
        # todo select_related is not implemented yet
        raise NotImplementedError("select_related is not implemented yet")
        # Perform async dereferencing on the queryset
        # await (await queryset._dereference)(queryset, max_depth=max_depth)

        # Return queryset for chaining
        return queryset

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

    def hint(self, index: str | None) -> 'BaseQuerySet':
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

    @abc.abstractmethod
    def distinct(self, field):
        # todo description
        pass

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

    @abc.abstractmethod
    def explain(self):
        """Return an explain plan record for the: class:`~mongoengine.queryset.BaseQuerySet` cursor.
        """

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

    @abc.abstractmethod
    def to_json(self, *args, **kwargs):
        """Converts a queryset to JSON"""

    def from_json(self, json_data):
        """Converts json data to unsaved objects"""
        son_data = json_util.loads(json_data)
        return [self._document._from_son(data) for data in son_data]

    @abc.abstractmethod
    def aggregate(self, pipeline, **kwargs):
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
            CommandCursor/AsyncCommandCursor: Async cursor over aggregation results

        Raises:
            TypeError: If a pipeline is not a list or tuple

        Note:
            geoNear and collStats must be first in the pipeline if used
        """

    # JS functionality
    @abc.abstractmethod
    def map_reduce(
            self, map_f, reduce_f, output, finalize_f=None, limit=None, scope=None
    ):
        """Execute the map-reduce operation on the queryset."""

    @abc.abstractmethod
    def exec_js(self, code, *fields, **options):
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

    def where(self, where_clause):
        """Filter ``BaseQuerySet`` results with a ``$where`` clause (a Javascript
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

    @abc.abstractmethod
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

    @abc.abstractmethod
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

    @abc.abstractmethod
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

    @abc.abstractmethod
    def rewind(self):
        """Rewind the cursor to its unevaluated state."""

    # Properties
    @property
    @abc.abstractmethod
    async def _collection(self):
        """
        Return the Collection/AsyncCollection for this queryset, considering:
        - instance-level db switch
        - queryset-level .using("alias")
        - document-class default alias
        """

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
    @abc.abstractmethod
    def _cursor(self, max_depth=1):
        """Get or create the MongoDB cursor for this queryset.

        Async equivalent of sync BaseQuerySet._cursor property.
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

    @abc.abstractmethod
    def _item_frequencies_map_reduce(self, field, normalize=False):
        pass

    @abc.abstractmethod
    def _item_frequencies_exec_js(self, field, normalize=False):
        """Uses exec_js to execute"""

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

    def _sub_js_fields(self, code):
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

    def _chainable_method(self, method_name, val):
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

        # Cache the parameter for lazy application at execution time
        setattr(queryset, f"_{method_name}", val)

        return queryset
