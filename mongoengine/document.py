import json
import re

import pymongo
from bson.dbref import DBRef
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import OperationFailure
from pymongo.read_preferences import ReadPreference
from pymongo.synchronous.collection import Collection

from mongoengine import signals
from mongoengine.asynchronous import AsyncQuerySet
from mongoengine.base import (
    BaseDict,
    BaseDocument,
    BaseList,
    DocumentMetaclass,
    EmbeddedDocumentList,
    TopLevelDocumentMetaclass,
    _DocumentRegistry,
)
from mongoengine.base.utils import NonOrderedList
from mongoengine.common import _import_class, _normalize_async_values_document
from mongoengine.registry import _CollectionRegistry
from mongoengine.registry.collection import CollectionType
from mongoengine.synchronous.connection import (
    DEFAULT_CONNECTION_NAME,
    get_db,
)
from mongoengine.session import _get_session
from mongoengine.asynchronous import async_get_db
from mongoengine.context_managers import (
    set_write_concern,
    CURRENT_DB_ALIAS,
    CURRENT_COLLECTION,
)
from mongoengine.errors import (
    InvalidDocumentError,
    InvalidQueryError,
    SaveConditionError,
    DoesNotExist,
    OperationError,
    NotUniqueError,
)
from mongoengine.pymongo_support import (
    list_collection_names,
    async_list_collection_names,
)
from mongoengine.base.queryset import (
    transform,
)

__all__ = (
    "Document",
    "EmbeddedDocument",
    "DynamicDocument",
    "DynamicEmbeddedDocument",
    "InvalidCollectionError",
    "MapReduceDocument",
)

from mongoengine.synchronous import QuerySet


def includes_cls(fields):
    """Helper function used for ensuring and comparing indexes."""
    first_field = None
    if len(fields):
        if isinstance(fields[0], str):
            first_field = fields[0]
        elif isinstance(fields[0], (list, tuple)) and len(fields[0]):
            first_field = fields[0][0]
    return first_field == "_cls"


class InvalidCollectionError(Exception):
    pass


class EmbeddedDocument(BaseDocument, metaclass=DocumentMetaclass):
    r"""A :class:`~mongoengine.Document` that isn't stored in its own
    collection.  :class:`~mongoengine.EmbeddedDocument`\ s should be used as
    fields on :class:`~mongoengine.Document`\ s through the
    :class:`~mongoengine.EmbeddedDocumentField` field type.

    A :class:`~mongoengine.EmbeddedDocument` subclass may be itself subclassed,
    to create a specialised version of the embedded document that will be
    stored in the same collection. To facilitate this behaviour a `_cls`
    field is added to documents (hidden though the MongoEngine interface).
    To enable this behaviour set :attr:`allow_inheritance` to ``True`` in the
    :attr:`meta` dictionary.
    """

    __slots__ = ("_instance",)

    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = DocumentMetaclass

    # A generic embedded document doesn't have any immutable properties
    # that describe it uniquely, hence it shouldn't be hashable. You can
    # define your own __hash__ method on a subclass if you need your
    # embedded documents to be hashable.
    __hash__ = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._instance = None
        self._changed_fields = []

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._data == other._data
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __getstate__(self):
        data = super().__getstate__()
        data["_instance"] = None
        return data

    def __setstate__(self, state):
        super().__setstate__(state)
        self._instance = state["_instance"]

    def to_mongo(self, *args, **kwargs):
        data = super().to_mongo(*args, **kwargs)

        # remove _id from the SON if it's in it and it's None
        if "_id" in data and data["_id"] is None:
            del data["_id"]

        return data


class Document(BaseDocument, metaclass=TopLevelDocumentMetaclass):
    """The base class used for defining the structure and properties of
    collections of documents stored in MongoDB. Inherit from this class, and
    add fields as class attributes to define a document's structure.
    Individual documents may then be created by making instances of the
    :class:`~mongoengine.Document` subclass.

    By default, the MongoDB collection used to store documents created using a
    :class:`~mongoengine.Document` subclass will be the name of the subclass
    converted to snake_case. A different collection may be specified by
    providing :attr:`collection` to the :attr:`meta` dictionary in the class
    definition.

    A :class:`~mongoengine.Document` subclass may be itself subclassed, to
    create a specialised version of the document that will be stored in the
    same collection. To facilitate this behaviour a `_cls`
    field is added to documents (hidden though the MongoEngine interface).
    To enable this behaviour set :attr:`allow_inheritance` to ``True`` in the
    :attr:`meta` dictionary.

    A :class:`~mongoengine.Document` may use a **Capped Collection** by
    specifying :attr:`max_documents` and :attr:`max_size` in the :attr:`meta`
    dictionary. :attr:`max_documents` is the maximum number of documents that
    is allowed to be stored in the collection, and :attr:`max_size` is the
    maximum size of the collection in bytes. :attr:`max_size` is rounded up
    to the next multiple of 256 by MongoDB internally and mongoengine before.
    Use also a multiple of 256 to avoid confusions.  If :attr:`max_size` is not
    specified and :attr:`max_documents` is, :attr:`max_size` defaults to
    10485760 bytes (10MB).

    Indexes may be created by specifying :attr:`indexes` in the :attr:`meta`
    dictionary. The value should be a list of field names or tuples of field
    names. Index direction may be specified by prefixing the field names with
    a **+** or **-** sign.

    Automatic index creation can be disabled by specifying
    :attr:`auto_create_index` in the :attr:`meta` dictionary. If this is set to
    False then indexes will not be created by MongoEngine.  This is useful in
    production systems where index creation is performed as part of a
    deployment system.

    By default, _cls will be added to the start of every index (that
    doesn't contain a list) if allow_inheritance is True. This can be
    disabled by either setting cls to False on the specific index or
    by setting index_cls to False on the meta dictionary for the document.

    By default, any extra attribute existing in stored data but not declared
    in your model will raise a :class:`~mongoengine.FieldDoesNotExist` error.
    This can be disabled by setting :attr:`strict` to ``False``
    in the :attr:`meta` dictionary.
    """

    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = TopLevelDocumentMetaclass

    __slots__ = "__objects"

    @property
    def pk(self):
        """Get the primary key."""
        if "id_field" not in self._meta:
            return None
        return getattr(self, self._meta["id_field"])

    @pk.setter
    def pk(self, value):
        """Set the primary key."""
        setattr(self, self._meta["id_field"], value)

    def __hash__(self):
        """Return the hash based on the PK of this document. If it's new
        and doesn't have a PK yet, return the default object hash instead.
        """
        if self.pk is None:
            return super(BaseDocument, self).__hash__()

        return hash(self.pk)

    @classmethod
    def _db_alias(cls, db_alias: str | None = None):
        # 1) explicit argument always wins
        if db_alias is not None:
            return db_alias

        # 2) per-class override from ContextVar dict
        mapping = CURRENT_DB_ALIAS.get() or {}
        if cls in mapping:
            return mapping[cls]

        # 3) fallback to document meta / default
        return cls._meta.get("db_alias", DEFAULT_CONNECTION_NAME)

    @classmethod
    def _get_collection_name(cls, collection_name: str | None = None):
        """Return the collection name for this class. None for abstract class."""
        # 1) explicit argument always wins
        if collection_name is not None:
            return collection_name
        overrides = CURRENT_COLLECTION.get()
        if overrides and cls in overrides:
            return overrides[cls]

        return cls._meta.get("collection", None)

    @classmethod
    def _get_db(cls, db_alias: str | None = None):
        """Some Model using other db_alias"""
        return get_db(cls._db_alias(db_alias=db_alias))

    @classmethod
    async def _async_get_db(cls, db_alias: str | None = None):
        """Some Model using other db_alias"""
        return await async_get_db(cls._db_alias(db_alias=db_alias))

    @classmethod
    def _get_collection(cls, db_alias: str = None, collection_name: str | None = None):
        """Return the PyMongo collection corresponding to this document.

        Upon first call, this method:
        1. Initializes a :class:`~pymongo.collection.Collection` corresponding
           to this document.
        2. Creates indexes defined in this document's :attr:`meta` dictionary.
           This happens only if `auto_create_index` is True.
        """
        db_alias = cls._db_alias(db_alias=db_alias)
        collection_name = cls._get_collection_name(collection_name=collection_name)
        collection_type = cls._collection_type()
        collection_fingerprint = cls._collection_fingerprint()
        collection = _CollectionRegistry.get(
            db_alias=db_alias,
            name=collection_name,
            is_async=False,
            type_=collection_type,
            fingerprint=collection_fingerprint,
        )
        if collection is not None:
            return collection
        else:
            db = cls._get_db(db_alias=db_alias)
            # Get the collection, either capped or regular.
            if collection_type == CollectionType.CAPPED:
                collection = cls._get_capped_collection()
            elif collection_type == CollectionType.TIMESERIES:
                collection = cls._get_timeseries_collection()
            else:
                collection = db[collection_name]
            # Ensure indexes on the collection unless auto_create_index was
            # set to False. Plus, there is no need to ensure indexes on the slave.
            if cls._meta.get("auto_create_index", True) and db.client.is_primary:
                cls.ensure_indexes(collection)
            _CollectionRegistry.register(
                db_alias=db_alias,
                name=collection_name,
                collection=collection,
                type_=collection_type,
                fingerprint=collection_fingerprint,
            )
            return collection

    @classmethod
    def _collection_type(cls) -> CollectionType:
        if cls._meta.get("max_size") or cls._meta.get("max_documents"):
            return CollectionType.CAPPED
        elif cls._meta.get("timeseries"):
            return CollectionType.TIMESERIES
        else:
            return CollectionType.DEFAULT

    @classmethod
    def _collection_fingerprint(cls) -> str | None:
        """Return a deterministic string fingerprint for collection options."""
        if cls._meta.get("max_size") or cls._meta.get("max_documents"):
            opts = {
                "type": "capped",
                "max_size": cls._meta.get("max_size"),
                "max_documents": cls._meta.get("max_documents"),
            }
        elif cls._meta.get("timeseries"):
            opts = {
                "type": "timeseries",
                "options": cls._meta.get("timeseries"),
            }
        else:
            return None
        return json.dumps(opts, sort_keys=True, separators=(",", ":"))

    @classmethod
    async def _aget_collection(
        cls, db_alias: str | None = None, collection_name: str | None = None
    ) -> AsyncCollection:
        """Return the PyMongo collection corresponding to this document.

        Upon the first call, this method:
        1. Initializes a :class:`~pymongo.collection.Collection` corresponding
           to this document.
        2. Creates indexes defined in this document's :attr:`meta` dictionary.
           This happens only if `auto_create_index` is True.
        """
        db_alias = cls._db_alias(db_alias=db_alias)
        collection_name = cls._get_collection_name(collection_name=collection_name)
        collection_type = cls._collection_type()
        collection_fingerprint = cls._collection_fingerprint()
        collection = _CollectionRegistry.get(
            db_alias=db_alias,
            name=collection_name,
            is_async=True,
            type_=collection_type,
            fingerprint=collection_fingerprint,
        )
        if collection is not None:
            return collection
        else:
            db = await cls._async_get_db(db_alias=db_alias)
            # Get the collection, either capped or regular.
            if collection_type == CollectionType.CAPPED:
                collection = await cls._aget_capped_collection()
            elif collection_type == CollectionType.TIMESERIES:
                collection = await cls._aget_timeseries_collection()
            else:
                collection = db[collection_name]
            # Ensure indexes on the collection unless auto_create_index was
            # set to False. Plus, there is no need to ensure indexes on the slave.
            if cls._meta.get("auto_create_index", True) and await db.client.is_primary:
                await cls.aensure_indexes(collection)
            _CollectionRegistry.register(
                db_alias=db_alias,
                name=collection_name,
                collection=collection,
                type_=collection_type,
                fingerprint=collection_fingerprint,
            )
            return collection

    @classmethod
    def _get_capped_collection(cls):
        """Create a new or get an existing capped PyMongo collection."""
        db = cls._get_db()
        collection_name = cls._get_collection_name()

        # Get max document limit and max byte size from meta.
        max_size = cls._meta.get("max_size") or 10 * 2**20  # 10MB default
        max_documents = cls._meta.get("max_documents")

        # MongoDB will automatically raise the size to make it a multiple of
        # 256 bytes. We raise it here ourselves to be able to reliably compare
        # the options below.
        if max_size % 256:
            max_size = (max_size // 256 + 1) * 256

        # If the collection already exists and has different options
        # (i.e. isn't capped or has different max/size), raise an error.
        if collection_name in list_collection_names(
            db, include_system_collections=True
        ):
            collection = db[collection_name]
            options = collection.options()
            if options.get("max") != max_documents or options.get("size") != max_size:
                raise InvalidCollectionError(
                    'Cannot create collection "{}" as a capped '
                    "collection as it already exists".format(cls._collection)
                )

            return collection

        # Create a new capped collection.
        opts = {"capped": True, "size": max_size}
        if max_documents:
            opts["max"] = max_documents

        return db.create_collection(collection_name, session=_get_session(), **opts)

    @classmethod
    async def _aget_capped_collection(cls):
        """Create a new or get an existing capped PyMongo collection."""
        db = await cls._async_get_db()
        collection_name = cls._get_collection_name()

        # Get max document limit and max byte size from meta.
        max_size = cls._meta.get("max_size") or 10 * 2**20  # 10MB default
        max_documents = cls._meta.get("max_documents")

        # MongoDB will automatically raise the size to make it a multiple of
        # 256 bytes. We raise it here ourselves to be able to reliably compare
        # the options below.
        if max_size % 256:
            max_size = (max_size // 256 + 1) * 256

        # If the collection already exists and has different options
        # (i.e. isn't capped or has different max/size), raise an error.
        if collection_name in await async_list_collection_names(
            db, include_system_collections=True
        ):
            collection = db[collection_name]
            options = await collection.options()
            if options.get("max") != max_documents or options.get("size") != max_size:
                raise InvalidCollectionError(
                    'Cannot create collection "{}" as a capped '
                    "collection as it already exists".format(cls._collection)
                )

            return collection

        # Create a new capped collection.
        opts = {"capped": True, "size": max_size}
        if max_documents:
            opts["max"] = max_documents

        return await db.create_collection(
            collection_name, session=_get_session(), **opts
        )

    @classmethod
    def _get_timeseries_collection(cls):
        """Create a new or get an existing timeseries PyMongo collection."""
        db = cls._get_db()
        collection_name = cls._get_collection_name()
        timeseries_opts = cls._meta.get("timeseries")

        if collection_name in list_collection_names(
            db, include_system_collections=True
        ):
            collection = db[collection_name]
            collection.options()
            return collection

        opts = {"expireAfterSeconds": timeseries_opts.pop("expireAfterSeconds", None)}
        return db.create_collection(
            name=collection_name,
            timeseries=timeseries_opts,
            **opts,
        )

    @classmethod
    async def _aget_timeseries_collection(cls):
        """Create a new or get an existing timeseries PyMongo collection."""
        db = await cls._async_get_db()
        collection_name = cls._get_collection_name()
        timeseries_opts = cls._meta.get("timeseries")

        if collection_name in await async_list_collection_names(
            db, include_system_collections=True
        ):
            collection = db[collection_name]
            collection.options()
            return collection

        opts = {"expireAfterSeconds": timeseries_opts.pop("expireAfterSeconds", None)}
        return await db.create_collection(
            name=collection_name,
            timeseries=timeseries_opts,
            **opts,
        )

    def to_mongo(self, *args, **kwargs):
        data = super().to_mongo(*args, **kwargs)

        # If '_id' is None, try and set it from self._data. If that
        # doesn't exist either, remove '_id' from the SON completely.
        if "_id" in data and data["_id"] is None:
            if self._data.get("id") is None:
                data.pop("_id")
            else:
                data["_id"] = self._data["id"]

        return data

    def modify(self, query=None, **update):
        """Perform an atomic update of the document in the database and reload
        the document object using updated version.

        Returns True if the document has been updated or False if the document
        in the database doesn't match the query.

        .. note:: All unsaved changes that have been made to the document are
            rejected if the method returns True.

        :param query: the update will be performed only if the document in the
            database matches the query
        :param update: Django-style update keyword arguments
        """
        if query is None:
            query = {}

        if self.pk is None:
            raise InvalidDocumentError("The document does not have a primary key.")

        id_field = self._meta["id_field"]
        query = query.copy() if isinstance(query, dict) else query.to_query(self)

        if id_field not in query:
            query[id_field] = self.pk
        elif query[id_field] != self.pk:
            raise InvalidQueryError(
                "Invalid document modify query: it must modify only this document."
            )

        # Need to add shard key to query, or you get an error
        query.update(self._object_key)

        updated = self._qs(**query).modify(new=True, **update)
        if updated is None:
            return False

        for field in self._fields_ordered:
            setattr(self, field, self._reload(field, updated[field]))

        self._changed_fields = updated._changed_fields
        self._created = False

        return True

    async def amodify(self, query=None, **update):
        """Perform an atomic update of the document in the database and reload
        the document object using updated version.

        Returns True if the document has been updated or False if the document
        in the database doesn't match the query.

        .. note:: All unsaved changes that have been made to the document are
            rejected if the method returns True.

        :param query: the update will be performed only if the document in the
            database matches the query
        :param update: Django-style update keyword arguments
        """
        if query is None:
            query = {}

        if self.pk is None:
            raise InvalidDocumentError("The document does not have a primary key.")

        id_field = self._meta["id_field"]
        query = query.copy() if isinstance(query, dict) else query.to_query(self)

        if id_field not in query:
            query[id_field] = self.pk
        elif query[id_field] != self.pk:
            raise InvalidQueryError(
                "Invalid document modify query: it must modify only this document."
            )

        # Need to add shard key to query, or you get an error
        query.update(self._object_key)

        updated = await self._aqs(**query).modify(new=True, **update)
        if updated is None:
            return False

        for field in self._fields_ordered:
            setattr(self, field, self._reload(field, updated[field]))

        self._changed_fields = updated._changed_fields
        self._created = False

        return True

    def save(
        self,
        force_insert=False,
        validate=True,
        clean=True,
        write_concern=None,
        cascade=None,
        cascade_kwargs=None,
        _refs=None,
        save_condition=None,
        signal_kwargs=None,
        **kwargs,
    ) -> "Document":
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created. Returns the saved object instance.

        :param force_insert: only try to create a new document, don't allow
            updates of existing documents.
        :param validate: validates the document; set to ``False`` to skip.
        :param clean: call the document clean method, requires `validate` to be
            True.
        :param write_concern: Extra keyword arguments are passed down to
            :meth:`~pymongo.collection.Collection.save` OR
            :meth:`~pymongo.collection.Collection.insert`
            which will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param cascade: Sets the flag for cascading saves.  You can set a
            default by setting "cascade" in the document __meta__
        :param cascade_kwargs: (optional) kwargs dictionary to be passed throw
            to cascading saves.  Implies ``cascade=True``.
        :param _refs: A list of processed references used in cascading saves
        :param save_condition: only perform save if matching record in db
            satisfies condition(s) (e.g. version number).
            Raises :class:`OperationError` if the conditions are not satisfied
        :param signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.

        .. versionchanged:: 0.5
            In existing documents it only saves changed fields using
            set / unset.  Saves are cascaded and any
            :class:`~bson.dbref.DBRef` objects that have changes are
            saved as well.
        .. versionchanged:: 0.6
            Added cascading saves
        .. versionchanged:: 0.8
            Cascade saves are optional and default to False.  If you want
            fine grain control then you can turn off using document
            meta['cascade'] = True.  Also you can pass different kwargs to
            the cascade save using cascade_kwargs which overwrites the
            existing kwargs with custom values.
        .. versionchanged:: 0.26
           save() no longer calls :meth:`~mongoengine.Document.ensure_indexes`
           unless ``meta['auto_create_index_on_save']`` is set to True.

        """
        signal_kwargs = signal_kwargs or {}

        if self._meta.get("abstract"):
            raise InvalidDocumentError("Cannot save an abstract document.")

        signals.pre_save.send(self.__class__, document=self, **signal_kwargs)

        if validate:
            self.validate(clean=clean)

        if write_concern is None:
            write_concern = {}

        doc_id = self.to_mongo(fields=[self._meta["id_field"]])
        created = "_id" not in doc_id or self._created or force_insert

        signals.pre_save_post_validation.send(
            self.__class__, document=self, created=created, **signal_kwargs
        )
        # it might be refreshed by the pre_save_post_validation hook, e.g., for etag generation
        # Handle self generating fields
        for name, field in self._fields.items():
            value = self._data.get(name)
            self._data[name] = self._generate_auto_fields_sync(value, field)

        doc = self.to_mongo()

        # Initialize the Document's underlying pymongo.Collection (+create indexes) if not already initialized
        # Important to do this here to avoid that the index creation gets wrapped in the try/except block below
        # and turned into mongoengine.OperationError
        try:
            # Save a new document or update an existing one
            if created:
                object_id = self._save_create(
                    doc=doc, force_insert=force_insert, write_concern=write_concern
                )
            else:
                object_id, created = self._save_update(
                    doc, save_condition, write_concern
                )

            if cascade is None:
                cascade = self._meta.get("cascade", False) or cascade_kwargs is not None

            if cascade:
                kwargs = {
                    "force_insert": force_insert,
                    "validate": validate,
                    "write_concern": write_concern,
                    "cascade": cascade,
                }
                if cascade_kwargs:  # Allow granular control over cascades
                    kwargs.update(cascade_kwargs)
                kwargs["_refs"] = _refs
                self.cascade_save(**kwargs)

        except pymongo.errors.DuplicateKeyError as err:
            message = "Tried to save duplicate unique keys (%s)"
            raise NotUniqueError(message % err)
        except pymongo.errors.OperationFailure as err:
            message = "Could not save document (%s)"
            if re.match("^E1100[01] duplicate key", str(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = "Tried to save duplicate unique keys (%s)"
                raise NotUniqueError(message % err)
            raise OperationError(message % err)

        # Make sure we store the PK on this document now that it's saved
        id_field = self._meta["id_field"]
        if created or id_field not in self._meta.get("shard_key", []):
            self[id_field] = self._fields[id_field].to_python(object_id)

        signals.post_save.send(
            self.__class__, document=self, created=created, **signal_kwargs
        )

        self._clear_changed_fields()
        self._created = False

        return self

    def _generate_auto_fields_sync(self, value, field):
        from mongoengine.base import BaseDocument
        from mongoengine.fields import ListField, DictField

        # EmbeddedDocument
        if isinstance(value, BaseDocument) and not value._is_document:
            for name, sub_field in value._fields.items():
                sub_val = value._data.get(name)
                value._data[name] = self._generate_auto_fields_sync(sub_val, sub_field)
            return value

        # ListField
        if isinstance(field, ListField) and isinstance(value, list):
            return [
                self._generate_auto_fields_sync(item, field.field) for item in value
            ]

        # DictField
        if isinstance(field, DictField) and isinstance(value, dict):
            return {
                k: self._generate_auto_fields_sync(v, field.field)
                for k, v in value.items()
            }

        # Auto-generation (SYNC ONLY)
        if field and field._auto_gen and value is None:
            return field.generate()

        return value

    async def _generate_auto_fields_async(self, value, field):
        from mongoengine.base import BaseDocument
        from mongoengine.fields import ListField, DictField

        # EmbeddedDocument
        if isinstance(value, BaseDocument) and not value._is_document:
            for name, sub_field in value._fields.items():
                sub_val = value._data.get(name)
                value._data[name] = await self._generate_auto_fields_async(
                    sub_val, sub_field
                )
            return value

        # ListField
        if isinstance(field, ListField) and isinstance(value, list):
            return [
                await self._generate_auto_fields_async(item, field.field)
                for item in value
            ]

        # DictField
        if isinstance(field, DictField) and isinstance(value, dict):
            return {
                k: await self._generate_auto_fields_async(v, field.field)
                for k, v in value.items()
            }

        # Auto-generation (ASYNC ONLY)
        if field and field._auto_gen and value is None:
            return await field.async_generate()

        return value

    async def asave(
        self,
        force_insert=False,
        validate=True,
        clean=True,
        write_concern=None,
        cascade=None,
        cascade_kwargs=None,
        _refs=None,
        save_condition=None,
        signal_kwargs=None,
        **kwargs,
    ) -> "Document":
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created. Returns the saved object instance.

        :param force_insert: only try to create a new document, don't allow
            updates of existing documents.
        :param validate: validates the document; set to ``False`` to skip.
        :param clean: call the document clean method, requires `validate` to be
            True.
        :param write_concern: Extra keyword arguments are passed down to
            :meth:`~pymongo.collection.Collection.save` OR
            :meth:`~pymongo.collection.Collection.insert`
            which will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param cascade: Sets the flag for cascading saves.  You can set a
            default by setting "cascade" in the document __meta__
        :param cascade_kwargs: (optional) kwargs dictionary to be passed throw
            to cascading saves.  Implies ``cascade=True``.
        :param _refs: A list of processed references used in cascading saves
        :param save_condition: only perform save if matching record in db
            satisfies condition(s) (e.g. version number).
            Raises :class:`OperationError` if the conditions are not satisfied
        :param signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.

        .. versionchanged:: 0.5
            In existing documents it only saves changed fields using
            set / unset.  Saves are cascaded and any
            :class:`~bson.dbref.DBRef` objects that have changes are
            saved as well.
        .. versionchanged:: 0.6
            Added cascading saves
        .. versionchanged:: 0.8
            Cascade saves are optional and default to False.  If you want
            fine grain control then you can turn off using document
            meta['cascade'] = True.  Also you can pass different kwargs to
            the cascade save using cascade_kwargs which overwrites the
            existing kwargs with custom values.
        .. versionchanged:: 0.26
           save() no longer calls :meth:`~mongoengine.Document.ensure_indexes`
           unless ``meta['auto_create_index_on_save']`` is set to True.

        """
        await _normalize_async_values_document(self)
        signal_kwargs = signal_kwargs or {}

        if self._meta.get("abstract"):
            raise InvalidDocumentError("Cannot save an abstract document.")

        await signals.pre_save.send_async(
            self.__class__, document=self, **signal_kwargs
        )

        if validate:
            self.validate(clean=clean)

        if write_concern is None:
            write_concern = {}

        doc_id = self.to_mongo(fields=[self._meta["id_field"]])
        created = "_id" not in doc_id or self._created or force_insert

        await signals.pre_save_post_validation.send_async(
            self.__class__, document=self, created=created, **signal_kwargs
        )
        # it might be refreshed by the pre_save_post_validation hook, e.g., for etag generation
        # Handle self generating fields
        for name, field in self._fields.items():
            value = self._data.get(name)
            self._data[name] = await self._generate_auto_fields_async(value, field)

        doc = self.to_mongo()

        # Initialize the Document's underlying pymongo.Collection (+create indexes) if not already initialized
        # Important to do this here to avoid that the index creation gets wrapped in the try/except block below
        # and turned into mongoengine.OperationError
        try:
            # Save a new document or update an existing one
            if created:
                object_id = await self._asave_create(
                    doc=doc, force_insert=force_insert, write_concern=write_concern
                )
            else:
                object_id, created = await self._asave_update(
                    doc, save_condition, write_concern
                )

            if cascade is None:
                cascade = self._meta.get("cascade", False) or cascade_kwargs is not None

            if cascade:
                kwargs = {
                    "force_insert": force_insert,
                    "validate": validate,
                    "write_concern": write_concern,
                    "cascade": cascade,
                }
                if cascade_kwargs:  # Allow granular control over cascades
                    kwargs.update(cascade_kwargs)
                kwargs["_refs"] = _refs
                await self.acascade_save(**kwargs)

        except pymongo.errors.DuplicateKeyError as err:
            message = "Tried to save duplicate unique keys (%s)"
            raise NotUniqueError(message % err)
        except pymongo.errors.OperationFailure as err:
            message = "Could not save document (%s)"
            if re.match("^E1100[01] duplicate key", str(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = "Tried to save duplicate unique keys (%s)"
                raise NotUniqueError(message % err)
            raise OperationError(message % err)

        # Make sure we store the PK on this document now that it's saved
        id_field = self._meta["id_field"]
        if created or id_field not in self._meta.get("shard_key", []):
            self[id_field] = self._fields[id_field].to_python(object_id)

        await signals.post_save.send_async(
            self.__class__, document=self, created=created, **signal_kwargs
        )

        self._clear_changed_fields()
        self._created = False

        return self

    def _save_create(self, doc, force_insert, write_concern):
        """Save a new document.

        Helper method, should only be used inside save().
        """
        state = self._data.get("_instance_state")
        if state:
            db_alias = state.get("db_alias")
            collection = state.get("collection")
            collection_name = state.get("collection_name")
        else:
            db_alias = None
            collection = None
            collection_name = None
        if collection is None:
            collection = self._get_collection(
                db_alias=db_alias, collection_name=collection_name
            )
        if self._meta.get("auto_create_index_on_save", False):
            # ensure_indexes is called as part of _get_collection so no need to re-call it again here
            self.ensure_indexes(collection)
        with set_write_concern(collection, write_concern) as wc_collection:
            if force_insert:
                return wc_collection.insert_one(doc, session=_get_session()).inserted_id
            # insert_one will provoke UniqueError alongside save does not
            # therefore, it need to catch and call replace_one.
            if "_id" in doc:
                select_dict = {"_id": doc["_id"]}
                select_dict = self._integrate_shard_key(doc, select_dict)
                raw_object = wc_collection.find_one_and_replace(
                    select_dict, doc, session=_get_session()
                )
                if raw_object:
                    return doc["_id"]

            object_id = wc_collection.insert_one(
                doc, session=_get_session()
            ).inserted_id

        return object_id

    async def _asave_create(self, doc, force_insert, write_concern):
        """Save a new document.

        Helper method, should only be used inside save().
        """
        state = self._data.get("_instance_state")
        if state:
            db_alias = state.get("db_alias")
            collection = state.get("collection")
            collection_name = state.get("collection_name")
        else:
            db_alias = None
            collection = None
            collection_name = None
        if collection is None:
            collection = await self._aget_collection(
                db_alias=db_alias, collection_name=collection_name
            )
        if self._meta.get("auto_create_index_on_save", False):
            # ensure_indexes is called as part of _get_collection so no need to re-call it again here
            await self.aensure_indexes(collection)
        with set_write_concern(collection, write_concern) as wc_collection:
            if force_insert:
                return (
                    await wc_collection.insert_one(doc, session=_get_session())
                ).inserted_id
            # insert_one will provoke UniqueError alongside save does not
            # therefore, it need to catch and call replace_one.
            if "_id" in doc:
                select_dict = {"_id": doc["_id"]}
                select_dict = self._integrate_shard_key(doc, select_dict)
                raw_object = await wc_collection.find_one_and_replace(
                    select_dict, doc, session=_get_session()
                )
                if raw_object:
                    return doc["_id"]

            object_id = (
                await wc_collection.insert_one(doc, session=_get_session())
            ).inserted_id

        return object_id

    def _get_update_doc(self):
        """Return a dict containing all the $set and $unset operations
        that should be sent to MongoDB based on the changes made to this
        Document.
        """
        updates, removals = self._delta()

        update_doc = {}
        if updates:
            update_doc["$set"] = updates
        if removals:
            update_doc["$unset"] = removals

        return update_doc

    def _integrate_shard_key(self, doc, select_dict):
        """Integrates the collection's shard key to the `select_dict`, which will be used for the query.
        The value from the shard key is taken from the `doc` and finally the select_dict is returned.
        """

        # Need to add shard key to query, or you get an error
        shard_key = self._meta.get("shard_key", tuple())
        for k in shard_key:
            path = self._lookup_field(k.split("."))
            actual_key = [p.db_field for p in path]
            val = doc
            for ak in actual_key:
                val = val[ak]
            select_dict[".".join(actual_key)] = val

        return select_dict

    def _save_update(self, doc, save_condition, write_concern):
        """Update an existing document.

        Helper method, should only be used inside save().
        """
        state = self._data.get("_instance_state")
        if state:
            db_alias = state.get("db_alias")
            collection = state.get("collection")
            collection_name = state.get("collection_name")
        else:
            db_alias = None
            collection = None
            collection_name = None
        if collection is None:
            collection = self._get_collection(
                db_alias=db_alias, collection_name=collection_name
            )
        if self._meta.get("auto_create_index_on_save", False):
            # ensure_indexes is called as part of _get_collection so no need to re-call it again here
            self.ensure_indexes(collection)

        object_id = doc["_id"]
        created = False

        select_dict = {}
        if save_condition is not None:
            select_dict = transform.query(self.__class__, **save_condition)

        select_dict["_id"] = object_id

        select_dict = self._integrate_shard_key(doc, select_dict)

        update_doc = self._get_update_doc()
        if update_doc:
            upsert = save_condition is None
            with set_write_concern(collection, write_concern) as wc_collection:
                last_error = wc_collection.update_one(
                    select_dict, update_doc, upsert=upsert, session=_get_session()
                ).raw_result
            if not upsert and last_error["n"] == 0:
                raise SaveConditionError(
                    "Race condition preventing document update detected"
                )
            if last_error is not None:
                updated_existing = last_error.get("updatedExisting")
                if updated_existing is False:
                    created = True
                    # !!! This is bad, means we accidentally created a new,
                    # potentially corrupted document. See
                    # https://github.com/MongoEngine/mongoengine/issues/564

        return object_id, created

    async def _asave_update(self, doc, save_condition, write_concern):
        """Update an existing document.

        Helper method, should only be used inside save().
        """
        state = self._data.get("_instance_state")
        if state:
            db_alias = state.get("db_alias")
            collection = state.get("collection")
            collection_name = state.get("collection_name")
        else:
            db_alias = None
            collection = None
            collection_name = None
        if collection is None:
            collection = await self._aget_collection(
                db_alias=db_alias, collection_name=collection_name
            )
        if self._meta.get("auto_create_index_on_save", False):
            # ensure_indexes is called as part of _get_collection so no need to re-call it again here
            await self.aensure_indexes(collection)
        object_id = doc["_id"]
        created = False

        select_dict = {}
        if save_condition is not None:
            select_dict = transform.query(self.__class__, **save_condition)

        select_dict["_id"] = object_id

        select_dict = self._integrate_shard_key(doc, select_dict)

        update_doc = self._get_update_doc()
        if update_doc:
            upsert = save_condition is None
            with set_write_concern(collection, write_concern) as wc_collection:
                last_error = (
                    await wc_collection.update_one(
                        select_dict, update_doc, upsert=upsert, session=_get_session()
                    )
                ).raw_result
            if not upsert and last_error["n"] == 0:
                raise SaveConditionError(
                    "Race condition preventing document update detected"
                )
            if last_error is not None:
                updated_existing = last_error.get("updatedExisting")
                if updated_existing is False:
                    created = True
                    # !!! This is bad, means we accidentally created a new,
                    # potentially corrupted document. See
                    # https://github.com/MongoEngine/mongoengine/issues/564

        return object_id, created

    def cascade_save(self, **kwargs):
        """Recursively save any references and generic references on the
        document.
        """
        _refs = kwargs.get("_refs") or []

        ReferenceField = _import_class("ReferenceField")
        GenericReferenceField = _import_class("GenericReferenceField")

        for name, cls in self._fields.items():
            if not isinstance(cls, (ReferenceField, GenericReferenceField)):
                continue

            ref = self._data.get(name)
            if not ref or isinstance(ref, DBRef):
                continue

            if not getattr(ref, "_changed_fields", True):
                continue

            ref_id = f"{ref.__class__.__name__},{str(ref._data)}"
            if ref and ref_id not in _refs:
                _refs.append(ref_id)
                kwargs["_refs"] = _refs
                ref.save(**kwargs)
                ref._changed_fields = []

    async def acascade_save(self, **kwargs):
        """Recursively save any references and generic references on the
        document.
        """
        _refs = kwargs.get("_refs") or []

        ReferenceField = _import_class("ReferenceField")
        GenericReferenceField = _import_class("GenericReferenceField")

        for name, cls in self._fields.items():
            if not isinstance(cls, (ReferenceField, GenericReferenceField)):
                continue

            ref = self._data.get(name)
            if not ref or isinstance(ref, DBRef):
                continue

            if not getattr(ref, "_changed_fields", True):
                continue

            ref_id = f"{ref.__class__.__name__},{str(ref._data)}"
            if ref and ref_id not in _refs:
                _refs.append(ref_id)
                kwargs["_refs"] = _refs
                await ref.asave(**kwargs)
                ref._changed_fields = []

    @property
    def _qs(self):
        """Return the default queryset corresponding to this document."""
        if not hasattr(self, "__objects"):
            queryset_class = self._meta.get("queryset_class", QuerySet)
            self.__objects = queryset_class(self.__class__)
        return self.__objects

    @property
    def _aqs(self):
        """Return the default queryset corresponding to this document."""
        if not hasattr(self, "__objects"):
            queryset_class = self._meta.get("queryset_class", AsyncQuerySet)
            self.__objects = queryset_class(self.__class__)
        return self.__objects

    @property
    def _object_key(self):
        """Return a query dict that can be used to fetch this document.

        Most of the time the dict is a simple PK lookup, but in case of
        a sharded collection with a compound shard key, it can contain a more
        complex query.

        Note that the dict returned by this method uses MongoEngine field
        names instead of PyMongo field names (e.g. "pk" instead of "_id",
        "some__nested__field" instead of "some.nested.field", etc.).
        """
        select_dict = {"pk": self.pk}
        shard_key = self.__class__._meta.get("shard_key", tuple())
        for k in shard_key:
            val = self
            field_parts = k.split(".")
            for part in field_parts:
                val = getattr(val, part)
            select_dict["__".join(field_parts)] = val
        return select_dict

    def update(self, **kwargs):
        """Performs an update on the :class:`~mongoengine.Document`
        A convenience wrapper to :meth:`~mongoengine.QuerySet.update`.

        Raises :class:`OperationError` if called on an object that has not yet
        been saved.
        """
        state = self._data.get("_instance_state")
        if state:
            db_alias = state.get("db_alias")
            collection_name = state.get("collection_name")
        else:
            db_alias = None
            collection_name = None
        db_alias = self._db_alias(db_alias=db_alias)
        if self.pk is None:
            if kwargs.get("upsert", False):
                query = self.to_mongo()
                if "_cls" in query:
                    del query["_cls"]
                return (
                    self._qs.using(db_alias, collection_name)
                    .filter(**query)
                    .update_one(**kwargs)
                )
            else:
                raise OperationError("attempt to update a document not yet saved")

        # Need to add shard key to query, or you get an error
        return (
            self._qs.using(db_alias, collection_name)
            .filter(**self._object_key)
            .update_one(**kwargs)
        )

    async def aupdate(self, **kwargs):
        """Performs an update on the :class:`~mongoengine.Document`
        A convenience wrapper to :meth:`~mongoengine.QuerySet.update`.

        Raises :class:`OperationError` if called on an object that has not yet
        been saved.
        """
        state = self._data.get("_instance_state")
        if state:
            db_alias = state.get("db_alias")
            collection_name = state.get("collection_name")
        else:
            db_alias = None
            collection_name = None
        db_alias = self._db_alias(db_alias=db_alias)
        if self.pk is None:
            if kwargs.get("upsert", False):
                query = self.to_mongo()
                if "_cls" in query:
                    del query["_cls"]
                return (
                    await self._aqs.using(db_alias, collection_name)
                    .filter(**query)
                    .update_one(**kwargs)
                )
            else:
                raise OperationError("attempt to update a document not yet saved")

        # Need to add shard key to query, or you get an error
        return (
            await self._aqs.using(db_alias, collection_name)
            .filter(**self._object_key)
            .update_one(**kwargs)
        )

    def delete(self, signal_kwargs=None, **write_concern):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.
        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant ``getLastError`` command.
            For example, ``save(..., w: 2, fsync: True)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        """

        state = self._data.get("_instance_state")
        if state:
            db_alias = state.get("db_alias")
            collection_name = state.get("collection_name")
        else:
            db_alias = None
            collection_name = None
        signal_kwargs = signal_kwargs or {}
        signals.pre_delete.send(self.__class__, document=self, **signal_kwargs)

        # Delete FileFields separately
        FileField = _import_class("FileField")
        for name, field in self._fields.items():
            if isinstance(field, FileField):
                getattr(self, name).delete()

        try:
            self._qs.using(db_alias, collection_name).filter(**self._object_key).delete(
                write_concern=write_concern, _from_doc_delete=True
            )
        except OperationFailure as err:
            message = "Could not delete document (%s)" % err.args
            raise OperationError(message)
        signals.post_delete.send(self.__class__, document=self, **signal_kwargs)

    async def adelete(self, signal_kwargs=None, **write_concern):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.
        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant ``getLastError`` command.
            For example, ``save(..., w: 2, fsync: True)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        """
        state = self._data.get("_instance_state")
        if state:
            db_alias = state.get("db_alias")
            collection_name = state.get("collection_name")
        else:
            db_alias = None
            collection_name = None
        signal_kwargs = signal_kwargs or {}
        await signals.pre_delete.send_async(
            self.__class__, document=self, **signal_kwargs
        )

        # Delete FileFields separately
        FileField = _import_class("FileField")
        for name, field in self._fields.items():
            if isinstance(field, FileField):
                await getattr(self, name).adelete()

        try:
            await (
                self._aqs.using(db_alias, collection_name)
                .filter(**self._object_key)
                .delete(write_concern=write_concern, _from_doc_delete=True)
            )
        except OperationFailure as err:
            message = "Could not delete document (%s)" % err.args
            raise OperationError(message)
        await signals.post_delete.send_async(
            self.__class__, document=self, **signal_kwargs
        )

    def switch_db(self, db_alias: str = DEFAULT_CONNECTION_NAME, keep_created=True):
        """
        Temporarily switch the database for a document instance.

        Only really useful for archiving off data and calling `save()`::

            user = User.objects.get(id=user_id)
            user.switch_db('archive-db')
            user.save()

        :param str db_alias: The database alias to use for saving the document

        :param bool keep_created: keep self._created value after switching db, else is reset to True


        .. seealso::
            Use :class:`~mongoengine.context_managers.switch_collection`
            if you need to read from another collection
        """
        state = self._data.setdefault("_instance_state", {})
        # Store alias
        state["db_alias"] = db_alias
        # Invalidate cached collection
        state["collection"] = None
        self._created = True if not keep_created else self._created
        return self

    def switch_collection(self, collection_name, keep_created=True):
        """
        Temporarily switch the collection for a document instance.

        Only really useful for archiving off data and calling `save()`::

            user = User.objects.get(id=user_id)
            user.switch_collection('old-users')
            user.save()

        :param str collection_name: The database alias to use for saving the
            document

        :param bool keep_created: keep self._created value after switching collection, else is reset to True


        .. see also::
            Use :class:`~mongoengine.context_managers.switch_db`
            if you need to read from another database
        """
        state = self._data.setdefault("_instance_state", {})

        # Store collection override for this instance
        state["collection_name"] = collection_name

        # Invalidate cached collection for this instance
        state["collection"] = None

        self._created = True if not keep_created else self._created
        return self

    def reload(self, *fields, **kwargs):
        """Async reload the document from MongoDB using aggregation."""
        if self.pk is None:
            raise DoesNotExist("Document does not exist")

        # -------------------------
        # Handle max_depth
        # -------------------------
        if fields and isinstance(fields[0], int):
            max_depth = fields[0]
            fields = fields[1:]

        # -------------------------
        # Build queryset for reload
        # -------------------------
        if self._select_related:
            qs = (
                self.__class__.objects.read_preference(ReadPreference.PRIMARY)
                .filter(pk=self.pk)
                .select_related(*self._select_related)
                .limit(1)
            )
        else:
            qs = (
                self.__class__.objects.read_preference(ReadPreference.PRIMARY)
                .filter(pk=self.pk)
                .limit(1)
            )

        # Add shard key filter support (including nested keys)
        qs._query = self._integrate_shard_key(self.to_mongo(), qs._query)
        if fields:
            qs = qs.only(*fields)

        try:
            son = next(qs._cursor)
        except StopIteration:
            raise DoesNotExist("Document does not exist")

        # -------------------------
        # Convert SON → Document (new instance)
        # -------------------------
        fresh = self._from_son(son, created=True)

        # -------------------------
        # Copy fields from `fresh` → `self`
        # -------------------------
        for field in fresh._data:
            if not fields or field in fields:
                try:
                    setattr(self, field, self._reload(field, fresh[field]))
                except Exception:
                    setattr(self, field, self._reload(field, fresh._data.get(field)))

        # Remove fields that disappeared (same as normal reload)
        for field in list(self._data.keys()):
            if field not in fresh._data and (not fields or field in fields):
                delattr(self, field)

        # Update change tracking
        self._changed_fields = (
            list(set(self._changed_fields) - set(fields))
            if fields
            else fresh._changed_fields
        )

        self._created = False
        return self

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
        self._select_related = fields
        return self.reload()

    async def aselect_related(self, *fields: str):
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
        self._select_related = fields
        return await self.areload()

    async def areload(self, *fields, **kwargs):
        """Async reload the document from MongoDB using aggregation."""
        if self.pk is None:
            raise DoesNotExist("Document does not exist")

        # -------------------------
        # Handle max_depth
        # -------------------------
        if fields and isinstance(fields[0], int):
            max_depth = fields[0]
            fields = fields[1:]

        # -------------------------
        # Build queryset for reload
        # -------------------------
        if self._select_related:
            qs = (
                self.__class__.aobjects.read_preference(ReadPreference.PRIMARY)
                .filter(pk=self.pk)
                .select_related(*self._select_related)
                .limit(1)
            )
        else:
            qs = (
                self.__class__.aobjects.read_preference(ReadPreference.PRIMARY)
                .filter(pk=self.pk)
                .limit(1)
            )

        # Add shard key filter support (including nested keys)
        qs._query = self._integrate_shard_key(self.to_mongo(), qs._query)
        if fields:
            qs = qs.only(*fields)

        try:
            son = await anext(await qs._cursor)
        except StopAsyncIteration:
            raise DoesNotExist("Document does not exist")

        # -------------------------
        # Convert SON → Document (new instance)
        # -------------------------
        fresh = self._from_son(son, created=True)

        # -------------------------
        # Copy fields from `fresh` → `self`
        # -------------------------
        for field in fresh._data:
            if not fields or field in fields:
                try:
                    setattr(self, field, self._reload(field, fresh[field]))
                except Exception:
                    setattr(self, field, self._reload(field, fresh._data.get(field)))

        # Remove fields that disappeared (same as normal reload)
        for field in list(self._data.keys()):
            if field not in fresh._data and (not fields or field in fields):
                delattr(self, field)

        # Update change tracking
        self._changed_fields = (
            list(set(self._changed_fields) - set(fields))
            if fields
            else fresh._changed_fields
        )

        self._created = False
        return self

    def _reload(self, key, value):
        """Used by :meth:`~mongoengine.Document.reload` to ensure the
        correct instance is linked to self.
        """
        if isinstance(value, BaseDict):
            value = [(k, self._reload(k, v)) for k, v in value.items()]
            value = BaseDict(value, self, key)
        elif isinstance(value, EmbeddedDocumentList):
            value = [self._reload(key, v) for v in value]
            value = EmbeddedDocumentList(value, self, key)
        elif isinstance(value, BaseList):
            value = [self._reload(key, v) for v in value]
            value = BaseList(value, self, key)
        elif isinstance(value, (EmbeddedDocument, DynamicEmbeddedDocument)):
            value._instance = None
            value._changed_fields = []
        return value

    def to_dbref(self):
        """Returns an instance of :class:`~bson.dbref.DBRef` useful in
        `__raw__` queries."""
        if self.pk is None:
            msg = "Only saved documents can have a valid dbref"
            raise OperationError(msg)
        return DBRef(self.__class__._get_collection_name(), self.pk)

    @classmethod
    def register_delete_rule(cls, document_cls, field_name, rule):
        """This method registers the delete rules to apply when removing this
        object.
        """
        classes = [
            _DocumentRegistry.get(class_name)
            for class_name in cls._subclasses
            if class_name != cls.__name__
        ] + [cls]
        documents = [
            _DocumentRegistry.get(class_name)
            for class_name in document_cls._subclasses
            if class_name != document_cls.__name__
        ] + [document_cls]

        for klass in classes:
            for document_cls in documents:
                delete_rules = klass._meta.get("delete_rules") or {}
                delete_rules[(document_cls, field_name)] = rule
                klass._meta["delete_rules"] = delete_rules

    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this
        :class:`~mongoengine.Document` type from the database.

        Raises :class:`OperationError` if the document has no collection set
        (i.g. if it is `abstract`)
        """
        coll_name = cls._get_collection_name()
        coll_type = cls._collection_type()
        coll_fingerprint = cls._collection_fingerprint()
        if not coll_name:
            raise OperationError(
                "Document %s has no collection defined (is it abstract ?)" % cls
            )
        db_alias = cls._db_alias(db_alias=None)
        db = cls._get_db()
        db.drop_collection(coll_name, session=_get_session())
        _CollectionRegistry.unregister(
            db_alias=db_alias,
            name=coll_name,
            is_async=False,
            type_=coll_type,
            fingerprint=coll_fingerprint,
        )

    @classmethod
    async def adrop_collection(cls):
        """Drops the entire collection associated with this
        :class:`~mongoengine.Document` type from the database.

        Raises :class:`OperationError` if the document has no collection set
        (i.g. if it is `abstract`)
        """
        coll_name = cls._get_collection_name()
        coll_type = cls._collection_type()
        coll_fingerprint = cls._collection_fingerprint()
        if not coll_name:
            raise OperationError(
                "Document %s has no collection defined (is it abstract ?)" % cls
            )
        db_alias = cls._db_alias(db_alias=None)
        db = await cls._async_get_db(db_alias=None)
        await db.drop_collection(coll_name, session=_get_session())
        _CollectionRegistry.unregister(
            db_alias=db_alias,
            name=coll_name,
            is_async=True,
            type_=coll_type,
            fingerprint=coll_fingerprint,
        )

    @classmethod
    def create_index(cls, keys, background=False, **kwargs):
        """Creates the given indexes if required.

        :param keys: a single index key or a list of index keys (to
            construct a multi-field index); keys may be prefixed with a **+**
            or a **-** to determine the index ordering
        :param background: Allows index creation in the background
        """
        index_spec = cls._build_index_spec(keys)
        index_spec = index_spec.copy()
        fields = index_spec.pop("fields")
        index_spec["background"] = background
        index_spec.update(kwargs)

        return cls._get_collection().create_index(
            fields, session=_get_session(), **index_spec
        )

    @classmethod
    async def acreate_index(cls, keys, background=False, **kwargs):
        """Creates the given indexes if required.

        :param keys: a single index key or a list of index keys (to
            construct a multi-field index); keys may be prefixed with a **+**
            or a **-** to determine the index ordering
        :param background: Allows index creation in the background
        """
        index_spec = cls._build_index_spec(keys)
        index_spec = index_spec.copy()
        fields = index_spec.pop("fields")
        index_spec["background"] = background
        index_spec.update(kwargs)

        return await (await cls._aget_collection()).create_index(
            fields, session=_get_session(), **index_spec
        )

    @classmethod
    def ensure_indexes(cls, collection: Collection | None = None):
        """Checks the document meta data and ensures all the indexes exist.

        Global defaults can be set in the meta - see :doc:`guide/defining-documents`

        By default, this will get called automatically upon first interaction with the
        Document collection (query, save, etc) so unless you disabled `auto_create_index`, you
        shouldn't have to call this manually.

        This also gets called upon every call to Document.save if `auto_create_index_on_save` is set to True

        If called multiple times, MongoDB will not re-recreate indexes if they exist already

        .. note:: You can disable automatic index creation by setting
                  `auto_create_index` to False in the documents meta data
        """
        if collection is None:
            collection = cls._get_collection()
        background = cls._meta.get("index_background", False)
        index_opts = cls._meta.get("index_opts") or {}
        index_cls = cls._meta.get("index_cls", True)

        # determine if an index which we are creating includes
        # _cls as its first field; if so, we can avoid creating
        # an extra index on _cls, as mongodb will use the existing
        # index to service queries against _cls
        cls_indexed = False

        # Ensure document-defined indexes are created
        if cls._meta["index_specs"]:
            index_spec = cls._meta["index_specs"]
            for spec in index_spec:
                spec = spec.copy()
                fields = spec.pop("fields")
                cls_indexed = cls_indexed or includes_cls(fields)
                opts = index_opts.copy()
                opts.update(spec)

                # we shouldn't pass 'cls' to the collection.ensureIndex options
                # because of https://jira.mongodb.org/browse/SERVER-769
                if "cls" in opts:
                    del opts["cls"]

                collection.create_index(
                    fields, background=background, session=_get_session(), **opts
                )

        # If _cls is being used (for polymorphism), it needs an index,
        # only if another index doesn't begin with _cls
        if index_cls and not cls_indexed and cls._meta.get("allow_inheritance"):
            # we shouldn't pass 'cls' to the collection.ensureIndex options
            # because of https://jira.mongodb.org/browse/SERVER-769
            if "cls" in index_opts:
                del index_opts["cls"]

            collection.create_index(
                "_cls", background=background, session=_get_session(), **index_opts
            )

    @classmethod
    async def aensure_indexes(cls, collection: AsyncCollection | None = None):
        """Checks the document meta data and ensures all the indexes exist.

        Global defaults can be set in the meta - see :doc:`guide/defining-documents`

        By default, this will get called automatically upon first interaction with the
        Document collection (query, save, etc) so unless you disabled `auto_create_index`, you
        shouldn't have to call this manually.

        This also gets called upon every call to Document.save if `auto_create_index_on_save` is set to True

        If called multiple times, MongoDB will not re-recreate indexes if they exist already

        .. note:: You can disable automatic index creation by setting
                  `auto_create_index` to False in the documents meta data
        """
        if collection is None:
            collection = await cls._aget_collection()

        background = cls._meta.get("index_background", False)
        index_opts = cls._meta.get("index_opts") or {}
        index_cls = cls._meta.get("index_cls", True)

        # determine if an index which we are creating includes
        # _cls as its first field; if so, we can avoid creating
        # an extra index on _cls, as mongodb will use the existing
        # index to service queries against _cls
        cls_indexed = False

        # Ensure document-defined indexes are created
        if cls._meta["index_specs"]:
            index_spec = cls._meta["index_specs"]
            for spec in index_spec:
                spec = spec.copy()
                fields = spec.pop("fields")
                cls_indexed = cls_indexed or includes_cls(fields)
                opts = index_opts.copy()
                opts.update(spec)

                # we shouldn't pass 'cls' to the collection.ensureIndex options
                # because of https://jira.mongodb.org/browse/SERVER-769
                if "cls" in opts:
                    del opts["cls"]

                await collection.create_index(
                    fields, background=background, session=_get_session(), **opts
                )

        # If _cls is being used (for polymorphism), it needs an index,
        # only if another index doesn't begin with _cls
        if index_cls and not cls_indexed and cls._meta.get("allow_inheritance"):
            # we shouldn't pass 'cls' to the collection.ensureIndex options
            # because of https://jira.mongodb.org/browse/SERVER-769
            if "cls" in index_opts:
                del index_opts["cls"]

            await collection.create_index(
                "_cls", background=background, session=_get_session(), **index_opts
            )

    @classmethod
    def list_indexes(cls):
        """Lists all indexes that should be created for the Document collection.
        It includes all the indexes from super- and sub-classes.

        Note that it will only return the indexes' fields, not the indexes' options
        """
        if cls._meta.get("abstract"):
            return []

        # get all the base classes, subclasses and siblings
        classes = []

        def get_classes(cls):
            if cls not in classes and isinstance(cls, TopLevelDocumentMetaclass):
                classes.append(cls)

            for base_cls in cls.__bases__:
                if (
                    isinstance(base_cls, TopLevelDocumentMetaclass)
                    and base_cls != Document
                    and not base_cls._meta.get("abstract")
                    and base_cls._get_collection().full_name
                    == cls._get_collection().full_name
                    and base_cls not in classes
                ):
                    classes.append(base_cls)
                    get_classes(base_cls)
            for subclass in cls.__subclasses__():
                if (
                    isinstance(base_cls, TopLevelDocumentMetaclass)
                    and subclass._get_collection().full_name
                    == cls._get_collection().full_name
                    and subclass not in classes
                ):
                    classes.append(subclass)
                    get_classes(subclass)

        get_classes(cls)

        # get the indexes spec for all the gathered classes
        def get_indexes_spec(cls):
            indexes = []

            if cls._meta["index_specs"]:
                index_spec = cls._meta["index_specs"]
                for spec in index_spec:
                    spec = spec.copy()
                    fields = spec.pop("fields")
                    indexes.append(fields)
            return indexes

        indexes = []
        for klass in classes:
            for index in get_indexes_spec(klass):
                if index not in indexes:
                    indexes.append(index)

        # finish up by appending { '_id': 1 } and { '_cls': 1 }, if needed
        if [("_id", 1)] not in indexes:
            indexes.append([("_id", 1)])
        if cls._meta.get("index_cls", True) and cls._meta.get("allow_inheritance"):
            indexes.append([("_cls", 1)])

        return indexes

    @classmethod
    async def alist_indexes(cls):
        """Lists all indexes that should be created for the Document collection.
        It includes all the indexes from super- and sub-classes.

        Note that it will only return the indexes' fields, not the indexes' options
        """
        if cls._meta.get("abstract"):
            return []

        # get all the base classes, subclasses and siblings
        classes = []

        async def get_classes(cls):
            if cls not in classes and isinstance(cls, TopLevelDocumentMetaclass):
                classes.append(cls)

            for base_cls in cls.__bases__:
                if (
                    isinstance(base_cls, TopLevelDocumentMetaclass)
                    and base_cls != Document
                    and not base_cls._meta.get("abstract")
                    and (await base_cls._aget_collection()).full_name
                    == (await cls._aget_collection()).full_name
                    and base_cls not in classes
                ):
                    classes.append(base_cls)
                    await get_classes(base_cls)
            for subclass in cls.__subclasses__():
                if (
                    isinstance(base_cls, TopLevelDocumentMetaclass)
                    and (await subclass._aget_collection()).full_name
                    == (await cls._aget_collection()).full_name
                    and subclass not in classes
                ):
                    classes.append(subclass)
                    await get_classes(subclass)

        await get_classes(cls)

        # get the indexes spec for all the gathered classes
        def get_indexes_spec(cls):
            indexes = []

            if cls._meta["index_specs"]:
                index_spec = cls._meta["index_specs"]
                for spec in index_spec:
                    spec = spec.copy()
                    fields = spec.pop("fields")
                    indexes.append(fields)
            return indexes

        indexes = []
        for klass in classes:
            for index in get_indexes_spec(klass):
                if index not in indexes:
                    indexes.append(index)

        # finish up by appending { '_id': 1 } and { '_cls': 1 }, if needed
        if [("_id", 1)] not in indexes:
            indexes.append([("_id", 1)])
        if cls._meta.get("index_cls", True) and cls._meta.get("allow_inheritance"):
            indexes.append([("_cls", 1)])

        return indexes

    @classmethod
    def compare_indexes(cls):
        """Compares the indexes defined in MongoEngine with the ones
        existing in the database. Returns any missing/extra indexes.
        """

        required = cls.list_indexes()

        existing = []
        collection = cls._get_collection()
        for info in collection.index_information(session=_get_session()).values():
            if "_fts" in info["key"][0]:
                # Useful for text indexes (but not only)
                index_type = info["key"][0][1]
                text_index_fields = info.get("weights").keys()
                # Use NonOrderedList to avoid order comparison, see #2612
                existing.append(
                    NonOrderedList([(key, index_type) for key in text_index_fields])
                )
            else:
                existing.append(info["key"])

        missing = [index for index in required if index not in existing]
        extra = [index for index in existing if index not in required]

        # if { _cls: 1 } is missing, make sure it's *really* necessary
        if [("_cls", 1)] in missing:
            cls_obsolete = False
            for index in existing:
                if includes_cls(index) and index not in extra:
                    cls_obsolete = True
                    break
            if cls_obsolete:
                missing.remove([("_cls", 1)])

        return {"missing": missing, "extra": extra}

    @classmethod
    async def acompare_indexes(cls):
        """Compares the indexes defined in MongoEngine with the ones
        existing in the database. Returns any missing/extra indexes.
        """

        required = await cls.alist_indexes()

        existing = []
        collection = await cls._aget_collection()
        for info in (
            await collection.index_information(session=_get_session())
        ).values():
            if "_fts" in info["key"][0]:
                # Useful for text indexes (but not only)
                index_type = info["key"][0][1]
                text_index_fields = info.get("weights").keys()
                # Use NonOrderedList to avoid order comparison, see #2612
                existing.append(
                    NonOrderedList([(key, index_type) for key in text_index_fields])
                )
            else:
                existing.append(info["key"])

        missing = [index for index in required if index not in existing]
        extra = [index for index in existing if index not in required]

        # if { _cls: 1 } is missing, make sure it's *really* necessary
        if [("_cls", 1)] in missing:
            cls_obsolete = False
            for index in existing:
                if includes_cls(index) and index not in extra:
                    cls_obsolete = True
                    break
            if cls_obsolete:
                missing.remove([("_cls", 1)])

        return {"missing": missing, "extra": extra}


class DynamicDocument(Document, metaclass=TopLevelDocumentMetaclass):
    """A Dynamic Document class allowing flexible, expandable and uncontrolled
    schemas.  As a :class:`~mongoengine.Document` subclass, acts in the same
    way as an ordinary document but has expanded style properties.  Any data
    passed or set against the :class:`~mongoengine.DynamicDocument` that is
    not a field is automatically converted into a
    :class:`~mongoengine.fields.DynamicField` and data can be attributed to that
    field.

    .. note::

        There is one caveat on Dynamic Documents: undeclared fields cannot start with `_`
    """

    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = TopLevelDocumentMetaclass

    _dynamic = True

    def __delattr__(self, *args, **kwargs):
        """Delete the attribute by setting to None and allowing _delta
        to unset it.
        """
        field_name = args[0]
        if field_name in self._dynamic_fields:
            setattr(self, field_name, None)
            self._dynamic_fields[field_name].null = False
        else:
            super().__delattr__(*args, **kwargs)


class DynamicEmbeddedDocument(EmbeddedDocument, metaclass=DocumentMetaclass):
    """A Dynamic Embedded Document class allowing flexible, expandable and
    uncontrolled schemas. See :class:`~mongoengine.DynamicDocument` for more
    information about dynamic documents.
    """

    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = DocumentMetaclass

    _dynamic = True

    def __delattr__(self, *args, **kwargs):
        """Delete the attribute by setting to None and allowing _delta
        to unset it.
        """
        field_name = args[0]
        if field_name in self._fields:
            default = self._fields[field_name].default
            if callable(default):
                default = default()
            setattr(self, field_name, default)
        else:
            setattr(self, field_name, None)


class MapReduceDocument:
    """A document returned from a map/reduce query.

    :param collection: An instance of :class:`~pymongo.Collection`
    :param key: Document/result key, often an instance of
                :class:`~bson.objectid.ObjectId`. If supplied as
                an ``ObjectId`` found in the given ``collection``,
                the object can be accessed via the ``object`` property.
    :param value: The result(s) for this key.
    """

    def __init__(self, document, collection, key, value):
        self._document = document
        self._instance_collection = collection
        self.key = key
        self.value = value

    @property
    def object(self):
        """Lazy-load the object referenced by ``self.key``. ``self.key``
        should be the ``primary_key``.
        """
        id_field = self._document()._meta["id_field"]
        id_field_type = type(id_field)

        if not isinstance(self.key, id_field_type):
            try:
                self.key = id_field_type(self.key)
            except Exception:
                raise Exception("Could not cast key as %s" % id_field_type.__name__)

        if not hasattr(self, "_key_object"):
            self._key_object = self._document.objects.with_id(self.key)
            return self._key_object
        return self._key_object

    @property
    async def aobject(self):
        """Lazy-load the object referenced by ``self.key``. ``self.key``
        should be the ``primary_key``.
        """
        id_field = self._document()._meta["id_field"]
        id_field_type = type(id_field)

        if not isinstance(self.key, id_field_type):
            try:
                self.key = id_field_type(self.key)
            except Exception:
                raise Exception("Could not cast key as %s" % id_field_type.__name__)

        if not hasattr(self, "_key_object"):
            self._key_object = await self._document.aobjects.with_id(self.key)
            return self._key_object
        return self._key_object
