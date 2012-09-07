import warnings

import pymongo
import re

from bson.dbref import DBRef
from mongoengine import signals, queryset

from base import (DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument,
                  BaseDict, BaseList)
from queryset import OperationError, NotUniqueError
from connection import get_db, DEFAULT_CONNECTION_NAME

__all__ = ['Document', 'EmbeddedDocument', 'DynamicDocument',
           'DynamicEmbeddedDocument', 'OperationError',
           'InvalidCollectionError', 'NotUniqueError']


class InvalidCollectionError(Exception):
    pass


class EmbeddedDocument(BaseDocument):
    """A :class:`~mongoengine.Document` that isn't stored in its own
    collection.  :class:`~mongoengine.EmbeddedDocument`\ s should be used as
    fields on :class:`~mongoengine.Document`\ s through the
    :class:`~mongoengine.EmbeddedDocumentField` field type.

    A :class:`~mongoengine.EmbeddedDocument` subclass may be itself subclassed,
    to create a specialised version of the embedded document that will be
    stored in the same collection. To facilitate this behaviour, `_cls` and
    `_types` fields are added to documents (hidden though the MongoEngine
    interface though). To disable this behaviour and remove the dependence on
    the presence of `_cls` and `_types`, set :attr:`allow_inheritance` to
    ``False`` in the :attr:`meta` dictionary.
    """

    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass  = DocumentMetaclass
    __metaclass__ = DocumentMetaclass

    def __init__(self, *args, **kwargs):
        super(EmbeddedDocument, self).__init__(*args, **kwargs)
        self._changed_fields = []

    def __delattr__(self, *args, **kwargs):
        """Handle deletions of fields"""
        field_name = args[0]
        if field_name in self._fields:
            default = self._fields[field_name].default
            if callable(default):
                default = default()
            setattr(self, field_name, default)
        else:
            super(EmbeddedDocument, self).__delattr__(*args, **kwargs)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._data == other._data
        return False


class Document(BaseDocument):
    """The base class used for defining the structure and properties of
    collections of documents stored in MongoDB. Inherit from this class, and
    add fields as class attributes to define a document's structure.
    Individual documents may then be created by making instances of the
    :class:`~mongoengine.Document` subclass.

    By default, the MongoDB collection used to store documents created using a
    :class:`~mongoengine.Document` subclass will be the name of the subclass
    converted to lowercase. A different collection may be specified by
    providing :attr:`collection` to the :attr:`meta` dictionary in the class
    definition.

    A :class:`~mongoengine.Document` subclass may be itself subclassed, to
    create a specialised version of the document that will be stored in the
    same collection. To facilitate this behaviour, `_cls` and `_types`
    fields are added to documents (hidden though the MongoEngine interface
    though). To disable this behaviour and remove the dependence on the
    presence of `_cls` and `_types`, set :attr:`allow_inheritance` to
    ``False`` in the :attr:`meta` dictionary.

    A :class:`~mongoengine.Document` may use a **Capped Collection** by
    specifying :attr:`max_documents` and :attr:`max_size` in the :attr:`meta`
    dictionary. :attr:`max_documents` is the maximum number of documents that
    is allowed to be stored in the collection, and :attr:`max_size` is the
    maximum size of the collection in bytes. If :attr:`max_size` is not
    specified and :attr:`max_documents` is, :attr:`max_size` defaults to
    10000000 bytes (10MB).

    Indexes may be created by specifying :attr:`indexes` in the :attr:`meta`
    dictionary. The value should be a list of field names or tuples of field
    names. Index direction may be specified by prefixing the field names with
    a **+** or **-** sign.

    Automatic index creation can be disabled by specifying
    attr:`auto_create_index` in the :attr:`meta` dictionary. If this is set to
    False then indexes will not be created by MongoEngine.  This is useful in
    production systems where index creation is performed as part of a deployment
    system.

    By default, _types will be added to the start of every index (that
    doesn't contain a list) if allow_inheritance is True. This can be
    disabled by either setting types to False on the specific index or
    by setting index_types to False on the meta dictionary for the document.
    """

    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass  = TopLevelDocumentMetaclass
    __metaclass__ = TopLevelDocumentMetaclass

    def pk():
        """Primary key alias
        """
        def fget(self):
            return getattr(self, self._meta['id_field'])
        def fset(self, value):
            return setattr(self, self._meta['id_field'], value)
        return property(fget, fset)
    pk = pk()

    @classmethod
    def _get_db(cls):
        """Some Model using other db_alias"""
        return get_db(cls._meta.get("db_alias", DEFAULT_CONNECTION_NAME ))

    @classmethod
    def _get_collection(cls):
        """Returns the collection for the document."""
        if not hasattr(cls, '_collection') or cls._collection is None:
            db = cls._get_db()
            collection_name = cls._get_collection_name()
            # Create collection as a capped collection if specified
            if cls._meta['max_size'] or cls._meta['max_documents']:
                # Get max document limit and max byte size from meta
                max_size = cls._meta['max_size'] or 10000000  # 10MB default
                max_documents = cls._meta['max_documents']

                if collection_name in db.collection_names():
                    cls._collection = db[collection_name]
                    # The collection already exists, check if its capped
                    # options match the specified capped options
                    options = cls._collection.options()
                    if options.get('max') != max_documents or \
                       options.get('size') != max_size:
                        msg = (('Cannot create collection "%s" as a capped '
                               'collection as it already exists')
                                % cls._collection)
                        raise InvalidCollectionError(msg)
                else:
                    # Create the collection as a capped collection
                    opts = {'capped': True, 'size': max_size}
                    if max_documents:
                        opts['max'] = max_documents
                    cls._collection = db.create_collection(
                        collection_name, **opts
                    )
            else:
                cls._collection = db[collection_name]
        return cls._collection

    def save(self, safe=True, force_insert=False, validate=True,
             write_options=None,  cascade=None, cascade_kwargs=None,
             _refs=None):
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created.

        If ``safe=True`` and the operation is unsuccessful, an
        :class:`~mongoengine.OperationError` will be raised.

        :param safe: check if the operation succeeded before returning
        :param force_insert: only try to create a new document, don't allow
            updates of existing documents
        :param validate: validates the document; set to ``False`` to skip.
        :param write_options: Extra keyword arguments are passed down to
            :meth:`~pymongo.collection.Collection.save` OR
            :meth:`~pymongo.collection.Collection.insert`
            which will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_options={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param cascade: Sets the flag for cascading saves.  You can set a
            default by setting "cascade" in the document __meta__
        :param cascade_kwargs: optional kwargs dictionary to be passed throw
            to cascading saves
        :param _refs: A list of processed references used in cascading saves

        .. versionchanged:: 0.5
            In existing documents it only saves changed fields using
            set / unset.  Saves are cascaded and any
            :class:`~bson.dbref.DBRef` objects that have changes are
            saved as well.
        .. versionchanged:: 0.6
            Cascade saves are optional = defaults to True, if you want
            fine grain control then you can turn off using document
            meta['cascade'] = False  Also you can pass different kwargs to
            the cascade save using cascade_kwargs which overwrites the
            existing kwargs with custom values
        """
        signals.pre_save.send(self.__class__, document=self)

        if validate:
            self.validate()

        if not write_options:
            write_options = {}

        doc = self.to_mongo()

        created = force_insert or '_id' not in doc

        try:
            collection = self.__class__.objects._collection
            if created:
                if force_insert:
                    object_id = collection.insert(doc, safe=safe,
                                                  **write_options)
                else:
                    object_id = collection.save(doc, safe=safe,
                                                **write_options)
            else:
                object_id = doc['_id']
                updates, removals = self._delta()
                # Need to add shard key to query, or you get an error
                select_dict = {'_id': object_id}
                shard_key = self.__class__._meta.get('shard_key', tuple())
                for k in shard_key:
                    actual_key = self._db_field_map.get(k, k)
                    select_dict[actual_key] = doc[actual_key]

                upsert = self._created
                if updates:
                    collection.update(select_dict, {"$set": updates},
                        upsert=upsert, safe=safe, **write_options)
                if removals:
                    collection.update(select_dict, {"$unset": removals},
                        upsert=upsert, safe=safe, **write_options)

            warn_cascade = not cascade and 'cascade' not in self._meta
            cascade = (self._meta.get('cascade', True)
                       if cascade is None else cascade)
            if cascade:
                kwargs = {
                    "safe": safe,
                    "force_insert": force_insert,
                    "validate": validate,
                    "write_options": write_options,
                    "cascade": cascade
                }
                if cascade_kwargs:  # Allow granular control over cascades
                    kwargs.update(cascade_kwargs)
                kwargs['_refs'] = _refs
                self.cascade_save(warn_cascade=warn_cascade, **kwargs)

        except pymongo.errors.OperationFailure, err:
            message = 'Could not save document (%s)'
            if re.match('^E1100[01] duplicate key', unicode(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = u'Tried to save duplicate unique keys (%s)'
                raise NotUniqueError(message % unicode(err))
            raise OperationError(message % unicode(err))
        id_field = self._meta['id_field']
        if id_field not in self._meta.get('shard_key', []):
            self[id_field] = self._fields[id_field].to_python(object_id)

        self._changed_fields = []
        self._created = False
        signals.post_save.send(self.__class__, document=self, created=created)
        return self

    def cascade_save(self, warn_cascade=None, *args, **kwargs):
        """Recursively saves any references /
           generic references on an objects"""
        import fields
        _refs = kwargs.get('_refs', []) or []

        for name, cls in self._fields.items():
            if not isinstance(cls, (fields.ReferenceField,
                                    fields.GenericReferenceField)):
                continue

            ref = getattr(self, name)
            if not ref or isinstance(ref, DBRef):
                continue

            if not getattr(ref, '_changed_fields', True):
                continue

            ref_id = "%s,%s" % (ref.__class__.__name__, str(ref._data))
            if ref and ref_id not in _refs:
                if warn_cascade:
                    msg = ("Cascading saves will default to off in 0.8, "
                          "please  explicitly set `.save(cascade=True)`")
                    warnings.warn(msg, FutureWarning)
                _refs.append(ref_id)
                kwargs["_refs"] = _refs
                ref.save(**kwargs)
                ref._changed_fields = []

    @property
    def _object_key(self):
        """Dict to identify object in collection
        """
        select_dict = {'pk': self.pk}
        shard_key = self.__class__._meta.get('shard_key', tuple())
        for k in shard_key:
            select_dict[k] = getattr(self, k)
        return select_dict

    def update(self, **kwargs):
        """Performs an update on the :class:`~mongoengine.Document`
        A convenience wrapper to :meth:`~mongoengine.QuerySet.update`.

        Raises :class:`OperationError` if called on an object that has not yet
        been saved.
        """
        if not self.pk:
            raise OperationError('attempt to update a document not yet saved')

        # Need to add shard key to query, or you get an error
        return self.__class__.objects(**self._object_key).update_one(**kwargs)

    def delete(self, safe=False):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param safe: check if the operation succeeded before returning
        """
        signals.pre_delete.send(self.__class__, document=self)

        try:
            self.__class__.objects(**self._object_key).delete(safe=safe)
        except pymongo.errors.OperationFailure, err:
            message = u'Could not delete document (%s)' % err.message
            raise OperationError(message)

        signals.post_delete.send(self.__class__, document=self)

    def select_related(self, max_depth=1):
        """Handles dereferencing of :class:`~bson.dbref.DBRef` objects to
        a maximum depth in order to cut down the number queries to mongodb.

        .. versionadded:: 0.5
        """
        import dereference
        self._data = dereference.DeReference()(self._data, max_depth)
        return self

    def reload(self, max_depth=1):
        """Reloads all attributes from the database.

        .. versionadded:: 0.1.2
        .. versionchanged:: 0.6  Now chainable
        """
        id_field = self._meta['id_field']
        obj = self.__class__.objects(
                **{id_field: self[id_field]}
              ).first().select_related(max_depth=max_depth)
        for field in self._fields:
            setattr(self, field, self._reload(field, obj[field]))
        if self._dynamic:
            for name in self._dynamic_fields.keys():
                setattr(self, name, self._reload(name, obj._data[name]))
        self._changed_fields = obj._changed_fields
        return obj

    def _reload(self, key, value):
        """Used by :meth:`~mongoengine.Document.reload` to ensure the
        correct instance is linked to self.
        """
        if isinstance(value, BaseDict):
            value = [(k, self._reload(k, v)) for k, v in value.items()]
            value = BaseDict(value, self, key)
        elif isinstance(value, BaseList):
            value = [self._reload(key, v) for v in value]
            value = BaseList(value, self, key)
        elif isinstance(value, (EmbeddedDocument, DynamicEmbeddedDocument)):
            value._changed_fields = []
        return value

    def to_dbref(self):
        """Returns an instance of :class:`~bson.dbref.DBRef` useful in
        `__raw__` queries."""
        if not self.pk:
            msg = "Only saved documents can have a valid dbref"
            raise OperationError(msg)
        return DBRef(self.__class__._get_collection_name(), self.pk)

    @classmethod
    def register_delete_rule(cls, document_cls, field_name, rule):
        """This method registers the delete rules to apply when removing this
        object.
        """
        delete_rules = cls._meta.get('delete_rules') or {}
        delete_rules[(document_cls, field_name)] = rule
        cls._meta['delete_rules'] = delete_rules

    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this
        :class:`~mongoengine.Document` type from the database.
        """
        db = cls._get_db()
        db.drop_collection(cls._get_collection_name())
        queryset.QuerySet._reset_already_indexed(cls)


class DynamicDocument(Document):
    """A Dynamic Document class allowing flexible, expandable and uncontrolled
    schemas.  As a :class:`~mongoengine.Document` subclass, acts in the same
    way as an ordinary document but has expando style properties.  Any data
    passed or set against the :class:`~mongoengine.DynamicDocument` that is
    not a field is automatically converted into a
    :class:`~mongoengine.DynamicField` and data can be attributed to that
    field.

    .. note::

        There is one caveat on Dynamic Documents: fields cannot start with `_`
    """

    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass  = TopLevelDocumentMetaclass
    __metaclass__ = TopLevelDocumentMetaclass

    _dynamic = True

    def __delattr__(self, *args, **kwargs):
        """Deletes the attribute by setting to None and allowing _delta to unset
        it"""
        field_name = args[0]
        if field_name in self._dynamic_fields:
            setattr(self, field_name, None)
        else:
            super(DynamicDocument, self).__delattr__(*args, **kwargs)


class DynamicEmbeddedDocument(EmbeddedDocument):
    """A Dynamic Embedded Document class allowing flexible, expandable and
    uncontrolled schemas. See :class:`~mongoengine.DynamicDocument` for more
    information about dynamic documents.
    """

    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass  = DocumentMetaclass
    __metaclass__ = DocumentMetaclass

    _dynamic = True

    def __delattr__(self, *args, **kwargs):
        """Deletes the attribute by setting to None and allowing _delta to unset
        it"""
        field_name = args[0]
        setattr(self, field_name, None)


class MapReduceDocument(object):
    """A document returned from a map/reduce query.

    :param collection: An instance of :class:`~pymongo.Collection`
    :param key: Document/result key, often an instance of
                :class:`~bson.objectid.ObjectId`. If supplied as
                an ``ObjectId`` found in the given ``collection``,
                the object can be accessed via the ``object`` property.
    :param value: The result(s) for this key.

    .. versionadded:: 0.3
    """

    def __init__(self, document, collection, key, value):
        self._document = document
        self._collection = collection
        self.key = key
        self.value = value

    @property
    def object(self):
        """Lazy-load the object referenced by ``self.key``. ``self.key``
        should be the ``primary_key``.
        """
        id_field = self._document()._meta['id_field']
        id_field_type = type(id_field)

        if not isinstance(self.key, id_field_type):
            try:
                self.key = id_field_type(self.key)
            except:
                raise Exception("Could not cast key as %s" % \
                                id_field_type.__name__)

        if not hasattr(self, "_key_object"):
            self._key_object = self._document.objects.with_id(self.key)
            return self._key_object
        return self._key_object
