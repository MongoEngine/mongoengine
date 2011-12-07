from mongoengine import signals
from base import (DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument,
                  BaseDict, BaseList, DataObserver)
from queryset import OperationError
from connection import get_db, DEFAULT_CONNECTION_NAME

import pymongo

__all__ = ['Document', 'EmbeddedDocument', 'DynamicDocument',
           'DynamicEmbeddedDocument', 'OperationError', 'InvalidCollectionError']


class InvalidCollectionError(Exception):
    pass


class EmbeddedDocument(BaseDocument):
    """A :class:`~mongoengine.Document` that isn't stored in its own
    collection.  :class:`~mongoengine.EmbeddedDocument`\ s should be used as
    fields on :class:`~mongoengine.Document`\ s through the
    :class:`~mongoengine.EmbeddedDocumentField` field type.
    """

    __metaclass__ = DocumentMetaclass

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

    By default, _types will be added to the start of every index (that
    doesn't contain a list) if allow_inheritence is True. This can be
    disabled by either setting types to False on the specific index or
    by setting index_types to False on the meta dictionary for the document.
    """
    __metaclass__ = TopLevelDocumentMetaclass

    @apply
    def pk():
        """Primary key alias
        """
        def fget(self):
            return getattr(self, self._meta['id_field'])
        def fset(self, value):
            return setattr(self, self._meta['id_field'], value)
        return property(fget, fset)

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
                        msg = ('Cannot create collection "%s" as a capped '
                               'collection as it already exists') % cls._collection
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

    def save(self, safe=True, force_insert=False, validate=True, write_options=None,
            cascade=None, cascade_kwargs=None, _refs=None):
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
                which will be used as options for the resultant ``getLastError`` command.
                For example, ``save(..., w=2, fsync=True)`` will wait until at least two servers
                have recorded the write and will force an fsync on each server being written to.
        :param cascade: Sets the flag for cascading saves.  You can set a default by setting
            "cascade" in the document __meta__
        :param cascade_kwargs: optional kwargs dictionary to be passed throw to cascading saves
        :param _refs: A list of processed references used in cascading saves

        .. versionchanged:: 0.5
            In existing documents it only saves changed fields using set / unset
            Saves are cascaded and any :class:`~pymongo.dbref.DBRef` objects
            that have changes are saved as well.
        .. versionchanged:: 0.6
            Cascade saves are optional = defaults to True, if you want fine grain
            control then you can turn off using document meta['cascade'] = False
            Also you can pass different kwargs to the cascade save using cascade_kwargs
            which overwrites the existing kwargs with custom values

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
                    object_id = collection.insert(doc, safe=safe, **write_options)
                else:
                    object_id = collection.save(doc, safe=safe, **write_options)
            else:
                object_id = doc['_id']
                updates, removals = self._delta()
                if updates:
                    collection.update({'_id': object_id}, {"$set": updates}, upsert=True, safe=safe, **write_options)
                if removals:
                    collection.update({'_id': object_id}, {"$unset": removals}, upsert=True, safe=safe, **write_options)

            cascade = self._meta.get('cascade', True) if cascade is None else cascade
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
                self.cascade_save(**kwargs)

        except pymongo.errors.OperationFailure, err:
            message = 'Could not save document (%s)'
            if u'duplicate key' in unicode(err):
                message = u'Tried to save duplicate unique keys (%s)'
            raise OperationError(message % unicode(err))
        id_field = self._meta['id_field']
        self[id_field] = self._fields[id_field].to_python(object_id)

        self._changed_fields = []
        signals.post_save.send(self.__class__, document=self, created=created)

    def cascade_save(self, *args, **kwargs):
        """Recursively saves any references / generic references on an object"""
        from fields import ReferenceField, GenericReferenceField
        _refs = kwargs.get('_refs', []) or []
        for name, cls in self._fields.items():
            if not isinstance(cls, (ReferenceField, GenericReferenceField)):
                continue
            ref = getattr(self, name)
            if not ref:
                continue
            ref_id = "%s,%s" % (ref.__class__.__name__, str(ref._data))
            if ref and ref_id not in _refs:
                _refs.append(ref_id)
                kwargs["_refs"] = _refs
                ref.save(**kwargs)
                ref._changed_fields = []

    def update(self, **kwargs):
        """Performs an update on the :class:`~mongoengine.Document`
        A convenience wrapper to :meth:`~mongoengine.QuerySet.update`.

        Raises :class:`OperationError` if called on an object that has not yet
        been saved.
        """
        if not self.pk:
            raise OperationError('attempt to update a document not yet saved')

        return self.__class__.objects(pk=self.pk).update_one(**kwargs)

    def delete(self, safe=False):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param safe: check if the operation succeeded before returning
        """
        signals.pre_delete.send(self.__class__, document=self)

        id_field = self._meta['id_field']
        object_id = self._fields[id_field].to_mongo(self[id_field])
        try:
            self.__class__.objects(**{id_field: object_id}).delete(safe=safe)
        except pymongo.errors.OperationFailure, err:
            message = u'Could not delete document (%s)' % err.message
            raise OperationError(message)

        signals.post_delete.send(self.__class__, document=self)

    def select_related(self, max_depth=1):
        """Handles dereferencing of :class:`~pymongo.dbref.DBRef` objects to
        a maximum depth in order to cut down the number queries to mongodb.

        .. versionadded:: 0.5
        """
        from dereference import dereference
        self._data = dereference(self._data, max_depth)
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
            observer = DataObserver(self, key)
            value = BaseDict(value, observer)
        elif isinstance(value, BaseList):
            value = [self._reload(key, v) for v in value]
            observer = DataObserver(self, key)
            value = BaseList(value, observer)
        elif isinstance(value, (EmbeddedDocument, DynamicEmbeddedDocument)):
            value._changed_fields = []
        return value

    def to_dbref(self):
        """Returns an instance of :class:`~pymongo.dbref.DBRef` useful in
        `__raw__` queries."""
        if not self.pk:
            msg = "Only saved documents can have a valid dbref"
            raise OperationError(msg)
        return pymongo.dbref.DBRef(self.__class__._get_collection_name(), self.pk)

    @classmethod
    def register_delete_rule(cls, document_cls, field_name, rule):
        """This method registers the delete rules to apply when removing this
        object.
        """
        cls._meta['delete_rules'][(document_cls, field_name)] = rule

    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this
        :class:`~mongoengine.Document` type from the database.
        """
        from mongoengine.queryset import QuerySet
        db = cls._get_db()
        db.drop_collection(cls._get_collection_name())
        QuerySet._reset_already_indexed(cls)


class DynamicDocument(Document):
    """A Dynamic Document class allowing flexible, expandable and uncontrolled
    schemas.  As a :class:`~mongoengine.Document` subclass, acts in the same
    way as an ordinary document but has expando style properties.  Any data
    passed or set against the :class:`~mongoengine.DynamicDocument` that is
    not a field is automatically converted into a
    :class:`~mongoengine.BaseDynamicField` and data can be attributed to that
    field.

    ..note::

        There is one caveat on Dynamic Documents: fields cannot start with `_`
    """
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
                :class:`~pymongo.objectid.ObjectId`. If supplied as
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
