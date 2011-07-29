from mongoengine import signals
from base import (DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument,
                  ValidationError, BaseDict, BaseList)
from queryset import OperationError
from connection import _get_db

import pymongo

__all__ = ['Document', 'EmbeddedDocument', 'ValidationError',
           'OperationError', 'InvalidCollectionError']


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

    @classmethod
    def _get_collection(self):
        """Returns the collection for the document."""
        db = _get_db()
        collection_name = self._get_collection_name()

        if not hasattr(self, '_collection') or self._collection is None:
            # Create collection as a capped collection if specified
            if self._meta['max_size'] or self._meta['max_documents']:
                # Get max document limit and max byte size from meta
                max_size = self._meta['max_size'] or 10000000  # 10MB default
                max_documents = self._meta['max_documents']

                if collection_name in db.collection_names():
                    self._collection = db[collection_name]
                    # The collection already exists, check if its capped
                    # options match the specified capped options
                    options = self._collection.options()
                    if options.get('max') != max_documents or \
                       options.get('size') != max_size:
                        msg = ('Cannot create collection "%s" as a capped '
                               'collection as it already exists') % self._collection
                        raise InvalidCollectionError(msg)
                else:
                    # Create the collection as a capped collection
                    opts = {'capped': True, 'size': max_size}
                    if max_documents:
                        opts['max'] = max_documents
                    self._collection = db.create_collection(
                        collection_name, **opts
                    )
            else:
                self._collection = db[collection_name]
        return self._collection

    def save(self, safe=True, force_insert=False, validate=True, write_options=None, _refs=None):
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
        """
        from fields import ReferenceField, GenericReferenceField

        signals.pre_save.send(self.__class__, document=self)

        if validate:
            self.validate()

        if not write_options:
            write_options = {}

        doc = self.to_mongo()

        created = '_id' in doc
        creation_mode = force_insert or not created
        try:
            collection = self.__class__.objects._collection
            if creation_mode:
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

            # Save any references / generic references
            _refs = _refs or []
            for name, cls in self._fields.items():
                if isinstance(cls, (ReferenceField, GenericReferenceField)):
                    ref = getattr(self, name)
                    if ref and str(ref) not in _refs:
                        _refs.append(str(ref))
                        ref.save(safe=safe, force_insert=force_insert,
                                 validate=validate, write_options=write_options,
                                 _refs=_refs)

        except pymongo.errors.OperationFailure, err:
            message = 'Could not save document (%s)'
            if u'duplicate key' in unicode(err):
                message = u'Tried to save duplicate unique keys (%s)'
            raise OperationError(message % unicode(err))
        id_field = self._meta['id_field']
        self[id_field] = self._fields[id_field].to_python(object_id)

        def reset_changed_fields(doc, inspected_docs=None):
            """Loop through and reset changed fields lists"""

            inspected_docs = inspected_docs or []
            inspected_docs.append(doc)
            if hasattr(doc, '_changed_fields'):
                doc._changed_fields = []

            for field_name in doc._fields:
                field = getattr(doc, field_name)
                if field not in inspected_docs and hasattr(field, '_changed_fields'):
                    reset_changed_fields(field, inspected_docs)

        reset_changed_fields(self)
        signals.post_save.send(self.__class__, document=self, created=creation_mode)

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
        from dereference import dereference
        self._data = dereference(self._data, max_depth)
        return self

    def reload(self):
        """Reloads all attributes from the database.

        .. versionadded:: 0.1.2
        """
        id_field = self._meta['id_field']
        obj = self.__class__.objects(**{id_field: self[id_field]}).first()
        for field in self._fields:
            setattr(self, field, self._reload(field, obj[field]))
        self._changed_fields = []

    def _reload(self, key, value):
        """Used by :meth:`~mongoengine.Document.reload` to ensure the
        correct instance is linked to self.
        """
        if isinstance(value, BaseDict):
            value = [(k, self._reload(k,v)) for k,v in value.items()]
            value = BaseDict(value, instance=self, name=key)
        elif isinstance(value, BaseList):
            value = [self._reload(key, v) for v in value]
            value = BaseList(value, instance=self, name=key)
        elif isinstance(value, EmbeddedDocument):
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
        db = _get_db()
        db.drop_collection(cls._get_collection_name())


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
