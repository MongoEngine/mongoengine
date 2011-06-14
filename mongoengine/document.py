from mongoengine import signals
from base import (DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument,
                  ValidationError, BaseDict, BaseList)
from queryset import OperationError
from connection import _get_db

import pymongo

__all__ = ['Document', 'EmbeddedDocument', 'ValidationError', 'OperationError']


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

    def save(self, safe=True, force_insert=False, validate=True, write_options=None):
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
        signals.pre_save.send(self.__class__, document=self)

        if validate:
            self.validate()

        if not write_options:
            write_options = {}

        doc = self.to_mongo()
        created = '_id' not in doc
        try:
            collection = self.__class__.objects._collection
            if force_insert:
                object_id = collection.insert(doc, safe=safe, **write_options)
            if created:
                object_id = collection.save(doc, safe=safe, **write_options)
            else:
                object_id = doc['_id']
                updates, removals = self._delta()
                if updates:
                    collection.update({'_id': object_id}, {"$set": updates}, upsert=True, safe=safe, **write_options)
                if removals:
                    collection.update({'_id': object_id}, {"$unset": removals}, upsert=True, safe=safe, **write_options)
        except pymongo.errors.OperationFailure, err:
            message = 'Could not save document (%s)'
            if u'duplicate key' in unicode(err):
                message = u'Tried to save duplicate unique keys (%s)'
            raise OperationError(message % unicode(err))
        id_field = self._meta['id_field']
        self[id_field] = self._fields[id_field].to_python(object_id)
        self._changed_fields = []
        signals.post_save.send(self.__class__, document=self, created=created)

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
        db.drop_collection(cls._meta['collection'])


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
