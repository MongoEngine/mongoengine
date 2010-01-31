from base import (DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument,
                  ValidationError)
from queryset import OperationError, QuerySet
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
    """

    __metaclass__ = TopLevelDocumentMetaclass

    def save(self, safe=True, force_insert=False):
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created.

        If ``safe=True`` and the operation is unsuccessful, an 
        :class:`~mongoengine.OperationError` will be raised.

        :param safe: check if the operation succeeded before returning
        :param force_insert: only try to create a new document, don't allow 
            updates of existing documents
        """
        self.validate()
        doc = self.to_mongo()
        try:
            collection = self.__class__.objects._collection
            if force_insert:
                object_id = collection.insert(doc, safe=safe)
            else:
                if getattr(self, 'id', None) == None:
                    # new document
                    object_id = collection.save(doc, safe=safe)
                else:
                    # update document
                    modified_fields = map(lambda obj: obj[0], filter(lambda obj: obj[1].modified, self._fields.items()))
                    modified_doc = dict(filter(lambda k: k[0] in modified_fields, doc.items()))                
                    try:
                        id_field = self._meta['id_field']
                        idObj = self._fields[id_field].to_mongo(self['id'])
                        collection.update({'_id': idObj}, {'$set': modified_doc}, safe=safe)
                    except pymongo.errors.OperationFailure, err:
                        if str(err) == 'multi not coded yet':
                            raise OperationError('update() method requires MongoDB 1.1.3+')
                        raise OperationError('Update failed (%s)' % str(err))
                    object_id = self['id']

            for field in self._fields.values(): field.modified = False
        except pymongo.errors.OperationFailure, err:
            message = 'Could not save document (%s)'
            if 'duplicate key' in str(err):
                message = 'Tried to save duplicate unique keys (%s)'
            raise OperationError(message % str(err))

        id_field = self._meta['id_field']
        self[id_field] = self._fields[id_field].to_python(object_id)

    def delete(self, safe=False):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param safe: check if the operation succeeded before returning
        """
        id_field = self._meta['id_field']
        object_id = self._fields[id_field].to_mongo(self[id_field])
        try:
            self.__class__.objects(**{id_field: object_id}).delete(safe=safe)
        except pymongo.errors.OperationFailure, err:
            raise OperationError('Could not delete document (%s)' % str(err))

    def reload(self):
        """Reloads all attributes from the database.

        .. versionadded:: 0.1.2
        """
        id_field = self._meta['id_field']
        obj = self.__class__.objects(**{id_field: self[id_field]}).first()
        for field in self._fields:
            setattr(self, field, obj[field])
            obj.modified = False

    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this
        :class:`~mongoengine.Document` type from the database.
        """
        db = _get_db()
        db.drop_collection(cls._meta['collection'])
