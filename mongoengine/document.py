from base import DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument
from connection import _get_db


__all__ = ['Document', 'EmbeddedDocument']


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
    """

    __metaclass__ = TopLevelDocumentMetaclass

    def save(self):
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created.
        """
        object_id = self.objects._collection.save(self.to_mongo())
        self.id = object_id

    def delete(self):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.
        """
        object_id = self._fields['id'].to_mongo(self.id)
        self.__class__.objects(_id=object_id).delete()

    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this
        :class:`~mongoengine.Document` type from the database.
        """
        db = _get_db()
        db.drop_collection(cls._meta['collection'])
