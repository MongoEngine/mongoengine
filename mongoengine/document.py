from base import DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument
from connection import _get_db


__all__ = ['Document', 'EmbeddedDocument']


class EmbeddedDocument(BaseDocument):
    
    __metaclass__ = DocumentMetaclass


class Document(BaseDocument):

    __metaclass__ = TopLevelDocumentMetaclass

    def save(self):
        """Save the document to the database. If the document already exists,
        it will be updated, otherwise it will be created.
        """
        _id = self.objects._collection.save(self._to_mongo())
        self._id = _id

    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this Document type from
        the database.
        """
        db = _get_db()
        db.drop_collection(cls._meta['collection'])
