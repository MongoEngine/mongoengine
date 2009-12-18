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
        object_id = self.objects._collection.save(self.to_mongo())
        self.id = object_id

    def delete(self):
        """Delete the document from the database. This will only take effect
        if the document has been previously saved.
        """
        self.objects._collection.remove(self.id)

    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this Document type from
        the database.
        """
        db = _get_db()
        db.drop_collection(cls._meta['collection'])
