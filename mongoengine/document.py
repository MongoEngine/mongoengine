from base import DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument


__all__ = ['Document', 'EmbeddedDocument']


class EmbeddedDocument(BaseDocument):
    
    __metaclass__ = DocumentMetaclass


class Document(BaseDocument):

    __metaclass__ = TopLevelDocumentMetaclass

    def save(self):
        """Save the document to the database. If the document already exists,
        it will be updated, otherwise it will be created.
        """
        self.objects._save_document(self)
