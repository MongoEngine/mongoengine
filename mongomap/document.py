from base import DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument


__all__ = ['Document', 'EmbeddedDocument']


class EmbeddedDocument(BaseDocument):
    
    __metaclass__ = DocumentMetaclass


class Document(BaseDocument):

    __metaclass__ = TopLevelDocumentMetaclass

    def save(self):
        self.collection._save_document(self._to_mongo())
