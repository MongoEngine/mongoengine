from base import DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument

#import pymongo

class EmbeddedDocument(BaseDocument):
    
    __metaclass__ = DocumentMetaclass


class Document(BaseDocument):

    __metaclass__ = TopLevelDocumentMetaclass

