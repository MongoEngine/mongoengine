from base import DocumentMetaclass, BaseDocument

#import pymongo

class TopLevelDocumentMetaclass(DocumentMetaclass):
    """Metaclass for top-level documents (i.e. documents that have their own
    collection in the database.
    """

    def __new__(cls, name, bases, attrs):
        # Classes defined in this module are abstract and should not have 
        # their own metadata with DB collection, etc.
        if attrs['__module__'] != __name__:
            collection = name.lower()
            # Subclassed documents inherit collection from superclass
            for base in bases:
                if hasattr(base, '_meta') and 'collection' in base._meta:
                    collection = base._meta['collection']

            meta = {
                'collection': collection,
            }
            meta.update(attrs.get('meta', {}))
            attrs['_meta'] = meta
        return DocumentMetaclass.__new__(cls, name, bases, attrs)


class EmbeddedDocument(BaseDocument):
    
    __metaclass__ = DocumentMetaclass


class Document(BaseDocument):

    __metaclass__ = TopLevelDocumentMetaclass

