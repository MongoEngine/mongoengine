import pymongo

import fields

class DocumentMetaclass(type):
    """Metaclass for all documents.
    """

    def __new__(cls, name, bases, attrs):
        doc_fields = {}

        # Include all fields present in superclasses
        for base in bases:
            if hasattr(base, '_fields'):
                doc_fields.update(base._fields)

        # Add the document's fields to the _fields attribute
        for attr_name, attr_val in attrs.items():
            if issubclass(attr_val.__class__, fields.Field):
                if not attr_val.name:
                    attr_val.name = attr_name
                doc_fields[attr_name] = attr_val
        attrs['_fields'] = doc_fields

        return type.__new__(cls, name, bases, attrs)


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


class Document(object):

    __metaclass__ = TopLevelDocumentMetaclass

    def __init__(self, **values):
        self._data = {}
        # Assign initial values to instance
        for attr_name, attr_value in self._fields.items():
            if attr_name in values:
                setattr(self, attr_name, values.pop(attr_name))
            else:
                # Use default value
                setattr(self, attr_name, getattr(self, attr_name))

    def __iter__(self):
        # Use _data rather than _fields as iterator only looks at names so
        # values don't need to be converted to Python types
        return iter(self._data)
