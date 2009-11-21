from base import BaseField, ObjectIdField, ValidationError
from document import Document, EmbeddedDocument
from connection import _get_db
 
import re
import pymongo


__all__ = ['StringField', 'IntField', 'EmbeddedDocumentField', 'ListField',
           'ObjectIdField', 'ReferenceField', 'ValidationError']


class StringField(BaseField):
    """A unicode string field.
    """
    
    def __init__(self, regex=None, max_length=None, **kwargs):
        self.regex = re.compile(regex) if regex else None
        self.max_length = max_length
        super(StringField, self).__init__(**kwargs)
    
    def _to_python(self, value):
        assert(isinstance(value, (str, unicode)))
        return unicode(value)

    def _validate(self, value):
        assert(isinstance(value, (str, unicode)))

        if self.max_length is not None and len(value) > self.max_length:
            raise ValidationError('String value is too long')

        if self.regex is not None and self.regex.match(value) is None:
            message = 'String value did not match validation regex'
            raise ValidationError(message)


class IntField(BaseField):
    """An integer field.
    """

    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value, self.max_value = min_value, max_value
        super(IntField, self).__init__(**kwargs)
    
    def _to_python(self, value):
        assert(isinstance(value, int))
        return int(value)

    def _validate(self, value):
        assert(isinstance(value, int))

        if self.min_value is not None and value < self.min_value:
            raise ValidationError('Integer value is too small')

        if self.max_value is not None and value > self.max_value:
            raise ValidationError('Integer value is too large')


class EmbeddedDocumentField(BaseField):
    """An embedded document field. Only valid values are subclasses of 
    EmbeddedDocument. 
    """

    def __init__(self, document, **kwargs):
        if not issubclass(document, EmbeddedDocument):
            raise ValidationError('Invalid embedded document class provided '
                                  'to an EmbeddedDocumentField')
        self.document = document
        super(EmbeddedDocumentField, self).__init__(**kwargs)
    
    def _to_python(self, value):
        if not isinstance(value, self.document):
            assert(isinstance(value, (dict, pymongo.son.SON)))
            return self.document._from_son(value)
        return value

    def _to_mongo(self, value):
        return self.document._to_mongo(value)

    def _validate(self, value):
        """Make sure that the document instance is an instance of the 
        EmbeddedDocument subclass provided when the document was defined.
        """
        # Using isinstance also works for subclasses of self.document
        if not isinstance(value, self.document):
            raise ValidationError('Invalid embedded document instance '
                                  'provided to an EmbeddedDocumentField')


class ListField(BaseField):
    """A list field that wraps a standard field, allowing multiple instances
    of the field to be used as a list in the database.
    """

    def __init__(self, field, **kwargs):
        if not isinstance(field, BaseField):
            raise ValidationError('Argument to ListField constructor must be '
                                  'a valid field')
        self.field = field
        super(ListField, self).__init__(**kwargs)

    def _to_python(self, value):
        assert(isinstance(value, (list, tuple)))
        return [self.field._to_python(item) for item in value]

    def _to_mongo(self, value):
        return [self.field._to_mongo(item) for item in value]

    def _validate(self, value):
        """Make sure that a list of valid fields is being used.
        """
        if not isinstance(value, (list, tuple)):
            raise ValidationError('Only lists and tuples may be used in a '
                                  'list field')

        try:
            [self.field._validate(item) for item in value]
        except:
            raise ValidationError('All items in a list field must be of the '
                                  'specified type')


class ReferenceField(BaseField):
    """A reference to a document that will be automatically dereferenced on
    access (lazily).
    """

    def __init__(self, document_type, **kwargs):
        if not issubclass(document_type, Document):
            raise ValidationError('Argument to ReferenceField constructor '
                                  'must be a top level document class')
        self.document_type = document_type
        self.document_obj = None
        super(ReferenceField, self).__init__(**kwargs)

    def __get__(self, instance, owner):
        """Descriptor to allow lazy dereferencing.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        # Get value from document instance if available
        value = instance._data.get(self.name)
        # Dereference DBRefs
        if isinstance(value, (pymongo.dbref.DBRef)):
            value = _get_db().dereference(value)
            instance._data[self.name] = self.document_type._from_son(value)
        
        return super(ReferenceField, self).__get__(instance, owner)

    def _to_python(self, document):
        assert(isinstance(document, (self.document_type, pymongo.dbref.DBRef)))
        return document

    def _to_mongo(self, document):
        if isinstance(document, (str, unicode, pymongo.objectid.ObjectId)):
            _id = document
        else:
            try:
                _id = document._id
            except:
                raise ValidationError('You can only reference documents once '
                                      'they have been saved to the database')

        if not isinstance(_id, pymongo.objectid.ObjectId):
            _id = pymongo.objectid.ObjectId(_id)

        collection = self.document_type._meta['collection']
        return pymongo.dbref.DBRef(collection, _id)
