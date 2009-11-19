from collection import CollectionManager

import pymongo


class ValidationError(Exception):
    pass


class BaseField(object):
    """A base class for fields in a MongoDB document. Instances of this class
    may be added to subclasses of `Document` to define a document's schema.
    """
    
    def __init__(self, name=None, required=False, default=None):
        self.name = name
        self.required = required
        self.default = default

    def __get__(self, instance, owner):
        """Descriptor for retrieving a value from a field in a document. Do 
        any necessary conversion between Python and MongoDB types.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        # Get value from document instance if available, if not use default
        value = instance._data.get(self.name)
        if value is None:
            if self.default is not None:
                value = self.default
                if callable(value):
                    value = value()
            else:
                raise AttributeError(self.name)
        return value

    def __set__(self, instance, value):
        """Descriptor for assigning a value to a field in a document. Do any 
        necessary conversion between Python and MongoDB types.
        """
        if value is not None:
            try:
                value = self._to_python(value)
                self._validate(value)
            except (ValueError, AttributeError, AssertionError):
                raise ValidationError('Invalid value for field of type "' +
                                      self.__class__.__name__ + '"')
        elif self.required:
            raise ValidationError('Field "%s" is required' % self.name)
        instance._data[self.name] = value

    def _to_python(self, value):
        """Convert a MongoDB-compatible type to a Python type.
        """
        return unicode(value)

    def _to_mongo(self, value):
        """Convert a Python type to a MongoDB-compatible type.
        """
        return self._to_python(value)

    def _validate(self, value):
        """Perform validation on a value.
        """
        pass


class ObjectIdField(BaseField):
    """An field wrapper around MongoDB's ObjectIds.
    """
    
    def _to_python(self, value):
        return str(value)

    def _to_mongo(self, value):
        if not isinstance(value, pymongo.objectid.ObjectId):
            return pymongo.objectid.ObjectId(value)
        return value

    def _validate(self, value):
        try:
            pymongo.objectid.ObjectId(str(value))
        except:
            raise ValidationError('Invalid Object ID')


class DocumentMetaclass(type):
    """Metaclass for all documents.
    """

    def __new__(cls, name, bases, attrs):
        metaclass = attrs.get('__metaclass__')
        super_new = super(DocumentMetaclass, cls).__new__
        if metaclass and issubclass(metaclass, DocumentMetaclass):
            return super_new(cls, name, bases, attrs)

        doc_fields = {}
        # Include all fields present in superclasses
        for base in bases:
            if hasattr(base, '_fields'):
                doc_fields.update(base._fields)

        # Add the document's fields to the _fields attribute
        for attr_name, attr_value in attrs.items():
            if issubclass(attr_value.__class__, BaseField):
                if not attr_value.name:
                    attr_value.name = attr_name
                doc_fields[attr_name] = attr_value
        attrs['_fields'] = doc_fields

        return super_new(cls, name, bases, attrs)


class TopLevelDocumentMetaclass(DocumentMetaclass):
    """Metaclass for top-level documents (i.e. documents that have their own
    collection in the database.
    """

    def __new__(cls, name, bases, attrs):
        super_new = super(TopLevelDocumentMetaclass, cls).__new__
        # Classes defined in this package are abstract and should not have 
        # their own metadata with DB collection, etc.
        # __metaclass__ is only set on the class with the __metaclass__ 
        # attribute (i.e. it is not set on subclasses). This differentiates
        # 'real' documents from the 'Document' class
        if attrs.get('__metaclass__') == TopLevelDocumentMetaclass:
            return super_new(cls, name, bases, attrs)

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

        attrs['_id'] = ObjectIdField()

        # Set up collection manager, needs the class to have fields so use
        # DocumentMetaclass before instantiating CollectionManager object
        new_class = super_new(cls, name, bases, attrs)
        new_class.objects = CollectionManager(new_class)

        return new_class


class BaseDocument(object):

    def __init__(self, **values):
        self._data = {}
        # Assign initial values to instance
        for attr_name, attr_value in self._fields.items():
            if attr_name in values:
                setattr(self, attr_name, values.pop(attr_name))
            else:
                if attr_value.required:
                    raise ValidationError('Field "%s" is required' % attr_name)
                # Use default value
                setattr(self, attr_name, getattr(self, attr_name, None))

    def __iter__(self):
        # Use _data rather than _fields as iterator only looks at names so
        # values don't need to be converted to Python types
        return iter(self._data)

    def __getitem__(self, name):
        """Dictionary-style field access, return a field's value if present.
        """
        try:
            return getattr(self, name)
        except AttributeError:
            raise KeyError(name)

    def __setitem__(self, name, value):
        """Dictionary-style field access, set a field's value.
        """
        # Ensure that the field exists before settings its value
        if name not in self._fields:
            raise KeyError(name)
        return setattr(self, name, value)

    def __contains__(self, name):
        try:
            getattr(self, name)
            return True
        except AttributeError:
            return False

    def __len__(self):
        return len(self._data)

    def _to_mongo(self):
        """Return data dictionary ready for use with MongoDB.
        """
        data = {}
        for field_name, field in self._fields.items():
            value = getattr(self, field_name, None)
            if value is not None:
                data[field_name] = field._to_mongo(value)
        return data
    
    @classmethod
    def _from_son(cls, son):
        """Create an instance of a Document (subclass) from a PyMongo SOM.
        """
        data = dict((str(key), value) for key, value in son.items())
        return cls(**data)

