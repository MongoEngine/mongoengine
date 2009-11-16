
class ValidationError(Exception):
    pass


class BaseField(object):
    """A base class for fields in a MongoDB document. Instances of this class
    may be added to subclasses of `Document` to define a document's schema.
    """
    
    def __init__(self, name=None, default=None):
        self.name = name
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
        if value is not None:
            value = self._to_python(value)
        elif self.default is not None:
            value = self.default
            if callable(value):
                value = value()
        return value

    def __set__(self, instance, value):
        """Descriptor for assigning a value to a field in a document. Do any 
        necessary conversion between Python and MongoDB types.
        """
        if value is not None:
            try:
                value = self._to_python(value)
                self._validate(value)
                value = self._to_mongo(value)
            except ValueError:
                raise ValidationError('Invalid value for field of type "' +
                                      self.__class__.__name__ + '"')
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
        return value


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
            if issubclass(attr_val.__class__, BaseField):
                if not attr_val.name:
                    attr_val.name = attr_name
                doc_fields[attr_name] = attr_val
        attrs['_fields'] = doc_fields

        return type.__new__(cls, name, bases, attrs)


class BaseDocument(object):

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
