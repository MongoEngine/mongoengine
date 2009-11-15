import re


__all__ = ['StringField', 'IntField', 'ValidationError']


class ValidationError(Exception):
    pass


class Field(object):
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


class NestedDocumentField(Field):
    """A nested document field. Only valid values are subclasses of 
    NestedDocument. 
    """
    pass


class StringField(Field):
    """A unicode string field.
    """
    
    def __init__(self, regex=None, max_length=None, **kwargs):
        self.regex = re.compile(regex) if regex else None
        self.max_length = max_length
        Field.__init__(self, **kwargs)

    def _validate(self, value):
        if self.max_length is not None and len(value) > self.max_length:
            raise ValidationError('String value is too long')

        if self.regex is not None and self.regex.match(value) is None:
            message = 'String value did not match validation regex'
            raise ValidationError(message)


class IntField(Field):
    """An integer field.
    """

    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value, self.max_value = min_value, max_value
        Field.__init__(self, **kwargs)
    
    def _to_python(self, value):
        return int(value)

    def _validate(self, value):
        if self.min_value is not None and value < self.min_value:
            raise ValidationError('Integer value is too small')

        if self.max_value is not None and value > self.max_value:
            raise ValidationError('Integer value is too large')
