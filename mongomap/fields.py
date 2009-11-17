from base import BaseField, ValidationError
from document import EmbeddedDocument
 
import re


__all__ = ['StringField', 'IntField', 'ValidationError']


class StringField(BaseField):
    """A unicode string field.
    """
    
    def __init__(self, regex=None, max_length=None, object_id=False, **kwargs):
        self.regex = re.compile(regex) if regex else None
        self.object_id = object_id
        self.max_length = max_length
        super(StringField, self).__init__(**kwargs)

    def _validate(self, value):
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
        return int(value)

    def _validate(self, value):
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
            raise ValidationError('Invalid embedded document provided to an '
                                  'EmbeddedDocumentField')
        self.document = document
        super(EmbeddedDocumentField, self).__init__(**kwargs)
