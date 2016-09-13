import datetime
import itertools
import re
import time
import uuid
from operator import itemgetter
import collections

try:
    import dateutil
except ImportError:
    dateutil = None
else:
    import dateutil.parser

import pymongo
import gridfs
from bson import Binary, DBRef, SON, ObjectId

from mongoengine.errors import ValidationError
from mongoengine.python_support import (PY3, bin_type, txt_type,
                                        str_types, StringIO)
from mongoengine.base import (BaseField, ComplexBaseField, ObjectIdField, GeoJsonBaseField,
                  get_document, BaseDocument)
from mongoengine.base.datastructures import BaseList, BaseDict
from mongoengine.base.proxy import DocumentProxy
from mongoengine.queryset import DoesNotExist
from .queryset import DO_NOTHING, QuerySet
from .document import Document, EmbeddedDocument
from .connection import get_db, DEFAULT_CONNECTION_NAME

try:
    from PIL import Image, ImageOps
except ImportError:
    Image = None
    ImageOps = None

__all__ = ['StringField',  'URLField',  'EmailField',  'IntField',
           'FloatField',  'BooleanField',  'DateTimeField',
           'ComplexDateTimeField',  'EmbeddedDocumentField', 'ObjectIdField',
           'GenericEmbeddedDocumentField',  'DynamicField',  'ListField',
           'SortedListField',  'DictField',  'MapField',  'ReferenceField',
           'SafeReferenceField', 'SafeReferenceListField',
           'GenericReferenceField',  'BinaryField',  'GridFSError',
           'GridFSProxy',  'FileField',  'ImageGridFsProxy',
           'ImproperlyConfigured',  'ImageField',  'GeoPointField', 'PointField',
           'LineStringField', 'PolygonField', 'SequenceField',  'UUIDField']


RECURSIVE_REFERENCE_CONSTANT = 'self'


class StringField(BaseField):
    """A unicode string field.
    """

    def __init__(self, regex=None, max_length=None, min_length=None, **kwargs):
        self.regex = re.compile(regex) if regex else None
        self.max_length = max_length
        self.min_length = min_length
        super(StringField, self).__init__(**kwargs)

    def from_python(self, value):
        if isinstance(value, str):
            return value
        else:
            return super(StringField, self).from_python(value)

    def validate(self, value):
        if not isinstance(value, str):
            self.error('StringField only accepts string values')

        if self.max_length is not None and len(value) > self.max_length:
            self.error('String value is too long')

        if self.min_length is not None and len(value) < self.min_length:
            self.error('String value is too short')

        if self.regex is not None and self.regex.match(value) is None:
            self.error('String value did not match validation regex')

    def lookup_member(self, member_name):
        return None

    def prepare_query_value(self, op, value):
        if not isinstance(op, str):
            return value

        # for exact search, return the value immediately
        if op == 'exact':
            return value

        if op.lstrip('i') in ('startswith', 'endswith', 'contains', 'exact'):
            flags = 0
            if op.startswith('i'):
                flags = re.IGNORECASE
                op = op.lstrip('i')

            regex = r'%s'
            if op == 'startswith':
                regex = r'^%s'
            elif op == 'endswith':
                regex = r'%s$'
            elif op == 'exact':
                regex = r'^%s$'

            # escape unsafe characters which could lead to a re.error
            value = re.escape(value)
            value = re.compile(regex % value, flags)
        return value


class URLField(StringField):
    """A field that validates input as an URL.

    .. versionadded:: 0.3
    """

    _URL_REGEX = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    def __init__(self, url_regex=None, **kwargs):
        self.url_regex = url_regex or self._URL_REGEX
        super(URLField, self).__init__(**kwargs)

    def validate(self, value):
        if not self.url_regex.match(value):
            self.error('Invalid URL: %s' % value)
            return


class EmailField(StringField):
    """A field that validates input as an email address.

    .. versionadded:: 0.4
    """

    EMAIL_REGEX = re.compile(r'^.+@[^.].*\.[a-z]{2,63}$', re.IGNORECASE)

    def validate(self, value):
        if not EmailField.EMAIL_REGEX.match(value):
            self.error('Invalid email address: %s' % value)
        super(EmailField, self).validate(value)


class IntField(BaseField):
    """An integer field.
    """

    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value, self.max_value = min_value, max_value
        super(IntField, self).__init__(**kwargs)

    def from_python(self, value):
        return self.prepare_query_value(None, value)

    def validate(self, value):
        try:
            value = int(value)
        except:
            self.error('%s could not be converted to int' % value)

        if self.min_value is not None and value < self.min_value:
            self.error('Integer value is too small')

        if self.max_value is not None and value > self.max_value:
            self.error('Integer value is too large')

    def prepare_query_value(self, op, value):
        if value is None:
            return value
        else:
            return int(value)


class FloatField(BaseField):
    """A floating point number field.
    """

    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value, self.max_value = min_value, max_value
        super(FloatField, self).__init__(**kwargs)

    def validate(self, value):
        if isinstance(value, int):
            value = float(value)
        if not isinstance(value, float):
            self.error('FloatField only accepts float values')

        if self.min_value is not None and value < self.min_value:
            self.error('Float value is too small')

        if self.max_value is not None and value > self.max_value:
            self.error('Float value is too large')


class BooleanField(BaseField):
    """A boolean field type.

    .. versionadded:: 0.1.2
    """

    def validate(self, value):
        if not isinstance(value, bool):
            self.error('BooleanField only accepts boolean values')


class DateTimeField(BaseField):
    """A datetime field.

    Uses the python-dateutil library if available alternatively use time.strptime
    to parse the dates.  Note: python-dateutil's parser is fully featured and when
    installed you can utilise it to convert varing types of date formats into valid
    python datetime objects.

    Note: Microseconds are rounded to the nearest millisecond.
      Pre UTC microsecond support is effecively broken.
      Use :class:`~mongoengine.fields.ComplexDateTimeField` if you
      need accurate microsecond support.
    """

    def _parse_datetime(self, value):
        # Attempt to parse a datetime:
        if dateutil:
            try:
                return dateutil.parser.parse(value)
            except (TypeError, ValueError, OverflowError):
                return None

        # split usecs, because they are not recognized by strptime.
        if '.' in value:
            try:
                value, usecs = value.split('.')
                usecs = int(usecs)
            except ValueError:
                return None
        else:
            usecs = 0
        kwargs = {'microsecond': usecs}
        try:  # Seconds are optional, so try converting seconds first.
            return datetime.datetime(*time.strptime(value,
                                     '%Y-%m-%d %H:%M:%S')[:6], **kwargs)
        except ValueError:
            try:  # Try without seconds.
                return datetime.datetime(*time.strptime(value,
                                         '%Y-%m-%d %H:%M')[:5], **kwargs)
            except ValueError:  # Try without hour/minutes/seconds.
                try:
                    return datetime.datetime(*time.strptime(value,
                                             '%Y-%m-%d')[:3], **kwargs)
                except ValueError:
                    return None

    def validate(self, value):
        orig_value = value
        if not isinstance(value, (datetime.datetime, datetime.date)):
            value = self._parse_datetime(value)
            if not value:
                self.error('cannot parse date "%s"' % orig_value)

    def from_python(self, value):
        return self.prepare_query_value(None, value) or value

    def prepare_query_value(self, op, value):
        if value is None:
            return value
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)
        if isinstance(value, collections.Callable):
            return value()

        if not isinstance(value, str):
            return None

        return self._parse_datetime(value)


class ComplexDateTimeField(StringField):
    """
    ComplexDateTimeField handles microseconds exactly instead of rounding
    like DateTimeField does.

    Derives from a StringField so you can do `gte` and `lte` filtering by
    using lexicographical comparison when filtering / sorting strings.

    The stored string has the following format:

        YYYY,MM,DD,HH,MM,SS,NNNNNN

    Where NNNNNN is the number of microseconds of the represented `datetime`.
    The `,` as the separator can be easily modified by passing the `separator`
    keyword when initializing the field.

    .. versionadded:: 0.5
    """

    # TODO

    def __init__(self, separator=',', **kwargs):
        self.names = ['year', 'month', 'day', 'hour', 'minute', 'second',
                      'microsecond']
        self.separtor = separator
        super(ComplexDateTimeField, self).__init__(**kwargs)

    def _leading_zero(self, number):
        """
        Converts the given number to a string.

        If it has only one digit, a leading zero so as it has always at least
        two digits.
        """
        if int(number) < 10:
            return "0%s" % number
        else:
            return str(number)

    def _convert_from_datetime(self, val):
        """
        Convert a `datetime` object to a string representation (which will be
        stored in MongoDB). This is the reverse function of
        `_convert_from_string`.

        >>> a = datetime(2011, 6, 8, 20, 26, 24, 192284)
        >>> RealDateTimeField()._convert_from_datetime(a)
        '2011,06,08,20,26,24,192284'
        """
        data = []
        for name in self.names:
            data.append(self._leading_zero(getattr(val, name)))
        return ','.join(data)

    def _convert_from_string(self, data):
        """
        Convert a string representation to a `datetime` object (the object you
        will manipulate). This is the reverse function of
        `_convert_from_datetime`.

        >>> a = '2011,06,08,20,26,24,192284'
        >>> ComplexDateTimeField()._convert_from_string(a)
        datetime.datetime(2011, 6, 8, 20, 26, 24, 192284)
        """
        data = data.split(',')
        data = list(map(int, data))
        values = {}
        for i in range(7):
            values[self.names[i]] = data[i]
        return datetime.datetime(**values)

    def __get__(self, instance, owner):
        data = super(ComplexDateTimeField, self).__get__(instance, owner)
        if data is None:
            return datetime.datetime.now()
        if isinstance(data, datetime.datetime):
            return data
        return self._convert_from_string(data)

    def __set__(self, instance, value):
        value = self._convert_from_datetime(value) if value else value
        return super(ComplexDateTimeField, self).__set__(instance, value)

    def validate(self, value):
        value = self.to_python(value)
        if not isinstance(value, datetime.datetime):
            self.error('Only datetime objects may used in a '
                       'ComplexDateTimeField')

    def to_python(self, value):
        original_value = value
        try:
            return self._convert_from_string(value)
        except:
            return original_value

    def to_mongo(self, value):
        value = self.to_python(value)
        return self._convert_from_datetime(value)

    def prepare_query_value(self, op, value):
        return self._convert_from_datetime(value)


class EmbeddedDocumentField(BaseField):
    """An embedded document field - with a declared document_type.
    Only valid values are subclasses of :class:`~mongoengine.EmbeddedDocument`.
    """

    def __init__(self, document_type, **kwargs):
        if not isinstance(document_type, str):
            if not issubclass(document_type, EmbeddedDocument):
                self.error('Invalid embedded document class provided to an '
                           'EmbeddedDocumentField')
        self.document_type_obj = document_type
        super(EmbeddedDocumentField, self).__init__(**kwargs)

    @property
    def document_type(self):
        if isinstance(self.document_type_obj, str):
            if self.document_type_obj == RECURSIVE_REFERENCE_CONSTANT:
                self.document_type_obj = self.owner_document
            else:
                self.document_type_obj = get_document(self.document_type_obj)
        return self.document_type_obj

    def to_python(self, val):
        return self.document_type._from_son(val)

    def to_mongo(self, val):
        return val and val.to_mongo()

    def validate(self, value, clean=True):
        """Make sure that the document instance is an instance of the
        EmbeddedDocument subclass provided when the document was defined.
        """
        # Using isinstance also works for subclasses of self.document
        if not isinstance(value, self.document_type):
            self.error('Invalid embedded document instance provided to an '
                       'EmbeddedDocumentField')
        self.document_type.validate(value, clean)

    def lookup_member(self, member_name):
        return self.document_type._fields.get(member_name)

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)


class GenericEmbeddedDocumentField(BaseField):
    """A generic embedded document field - allows any
    :class:`~mongoengine.EmbeddedDocument` to be stored.

    Only valid values are subclasses of :class:`~mongoengine.EmbeddedDocument`.

    .. note ::
        You can use the choices param to limit the acceptable
        EmbeddedDocument types
    """

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)

    def to_python(self, value):
        doc_cls = get_document(value['_cls'])
        value = doc_cls._from_son(value)

        return value

    def validate(self, value, clean=True):
        if not isinstance(value, EmbeddedDocument):
            self.error('Invalid embedded document instance provided to an '
                       'GenericEmbeddedDocumentField')

        value.validate(clean=clean)

    def to_mongo(self, document):
        if document is None:
            return None

        data = document.to_mongo()
        if not '_cls' in data:
            data['_cls'] = document._class_name
        return data


class DynamicField(BaseField):
    """A truly dynamic field type capable of handling different and varying
    types of data.

    Used by :class:`~mongoengine.DynamicDocument` to handle dynamic data"""

    def to_mongo(self, value):
        """Convert a Python type to a MongoDBcompatible type.
        """

        if isinstance(value, str):
            return value

        if hasattr(value, 'to_mongo'):
            cls = value.__class__
            val = value.to_mongo()
            # If we its a document thats not inherited add _cls
            if (isinstance(value, (Document, EmbeddedDocument))):
                val['_cls'] = cls.__name__
            return val

        if not isinstance(value, (dict, list, tuple)):
            return value

        is_list = False
        if not hasattr(value, 'items'):
            is_list = True
            value = dict([(k, v) for k, v in enumerate(value)])

        data = {}
        for k, v in value.items():
            data[k] = self.to_mongo(v)

        value = data
        if is_list:  # Convert back to a list
            value = [v for k, v in sorted(iter(data.items()), key=itemgetter(0))]
        return value

    def lookup_member(self, member_name):
        return member_name

    def prepare_query_value(self, op, value):
        if isinstance(value, str):
            from mongoengine.fields import StringField
            return StringField().prepare_query_value(op, value)
        return self.to_mongo(value)

    def validate(self, value, clean=True):
        if hasattr(value, "validate"):
            value.validate(clean=clean)


class ListField(ComplexBaseField):
    """A list field that wraps a standard field, allowing multiple instances
    of the field to be used as a list in the database.

    You can add validation to each list item by specifying the `field`
    argument. For example, ListField(IntField()) will ensure that all the
    items are integers.

    You can also limit the maximum number of items in the list with
    `max_length`. However, keep in mind that the validation can be bypassed by
    using push__list_field_name.

    If using with ReferenceFields see: :ref:`one-to-many-with-listfields`

    .. note::
        Required means it cannot be empty - as the default for ListFields is []
    """

    def __init__(self, field=None, max_length=None, **kwargs):
        self.field = field
        self.max_length = max_length
        kwargs.setdefault('default', lambda: [])
        super(ListField, self).__init__(**kwargs)

    def value_for_instance(self, value, instance, name=None):
        name = name or self.name
        if value and self.field:
            value_for_instance = getattr(self.field, 'value_for_instance', None)
            if value_for_instance:
                value = [value_for_instance(v, instance, name) for v in value]
        return BaseList(value or [], instance, name)

    def from_python(self, val):
        from_python = getattr(self.field, 'from_python', None)
        return [from_python(v) for v in val] if from_python else val

    def to_python(self, val):
        to_python = getattr(self.field, 'to_python', None)
        return [to_python(v) for v in val] if to_python and val else val or None

    def to_mongo(self, val):
        to_mongo = getattr(self.field, 'to_mongo', None)
        return [to_mongo(v) for v in val] if to_mongo and val else val or None

    def validate(self, value):
        """Make sure that a list of valid fields is being used.
        """
        if (not isinstance(value, (list, tuple, QuerySet)) or
           isinstance(value, str)):
            self.error('Only lists and tuples may be used in a list field')

        # Validate that max_length is not exceeded. Note that it's still
        # possible to bypass this enforcement by using $push. However, if the
        # document is reloaded after $push and then re-saved, the validation
        # error will be raised.
        if self.max_length is not None and len(value) > self.max_length:
            self.error('ListField max length is exceeded')

        super(ListField, self).validate(value)

    def prepare_query_value(self, op, value):
        # validate that $set doesn't contain more items than max_length
        if op == 'set' and self.max_length is not None and len(value) > self.max_length:
            self.error('ListField max length is exceeded')

        if self.field:
            if op in ('set', 'unset') and (
                not isinstance(value, str) and
                not isinstance(value, BaseDocument) and
                hasattr(value, '__iter__')
            ):
                return [self.field.prepare_query_value(op, v) for v in value]
            return self.field.prepare_query_value(op, value)
        else:
            if op in ('set', 'unset'):
                return value
        return super(ListField, self).prepare_query_value(op, value)


class SortedListField(ListField):
    """A ListField that sorts the contents of its list before writing to
    the database in order to ensure that a sorted list is always
    retrieved.

    .. warning::
        There is a potential race condition when handling lists.  If you set /
        save the whole list then other processes trying to save the whole list
        as well could overwrite changes.  The safest way to append to a list is
        to perform a push operation.

    .. versionadded:: 0.4
    .. versionchanged:: 0.6 - added reverse keyword
    """

    _ordering = None
    _order_reverse = False

    def __init__(self, field, **kwargs):
        if 'ordering' in list(kwargs.keys()):
            self._ordering = kwargs.pop('ordering')
        if 'reverse' in list(kwargs.keys()):
            self._order_reverse = kwargs.pop('reverse')
        super(SortedListField, self).__init__(field, **kwargs)

    def to_mongo(self, value):
        value = super(SortedListField, self).to_mongo(value)
        if value:
            if self._ordering is not None:
                return sorted(value, key=itemgetter(self._ordering),
                              reverse=self._order_reverse)
            return sorted(value, reverse=self._order_reverse)
        else:
            return value

def key_not_string(d):
    """ Helper function to recursively determine if any key in a dictionary is
    not a string.
    """
    for k, v in list(d.items()):
        if not isinstance(k, str) or (isinstance(v, dict) and key_not_string(v)):
            return True

def key_has_dot_or_dollar(d):
    """ Helper function to recursively determine if any key in a dictionary
    contains a dot or a dollar sign.
    """
    for k, v in list(d.items()):
        if ('.' in k or '$' in k) or (isinstance(v, dict) and key_has_dot_or_dollar(v)):
            return True

class DictField(ComplexBaseField):
    """A dictionary field that wraps a standard Python dictionary. This is
    similar to an embedded document, but the structure is not defined.

    .. note::
        Required means it cannot be empty - as the default for ListFields is []

    .. versionadded:: 0.3
    .. versionchanged:: 0.5 - Can now handle complex / varying types of data
    """

    def __init__(self, basecls=None, field=None, *args, **kwargs):
        self.field = field
        self.basecls = basecls or BaseField
        if not issubclass(self.basecls, BaseField):
            self.error('DictField only accepts dict values')
        kwargs.setdefault('default', lambda: {})
        super(DictField, self).__init__(*args, **kwargs)

    def from_python(self, val):
        from_python = getattr(self.field, 'from_python', None)
        return {k: from_python(v) for k, v in val.items()} if from_python else val

    def to_python(self, val):
        to_python = getattr(self.field, 'to_python', None)
        return {k: to_python(v) for k, v in val.items()} if to_python and val else val or None

    def value_for_instance(self, value, instance, name=None):
        name = name or self.name
        if value and self.field:
            value_for_instance = getattr(self.field, 'value_for_instance', None)
            if value_for_instance:
                value = {k: value_for_instance(v, instance, name) for k, v in value.items()}
        return BaseDict(value or {}, instance, name)

    def to_mongo(self, val):
        to_mongo = getattr(self.field, 'to_mongo', None)
        return {k: to_mongo(v) for k, v in val.items()} if to_mongo and val else val or None

    def validate(self, value):
        """Make sure that a list of valid fields is being used.
        """
        if not isinstance(value, dict):
            self.error('Only dictionaries may be used in a DictField')

        if key_not_string(value):
            msg = ("Invalid dictionary key - documents must "
                   "have only string keys")
            self.error(msg)
        if key_has_dot_or_dollar(value):
            self.error('Invalid dictionary key name - keys may not contain "."'
                       ' or "$" characters')
        super(DictField, self).validate(value)

    def lookup_member(self, member_name):
        return DictField(basecls=self.basecls, db_field=member_name)

    def prepare_query_value(self, op, value):
        match_operators = ['contains', 'icontains', 'startswith',
                           'istartswith', 'endswith', 'iendswith',
                           'exact', 'iexact']

        if op in match_operators and isinstance(value, str):
            return StringField().prepare_query_value(op, value)
        return super(DictField, self).prepare_query_value(op, value)


class MapField(DictField):
    """A field that maps a name to a specified field type. Similar to
    a DictField, except the 'value' of each item must match the specified
    field type.

    .. versionadded:: 0.5
    """

    def __init__(self, field=None, *args, **kwargs):
        if not isinstance(field, BaseField):
            self.error('Argument to MapField constructor must be a valid '
                       'field')
        super(MapField, self).__init__(field=field, *args, **kwargs)


class ReferenceField(BaseField):
    """A reference to a document that will be automatically dereferenced on
    access (lazily).

    Use the `reverse_delete_rule` to handle what should happen if the document
    the field is referencing is deleted.  EmbeddedDocuments, DictFields and
    MapFields do not support reverse_delete_rules and an `InvalidDocumentError`
    will be raised if trying to set on one of these Document / Field types.

    The options are:

      * DO_NOTHING  - don't do anything (default).
      * NULLIFY     - Updates the reference to null.
      * CASCADE     - Deletes the documents associated with the reference.
      * DENY        - Prevent the deletion of the reference object.
      * PULL        - Pull the reference from a :class:`~mongoengine.fields.ListField`
                      of references

    Alternative syntax for registering delete rules (useful when implementing
    bi-directional delete rules)

    .. code-block:: python

        class Bar(Document):
            content = StringField()
            foo = ReferenceField('Foo')

        Bar.register_delete_rule(Foo, 'bar', NULLIFY)

    .. note ::
        `reverse_delete_rules` do not trigger pre / post delete signals to be
        triggered.

    .. versionchanged:: 0.5 added `reverse_delete_rule`
    """

    def __init__(self, document_type, dbref=False,
                 reverse_delete_rule=DO_NOTHING, **kwargs):
        """Initialises the Reference Field.

        :param dbref:  Store the reference as :class:`~pymongo.dbref.DBRef`
          or as the :class:`~pymongo.objectid.ObjectId`.id .
        :param reverse_delete_rule: Determines what to do when the referring
          object is deleted
        """
        if not isinstance(document_type, str):
            if not issubclass(document_type, (Document, str)):
                self.error('Argument to ReferenceField constructor must be a '
                           'document class or a string')

        self.dbref = dbref
        self.document_type_obj = document_type
        self.reverse_delete_rule = reverse_delete_rule
        super(ReferenceField, self).__init__(**kwargs)

    @property
    def document_type(self):
        if isinstance(self.document_type_obj, str):
            if self.document_type_obj == RECURSIVE_REFERENCE_CONSTANT:
                self.document_type_obj = self.owner_document
            else:
                self.document_type_obj = get_document(self.document_type_obj)
        return self.document_type_obj

    def to_mongo(self, value):
        if isinstance(value, DBRef):
            if self.dbref:
                return value
            else:
                return value.id
        elif isinstance(value, (Document, DocumentProxy)):
            document_type = self.document_type
            # We need the id from the saved object to create the DBRef
            pk = value.pk
            if pk is None:
                self.error('You can only reference documents once they have'
                           ' been saved to the database')
            id_field_name = document_type._meta['id_field']
            id_field = document_type._fields[id_field_name]
            pk = id_field.to_mongo(pk)
            if self.dbref:
                collection = document_type._get_collection_name()
                return DBRef(collection, pk)
            else:
                return pk
        elif value != None: # string ID
            document_type = self.document_type
            collection = document_type._get_collection_name()
            return DBRef(collection, value)

    def to_python(self, value):
        if value != None:
            document_type = self.document_type
            if self.dbref:
                pk = value.id
            else:
                if isinstance(value, DBRef):
                    pk = value.id
                else:
                    pk = value
            if document_type._meta['allow_inheritance']:
                # We don't know of which type the object will be.
                obj = DocumentProxy(document_type, pk)
            else:
                obj = document_type(pk=pk)
                obj._lazy = True
            return obj

    def from_python(self, value):
        if isinstance(value, (BaseDocument, DocumentProxy)):
            return value
        elif value == None:
            return super(ReferenceField, self).from_python(value)
        else:
            # Support for werkzeug.local.LocalProxy
            if hasattr(value, '_get_current_object'):
                return value._get_current_object()
            else:
                # DBRef or ID
                document_type = self.document_type
                if isinstance(value, DBRef):
                    pk = value.id
                else:
                    pk = value
                if document_type._meta['allow_inheritance']:
                    # We don't know of which type the object will be.
                    obj = DocumentProxy(document_type, pk)
                else:
                    obj = document_type(pk=pk)
                    obj._lazy = True
                return obj

    def prepare_query_value(self, op, value):
        return self.to_mongo(self.from_python(value))

    def validate(self, value):
        if not isinstance(value, (self.document_type, DBRef, DocumentProxy)):
            self.error("A ReferenceField only accepts DBRef or documents")

        if isinstance(value, Document) and value.pk is None:
            self.error('You can only reference documents once they have been '
                       'saved to the database')

    def lookup_member(self, member_name):
        return self.document_type._fields.get(member_name)


class SafeReferenceField(ReferenceField):
    """
    Like a ReferenceField, but doesn't return non-existing references when
    dereferencing, i.e. no DBRefs are returned. This means that the next time
    an object is saved, the non-existing references are removed and application
    code can rely on having only valid dereferenced objects.

    When the field is referenced, the referenced object is loaded from the
    database.
    """

    def to_python(self, value):
        obj = super(SafeReferenceField, self).to_python(value)
        if obj:
            # Must dereference so we don't get an invalid ObjectId back.
            try:
                obj.reload()

                # No need for a proxy in this case.
                if isinstance(obj, DocumentProxy):
                    obj = obj._get_current_object()
            except DoesNotExist:
                return None
        return obj


class SafeReferenceListField(ListField):
    """
    Like a ListField, but doesn't return non-existing references when
    dereferencing, i.e. no DBRefs are returned. This means that the next time
    an object is saved, the non-existing references are removed and application
    code can rely on having only valid dereferenced objects.

    When the field is referenced, all referenced objects are loaded from the
    database.

    Must use ReferenceField as its field class.
    """

    def __init__(self, field, **kwargs):
        if not isinstance(field, ReferenceField):
            raise ValueError('Field argument must be a ReferenceField instance.')
        return super(SafeReferenceListField, self).__init__(field, **kwargs)

    def to_python(self, value):
        result = super(SafeReferenceListField, self).to_python(value)
        if result:
            objs = self.field.document_type.objects.in_bulk([obj.id for obj in result])
            return [_f for _f in [objs.get(obj.id) for obj in result] if _f]

class GenericReferenceField(BaseField):
    """A reference to *any* :class:`~mongoengine.document.Document` subclass
    that will be automatically dereferenced on access (lazily).

    .. note ::
        * Any documents used as a generic reference must be registered in the
          document registry.  Importing the model will automatically register
          it.

        * You can use the choices param to limit the acceptable Document types

    .. versionadded:: 0.3
    """

    def validate(self, value):
        if not isinstance(value, (Document, DBRef, dict, SON)):
            self.error('GenericReferences can only contain documents')

        if isinstance(value, (dict, SON)):
            if '_ref' not in value or '_cls' not in value:
                self.error('GenericReferences can only contain documents')

        # We need the id from the saved object to create the DBRef
        elif isinstance(value, Document) and value.id is None:
            self.error('You can only reference documents once they have been'
                       ' saved to the database')

    def dereference(self, value):
        doc_cls = get_document(value['_cls'])
        reference = value['_ref']
        doc = doc_cls._get_db().dereference(reference)
        if doc is not None:
            doc = doc_cls._from_son(doc)
        return doc

    def to_python(self, value):
        if value != None:
            doc_cls = get_document(value['_cls'])
            reference = value['_ref']
            obj = doc_cls(pk=reference.id)
            obj._lazy = True
            return obj

    def to_mongo(self, document):
        if document is None:
            return None

        if isinstance(document, (dict, SON)):
            return document

        id_field_name = document.__class__._meta['id_field']
        id_field = document.__class__._fields[id_field_name]

        if isinstance(document, Document):
            # We need the id from the saved object to create the DBRef
            id_ = document.id
            if id_ is None:
                self.error('You can only reference documents once they have'
                           ' been saved to the database')
        else:
            id_ = document

        id_ = id_field.to_mongo(id_)
        collection = document._get_collection_name()
        ref = DBRef(collection, id_)
        return {'_cls': document._class_name, '_ref': ref}

    def prepare_query_value(self, op, value):
        if value is None:
            return None

        return self.to_mongo(value)


class BinaryField(BaseField):
    """A binary data field.
    """

    def __init__(self, max_bytes=None, **kwargs):
        self.max_bytes = max_bytes
        super(BinaryField, self).__init__(**kwargs)

    def __set__(self, instance, value):
        """Handle bytearrays in python 3.1"""
        if PY3 and isinstance(value, bytearray):
            value = bin_type(value)
        return super(BinaryField, self).__set__(instance, value)

    def to_mongo(self, value):
        return Binary(value)

    def validate(self, value):
        if not isinstance(value, (bin_type, txt_type, Binary)):
            self.error("BinaryField only accepts instances of "
                       "(%s, %s, Binary)" % (
                       bin_type.__name__, txt_type.__name__))

        if self.max_bytes is not None and len(value) > self.max_bytes:
            self.error('Binary value is too long')


class GridFSError(Exception):
    pass


class GridFSProxy(object):
    """Proxy object to handle writing and reading of files to and from GridFS

    .. versionadded:: 0.4
    .. versionchanged:: 0.5 - added optional size param to read
    .. versionchanged:: 0.6 - added collection name param
    """

    _fs = None

    def __init__(self, grid_id=None, key=None,
                 instance=None,
                 db_alias=DEFAULT_CONNECTION_NAME,
                 collection_name='fs'):
        self.grid_id = grid_id                  # Store GridFS id for file
        self.key = key
        self.instance = instance
        self.db_alias = db_alias
        self.collection_name = collection_name
        self.newfile = None                     # Used for partial writes
        self.gridout = None

    def __getattr__(self, name):
        attrs = ('_fs', 'grid_id', 'key', 'instance', 'db_alias',
                 'collection_name', 'newfile', 'gridout')
        if name in attrs:
            return self.__getattribute__(name)
        obj = self.get()
        if hasattr(obj, name):
            return getattr(obj, name)
        raise AttributeError

    def __get__(self, instance, value):
        return self

    def __bool__(self):
        return bool(self.grid_id)

    def __getstate__(self):
        self_dict = self.__dict__
        self_dict['_fs'] = None
        return self_dict

    def __copy__(self):
        copied = GridFSProxy()
        copied.__dict__.update(self.__getstate__())
        return copied

    def __deepcopy__(self, memo):
        return self.__copy__()

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.grid_id)

    def __eq__(self, other):
        if isinstance(other, GridFSProxy):
            return ((self.grid_id == other.grid_id) and
                    (self.collection_name == other.collection_name) and
                    (self.db_alias == other.db_alias))
        else:
            return False

    @property
    def fs(self):
        if not self._fs:
            self._fs = gridfs.GridFS(get_db(self.db_alias), self.collection_name)
        return self._fs

    def get(self, id=None):
        if id:
            self.grid_id = id
        if self.grid_id is None:
            return None
        try:
            if self.gridout is None:
                self.gridout = self.fs.get(self.grid_id)
            return self.gridout
        except:
            # File has been deleted
            return None

    def new_file(self, **kwargs):
        self.newfile = self.fs.new_file(**kwargs)
        self.grid_id = self.newfile._id

    def put(self, file_obj, **kwargs):
        if self.grid_id:
            raise GridFSError('This document already has a file. Either delete '
                              'it or call replace to overwrite it')
        self.grid_id = self.fs.put(file_obj, **kwargs)
        self._mark_as_changed()

    def write(self, string):
        if self.grid_id:
            if not self.newfile:
                raise GridFSError('This document already has a file. Either '
                                  'delete it or call replace to overwrite it')
        else:
            self.new_file()
        self.newfile.write(string)

    def writelines(self, lines):
        if not self.newfile:
            self.new_file()
            self.grid_id = self.newfile._id
        self.newfile.writelines(lines)

    def read(self, size=-1):
        gridout = self.get()
        if gridout is None:
            return None
        else:
            try:
                return gridout.read(size)
            except:
                return ""

    def delete(self):
        # Delete file from GridFS, FileField still remains
        self.fs.delete(self.grid_id)
        self.grid_id = None
        self.gridout = None
        self._mark_as_changed()

    def replace(self, file_obj, **kwargs):
        self.delete()
        self.put(file_obj, **kwargs)

    def close(self):
        if self.newfile:
            self.newfile.close()

    def _mark_as_changed(self):
        """Inform the instance that `self.key` has been changed"""
        if self.instance:
            self.instance._mark_as_changed(self.key)


class FileField(BaseField):
    """A GridFS storage field.

    .. versionadded:: 0.4
    .. versionchanged:: 0.5 added optional size param for read
    .. versionchanged:: 0.6 added db_alias for multidb support
    """
    proxy_class = GridFSProxy

    def __init__(self,
                 db_alias=DEFAULT_CONNECTION_NAME,
                 collection_name="fs", **kwargs):
        super(FileField, self).__init__(**kwargs)
        self.collection_name = collection_name
        self.db_alias = db_alias

    def __get__(self, instance, owner):
        if instance is None:
            return self

        # Check if a file already exists for this model
        grid_file = instance._data.get(self.name)
        if not isinstance(grid_file, self.proxy_class):
            grid_file = self.proxy_class(key=self.name, instance=instance,
                                         db_alias=self.db_alias,
                                         collection_name=self.collection_name)
            instance._data[self.name] = grid_file

        if not grid_file.key:
            grid_file.key = self.name
            grid_file.instance = instance
        return grid_file

    def __set__(self, instance, value):
        key = self.name
        if ((hasattr(value, 'read') and not
             isinstance(value, GridFSProxy)) or isinstance(value, str_types)):
            # using "FileField() = file/string" notation
            grid_file = instance._data.get(self.name)
            # If a file already exists, delete it
            if grid_file:
                try:
                    grid_file.delete()
                except:
                    pass

            # Create a new proxy object as we don't already have one
            instance._data[key] = self.proxy_class(key=key, instance=instance,
                                                   db_alias=self.db_alias,
                                                   collection_name=self.collection_name)
            instance._data[key].put(value)
        else:
            instance._data[key] = value

        instance._mark_as_changed(key)

    def to_mongo(self, value):
        # Store the GridFS file id in MongoDB
        if isinstance(value, self.proxy_class) and value.grid_id is not None:
            return value.grid_id
        return None

    def to_python(self, value):
        if value is not None:
            return self.proxy_class(value,
                                    collection_name=self.collection_name,
                                    db_alias=self.db_alias)

    def validate(self, value):
        if value.grid_id is not None:
            if not isinstance(value, self.proxy_class):
                self.error('FileField only accepts GridFSProxy values')
            if not isinstance(value.grid_id, ObjectId):
                self.error('Invalid GridFSProxy value')


class ImageGridFsProxy(GridFSProxy):
    """
    Proxy for ImageField

    versionadded: 0.6
    """
    def put(self, file_obj, **kwargs):
        """
        Insert a image in database
        applying field properties (size, thumbnail_size)
        """
        field = self.instance._fields[self.key]

        try:
            img = Image.open(file_obj)
            img_format = img.format
        except Exception as e:
            raise ValidationError('Invalid image: %s' % e)

        if (field.size and (img.size[0] > field.size['width'] or
                            img.size[1] > field.size['height'])):
            size = field.size

            if size['force']:
                img = ImageOps.fit(img,
                                   (size['width'],
                                    size['height']),
                                   Image.ANTIALIAS)
            else:
                img.thumbnail((size['width'],
                               size['height']),
                              Image.ANTIALIAS)

        thumbnail = None
        if field.thumbnail_size:
            size = field.thumbnail_size

            if size['force']:
                thumbnail = ImageOps.fit(img, (size['width'], size['height']), Image.ANTIALIAS)
            else:
                thumbnail = img.copy()
                thumbnail.thumbnail((size['width'],
                                     size['height']),
                                    Image.ANTIALIAS)

        if thumbnail:
            thumb_id = self._put_thumbnail(thumbnail, img_format)
        else:
            thumb_id = None

        w, h = img.size

        io = StringIO()
        img.save(io, img_format)
        io.seek(0)

        return super(ImageGridFsProxy, self).put(io,
                                                 width=w,
                                                 height=h,
                                                 format=img_format,
                                                 thumbnail_id=thumb_id,
                                                 **kwargs)

    def delete(self, *args, **kwargs):
        #deletes thumbnail
        out = self.get()
        if out and out.thumbnail_id:
            self.fs.delete(out.thumbnail_id)

        return super(ImageGridFsProxy, self).delete(*args, **kwargs)

    def _put_thumbnail(self, thumbnail, format, **kwargs):
        w, h = thumbnail.size

        io = StringIO()
        thumbnail.save(io, format)
        io.seek(0)

        return self.fs.put(io, width=w,
                           height=h,
                           format=format,
                           **kwargs)

    @property
    def size(self):
        """
        return a width, height of image
        """
        out = self.get()
        if out:
            return out.width, out.height

    @property
    def format(self):
        """
        return format of image
        ex: PNG, JPEG, GIF, etc
        """
        out = self.get()
        if out:
            return out.format

    @property
    def thumbnail(self):
        """
        return a gridfs.grid_file.GridOut
        representing a thumbnail of Image
        """
        out = self.get()
        if out and out.thumbnail_id:
            return self.fs.get(out.thumbnail_id)

    def write(self, *args, **kwargs):
        raise RuntimeError("Please use \"put\" method instead")

    def writelines(self, *args, **kwargs):
        raise RuntimeError("Please use \"put\" method instead")


class ImproperlyConfigured(Exception):
    pass


class ImageField(FileField):
    """
    A Image File storage field.

    @size (width, height, force):
        max size to store images, if larger will be automatically resized
        ex: size=(800, 600, True)

    @thumbnail (width, height, force):
        size to generate a thumbnail

    .. versionadded:: 0.6
    """
    proxy_class = ImageGridFsProxy

    def __init__(self, size=None, thumbnail_size=None,
                 collection_name='images', **kwargs):
        if not Image:
            raise ImproperlyConfigured("PIL library was not found")

        params_size = ('width', 'height', 'force')
        extra_args = dict(size=size, thumbnail_size=thumbnail_size)
        for att_name, att in list(extra_args.items()):
            value = None
            if isinstance(att, (tuple, list)):
                if PY3:
                    value = dict(itertools.zip_longest(params_size, att,
                                                       fillvalue=None))
                else:
                    value = dict(map(None, params_size, att))

            setattr(self, att_name, value)

        super(ImageField, self).__init__(
            collection_name=collection_name,
            **kwargs)


class SequenceField(BaseField):
    """Provides a sequental counter see:
     http://www.mongodb.org/display/DOCS/Object+IDs#ObjectIDs-SequenceNumbers

    .. note::

             Although traditional databases often use increasing sequence
             numbers for primary keys. In MongoDB, the preferred approach is to
             use Object IDs instead.  The concept is that in a very large
             cluster of machines, it is easier to create an object ID than have
             global, uniformly increasing sequence numbers.

    Use any callable as `value_decorator` to transform calculated counter into
    any value suitable for your needs, e.g. string or hexadecimal
    representation of the default integer counter value.

    .. versionadded:: 0.5

    .. versionchanged:: 0.8 added `value_decorator`
    """

    _auto_gen = True
    COLLECTION_NAME = 'mongoengine.counters'
    VALUE_DECORATOR = int

    def __init__(self, collection_name=None, db_alias=None, sequence_name=None,
                 value_decorator=None, *args, **kwargs):
        self.collection_name = collection_name or self.COLLECTION_NAME
        self.db_alias = db_alias or DEFAULT_CONNECTION_NAME
        self.sequence_name = sequence_name
        self.value_decorator = (isinstance(value_decorator, collections.Callable) and
                                value_decorator or self.VALUE_DECORATOR)
        return super(SequenceField, self).__init__(*args, **kwargs)

    def generate(self):
        """
        Generate and Increment the counter
        """
        sequence_name = self.get_sequence_name()
        sequence_id = "%s.%s" % (sequence_name, self.name)
        collection = get_db(alias=self.db_alias)[self.collection_name]
        counter = collection.find_and_modify(query={"_id": sequence_id},
                                             update={"$inc": {"next": 1}},
                                             new=True,
                                             upsert=True)
        return self.value_decorator(counter['next'])

    def set_next_value(self, value):
        """Helper method to set the next sequence value"""
        sequence_name = self.get_sequence_name()
        sequence_id = "%s.%s" % (sequence_name, self.name)
        collection = get_db(alias=self.db_alias)[self.collection_name]
        counter = collection.find_and_modify(query={"_id": sequence_id},
                                             update={"$set": {"next": value}},
                                             new=True,
                                             upsert=True)
        return self.value_decorator(counter['next'])

    def get_next_value(self):
        """Helper method to get the next value for previewing.

        .. warning:: There is no guarantee this will be the next value
        as it is only fixed on set.
        """
        sequence_name = self.get_sequence_name()
        sequence_id = "%s.%s" % (sequence_name, self.name)
        collection = get_db(alias=self.db_alias)[self.collection_name]
        data = collection.find_one({"_id": sequence_id})

        if data:
            return self.value_decorator(data['next']+1)

        return self.value_decorator(1)

    def get_sequence_name(self):
        if self.sequence_name:
            return self.sequence_name
        owner = self.owner_document
        if issubclass(owner, Document):
            return owner._get_collection_name()
        else:
            return ''.join('_%s' % c if c.isupper() else c
                           for c in owner._class_name).strip('_').lower()

    def __get__(self, instance, owner):
        value = super(SequenceField, self).__get__(instance, owner)
        if value is None and instance._initialised:
            value = self.generate()
            instance._data[self.name] = value
            instance._mark_as_changed(self.name)

        return value

    def __set__(self, instance, value):

        if value is None and instance._initialised:
            value = self.generate()

        return super(SequenceField, self).__set__(instance, value)

    def to_python(self, value):
        if value is None:
            value = self.generate()
        return value


class UUIDField(BaseField):
    """A UUID field.

    .. versionadded:: 0.6
    """
    _binary = None

    def __init__(self, binary=True, **kwargs):
        """
        Store UUID data in the database

        :param binary: if False store as a string.

        .. versionchanged:: 0.8.0
        .. versionchanged:: 0.6.19
        """
        self._binary = binary
        super(UUIDField, self).__init__(**kwargs)

    def to_python(self, value):
        if not self._binary:
            original_value = value
            try:
                if not isinstance(value, str):
                    value = str(value)
                return uuid.UUID(value)
            except:
                return original_value
        return value

    def to_mongo(self, value):
        if not self._binary:
            return str(value)
        elif isinstance(value, str):
            return uuid.UUID(value)
        return value

    def prepare_query_value(self, op, value):
        if value is None:
            return None
        return self.to_mongo(value)

    def validate(self, value):
        if not isinstance(value, uuid.UUID):
            if not isinstance(value, str):
                value = str(value)
            try:
                value = uuid.UUID(value)
            except Exception as exc:
                self.error('Could not convert to UUID: %s' % exc)


class GeoPointField(BaseField):
    """A list storing a latitude and longitude.

    .. versionadded:: 0.4
    """

    _geo_index = pymongo.GEO2D

    def validate(self, value):
        """Make sure that a geo-value is of type (x, y)
        """
        if not isinstance(value, (list, tuple)):
            self.error('GeoPointField can only accept tuples or lists '
                       'of (x, y)')

        if not len(value) == 2:
            self.error("Value (%s) must be a two-dimensional point" % repr(value))
        elif (not isinstance(value[0], (float, int)) or
              not isinstance(value[1], (float, int))):
            self.error("Both values (%s) in point must be float or int" % repr(value))


class PointField(GeoJsonBaseField):
    """A geo json field storing a latitude and longitude.

    The data is represented as:

    .. code-block:: js

        { "type" : "Point" ,
          "coordinates" : [x, y]}

    You can either pass a dict with the full information or a list
    to set the value.

    Requires mongodb >= 2.4
    .. versionadded:: 0.8
    """
    _type = "Point"


class LineStringField(GeoJsonBaseField):
    """A geo json field storing a line of latitude and longitude coordinates.

    The data is represented as:

    .. code-block:: js

        { "type" : "LineString" ,
          "coordinates" : [[x1, y1], [x1, y1] ... [xn, yn]]}

    You can either pass a dict with the full information or a list of points.

    Requires mongodb >= 2.4
    .. versionadded:: 0.8
    """
    _type = "LineString"


class PolygonField(GeoJsonBaseField):
    """A geo json field storing a polygon of latitude and longitude coordinates.

    The data is represented as:

    .. code-block:: js

        { "type" : "Polygon" ,
          "coordinates" : [[[x1, y1], [x1, y1] ... [xn, yn]],
                           [[x1, y1], [x1, y1] ... [xn, yn]]}

    You can either pass a dict with the full information or a list
    of LineStrings. The first LineString being the outside and the rest being
    holes.

    Requires mongodb >= 2.4
    .. versionadded:: 0.8
    """
    _type = "Polygon"
