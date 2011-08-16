from base import (BaseField, ComplexBaseField, ObjectIdField,
                  ValidationError, get_document)
from queryset import DO_NOTHING
from document import Document, EmbeddedDocument
from connection import _get_db
from operator import itemgetter

import re
import pymongo
import pymongo.dbref
import pymongo.son
import pymongo.binary
import datetime, time
import decimal
import gridfs


__all__ = ['StringField', 'IntField', 'FloatField', 'BooleanField',
           'DateTimeField', 'EmbeddedDocumentField', 'ListField', 'DictField',
           'ObjectIdField', 'ReferenceField', 'ValidationError', 'MapField',
           'DecimalField', 'ComplexDateTimeField', 'URLField',
           'GenericReferenceField', 'FileField', 'BinaryField',
           'SortedListField', 'EmailField', 'GeoPointField',
           'SequenceField']

RECURSIVE_REFERENCE_CONSTANT = 'self'


class StringField(BaseField):
    """A unicode string field.
    """

    def __init__(self, regex=None, max_length=None, min_length=None, **kwargs):
        self.regex = re.compile(regex) if regex else None
        self.max_length = max_length
        self.min_length = min_length
        super(StringField, self).__init__(**kwargs)

    def to_python(self, value):
        return unicode(value)

    def validate(self, value):
        assert isinstance(value, (str, unicode))

        if self.max_length is not None and len(value) > self.max_length:
            raise ValidationError('String value is too long')

        if self.min_length is not None and len(value) < self.min_length:
            raise ValidationError('String value is too short')

        if self.regex is not None and self.regex.match(value) is None:
            message = 'String value did not match validation regex'
            raise ValidationError(message)

    def lookup_member(self, member_name):
        return None

    def prepare_query_value(self, op, value):
        if not isinstance(op, basestring):
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

    URL_REGEX = re.compile(
        r'^https?://'
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'
        r'localhost|'
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
        r'(?::\d+)?'
        r'(?:/?|[/?]\S+)$', re.IGNORECASE
    )

    def __init__(self, verify_exists=False, **kwargs):
        self.verify_exists = verify_exists
        super(URLField, self).__init__(**kwargs)

    def validate(self, value):
        if not URLField.URL_REGEX.match(value):
            raise ValidationError('Invalid URL: %s' % value)

        if self.verify_exists:
            import urllib2
            try:
                request = urllib2.Request(value)
                response = urllib2.urlopen(request)
            except Exception, e:
                message = 'This URL appears to be a broken link: %s' % e
                raise ValidationError(message)


class EmailField(StringField):
    """A field that validates input as an E-Mail-Address.

    .. versionadded:: 0.4
    """

    EMAIL_REGEX = re.compile(
        r"(^[-!#$%&'*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z]+)*"  # dot-atom
        r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-011\013\014\016-\177])*"'  # quoted-string
        r')@(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?$', re.IGNORECASE  # domain
    )

    def validate(self, value):
        if not EmailField.EMAIL_REGEX.match(value):
            raise ValidationError('Invalid Mail-address: %s' % value)


class IntField(BaseField):
    """An integer field.
    """

    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value, self.max_value = min_value, max_value
        super(IntField, self).__init__(**kwargs)

    def to_python(self, value):
        return int(value)

    def validate(self, value):
        try:
            value = int(value)
        except:
            raise ValidationError('%s could not be converted to int' % value)

        if self.min_value is not None and value < self.min_value:
            raise ValidationError('Integer value is too small')

        if self.max_value is not None and value > self.max_value:
            raise ValidationError('Integer value is too large')

    def prepare_query_value(self, op, value):
        return int(value)


class FloatField(BaseField):
    """An floating point number field.
    """

    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value, self.max_value = min_value, max_value
        super(FloatField, self).__init__(**kwargs)

    def to_python(self, value):
        return float(value)

    def validate(self, value):
        if isinstance(value, int):
            value = float(value)
        assert isinstance(value, float)

        if self.min_value is not None and value < self.min_value:
            raise ValidationError('Float value is too small')

        if self.max_value is not None and value > self.max_value:
            raise ValidationError('Float value is too large')

    def prepare_query_value(self, op, value):
        return float(value)


class DecimalField(BaseField):
    """A fixed-point decimal number field.

    .. versionadded:: 0.3
    """

    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value, self.max_value = min_value, max_value
        super(DecimalField, self).__init__(**kwargs)

    def to_python(self, value):
        if not isinstance(value, basestring):
            value = unicode(value)
        return decimal.Decimal(value)

    def to_mongo(self, value):
        return unicode(value)

    def validate(self, value):
        if not isinstance(value, decimal.Decimal):
            if not isinstance(value, basestring):
                value = str(value)
            try:
                value = decimal.Decimal(value)
            except Exception, exc:
                raise ValidationError('Could not convert to decimal: %s' % exc)

        if self.min_value is not None and value < self.min_value:
            raise ValidationError('Decimal value is too small')

        if self.max_value is not None and value > self.max_value:
            raise ValidationError('Decimal value is too large')


class BooleanField(BaseField):
    """A boolean field type.

    .. versionadded:: 0.1.2
    """

    def to_python(self, value):
        return bool(value)

    def validate(self, value):
        assert isinstance(value, bool)


class DateTimeField(BaseField):
    """A datetime field.

    Note: Microseconds are rounded to the nearest millisecond.
      Pre UTC microsecond support is effecively broken.
      Use :class:`~mongoengine.fields.ComplexDateTimeField` if you
      need accurate microsecond support.
    """

    def validate(self, value):
        assert isinstance(value, (datetime.datetime, datetime.date))

    def to_mongo(self, value):
        return self.prepare_query_value(None, value)

    def prepare_query_value(self, op, value):
        if value is None:
            return value
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)

        # Attempt to parse a datetime:
        # value = smart_str(value)
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
            return datetime.datetime(*time.strptime(value, '%Y-%m-%d %H:%M:%S')[:6],
                                     **kwargs)
        except ValueError:
            try:  # Try without seconds.
                return datetime.datetime(*time.strptime(value, '%Y-%m-%d %H:%M')[:5],
                                         **kwargs)
            except ValueError:  # Try without hour/minutes/seconds.
                try:
                    return datetime.datetime(*time.strptime(value, '%Y-%m-%d')[:3],
                                             **kwargs)
                except ValueError:
                    return None



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
    """

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
        data = map(int, data)
        values = {}
        for i in range(7):
            values[self.names[i]] = data[i]
        return datetime.datetime(**values)

    def __get__(self, instance, owner):
        data = super(ComplexDateTimeField, self).__get__(instance, owner)
        if data == None:
            return datetime.datetime.now()
        return self._convert_from_string(data)

    def __set__(self, instance, value):
        value = self._convert_from_datetime(value)
        return super(ComplexDateTimeField, self).__set__(instance, value)

    def validate(self, value):
        if not isinstance(value, datetime.datetime):
            raise ValidationError('Only datetime objects may used in a \
                                   ComplexDateTimeField')

    def to_python(self, value):
        return self._convert_from_string(value)

    def to_mongo(self, value):
        return self._convert_from_datetime(value)

    def prepare_query_value(self, op, value):
        return self._convert_from_datetime(value)


class EmbeddedDocumentField(BaseField):
    """An embedded document field. Only valid values are subclasses of
    :class:`~mongoengine.EmbeddedDocument`.
    """

    def __init__(self, document_type, **kwargs):
        if not isinstance(document_type, basestring):
            if not issubclass(document_type, EmbeddedDocument):
                raise ValidationError('Invalid embedded document class '
                                      'provided to an EmbeddedDocumentField')
        self.document_type_obj = document_type
        super(EmbeddedDocumentField, self).__init__(**kwargs)

    @property
    def document_type(self):
        if isinstance(self.document_type_obj, basestring):
            if self.document_type_obj == RECURSIVE_REFERENCE_CONSTANT:
                self.document_type_obj = self.owner_document
            else:
                self.document_type_obj = get_document(self.document_type_obj)
        return self.document_type_obj

    def to_python(self, value):
        if not isinstance(value, self.document_type):
            return self.document_type._from_son(value)
        return value

    def to_mongo(self, value):
        if not isinstance(value, self.document_type):
            return value
        return self.document_type.to_mongo(value)

    def validate(self, value):
        """Make sure that the document instance is an instance of the
        EmbeddedDocument subclass provided when the document was defined.
        """
        # Using isinstance also works for subclasses of self.document
        if not isinstance(value, self.document_type):
            raise ValidationError('Invalid embedded document instance '
                                  'provided to an EmbeddedDocumentField')
        self.document_type.validate(value)

    def lookup_member(self, member_name):
        return self.document_type._fields.get(member_name)

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)


class ListField(ComplexBaseField):
    """A list field that wraps a standard field, allowing multiple instances
    of the field to be used as a list in the database.
    """

    # ListFields cannot be indexed with _types - MongoDB doesn't support this
    _index_with_types = False

    def __init__(self, field=None, **kwargs):
        self.field = field
        kwargs.setdefault('default', lambda: [])
        super(ListField, self).__init__(**kwargs)

    def validate(self, value):
        """Make sure that a list of valid fields is being used.
        """
        if not isinstance(value, (list, tuple)):
            raise ValidationError('Only lists and tuples may be used in a '
                                  'list field')
        super(ListField, self).validate(value)

    def prepare_query_value(self, op, value):
        if self.field:
            if op in ('set', 'unset') and (not isinstance(value, basestring)
                and hasattr(value, '__iter__')):
                return [self.field.prepare_query_value(op, v) for v in value]
            return self.field.prepare_query_value(op, value)
        return super(ListField, self).prepare_query_value(op, value)


class SortedListField(ListField):
    """A ListField that sorts the contents of its list before writing to
    the database in order to ensure that a sorted list is always
    retrieved.

    .. versionadded:: 0.4
    """

    _ordering = None

    def __init__(self, field, **kwargs):
        if 'ordering' in kwargs.keys():
            self._ordering = kwargs.pop('ordering')
        super(SortedListField, self).__init__(field, **kwargs)

    def to_mongo(self, value):
        value = super(SortedListField, self).to_mongo(value)
        if self._ordering is not None:
            return sorted(value, key=itemgetter(self._ordering))
        return sorted(value)


class DictField(ComplexBaseField):
    """A dictionary field that wraps a standard Python dictionary. This is
    similar to an embedded document, but the structure is not defined.

    .. versionadded:: 0.3
    """

    def __init__(self, basecls=None, field=None, *args, **kwargs):
        self.field = field
        self.basecls = basecls or BaseField
        assert issubclass(self.basecls, BaseField)
        kwargs.setdefault('default', lambda: {})
        super(DictField, self).__init__(*args, **kwargs)

    def validate(self, value):
        """Make sure that a list of valid fields is being used.
        """
        if not isinstance(value, dict):
            raise ValidationError('Only dictionaries may be used in a '
                                  'DictField')

        if any(('.' in k or '$' in k) for k in value):
            raise ValidationError('Invalid dictionary key name - keys may not '
                                  'contain "." or "$" characters')
        super(DictField, self).validate(value)

    def lookup_member(self, member_name):
        return DictField(basecls=self.basecls, db_field=member_name)

    def prepare_query_value(self, op, value):
        match_operators = ['contains', 'icontains', 'startswith',
                           'istartswith', 'endswith', 'iendswith',
                           'exact', 'iexact']

        if op in match_operators and isinstance(value, basestring):
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
            raise ValidationError('Argument to MapField constructor must be '
                                  'a valid field')
        super(MapField, self).__init__(field=field, *args, **kwargs)



class ReferenceField(BaseField):
    """A reference to a document that will be automatically dereferenced on
    access (lazily).

    Use the `reverse_delete_rule` to handle what should happen if the document
    the field is referencing is deleted.

    The options are:

      * DO_NOTHING  - don't do anything (default).
      * NULLIFY     - Updates the reference to null.
      * CASCADE     - Deletes the documents associated with the reference.
      * DENY        - Prevent the deletion of the reference object.
    """

    def __init__(self, document_type, reverse_delete_rule=DO_NOTHING, **kwargs):
        """Initialises the Reference Field.

        :param reverse_delete_rule: Determines what to do when the referring
          object is deleted
        """
        if not isinstance(document_type, basestring):
            if not issubclass(document_type, (Document, basestring)):
                raise ValidationError('Argument to ReferenceField constructor '
                                      'must be a document class or a string')
        self.document_type_obj = document_type
        self.reverse_delete_rule = reverse_delete_rule
        super(ReferenceField, self).__init__(**kwargs)

    @property
    def document_type(self):
        if isinstance(self.document_type_obj, basestring):
            if self.document_type_obj == RECURSIVE_REFERENCE_CONSTANT:
                self.document_type_obj = self.owner_document
            else:
                self.document_type_obj = get_document(self.document_type_obj)
        return self.document_type_obj

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
            if value is not None:
                instance._data[self.name] = self.document_type._from_son(value)

        return super(ReferenceField, self).__get__(instance, owner)

    def to_mongo(self, document):
        id_field_name = self.document_type._meta['id_field']
        id_field = self.document_type._fields[id_field_name]

        if isinstance(document, Document):
            # We need the id from the saved object to create the DBRef
            id_ = document.id
            if id_ is None:
                raise ValidationError('You can only reference documents once '
                                      'they have been saved to the database')
        else:
            id_ = document

        id_ = id_field.to_mongo(id_)
        collection = self.document_type._get_collection_name()
        return pymongo.dbref.DBRef(collection, id_)

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)

    def validate(self, value):
        assert isinstance(value, (self.document_type, pymongo.dbref.DBRef))

        if isinstance(value, Document) and value.id is None:
            raise ValidationError('You can only reference documents once '
                                  'they have been saved to the database')


    def lookup_member(self, member_name):
        return self.document_type._fields.get(member_name)


class GenericReferenceField(BaseField):
    """A reference to *any* :class:`~mongoengine.document.Document` subclass
    that will be automatically dereferenced on access (lazily).

    ..note ::  Any documents used as a generic reference must be registered in the
    document registry.  Importing the model will automatically register it.

    .. versionadded:: 0.3
    """

    def __get__(self, instance, owner):
        if instance is None:
            return self

        value = instance._data.get(self.name)
        if isinstance(value, (dict, pymongo.son.SON)):
            instance._data[self.name] = self.dereference(value)

        return super(GenericReferenceField, self).__get__(instance, owner)

    def validate(self, value):
        if not isinstance(value, (Document, pymongo.dbref.DBRef)):
            raise ValidationError('GenericReferences can only contain documents')

        # We need the id from the saved object to create the DBRef
        if isinstance(value, Document) and value.id is None:
            raise ValidationError('You can only reference documents once '
                                  'they have been saved to the database')

    def dereference(self, value):
        doc_cls = get_document(value['_cls'])
        reference = value['_ref']
        doc = _get_db().dereference(reference)
        if doc is not None:
            doc = doc_cls._from_son(doc)
        return doc

    def to_mongo(self, document):
        if document is None:
            return None

        id_field_name = document.__class__._meta['id_field']
        id_field = document.__class__._fields[id_field_name]

        if isinstance(document, Document):
            # We need the id from the saved object to create the DBRef
            id_ = document.id
            if id_ is None:
                raise ValidationError('You can only reference documents once '
                                      'they have been saved to the database')
        else:
            id_ = document

        id_ = id_field.to_mongo(id_)
        collection = document._get_collection_name()
        ref = pymongo.dbref.DBRef(collection, id_)
        return {'_cls': document._class_name, '_ref': ref}

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)


class BinaryField(BaseField):
    """A binary data field.
    """

    def __init__(self, max_bytes=None, **kwargs):
        self.max_bytes = max_bytes
        super(BinaryField, self).__init__(**kwargs)

    def to_mongo(self, value):
        return pymongo.binary.Binary(value)

    def to_python(self, value):
        # Returns str not unicode as this is binary data
        return str(value)

    def validate(self, value):
        assert isinstance(value, str)

        if self.max_bytes is not None and len(value) > self.max_bytes:
            raise ValidationError('Binary value is too long')


class GridFSError(Exception):
    pass


class GridFSProxy(object):
    """Proxy object to handle writing and reading of files to and from GridFS

    .. versionadded:: 0.4
    """

    def __init__(self, grid_id=None, key=None, instance=None):
        self.fs = gridfs.GridFS(_get_db())  # Filesystem instance
        self.newfile = None                 # Used for partial writes
        self.grid_id = grid_id              # Store GridFS id for file
        self.gridout = None
        self.key = key
        self.instance = instance

    def __getattr__(self, name):
        obj = self.get()
        if name in dir(obj):
            return getattr(obj, name)
        raise AttributeError

    def __get__(self, instance, value):
        return self

    def __nonzero__(self):
        return bool(self.grid_id)

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
        try:
            return self.get().read(size)
        except:
            return None

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
    """

    def __init__(self, **kwargs):
        super(FileField, self).__init__(**kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self

        # Check if a file already exists for this model
        grid_file = instance._data.get(self.name)
        self.grid_file = grid_file
        if isinstance(self.grid_file, GridFSProxy):
            if not self.grid_file.key:
                self.grid_file.key = self.name
                self.grid_file.instance = instance
            return self.grid_file
        return GridFSProxy(key=self.name, instance=instance)

    def __set__(self, instance, value):
        key = self.name
        if isinstance(value, file) or isinstance(value, str):
            # using "FileField() = file/string" notation
            grid_file = instance._data.get(self.name)
            # If a file already exists, delete it
            if grid_file:
                try:
                    grid_file.delete()
                except:
                    pass
                # Create a new file with the new data
                grid_file.put(value)
            else:
                # Create a new proxy object as we don't already have one
                instance._data[key] = GridFSProxy(key=key, instance=instance)
                instance._data[key].put(value)
        else:
            instance._data[key] = value

        instance._mark_as_changed(key)

    def to_mongo(self, value):
        # Store the GridFS file id in MongoDB
        if isinstance(value, GridFSProxy) and value.grid_id is not None:
            return value.grid_id
        return None

    def to_python(self, value):
        if value is not None:
            return GridFSProxy(value)

    def validate(self, value):
        if value.grid_id is not None:
            assert isinstance(value, GridFSProxy)
            assert isinstance(value.grid_id, pymongo.objectid.ObjectId)


class GeoPointField(BaseField):
    """A list storing a latitude and longitude.

    .. versionadded:: 0.4
    """

    _geo_index = True

    def validate(self, value):
        """Make sure that a geo-value is of type (x, y)
        """
        if not isinstance(value, (list, tuple)):
            raise ValidationError('GeoPointField can only accept tuples or '
                                  'lists of (x, y)')

        if not len(value) == 2:
            raise ValidationError('Value must be a two-dimensional point.')
        if (not isinstance(value[0], (float, int)) and
            not isinstance(value[1], (float, int))):
            raise ValidationError('Both values in point must be float or int.')


class SequenceField(IntField):
    """Provides a sequental counter.

    ..note:: Although traditional databases often use increasing sequence
             numbers for primary keys. In MongoDB, the preferred approach is to
             use Object IDs instead.  The concept is that in a very large
             cluster of machines, it is easier to create an object ID than have
             global, uniformly increasing sequence numbers.

    .. versionadded:: 0.5
    """
    def __init__(self, collection_name=None, *args, **kwargs):
        self.collection_name = collection_name or 'mongoengine.counters'
        return super(SequenceField, self).__init__(*args, **kwargs)

    def generate_new_value(self):
        """
        Generate and Increment the counter
        """
        sequence_id = "{0}.{1}".format(self.owner_document._get_collection_name(),
                                       self.name)
        collection = _get_db()[self.collection_name]
        counter = collection.find_and_modify(query={"_id": sequence_id},
                                             update={"$inc": {"next": 1}},
                                             new=True,
                                             upsert=True)
        return counter['next']

    def __get__(self, instance, owner):

        if instance is None:
            return self

        if not instance._data:
            return

        value = instance._data.get(self.name)

        if not value and instance._initialised:
            value = self.generate_new_value()
            instance._data[self.name] = value
            instance._mark_as_changed(self.name)

        return value

    def __set__(self, instance, value):

        if value is None and instance._initialised:
            value = self.generate_new_value()

        return super(SequenceField, self).__set__(instance, value)

    def to_python(self, value):
        if value is None:
            value = self.generate_new_value()
        return value
