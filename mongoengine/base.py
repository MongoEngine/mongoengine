from queryset import QuerySet, QuerySetManager
from queryset import DoesNotExist, MultipleObjectsReturned

import copy
import sys
import pymongo
import bson.objectid
import bson.dbref
import logging
import socket
import traceback
import greenlet
from collections import defaultdict

_document_registry = {}


def get_document(name):
    return _document_registry[name]


class ValidationError(Exception):
    pass


class FieldNotLoadedError(Exception):
    def __init__(self, collection_name, field_name):
        self.collection_name = collection_name
        self.field_name = field_name
        super(FieldNotLoadedError, self).__init__(
            'Field accessed, but not loaded: %s.%s' % (collection_name,
                                                       field_name))


class UnloadedFieldHandler(object):
    def handle_exception(self, exception):
        raise NotImplementedError


class UnloadedFieldNoopHandler(UnloadedFieldHandler):
    def handle_exception(self, exception):
        pass


class UnloadedFieldExceptionHandler(UnloadedFieldHandler):
    # pylint: disable=no-self-use
    def handle_exception(self, exception):
        raise exception


class UnloadedFieldLogHandler(UnloadedFieldHandler):
    def __init__(self, log_root):
        self.logged = defaultdict(dict)
        self.field_logger = logging.getLogger('%s.field_unloaded' % log_root)

    def handle_exception(self, exception):
        if exception.field_name not in self.logged[exception.collection_name]:
            self.logged[exception.collection_name][exception.field_name] = True
            exc_info = {
                'model': exception.collection_name,
                'field': exception.field_name,
                'stack': ''.join(traceback.format_list(
                    traceback.extract_stack()[:-2]))
            }
            self.field_logger.info(exc_info)

_unloaded_field_handler = UnloadedFieldNoopHandler()


def set_unloaded_field_handler(handler):
    global _unloaded_field_handler
    assert isinstance(handler, UnloadedFieldHandler)
    _unloaded_field_handler = handler


# a primer on load statuses and _data members (patrick, March 2016):
#
# There are two states a field can be in. these states are enumerated
# in FieldStatus.
# NOT_LOADED: This field was excluded from the query to mongo. It is generally
#             unsafe to access a field in this state. Doing so through normal
#             channels will cause a FieldNotLoadedError to be generated and
#             handled with the configured UnloadedFieldHandler.

# LOADED: This field has been requested from the database.
#
#
# There are three members of a BaseDocument that describe the data contained
# within: _raw_data, _lazy_data, and _data.  _lazy_data is a data member that
# returns a dictionary mapping the **mongo** field name to some form of data.
# _raw_data and _data map the **python** field name to the processed data.
#
# _raw_data: This is data that has been processed into python form. It maps
#            raw field name to python data. If data has been loaded but not yet
#            accessed, it will not be in this dictionary.
#
# _lazy_data: This is data that has not yet been processed into python form. It
#             was loaded from mongoengine. When it is processed, it is removed
#             and placed in _raw_data.
#
# _data: For historical reasons, this is a property that returns a dictionary
#        with a processed entry for each field in the document. The priority
#        for retrieving a value is _raw_data > to_python(_lazy_data) > default
#        value for field. This is generally an unsafe way to access data and
#        should not be used in new code.

class FieldStatus(object):
    """
        Enum representing field status
    """
    NOT_LOADED  = 1
    LOADED      = 2


class BaseField(object):
    """A base class for fields in a MongoDB document. Instances of this class
    may be added to subclasses of `Document` to define a document's schema.
    """

    # Fields may have _types inserted into indexes by default
    _index_with_types = True
    _geo_index = False

    def __init__(self, db_field=None, name=None, required=False, default=None,
                 unique=False, unique_with=None, primary_key=False,
                 validation=None, choices=None, dup_check=True):
        self.db_field = (db_field or name) if not primary_key else '_id'
        if name:
            import warnings
            msg = "Fields' 'name' attribute deprecated in favour of 'db_field'"
            warnings.warn(msg, DeprecationWarning)
        self.name = None
        self.required = required or primary_key
        self.default = default
        self.unique = bool(unique or unique_with)
        self.unique_with = unique_with
        self.primary_key = primary_key
        self.validation = validation
        self.choices = choices
        self.dup_check= dup_check
        self._in_list = False

    def __get__(self, instance, owner):
        """Descriptor for retrieving a value from a field in a document. Do
        any necessary conversion between Python and MongoDB types.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        if not instance._all_loaded and \
           not instance._allow_unloaded and \
           instance._get_field_status(self.db_field) == FieldStatus.NOT_LOADED:
            _unloaded_field_handler.handle_exception(
                FieldNotLoadedError(instance.__class__.__name__, self.name))

        if self.db_field in instance._lazy_data:
            # process the data, move it from _lazy to _raw
            value = self.to_python(instance._lazy_data.pop(self.db_field))
            instance._raw_data[self.name] = value
        else:
            # Get value from document instance if available, if not use default
            value = instance._raw_data.get(self.name)
            if value is None:
                value = self.default
                # Allow callable default values
                if callable(value):
                    value = value()
        return value

    def __set__(self, instance, value):
        """Descriptor for assigning a value to a field in a document.
        """
        instance._fields_status[self.db_field] = FieldStatus.LOADED
        instance._raw_data[self.name] = value
        if self.db_field in instance._lazy_data:
            del instance._lazy_data[self.db_field]

    def to_python(self, value):
        """Convert a MongoDB-compatible type to a Python type.
        """
        return value

    def to_mongo(self, value):
        """Convert a Python type to a MongoDB-compatible type.
        """
        return self.to_python(value)

    def prepare_query_value(self, op, value):
        """Prepare a value that is being used in a query for PyMongo.
        """
        return value

    def validate(self, value):
        """Perform validation on a value.
        """
        pass

    def validate_choices(self, value):
        '''Ensures the value is one of the choices
        '''
        if self.choices is None:
            return

        if value not in self.choices:
            raise ValidationError("Value must be one of %s."
                % unicode(self.choices))

    def _validate(self, value):
        self.validate_choices(value)

        # check validation argument
        if self.validation is not None:
            if callable(self.validation):
                if not self.validation(value):
                    raise ValidationError('Value does not match custom' \
                                          'validation method.')
            else:
                raise ValueError('validation argument must be a callable.')

        self.validate(value)

class ObjectIdField(BaseField):
    """An field wrapper around MongoDB's ObjectIds.
    """

    def to_python(self, value):
        return value
        # return unicode(value)

    def to_mongo(self, value):
        if not isinstance(value, bson.objectid.ObjectId):
            try:
                return bson.objectid.ObjectId(unicode(value))
            except Exception, e:
                if self.dup_check:
                    #e.message attribute has been deprecated since Python 2.6
                    raise ValidationError(unicode(e))
                else:
                    return value
        return value

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)

    def validate(self, value):
        if value is None and not self.primary_key:
            # allow None value if field not primary_key
            return
        try:
            bson.objectid.ObjectId(unicode(value))
        except:
            raise ValidationError('Invalid Object ID')

'''
A class for specifying relationships in Mongo Documents
'''
class Relationship(object):

    '''
    model            - The model class (or name) of the related document
    id_field         - The name of the attribute in this document which
                        holds the id of the related document
    related_id_field - The name of the attribute in the related docment
                        which id_field references
    '''
    def __init__(self, model, id_field, related_id_field="id", multi=False):
        self.model            = model
        self.id_field         = id_field
        self.related_id_field = related_id_field
        self.multi            = multi
        self.name             = None

    def __get__(self, instance, owner):
        if instance is None:
            # Document class being used rather than a document object
            return self

        # Get value from document instance if available, if not use default
        value = instance._raw_data.get(self.name)
        if isinstance(value, Relationship):
            # Not resolved
            raise RuntimeError("Relationship %s not resolved" % self.name)
        return value

    def __set__(self, instance, value):
        instance._raw_data[self.name] = value

    # Converts the string name to a model object (if necessary)
    # Validates model object
    def validate_model(self):
        if isinstance(self.model, basestring):
            self.model = get_document(self.model)
        if not issubclass(self.model, BaseDocument):
            raise RuntimeError("model must be a subclass of BaseDocument")

class DocumentMetaclass(type):
    """Metaclass for all documents.
    """

    def __new__(cls, name, bases, attrs):
        metaclass = attrs.get('__metaclass__')
        super_new = super(DocumentMetaclass, cls).__new__
        if metaclass and issubclass(metaclass, DocumentMetaclass):
            return super_new(cls, name, bases, attrs)

        doc_fields = {}
        class_name = [name]
        superclasses = {}
        simple_class = True
        doc_relationships = {}

        # maps db_field to their name, for db_field de-dup purpose
        field_to_name_map = {}

        for base in bases:
            # Include all fields present in superclasses
            if hasattr(base, '_fields'):
                doc_fields.update(base._fields)
                class_name.append(base._class_name)
                # Get superclasses from superclass
                superclasses[base._class_name] = base
                superclasses.update(base._superclasses)

                #inherit field_names from superclass
                field_to_name_map.update(
                    [(_field.db_field, attr_name)
                     for attr_name, _field in base._fields.iteritems()])

            if hasattr(base, '_meta'):
                # Ensure that the Document class may be subclassed -
                # inheritance may be disabled to remove dependency on
                # additional fields _cls and _types
                if base._meta.get('allow_inheritance', True) == False:
                    raise ValueError('Document %s may not be subclassed' %
                                     base.__name__)
                else:
                    simple_class = False

            # Include all relationships present in superclasses
            if hasattr(base, '_relationships'):
                doc_relationships.update(base._relationships)


        meta = attrs.get('_meta', attrs.get('meta', {}))

        if 'allow_inheritance' not in meta:
            meta['allow_inheritance'] = True

        # Only simple classes - direct subclasses of Document - may set
        # allow_inheritance to False
        if not simple_class and not meta['allow_inheritance']:
            raise ValueError('Only direct subclasses of Document may set '
                             '"allow_inheritance" to False')
        attrs['_meta'] = meta

        attrs['_class_name'] = '.'.join(reversed(class_name))
        attrs['_superclasses'] = superclasses

        # Add the document's fields and relationships to the _fields attribute
        for attr_name, attr_value in attrs.items():
            if hasattr(attr_value, "__class__"):
                if issubclass(attr_value.__class__, BaseField):
                    attr_value.name = attr_name
                    if not attr_value.db_field:
                        attr_value.db_field = attr_name
                        field_name = attr_name
                    else:
                        field_name = attr_value.db_field

                    if attr_value.dup_check:
                        # a sanity check, can't have two fields with same db_field
                        # unless they also have the same name
                        assert field_name not in field_to_name_map or \
                                field_to_name_map[field_name] == attr_name, \
                                "Field %s with db_field %s already exists in %s " \
                                "or its superclass!" % (attr_name, field_name, name)
                        field_to_name_map[field_name] = attr_name
                    doc_fields[attr_name] = attr_value

                elif issubclass(attr_value.__class__, Relationship):
                    attr_value.name = attr_name
                    doc_relationships[attr_name] = attr_value

        attrs['_fields'] = doc_fields
        attrs['_relationships'] = doc_relationships
        attrs['_bulk_op'] = None
        attrs['_allow_unloaded'] = False

        new_class = super_new(cls, name, bases, attrs)
        for field in new_class._fields.values():
            field.owner_document = new_class

        module = attrs.get('__module__')

        base_excs = tuple(base.DoesNotExist for base in bases
                          if hasattr(base, 'DoesNotExist')) or (DoesNotExist,)
        exc = subclass_exception('DoesNotExist', base_excs, module)
        new_class.add_to_class('DoesNotExist', exc)

        base_excs = tuple(base.MultipleObjectsReturned for base in bases
                          if hasattr(base, 'MultipleObjectsReturned'))
        base_excs = base_excs or (MultipleObjectsReturned,)
        exc = subclass_exception('MultipleObjectsReturned', base_excs, module)
        new_class.add_to_class('MultipleObjectsReturned', exc)

        if name in _document_registry and _document_registry[name] != new_class:
            raise ValueError("Different class named %s already exists" %
                             new_class._class_name)
        _document_registry[name] = new_class

        return new_class

    def add_to_class(self, name, value):
        setattr(self, name, value)


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

        id_field = None
        base_indexes = []
        base_meta = {}

        # Subclassed documents inherit collection from superclass
        for base in bases:
            if hasattr(base, '_meta') and 'collection' in base._meta:
                collection = base._meta['collection']

                # Propagate index options.
                for key in ('index_background', 'index_drop_dups', 'index_opts'):
                   if key in base._meta:
                      base_meta[key] = base._meta[key]

                id_field = id_field or base._meta.get('id_field')
                base_indexes += base._meta.get('indexes', [])

        meta = {
            'collection': collection,
            'max_documents': None,
            'max_size': None,
            'ordering': [], # default ordering applied at runtime
            'indexes': [], # indexes to be ensured at runtime
            'id_field': id_field,
            'index_background': True,
            'index_drop_dups': False,
            'index_opts': {},
            'queryset_class': QuerySet,
            'db_name': None,

            'force_insert': False,

            'hash_field': None,
            'hash_db_field': '_h',
            'sharded': True,

            'write_concern': 1
        }
        meta.update(base_meta)

        # Apply document-defined meta options
        meta.update(attrs.get('meta', {}))
        attrs['_meta'] = meta

        # Set up collection manager, needs the class to have fields so use
        # DocumentMetaclass before instantiating CollectionManager object
        new_class = super_new(cls, name, bases, attrs)

        # Provide a default queryset unless one has been manually provided
        if not hasattr(new_class, 'objects'):
            new_class.objects = QuerySetManager()

        user_indexes = [QuerySet._build_index_spec(new_class, spec)
                        for spec in meta['indexes']] + base_indexes
        new_class._meta['indexes'] = user_indexes

        unique_indexes = []
        for field_name, field in new_class._fields.items():
            # Generate a list of indexes needed by uniqueness constraints
            if field.unique:
                field.required = True
                unique_fields = [field.db_field]

                # Add any unique_with fields to the back of the index spec
                if field.unique_with:
                    if isinstance(field.unique_with, basestring):
                        field.unique_with = [field.unique_with]

                    # Convert unique_with field names to real field names
                    unique_with = []
                    for other_name in field.unique_with:
                        parts = other_name.split('.')
                        # Lookup real name
                        parts = QuerySet._lookup_field(new_class, parts)
                        name_parts = [part.db_field for part in parts]
                        unique_with.append('.'.join(name_parts))
                        # Unique field should be required
                        parts[-1].required = True
                    unique_fields += unique_with

                # Add the new index to the list
                index = [(f, pymongo.ASCENDING) for f in unique_fields]
                unique_indexes.append(index)

            # Check for custom primary key
            if field.primary_key:
                current_pk = new_class._meta['id_field']
                if current_pk and current_pk != field_name:
                    raise ValueError('Cannot override primary key field')

                if not current_pk:
                    new_class._meta['id_field'] = field_name
                    # Make 'Document.id' an alias to the real primary key field
                    new_class.id = field

        new_class._meta['unique_indexes'] = unique_indexes

        if not new_class._meta['id_field']:
            new_class._meta['id_field'] = 'id'
            id_field = ObjectIdField(db_field='_id')
            id_field.name = 'id'
            id_field.primary_key = True
            id_field.required = False
            new_class._fields['id'] = id_field
            new_class.id = new_class._fields['id']

        if meta['hash_field']:
            assert 'shard_hash' not in new_class._fields, \
                    "You already have a shard hash"

            assert meta['hash_field'] in new_class._fields, \
                    "The field you want to hash doesn't exist"

            from fields import IntField

            field = IntField(db_field=meta['hash_db_field'], required=True)
            new_class._fields['shard_hash'] = field
            field.owner_document = new_class
            new_class.shard_hash = field

        return new_class

    def _get_collection_name(cls):
        return cls._meta.get('collection', None)


class BaseDocument(object):

    def __init__(self, from_son=False, **values):
        self._all_loaded = False
        self._raw_data = {}
        self._lazy_data = dict()
        self._fields_status = dict()
        self._default_load_status = FieldStatus.NOT_LOADED
        if from_son:
            self._lazy_data = values
            # set _id by default
            setattr(self, '_id', values.get('_id'))
        else:
            self._all_loaded = True
            self._allow_unloaded = True
            try:
                for attr_name, attr_value in self._fields.iteritems():
                    # Use default value if present
                    value = getattr(self, attr_name, None)
                    setattr(self, attr_name, value)
            finally:
                self._allow_unloaded = False

            # Assign initial values to instance
            for attr_name, attr_value in values.iteritems():
                try:
                    setattr(self, attr_name, attr_value)
                except AttributeError:
                    pass

        for rel_name, rel in self._relationships.iteritems():
            setattr(self, rel_name, rel)

    @property
    def _data(self):
        data_dict = {}
        for field_name in self._fields.iterkeys():
            value = self._get_raw(field_name)
            if isinstance(value, (dict, list)):
                value = copy.deepcopy(value)
            data_dict[field_name] = value
        for rel_name in self._relationships.iterkeys():
            data_dict[rel_name] = self._raw_data.get(rel_name)
        return data_dict

    def _get_raw(self, field_name):
        self._allow_unloaded = True
        try:
            field = self._fields[field_name]
            if field.db_field in self._lazy_data:
                value = self._lazy_data[field.db_field]
                value = field.to_python(value)
                # set actual value since we've done the conversion anyway,
                # but not for (generic)reference/list fields since those have
                # extra processing on __get__
                # (we can't import these here so just check name)
                if field.__class__.__name__ not in ['GenericReferenceField',
                                                    'ReferenceField',
                                                    'ListField']:
                    setattr(self, field_name, value)
            elif field_name in self._raw_data:
                value = self._raw_data[field_name]
            else:
                value = field.default
                if callable(value):
                    value = value()
        finally:
            self._allow_unloaded = False
        if isinstance(value, (dict, list)):
            value = copy.deepcopy(value)
        return value

    def _get_field_status(self, field):
        return self._fields_status.get(field, self._default_load_status)

    def field_is_loaded(self, field):
        field_id = self._fields[field].db_field
        return self._get_field_status(field_id) != FieldStatus.NOT_LOADED

    def validate(self):
        """Ensure that all fields' values are valid and that required fields
        are present.
        """
        # Get a list of tuples of field names and their current values
        fields = list()
        for name, field in self._fields.items():
            db_field = field.db_field
            if self._get_field_status(db_field) == FieldStatus.NOT_LOADED:
                continue
            if db_field in self._lazy_data:
                # don't bother checking data the user hasn't touched
                continue
            fields.append((field, getattr(self, name)))

        # Ensure that each field is matched to a valid value
        for field, value in fields:
            if not field.dup_check:
                continue
            if value is not None:
                try:
                    field._validate(value)
                except (ValueError, AttributeError, AssertionError), e:
                    raise ValidationError('Invalid value for field of type "%s": %s'
                                          % (field.__class__.__name__, value))
            elif field.required:
                raise ValidationError('Field "%s" is required' % field.name)


    @classmethod
    def _get_subclasses(cls):
        """Return a dictionary of all subclasses (found recursively).
        """
        try:
            subclasses = cls.__subclasses__()
        except:
            subclasses = cls.__subclasses__(cls)

        all_subclasses = {}
        for subclass in subclasses:
            all_subclasses[subclass._class_name] = subclass
            all_subclasses.update(subclass._get_subclasses())
        return all_subclasses

    @apply
    def pk():
        """Primary key alias
        """
        def fget(self):
            return getattr(self, self._meta['id_field'])
        def fset(self, value):
            return setattr(self, self._meta['id_field'], value)
        return property(fget, fset)

    def __iter__(self):
        return iter(self._fields)

    def __getitem__(self, name):
        """Dictionary-style field access, return a field's value if present.
        """
        try:
            if name in self._fields:
                return getattr(self, name)
        except AttributeError:
            pass
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
            val = getattr(self, name)
            return val is not None
        except AttributeError:
            return False

    def __len__(self):
        return len(self._raw_data) + len(self._lazy_data)

    def __nonzero__(self):
        return True

    def __repr__(self):
        try:
            u = unicode(self)
        except (UnicodeEncodeError, UnicodeDecodeError):
            u = '[Bad Unicode data]'
        return u'<%s: %s>' % (self.__class__.__name__, u)

    def __str__(self):
        if hasattr(self, '__unicode__'):
            return unicode(self).encode('utf-8')
        return '%s object' % self.__class__.__name__

    def to_mongo(self):
        """Return data dictionary ready for use with MongoDB.
        """
        data = {}
        self._allow_unloaded = True
        try:
            for field_name, field in self._fields.items():
                # don't deference ReferenceField if it's not
                # dereferenced yet
                if field.db_field in self._lazy_data:
                    data[field.db_field] = self._lazy_data[field.db_field]
                elif field_name in self._raw_data and \
                   isinstance(self._raw_data[field_name], (bson.dbref.DBRef)):
                    data[field.db_field] = self._raw_data[field_name]
                else:
                    value = getattr(self, field_name, None)
                    if value is not None:
                        data[field.db_field] = field.to_mongo(value)
            # Only add _cls and _types if allow_inheritance is not False
            if not (hasattr(self, '_meta') and
                    self._meta.get('allow_inheritance', True) is False):
                data['_cls'] = self._class_name
                data['_types'] = self._superclasses.keys() + [self._class_name]
            if '_id' in data and not data['_id']:
                del data['_id']
        finally:
            self._allow_unloaded = False
        return data

    @classmethod
    def _from_son(cls, son):
        """Create an instance of a Document (subclass) from a PyMongo SON.
        """
        # get the class name from the document, falling back to the given
        # class if unavailable
        class_name = son.pop(u'_cls', cls._class_name)

        data = dict((str(key), value) for key, value in son.items())

        if '_types' in data:
            del data['_types']

        # Return correct subclass for document type
        if class_name != cls._class_name:
            subclasses = cls._get_subclasses()
            if class_name not in subclasses:
                # Type of document is probably more generic than the class
                # that has been queried to return this SON
                return None
            cls = subclasses[class_name]


        return cls(from_son=True, **data)

    def __eq__(self, other):
        if isinstance(other, self.__class__) and hasattr(other, 'id'):
            if self.id == other.id:
                return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        """ For list, dic key  """
        if self.pk is None:
            # For new object
            return super(BaseDocument,self).__hash__()
        else:
            return hash(self.pk)

if sys.version_info < (2, 5):
    # Prior to Python 2.5, Exception was an old-style class
    def subclass_exception(name, parents, unused):
        return types.ClassType(name, parents, {})
else:
    def subclass_exception(name, parents, module):
        return type(name, parents, {'__module__': module})

class MongoComment(object):
    _ip = None
    AUTO_FRAME_LIMIT = 10
    AUTO_BLACKLIST = (
        'mongoengine/base.py',
        'mongoengine/document.py',
        'cl/utils/mongo.py',
        'cl/utils/pipeline.py',
        'cl/utils/iter.py',
        'cl/utils/deco.py',
        'cl/utils/memcache.py',
        'cl/utils/localmemcache.py',
        'cl/utils/lfu_cache.py',
    )

    # If we override any base methods upstream in a child document class,
    # we need to go up one more stack to get the proper comment
    FUNCTION_BLACKLIST = ('find_raw', 'find', 'update', 'insert')

    @classmethod
    def blacklisted(cls, filename):
        return any(filename.endswith(bl) for bl in cls.AUTO_BLACKLIST)

    @classmethod
    def function_blacklisted(cls, function_name):
        return function_name in cls.FUNCTION_BLACKLIST

    @classmethod
    def context(cls, filename):
        if not filename.startswith('/'):
            return filename
        return '/'.join(filename.split('/')[4:])

    @classmethod
    def get_query_comment(cls):
        """
        Retrieves comment from greenlet if called in one, else examine stack
        """
        current_greenlet = greenlet.getcurrent()
        if hasattr(current_greenlet, '__mongoengine_comment__'):
            return current_greenlet.__mongoengine_comment__
        return cls.get_comment()

    @classmethod
    def get_comment(cls):
        try:
            if cls._ip == None:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.connect(('8.8.8.8',80))
                cls._ip = sock.getsockname()[0]
                sock.close()

            last_stacks = traceback.extract_stack(limit=cls.AUTO_FRAME_LIMIT)
            for i in xrange(-3, -len(last_stacks) - 1, -1):
                filename, line, functionname, text = last_stacks[i]

                if cls.blacklisted(filename):
                    continue

                if cls.function_blacklisted(functionname):
                    continue

                msg = '[%s]%s @ %s:%s' % (
                    cls._ip, functionname, cls.context(filename), line
                )
                return msg
            return 'ERROR: Could not retrieve external stack frame'
        except:
            return 'ERROR: Failed to get comment'
