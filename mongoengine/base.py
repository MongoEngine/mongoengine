from queryset import QuerySet, QuerySetManager
from queryset import DoesNotExist, MultipleObjectsReturned
from queryset import DO_NOTHING

from mongoengine import signals

import sys
import pymongo
import pymongo.objectid
from operator import itemgetter
from functools import partial


class NotRegistered(Exception):
    pass


class ValidationError(Exception):
    pass


_document_registry = {}

def get_document(name):
    if name not in _document_registry:
        raise NotRegistered("""
            `%s` has not been registered in the document registry.
            Importing the document class automatically registers it, has it
            been imported?
        """.strip() % name)
    return _document_registry[name]


class BaseField(object):
    """A base class for fields in a MongoDB document. Instances of this class
    may be added to subclasses of `Document` to define a document's schema.
    """

    # Fields may have _types inserted into indexes by default
    _index_with_types = True
    _geo_index = False

    # These track each time a Field instance is created. Used to retain order.
    # The auto_creation_counter is used for fields that MongoEngine implicitly
    # creates, creation_counter is used for all user-specified fields.
    creation_counter = 0
    auto_creation_counter = -1

    def __init__(self, db_field=None, name=None, required=False, default=None,
                 unique=False, unique_with=None, primary_key=False,
                 validation=None, choices=None):
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

        # Adjust the appropriate creation counter, and save our local copy.
        if self.db_field == '_id':
            self.creation_counter = BaseField.auto_creation_counter
            BaseField.auto_creation_counter -= 1
        else:
            self.creation_counter = BaseField.creation_counter
            BaseField.creation_counter += 1

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
            value = self.default
            # Allow callable default values
            if callable(value):
                value = value()
        return value

    def __set__(self, instance, value):
        """Descriptor for assigning a value to a field in a document.
        """
        instance._data[self.name] = value

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

    def _validate(self, value):
        # check choices
        if self.choices is not None:
            option_keys = [option_key for option_key, option_value in self.choices]
            if value not in option_keys:
                raise ValidationError("Value must be one of %s." % unicode(option_keys))

        # check validation argument
        if self.validation is not None:
            if callable(self.validation):
                if not self.validation(value):
                    raise ValidationError('Value does not match custom' \
                                          'validation method.')
            else:
                raise ValueError('validation argument must be a callable.')

        self.validate(value)


class DereferenceBaseField(BaseField):
    """Handles the lazy dereferencing of a queryset.  Will dereference all
    items in a list / dict rather than one at a time.
    """

    def __get__(self, instance, owner):
        """Descriptor to automatically dereference references.
        """
        from fields import ReferenceField, GenericReferenceField
        from connection import _get_db

        if instance is None:
            # Document class being used rather than a document object
            return self

        # Get value from document instance if available
        value_list = instance._data.get(self.name)
        if not value_list:
            return super(DereferenceBaseField, self).__get__(instance, owner)

        is_list = False
        if not hasattr(value_list, 'items'):
            is_list = True
            value_list = dict([(k,v) for k,v in enumerate(value_list)])

        if isinstance(self.field, ReferenceField) and value_list:
            db = _get_db()
            dbref = {}
            collections = {}

            for k, v in value_list.items():
                dbref[k] = v
                # Save any DBRefs
                if isinstance(v, (pymongo.dbref.DBRef)):
                    collections.setdefault(v.collection, []).append((k, v))

            # For each collection get the references
            for collection, dbrefs in collections.items():
                id_map = dict([(v.id, k) for k, v in dbrefs])
                references = db[collection].find({'_id': {'$in': id_map.keys()}})
                for ref in references:
                    key = id_map[ref['_id']]
                    dbref[key] = get_document(ref['_cls'])._from_son(ref)

            if is_list:
                dbref = [v for k,v in sorted(dbref.items(), key=itemgetter(0))]
            instance._data[self.name] = dbref

        # Get value from document instance if available
        if isinstance(self.field, GenericReferenceField) and value_list:
            db = _get_db()
            value_list = [(k,v) for k,v in value_list.items()]
            dbref = {}
            classes = {}

            for k, v in value_list:
                dbref[k] = v
                # Save any DBRefs
                if isinstance(v, (dict, pymongo.son.SON)):
                    classes.setdefault(v['_cls'], []).append((k, v))

            # For each collection get the references
            for doc_cls, dbrefs in classes.items():
                id_map = dict([(v['_ref'].id, k) for k, v in dbrefs])
                doc_cls = get_document(doc_cls)
                collection = doc_cls._meta['collection']
                references = db[collection].find({'_id': {'$in': id_map.keys()}})

                for ref in references:
                    key = id_map[ref['_id']]
                    dbref[key] = doc_cls._from_son(ref)

            if is_list:
                dbref = [v for k,v in sorted(dbref.items(), key=itemgetter(0))]

            instance._data[self.name] = dbref

        return super(DereferenceBaseField, self).__get__(instance, owner)



class ObjectIdField(BaseField):
    """An field wrapper around MongoDB's ObjectIds.
    """

    def to_python(self, value):
        return value
        # return unicode(value)

    def to_mongo(self, value):
        if not isinstance(value, pymongo.objectid.ObjectId):
            try:
                return pymongo.objectid.ObjectId(unicode(value))
            except Exception, e:
                #e.message attribute has been deprecated since Python 2.6
                raise ValidationError(unicode(e))
        return value

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)

    def validate(self, value):
        try:
            pymongo.objectid.ObjectId(unicode(value))
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
        class_name = [name]
        superclasses = {}
        simple_class = True
        for base in bases:
            # Include all fields present in superclasses
            if hasattr(base, '_fields'):
                doc_fields.update(base._fields)
                class_name.append(base._class_name)
                # Get superclasses from superclass
                superclasses[base._class_name] = base
                superclasses.update(base._superclasses)

            if hasattr(base, '_meta') and not base._meta.get('abstract'):
                # Ensure that the Document class may be subclassed -
                # inheritance may be disabled to remove dependency on
                # additional fields _cls and _types
                if base._meta.get('allow_inheritance', True) == False:
                    raise ValueError('Document %s may not be subclassed' %
                                     base.__name__)
                else:
                    simple_class = False

        meta = attrs.get('_meta', attrs.get('meta', {}))

        if 'allow_inheritance' not in meta:
            meta['allow_inheritance'] = True

        # Only simple classes - direct subclasses of Document - may set
        # allow_inheritance to False
        if not simple_class and not meta['allow_inheritance'] and not meta['abstract']:
            raise ValueError('Only direct subclasses of Document may set '
                             '"allow_inheritance" to False')
        attrs['_meta'] = meta

        attrs['_class_name'] = '.'.join(reversed(class_name))
        attrs['_superclasses'] = superclasses

        # Add the document's fields to the _fields attribute
        for attr_name, attr_value in attrs.items():
            if hasattr(attr_value, "__class__") and \
               issubclass(attr_value.__class__, BaseField):
                attr_value.name = attr_name
                if not attr_value.db_field:
                    attr_value.db_field = attr_name
                doc_fields[attr_name] = attr_value
        attrs['_fields'] = doc_fields

        new_class = super_new(cls, name, bases, attrs)
        for field in new_class._fields.values():
            field.owner_document = new_class
            delete_rule = getattr(field, 'reverse_delete_rule', DO_NOTHING)
            if delete_rule != DO_NOTHING:
                field.document_type.register_delete_rule(new_class, field.name,
                        delete_rule)

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

        global _document_registry
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
        #
        # Also assume a class is abstract if it has abstract set to True in
        # its meta dictionary. This allows custom Document superclasses.
        if (attrs.get('__metaclass__') == TopLevelDocumentMetaclass or
            ('meta' in attrs and attrs['meta'].get('abstract', False))):
            # Make sure no base class was non-abstract
            non_abstract_bases = [b for b in bases
                if hasattr(b,'_meta') and not b._meta.get('abstract', False)]
            if non_abstract_bases:
                raise ValueError("Abstract document cannot have non-abstract base")
            return super_new(cls, name, bases, attrs)

        collection = name.lower()

        id_field = None
        base_indexes = []
        base_meta = {}

        # Subclassed documents inherit collection from superclass
        for base in bases:
            if hasattr(base, '_meta'):
                if 'collection' in base._meta:
                    collection = base._meta['collection']

                # Propagate index options.
                for key in ('index_background', 'index_drop_dups', 'index_opts'):
                    if key in base._meta:
                        base_meta[key] = base._meta[key]

                id_field = id_field or base._meta.get('id_field')
                base_indexes += base._meta.get('indexes', [])
                # Propagate 'allow_inheritance'
                if 'allow_inheritance' in base._meta:
                    base_meta['allow_inheritance'] = base._meta['allow_inheritance']

        meta = {
            'abstract': False,
            'collection': collection,
            'max_documents': None,
            'max_size': None,
            'ordering': [], # default ordering applied at runtime
            'indexes': [], # indexes to be ensured at runtime
            'id_field': id_field,
            'index_background': False,
            'index_drop_dups': False,
            'index_opts': {},
            'queryset_class': QuerySet,
            'delete_rules': {},
            'allow_inheritance': True
        }
        meta.update(base_meta)

        # Apply document-defined meta options
        meta.update(attrs.get('meta', {}))
        attrs['_meta'] = meta

        # Set up collection manager, needs the class to have fields so use
        # DocumentMetaclass before instantiating CollectionManager object
        new_class = super_new(cls, name, bases, attrs)

        # Provide a default queryset unless one has been manually provided
        manager = attrs.get('objects', QuerySetManager())
        if hasattr(manager, 'queryset_class'):
            meta['queryset_class'] = manager.queryset_class
        new_class.objects = manager

        user_indexes = [QuerySet._build_index_spec(new_class, spec)
                        for spec in meta['indexes']] + base_indexes
        new_class._meta['indexes'] = user_indexes

        unique_indexes = cls._unique_with_indexes(new_class)
        new_class._meta['unique_indexes'] = unique_indexes

        for field_name, field in new_class._fields.items():
            # Check for custom primary key
            if field.primary_key:
                current_pk = new_class._meta['id_field']
                if current_pk and current_pk != field_name:
                    raise ValueError('Cannot override primary key field')

                if not current_pk:
                    new_class._meta['id_field'] = field_name
                    # Make 'Document.id' an alias to the real primary key field
                    new_class.id = field

        if not new_class._meta['id_field']:
            new_class._meta['id_field'] = 'id'
            new_class._fields['id'] = ObjectIdField(db_field='_id')
            new_class.id = new_class._fields['id']

        return new_class

    @classmethod
    def _unique_with_indexes(cls, new_class, namespace=""):
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
                index = [("%s%s" % (namespace, f), pymongo.ASCENDING) for f in unique_fields]
                unique_indexes.append(index)

            # Grab any embedded document field unique indexes
            if field.__class__.__name__ == "EmbeddedDocumentField":
                field_namespace = "%s." % field_name
                unique_indexes += cls._unique_with_indexes(field.document_type,
                                    field_namespace)

        return unique_indexes


class BaseDocument(object):

    def __init__(self, **values):
        signals.pre_init.send(self, values=values)

        self._data = {}
        # Assign default values to instance
        for attr_name, field in self._fields.items():
            if field.choices:  # dynamically adds a way to get the display value for a field with choices
                setattr(self, 'get_%s_display' % attr_name, partial(self._get_FIELD_display, field=field))

            # Use default value if present
            value = getattr(self, attr_name, None)
            setattr(self, attr_name, value)
        # Assign initial values to instance
        for attr_name in values.keys():
            try:
                setattr(self, attr_name, values.pop(attr_name))
            except AttributeError:
                pass

        signals.post_init.send(self)

    def _get_FIELD_display(self, field):
        """Returns the display value for a choice field"""
        value = getattr(self, field.name)
        return dict(field.choices).get(value, value)

    def validate(self):
        """Ensure that all fields' values are valid and that required fields
        are present.
        """
        # Get a list of tuples of field names and their current values
        fields = [(field, getattr(self, name))
                  for name, field in self._fields.items()]

        # Ensure that each field is matched to a valid value
        for field, value in fields:
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
        return len(self._data)

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
        for field_name, field in self._fields.items():
            value = getattr(self, field_name, None)
            if value is not None:
                data[field.db_field] = field.to_mongo(value)
        # Only add _cls and _types if allow_inheritance is not False
        if not (hasattr(self, '_meta') and
                self._meta.get('allow_inheritance', True) == False):
            data['_cls'] = self._class_name
            data['_types'] = self._superclasses.keys() + [self._class_name]
        if data.has_key('_id') and data['_id'] is None:
            del data['_id']
        return data

    @classmethod
    def _from_son(cls, son):
        """Create an instance of a Document (subclass) from a PyMongo SON.
        """
        # get the class name from the document, falling back to the given
        # class if unavailable
        class_name = son.get(u'_cls', cls._class_name)

        data = dict((str(key), value) for key, value in son.items())

        if '_types' in data:
            del data['_types']

        if '_cls' in data:
            del data['_cls']

        # Return correct subclass for document type
        if class_name != cls._class_name:
            subclasses = cls._get_subclasses()
            if class_name not in subclasses:
                # Type of document is probably more generic than the class
                # that has been queried to return this SON
                return None
            cls = subclasses[class_name]

        present_fields = data.keys()

        for field_name, field in cls._fields.items():
            if field.db_field in data:
                value = data[field.db_field]
                data[field_name] = (value if value is None
                                    else field.to_python(value))

        obj = cls(**data)
        obj._present_fields = present_fields
        return obj

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
    import types
    def subclass_exception(name, parents, unused):
        import types
        return types.ClassType(name, parents, {})
else:
    def subclass_exception(name, parents, module):
        return type(name, parents, {'__module__': module})
