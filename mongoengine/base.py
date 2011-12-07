from queryset import QuerySet, QuerySetManager
from queryset import DoesNotExist, MultipleObjectsReturned
from queryset import DO_NOTHING

from mongoengine import signals

import sys
import pymongo
import pymongo.objectid
import operator
from functools import partial


class NotRegistered(Exception):
    pass


class InvalidDocumentError(Exception):
    pass


class ValidationError(AssertionError):
    """Validation exception.
    """
    errors = {}
    field_name = None
    _message = None

    def __init__(self, message="", **kwargs):
        self.errors = kwargs.get('errors', {})
        self.field_name = kwargs.get('field_name')
        self.message = message

    def __str__(self):
        return self.message

    def __repr__(self):
        return '%s(%s,)' % (self.__class__.__name__, self.message)

    def __getattribute__(self, name):
        message = super(ValidationError, self).__getattribute__(name)
        if name == 'message' and self.field_name:
            return message + ' ("%s")' % self.field_name
        else:
            return message

    def _get_message(self):
        return self._message

    def _set_message(self, message):
        self._message = message

    message = property(_get_message, _set_message)

    def to_dict(self):
        def build_dict(source):
            errors_dict = {}
            if not source:
                return errors_dict
            if isinstance(source, dict):
                for field_name, error in source.iteritems():
                    errors_dict[field_name] = build_dict(error)
            elif isinstance(source, ValidationError) and source.errors:
                return build_dict(source.errors)
            else:
                return unicode(source)
            return errors_dict
        if not self.errors:
            return {}
        return build_dict(self.errors)


_document_registry = {}


def get_document(name):
    doc = _document_registry.get(name, None)
    if not doc:
        # Possible old style names
        end = ".%s" % name
        possible_match = [k for k in _document_registry.keys() if k.endswith(end)]
        if len(possible_match) == 1:
            doc = _document_registry.get(possible_match.pop(), None)
    if not doc:
        raise NotRegistered("""
            `%s` has not been registered in the document registry.
            Importing the document class automatically registers it, has it
            been imported?
        """.strip() % name)
    return doc


class BaseField(object):
    """A base class for fields in a MongoDB document. Instances of this class
    may be added to subclasses of `Document` to define a document's schema.

    .. versionchanged:: 0.5 - added verbose and help text
    """

    name = None

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
                 validation=None, choices=None, verbose_name=None, help_text=None):
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
        self.verbose_name = verbose_name
        self.help_text = help_text

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

        # Convert lists / values so we can watch for any changes on them
        if isinstance(value, (list, tuple)) and not isinstance(value, BaseList):
            observer = DataObserver(instance, self.name)
            value = BaseList(value, observer)
            instance._data[self.name] = value
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            observer = DataObserver(instance, self.name)
            value = BaseDict(value, observer)
            instance._data[self.name] = value
        return value

    def __set__(self, instance, value):
        """Descriptor for assigning a value to a field in a document.
        """
        instance._data[self.name] = value
        instance._mark_as_changed(self.name)

    def error(self, message="", errors=None, field_name=None):
        """Raises a ValidationError.
        """
        field_name = field_name if field_name else self.name
        raise ValidationError(message, errors=errors, field_name=field_name)

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
        if self.choices:
            if isinstance(self.choices[0], (list, tuple)):
                option_keys = [option_key for option_key, option_value in self.choices]
                if value not in option_keys:
                    self.error('Value must be one of %s' % unicode(option_keys))
            else:
                if value not in self.choices:
                    self.error('Value must be one of %s' % unicode(self.choices))

        # check validation argument
        if self.validation is not None:
            if callable(self.validation):
                if not self.validation(value):
                    self.error('Value does not match custom validation method')
            else:
                raise ValueError('validation argument for "%s" must be a '
                                 'callable.' % self.name)

        self.validate(value)


class ComplexBaseField(BaseField):
    """Handles complex fields, such as lists / dictionaries.

    Allows for nesting of embedded documents inside complex types.
    Handles the lazy dereferencing of a queryset by lazily dereferencing all
    items in a list / dict rather than one at a time.

    .. versionadded:: 0.5
    """

    field = None

    def __get__(self, instance, owner):
        """Descriptor to automatically dereference references.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        from dereference import dereference
        instance._data[self.name] = dereference(
            instance._data.get(self.name), max_depth=1, instance=instance, name=self.name
        )
        return super(ComplexBaseField, self).__get__(instance, owner)

    def to_python(self, value):
        """Convert a MongoDB-compatible type to a Python type.
        """
        from mongoengine import Document

        if isinstance(value, basestring):
            return value

        if hasattr(value, 'to_python'):
            return value.to_python()

        is_list = False
        if not hasattr(value, 'items'):
            try:
                is_list = True
                value = dict([(k, v) for k, v in enumerate(value)])
            except TypeError:  # Not iterable return the value
                return value

        if self.field:
            value_dict = dict([(key, self.field.to_python(item)) for key, item in value.items()])
        else:
            value_dict = {}
            for k, v in value.items():
                if isinstance(v, Document):
                    # We need the id from the saved object to create the DBRef
                    if v.pk is None:
                        self.error('You can only reference documents once they'
                                   ' have been saved to the database')
                    collection = v._get_collection_name()
                    value_dict[k] = pymongo.dbref.DBRef(collection, v.pk)
                elif hasattr(v, 'to_python'):
                    value_dict[k] = v.to_python()
                else:
                    value_dict[k] = self.to_python(v)

        if is_list:  # Convert back to a list
            return [v for k, v in sorted(value_dict.items(), key=operator.itemgetter(0))]
        return value_dict

    def to_mongo(self, value):
        """Convert a Python type to a MongoDB-compatible type.
        """
        from mongoengine import Document

        if isinstance(value, basestring):
            return value

        if hasattr(value, 'to_mongo'):
            return value.to_mongo()

        is_list = False
        if not hasattr(value, 'items'):
            try:
                is_list = True
                value = dict([(k, v) for k, v in enumerate(value)])
            except TypeError:  # Not iterable return the value
                return value

        if self.field:
            value_dict = dict([(key, self.field.to_mongo(item)) for key, item in value.items()])
        else:
            value_dict = {}
            for k, v in value.items():
                if isinstance(v, Document):
                    # We need the id from the saved object to create the DBRef
                    if v.pk is None:
                        self.error('You can only reference documents once they'
                                   ' have been saved to the database')

                    # If its a document that is not inheritable it won't have
                    # _types / _cls data so make it a generic reference allows
                    # us to dereference
                    meta = getattr(v, 'meta', getattr(v, '_meta', {}))
                    if meta and not meta['allow_inheritance'] and not self.field:
                        from fields import GenericReferenceField
                        value_dict[k] = GenericReferenceField().to_mongo(v)
                    else:
                        collection = v._get_collection_name()
                        value_dict[k] = pymongo.dbref.DBRef(collection, v.pk)
                elif hasattr(v, 'to_mongo'):
                    value_dict[k] = v.to_mongo()
                else:
                    value_dict[k] = self.to_mongo(v)

        if is_list:  # Convert back to a list
            return [v for k, v in sorted(value_dict.items(), key=operator.itemgetter(0))]
        return value_dict

    def validate(self, value):
        """If field is provided ensure the value is valid.
        """
        errors = {}
        if self.field:
            if hasattr(value, 'iteritems'):
                sequence = value.iteritems()
            else:
                sequence = enumerate(value)
            for k, v in sequence:
                try:
                    self.field.validate(v)
                except (ValidationError, AssertionError), error:
                    if hasattr(error, 'errors'):
                        errors[k] = error.errors
                    else:
                        errors[k] = error
            if errors:
                field_class = self.field.__class__.__name__
                self.error('Invalid %s item (%s)' % (field_class, value),
                           errors=errors)
        # Don't allow empty values if required
        if self.required and not value:
            self.error('Field is required and cannot be empty')

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)

    def lookup_member(self, member_name):
        if self.field:
            return self.field.lookup_member(member_name)
        return None

    def _set_owner_document(self, owner_document):
        if self.field:
            self.field.owner_document = owner_document
        self._owner_document = owner_document

    def _get_owner_document(self, owner_document):
        self._owner_document = owner_document

    owner_document = property(_get_owner_document, _set_owner_document)


class BaseDynamicField(BaseField):
    """Used by :class:`~mongoengine.DynamicDocument` to handle dynamic data"""

    def to_mongo(self, value):
        """Convert a Python type to a MongoDBcompatible type.
        """

        if isinstance(value, basestring):
            return value

        if hasattr(value, 'to_mongo'):
            return value.to_mongo()

        if not isinstance(value, (dict, list, tuple)):
            return value

        is_list = False
        if not hasattr(value, 'items'):
            is_list = True
            value = dict([(k, v) for k, v in enumerate(value)])

        data = {}
        for k, v in value.items():
            data[k] = self.to_mongo(v)

        if is_list:  # Convert back to a list
            value = [v for k, v in sorted(data.items(), key=operator.itemgetter(0))]
        else:
            value = data
        return value

    def lookup_member(self, member_name):
        return member_name

    def prepare_query_value(self, op, value):
        if isinstance(value, basestring):
            from mongoengine.fields import StringField
            return StringField().prepare_query_value(op, value)
        return self.to_mongo(value)


class ObjectIdField(BaseField):
    """An field wrapper around MongoDB's ObjectIds.
    """

    def to_python(self, value):
        return value

    def to_mongo(self, value):
        if not isinstance(value, pymongo.objectid.ObjectId):
            try:
                return pymongo.objectid.ObjectId(unicode(value))
            except Exception, e:
                # e.message attribute has been deprecated since Python 2.6
                self.error(unicode(e))
        return value

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)

    def validate(self, value):
        try:
            pymongo.objectid.ObjectId(unicode(value))
        except:
            self.error('Invalid Object ID')


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
                # Get superclasses from superclass
                superclasses[base._class_name] = base
                superclasses.update(base._superclasses)
            else:  # Add any mixin fields
                attrs.update(dict([(k, v) for k, v in base.__dict__.items()
                                    if issubclass(v.__class__, BaseField)]))

            if hasattr(base, '_meta') and not base._meta.get('abstract'):
                # Ensure that the Document class may be subclassed -
                # inheritance may be disabled to remove dependency on
                # additional fields _cls and _types
                class_name.append(base._class_name)
                if base._meta.get('allow_inheritance', True) == False:
                    raise ValueError('Document %s may not be subclassed' %
                                     base.__name__)
                else:
                    simple_class = False

        doc_class_name = '.'.join(reversed(class_name))
        meta = attrs.get('_meta', attrs.get('meta', {}))

        if 'allow_inheritance' not in meta:
            meta['allow_inheritance'] = True

        # Only simple classes - direct subclasses of Document - may set
        # allow_inheritance to False
        if not simple_class and not meta['allow_inheritance'] and not meta['abstract']:
            raise ValueError('Only direct subclasses of Document may set '
                             '"allow_inheritance" to False')
        attrs['_meta'] = meta
        attrs['_class_name'] = doc_class_name
        attrs['_superclasses'] = superclasses

        # Add the document's fields to the _fields attribute
        field_names = {}
        for attr_name, attr_value in attrs.items():
            if hasattr(attr_value, "__class__") and \
               issubclass(attr_value.__class__, BaseField):
                attr_value.name = attr_name
                if not attr_value.db_field:
                    attr_value.db_field = attr_name
                doc_fields[attr_name] = attr_value
                field_names[attr_value.db_field] = field_names.get(attr_value.db_field, 0) + 1

        duplicate_db_fields = [k for k, v in field_names.items() if v > 1]
        if duplicate_db_fields:
            raise InvalidDocumentError("Multiple db_fields defined for: %s " % ", ".join(duplicate_db_fields))
        attrs['_fields'] = doc_fields
        attrs['_db_field_map'] = dict([(k, v.db_field) for k, v in doc_fields.items() if k != v.db_field])
        attrs['_reverse_db_field_map'] = dict([(v, k) for k, v in attrs['_db_field_map'].items()])

        from mongoengine import Document, EmbeddedDocument, DictField

        new_class = super_new(cls, name, bases, attrs)
        for field in new_class._fields.values():
            field.owner_document = new_class

            delete_rule = getattr(field, 'reverse_delete_rule', DO_NOTHING)
            f = field
            if isinstance(f, ComplexBaseField) and hasattr(f, 'field'):
                delete_rule = getattr(f.field, 'reverse_delete_rule', DO_NOTHING)
                if isinstance(f, DictField) and delete_rule != DO_NOTHING:
                    raise InvalidDocumentError("Reverse delete rules are not supported for %s (field: %s)" % (field.__class__.__name__, field.name))
                f = field.field

            if delete_rule != DO_NOTHING:
                if issubclass(new_class, EmbeddedDocument):
                    raise InvalidDocumentError("Reverse delete rules are not supported for EmbeddedDocuments (field: %s)" % field.name)
                f.document_type.register_delete_rule(new_class, field.name, delete_rule)

            if field.name and hasattr(Document, field.name) and EmbeddedDocument not in new_class.mro():
                raise InvalidDocumentError("%s is a document method and not a valid field name" % field.name)

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
        _document_registry[doc_class_name] = new_class

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
                if hasattr(b, '_meta') and not b._meta.get('abstract', False)]
            if non_abstract_bases:
                raise ValueError("Abstract document cannot have non-abstract base")
            return super_new(cls, name, bases, attrs)

        collection = ''.join('_%s' % c if c.isupper() else c for c in name).strip('_').lower()

        id_field = None
        abstract_base_indexes = []
        base_indexes = []
        base_meta = {}

        # Subclassed documents inherit collection from superclass
        for base in bases:
            if hasattr(base, '_meta'):
                if 'collection' in attrs.get('meta', {}) and not base._meta.get('abstract', False):
                    import warnings
                    msg = "Trying to set a collection on a subclass (%s)" % name
                    warnings.warn(msg, SyntaxWarning)
                    del(attrs['meta']['collection'])
                if base._get_collection_name():
                    collection = base._get_collection_name()
                # Propagate index options.
                for key in ('index_background', 'index_drop_dups', 'index_opts'):
                    if key in base._meta:
                        base_meta[key] = base._meta[key]

                id_field = id_field or base._meta.get('id_field')
                if base._meta.get('abstract', False):
                    abstract_base_indexes += base._meta.get('indexes', [])
                else:
                    base_indexes += base._meta.get('indexes', [])
                # Propagate 'allow_inheritance'
                if 'allow_inheritance' in base._meta:
                    base_meta['allow_inheritance'] = base._meta['allow_inheritance']
                if 'queryset_class' in base._meta:
                    base_meta['queryset_class'] = base._meta['queryset_class']
            try:
                base_meta['objects'] = base.__getattribute__(base, 'objects')
            except AttributeError:
                pass

        meta = {
            'abstract': False,
            'collection': collection,
            'max_documents': None,
            'max_size': None,
            'ordering': [],  # default ordering applied at runtime
            'indexes': [],  # indexes to be ensured at runtime
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

        collection = attrs['_meta'].get('collection', None)
        if callable(collection):
            new_class._meta['collection'] = collection(new_class)

        # Provide a default queryset unless one has been manually provided
        manager = attrs.get('objects', meta.get('objects', QuerySetManager()))
        if hasattr(manager, 'queryset_class'):
            meta['queryset_class'] = manager.queryset_class
        new_class.objects = manager

        indicies = meta['indexes'] + abstract_base_indexes
        user_indexes = [QuerySet._build_index_spec(new_class, spec)
                        for spec in indicies] + base_indexes
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
            if field.__class__.__name__ == "EmbeddedDocumentField" and field.document_type != new_class:
                field_namespace = "%s." % field_name
                unique_indexes += cls._unique_with_indexes(field.document_type,
                                    field_namespace)

        return unique_indexes


class BaseDocument(object):

    _dynamic = False

    def __init__(self, **values):
        signals.pre_init.send(self.__class__, document=self, values=values)

        self._data = {}
        self._initialised = False

        # Assign default values to instance
        for attr_name, field in self._fields.items():
            value = getattr(self, attr_name, None)
            setattr(self, attr_name, value)

        # Set passed values after initialisation
        if self._dynamic:
            self._dynamic_fields = {}
            dynamic_data = {}
            for key, value in values.items():
                if key in self._fields or key == '_id':
                    setattr(self, key, value)
                elif self._dynamic:
                    dynamic_data[key] = value
        else:
            for key, value in values.items():
                setattr(self, key, value)

        # Set any get_fieldname_display methods
        self.__set_field_display()
        # Flag initialised
        self._initialised = True

        if self._dynamic:
            for key, value in dynamic_data.items():
                setattr(self, key, value)
        signals.post_init.send(self.__class__, document=self)

    def __setattr__(self, name, value):
        # Handle dynamic data only if an initialised dynamic document
        if self._dynamic and getattr(self, '_initialised', False):

            field = None
            if not hasattr(self, name) and not name.startswith('_'):
                field = BaseDynamicField(db_field=name)
                field.name = name
                self._dynamic_fields[name] = field

            if not name.startswith('_'):
                value = self.__expand_dynamic_values(name, value)

            # Handle marking data as changed
            if name in self._dynamic_fields:
                self._data[name] = value
                if hasattr(self, '_changed_fields'):
                    self._mark_as_changed(name)

        # Handle None values for required fields
        if value is None and name in getattr(self, '_fields', {}):
            self._data[name] = value
            if hasattr(self, '_changed_fields'):
                self._mark_as_changed(name)
            return
        super(BaseDocument, self).__setattr__(name, value)

    def __expand_dynamic_values(self, name, value):
        """expand any dynamic values to their correct types / values"""
        if not isinstance(value, (dict, list, tuple)):
            return value

        is_list = False
        if not hasattr(value, 'items'):
            is_list = True
            value = dict([(k, v) for k, v in enumerate(value)])

        if not is_list and '_cls' in value:
            cls = get_document(value['_cls'])
            value = cls(**value)
            value._dynamic = True
            value._changed_fields = []
            return value

        data = {}
        for k, v in value.items():
            key = name if is_list else k
            data[k] = self.__expand_dynamic_values(key, v)

        if is_list:  # Convert back to a list
            data_items = sorted(data.items(), key=operator.itemgetter(0))
            value = [v for k, v in data_items]
        else:
            value = data

        # Convert lists / values so we can watch for any changes on them
        if isinstance(value, (list, tuple)) and not isinstance(value, BaseList):
            observer = DataObserver(self, name)
            value = BaseList(value, observer)
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            observer = DataObserver(self, name)
            value = BaseDict(value, observer)

        return value

    def validate(self):
        """Ensure that all fields' values are valid and that required fields
        are present.
        """
        # Get a list of tuples of field names and their current values
        fields = [(field, getattr(self, name))
                  for name, field in self._fields.items()]

        # Ensure that each field is matched to a valid value
        errors = {}
        for field, value in fields:
            if value is not None:
                try:
                    field._validate(value)
                except ValidationError, error:
                    errors[field.name] = error.errors or error
                except (ValueError, AttributeError, AssertionError), error:
                    errors[field.name] = error
            elif field.required:
                errors[field.name] = ValidationError('Field is required',
                                                     field_name=field.name)
        if errors:
            raise ValidationError('Errors encountered validating document',
                                  errors=errors)

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
        if '_id' in data and data['_id'] is None:
            del data['_id']

        if not self._dynamic:
            return data

        for name, field in self._dynamic_fields.items():
            data[name] = field.to_mongo(self._data.get(name, None))
        return data

    @classmethod
    def _get_collection_name(cls):
        """Returns the collection name for this class.
        """
        return cls._meta.get('collection', None)

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
            cls = get_document(class_name)

        changed_fields = []
        for field_name, field in cls._fields.items():
            if field.db_field in data:
                value = data[field.db_field]
                data[field_name] = (value if value is None
                                    else field.to_python(value))
            elif field.default:
                default = field.default
                if callable(default):
                    default = default()
                if isinstance(default, BaseDocument):
                    changed_fields.append(field_name)

        obj = cls(**data)
        obj._changed_fields = changed_fields
        return obj

    def _mark_as_changed(self, key):
        """Marks a key as explicitly changed by the user
        """
        if not key:
            return
        key = self._db_field_map.get(key, key)
        if hasattr(self, '_changed_fields') and key not in self._changed_fields:
            self._changed_fields.append(key)

    def _get_changed_fields(self, key='', inspected=None):
        """Returns a list of all fields that have explicitly been changed.
        """
        from mongoengine import EmbeddedDocument, DynamicEmbeddedDocument
        _changed_fields = []
        _changed_fields += getattr(self, '_changed_fields', [])

        inspected = inspected or set()
        if hasattr(self, 'id'):
            if self.id in inspected:
                return _changed_fields
            inspected.add(self.id)

        field_list = self._fields.copy()
        if self._dynamic:
            field_list.update(self._dynamic_fields)

        for field_name in field_list:
            db_field_name = self._db_field_map.get(field_name, field_name)
            key = '%s.' % db_field_name
            field = getattr(self, field_name, None)
            if hasattr(field, 'id'):
                if field.id in inspected:
                    continue
                inspected.add(field.id)

            if isinstance(field, (EmbeddedDocument, DynamicEmbeddedDocument)) and db_field_name not in _changed_fields:  # Grab all embedded fields that have been changed
                _changed_fields += ["%s%s" % (key, k) for k in field._get_changed_fields(key, inspected) if k]
            elif isinstance(field, (list, tuple, dict)) and db_field_name not in _changed_fields:  # Loop list / dict fields as they contain documents
                # Determine the iterator to use
                if not hasattr(field, 'items'):
                    iterator = enumerate(field)
                else:
                    iterator = field.iteritems()
                for index, value in iterator:
                    if not hasattr(value, '_get_changed_fields'):
                        continue
                    list_key = "%s%s." % (key, index)
                    _changed_fields += ["%s%s" % (list_key, k) for k in value._get_changed_fields(list_key, inspected) if k]
        return _changed_fields

    def _delta(self):
        """Returns the delta (set, unset) of the changes for a document.
        Gets any values that have been explicitly changed.
        """
        # Handles cases where not loaded from_son but has _id
        doc = self.to_mongo()
        set_fields = self._get_changed_fields()
        set_data = {}
        unset_data = {}
        parts = []
        if hasattr(self, '_changed_fields'):
            set_data = {}
            # Fetch each set item from its path
            for path in set_fields:
                parts = path.split('.')
                d = doc
                for p in parts:
                    if hasattr(d, '__getattr__'):
                        d = getattr(p, d)
                    elif p.isdigit():
                        d = d[int(p)]
                    else:
                        d = d.get(p)
                set_data[path] = d
        else:
            set_data = doc
            if '_id' in set_data:
                del(set_data['_id'])

        # Determine if any changed items were actually unset.
        for path, value in set_data.items():
            if value or isinstance(value, bool):
                continue

            # If we've set a value that ain't the default value dont unset it.
            default = None
            if self._dynamic and len(parts) and parts[0] in self._dynamic_fields:
                del(set_data[path])
                unset_data[path] = 1
                continue
            elif path in self._fields:
                default = self._fields[path].default
            else:  # Perform a full lookup for lists / embedded lookups
                d = self
                parts = path.split('.')
                db_field_name = parts.pop()
                for p in parts:
                    if p.isdigit():
                        d = d[int(p)]
                    elif hasattr(d, '__getattribute__') and not isinstance(d, dict):
                        real_path = d._reverse_db_field_map.get(p, p)
                        d = getattr(d, real_path)
                    else:
                        d = d.get(p)

                if hasattr(d, '_fields'):
                    field_name = d._reverse_db_field_map.get(db_field_name,
                                                             db_field_name)

                    if field_name in d._fields:
                        default = d._fields.get(field_name).default
                    else:
                        default = None

            if default is not None:
                if callable(default):
                    default = default()
            if default != value:
                continue

            del(set_data[path])
            unset_data[path] = 1
        return set_data, unset_data

    @classmethod
    def _geo_indices(cls, inspected=None):
        inspected = inspected or []
        geo_indices = []
        inspected.append(cls)
        for field in cls._fields.values():
            if hasattr(field, 'document_type'):
                field_cls = field.document_type
                if field_cls in inspected:
                    continue
                if hasattr(field_cls, '_geo_indices'):
                    geo_indices += field_cls._geo_indices(inspected)
            elif field._geo_index:
                geo_indices.append(field)
        return geo_indices

    def __getstate__(self):
        removals = ["get_%s_display" % k for k, v in self._fields.items() if v.choices]
        for k in removals:
            if hasattr(self, k):
                delattr(self, k)
        return self.__dict__

    def __setstate__(self, __dict__):
        self.__dict__ = __dict__
        self.__set_field_display()

    def __set_field_display(self):
        for attr_name, field in self._fields.items():
            if field.choices:  # dynamically adds a way to get the display value for a field with choices
                setattr(self, 'get_%s_display' % attr_name, partial(self.__get_field_display, field=field))

    def __get_field_display(self, field):
        """Returns the display value for a choice field"""
        value = getattr(self, field.name)
        if field.choices and isinstance(field.choices[0], (list, tuple)):
            return dict(field.choices).get(value, value)
        return value

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
            u = unicode(self).encode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            u = '[Bad Unicode data]'
        return '<%s: %s>' % (self.__class__.__name__, u)

    def __str__(self):
        if hasattr(self, '__unicode__'):
            return unicode(self).encode('utf-8')
        return '%s object' % self.__class__.__name__

    def __eq__(self, other):
        if isinstance(other, self.__class__) and hasattr(other, 'id'):
            if self.id == other.id:
                return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.pk is None:
            # For new object
            return super(BaseDocument, self).__hash__()
        else:
            return hash(self.pk)


class DataObserver(object):

    __slots__ = ["instance", "name"]

    def __init__(self, instance, name):
        self.instance = instance
        self.name = name

    def updated(self):
        if hasattr(self.instance, '_mark_as_changed'):
            self.instance._mark_as_changed(self.name)


class BaseList(list):
    """A special list so we can watch any changes
    """

    def __init__(self, list_items, observer):
        self.observer = observer
        super(BaseList, self).__init__(list_items)

    def __setitem__(self, *args, **kwargs):
        self._updated()
        super(BaseList, self).__setitem__(*args, **kwargs)

    def __delitem__(self, *args, **kwargs):
        self._updated()
        super(BaseList, self).__delitem__(*args, **kwargs)

    def __getstate__(self):
        self.observer = None
        return self

    def __setstate__(self, state):
        self = state

    def append(self, *args, **kwargs):
        self._updated()
        return super(BaseList, self).append(*args, **kwargs)

    def extend(self, *args, **kwargs):
        self._updated()
        return super(BaseList, self).extend(*args, **kwargs)

    def insert(self, *args, **kwargs):
        self._updated()
        return super(BaseList, self).insert(*args, **kwargs)

    def pop(self, *args, **kwargs):
        self._updated()
        return super(BaseList, self).pop(*args, **kwargs)

    def remove(self, *args, **kwargs):
        self._updated()
        return super(BaseList, self).remove(*args, **kwargs)

    def reverse(self, *args, **kwargs):
        self._updated()
        return super(BaseList, self).reverse(*args, **kwargs)

    def sort(self, *args, **kwargs):
        self._updated()
        return super(BaseList, self).sort(*args, **kwargs)

    def _updated(self):
        try:
            self.observer.updated()
        except AttributeError:
            pass


class BaseDict(dict):
    """A special dict so we can watch any changes
    """

    def __init__(self, dict_items, observer):
        self.observer = observer
        super(BaseDict, self).__init__(dict_items)

    def __setitem__(self, *args, **kwargs):
        self._updated()
        super(BaseDict, self).__setitem__(*args, **kwargs)

    def __delete__(self, *args, **kwargs):
        self._updated()
        super(BaseDict, self).__delete__(*args, **kwargs)

    def __delitem__(self, *args, **kwargs):
        self._updated()
        super(BaseDict, self).__delitem__(*args, **kwargs)

    def __delattr__(self, *args, **kwargs):
        self._updated()
        super(BaseDict, self).__delattr__(*args, **kwargs)

    def __getstate__(self):
        self.observer = None
        return self

    def __setstate__(self, state):
        self = state

    def clear(self, *args, **kwargs):
        self._updated()
        super(BaseDict, self).clear(*args, **kwargs)

    def pop(self, *args, **kwargs):
        self._updated()
        super(BaseDict, self).clear(*args, **kwargs)

    def popitem(self, *args, **kwargs):
        self._updated()
        super(BaseDict, self).clear(*args, **kwargs)

    def _updated(self):
        try:
            self.observer.updated()
        except AttributeError:
            pass

if sys.version_info < (2, 5):
    # Prior to Python 2.5, Exception was an old-style class
    import types
    def subclass_exception(name, parents, unused):
        import types
        return types.ClassType(name, parents, {})
else:
    def subclass_exception(name, parents, module):
        return type(name, parents, {'__module__': module})
