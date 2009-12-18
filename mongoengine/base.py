from queryset import QuerySetManager

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
                self.validate(value)
            except (ValueError, AttributeError, AssertionError), e:
                raise ValidationError('Invalid value for field of type "' +
                                      self.__class__.__name__ + '"')
        elif self.required:
            raise ValidationError('Field "%s" is required' % self.name)
        instance._data[self.name] = value

    def to_python(self, value):
        """Convert a MongoDB-compatible type to a Python type.
        """
        return value

    def to_mongo(self, value):
        """Convert a Python type to a MongoDB-compatible type.
        """
        return self.to_python(value)

    def validate(self, value):
        """Perform validation on a value.
        """
        pass


class ObjectIdField(BaseField):
    """An field wrapper around MongoDB's ObjectIds.
    """
    
    def to_python(self, value):
        return str(value)

    def to_mongo(self, value):
        if not isinstance(value, pymongo.objectid.ObjectId):
            return pymongo.objectid.ObjectId(value)
        return value

    def validate(self, value):
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
        class_name = [name]
        superclasses = {}
        for base in bases:
            # Include all fields present in superclasses
            if hasattr(base, '_fields'):
                doc_fields.update(base._fields)
                class_name.append(base._class_name)
                # Get superclasses from superclass
                superclasses[base._class_name] = base
                superclasses.update(base._superclasses)
        attrs['_class_name'] = '.'.join(reversed(class_name))
        attrs['_superclasses'] = superclasses

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
        new_class.objects = QuerySetManager(new_class)

        return new_class


class BaseDocument(object):

    def __init__(self, **values):
        self._data = {}
        # Assign initial values to instance
        for attr_name, attr_value in self._fields.items():
            if attr_name in values:
                setattr(self, attr_name, values.pop(attr_name))
            else:
                # Use default value if present
                value = getattr(self, attr_name, None)
                if value is None and attr_value.required:
                    raise ValidationError('Field "%s" is required' % attr_name)
                setattr(self, attr_name, value)

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

    def to_mongo(self):
        """Return data dictionary ready for use with MongoDB.
        """
        data = {}
        for field_name, field in self._fields.items():
            value = getattr(self, field_name, None)
            if value is not None:
                data[field_name] = field.to_mongo(value)
        data['_cls'] = self._class_name
        data['_types'] = self._superclasses.keys() + [self._class_name]
        return data
    
    @classmethod
    def _from_son(cls, son):
        """Create an instance of a Document (subclass) from a PyMongo SOM.
        """
        class_name = son[u'_cls']
        data = dict((str(key), value) for key, value in son.items())
        del data['_cls']

        # Return correct subclass for document type
        if class_name != cls._class_name:
            subclasses = cls._get_subclasses()
            if class_name not in subclasses:
                # Type of document is probably more generic than the class
                # that has been queried to return this SON
                return None
            cls = subclasses[class_name]

        for field_name, field in cls._fields.items():
            if field_name in data:
                data[field_name] = field.to_python(data[field_name])

        return cls(**data)
