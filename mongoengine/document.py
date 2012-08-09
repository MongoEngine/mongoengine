import pymongo

from bson.dbref import DBRef

from mongoengine import signals
from base import (DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument,
                  BaseDict, BaseList, ValidationError, get_document)
from queryset import OperationError
from connection import get_db, DEFAULT_CONNECTION_NAME
import time
from bson import SON, ObjectId


__all__ = ['Document', 'EmbeddedDocument', 'DynamicDocument',
           'DynamicEmbeddedDocument', 'OperationError', 'InvalidCollectionError']

class InvalidCollectionError(Exception):
    pass


class EmbeddedDocument(BaseDocument):
    """A :class:`~mongoengine.Document` that isn't stored in its own
    collection.  :class:`~mongoengine.EmbeddedDocument`\ s should be used as
    fields on :class:`~mongoengine.Document`\ s through the
    :class:`~mongoengine.EmbeddedDocumentField` field type.
    """

    __metaclass__ = DocumentMetaclass

    def __init__(self, *args, **kwargs):
        super(EmbeddedDocument, self).__init__(*args, **kwargs)
        self._changed_fields = []

    def __delattr__(self, *args, **kwargs):
        """Handle deletions of fields"""
        field_name = args[0]
        if field_name in self._fields:
            default = self._fields[field_name].default
            if callable(default):
                default = default()
            setattr(self, field_name, default)
        else:
            super(EmbeddedDocument, self).__delattr__(*args, **kwargs)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._data == other._data
        return False


class Document(BaseDocument):
    """The base class used for defining the structure and properties of
    collections of documents stored in MongoDB. Inherit from this class, and
    add fields as class attributes to define a document's structure.
    Individual documents may then be created by making instances of the
    :class:`~mongoengine.Document` subclass.

    By default, the MongoDB collection used to store documents created using a
    :class:`~mongoengine.Document` subclass will be the name of the subclass
    converted to lowercase. A different collection may be specified by
    providing :attr:`collection` to the :attr:`meta` dictionary in the class
    definition.

    A :class:`~mongoengine.Document` subclass may be itself subclassed, to
    create a specialised version of the document that will be stored in the
    same collection. To facilitate this behaviour, `_cls` and `_types`
    fields are added to documents (hidden though the MongoEngine interface
    though). To disable this behaviour and remove the dependence on the
    presence of `_cls` and `_types`, set :attr:`allow_inheritance` to
    ``False`` in the :attr:`meta` dictionary.

    A :class:`~mongoengine.Document` may use a **Capped Collection** by
    specifying :attr:`max_documents` and :attr:`max_size` in the :attr:`meta`
    dictionary. :attr:`max_documents` is the maximum number of documents that
    is allowed to be stored in the collection, and :attr:`max_size` is the
    maximum size of the collection in bytes. If :attr:`max_size` is not
    specified and :attr:`max_documents` is, :attr:`max_size` defaults to
    10000000 bytes (10MB).

    Indexes may be created by specifying :attr:`indexes` in the :attr:`meta`
    dictionary. The value should be a list of field names or tuples of field
    names. Index direction may be specified by prefixing the field names with
    a **+** or **-** sign.

    Automatic index creation can be disabled by specifying
    attr:`auto_create_index` in the :attr:`meta` dictionary. If this is set to
    False then indexes will not be created by MongoEngine.  This is useful in
    production systems where index creation is performed as part of a deployment
    system.

    By default, _types will be added to the start of every index (that
    doesn't contain a list) if allow_inheritance is True. This can be
    disabled by either setting types to False on the specific index or
    by setting index_types to False on the meta dictionary for the document.
    """
    __metaclass__ = TopLevelDocumentMetaclass

    @apply
    def pk():
        """Primary key alias
        """
        def fget(self):
            return getattr(self, self._meta['id_field'])
        def fset(self, value):
            return setattr(self, self._meta['id_field'], value)
        return property(fget, fset)

    @classmethod
    def _get_db(cls):
        """Some Model using other db_alias"""
        return get_db(cls._meta.get("db_alias", DEFAULT_CONNECTION_NAME ))

    @classmethod
    def _get_collection(cls):
        """Returns the collection for the document."""
        if not hasattr(cls, '_collection') or cls._collection is None:
            db = cls._get_db()
            collection_name = cls._get_collection_name()
            # Create collection as a capped collection if specified
            if cls._meta['max_size'] or cls._meta['max_documents']:
                # Get max document limit and max byte size from meta
                max_size = cls._meta['max_size'] or 10000000  # 10MB default
                max_documents = cls._meta['max_documents']

                if collection_name in db.collection_names():
                    cls._collection = db[collection_name]
                    # The collection already exists, check if its capped
                    # options match the specified capped options
                    options = cls._collection.options()
                    if options.get('max') != max_documents or \
                       options.get('size') != max_size:
                        msg = ('Cannot create collection "%s" as a capped '
                               'collection as it already exists') % cls._collection
                        raise InvalidCollectionError(msg)
                else:
                    # Create the collection as a capped collection
                    opts = {'capped': True, 'size': max_size}
                    if max_documents:
                        opts['max'] = max_documents
                    cls._collection = db.create_collection(
                        collection_name, **opts
                    )
            else:
                cls._collection = db[collection_name]
        return cls._collection

    def save(self, safe=True, force_insert=False, validate=True, write_options=None,
            cascade=None, cascade_kwargs=None, _refs=None):
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created.

        If ``safe=True`` and the operation is unsuccessful, an
        :class:`~mongoengine.OperationError` will be raised.

        :param safe: check if the operation succeeded before returning
        :param force_insert: only try to create a new document, don't allow
            updates of existing documents
        :param validate: validates the document; set to ``False`` to skip.
        :param write_options: Extra keyword arguments are passed down to
                :meth:`~pymongo.collection.Collection.save` OR
                :meth:`~pymongo.collection.Collection.insert`
                which will be used as options for the resultant ``getLastError`` command.
                For example, ``save(..., write_options={w: 2, fsync: True}, ...)`` will
                wait until at least two servers have recorded the write and will force an
                fsync on each server being written to.
        :param cascade: Sets the flag for cascading saves.  You can set a default by setting
            "cascade" in the document __meta__
        :param cascade_kwargs: optional kwargs dictionary to be passed throw to cascading saves
        :param _refs: A list of processed references used in cascading saves

        .. versionchanged:: 0.5
            In existing documents it only saves changed fields using set / unset
            Saves are cascaded and any :class:`~bson.dbref.DBRef` objects
            that have changes are saved as well.
        .. versionchanged:: 0.6
            Cascade saves are optional = defaults to True, if you want fine grain
            control then you can turn off using document meta['cascade'] = False
            Also you can pass different kwargs to the cascade save using cascade_kwargs
            which overwrites the existing kwargs with custom values

        """
        signals.pre_save.send(self.__class__, document=self)

        if validate:
            self.validate()

        if not write_options:
            write_options = {}

        doc = self.to_mongo()

        created = force_insert or '_id' not in doc

        try:
            collection = self.__class__.objects._collection
            if created:
                if force_insert:
                    object_id = collection.insert(doc, safe=safe, **write_options)
                else:
                    object_id = collection.save(doc, safe=safe, **write_options)
            else:
                object_id = doc['_id']
                updates, removals = self._delta()

                # Need to add shard key to query, or you get an error
                select_dict = {'_id': object_id}
                shard_key = self.__class__._meta.get('shard_key', tuple())
                for k in shard_key:
                    actual_key = self._db_field_map.get(k, k)
                    select_dict[actual_key] = doc[actual_key]

                upsert = self._created
                if updates:
                    collection.update(select_dict, {"$set": updates}, upsert=upsert, safe=safe, **write_options)
                if removals:
                    collection.update(select_dict, {"$unset": removals}, upsert=upsert, safe=safe, **write_options)

            cascade = self._meta.get('cascade', True) if cascade is None else cascade
            if cascade:
                kwargs = {
                    "safe": safe,
                    "force_insert": force_insert,
                    "validate": validate,
                    "write_options": write_options,
                    "cascade": cascade
                }
                if cascade_kwargs:  # Allow granular control over cascades
                    kwargs.update(cascade_kwargs)
                kwargs['_refs'] = _refs
                #self._changed_fields = []
                self.cascade_save(**kwargs)

        except pymongo.errors.OperationFailure, err:
            message = 'Could not save document (%s)'
            if u'duplicate key' in unicode(err):
                message = u'Tried to save duplicate unique keys (%s)'
            raise OperationError(message % unicode(err))
        id_field = self._meta['id_field']
        self[id_field] = self._fields[id_field].to_python(object_id)

        self._changed_fields = []
        self._created = False
        signals.post_save.send(self.__class__, document=self, created=created)
        return self

    def cascade_save(self, *args, **kwargs):
        """Recursively saves any references / generic references on an object"""
        from fields import ReferenceField, GenericReferenceField
        _refs = kwargs.get('_refs', []) or []

        for name, cls in self._fields.items():

            if not isinstance(cls, (ReferenceField, GenericReferenceField)):
                continue

            ref = getattr(self, name)
            if not ref:
                continue
            if isinstance(ref, DBRef):
                continue

            ref_id = "%s,%s" % (ref.__class__.__name__, str(ref._data))
            if ref and ref_id not in _refs:
                _refs.append(ref_id)
                kwargs["_refs"] = _refs
                ref.save(**kwargs)
                ref._changed_fields = []

    def update(self, **kwargs):
        """Performs an update on the :class:`~mongoengine.Document`
        A convenience wrapper to :meth:`~mongoengine.QuerySet.update`.

        Raises :class:`OperationError` if called on an object that has not yet
        been saved.
        """
        if not self.pk:
            raise OperationError('attempt to update a document not yet saved')

        # Need to add shard key to query, or you get an error
        select_dict = {'pk': self.pk}
        shard_key = self.__class__._meta.get('shard_key', tuple())
        for k in shard_key:
            select_dict[k] = getattr(self, k)
        return self.__class__.objects(**select_dict).update_one(**kwargs)

    def delete(self, safe=False):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param safe: check if the operation succeeded before returning
        """
        signals.pre_delete.send(self.__class__, document=self)

        try:
            self.__class__.objects(pk=self.pk).delete(safe=safe)
        except pymongo.errors.OperationFailure, err:
            message = u'Could not delete document (%s)' % err.message
            raise OperationError(message)

        signals.post_delete.send(self.__class__, document=self)

    def select_related(self, max_depth=1):
        """Handles dereferencing of :class:`~bson.dbref.DBRef` objects to
        a maximum depth in order to cut down the number queries to mongodb.

        .. versionadded:: 0.5
        """
        from dereference import DeReference
        self._data = DeReference()(self._data, max_depth)
        return self

    def reload(self, max_depth=1):
        """Reloads all attributes from the database.

        .. versionadded:: 0.1.2
        .. versionchanged:: 0.6  Now chainable
        """
        id_field = self._meta['id_field']
        obj = self.__class__.objects(
                **{id_field: self[id_field]}
              ).first().select_related(max_depth=max_depth)
        for field in self._fields:
            setattr(self, field, self._reload(field, obj[field]))
        if self._dynamic:
            for name in self._dynamic_fields.keys():
                setattr(self, name, self._reload(name, obj._data[name]))
        self._changed_fields = obj._changed_fields
        return obj

    def _reload(self, key, value):
        """Used by :meth:`~mongoengine.Document.reload` to ensure the
        correct instance is linked to self.
        """
        if isinstance(value, BaseDict):
            value = [(k, self._reload(k, v)) for k, v in value.items()]
            value = BaseDict(value, self, key)
        elif isinstance(value, BaseList):
            value = [self._reload(key, v) for v in value]
            value = BaseList(value, self, key)
        elif isinstance(value, (EmbeddedDocument, DynamicEmbeddedDocument)):
            value._changed_fields = []
        return value

    def to_dbref(self):
        """Returns an instance of :class:`~bson.dbref.DBRef` useful in
        `__raw__` queries."""
        if not self.pk:
            msg = "Only saved documents can have a valid dbref"
            raise OperationError(msg)
        return DBRef(self.__class__._get_collection_name(), self.pk)

    @classmethod
    def register_delete_rule(cls, document_cls, field_name, rule):
        """This method registers the delete rules to apply when removing this
        object.
        """
        cls._meta['delete_rules'][(document_cls, field_name)] = rule

    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this
        :class:`~mongoengine.Document` type from the database.
        """
        from mongoengine.queryset import QuerySet
        db = cls._get_db()
        db.drop_collection(cls._get_collection_name())
        QuerySet._reset_already_indexed(cls)


class DynamicDocument(Document):
    """A Dynamic Document class allowing flexible, expandable and uncontrolled
    schemas.  As a :class:`~mongoengine.Document` subclass, acts in the same
    way as an ordinary document but has expando style properties.  Any data
    passed or set against the :class:`~mongoengine.DynamicDocument` that is
    not a field is automatically converted into a
    :class:`~mongoengine.DynamicField` and data can be attributed to that
    field.

    .. note::

        There is one caveat on Dynamic Documents: fields cannot start with `_`
    """
    __metaclass__ = TopLevelDocumentMetaclass
    _dynamic = True

    def __delattr__(self, *args, **kwargs):
        """Deletes the attribute by setting to None and allowing _delta to unset
        it"""
        field_name = args[0]
        if field_name in self._dynamic_fields:
            setattr(self, field_name, None)
        else:
            super(DynamicDocument, self).__delattr__(*args, **kwargs)


class DynamicEmbeddedDocument(EmbeddedDocument):
    """A Dynamic Embedded Document class allowing flexible, expandable and
    uncontrolled schemas. See :class:`~mongoengine.DynamicDocument` for more
    information about dynamic documents.
    """

    __metaclass__ = DocumentMetaclass
    _dynamic = True

    def __delattr__(self, *args, **kwargs):
        """Deletes the attribute by setting to None and allowing _delta to unset
        it"""
        field_name = args[0]
        setattr(self, field_name, None)

    @classmethod
    def pk_field(cls):
        return cls._fields[cls._meta['id_field']]

    @classmethod
    def _pymongo(cls):
        return cls.objects._collection

    def _update_one_key(self):
        """
            Designed to be overloaded in children when a shard key needs to be
            included in update_one() queries
        """
        return {'_id': self.id}

    @classmethod
    def _by_id_key(cls, doc_id):
        """
            Designed to be overloaded in children when a shard key needs to be
            included in a by_id()
        """
        return {'_id': doc_id}

    @classmethod
    def find_raw(cls, spec, fields=None, skip=0, limit=0, sort=None,
                 slave_ok=False, find_one=False, **kwargs):
        # transform query
        spec = cls._transform_value(spec, cls)

        # handle queries with inheritance
        if cls._meta.get('allow_inheritance'):
            spec['_types'] = cls._class_name

        # transform fields to include
        if isinstance(fields, list) or isinstance(fields, tuple):
            fields = [cls._transform_key(f, cls)[0] for f in fields]
        elif isinstance(fields, dict):
            fields = dict([[cls._transform_key(f, cls)[0], fields[f]] for f in
                           fields])

        # transform sort
        if sort:
            new_sort = []
            for f, dir in sort:
                f, _ = cls._transform_key(f, cls)
                new_sort.append((f, dir))

            sort = new_sort

        if slave_ok:
            read_preference = pymongo.ReadPreference.SECONDARY
        else:
            read_preference = pymongo.ReadPreference.PRIMARY

        for i in xrange(2):
            try:
                if find_one:
                    return cls._pymongo().find_one(spec, fields, skip=skip, sort=sort,
                                           read_preference=read_preference, **kwargs)
                else:
                    return cls._pymongo().find(spec, fields, skip=skip, limit=limit,
                                           sort=sort, read_preference=read_preference,
                                           **kwargs)
                break
            # delay & retry once on AutoReconnect error
            except pymongo.errors.AutoReconnect:
                time.sleep(0.1)


    @classmethod
    def find(cls, spec, fields=None, skip=0, limit=0, sort=None,
             slave_ok=False, **kwargs):
        cur = cls.find_raw(spec, fields, skip, limit, sort, **kwargs)

        return [cls._from_son(d) for d in cur]

    @classmethod
    def find_iter(cls, spec, fields=None, skip=0, limit=0, sort=None,
             slave_ok=False, timeout=False, **kwargs):
        cur = cls.find_raw(spec, fields, skip, limit,
                           sort, timeout=False, **kwargs)

        for doc in cur:
            yield cls._from_son(doc)

    @classmethod
    def find_one(cls, spec, fields=None, skip=0, sort=None,
                 slave_ok=False, **kwargs):
        d = cls.find_raw(spec, fields, skip=skip, sort=sort,
                         slave_ok=slave_ok, find_one=True, **kwargs)

        if d:
            return cls._from_son(d)
        else:
            return None

    @classmethod
    def count(cls, spec, slave_ok=False, **kwargs):
        cur = cls.find_raw(spec, slave_ok=slave_ok, **kwargs)
        return cur.count()

    @classmethod
    def update(cls, spec, document, upsert=False, multi=True, **kwargs):
        document = cls._transform_value(document, cls)
        spec = cls._transform_value(spec, cls)

        # handle queries with inheritance
        if cls._meta.get('allow_inheritance'):
            spec['_types'] = cls._class_name

        result = cls._pymongo().update(spec, document, upsert=upsert,
                                        multi=multi, safe=True, **kwargs)
        return result

    def update_one(self, document, spec=None, upsert=False, **kwargs):
        ops = {}

        for operator, operand in document.iteritems():
            # safety check - these updates should only have atomic ops in them
            if operator[0] != '$':
                raise ValueError("All updates should be atomic operators")

            if '.' not in operand:
                for field, new_val in operand.iteritems():
                    # for now, skip doing in-memory sets on dicts
                    if '.' in field:
                        continue

                    if operator == '$set':
                        ops[field] = new_val
                    elif operator == '$unset':
                        ops[field] = None
                    elif operator == '$inc':
                        ops[field] = self[field] + new_val
                    elif operator == '$push':
                        ops[field] = self[field][:] + [new_val]
                    elif operator == '$pushAll':
                        ops[field] = self[field][:] + new_val
                    elif operator == '$addToSet':
                        if '$each' in new_val:
                            vals_to_add = new_val['$each']
                        else:
                            vals_to_add = [new_val]

                        for val in vals_to_add:
                            if new_val not in self[field]:
                                ops[field] = self[field][:] + [val]

        document = self._transform_value(document, type(self))
        query_spec = self._update_one_key()

        if spec:
            query_spec.update(spec)

        query_spec = self._transform_value(query_spec, type(self))

        result = self._pymongo().update(query_spec, document, upsert=upsert,
                                        safe=True, multi=False, **kwargs)

        # do in-memory updates on the object if the query succeeded
        if result['n'] == 1:
            for field, new_val in ops.iteritems():
                self[field] = new_val

        return result

    def set(self, **kwargs):
        return self.update_one({'$set': kwargs})

    def inc(self, **kwargs):
        return self.update_one({'$inc': kwargs})

    def push(self, **kwargs):
        return self.update_one({'$push': kwargs})

    def add_to_set(self, **kwargs):
        return self.update_one({'$addToSet': kwargs})

    def _transform_query(self, query, validate=True):
        cls = type(self)
        return cls._transform_value(query, cls, validate=validate)

    @staticmethod
    def _transform_value(value, context, op=None, validate=True):
        from fields import DictField, EmbeddedDocumentField, ListField, \
                            ObjectIdField

        VALIDATE_OPS = ['$set', '$inc', None, '$eq', '$gte', '$lte', '$lt',
                        '$gt', '$ne']
        LIST_VALIDATE_OPS = ['$addToSet', '$push', '$pull']
        LIST_VALIDATE_ALL_OPS = ['$pushAll', '$pullAll', '$each', '$in',
                                 '$nin', '$all']
        NO_VALIDATE_OPS = ['$unset', '$pop', '$rename', '$bit',
                           '$all', '$and', '$or', '$exists', '$mod',
                           '$elemMatch', '$size', '$type', '$not', '$returnKey',
                           '$maxScan', '$orderby', '$explain', '$snapshot',
                           '$max', '$min', '$showDiskLoc', '$hint', '$comment']

        # recurse on dict, unless we're at a DictField
        if isinstance(value, dict) and not isinstance(context, DictField):
            transformed_value = SON()

            for key, subvalue in value.iteritems():
                if key[0] == '$':
                    op = key

                new_key, value_context = Document._transform_key(key, context)

                transformed_value[new_key] = \
                    Document._transform_value(subvalue, value_context,
                                              op, validate)

            return transformed_value
        # else, validate & return
        else:
            # automatically encode ObjectIds (they're often strings)
            if isinstance(context, ObjectIdField) and op in VALIDATE_OPS:
                value = ObjectId(value)

            # automatically encode ObjectIds in lists too...
            elif isinstance(context, ObjectIdField) and \
               (op in LIST_VALIDATE_OPS or op in LIST_VALIDATE_ALL_OPS):
                value = [ObjectId(v) for v in value]

            if validate:
                if op in VALIDATE_OPS:
                    context.validate(value)
                elif op in LIST_VALIDATE_OPS:
                    context.field.validate(value)
                elif op in LIST_VALIDATE_ALL_OPS:
                    for entry in value:
                        if isinstance(context, ListField):
                            context.field.validate(entry)
                        else:
                            context.validate(entry)
                elif op not in NO_VALIDATE_OPS:
                    raise ValidationError("Unknown atomic operator %s" % op)

            # handle EmbeddedDocuments
            if isinstance(value, BaseDocument):
                value = value.to_mongo()

            # handle lists (force to_mongo() everything if it's a list of docs)
            elif isinstance(context, ListField) and \
               isinstance(context.field, EmbeddedDocumentField):
                value = [d.to_mongo() for d in value]

            # handle dicts (just blindly to_mongo() anything that'll take it)
            elif isinstance(context, DictField):
                for k, v in value.iteritems():
                    if isinstance(v, BaseDocument):
                        value[k] = v.to_mongo()

            return value

    @staticmethod
    def _transform_key(key, context, prefix=''):
        from fields import BaseField, DictField, ListField, \
                            EmbeddedDocumentField, ArbitraryField

        parts = key.split('.', 1)
        first_part = parts[0]

        if len(parts) > 1:
            rest = parts[1]
        else:
            rest = None

        # a key as a digit means a list index... set context as the list's value
        if first_part.isdigit():
            if isinstance(context.field, basestring):
                context = get_document(context.field)
            elif isinstance(context.field, BaseField):
                context = context.field

        if first_part == '_id':
            context = context.pk_field()

        # atomic ops, digits (list indexes), or _ids get no processing
        if first_part[0] == '$' or first_part.isdigit() or first_part == '_id':
            if prefix:
                new_prefix = "%s.%s" % (prefix, first_part)
            else:
                new_prefix = first_part

            if rest:
                return Document._transform_key(rest, context, prefix=new_prefix)
            else:
                return new_prefix, context


        def is_subclass_or_instance(obj, parent):
            try:
                if issubclass(obj, parent):
                    return True
            except TypeError:
                if isinstance(obj, parent):
                    return True

            return False

        field = None

        if is_subclass_or_instance(context, BaseDocument):
            field = context._fields.get(first_part, None)
        elif is_subclass_or_instance(context, EmbeddedDocumentField):
            field = context.document_type._fields.get(first_part, None)
        elif is_subclass_or_instance(context, ListField):
            if is_subclass_or_instance(context.field, basestring):
                field = get_document(context.field)
            elif is_subclass_or_instance(context.field, BaseField):
                field = context.field
            else:
                raise ValueError("Can't parse field %s" % first_part)
        # if we hit a DictField, values can be anything, so use the sentinal
        # ArbitraryField value (I prefer this over None, since None can be
        # introduced in other ways that would be considered errors & should not
        # be silently ignored)
        elif is_subclass_or_instance(context, DictField):
            field = ArbitraryField()
        elif is_subclass_or_instance(context, ArbitraryField):
            field = context

        if not field:
            raise ValueError("Can't find field %s" % first_part)

        if is_subclass_or_instance(field, ArbitraryField):
            db_field = first_part
        else:
            db_field = field.db_field

        if prefix:
            result = "%s.%s" % (prefix, db_field)
        else:
            result = db_field

        if rest:
            return Document._transform_key(rest, field, prefix=result)
        else:
            return result, field

class MapReduceDocument(object):
    """A document returned from a map/reduce query.

    :param collection: An instance of :class:`~pymongo.Collection`
    :param key: Document/result key, often an instance of
                :class:`~bson.objectid.ObjectId`. If supplied as
                an ``ObjectId`` found in the given ``collection``,
                the object can be accessed via the ``object`` property.
    :param value: The result(s) for this key.

    .. versionadded:: 0.3
    """

    def __init__(self, document, collection, key, value):
        self._document = document
        self._collection = collection
        self.key = key
        self.value = value

    @property
    def object(self):
        """Lazy-load the object referenced by ``self.key``. ``self.key``
        should be the ``primary_key``.
        """
        id_field = self._document()._meta['id_field']
        id_field_type = type(id_field)

        if not isinstance(self.key, id_field_type):
            try:
                self.key = id_field_type(self.key)
            except:
                raise Exception("Could not cast key as %s" % \
                                id_field_type.__name__)

        if not hasattr(self, "_key_object"):
            self._key_object = self._document.objects.with_id(self.key)
            return self._key_object
        return self._key_object
