from base import (DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument,
                  ValidationError, get_document)
from queryset import OperationError

import pymongo
import time
from bson import SON, ObjectId


__all__ = ['Document', 'EmbeddedDocument', 'ValidationError', 'OperationError']


class EmbeddedDocument(BaseDocument):
    """A :class:`~mongoengine.Document` that isn't stored in its own
    collection.  :class:`~mongoengine.EmbeddedDocument`\ s should be used as
    fields on :class:`~mongoengine.Document`\ s through the
    :class:`~mongoengine.EmbeddedDocumentField` field type.
    """

    __metaclass__ = DocumentMetaclass


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
    """

    __metaclass__ = TopLevelDocumentMetaclass

    def save(self, safe=True, force_insert=False, validate=True):
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created.

        If ``safe=True`` and the operation is unsuccessful, an
        :class:`~mongoengine.OperationError` will be raised.

        :param safe: check if the operation succeeded before returning
        :param force_insert: only try to create a new document, don't allow
            updates of existing documents
        :param validate: validates the document; set to ``False`` to skip.
        """
        if validate:
            self.validate()
        doc = self.to_mongo()
        try:
            collection = self.__class__.objects._collection
            if force_insert:
                object_id = collection.insert(doc, safe=safe)
            else:
                object_id = collection.save(doc, safe=safe)
        except pymongo.errors.OperationFailure, err:
            message = 'Could not save document (%s)'
            if u'duplicate key' in unicode(err):
                message = u'Tried to save duplicate unique keys (%s)'
            raise OperationError(message % unicode(err))
        id_field = self._meta['id_field']
        self[id_field] = self._fields[id_field].to_python(object_id)

    def delete(self, safe=True):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param safe: check if the operation succeeded before returning
        """
        id_field = self._meta['id_field']
        object_id = self._fields[id_field].to_mongo(self[id_field])
        try:
            self.__class__.objects(**{id_field: object_id}).delete(safe=safe)
        except pymongo.errors.OperationFailure, err:
            message = u'Could not delete document (%s)' % err.message
            raise OperationError(message)

    def reload(self):
        """Reloads all attributes from the database.

        .. versionadded:: 0.1.2
        """
        id_field = self._meta['id_field']
        obj = self.__class__.objects(**{id_field: self[id_field]}).first()
        for field in self._fields:
            setattr(self, field, obj[field])

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
                :class:`~pymongo.objectid.ObjectId`. If supplied as
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
