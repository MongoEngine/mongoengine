from base import (DocumentMetaclass, TopLevelDocumentMetaclass, BaseDocument,
                  ValidationError, MongoComment, get_document, get_embedded_doc_fields,
                  FieldStatus, FieldNotLoadedError)
from queryset import OperationError
from cl.utils.greenletutil import CLGreenlet, GreenletUtil
import contextlib
import pymongo
import time
import greenlet
import smtplib
import socket
import sys
import traceback
import logging
from timer import log_slow_event
import warnings

from bson import SON, ObjectId, DBRef
from connection import _get_db, _get_slave_ok, _get_proxy_client, _get_proxy_decider, OpClass

try:
    from soa.services.base_client import RPCException
    from soa.services.base_grpc_client import ProxiedGrpcError
except:
    RPCException = None
    ProxiedGrpcError = None

__all__ = ['Document', 'EmbeddedDocument', 'ValidationError',
           'OperationError', 'BulkOperationError']

# set the sleep function to be used after an AutoReconnect exception from
# connection close. default to time.sleep(), but we can replace this with
# async sleep in Tornado-based apps (i.e. FEs)
_sleep = time.sleep
OPS_EMAIL = 'ops@wish.com'

high_offset_logger = logging.getLogger('sweeper.prod.mongodb_high_offset')
execution_timeout_logger = logging.getLogger('sweeper.prod.mongodb_execution_timeout')
notimeout_cursor_logger = logging.getLogger('sweeper.prod.mongodb_notimeout')

class CLSContext(object):
    pass

class BulkOperationError(OperationError):
    pass


class EmbeddedDocument(BaseDocument):
    """A :class:`~mongoengine.Document` that isn't stored in its own
    collection.  :class:`~mongoengine.EmbeddedDocument`\ s should be used as
    fields on :class:`~mongoengine.Document`\ s through the
    :class:`~mongoengine.EmbeddedDocumentField` field type.
    """

    __metaclass__ = DocumentMetaclass


class WrappedCounter(object):

    def __init__(self):
        self.value = 0

    def inc(self):
        self.value += 1

    def get(self):
        return self.value

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

    MAX_AUTO_RECONNECT_TRIES = 6
    AUTO_RECONNECT_SLEEP = 5
    INCLUDE_SHARD_KEY = []
    RETRY_MAX_TIME_MS = 5000
    MAX_TIME_MS = 2500
    ALLOW_TIMEOUT_RETRY = True
    NO_TIMEOUT_DEFAULT = object()

    __metaclass__ = TopLevelDocumentMetaclass

    def save(self, safe=True, force_insert=None, validate=True):
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
        if self.__class__._bulk_op is not None:
            warnings.warn('Non-bulk update inside bulk operation')

        proxy_client = self._get_proxy_client()

        if self._meta['hash_field']:
            # if we're hashing the ID and it hasn't been set yet, autogenerate it
            from fields import ObjectIdField
            if self._meta['hash_field'] == self._meta['id_field'] and \
               not self.id and isinstance(self._fields['id'], ObjectIdField):
                self.id = ObjectId()

            self['shard_hash'] = self._hash(self[self._meta['hash_field']])

        if force_insert is None:
            force_insert = self._meta['force_insert']

        if validate:
            self.validate()
        doc = self.to_mongo()
        try:
            w = self._meta.get('write_concern', 1)
            if force_insert or "_id" not in doc:
                if proxy_client:
                    if self._get_write_decider():
                        # Copied from pymongo/collection.py. If the _id doesn't exist
                        # generate it on the client side.
                        if '_id' not in doc:
                            doc['_id'] = ObjectId()
                        proxy_client.instance().insert(
                            self.__class__, [doc], write_concern=w)
                        object_id = doc['_id']
                    else:
                        collection = self._pymongo()
                        object_id = collection.insert(doc, w=w)
                else:
                    collection = self._pymongo()
                    object_id = collection.insert(doc, w=w)
            else:
                if proxy_client:
                    if self._get_write_decider():
                        proxy_client.instance().update(
                            self.__class__,
                            self.__class__.get_upsert_filter(doc),
                            doc,
                            upsert=True,
                            write_concern=w,
                            multi=False
                        )
                        object_id = doc["_id"]
                    else:
                        collection = self._pymongo()
                        object_id = collection.save(doc, w=w)
                else:
                    collection = self._pymongo()
                    object_id = collection.save(doc, w=w)
        except (pymongo.errors.OperationFailure, ProxiedGrpcError, RPCException), err:
            message = 'Could not save document (%s)'
            if u'duplicate key' in unicode(err):
                message = u'Tried to save duplicate unique keys (%s)'
            raise OperationError(message % unicode(err))
        id_field = self._meta['id_field']
        self[id_field] = self._fields[id_field].to_python(object_id)

    # For Mongo 4.2 and above, the filter must contain shard key if upsert is true.
    @classmethod
    def get_upsert_filter(cls, doc):
        filter = {"_id" : doc["_id"]}
        shard_keys = cls._meta.get('shard_key', "")
        shard_fields = [s.split(':')[0] for s in shard_keys.split(',')]
        for field in shard_fields:
            if field in cls._fields:
                mongo_field = cls._fields.get(field).db_field
                if mongo_field in doc:
                    filter[mongo_field] = doc[mongo_field]
        return filter

    def delete(self, safe=True):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param safe: check if the operation succeeded before returning
        """
        id_field = self._meta['id_field']
        object_id = self._fields[id_field].to_mongo(self[id_field])
        try:
            self.remove({id_field: object_id}, from_delete=True)
        except (pymongo.errors.OperationFailure, ProxiedGrpcError, RPCException), err:
            message = u'Could not delete document (%s)' % err.message
            raise OperationError(message)

    def reload(self, slave_ok=False):
        """Reloads all attributes from the database.

        .. versionadded:: 0.1.2
        """
        id_field = self._meta['id_field']
        obj = self.__class__.find_one(self._by_id_key(self[id_field]),
                                      slave_ok=slave_ok)
        for field in self._fields:
            setattr(self, field, obj[field])

    BULK_INDEX = "bulk_index"
    BULK_SAVE_OBJECTS = "bulk_save_objects"
    BULK_OP = "bulk_op"
    PROXY_BULK_OP = "proxy_bulk_op"

    @classmethod
    def bulk_name(cls, name):
        return "_bulk_%s_%s" % (name, cls.__name__)

    @classmethod
    def get_bulk_attr(cls, name):
        current_greenlet = greenlet.getcurrent()
        if hasattr(current_greenlet, cls.bulk_name(name)):
            return getattr(current_greenlet, cls.bulk_name(name))
        return None

    @classmethod
    def init_bulk_attr(cls, name, default):
        current_greenlet = greenlet.getcurrent()
        if not hasattr(current_greenlet, cls.bulk_name(name)) or \
            getattr(current_greenlet, cls.bulk_name(name)) is None:
            setattr(current_greenlet, cls.bulk_name(name), default)
        return getattr(current_greenlet, cls.bulk_name(name))

    @classmethod
    def clear_bulk_attr(cls, name):
        current_greenlet = greenlet.getcurrent()
        setattr(current_greenlet, cls.bulk_name(name), None)

    @classmethod
    @contextlib.contextmanager
    def bulk(cls, allow_empty=None, unordered=False):
        if cls.get_bulk_attr(cls.BULK_OP) is not None or cls.get_bulk_attr(cls.PROXY_BULK_OP) is not None:
            raise RuntimeError('Cannot nest bulk operations')
        try:
            cls.init_bulk_attr(cls.BULK_INDEX, WrappedCounter())
            cls.init_bulk_attr(cls.BULK_SAVE_OBJECTS, dict())

            usemongoproxy=False
            proxy_client = cls._get_proxy_client()
            if proxy_client:
                usemongoproxy = cls._get_write_decider()
            if not usemongoproxy:
                if unordered:
                    cls.init_bulk_attr(cls.BULK_OP,cls._pymongo().initialize_unordered_bulk_op())
                else:
                    cls.init_bulk_attr(cls.BULK_OP,cls._pymongo().initialize_ordered_bulk_op())
            else:
                cls.init_bulk_attr(cls.PROXY_BULK_OP, list())
            yield
            try:
                if usemongoproxy:
                    proxy_client.instance().bulk(cls, cls.get_bulk_attr(cls.PROXY_BULK_OP), unordered)
                else:
                    w = cls._meta.get('write_concern', 1)
                    cls.get_bulk_attr(cls.BULK_OP).execute(write_concern={'w': w})

                for object_id, props in cls.get_bulk_attr(cls.BULK_SAVE_OBJECTS).iteritems():
                    instance = props['obj']
                    if instance.id is None:
                        id_field = cls.pk_field()
                        id_name = id_field.name or 'id'
                        instance[id_name] = id_field.to_python(object_id)
            except pymongo.errors.BulkWriteError as e:
                wc_errors = e.details.get('writeConcernErrors')
                # only one write error should occur for an ordered op
                w_error = e.details['writeErrors'][0] \
                    if e.details.get('writeErrors') else None
                if wc_errors:
                    messages = '\n'.join(_['errmsg'] for _ in wc_errors)
                    message = 'Write concern errors for bulk op: %s' % messages
                elif w_error:
                    for object_id, props in cls.get_bulk_attr(cls.BULK_SAVE_OBJECTS):
                        if props['index'] < w_error['index']:
                            instance = props['obj']
                            if instance.id is None:
                                id_field = cls.pk_field()
                                id_name = id_field.name or 'id'
                                instance[id_name] = \
                                    id_field.to_python(object_id)
                    message = 'Write errors for bulk op: %s' % \
                        w_error['errmsg']

                bo_error = BulkOperationError(message)
                bo_error.details = e.details
                if w_error:
                    bo_error.op = w_error['op']
                    bo_error.index = w_error['index']
                raise bo_error
            except pymongo.errors.InvalidOperation as e:
                if 'No operations' in e.message:
                    if allow_empty is None:
                        warnings.warn('Empty bulk operation; use allow_empty')
                    elif allow_empty is False:
                        raise
                    else:
                        pass
                else:
                    raise
            except (pymongo.errors.OperationFailure, ProxiedGrpcError, RPCException), err:
                message = u'Could not perform bulk operation (%s)' % err.message
                raise OperationError(message)
        finally:
            cls.clear_bulk_attr(cls.BULK_OP)
            cls.clear_bulk_attr(cls.PROXY_BULK_OP)
            cls.clear_bulk_attr(cls.BULK_INDEX)
            cls.clear_bulk_attr(cls.BULK_SAVE_OBJECTS)

    @classmethod
    def bulk_update(cls, spec, document, upsert=False, multi=True, **kwargs):
        if cls.get_bulk_attr(cls.BULK_OP) is None and cls.get_bulk_attr(cls.PROXY_BULK_OP) is None:
            raise RuntimeError('Cannot do bulk operation outside of bulk context')

        document = cls._transform_value(document, cls, op='$set')
        spec = cls._transform_value(spec, cls)

        if not document:
            raise ValueError("Cannot do empty updates")

        if not spec:
            raise ValueError("Cannot do empty specs")

        spec = cls._update_spec(spec, **kwargs)
        bulk_step = {
            'filter': spec,
            'document': document
        }

        bulk_op = cls.get_bulk_attr(cls.BULK_OP)
        proxy_bulk_op = cls.get_bulk_attr(cls.PROXY_BULK_OP)
        # pymongo's bulk operation support is based on chaining
        if upsert:
            if bulk_op is not None:
                op = bulk_op.find(spec).upsert()
            bulk_step['op'] = 'upsert'
        else:
            if multi:
                bulk_step['op'] = 'update_all'
            else:
                bulk_step['op'] = 'update'
            if bulk_op is not None:
                op = bulk_op.find(spec)

        if bulk_op is not None:
            if multi:
                op.update(document)
            else:
                op.update_one(document)

            cls.get_bulk_attr(cls.BULK_INDEX).inc()
        else:
            proxy_bulk_op.append(bulk_step)

    @classmethod
    def bulk_remove(cls, spec, multi=True, **kwargs):
        if cls.get_bulk_attr(cls.BULK_OP) is None and cls.get_bulk_attr(cls.PROXY_BULK_OP) is None:
            raise RuntimeError('Cannot do bulk operation outside of bulk context')

        spec = cls._transform_value(spec, cls)

        if not spec:
            raise ValueError("Cannot do empty specs")

        spec = cls._update_spec(spec, **kwargs)

        bulk_op = cls.get_bulk_attr(cls.BULK_OP)
        proxy_bulk_op = cls.get_bulk_attr(cls.PROXY_BULK_OP)

        if bulk_op is not None:
            op = bulk_op.find(spec)
        if multi:
            if bulk_op is not None:
                op.remove()
            else:
                proxy_bulk_op.append({
                    'op': 'remove_all',
                    'filter': spec
                })
        else:
            if bulk_op is not None:
                op.remove_one()
            else:
                proxy_bulk_op.append({
                    'op': 'remove',
                    'filter': spec
                })

        if bulk_op is not None:
            cls.get_bulk_attr(cls.BULK_INDEX).inc()

    def bulk_save(self, validate=True):
        cls = self.__class__
        if cls.get_bulk_attr(cls.BULK_OP) is None and cls.get_bulk_attr(cls.PROXY_BULK_OP) is None:
            raise RuntimeError('Cannot do bulk operation outside of bulk context')

        if validate:
            self.validate()
        doc = self.to_mongo()

        id_field = cls.pk_field()
        id_name = id_field.name or 'id'

        bulk_op = cls.get_bulk_attr(cls.BULK_OP)
        proxy_bulk_op = cls.get_bulk_attr(cls.PROXY_BULK_OP)

        if self[id_name] is None:
            object_id = ObjectId()
            doc[id_field.db_field] = id_field.to_mongo(object_id)
        else:
            object_id = self[id_name]

        if cls._meta['hash_field']:
            # id is not yet set on object
            if cls._meta['hash_field'] == cls._meta['id_field']:
                hash_value = object_id
            else:
                hash_value = self[cls._meta['hash_field']]

            self['shard_hash'] = cls._hash(hash_value)
            hash_field = cls._fields[cls._meta['hash_field']]
            doc[hash_field.db_field] = hash_field.to_mongo(self['shard_hash'])

        if bulk_op is not None:
            bulk_op.insert(doc)
            cls.get_bulk_attr(cls.BULK_SAVE_OBJECTS)[object_id] = {
                'index': cls.get_bulk_attr(cls.BULK_INDEX).get(),
                'obj': self
            }
            cls.get_bulk_attr(cls.BULK_INDEX).inc()
        else:
            proxy_bulk_op.append({
                'op': 'insert',
                'document': doc
            })
        return object_id

    @classmethod
    def _from_augmented_son(cls, d, fields, excluded_fields=None):
        # load from son, and set field status correctly
        obj = cls._from_son(d)
        if obj is None:
            return None

        fields = cls._transform_fields(fields, excluded_fields)

        if fields is None:
            obj._all_loaded = True
            obj._default_load_status = FieldStatus.LOADED
            return obj


        # _id is always loaded unless it is specifically excluded
        obj._fields_status['_id'] = FieldStatus.LOADED
        if '_id' in fields:
            value = fields.pop('_id')
            if value == 0:
                obj._fields_status['_id'] = FieldStatus.NOT_LOADED
            if not fields:
                obj._default_load_status = FieldStatus.LOADED if value == 0 \
                    else FieldStatus.NOT_LOADED
                return obj

        # fields is now a dict of {db_field: (VALUE|<projection operator>)}
        #   where VALUE is always 1 (include) or always 0 (exclude)
        #   semantics are as follows:
        #       dict contains a 0/1 (exclude/include mode respectively)
        #       dict contains a $elemMatch (forces include mode)
        #       dict contains a $slice (forces exclude mode)
        #       otherwise include mode

        if 0 in fields.itervalues():
            dflt_load_status = FieldStatus.LOADED
        elif 1 in fields.itervalues():
            dflt_load_status = FieldStatus.NOT_LOADED
        elif len(fields) > 0:
            # true if there are any '$elemMatch's
            dflt_load_status = FieldStatus.NOT_LOADED if \
                    any(isinstance(v, dict) and '$elemMatch' in v \
                        for v in fields.itervalues()) \
                    else FieldStatus.LOADED
        else:
            dflt_load_status = FieldStatus.NOT_LOADED

        for (field, val) in fields.iteritems():
            status = FieldStatus.NOT_LOADED if val == 0 \
                    else FieldStatus.LOADED
            cls._set_field_status(field, obj, status, dflt_load_status)

        return obj

    @staticmethod
    def _set_field_status(field_name, context, status, dflt_status):
        if isinstance(context, BaseDocument):
            parts = field_name.split('.')
            first_part = parts[0]
            rest = parts[1] if len(parts) > 1 else None

            context._default_load_status = dflt_status
            context._all_loaded = False

            if rest and rest != "$":
                # if the field is recursive the parent field must be loaded
                context._fields_status[first_part] = FieldStatus.LOADED
                name = [n for (n, f) in context._fields.iteritems()
                        if f.db_field == first_part][0]

                Document._set_field_status(rest, getattr(context, name),
                        status, dflt_status)
            else:
                context._fields_status[first_part] = status
        elif isinstance(context, list):
            for el in context:
                Document._set_field_status(field_name, el, status, dflt_status)
        else:
            raise ValueError("Invalid field name %s in context %s" %
                    (field_name, context))

    @classmethod
    def _transform_fields(cls, fields=None, excluded_fields=None):
        if fields is not None and excluded_fields is not None:
            raise ValueError(
                'Cannot specify both included and excluded fields.'
            )
        if isinstance(fields, dict):
            new_fields = {}
            for key, val in fields.iteritems():
                db_key, field = cls._transform_key(key, cls, is_find=True)
                if isinstance(val, dict):
                    if val.keys() not in (['$elemMatch'], ['$slice']):
                        raise ValueError('Invalid field value')
                    new_fields[db_key] = cls._transform_value(val, field,
                            fields=True)
                else:
                    if val not in [0, 1]:
                        raise ValueError('Invalid field value')

                    new_fields[db_key] = val
            fields = new_fields
        elif isinstance(fields, (list, tuple)):
            fields = {
                cls._transform_key(f, cls, is_find=True)[0]: 1 for f in fields
            }
        elif isinstance(excluded_fields, (list, tuple)):
            fields = {
                cls._transform_key(f, cls, is_find=True)[0]: 0
                    for f in excluded_fields
            }

        return fields

    @classmethod
    def pk_field(cls):
        return cls._fields[cls._meta['id_field']]

    @classmethod
    def _hash(cls, value):
        # chances are this is a mistake and we didn't mean to hash "None"...
        # protect ourselves from our potential future stupidity
        if value is None:
            raise ValueError("Shard hash key is None")

        return hash(str(value))

    @classmethod
    def _get_proxy_client(cls):
        db_name = cls._meta['db_name']
        return _get_proxy_client(db_name)

    @classmethod
    def _get_read_decider(cls):
        return _get_proxy_decider(OpClass.READ)

    @classmethod
    def _get_write_decider(cls):
        return _get_proxy_decider(OpClass.WRITE)

    @classmethod
    def _pymongo(cls, use_async=True):
        # we can't do async queries if we're on the root greenlet since we have
        # nothing to yield back to
        use_async &= bool(greenlet.getcurrent().parent)

        if not hasattr(cls, '_pymongo_collection'):
            cls._pymongo_collection = {}

        if use_async not in cls._pymongo_collection:
            cls._pymongo_collection[use_async] = \
                    _get_db(cls._meta['db_name'],
                            allow_async=use_async)[cls._meta['collection']]

        return cls._pymongo_collection[use_async]

    def _update_one_key(self):
        """
            Designed to be overloaded in children when a shard key needs to be
            included in update_one() queries
        """
        key = {'_id': self.id}

        if self._meta['hash_field'] and self._meta['sharded']:
            key['shard_hash'] = self._hash(self[self._meta['hash_field']])

        return key

    @classmethod
    def _by_id_key(cls, doc_id):
        """
            Designed to be overloaded in children when a shard key needs to be
            included in a by_id()
        """
        key = {'_id': doc_id}

        # NOTE(mzeng): The 'hash_field' and 'shard_hash' are Wish-specific hacks to hash shard
        # a collection before Mongo support hash sharding. They should not be in use now.
        if cls._meta['hash_field'] == cls._meta['id_field'] \
           and cls._meta['sharded']:
            key['shard_hash'] = cls._hash(doc_id)

        return key

    @classmethod
    def _by_ids_key(cls, doc_ids):
        key = {'_id': {'$in': doc_ids}}

        if cls._meta['hash_field'] == cls._meta['id_field'] \
           and cls._meta['sharded']:
            key['shard_hash'] = {'$in': [cls._hash(doc_id)
                                         for doc_id in doc_ids]}

        return key

    @classmethod
    def _transform_hint(cls, hint_doc):
        new_hint_doc = []
        for i, index_field in enumerate(hint_doc):
            field, direction = hint_doc[i]
            db_field, context = cls._transform_key(field, cls)
            new_hint_doc.append((db_field, direction))

        return new_hint_doc

    @classmethod
    def _update_spec(cls, spec, cursor_comment=True, comment=None, **kwargs):
        # handle queries with inheritance
        if cls._meta.get('allow_inheritance'):
            spec['_types'] = cls._class_name
        if cursor_comment is True and spec: # comment doesn't with empty spec..
            if not comment:
                comment = MongoComment.get_query_comment()
            spec['$comment'] = comment
        return spec

    @classmethod
    def is_scatter_gather(cls, spec):
        '''
            Determine if the query is a scatter gather. Allow the caller
            to override this logic with is_scatter_gather since this method
            isn't fullproof
        '''
        is_scatter_gather = False
        try:
            shard_keys = cls.__dict__['meta']['shard_key']

            # if shard_keys is false, the collection is not sharded
            if not shard_keys:
                return False

            shard_keys = shard_keys.split(',')
            shard_keys = [s.split(':')[0] for s in shard_keys]

            # For custom primary key fields, convert them all to id here
            id_field = cls._meta['id_field']
            spec_keys = set(spec.keys())

            if id_field in spec_keys:
                spec_keys.remove(id_field)
                spec_keys.add('id')

            for sk in shard_keys:
                if sk == id_field:
                    sk = 'id'

                if sk == 'id' or sk == '_id':
                    if 'id' not in spec_keys and '_id' not in spec_keys:
                        is_scatter_gather = True
                        break;
                else:
                    if sk not in spec_keys:
                        is_scatter_gather = True
                        break;
        except Exception as e:
            pass

        return is_scatter_gather

    @classmethod
    def attach_trace(cls, comment, is_scatter_gather):
        set_comment = False
        current_greenlet = greenlet.getcurrent()
        if isinstance(current_greenlet, CLGreenlet):
            if not hasattr(current_greenlet,
                '__mongoengine_comment__'):
                trace_comment = comment#'%f:%s' % (time.time(), comment)
                setattr(current_greenlet,
                    '__mongoengine_comment__', trace_comment)
                set_comment = True
                current_greenlet.add_mongo_start(
                    trace_comment, time.time())
            else:
                trace_comment = getattr(current_greenlet, '__mongoengine_comment__')
                current_greenlet.add_mongo_start(
                    trace_comment, time.time())
            setattr(current_greenlet,
                '__scatter_gather__', is_scatter_gather)

        return set_comment

    @classmethod
    def cleanup_trace(cls, set_comment):
        current = greenlet.getcurrent()
        if hasattr(current, '__mongoengine_comment__'):
            is_scatter_gather = False
            if hasattr(current, '__scatter_gather__'):
                is_scatter_gather = current.__scatter_gather__
            current.add_mongo_end(
                current.__mongoengine_comment__, time.time(),
                is_scatter_gather)

        if hasattr(
            current,'__mongoengine_comment__'):
            delattr(current,
                '__mongoengine_comment__')

    @classmethod
    def find_raw(cls, spec, fields=None, skip=0, limit=0, sort=None,
                 slave_ok=True, find_one=False, allow_async=True, hint=None,
                 batch_size=10000, excluded_fields=None, max_time_ms=None,
                 comment=None, from_mengine=True, **kwargs):
        proxy_client = cls._get_proxy_client()
        if not from_mengine and proxy_client:
            if cls._get_read_decider():
                if 'comment' not in kwargs or kwargs['comment'] is None:
                    kwargs['comment'] = MongoComment.get_comment()
                is_scatter_gather = cls.is_scatter_gather(spec)
                set_comment = cls.attach_trace(kwargs['comment'], is_scatter_gather)
                try:
                    return proxy_client.instance().find_raw(
                        cls, spec, fields=fields, skip=skip,
                        limit=limit, sort=sort, slave_ok=slave_ok,
                        excluded_fields=excluded_fields, max_time_ms=max_time_ms,
                        batch_size=batch_size, hint=hint,
                        find_one=find_one,**kwargs
                    ), False
                finally:
                    cls.cleanup_trace(set_comment)
        if kwargs.get("timeout") is False and slave_ok != "offline":
            trace = "".join(traceback.format_stack())
            notimeout_cursor_logger.info({
                'trace' : trace,
            })
            warnings.warn('Avoid noTimeout cursors on primaries')
            del kwargs["timeout"]

        is_scatter_gather = cls.is_scatter_gather(spec)

        # HACK [adam May/2/16]: log high-offset queries with sorts to TD. these
        #      queries tend to cause significant load on mongo
        set_comment = False

        if sort and skip > 100000:
            trace = "".join(traceback.format_stack())
            high_offset_logger.info({
                'limit': limit,
                'skip': skip,
                'trace': trace
            })

        # transform query
        spec = cls._transform_value(spec, cls)
        spec = cls._update_spec(spec, **kwargs)

        # transform fields to include
        fields = cls._transform_fields(fields, excluded_fields)

        # transform sort
        if sort:
            new_sort = []
            for f, dir in sort:
                f, _ = cls._transform_key(f, cls)
                new_sort.append((f, dir))

            sort = new_sort

        # grab read preference & tags from slave_ok value
        try:
            slave_ok = _get_slave_ok(slave_ok)
        except KeyError:
            raise ValueError("Invalid slave_ok preference: %s" % slave_ok)

        # do field name transformation on hints
        #
        # the mongodb index {f1: 1, f2: 1} is expressed to pymongo as
        # [ ("field1", 1), ("field2", 1) ]. here we need to transform
        # "field1" to its db_field, etc.
        if hint:
            hint = cls._transform_hint(hint)

        # in case count passed in instead of limit
        if 'count' in kwargs and limit == 0:
            limit = kwargs['count']

        for i in xrange(cls.MAX_AUTO_RECONNECT_TRIES):
            try:
                set_comment = False

                with log_slow_event('find', cls._meta['collection'], spec):
                    cur = cls._pymongo(allow_async).find(spec, fields,
                                          skip=skip, limit=limit, sort=sort,
                                          read_preference=slave_ok.read_pref,
                                          tag_sets=slave_ok.tags,
                                          **kwargs)

                    # max_time_ms <= 0 means its disabled, None means
                    # use default value, otherwise use the value specified
                    if max_time_ms is None:
                        # if the default value is set to 0, then this feature is
                        # disabled by default
                        if cls.MAX_TIME_MS > 0:
                            cur.max_time_ms(cls.MAX_TIME_MS)
                    elif max_time_ms > 0:
                        cur.max_time_ms(max_time_ms)

                    if hint:
                        cur.hint(hint)

                    if not comment:
                        comment = MongoComment.get_query_comment()

                    set_comment = cls.attach_trace(comment, is_scatter_gather)

                    cur.comment(comment)

                    if find_one:
                        for result in cur.limit(-1):
                            return result, set_comment
                        return None, set_comment
                    else:
                        cur.batch_size(batch_size)

                    return cur, set_comment
                break
            # delay & retry once on AutoReconnect error
            except pymongo.errors.AutoReconnect:
                if i == (cls.MAX_AUTO_RECONNECT_TRIES - 1):
                    raise
                else:
                    _sleep(cls.AUTO_RECONNECT_SLEEP)

    @classmethod
    def explain(cls, spec, fields=None, skip=0, limit=0, sort=None,
             slave_ok=True, excluded_fields=None, max_time_ms=None,
             timeout_value=NO_TIMEOUT_DEFAULT,**kwargs):
        # If the client has been initialized, use the proxy
        proxy_client = cls._get_proxy_client()
        if proxy_client:
            if cls._get_read_decider():
                if 'comment' not in kwargs or kwargs['comment'] is None:
                    kwargs['comment'] = MongoComment.get_comment()
                is_scatter_gather = cls.is_scatter_gather(spec)
                set_comment = cls.attach_trace(kwargs['comment'], is_scatter_gather)
                try:
                    return proxy_client.instance().explain(
                        cls, spec, fields=fields, skip=skip,
                        limit=limit, sort=sort, slave_ok=slave_ok,
                        excluded_fields=excluded_fields, max_time_ms=max_time_ms,
                        timeout_value=timeout_value, **kwargs
                    )
                finally:
                    cls.cleanup_trace(set_comment)
        raise Exception("Explain not supported")

    @classmethod
    def find(cls, spec, fields=None, skip=0, limit=0, sort=None,
             slave_ok=True, excluded_fields=None, max_time_ms=None,
             timeout_value=NO_TIMEOUT_DEFAULT,**kwargs):
        # If the client has been initialized, use the proxy
        proxy_client = cls._get_proxy_client()
        if proxy_client:
            if cls._get_read_decider():
                if 'comment' not in kwargs or kwargs['comment'] is None:
                    kwargs['comment'] = MongoComment.get_comment()
                is_scatter_gather = cls.is_scatter_gather(spec)
                set_comment = cls.attach_trace(kwargs['comment'], is_scatter_gather)
                try:
                    return proxy_client.instance().find(
                        cls, spec, fields=fields, skip=skip,
                        limit=limit, sort=sort, slave_ok=slave_ok,
                        excluded_fields=excluded_fields, max_time_ms=max_time_ms,
                        timeout_value=timeout_value, **kwargs
                    )
                finally:
                    cls.cleanup_trace(set_comment)

        for i in xrange(cls.MAX_AUTO_RECONNECT_TRIES):
            cur, set_comment = cls.find_raw(spec, fields, skip, limit, sort,
                               slave_ok=slave_ok,
                               excluded_fields=excluded_fields,
                               max_time_ms=max_time_ms,**kwargs)

            try:
                return [
                    cls._from_augmented_son(d, fields, excluded_fields)
                    for d in cls._iterate_cursor(cur)
                ]
            except pymongo.errors.ExecutionTimeout:
                execution_timeout_logger.info({
                    '_comment' : str(cur._Cursor__comment),
                    '_max_time_ms' : cur._Cursor__max_time_ms,
                })
                if cls.ALLOW_TIMEOUT_RETRY and (max_time_ms is None or \
                    max_time_ms < cls.MAX_TIME_MS):
                    return cls.find(
                        spec, fields=fields,
                        skip=skip, limit=limit,
                        sort=sort, slave_ok=slave_ok,
                        excluded_fields=excluded_fields,
                        max_time_ms=cls.RETRY_MAX_TIME_MS,
                        timeout_value=timeout_value,
                        **kwargs
                    )
                if timeout_value is not cls.NO_TIMEOUT_DEFAULT:
                    return timeout_value
                raise
            except pymongo.errors.AutoReconnect:
                if i == (cls.MAX_AUTO_RECONNECT_TRIES - 1):
                    raise
                else:
                    _sleep(cls.AUTO_RECONNECT_SLEEP)
            finally:
                cls.cleanup_trace(set_comment)

    @classmethod
    def find_iter(cls, spec, fields=None, skip=0, limit=0, sort=None,
                  slave_ok=True, timeout=True, batch_size=10000,
                  excluded_fields=None, max_time_ms=0, **kwargs):
        def _old_find_iter():
            last_doc = None
            cur, set_comment = cls.find_raw(spec, fields, skip, limit,
                               sort, slave_ok=slave_ok, timeout=timeout,
                               batch_size=batch_size,
                               excluded_fields=excluded_fields,
                               max_time_ms=max_time_ms,**kwargs)
            try:
                for doc in cls._iterate_cursor(cur):
                    try:
                        last_doc = cls._from_augmented_son(doc, fields, excluded_fields)
                        yield last_doc
                    except pymongo.errors.ExecutionTimeout:
                        execution_timeout_logger.info({
                            '_comment' : str(cur._Cursor__comment),
                            '_max_time_ms' : cur._Cursor__max_time_ms,
                         })
                        raise
            finally:
                cls.cleanup_trace(set_comment)

        # If the client has been initialized, use the proxy
        proxy_client = cls._get_proxy_client()
        if proxy_client:
            if cls._get_read_decider():
                if 'comment' not in kwargs or kwargs['comment'] is None:
                    kwargs['comment'] = MongoComment.get_comment()
                is_scatter_gather = cls.is_scatter_gather(spec)
                set_comment = cls.attach_trace(kwargs['comment'], is_scatter_gather)
                cls.cleanup_trace(set_comment)
                for doc in proxy_client.instance().find_iter(
                    cls, spec, fields=fields, skip=skip,
                    limit=limit, sort=sort, slave_ok=slave_ok,
                    batch_size=batch_size,
                    excluded_fields=excluded_fields, max_time_ms=max_time_ms,
                    **kwargs
                    ):
                    yield doc
            else:
                for doc in _old_find_iter():
                    yield doc
        else:
            for doc in _old_find_iter():
                yield doc

    @classmethod
    def aggregate(cls, pipeline=None, **kwargs):
        if kwargs.get('allowDiskUse', False):
            raise ValueError("Writing to temporary files is disabled. allowDiskUse=True is not allowed.")

        proxy_client = cls._get_proxy_client()
        if proxy_client:
            if cls._get_read_decider():
                results = []
                for doc in proxy_client.instance().aggregate(
                        cls, pipeline=pipeline):
                    results.append(doc)
                return {'result': results}
        result = cls._pymongo().aggregate(
            pipeline,
            read_preference=pymongo.read_preferences.ReadPreference.SECONDARY)
        if result:
            return result
        else:
            return {'result': []}

    @classmethod
    def distinct(cls, spec, key, fields=None, skip=0, limit=0, sort=None,
                 slave_ok=True, timeout=True, excluded_fields=None,
                 max_time_ms=None, timeout_value=NO_TIMEOUT_DEFAULT,
                 **kwargs):

        proxy_client = cls._get_proxy_client()
        if proxy_client:
            if cls._get_read_decider():
                key = cls._transform_key(key, cls)[0]
                return proxy_client.instance().distinct(
                    cls, spec, distinct_key=key, fields=fields, skip=skip,
                    limit=limit, sort=sort, slave_ok=slave_ok,
                    excluded_fields=excluded_fields, max_time_ms=max_time_ms,
                    timeout_value=timeout_value, **kwargs
                )

        cur, set_comment = cls.find_raw(spec, fields, skip, limit,
                           sort, slave_ok=slave_ok, timeout=timeout,
                           excluded_fields=excluded_fields,
                           max_time_ms=max_time_ms,**kwargs)

        try:
            return cur.distinct(cls._transform_key(key, cls)[0])
        except pymongo.errors.ExecutionTimeout:
            execution_timeout_logger.info({
                '_comment' : str(cur._Cursor__comment),
                '_max_time_ms' : cur._Cursor__max_time_ms,
            })
            if cls.ALLOW_TIMEOUT_RETRY and (max_time_ms is None or \
                max_time_ms < cls.MAX_TIME_MS):
                return cls.distinct(
                    spec, key, fields=fields,
                    skip=skip, limit=limit,
                    sort=sort, slave_ok=slave_ok,
                    timeout=timeout,
                    excluded_fields=excluded_fields,
                    max_time_ms=cls.RETRY_MAX_TIME_MS,
                    timeout_value=timeout_value,
                    **kwargs
                )
            if timeout_value is not cls.NO_TIMEOUT_DEFAULT:
                return timeout_value
            raise
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    def _iterate_cursor(cls, cur):
        """
            Iterates over a cursor, gracefully handling AutoReconnect exceptions
        """
        while True:
            with log_slow_event('getmore', cur.collection.name, None):
                # the StopIteration from .next() will bubble up and kill
                # this while loop
                doc = cur.next()

                # handle pymongo letting an error document slip through
                # (T18431 / CS-22167). convert it into an exception
                if '$err' in doc:
                    err_code = None
                    if 'code' in doc:
                        err_code = doc['code']

                    raise pymongo.errors.OperationFailure(doc['$err'],
                                                          err_code)
            yield doc

    @classmethod
    def find_one(cls, spec, fields=None, skip=0, sort=None, slave_ok=True,
                 excluded_fields=None, max_time_ms=None,
                 timeout_value=NO_TIMEOUT_DEFAULT, **kwargs):
        # If the client has been initialized, use the proxy
        proxy_client = cls._get_proxy_client()
        if proxy_client:
            if cls._get_read_decider():
                if 'comment' not in kwargs or kwargs['comment'] is None:
                    kwargs['comment'] = MongoComment.get_comment()
                kwargs['find_one'] = True
                is_scatter_gather = cls.is_scatter_gather(spec)
                set_comment = cls.attach_trace(kwargs['comment'], is_scatter_gather)
                try:
                    return proxy_client.instance().find(
                        cls, spec, fields=fields, skip=skip,
                        sort=sort, slave_ok=slave_ok,
                        excluded_fields=excluded_fields, max_time_ms=max_time_ms,
                        timeout_value=timeout_value, **kwargs
                    )
                finally:
                    cls.cleanup_trace(set_comment)

        cur, set_comment = cls.find_raw(spec, fields, skip=skip, sort=sort,
                         slave_ok=slave_ok, find_one=True,
                         excluded_fields=excluded_fields,
                         max_time_ms=max_time_ms,**kwargs)

        try:
            if cur:
                return cls._from_augmented_son(cur, fields, excluded_fields)
            else:
                return None
        except pymongo.errors.ExecutionTimeout:
            execution_timeout_logger.info({
                '_comment' : str(cur._Cursor__comment),
                '_max_time_ms' : cur._Cursor__max_time_ms,
            })
            if cls.ALLOW_TIMEOUT_RETRY and (max_time_ms is None or \
                max_time_ms < cls.MAX_TIME_MS):
                return cls.find_one(
                    spec, fields=fields,
                    skip=skip,
                    sort=sort, slave_ok=slave_ok,
                    excluded_fields=excluded_fields,
                    max_time_ms=cls.RETRY_MAX_TIME_MS,
                    timeout_value=timeout_value,
                    **kwargs
                )
            if timeout_value is not cls.NO_TIMEOUT_DEFAULT:
                return timeout_value
            raise
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    def find_and_modify(cls, spec, update=None, sort=None, remove=False,
                        new=False, fields=None, upsert=False,
                        excluded_fields=None, skip_transform=False, **kwargs):
        if skip_transform:
            if update is None and not remove:
                raise ValueError("Cannot have empty update and no remove flag")
            if fields or excluded_fields:
                raise ValueError("Cannot specify fields or excluded fields when using skip_transform=True")
            transformed_fields = None
        else:
            spec = cls._transform_value(spec, cls)
            if update is not None:
                update = cls._transform_value(update, cls, op='$set')
            elif not remove:
                raise ValueError("Cannot have empty update and no remove flag")

            # handle queries with inheritance
            spec = cls._update_spec(spec, **kwargs)
            if sort is None:
                sort = {}
            else:
                new_sort = {}
                for f, dir in sort.iteritems():
                    f, _ = cls._transform_key(f, cls)
                    new_sort[f] = dir

                sort = new_sort

            transformed_fields = cls._transform_fields(fields, excluded_fields)

        is_scatter_gather = cls.is_scatter_gather(spec)

        set_comment = cls.attach_trace(
            MongoComment.get_query_comment(), is_scatter_gather)

        proxy_client = cls._get_proxy_client()
        if proxy_client:
            if cls._get_write_decider():
                try:
                    return proxy_client.instance().find_and_modify(
                        cls, spec, sort=sort, remove=remove, update=update, new=new,
                        fields=fields, upsert=upsert, excluded_fields=excluded_fields,
                        **kwargs
                    )
                finally:
                    cls.cleanup_trace(set_comment)

        try:
            with log_slow_event("find_and_modify", cls._meta['collection'], spec):
                result = cls._pymongo().find_and_modify(
                    spec, sort=sort, remove=remove, update=update, new=new,
                    fields=transformed_fields, upsert=upsert, **kwargs
                )
            if result:
                return cls._from_augmented_son(result, fields, excluded_fields)
            else:
                return None
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    def count(cls, spec, slave_ok=True, comment=None, max_time_ms=None,
        timeout_value=NO_TIMEOUT_DEFAULT,**kwargs):

        # If the client has been initialized, use the proxy
        proxy_client = cls._get_proxy_client()
        if proxy_client:
            if cls._get_read_decider():
                if 'comment' not in kwargs or kwargs['comment'] is None:
                    kwargs['comment'] = MongoComment.get_comment()
                is_scatter_gather = cls.is_scatter_gather(spec)
                set_comment = cls.attach_trace(kwargs['comment'], is_scatter_gather)
                try:
                    return proxy_client.instance().count(
                        cls, spec, slave_ok=slave_ok,
                        max_time_ms=max_time_ms,
                        timeout_value=timeout_value, **kwargs
                    )
                finally:
                    cls.cleanup_trace(set_comment)

        kwargs['comment'] = comment

        cur, set_comment = cls.find_raw(spec, slave_ok=slave_ok,
            max_time_ms=max_time_ms, **kwargs)
        try:
            for i in xrange(cls.MAX_AUTO_RECONNECT_TRIES):
                try:
                    return cur.count()
                except pymongo.errors.AutoReconnect:
                    if i == (cls.MAX_AUTO_RECONNECT_TRIES - 1):
                        raise
                    else:
                        _sleep(cls.AUTO_RECONNECT_SLEEP)
                except pymongo.errors.ExecutionTimeout:
                    execution_timeout_logger.info({
                        '_comment' : str(cur._Cursor__comment),
                        '_max_time_ms' : cur._Cursor__max_time_ms,
                    })
                    if cls.ALLOW_TIMEOUT_RETRY and (max_time_ms is None or \
                    max_time_ms < cls.MAX_TIME_MS):
                        kwargs.pop('comment', None)
                        return cls.count(
                            spec, slave_ok=slave_ok,
                            comment=comment,
                            max_time_ms=cls.RETRY_MAX_TIME_MS,
                            timeout_value=timeout_value,
                            **kwargs
                        )
                    if timeout_value is not cls.NO_TIMEOUT_DEFAULT:
                        return timeout_value
                    raise
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    def update(cls, spec, document, upsert=False, multi=True,
                **kwargs):
        # updates behave like set instead of find (None)... this is relevant for
        # setting list values since you're setting the value of the whole list,
        # not an element inside the list (like in find)
        is_scatter_gather = cls.is_scatter_gather(spec)

        document = cls._transform_value(document, cls, op='$set')
        spec = cls._transform_value(spec, cls)

        if cls._bulk_op is not None:
            warnings.warn('Non-bulk update inside bulk operation')

        if not document:
            raise ValueError("Cannot do empty updates")

        if not spec:
            # send email to ops
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.connect(('8.8.8.8',80))
                ip_address = sock.getsockname()[0]

                # send email to ops
                tb_msg = []
                for file_name, line_num, call_fn, fn_line in \
                        traceback.extract_stack():
                    tb_msg.append('%s / %d / %s / %s' % (file_name, line_num,
                            call_fn, fn_line))
                trace_msg = "Subject: [Mongo] Empty Spec Update\n\n%s\n%s" % \
                    (ip_address, '\n'.join(tb_msg))
                smtpObj = smtplib.SMTP('localhost')
                smtpObj.sendmail('ubuntu@localhost', OPS_EMAIL, trace_msg)
            except:
                pass

            raise ValueError("Cannot do empty specs")


        spec = cls._update_spec(spec, **kwargs)

        proxy_client = cls._get_proxy_client()
        if proxy_client:
            if cls._get_write_decider():
                if 'comment' not in kwargs or kwargs['comment'] is None:
                    kwargs['comment'] = MongoComment.get_comment()
                return proxy_client.instance().update(
                    cls, spec, document, upsert=upsert, multi=multi,
                    w=cls._meta['write_concern'], **kwargs
                )

        set_comment = cls.attach_trace(
            MongoComment.get_query_comment(), is_scatter_gather)

        try:
            with log_slow_event("update", cls._meta['collection'], spec):
                result = cls._pymongo().update(spec,
                                               document,
                                               upsert=upsert,
                                               multi=multi,
                                               w=cls._meta['write_concern'],
                                               **kwargs)
            return result
        finally:
            cls.cleanup_trace(set_comment)

    @classmethod
    def remove(cls, spec, **kwargs):
        if not spec:
            raise ValueError("Cannot do empty specs")

        is_scatter_gather = cls.is_scatter_gather(spec)
        set_comment = cls.attach_trace(
            MongoComment.get_query_comment(), is_scatter_gather)
        # transform query
        spec = cls._transform_value(spec, cls)
        spec = cls._update_spec(spec, **kwargs)

        proxy_client = cls._get_proxy_client()
        if proxy_client:
            if cls._get_write_decider():
                if 'comment' not in kwargs or kwargs['comment'] is None:
                    kwargs['comment'] = MongoComment.get_comment()
                return proxy_client.instance().remove(
                    cls, spec,
                    w=cls._meta['write_concern'], **kwargs
                )


        try:
            with log_slow_event("remove", cls._meta['collection'], spec):
                result = cls._pymongo().remove(
                    spec,
                    w=cls._meta['write_concern'],
                    **kwargs
                )
            return result
        finally:
            cls.cleanup_trace(set_comment)

    def update_one(self, document, spec=None, upsert=False,
                   criteria=None, comment=None,
                   **kwargs):
        ops = {}

        if not document:
            raise ValueError("Cannot do empty updates")

        # only do in-memory updates if criteria is None since the updates may
        # not be correct otherwise (since we don't know if the criteria is
        # matched)
        if not criteria:
            for operator, operand in document.iteritems():
                # safety check - these updates should only have atomic ops
                if operator[0] != '$':
                    raise ValueError("All updates should be atomic operators")

                if '.' not in operand:
                    for field, new_val in operand.iteritems():
                        # for now, skip doing in-memory sets on dicts
                        if '.' in field:
                            continue

                        try:
                            field_loaded = self.field_is_loaded(field)
                        except KeyError:
                            raise ValueError('Field does not exist')

                        if operator == '$set':
                            ops[field] = new_val
                        elif operator == '$unset':
                            ops[field] = None
                        elif operator == '$inc':
                            if field_loaded:
                                if self[field] is None:
                                    ops[field] = new_val
                                else:
                                    ops[field] = self[field] + new_val
                        elif operator == '$push':
                            if field_loaded:
                                ops[field] = self[field][:] + [new_val]
                        elif operator == '$pushAll':
                            if field_loaded:
                                ops[field] = self[field][:] + new_val
                        elif operator == '$addToSet':
                            if field_loaded:
                                if isinstance(new_val, dict) and '$each' in \
                                   new_val:
                                    vals_to_add = new_val['$each']
                                else:
                                    vals_to_add = [new_val]

                                for val in vals_to_add:
                                    if self[field] is not None and new_val not in self[field]:
                                        ops[field] = self[field][:] + [val]
                                    elif self[field] is None:
                                        ops[field] = [val]

        document = self._transform_value(document, type(self))
        query_spec = self._update_one_key()

        # add in extra criteria, if it exists
        self._allow_unloaded = True
        try:
            for field in self.INCLUDE_SHARD_KEY:
                value = getattr(self, field)
                if value:
                    if criteria is None:
                        criteria = {}
                    criteria[field] = value
        finally:
            self._allow_unloaded = False

        if criteria:
            query_spec.update(criteria)

        if spec:
            query_spec.update(spec)

        if not comment:
            comment = MongoComment.get_query_comment()

        is_scatter_gather = self.is_scatter_gather(
            query_spec)

        query_spec['$comment'] = comment
        query_spec = self._transform_value(query_spec, type(self))

        proxy_client = self._get_proxy_client()
        if proxy_client:
            if self._get_write_decider():
                if 'comment' not in kwargs or kwargs['comment'] is None:
                    kwargs['comment'] = MongoComment.get_comment()
                result = proxy_client.instance().update(
                    self.__class__, query_spec, document, upsert=upsert, multi=False,
                    w=self._meta['write_concern'], **kwargs
                )
                # do in-memory updates on the object if the query succeeded
                if result['n'] == 1:
                    for field, new_val in ops.iteritems():
                        self[field] = new_val

                return result

        set_comment = self.attach_trace(comment, is_scatter_gather)
        try:
            with log_slow_event("update_one", self._meta['collection'], spec):
                result = self._pymongo().update(query_spec,
                                                document,
                                                upsert=upsert,
                                                multi=False,
                                                w=self._meta['write_concern'],
                                                **kwargs)

            # do in-memory updates on the object if the query succeeded
            if result['n'] == 1:
                for field, new_val in ops.iteritems():
                    self[field] = new_val

            return result
        finally:
            self.cleanup_trace(set_comment)

    def set(self, **kwargs):
        return self.update_one({'$set': kwargs})

    def unset(self, **kwargs):
        return self.update_one({'$unset': kwargs})

    def inc(self, **kwargs):
        return self.update_one({'$inc': kwargs})

    def push(self, **kwargs):
        return self.update_one({'$push': kwargs})

    def pull(self, **kwargs):
        return self.update_one({'$pull': kwargs})

    def add_to_set(self, **kwargs):
        return self.update_one({'$addToSet': kwargs})

    @classmethod
    def drop_collection(cls):
        cls._pymongo().drop()

    def _transform_query(self, query, validate=True):
        cls = type(self)
        return cls._transform_value(query, cls, validate=validate)

    @staticmethod
    def _transform_value(value, context, op=None, validate=True, fields=False,
            embeddeddoc=False):
        from fields import DictField, EmbeddedDocumentField, ListField, \
                           ArbitraryField

        VALIDATE_OPS = ['$set', '$inc', None, '$eq', '$gte', '$lte', '$lt',
                        '$gt', '$ne', '$setOnInsert']
        SINGLE_LIST_OPS = [None, '$gt', '$lt', '$gte', '$lte', '$ne']
        LIST_VALIDATE_OPS = ['$addToSet', '$push', '$pull']
        LIST_VALIDATE_ALL_OPS = ['$pushAll', '$pullAll', '$each', '$in',
                                 '$nin', '$all']
        NO_VALIDATE_OPS = ['$unset', '$pop', '$rename', '$bit',
                           '$all', '$and', '$or', '$exists', '$mod',
                           '$elemMatch', '$size', '$type', '$not', '$returnKey',
                           '$maxScan', '$orderby', '$explain', '$snapshot',
                           '$max', '$min', '$showDiskLoc', '$hint', '$comment',
                           '$slice', '$options', '$regex', '$position']
        GEO_OPS = ['$geometry', '$geoWithin', '$geoIntersects', '$near',
                   '$nearSphere', '$maxDistance', '$minDistance']
        NO_VALIDATE_OPS += GEO_OPS

        base_op = op

        # handle geo operators
        if op == '$geometry':
            if isinstance(value, dict):
                if 'type' not in value or 'coordinates' not in value:
                    raise ValidationError("wrong GSON format: %s" % value)
                return SON(value)
            raise ValidationError("%s operator must take a GSON object" % op)
        elif op == '$maxDistance' or op == '$minDistance':
            if isinstance(value, dict) or isinstance(value, list):
                raise ValidationError("%s operator takes a single number" % op)
        # ensure GeoJSON format for other geo operators
        elif op in GEO_OPS:
            if isinstance(value, dict):
                if "$geometry" in value:
                    value = value
                elif "coordinates" in value and "type" in value:
                    value = {"$geometry": value}
                else:
                    raise ValidationError( "Invalid $geometry dictionary \
                    should have type and coordinates keys")
            # infer geometry type if given a list or set
            elif isinstance(value, (list, set)):
                try:
                    value[0][0][0]
                    return {"$geometry": {"type": "Polygon", "coordinates": value}}
                except (TypeError, IndexError):
                    pass

                try:
                    value[0][0]
                    return {"$geometry": {"type": "LineString", "coordinates": value}}
                except (TypeError, IndexError):
                    pass

                try:
                    value[0]
                    return {"$geometry": {"type": "Point", "coordinates": value}}
                except (TypeError, IndexError):
                    pass

                raise ValidationError(
                    "Invalid $geometry data. Can be either a \
                    dictionary or (nested) lists of coordinate(s)"
                )


        # recurse on list, unless we're at a ListField
        if isinstance(value, list) and not isinstance(context, ListField):
            transformed_list = []
            for listel in value:
                if isinstance(listel, dict) and not isinstance(context, DictField):
                    transformed_value = SON()

                    for key, subvalue in listel.iteritems():
                        if key[0] == '$':
                            op = key
                        else:
                            op = base_op

                        new_key, value_context = Document._transform_key(key, context,
                                                     is_find=(op is None))

                        transformed_value[new_key] = \
                            Document._transform_value(subvalue, value_context,
                                                      op, validate, fields)

                        transformed_list.append(transformed_value)
                else:
                    transformed_list.append(listel)
            value = transformed_list

        # recurse on dict, unless we're at a DictField
        if isinstance(value, dict) and not isinstance(context, DictField):
            transformed_value = SON()


            for key, subvalue in value.iteritems():
                embeddeddoc = False
                if key[0] == '$':
                    op = key
                else:
                    op = base_op

                if isinstance(context, ListField):
                    if isinstance(context.field, EmbeddedDocumentField):
                        context = context.field
                        embeddeddoc = True

                new_key, value_context = Document._transform_key(key, context,
                                             is_find=(op is None))

                transformed_value[new_key] = \
                    Document._transform_value(subvalue, value_context,
                                              op, validate, fields, embeddeddoc=embeddeddoc)


            # a tricky part: $maxDistance and $minDistance should be merged
            # into $near operator value
            if '$near' in transformed_value:
                if '$maxDistance' in transformed_value:
                    transformed_value['$near']['$maxDistance'] = \
                        transformed_value['$maxDistance']
                    del transformed_value['$maxDistance']
                if '$minDistance' in transformed_value:
                    transformed_value['$near']['$minDistance'] = \
                        transformed_value['$minDistance']
                    del transformed_value['$minDistance']
            if '$nearSphere' in transformed_value:
                if '$maxDistance' in transformed_value:
                    transformed_value['$nearSphere']['$maxDistance'] = \
                        transformed_value['$maxDistance']
                    del transformed_value['$maxDistance']
                if '$minDistance' in transformed_value:
                    transformed_value['$nearSphere']['$minDistance'] = \
                        transformed_value['$minDistance']
                    del transformed_value['$minDistance']

            return transformed_value
        # if we're in a dict field and there's operations on it, recurse
        elif isinstance(value, dict) and value and value.keys()[0][0] == '$':
            transformed_value = SON()

            for key, subvalue in value.iteritems():
                op = key

                new_key, value_context = Document._transform_key(key, context,
                                             is_find=(op is None))

                transformed_value[new_key] = \
                    Document._transform_value(subvalue, value_context,
                                              op, validate, fields)

            return transformed_value
        # else, validate & return
        else:
            if isinstance(context, CLSContext):
                return value
            op_type = None
            # there's a special case here, since some ops on lists
            # behaves like a LIST_VALIDATE_OP (i.e. it has "x in list" instead
            # of "x = list" semantics or x not in list, etc).
            if op in LIST_VALIDATE_ALL_OPS or \
                    (op is None and
                     context._in_list and
                     (isinstance(value, list) or
                      isinstance(value, tuple))):
                op_type = 'list_all'
            elif op in LIST_VALIDATE_OPS or \
                   (op in SINGLE_LIST_OPS and isinstance(context, ListField)):
                op_type = 'list'
            elif op in VALIDATE_OPS:
                op_type = 'value'

            if op_type in ('list',) and embeddeddoc:
                op_type = 'value'

            value = Document._transform_id_reference_value(value, context,
                                                           op_type)

            if validate and not isinstance(context, ArbitraryField):
                # the caveat to the above is that those semantics are modified if
                # the value is a list. technically this isn't completely correct
                # since passing a list has a semantic of field == value OR value
                # IN field (the underlying implementation is probably that all
                # queries have (== or IN) semantics, but it's only relevant for
                # lists). so, this code won't work in a list of lists case where
                # you want to match lists on value
                if op in LIST_VALIDATE_ALL_OPS or \
                        (op is None and
                         context._in_list and
                         (isinstance(value, list) or
                          isinstance(value, tuple))):
                    for entry in value:
                        if isinstance(context, ListField):
                            context.field.validate(entry)
                        else:
                            context.validate(entry)
                # same special case as above (for {list: x} meaning "x in list")
                elif op in LIST_VALIDATE_OPS or \
                      (op in SINGLE_LIST_OPS and isinstance(context, ListField)):
                    if not isinstance(context, ListField):
                        context.validate(value)
                    else:
                        context.field.validate(value)
                elif op in VALIDATE_OPS:
                    context.validate(value)
                elif op not in NO_VALIDATE_OPS:
                    raise ValidationError("Unknown atomic operator %s" % op)

            # handle $slice by enforcing negative int
            if op == '$slice':
                if fields:
                    if not ((isinstance(value, list) or \
                            isinstance(value, tuple)) and len(value) == 2) \
                            and not isinstance(value, int):
                        raise ValidationError("Projection slices must be "\
                                "2-lists or ints")
                elif not isinstance(value, int) or value > 0:
                    raise ValidationError("Slices must be negative ints")

            # handle EmbeddedDocuments
            elif isinstance(value, BaseDocument):
                value = value.to_mongo()

            # handle EmbeddedDocuments in lists
            elif isinstance(value, list):
                value = [v.to_mongo() if isinstance(v, BaseDocument) else v\
                    for v in value]

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
    def _transform_id_reference_value(value, context, op_type):
        """
            Transform strings/documents into ObjectIds / DBRefs when appropriate

            This is slightly tricky because there are List(ReferenceField) and
            List(ObjectIdField) and you sometimes get lists of documents/strings
            that need conversion.

            op_type is 'value' (if it's an individual value), 'list_all' (if it's a
            list of values), 'list' (if it's going into a list but is an individual
            value), or None (if it's neither).

            If no conversion is necessary, just return the original value
        """

        from fields import ReferenceField, ObjectIdField, ListField

        # not an op we can work with
        if not op_type:
            return value

        if isinstance(context, ListField):
            f = context.field
        else:
            f = context

        if not isinstance(f, ObjectIdField) and \
           not isinstance(f, ReferenceField):
            return value

        # the logic is a bit complicated here. there are a few different
        # variables at work. the op can be value, list, or list_all and it can
        # be done on a list or on a single value. the actions are either we do
        # single conversion or we need to convert each element in a list.
        #
        # see _transform_value for the logic on what's a value, list, or
        # list_all.
        #
        # here's the matrix:
        #
        # op         type     outcome
        # --------   ------   -----------
        # value      list     convert all
        # list       list     convert one
        # list_all   list     convert all
        # value      single   convert one
        # list       single   invalid
        # list_all   single   convert all

        if not isinstance(context, ListField) and op_type == 'list':
            raise ValidationError("Can't do list operations on non-lists")

        if op_type == 'list_all' or \
           (isinstance(context, ListField) and op_type == 'value'):
            if not isinstance(value, list) and not isinstance(value, tuple):
                raise ValidationError("Expecting list, not value")

            if isinstance(f, ReferenceField):
                new_value = []

                for v in value:
                    if isinstance(v, DBRef):
                        new_value.append(v)
                    elif isinstance(v, Document):
                        new_value.append(DBRef(type(v)._meta['collection'], v.id))
                    else:
                        raise ValidationError("Invalid ReferenceField value")

                return new_value
            else:
                return [ObjectId(v) for v in value]
        else:
            if isinstance(value, list) or isinstance(value, tuple):
                raise ValidationError("Expecting value, not list")

            if isinstance(f, ReferenceField):
                if isinstance(value, DBRef):
                    return value
                return DBRef(type(value)._meta['collection'], value.id)
            else:
                if value is None and not f.primary_key:
                    return value
                return ObjectId(value)

        raise AssertionError("Failed to convert")

    @staticmethod
    def _transform_key(key, context, prefix='', is_find=False):
        from fields import BaseField, DictField, ListField, \
                            EmbeddedDocumentField, ArbitraryField

        parts = key.split('.', 1)
        first_part = parts[0]

        if len(parts) > 1:
            rest = parts[1]
        else:
            rest = None

        # a key as a digit means a list index... set context as the list's value
        if first_part.isdigit() or first_part == '$':
            if isinstance(context, DictField):
                context = ArbitraryField()
            if getattr(context, "field", None) is not None:
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
                return Document._transform_key(rest, context, prefix=new_prefix, is_find=is_find)
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
            field._in_list = True

        # if we hit a DictField, values can be anything, so use the sentinal
        # ArbitraryField value (I prefer this over None, since None can be
        # introduced in other ways that would be considered errors & should not
        # be silently ignored)
        elif is_subclass_or_instance(context, DictField):
            field = ArbitraryField()
        elif is_subclass_or_instance(context, ArbitraryField):
            field = context

        if not field:
            # This is a hack for subclassed EmbeddedDocuments. Since we validate
            # on the top level EmbeddedDocument, and it might not have all the fields
            # pull the info from the registry and use that instead.
            if isinstance(context, EmbeddedDocumentField):
                potential_fields = get_embedded_doc_fields(context.document_type)
                if first_part in potential_fields:
                    return ".".join([prefix,potential_fields[first_part].db_field]), potential_fields[first_part]
                if first_part == '_cls':
                    return ".".join([prefix,'_cls']), CLSContext()
            raise ValueError("Can't find field %s" % first_part)

        # another unfortunate hack... in find queries "list.field_name" means
        # field_name inside of the list's field... but in updates,
        # list.0.field_name means that... need to differentiate here
        list_field_name = None
        if is_subclass_or_instance(field, ListField) and is_find:
            list_field_name = field.db_field
            if is_subclass_or_instance(field.field, basestring):
                field = get_document(field.field)
            elif is_subclass_or_instance(field.field, BaseField):
                field = field.field
            else:
                raise ValueError("Can't parse field %s" % first_part)
            field._in_list = True

        if is_subclass_or_instance(field, ArbitraryField):
            db_field = first_part
        elif list_field_name:
            db_field = list_field_name
        else:
            db_field = field.db_field

        if prefix:
            if db_field is not None:
                result = "%s.%s" % (prefix, db_field)
            else:
                result = prefix
                rest = key

        else:
            result = db_field

        if rest:
            return Document._transform_key(rest, field, prefix=result, is_find=is_find)
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
