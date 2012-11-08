import copy
import itertools
import operator
import pprint
import re
import warnings

from bson.code import Code
from bson import json_util
import pymongo
from pymongo.common import validate_read_preference

from mongoengine import signals
from mongoengine.common import _import_class
from mongoengine.errors import (OperationError, NotUniqueError,
                                InvalidQueryError)

from . import transform
from .field_list import QueryFieldList
from .visitor import Q


__all__ = ('QuerySet', 'DO_NOTHING', 'NULLIFY', 'CASCADE', 'DENY', 'PULL')

# The maximum number of items to display in a QuerySet.__repr__
REPR_OUTPUT_SIZE = 20

# Delete rules
DO_NOTHING = 0
NULLIFY = 1
CASCADE = 2
DENY = 3
PULL = 4

RE_TYPE = type(re.compile(''))


class QuerySet(object):
    """A set of results returned from a query. Wraps a MongoDB cursor,
    providing :class:`~mongoengine.Document` objects as the results.
    """
    __dereference = False
    __none = False

    def __init__(self, document, collection):
        self._document = document
        self._collection_obj = collection
        self._mongo_query = None
        self._query_obj = Q()
        self._initial_query = {}
        self._where_clause = None
        self._loaded_fields = QueryFieldList()
        self._ordering = []
        self._snapshot = False
        self._timeout = True
        self._class_check = True
        self._slave_okay = False
        self._read_preference = None
        self._iter = False
        self._scalar = []

        # If inheritance is allowed, only return instances and instances of
        # subclasses of the class being used
        if document._meta.get('allow_inheritance') == True:
            self._initial_query = {"_cls": {"$in": self._document._subclasses}}
            self._loaded_fields = QueryFieldList(always_include=['_cls'])
        self._cursor_obj = None
        self._limit = None
        self._skip = None
        self._hint = -1  # Using -1 as None is a valid value for hint

    def clone(self):
        """Creates a copy of the current
          :class:`~mongoengine.queryset.QuerySet`

        .. versionadded:: 0.5
        """
        c = self.__class__(self._document, self._collection_obj)

        copy_props = ('_initial_query', '_query_obj', '_where_clause',
                    '_loaded_fields', '_ordering', '_snapshot',
                    '_timeout', '_limit', '_skip', '_slave_okay', '_hint',
                    '_read_preference')

        for prop in copy_props:
            val = getattr(self, prop)
            setattr(c, prop, copy.deepcopy(val))

        return c

    @property
    def _query(self):
        if self._mongo_query is None:
            self._mongo_query = self._query_obj.to_query(self._document)
            if self._class_check:
                self._mongo_query.update(self._initial_query)
        return self._mongo_query

    def __call__(self, q_obj=None, class_check=True, slave_okay=False,
                 read_preference=None, **query):
        """Filter the selected documents by calling the
        :class:`~mongoengine.queryset.QuerySet` with a query.

        :param q_obj: a :class:`~mongoengine.queryset.Q` object to be used in
            the query; the :class:`~mongoengine.queryset.QuerySet` is filtered
            multiple times with different :class:`~mongoengine.queryset.Q`
            objects, only the last one will be used
        :param class_check: If set to False bypass class name check when
            querying collection
        :param slave_okay: if True, allows this query to be run against a
            replica secondary.
        :params read_preference: if set, overrides connection-level
            read_preference from `ReplicaSetConnection`.
        :param query: Django-style query keyword arguments
        """
        query = Q(**query)
        if q_obj:
            query &= q_obj
        self._query_obj &= query
        self._mongo_query = None
        self._cursor_obj = None
        if read_preference is not None:
            self.read_preference(read_preference)
        self._class_check = class_check
        return self

    def filter(self, *q_objs, **query):
        """An alias of :meth:`~mongoengine.queryset.QuerySet.__call__`
        """
        return self.__call__(*q_objs, **query)

    def all(self):
        """Returns all documents."""
        return self.__call__()

    def ensure_index(self, **kwargs):
        """Deprecated use :func:`~Document.ensure_index`"""
        msg = ("Doc.objects()._ensure_index() is deprecated. "
              "Use Doc.ensure_index() instead.")
        warnings.warn(msg, DeprecationWarning)
        self._document.__class__.ensure_index(**kwargs)
        return self

    def _ensure_indexes(self):
        """Deprecated use :func:`~Document.ensure_indexes`"""
        msg = ("Doc.objects()._ensure_indexes() is deprecated. "
              "Use Doc.ensure_indexes() instead.")
        warnings.warn(msg, DeprecationWarning)
        self._document.__class__.ensure_indexes()

    @property
    def _collection(self):
        """Property that returns the collection object. This allows us to
        perform operations only if the collection is accessed.
        """
        return self._collection_obj

    @property
    def _cursor_args(self):
        cursor_args = {
            'snapshot': self._snapshot,
            'timeout': self._timeout,
            'slave_okay': self._slave_okay,
        }
        if self._read_preference is not None:
            cursor_args['read_preference'] = self._read_preference
        if self._loaded_fields:
            cursor_args['fields'] = self._loaded_fields.as_dict()
        return cursor_args

    @property
    def _cursor(self):
        if self._cursor_obj is None:

            self._cursor_obj = self._collection.find(self._query,
                                                     **self._cursor_args)
            # Apply where clauses to cursor
            if self._where_clause:
                self._cursor_obj.where(self._where_clause)

            # apply default ordering
            if self._ordering:
                self._cursor_obj.sort(self._ordering)
            elif self._document._meta['ordering']:
                self.order_by(*self._document._meta['ordering'])

            if self._limit is not None:
                self._cursor_obj.limit(self._limit - (self._skip or 0))

            if self._skip is not None:
                self._cursor_obj.skip(self._skip)

            if self._hint != -1:
                self._cursor_obj.hint(self._hint)
        return self._cursor_obj

    def get(self, *q_objs, **query):
        """Retrieve the the matching object raising
        :class:`~mongoengine.queryset.MultipleObjectsReturned` or
        `DocumentName.MultipleObjectsReturned` exception if multiple results
        and :class:`~mongoengine.queryset.DoesNotExist` or
        `DocumentName.DoesNotExist` if no results are found.

        .. versionadded:: 0.3
        """
        self.limit(2)
        self.__call__(*q_objs, **query)
        try:
            result = self.next()
        except StopIteration:
            msg = ("%s matching query does not exist."
                    % self._document._class_name)
            raise self._document.DoesNotExist(msg)
        try:
            self.next()
        except StopIteration:
            return result

        self.rewind()
        message = u'%d items returned, instead of 1' % self.count()
        raise self._document.MultipleObjectsReturned(message)

    def get_or_create(self, write_options=None, auto_save=True,
                      *q_objs, **query):
        """Retrieve unique object or create, if it doesn't exist. Returns a
        tuple of ``(object, created)``, where ``object`` is the retrieved or
        created object and ``created`` is a boolean specifying whether a new
        object was created. Raises
        :class:`~mongoengine.queryset.MultipleObjectsReturned` or
        `DocumentName.MultipleObjectsReturned` if multiple results are found.
        A new document will be created if the document doesn't exists; a
        dictionary of default values for the new document may be provided as a
        keyword argument called :attr:`defaults`.

        .. warning:: This requires two separate operations and therefore a
            race condition exists.  Because there are no transactions in
            mongoDB other approaches should be investigated, to ensure you
            don't accidently duplicate data when using this method.

        :param write_options: optional extra keyword arguments used if we
            have to create a new document.
            Passes any write_options onto :meth:`~mongoengine.Document.save`

        :param auto_save: if the object is to be saved automatically if
            not found.

        .. versionchanged:: 0.6 - added `auto_save`
        .. versionadded:: 0.3
        """
        defaults = query.get('defaults', {})
        if 'defaults' in query:
            del query['defaults']

        try:
            doc = self.get(*q_objs, **query)
            return doc, False
        except self._document.DoesNotExist:
            query.update(defaults)
            doc = self._document(**query)

            if auto_save:
                doc.save(write_options=write_options)
            return doc, True

    def create(self, **kwargs):
        """Create new object. Returns the saved object instance.

        .. versionadded:: 0.4
        """
        doc = self._document(**kwargs)
        doc.save()
        return doc

    def first(self):
        """Retrieve the first object matching the query.
        """
        try:
            result = self[0]
        except IndexError:
            result = None
        return result

    def insert(self, doc_or_docs, load_bulk=True, safe=False,
               write_options=None):
        """bulk insert documents

        If ``safe=True`` and the operation is unsuccessful, an
        :class:`~mongoengine.OperationError` will be raised.

        :param docs_or_doc: a document or list of documents to be inserted
        :param load_bulk (optional): If True returns the list of document
            instances
        :param safe: check if the operation succeeded before returning
        :param write_options: Extra keyword arguments are passed down to
                :meth:`~pymongo.collection.Collection.insert`
                which will be used as options for the resultant
                ``getLastError`` command.  For example,
                ``insert(..., {w: 2, fsync: True})`` will wait until at least
                two servers have recorded the write and will force an fsync on
                each server being written to.

        By default returns document instances, set ``load_bulk`` to False to
        return just ``ObjectIds``

        .. versionadded:: 0.5
        """
        Document = _import_class('Document')

        if not write_options:
            write_options = {}
        write_options.update({'safe': safe})

        docs = doc_or_docs
        return_one = False
        if isinstance(docs, Document) or issubclass(docs.__class__, Document):
            return_one = True
            docs = [docs]

        raw = []
        for doc in docs:
            if not isinstance(doc, self._document):
                msg = ("Some documents inserted aren't instances of %s"
                        % str(self._document))
                raise OperationError(msg)
            if doc.pk:
                msg = "Some documents have ObjectIds use doc.update() instead"
                raise OperationError(msg)
            raw.append(doc.to_mongo())

        signals.pre_bulk_insert.send(self._document, documents=docs)
        try:
            ids = self._collection.insert(raw, **write_options)
        except pymongo.errors.OperationFailure, err:
            message = 'Could not save document (%s)'
            if re.match('^E1100[01] duplicate key', unicode(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = u'Tried to save duplicate unique keys (%s)'
                raise NotUniqueError(message % unicode(err))
            raise OperationError(message % unicode(err))

        if not load_bulk:
            signals.post_bulk_insert.send(
                    self._document, documents=docs, loaded=False)
            return return_one and ids[0] or ids

        documents = self.in_bulk(ids)
        results = []
        for obj_id in ids:
            results.append(documents.get(obj_id))
        signals.post_bulk_insert.send(
                self._document, documents=results, loaded=True)
        return return_one and results[0] or results

    def with_id(self, object_id):
        """Retrieve the object matching the id provided.  Uses `object_id` only
        and raises InvalidQueryError if a filter has been applied.

        :param object_id: the value for the id of the document to look up

        .. versionchanged:: 0.6 Raises InvalidQueryError if filter has been set
        """
        if not self._query_obj.empty:
            msg = "Cannot use a filter whilst using `with_id`"
            raise InvalidQueryError(msg)
        return self.filter(pk=object_id).first()

    def in_bulk(self, object_ids):
        """Retrieve a set of documents by their ids.

        :param object_ids: a list or tuple of ``ObjectId``\ s
        :rtype: dict of ObjectIds as keys and collection-specific
                Document subclasses as values.

        .. versionadded:: 0.3
        """
        doc_map = {}

        docs = self._collection.find({'_id': {'$in': object_ids}},
                                     **self._cursor_args)
        if self._scalar:
            for doc in docs:
                doc_map[doc['_id']] = self._get_scalar(
                        self._document._from_son(doc))
        else:
            for doc in docs:
                doc_map[doc['_id']] = self._document._from_son(doc)

        return doc_map

    def next(self):
        """Wrap the result in a :class:`~mongoengine.Document` object.
        """
        self._iter = True
        try:
            if self._limit == 0 or self.__none:
                raise StopIteration
            if self._scalar:
                return self._get_scalar(self._document._from_son(
                        self._cursor.next()))
            return self._document._from_son(self._cursor.next())
        except StopIteration, e:
            self.rewind()
            raise e

    def rewind(self):
        """Rewind the cursor to its unevaluated state.

        .. versionadded:: 0.3
        """
        self._iter = False
        self._cursor.rewind()

    def none(self):
        """Helper that just returns a list"""
        self.__none = True
        return self

    def count(self):
        """Count the selected elements in the query.
        """
        if self._limit == 0:
            return 0
        return self._cursor.count(with_limit_and_skip=True)

    def __len__(self):
        return self.count()

    def map_reduce(self, map_f, reduce_f, output, finalize_f=None, limit=None,
                   scope=None):
        """Perform a map/reduce query using the current query spec
        and ordering. While ``map_reduce`` respects ``QuerySet`` chaining,
        it must be the last call made, as it does not return a maleable
        ``QuerySet``.

        See the :meth:`~mongoengine.tests.QuerySetTest.test_map_reduce`
        and :meth:`~mongoengine.tests.QuerySetTest.test_map_advanced`
        tests in ``tests.queryset.QuerySetTest`` for usage examples.

        :param map_f: map function, as :class:`~bson.code.Code` or string
        :param reduce_f: reduce function, as
                         :class:`~bson.code.Code` or string
        :param output: output collection name, if set to 'inline' will try to
           use :class:`~pymongo.collection.Collection.inline_map_reduce`
           This can also be a dictionary containing output options
           see: http://docs.mongodb.org/manual/reference/commands/#mapReduce
        :param finalize_f: finalize function, an optional function that
                           performs any post-reduction processing.
        :param scope: values to insert into map/reduce global scope. Optional.
        :param limit: number of objects from current query to provide
                      to map/reduce method

        Returns an iterator yielding
        :class:`~mongoengine.document.MapReduceDocument`.

        .. note::

            Map/Reduce changed in server version **>= 1.7.4**. The PyMongo
            :meth:`~pymongo.collection.Collection.map_reduce` helper requires
            PyMongo version **>= 1.11**.

        .. versionchanged:: 0.5
           - removed ``keep_temp`` keyword argument, which was only relevant
             for MongoDB server versions older than 1.7.4

        .. versionadded:: 0.3
        """
        MapReduceDocument = _import_class('MapReduceDocument')

        if not hasattr(self._collection, "map_reduce"):
            raise NotImplementedError("Requires MongoDB >= 1.7.1")

        map_f_scope = {}
        if isinstance(map_f, Code):
            map_f_scope = map_f.scope
            map_f = unicode(map_f)
        map_f = Code(self._sub_js_fields(map_f), map_f_scope)

        reduce_f_scope = {}
        if isinstance(reduce_f, Code):
            reduce_f_scope = reduce_f.scope
            reduce_f = unicode(reduce_f)
        reduce_f_code = self._sub_js_fields(reduce_f)
        reduce_f = Code(reduce_f_code, reduce_f_scope)

        mr_args = {'query': self._query}

        if finalize_f:
            finalize_f_scope = {}
            if isinstance(finalize_f, Code):
                finalize_f_scope = finalize_f.scope
                finalize_f = unicode(finalize_f)
            finalize_f_code = self._sub_js_fields(finalize_f)
            finalize_f = Code(finalize_f_code, finalize_f_scope)
            mr_args['finalize'] = finalize_f

        if scope:
            mr_args['scope'] = scope

        if limit:
            mr_args['limit'] = limit

        if output == 'inline' and not self._ordering:
            map_reduce_function = 'inline_map_reduce'
        else:
            map_reduce_function = 'map_reduce'
            mr_args['out'] = output

        results = getattr(self._collection, map_reduce_function)(
                            map_f, reduce_f, **mr_args)

        if map_reduce_function == 'map_reduce':
            results = results.find()

        if self._ordering:
            results = results.sort(self._ordering)

        for doc in results:
            yield MapReduceDocument(self._document, self._collection,
                                    doc['_id'], doc['value'])

    def limit(self, n):
        """Limit the number of returned documents to `n`. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[:5]``).

        :param n: the maximum number of objects to return
        """
        if n == 0:
            self._cursor.limit(1)
        else:
            self._cursor.limit(n)
        self._limit = n

        # Return self to allow chaining
        return self

    def skip(self, n):
        """Skip `n` documents before returning the results. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[5:]``).

        :param n: the number of objects to skip before returning results
        """
        self._cursor.skip(n)
        self._skip = n
        return self

    def hint(self, index=None):
        """Added 'hint' support, telling Mongo the proper index to use for the
        query.

        Judicious use of hints can greatly improve query performance. When
        doing a query on multiple fields (at least one of which is indexed)
        pass the indexed field as a hint to the query.

        Hinting will not do anything if the corresponding index does not exist.
        The last hint applied to this cursor takes precedence over all others.

        .. versionadded:: 0.5
        """
        self._cursor.hint(index)
        self._hint = index
        return self

    def __getitem__(self, key):
        """Support skip and limit using getitem and slicing syntax.
        """
        # Slice provided
        if isinstance(key, slice):
            try:
                self._cursor_obj = self._cursor[key]
                self._skip, self._limit = key.start, key.stop
            except IndexError, err:
                # PyMongo raises an error if key.start == key.stop, catch it,
                # bin it, kill it.
                start = key.start or 0
                if start >= 0 and key.stop >= 0 and key.step is None:
                    if start == key.stop:
                        self.limit(0)
                        self._skip, self._limit = key.start, key.stop - start
                        return self
                raise err
            # Allow further QuerySet modifications to be performed
            return self
        # Integer index provided
        elif isinstance(key, int):
            if self._scalar:
                return self._get_scalar(self._document._from_son(
                        self._cursor[key]))
            return self._document._from_son(self._cursor[key])
        raise AttributeError

    def distinct(self, field):
        """Return a list of distinct values for a given field.

        :param field: the field to select distinct values from

        .. versionadded:: 0.4
        .. versionchanged:: 0.5 - Fixed handling references
        .. versionchanged:: 0.6 - Improved db_field refrence handling
        """
        return self._dereference(self._cursor.distinct(field), 1,
                                 name=field, instance=self._document)

    def only(self, *fields):
        """Load only a subset of this document's fields. ::

            post = BlogPost.objects(...).only("title", "author.name")

        :param fields: fields to include

        .. versionadded:: 0.3
        .. versionchanged:: 0.5 - Added subfield support
        """
        fields = dict([(f, QueryFieldList.ONLY) for f in fields])
        return self.fields(**fields)

    def exclude(self, *fields):
        """Opposite to .only(), exclude some document's fields. ::

            post = BlogPost.objects(...).exclude("comments")

        :param fields: fields to exclude

        .. versionadded:: 0.5
        """
        fields = dict([(f, QueryFieldList.EXCLUDE) for f in fields])
        return self.fields(**fields)

    def fields(self, **kwargs):
        """Manipulate how you load this document's fields.  Used by `.only()`
        and `.exclude()` to manipulate which fields to retrieve.  Fields also
        allows for a greater level of control for example:

        Retrieving a Subrange of Array Elements:

        You can use the $slice operator to retrieve a subrange of elements in
        an array. For example to get the first 5 comments::

            post = BlogPost.objects(...).fields(slice__comments=5)

        :param kwargs: A dictionary identifying what to include

        .. versionadded:: 0.5
        """

        # Check for an operator and transform to mongo-style if there is
        operators = ["slice"]
        cleaned_fields = []
        for key, value in kwargs.items():
            parts = key.split('__')
            op = None
            if parts[0] in operators:
                op = parts.pop(0)
                value = {'$' + op: value}
            key = '.'.join(parts)
            cleaned_fields.append((key, value))

        fields = sorted(cleaned_fields, key=operator.itemgetter(1))
        for value, group in itertools.groupby(fields, lambda x: x[1]):
            fields = [field for field, value in group]
            fields = self._fields_to_dbfields(fields)
            self._loaded_fields += QueryFieldList(fields, value=value)
        return self

    def all_fields(self):
        """Include all fields. Reset all previously calls of .only() or
        .exclude(). ::

            post = BlogPost.objects.exclude("comments").all_fields()

        .. versionadded:: 0.5
        """
        self._loaded_fields = QueryFieldList(
            always_include=self._loaded_fields.always_include)
        return self

    def _fields_to_dbfields(self, fields):
        """Translate fields paths to its db equivalents"""
        ret = []
        for field in fields:
            field = ".".join(f.db_field for f in
                             self._document._lookup_field(field.split('.')))
            ret.append(field)
        return ret

    def order_by(self, *keys):
        """Order the :class:`~mongoengine.queryset.QuerySet` by the keys. The
        order may be specified by prepending each of the keys by a + or a -.
        Ascending order is assumed.

        :param keys: fields to order the query results by; keys may be
            prefixed with **+** or **-** to determine the ordering direction
        """
        key_list = []
        for key in keys:
            if not key:
                continue
            direction = pymongo.ASCENDING
            if key[0] == '-':
                direction = pymongo.DESCENDING
            if key[0] in ('-', '+'):
                key = key[1:]
            key = key.replace('__', '.')
            try:
                key = self._document._translate_field_name(key)
            except:
                pass
            key_list.append((key, direction))

        self._ordering = key_list
        self._cursor.sort(key_list)
        return self

    def explain(self, format=False):
        """Return an explain plan record for the
        :class:`~mongoengine.queryset.QuerySet`\ 's cursor.

        :param format: format the plan before returning it
        """

        plan = self._cursor.explain()
        if format:
            plan = pprint.pformat(plan)
        return plan

    def snapshot(self, enabled):
        """Enable or disable snapshot mode when querying.

        :param enabled: whether or not snapshot mode is enabled

        ..versionchanged:: 0.5 - made chainable
        """
        self._snapshot = enabled
        return self

    def timeout(self, enabled):
        """Enable or disable the default mongod timeout when querying.

        :param enabled: whether or not the timeout is used

        ..versionchanged:: 0.5 - made chainable
        """
        self._timeout = enabled
        return self

    def slave_okay(self, enabled):
        """Enable or disable the slave_okay when querying.

        :param enabled: whether or not the slave_okay is enabled
        """
        self._slave_okay = enabled
        return self

    def read_preference(self, read_preference):
        """Change the read_preference when querying.

        :param read_preference: override ReplicaSetConnection-level
            preference.
        """
        validate_read_preference('read_preference', read_preference)
        self._read_preference = read_preference
        return self

    def delete(self, safe=False):
        """Delete the documents matched by the query.

        :param safe: check if the operation succeeded before returning
        """
        doc = self._document

        # Handle deletes where skips or limits have been applied
        if self._skip or self._limit:
            for doc in self:
                doc.delete()
            return

        delete_rules = doc._meta.get('delete_rules') or {}
        # Check for DENY rules before actually deleting/nullifying any other
        # references
        for rule_entry in delete_rules:
            document_cls, field_name = rule_entry
            rule = doc._meta['delete_rules'][rule_entry]
            if rule == DENY and document_cls.objects(
                    **{field_name + '__in': self}).count() > 0:
                msg = ("Could not delete document (%s.%s refers to it)"
                        % (document_cls.__name__, field_name))
                raise OperationError(msg)

        for rule_entry in delete_rules:
            document_cls, field_name = rule_entry
            rule = doc._meta['delete_rules'][rule_entry]
            if rule == CASCADE:
                ref_q = document_cls.objects(**{field_name + '__in': self})
                ref_q_count = ref_q.count()
                if (doc != document_cls and ref_q_count > 0
                    or (doc == document_cls and ref_q_count > 0)):
                    ref_q.delete(safe=safe)
            elif rule == NULLIFY:
                document_cls.objects(**{field_name + '__in': self}).update(
                        safe_update=safe,
                        **{'unset__%s' % field_name: 1})
            elif rule == PULL:
                document_cls.objects(**{field_name + '__in': self}).update(
                        safe_update=safe,
                        **{'pull_all__%s' % field_name: self})

        self._collection.remove(self._query, safe=safe)

    def update(self, safe_update=True, upsert=False, multi=True,
               write_options=None, **update):
        """Perform an atomic update on the fields matched by the query. When
        ``safe_update`` is used, the number of affected documents is returned.

        :param safe_update: check if the operation succeeded before returning
        :param upsert: Any existing document with that "_id" is overwritten.
        :param write_options: extra keyword arguments for
            :meth:`~pymongo.collection.Collection.update`

        .. versionadded:: 0.2
        """
        if not update:
            raise OperationError("No update parameters, would remove data")

        if not write_options:
            write_options = {}

        update = transform.update(self._document, **update)
        query = self._query

        try:
            ret = self._collection.update(query, update, multi=multi,
                                          upsert=upsert, safe=safe_update,
                                          **write_options)
            if ret is not None and 'n' in ret:
                return ret['n']
        except pymongo.errors.OperationFailure, err:
            if unicode(err) == u'multi not coded yet':
                message = u'update() method requires MongoDB 1.1.3+'
                raise OperationError(message)
            raise OperationError(u'Update failed (%s)' % unicode(err))

    def update_one(self, safe_update=True, upsert=False, write_options=None,
                   **update):
        """Perform an atomic update on first field matched by the query. When
        ``safe_update`` is used, the number of affected documents is returned.

        :param safe_update: check if the operation succeeded before returning
        :param upsert: Any existing document with that "_id" is overwritten.
        :param write_options: extra keyword arguments for
            :meth:`~pymongo.collection.Collection.update`
        :param update: Django-style update keyword arguments

        .. versionadded:: 0.2
        """
        return self.update(safe_update=True, upsert=False, multi=False,
                           write_options=None, **update)

    def __iter__(self):
        self.rewind()
        return self

    def _get_scalar(self, doc):

        def lookup(obj, name):
            chunks = name.split('__')
            for chunk in chunks:
                obj = getattr(obj, chunk)
            return obj

        data = [lookup(doc, n) for n in self._scalar]
        if len(data) == 1:
            return data[0]

        return tuple(data)

    def scalar(self, *fields):
        """Instead of returning Document instances, return either a specific
        value or a tuple of values in order.

        This effects all results and can be unset by calling ``scalar``
        without arguments. Calls ``only`` automatically.

        :param fields: One or more fields to return instead of a Document.
        """
        self._scalar = list(fields)

        if fields:
            self.only(*fields)
        else:
            self.all_fields()

        return self

    def values_list(self, *fields):
        """An alias for scalar"""
        return self.scalar(*fields)

    def _sub_js_fields(self, code):
        """When fields are specified with [~fieldname] syntax, where
        *fieldname* is the Python name of a field, *fieldname* will be
        substituted for the MongoDB name of the field (specified using the
        :attr:`name` keyword argument in a field's constructor).
        """
        def field_sub(match):
            # Extract just the field name, and look up the field objects
            field_name = match.group(1).split('.')
            fields = self._document._lookup_field(field_name)
            # Substitute the correct name for the field into the javascript
            return u'["%s"]' % fields[-1].db_field

        def field_path_sub(match):
            # Extract just the field name, and look up the field objects
            field_name = match.group(1).split('.')
            fields = self._document._lookup_field(field_name)
            # Substitute the correct name for the field into the javascript
            return ".".join([f.db_field for f in fields])

        code = re.sub(u'\[\s*~([A-z_][A-z_0-9.]+?)\s*\]', field_sub, code)
        code = re.sub(u'\{\{\s*~([A-z_][A-z_0-9.]+?)\s*\}\}', field_path_sub,
                code)
        return code

    def exec_js(self, code, *fields, **options):
        """Execute a Javascript function on the server. A list of fields may be
        provided, which will be translated to their correct names and supplied
        as the arguments to the function. A few extra variables are added to
        the function's scope: ``collection``, which is the name of the
        collection in use; ``query``, which is an object representing the
        current query; and ``options``, which is an object containing any
        options specified as keyword arguments.

        As fields in MongoEngine may use different names in the database (set
        using the :attr:`db_field` keyword argument to a :class:`Field`
        constructor), a mechanism exists for replacing MongoEngine field names
        with the database field names in Javascript code. When accessing a
        field, use square-bracket notation, and prefix the MongoEngine field
        name with a tilde (~).

        :param code: a string of Javascript code to execute
        :param fields: fields that you will be using in your function, which
            will be passed in to your function as arguments
        :param options: options that you want available to the function
            (accessed in Javascript through the ``options`` object)
        """
        code = self._sub_js_fields(code)

        fields = [self._document._translate_field_name(f) for f in fields]
        collection = self._document._get_collection_name()

        scope = {
            'collection': collection,
            'options': options or {},
        }

        query = self._query
        if self._where_clause:
            query['$where'] = self._where_clause

        scope['query'] = query
        code = Code(code, scope=scope)

        db = self._document._get_db()
        return db.eval(code, *fields)

    def where(self, where_clause):
        """Filter ``QuerySet`` results with a ``$where`` clause (a Javascript
        expression). Performs automatic field name substitution like
        :meth:`mongoengine.queryset.Queryset.exec_js`.

        .. note:: When using this mode of query, the database will call your
                  function, or evaluate your predicate clause, for each object
                  in the collection.

        .. versionadded:: 0.5
        """
        where_clause = self._sub_js_fields(where_clause)
        self._where_clause = where_clause
        return self

    def sum(self, field):
        """Sum over the values of the specified field.

        :param field: the field to sum over; use dot-notation to refer to
            embedded document fields

        .. versionchanged:: 0.5 - updated to map_reduce as db.eval doesnt work
            with sharding.
        """
        map_func = Code("""
            function() {
                emit(1, this[field] || 0);
            }
        """, scope={'field': field})

        reduce_func = Code("""
            function(key, values) {
                var sum = 0;
                for (var i in values) {
                    sum += values[i];
                }
                return sum;
            }
        """)

        for result in self.map_reduce(map_func, reduce_func, output='inline'):
            return result.value
        else:
            return 0

    def average(self, field):
        """Average over the values of the specified field.

        :param field: the field to average over; use dot-notation to refer to
            embedded document fields

        .. versionchanged:: 0.5 - updated to map_reduce as db.eval doesnt work
            with sharding.
        """
        map_func = Code("""
            function() {
                if (this.hasOwnProperty(field))
                    emit(1, {t: this[field] || 0, c: 1});
            }
        """, scope={'field': field})

        reduce_func = Code("""
            function(key, values) {
                var out = {t: 0, c: 0};
                for (var i in values) {
                    var value = values[i];
                    out.t += value.t;
                    out.c += value.c;
                }
                return out;
            }
        """)

        finalize_func = Code("""
            function(key, value) {
                return value.t / value.c;
            }
        """)

        for result in self.map_reduce(map_func, reduce_func,
                            finalize_f=finalize_func, output='inline'):
            return result.value
        else:
            return 0

    def item_frequencies(self, field, normalize=False, map_reduce=True):
        """Returns a dictionary of all items present in a field across
        the whole queried set of documents, and their corresponding frequency.
        This is useful for generating tag clouds, or searching documents.

        .. note::

            Can only do direct simple mappings and cannot map across
            :class:`~mongoengine.ReferenceField` or
            :class:`~mongoengine.GenericReferenceField` for more complex
            counting a manual map reduce call would is required.

        If the field is a :class:`~mongoengine.ListField`, the items within
        each list will be counted individually.

        :param field: the field to use
        :param normalize: normalize the results so they add to 1.0
        :param map_reduce: Use map_reduce over exec_js

        .. versionchanged:: 0.5 defaults to map_reduce and can handle embedded
                            document lookups
        """
        if map_reduce:
            return self._item_frequencies_map_reduce(field,
                                                     normalize=normalize)
        return self._item_frequencies_exec_js(field, normalize=normalize)

    def _item_frequencies_map_reduce(self, field, normalize=False):
        map_func = """
            function() {
                var path = '{{~%(field)s}}'.split('.');
                var field = this;

                for (p in path) {
                    if (typeof field != 'undefined')
                       field = field[path[p]];
                    else
                       break;
                }
                if (field && field.constructor == Array) {
                    field.forEach(function(item) {
                        emit(item, 1);
                    });
                } else if (typeof field != 'undefined') {
                    emit(field, 1);
                } else {
                    emit(null, 1);
                }
            }
        """ % dict(field=field)
        reduce_func = """
            function(key, values) {
                var total = 0;
                var valuesSize = values.length;
                for (var i=0; i < valuesSize; i++) {
                    total += parseInt(values[i], 10);
                }
                return total;
            }
        """
        values = self.map_reduce(map_func, reduce_func, 'inline')
        frequencies = {}
        for f in values:
            key = f.key
            if isinstance(key, float):
                if int(key) == key:
                    key = int(key)
            frequencies[key] = int(f.value)

        if normalize:
            count = sum(frequencies.values())
            frequencies = dict([(k, float(v) / count)
                                for k, v in frequencies.items()])

        return frequencies

    def _item_frequencies_exec_js(self, field, normalize=False):
        """Uses exec_js to execute"""
        freq_func = """
            function(path) {
                var path = path.split('.');

                var total = 0.0;
                db[collection].find(query).forEach(function(doc) {
                    var field = doc;
                    for (p in path) {
                        if (field)
                            field = field[path[p]];
                         else
                            break;
                    }
                    if (field && field.constructor == Array) {
                       total += field.length;
                    } else {
                       total++;
                    }
                });

                var frequencies = {};
                var types = {};
                var inc = 1.0;

                db[collection].find(query).forEach(function(doc) {
                    field = doc;
                    for (p in path) {
                        if (field)
                            field = field[path[p]];
                        else
                            break;
                    }
                    if (field && field.constructor == Array) {
                        field.forEach(function(item) {
                            frequencies[item] = inc + (isNaN(frequencies[item]) ? 0: frequencies[item]);
                        });
                    } else {
                        var item = field;
                        types[item] = item;
                        frequencies[item] = inc + (isNaN(frequencies[item]) ? 0: frequencies[item]);
                    }
                });
                return [total, frequencies, types];
            }
        """
        total, data, types = self.exec_js(freq_func, field)
        values = dict([(types.get(k), int(v)) for k, v in data.iteritems()])

        if normalize:
            values = dict([(k, float(v) / total) for k, v in values.items()])

        frequencies = {}
        for k, v in values.iteritems():
            if isinstance(k, float):
                if int(k) == k:
                    k = int(k)

            frequencies[k] = v

        return frequencies

    def __repr__(self):
        """Provides the string representation of the QuerySet

        .. versionchanged:: 0.6.13 Now doesnt modify the cursor
        """

        if self._iter:
            return '.. queryset mid-iteration ..'

        data = []
        for i in xrange(REPR_OUTPUT_SIZE + 1):
            try:
                data.append(self.next())
            except StopIteration:
                break
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."

        self.rewind()
        return repr(data)

    def select_related(self, max_depth=1):
        """Handles dereferencing of :class:`~bson.dbref.DBRef` objects to
        a maximum depth in order to cut down the number queries to mongodb.

        .. versionadded:: 0.5
        """
        # Make select related work the same for querysets
        max_depth += 1
        return self._dereference(self, max_depth=max_depth)

    def to_json(self):
        """Converts a queryset to JSON"""
        return json_util.dumps(self._collection_obj.find(self._query))

    def from_json(self, json_data):
        """Converts json data to unsaved objects"""
        son_data = json_util.loads(json_data)
        return [self._document._from_son(data) for data in son_data]

    @property
    def _dereference(self):
        if not self.__dereference:
            self.__dereference = _import_class('DeReference')()
        return self.__dereference
