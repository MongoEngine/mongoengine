from connection import _get_db

import pymongo
import re
import copy

__all__ = ['queryset_manager', 'Q', 'InvalidQueryError',
           'InvalidCollectionError']

# The maximum number of items to display in a QuerySet.__repr__
REPR_OUTPUT_SIZE = 20


class DoesNotExist(Exception):
    pass

class MultipleObjectsReturned(Exception):
    pass


class InvalidQueryError(Exception):
    pass


class OperationError(Exception):
    pass

class InvalidCollectionError(Exception):
    pass

RE_TYPE = type(re.compile(''))


class Q(object):

    OR = '||'
    AND = '&&'
    OPERATORS = {
        'eq': ('((this.%(field)s instanceof Array) && '
               '  this.%(field)s.indexOf(%(value)s) != -1) ||'
               ' this.%(field)s == %(value)s'),
        'ne': 'this.%(field)s != %(value)s',
        'gt': 'this.%(field)s > %(value)s',
        'gte': 'this.%(field)s >= %(value)s',
        'lt': 'this.%(field)s < %(value)s',
        'lte': 'this.%(field)s <= %(value)s',
        'lte': 'this.%(field)s <= %(value)s',
        'in': '%(value)s.indexOf(this.%(field)s) != -1',
        'nin': '%(value)s.indexOf(this.%(field)s) == -1',
        'mod': '%(field)s %% %(value)s',
        'all': ('%(value)s.every(function(a){'
                'return this.%(field)s.indexOf(a) != -1 })'),
        'size': 'this.%(field)s.length == %(value)s',
        'exists': 'this.%(field)s != null',
        'regex_eq': '%(value)s.test(this.%(field)s)',
        'regex_ne': '!%(value)s.test(this.%(field)s)',
    }

    def __init__(self, **query):
        self.query = [query]

    def _combine(self, other, op):
        obj = Q()
        if not other.query[0]:
            return self
        if self.query[0]:
            obj.query = (['('] + copy.deepcopy(self.query) + [op] +
                         copy.deepcopy(other.query) + [')'])
        else:
            obj.query = copy.deepcopy(other.query)
        return obj

    def __or__(self, other):
        return self._combine(other, self.OR)

    def __and__(self, other):
        return self._combine(other, self.AND)

    def as_js(self, document):
        js = []
        js_scope = {}
        for i, item in enumerate(self.query):
            if isinstance(item, dict):
                item_query = QuerySet._transform_query(document, **item)
                # item_query will values will either be a value or a dict
                js.append(self._item_query_as_js(item_query, js_scope, i))
            else:
                js.append(item)
        return pymongo.code.Code(' '.join(js), js_scope)

    def _item_query_as_js(self, item_query, js_scope, item_num):
        # item_query will be in one of the following forms
        #    {'age': 25, 'name': 'Test'}
        #    {'age': {'$lt': 25}, 'name': {'$in': ['Test', 'Example']}
        #    {'age': {'$lt': 25, '$gt': 18}}
        js = []
        for i, (key, value) in enumerate(item_query.items()):
            op = 'eq'
            # Construct a variable name for the value in the JS
            value_name = 'i%sf%s' % (item_num, i)
            if isinstance(value, dict):
                # Multiple operators for this field
                for j, (op, value) in enumerate(value.items()):
                    # Create a custom variable name for this operator
                    op_value_name = '%so%s' % (value_name, j)
                    # Construct the JS that uses this op
                    value, operation_js = self._build_op_js(op, key, value,
                                                            op_value_name)
                    # Update the js scope with the value for this op
                    js_scope[op_value_name] = value
                    js.append(operation_js)
            else:
                # Construct the JS for this field
                value, field_js = self._build_op_js(op, key, value, value_name)
                js_scope[value_name] = value
                js.append(field_js)
        print ' && '.join(js)
        return ' && '.join(js)

    def _build_op_js(self, op, key, value, value_name):
        """Substitute the values in to the correct chunk of Javascript.
        """
        print op, key, value, value_name
        if isinstance(value, RE_TYPE):
            # Regexes are handled specially
            if op.strip('$') == 'ne':
                op_js = Q.OPERATORS['regex_ne']
            else:
                op_js = Q.OPERATORS['regex_eq']
        else:
            op_js = Q.OPERATORS[op.strip('$')]

        # Comparing two ObjectIds in Javascript doesn't work..
        if isinstance(value, pymongo.objectid.ObjectId):
            value = unicode(value)

        # Perform the substitution
        operation_js = op_js % {
            'field': key, 
            'value': value_name
        }
        return value, operation_js

class QuerySet(object):
    """A set of results returned from a query. Wraps a MongoDB cursor,
    providing :class:`~mongoengine.Document` objects as the results.
    """

    def __init__(self, document, collection):
        self._document = document
        self._collection_obj = collection
        self._accessed_collection = False
        self._query = {}
        self._where_clause = None
        self._loaded_fields = []
        self._ordering = []
        
        # If inheritance is allowed, only return instances and instances of
        # subclasses of the class being used
        if document._meta.get('allow_inheritance'):
            self._query = {'_types': self._document._class_name}
        self._cursor_obj = None
        self._limit = None
        self._skip = None

    def ensure_index(self, key_or_list):
        """Ensure that the given indexes are in place.

        :param key_or_list: a single index key or a list of index keys (to
            construct a multi-field index); keys may be prefixed with a **+**
            or a **-** to determine the index ordering
        """
        index_list = QuerySet._build_index_spec(self._document, key_or_list)
        self._collection.ensure_index(index_list)
        return self

    @classmethod
    def _build_index_spec(cls, doc_cls, key_or_list):
        """Build a PyMongo index spec from a MongoEngine index spec.
        """
        if isinstance(key_or_list, basestring):
            key_or_list = [key_or_list]

        index_list = []
        use_types = doc_cls._meta.get('allow_inheritance', True)
        for key in key_or_list:
            # Get direction from + or -
            direction = pymongo.ASCENDING
            if key.startswith("-"):
                direction = pymongo.DESCENDING
            if key.startswith(("+", "-")):
                    key = key[1:]

            # Use real field name, do it manually because we need field
            # objects for the next part (list field checking)
            parts = key.split('.')
            fields = QuerySet._lookup_field(doc_cls, parts)
            parts = [field.db_field for field in fields]
            key = '.'.join(parts)
            index_list.append((key, direction))

            # Check if a list field is being used, don't use _types if it is
            if use_types and not all(f._index_with_types for f in fields):
                use_types = False

        # If _types is being used, prepend it to every specified index
        if doc_cls._meta.get('allow_inheritance') and use_types:
            index_list.insert(0, ('_types', 1))

        return index_list

    def __call__(self, q_obj=None, **query):
        """Filter the selected documents by calling the
        :class:`~mongoengine.queryset.QuerySet` with a query.

        :param q_obj: a :class:`~mongoengine.queryset.Q` object to be used in
            the query; the :class:`~mongoengine.queryset.QuerySet` is filtered
            multiple times with different :class:`~mongoengine.queryset.Q`
            objects, only the last one will be used
        :param query: Django-style query keyword arguments
        """
        if q_obj:
            self._where_clause = q_obj.as_js(self._document)
        query = QuerySet._transform_query(_doc_cls=self._document, **query)
        self._query.update(query)
        return self

    def filter(self, *q_objs, **query):
        """An alias of :meth:`~mongoengine.queryset.QuerySet.__call__`
        """
        return self.__call__(*q_objs, **query)

    @property
    def _collection(self):
        """Property that returns the collection object. This allows us to
        perform operations only if the collection is accessed.
        """
        if not self._accessed_collection:
            self._accessed_collection = True
            
            # Ensure document-defined indexes are created
            if self._document._meta['indexes']:
                for key_or_list in self._document._meta['indexes']:
                    #self.ensure_index(key_or_list)
                    self._collection.ensure_index(key_or_list)

            # Ensure indexes created by uniqueness constraints
            for index in self._document._meta['unique_indexes']:
                self._collection.ensure_index(index, unique=True)

            # If _types is being used (for polymorphism), it needs an index
            if '_types' in self._query:
                self._collection.ensure_index('_types')
            
            # Ensure all needed field indexes are created
            for field_name, field_instance in self._document._fields.iteritems():
                if field_instance.__class__.__name__ == 'GeoLocationField':
                    self._collection.ensure_index([(field_name, pymongo.GEO2D),])
        return self._collection_obj

    @property
    def _cursor(self):
        if self._cursor_obj is None:
            cursor_args = {}
            if self._loaded_fields:
                cursor_args = {'fields': self._loaded_fields}
            self._cursor_obj = self._collection.find(self._query, 
                                                     **cursor_args)
            # Apply where clauses to cursor
            if self._where_clause:
                self._cursor_obj.where(self._where_clause)

            # apply default ordering
            if self._document._meta['ordering']:
                self.order_by(*self._document._meta['ordering'])

        return self._cursor_obj

    @classmethod
    def _lookup_field(cls, document, parts):
        """Lookup a field based on its attribute and return a list containing
        the field's parents and the field.
        """
        if not isinstance(parts, (list, tuple)):
            parts = [parts]
        fields = []
        field = None
        for field_name in parts:
            if field is None:
                # Look up first field from the document
                field = document._fields[field_name]
            else:
                # Look up subfield on the previous field
                field = field.lookup_member(field_name)
                if field is None:
                    raise InvalidQueryError('Cannot resolve field "%s"'
                                            % field_name)
            fields.append(field)
        return fields

    @classmethod
    def _translate_field_name(cls, doc_cls, field, sep='.'):
        """Translate a field attribute name to a database field name.
        """
        parts = field.split(sep)
        parts = [f.db_field for f in QuerySet._lookup_field(doc_cls, parts)]
        return '.'.join(parts)

    @classmethod
    def _transform_query(cls, _doc_cls=None, **query):
        """Transform a query from Django-style format to Mongo format.
        """
        operators = ['ne', 'gt', 'gte', 'lt', 'lte', 'in', 'nin', 'mod',
                     'all', 'size', 'exists', 'near']
        match_operators = ['contains', 'icontains', 'startswith',
                           'istartswith', 'endswith', 'iendswith',
                           'exact', 'iexact']

        mongo_query = {}
        for key, value in query.items():
            parts = key.split('__')
            # Check for an operator and transform to mongo-style if there is
            op = None
            if parts[-1] in operators + match_operators:
                op = parts.pop()

            if _doc_cls:
                # Switch field names to proper names [set in Field(name='foo')]
                fields = QuerySet._lookup_field(_doc_cls, parts)
                parts = [field.db_field for field in fields]

                # Convert value to proper value
                field = fields[-1]
                singular_ops = [None, 'ne', 'gt', 'gte', 'lt', 'lte']
                singular_ops += match_operators
                if op in singular_ops:
                    value = field.prepare_query_value(op, value)
                elif op in ('in', 'nin', 'all'):
                    # 'in', 'nin' and 'all' require a list of values
                    value = [field.prepare_query_value(op, v) for v in value]

                if field.__class__.__name__ == 'GenericReferenceField':
                    parts.append('_ref')

            if op and op not in match_operators:
                value = {'$' + op: value}

            key = '.'.join(parts)
            if op is None or key not in mongo_query:
                mongo_query[key] = value
            elif key in mongo_query and isinstance(mongo_query[key], dict):
                mongo_query[key].update(value)

        return mongo_query

    def get(self, *q_objs, **query):
        """Retrieve the the matching object raising
        :class:`~mongoengine.queryset.MultipleObjectsReturned` or
        `DocumentName.MultipleObjectsReturned` exception if multiple results and
        :class:`~mongoengine.queryset.DoesNotExist` or `DocumentName.DoesNotExist`
        if no results are found.

        .. versionadded:: 0.3
        """
        self.__call__(*q_objs, **query)
        count = self.count()
        if count == 1:
            return self[0]
        elif count > 1:
            message = u'%d items returned, instead of 1' % count
            raise self._document.MultipleObjectsReturned(message)
        else:
            raise self._document.DoesNotExist("%s matching query does not exist."
                                              % self._document._class_name)

    def get_or_create(self, *q_objs, **query):
        """Retrieve unique object or create, if it doesn't exist. Returns a tuple of 
        ``(object, created)``, where ``object`` is the retrieved or created object 
        and ``created`` is a boolean specifying whether a new object was created. Raises
        :class:`~mongoengine.queryset.MultipleObjectsReturned` or
        `DocumentName.MultipleObjectsReturned` if multiple results are found.
        A new document will be created if the document doesn't exists; a
        dictionary of default values for the new document may be provided as a
        keyword argument called :attr:`defaults`.

        .. versionadded:: 0.3
        """
        defaults = query.get('defaults', {})
        if 'defaults' in query:
            del query['defaults']

        self.__call__(*q_objs, **query)
        count = self.count()
        if count == 0:
            query.update(defaults)
            doc = self._document(**query)
            doc.save()
            return doc, True
        elif count == 1:
            return self.first(), False
        else:
            message = u'%d items returned, instead of 1' % count
            raise self._document.MultipleObjectsReturned(message)

    def first(self):
        """Retrieve the first object matching the query.
        """
        try:
            result = self[0]
        except IndexError:
            result = None
        return result

    def with_id(self, object_id):
        """Retrieve the object matching the id provided.

        :param object_id: the value for the id of the document to look up
        """
        id_field = self._document._meta['id_field']
        object_id = self._document._fields[id_field].to_mongo(object_id)

        result = self._collection.find_one({'_id': object_id})
        if result is not None:
            result = self._document._from_son(result)
        return result

    def in_bulk(self, object_ids):
        """Retrieve a set of documents by their ids.
        
        :param object_ids: a list or tuple of ``ObjectId``\ s
        :rtype: dict of ObjectIds as keys and collection-specific
                Document subclasses as values.

        .. versionadded:: 0.3
        """
        doc_map = {}

        docs = self._collection.find({'_id': {'$in': object_ids}})
        for doc in docs:
            doc_map[doc['_id']] = self._document._from_son(doc)
 
        return doc_map

    def next(self):
        """Wrap the result in a :class:`~mongoengine.Document` object.
        """
        try:
            if self._limit == 0:
                raise StopIteration
            return self._document._from_son(self._cursor.next())
        except StopIteration, e:
            self.rewind()
            raise e

    def rewind(self):
        """Rewind the cursor to its unevaluated state.

        .. versionadded:: 0.3
        """
        self._cursor.rewind()

    def count(self):
        """Count the selected elements in the query.
        """
        if self._limit == 0:
            return 0
        return self._cursor.count(with_limit_and_skip=True)

    def __len__(self):
        return self.count()

    def map_reduce(self, map_f, reduce_f, finalize_f=None, limit=None,
                   scope=None, keep_temp=False):
        """Perform a map/reduce query using the current query spec
        and ordering. While ``map_reduce`` respects ``QuerySet`` chaining,
        it must be the last call made, as it does not return a maleable
        ``QuerySet``.

        See the :meth:`~mongoengine.tests.QuerySetTest.test_map_reduce`
        and :meth:`~mongoengine.tests.QuerySetTest.test_map_advanced`
        tests in ``tests.queryset.QuerySetTest`` for usage examples.

        :param map_f: map function, as :class:`~pymongo.code.Code` or string
        :param reduce_f: reduce function, as
                         :class:`~pymongo.code.Code` or string
        :param finalize_f: finalize function, an optional function that
                           performs any post-reduction processing.
        :param scope: values to insert into map/reduce global scope. Optional.
        :param limit: number of objects from current query to provide
                      to map/reduce method
        :param keep_temp: keep temporary table (boolean, default ``True``)

        Returns an iterator yielding
        :class:`~mongoengine.document.MapReduceDocument`.

        .. note:: Map/Reduce requires server version **>= 1.1.1**. The PyMongo
           :meth:`~pymongo.collection.Collection.map_reduce` helper requires
           PyMongo version **>= 1.2**.

        .. versionadded:: 0.3
        """
        from document import MapReduceDocument

        if not hasattr(self._collection, "map_reduce"):
            raise NotImplementedError("Requires MongoDB >= 1.1.1")

        map_f_scope = {}
        if isinstance(map_f, pymongo.code.Code):
            map_f_scope = map_f.scope
            map_f = unicode(map_f)
        map_f = pymongo.code.Code(self._sub_js_fields(map_f), map_f_scope)

        reduce_f_scope = {}
        if isinstance(reduce_f, pymongo.code.Code):
            reduce_f_scope = reduce_f.scope
            reduce_f = unicode(reduce_f)
        reduce_f_code = self._sub_js_fields(reduce_f)
        reduce_f = pymongo.code.Code(reduce_f_code, reduce_f_scope)

        mr_args = {'query': self._query, 'keeptemp': keep_temp}

        if finalize_f:
            finalize_f_scope = {}
            if isinstance(finalize_f, pymongo.code.Code):
                finalize_f_scope = finalize_f.scope
                finalize_f = unicode(finalize_f)
            finalize_f_code = self._sub_js_fields(finalize_f)
            finalize_f = pymongo.code.Code(finalize_f_code, finalize_f_scope)
            mr_args['finalize'] = finalize_f

        if scope:
            mr_args['scope'] = scope

        if limit:
            mr_args['limit'] = limit

        results = self._collection.map_reduce(map_f, reduce_f, **mr_args)
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
            return self._document._from_son(self._cursor[key])

    def only(self, *fields):
        """Load only a subset of this document's fields. ::
        
            post = BlogPost.objects(...).only("title")
        
        :param fields: fields to include

        .. versionadded:: 0.3
        """
        self._loaded_fields = []
        for field in fields:
            if '.' in field:
                raise InvalidQueryError('Subfields cannot be used as '
                                        'arguments to QuerySet.only')
            # Translate field name
            field = QuerySet._lookup_field(self._document, field)[-1].db_field
            self._loaded_fields.append(field)

        # _cls is needed for polymorphism
        if self._document._meta.get('allow_inheritance'):
            self._loaded_fields += ['_cls']
        return self

    def order_by(self, *keys):
        """Order the :class:`~mongoengine.queryset.QuerySet` by the keys. The
        order may be specified by prepending each of the keys by a + or a -.
        Ascending order is assumed.

        :param keys: fields to order the query results by; keys may be
            prefixed with **+** or **-** to determine the ordering direction
        """
        key_list = []
        for key in keys:
            direction = pymongo.ASCENDING
            if key[0] == '-':
                direction = pymongo.DESCENDING
            if key[0] in ('-', '+'):
                key = key[1:]
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
            import pprint
            plan = pprint.pformat(plan)
        return plan

    def delete(self, safe=False):
        """Delete the documents matched by the query.

        :param safe: check if the operation succeeded before returning
        """
        self._collection.remove(self._query, safe=safe)

    @classmethod
    def _transform_update(cls, _doc_cls=None, **update):
        """Transform an update spec from Django-style format to Mongo format.
        """
        operators = ['set', 'unset', 'inc', 'dec', 'push', 'push_all', 'pull',
                     'pull_all']

        mongo_update = {}
        for key, value in update.items():
            parts = key.split('__')
            # Check for an operator and transform to mongo-style if there is
            op = None
            if parts[0] in operators:
                op = parts.pop(0)
                # Convert Pythonic names to Mongo equivalents
                if op in ('push_all', 'pull_all'):
                    op = op.replace('_all', 'All')
                elif op == 'dec':
                    # Support decrement by flipping a positive value's sign
                    # and using 'inc'
                    op = 'inc'
                    if value > 0:
                        value = -value

            if _doc_cls:
                # Switch field names to proper names [set in Field(name='foo')]
                fields = QuerySet._lookup_field(_doc_cls, parts)
                parts = [field.db_field for field in fields]

                # Convert value to proper value
                field = fields[-1]
                if op in (None, 'set', 'unset', 'push', 'pull'):
                    value = field.prepare_query_value(op, value)
                elif op in ('pushAll', 'pullAll'):
                    value = [field.prepare_query_value(op, v) for v in value]

            key = '.'.join(parts)

            if op:
                value = {key: value}
                key = '$' + op

            if op is None or key not in mongo_update:
                mongo_update[key] = value
            elif key in mongo_update and isinstance(mongo_update[key], dict):
                mongo_update[key].update(value)

        return mongo_update

    def update(self, safe_update=True, upsert=False, **update):
        """Perform an atomic update on the fields matched by the query.

        :param safe: check if the operation succeeded before returning
        :param update: Django-style update keyword arguments

        .. versionadded:: 0.2
        """
        if pymongo.version < '1.1.1':
            raise OperationError('update() method requires PyMongo 1.1.1+')

        update = QuerySet._transform_update(self._document, **update)
        try:
            self._collection.update(self._query, update, safe=safe_update, 
                                    upsert=upsert, multi=True)
        except pymongo.errors.OperationFailure, err:
            if unicode(err) == u'multi not coded yet':
                message = u'update() method requires MongoDB 1.1.3+'
                raise OperationError(message)
            raise OperationError(u'Update failed (%s)' % unicode(err))

    def update_one(self, safe_update=True, upsert=False, **update):
        """Perform an atomic update on first field matched by the query.

        :param safe: check if the operation succeeded before returning
        :param update: Django-style update keyword arguments

        .. versionadded:: 0.2
        """
        update = QuerySet._transform_update(self._document, **update)
        try:
            # Explicitly provide 'multi=False' to newer versions of PyMongo
            # as the default may change to 'True'
            if pymongo.version >= '1.1.1':
                self._collection.update(self._query, update, safe=safe_update, 
                                        upsert=upsert, multi=False)
            else:
                # Older versions of PyMongo don't support 'multi'
                self._collection.update(self._query, update, safe=safe_update)
        except pymongo.errors.OperationFailure, e:
            raise OperationError(u'Update failed [%s]' % unicode(e))

    def __iter__(self):
        return self

    def _sub_js_fields(self, code):
        """When fields are specified with [~fieldname] syntax, where 
        *fieldname* is the Python name of a field, *fieldname* will be 
        substituted for the MongoDB name of the field (specified using the
        :attr:`name` keyword argument in a field's constructor).
        """
        def field_sub(match):
            # Extract just the field name, and look up the field objects
            field_name = match.group(1).split('.')
            fields = QuerySet._lookup_field(self._document, field_name)
            # Substitute the correct name for the field into the javascript
            return u'["%s"]' % fields[-1].db_field

        return re.sub(u'\[\s*~([A-z_][A-z_0-9.]+?)\s*\]', field_sub, code)

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

        fields = [QuerySet._translate_field_name(self._document, f)
                  for f in fields]
        collection = self._document._meta['collection']

        scope = {
            'collection': collection,
            'options': options or {},
        }

        query = self._query
        if self._where_clause:
            query['$where'] = self._where_clause

        scope['query'] = query
        code = pymongo.code.Code(code, scope=scope)

        db = _get_db()
        return db.eval(code, *fields)

    def sum(self, field):
        """Sum over the values of the specified field.

        :param field: the field to sum over; use dot-notation to refer to
            embedded document fields
        """
        sum_func = """
            function(sumField) {
                var total = 0.0;
                db[collection].find(query).forEach(function(doc) {
                    total += (doc[sumField] || 0.0);
                });
                return total;
            }
        """
        return self.exec_js(sum_func, field)

    def average(self, field):
        """Average over the values of the specified field.

        :param field: the field to average over; use dot-notation to refer to
            embedded document fields
        """
        average_func = """
            function(averageField) {
                var total = 0.0;
                var num = 0;
                db[collection].find(query).forEach(function(doc) {
                    if (doc[averageField]) {
                        total += doc[averageField];
                        num += 1;
                    }
                });
                return total / num;
            }
        """
        return self.exec_js(average_func, field)

    def item_frequencies(self, list_field, normalize=False):
        """Returns a dictionary of all items present in a list field across
        the whole queried set of documents, and their corresponding frequency.
        This is useful for generating tag clouds, or searching documents.

        :param list_field: the list field to use
        :param normalize: normalize the results so they add to 1.0
        """
        freq_func = """
            function(listField) {
                if (options.normalize) {
                    var total = 0.0;
                    db[collection].find(query).forEach(function(doc) {
                        total += doc[listField].length;
                    });
                }

                var frequencies = {};
                var inc = 1.0;
                if (options.normalize) {
                    inc /= total;
                }
                db[collection].find(query).forEach(function(doc) {
                    doc[listField].forEach(function(item) {
                        frequencies[item] = inc + (frequencies[item] || 0);
                    });
                });
                return frequencies;
            }
        """
        return self.exec_js(freq_func, list_field, normalize=normalize)

    def __repr__(self):
        limit = REPR_OUTPUT_SIZE + 1
        if self._limit is not None and self._limit < limit:
            limit = self._limit
        data = list(self[self._skip:limit])
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."
        return repr(data)


class QuerySetManager(object):

    def __init__(self, manager_func=None):
        self._manager_func = manager_func
        self._collection = None

    def __get__(self, instance, owner):
        """Descriptor for instantiating a new QuerySet object when
        Document.objects is accessed.
        """
        if instance is not None:
            # Document class being used rather than a document object
            return self

        if self._collection is None:
            db = _get_db()
            collection = owner._meta['collection']

            # Create collection as a capped collection if specified
            if owner._meta['max_size'] or owner._meta['max_documents']:
                # Get max document limit and max byte size from meta
                max_size = owner._meta['max_size'] or 10000000 # 10MB default
                max_documents = owner._meta['max_documents']

                if collection in db.collection_names():
                    self._collection = db[collection]
                    # The collection already exists, check if its capped
                    # options match the specified capped options
                    options = self._collection.options()
                    if options.get('max') != max_documents or \
                       options.get('size') != max_size:
                        msg = ('Cannot create collection "%s" as a capped '
                               'collection as it already exists') % collection
                        raise InvalidCollectionError(msg)
                else:
                    # Create the collection as a capped collection
                    opts = {'capped': True, 'size': max_size}
                    if max_documents:
                        opts['max'] = max_documents
                    self._collection = db.create_collection(collection, **opts)
            else:
                self._collection = db[collection]

        # owner is the document that contains the QuerySetManager
        queryset = QuerySet(owner, self._collection)
        if self._manager_func:
            if self._manager_func.func_code.co_argcount == 1:
                queryset = self._manager_func(queryset)
            else:
                queryset = self._manager_func(owner, queryset)
        return queryset


def queryset_manager(func):
    """Decorator that allows you to define custom QuerySet managers on
    :class:`~mongoengine.Document` classes. The manager must be a function that
    accepts a :class:`~mongoengine.Document` class as its first argument, and a
    :class:`~mongoengine.queryset.QuerySet` as its second argument. The method
    function should return a :class:`~mongoengine.queryset.QuerySet`, probably
    the same one that was passed in, but modified in some way.
    """
    if func.func_code.co_argcount == 1:
        import warnings
        msg = 'Methods decorated with queryset_manager should take 2 arguments'
        warnings.warn(msg, DeprecationWarning)
    return QuerySetManager(func)
