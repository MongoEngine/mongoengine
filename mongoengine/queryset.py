from connection import _get_db

import pprint
import pymongo
import pymongo.errors
import re
import copy
import itertools
import time
import greenlet

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


class QNodeVisitor(object):
    """Base visitor class for visiting Q-object nodes in a query tree.
    """

    def visit_combination(self, combination):
        """Called by QCombination objects.
        """
        return combination

    def visit_query(self, query):
        """Called by (New)Q objects.
        """
        return query


class SimplificationVisitor(QNodeVisitor):
    """Simplifies query trees by combinging unnecessary 'and' connection nodes
    into a single Q-object.
    """

    def visit_combination(self, combination):
        if combination.operation == combination.AND:
            # The simplification only applies to 'simple' queries
            if all(isinstance(node, Q) for node in combination.children):
                queries = [node.query for node in combination.children]
                return Q(**self._query_conjunction(queries))
        return combination

    def _query_conjunction(self, queries):
        """Merges query dicts - effectively &ing them together.
        """
        query_ops = set()
        combined_query = {}
        for query in queries:
            ops = set(query.keys())
            # Make sure that the same operation isn't applied more than once
            # to a single field
            intersection = ops.intersection(query_ops)
            if intersection:
                msg = 'Duplicate query contitions: '
                raise InvalidQueryError(msg + ', '.join(intersection))

            query_ops.update(ops)
            combined_query.update(copy.deepcopy(query))
        return combined_query


class QueryTreeTransformerVisitor(QNodeVisitor):
    """Transforms the query tree in to a form that may be used with MongoDB.
    """

    def visit_combination(self, combination):
        if combination.operation == combination.AND:
            # MongoDB doesn't allow us to have too many $or operations in our
            # queries, so the aim is to move the ORs up the tree to one
            # 'master' $or. Firstly, we must find all the necessary parts (part
            # of an AND combination or just standard Q object), and store them
            # separately from the OR parts.
            or_groups = []
            and_parts = []
            for node in combination.children:
                if isinstance(node, QCombination):
                    if node.operation == node.OR:
                        # Any of the children in an $or component may cause
                        # the query to succeed
                        or_groups.append(node.children)
                    elif node.operation == node.AND:
                        and_parts.append(node)
                elif isinstance(node, Q):
                    and_parts.append(node)

            # Now we combine the parts into a usable query. AND together all of
            # the necessary parts. Then for each $or part, create a new query
            # that ANDs the necessary part with the $or part.
            clauses = []
            for or_group in itertools.product(*or_groups):
                q_object = reduce(lambda a, b: a & b, and_parts, Q())
                q_object = reduce(lambda a, b: a & b, or_group, q_object)
                clauses.append(q_object)

            # Finally, $or the generated clauses in to one query. Each of the
            # clauses is sufficient for the query to succeed.
            return reduce(lambda a, b: a | b, clauses, Q())

        if combination.operation == combination.OR:
            children = []
            # Crush any nested ORs in to this combination as MongoDB doesn't
            # support nested $or operations
            for node in combination.children:
                if (isinstance(node, QCombination) and
                    node.operation == combination.OR):
                    children += node.children
                else:
                    children.append(node)
            combination.children = children

        return combination


class QueryCompilerVisitor(QNodeVisitor):
    """Compiles the nodes in a query tree to a PyMongo-compatible query
    dictionary.
    """

    def __init__(self, document):
        self.document = document

    def visit_combination(self, combination):
        if combination.operation == combination.OR:
            return {'$or': combination.children}
        elif combination.operation == combination.AND:
            return self._mongo_query_conjunction(combination.children)
        return combination

    def visit_query(self, query):
        return QuerySet._transform_query(self.document, **query.query)

    def _mongo_query_conjunction(self, queries):
        """Merges Mongo query dicts - effectively &ing them together.
        """
        combined_query = {}
        for query in queries:
            for field, ops in query.items():
                if field not in combined_query:
                    combined_query[field] = ops
                else:
                    # The field is already present in the query the only way
                    # we can merge is if both the existing value and the new
                    # value are operation dicts, reject anything else
                    if (not isinstance(combined_query[field], dict) or
                        not isinstance(ops, dict)):
                        message = 'Conflicting values for ' + field
                        raise InvalidQueryError(message)

                    current_ops = set(combined_query[field].keys())
                    new_ops = set(ops.keys())
                    # Make sure that the same operation isn't applied more than
                    # once to a single field
                    intersection = current_ops.intersection(new_ops)
                    if intersection:
                        msg = 'Duplicate query contitions: '
                        raise InvalidQueryError(msg + ', '.join(intersection))

                    # Right! We've got two non-overlapping dicts of operations!
                    combined_query[field].update(copy.deepcopy(ops))
        return combined_query


class QNode(object):
    """Base class for nodes in query trees.
    """

    AND = 0
    OR = 1

    def to_query(self, document):
        query = self.accept(SimplificationVisitor())
        query = query.accept(QueryTreeTransformerVisitor())
        query = query.accept(QueryCompilerVisitor(document))
        return query

    def accept(self, visitor):
        raise NotImplementedError

    def _combine(self, other, operation):
        """Combine this node with another node into a QCombination object.
        """
        if other.empty:
            return self

        if self.empty:
            return other

        return QCombination(operation, [self, other])

    @property
    def empty(self):
        return False

    def __or__(self, other):
        return self._combine(other, self.OR)

    def __and__(self, other):
        return self._combine(other, self.AND)


class QCombination(QNode):
    """Represents the combination of several conditions by a given logical
    operator.
    """

    def __init__(self, operation, children):
        self.operation = operation
        self.children = []
        for node in children:
            # If the child is a combination of the same type, we can merge its
            # children directly into this combinations children
            if isinstance(node, QCombination) and node.operation == operation:
                self.children += node.children
            else:
                self.children.append(node)

    def accept(self, visitor):
        for i in range(len(self.children)):
            self.children[i] = self.children[i].accept(visitor)

        return visitor.visit_combination(self)

    @property
    def empty(self):
        return not bool(self.children)


class Q(QNode):
    """A simple query object, used in a query tree to build up more complex
    query structures.
    """

    def __init__(self, **query):
        self.query = query

    def accept(self, visitor):
        return visitor.visit_query(self)

    @property
    def empty(self):
        return not bool(self.query)


class QuerySet(object):
    """A set of results returned from a query. Wraps a MongoDB cursor,
    providing :class:`~mongoengine.Document` objects as the results.
    """

    _index_specs = {}
    _allow_index_creation = True

    def __init__(self, document, collection):
        self._document = document
        self._collection_obj = collection
        self._accessed_collection = False
        self._mongo_query = None
        self._query_obj = Q()
        self._initial_query = {}
        self._where_clause = None
        self._loaded_fields = []
        self._query_fields = None
        self._ordering = []
        self._snapshot = False
        self._timeout = True

        # If inheritance is allowed, only return instances and instances of
        # subclasses of the class being used
        if document._meta.get('allow_inheritance'):
            self._initial_query = {'_types': self._document._class_name}
        self._cursor_obj = None
        self._limit = None
        self._skip = None

    @property
    def _query(self):
        if self._mongo_query is None:
            self._mongo_query = self._query_obj.to_query(self._document)
            self._mongo_query.update(self._initial_query)
        return self._mongo_query

    def ensure_index(self, key_or_list, drop_dups=False, background=False,
        **kwargs):
        """Ensure that the given indexes are in place.

        :param key_or_list: a single index key or a list of index keys (to
            construct a multi-field index); keys may be prefixed with a **+**
            or a **-** to determine the index ordering
        """
        index_list = QuerySet._build_index_spec(self._document, key_or_list)
        self._collection.ensure_index(index_list, drop_dups=drop_dups,
            background=background)
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
        #if q_obj:
            #self._where_clause = q_obj.as_js(self._document)
        query = Q(**query)
        if q_obj:
            query &= q_obj
        self._query_obj &= query
        self._mongo_query = None
        self._cursor_obj = None
        return self

    def filter(self, *q_objs, **query):
        """An alias of :meth:`~mongoengine.queryset.QuerySet.__call__`
        """
        return self.__call__(*q_objs, **query)

    def all(self):
        """Returns all documents."""
        return self.__call__()

    @property
    def _collection(self):
        return self._collection_obj

    @property
    def _cursor(self):
        if self._cursor_obj is None:
            cursor_args = {
                'snapshot': self._snapshot,
                'no_cursor_timeout': self._timeout,
            }
            if self._loaded_fields:
                cursor_args['projection'] = self._loaded_fields

            try:
                self._cursor_obj = self._collection.find(self._query,
                                                         **cursor_args)
            except pymongo.errors.AutoReconnect:
                # if the primary changes, sleep for 100ms and try again
                time.sleep(0.1)
                self._cursor_obj = self._collection.find(self._query,
                                                         **cursor_args)

            # Apply where clauses to cursor
            if self._where_clause:
                self._cursor_obj.where(self._where_clause)

            # apply default ordering
            if self._document._meta['ordering']:
                self.order_by(*self._document._meta['ordering'])

            if self._limit is not None:
                try:
                    self._cursor_obj.limit(self._limit)
                except pymongo.errors.AutoReconnect:
                    # if the primary changes, sleep for 100ms and try again
                    time.sleep(0.1)
                    self._cursor_obj.limit(self._limit)

            if self._skip is not None:
                try:
                    self._cursor_obj.skip(self._skip)
                except pymongo.errors.AutoReconnect:
                    # if the primary changes, sleep for 100ms and try again
                    time.sleep(0.1)
                    self._cursor_obj.skip(self._skip)

        if not (isinstance(self._cursor_obj, pymongo.cursor.Cursor) or isinstance(self._cursor_obj, pymongo.command_cursor.CommandCursor)):
            return self._cursor_obj.delegate
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
                if field_name == 'pk':
                    # Deal with "primary key" alias
                    field_name = document._meta['id_field']
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
                     'all', 'size', 'exists', 'not']
        geo_operators = ['within_distance', 'within_spherical_distance',
                         'within_box', 'near', 'near_sphere', 'geoWithin',
                         'geo_intersects', 'geo_within_box', 'geo_within_polygon']
        match_operators = ['contains', 'icontains', 'startswith',
                           'istartswith', 'endswith', 'iendswith',
                           'exact', 'iexact']

        mongo_query = {}
        for key, value in query.items():
            if key == "__raw__":
                mongo_query.update(value)
                continue

            parts = key.split('__')
            indices = [(i, p) for i, p in enumerate(parts) if p.isdigit()]
            parts = [part for part in parts if not part.isdigit()]
            # Check for an operator and transform to mongo-style if there is
            op = None
            if parts[-1] in operators + match_operators + geo_operators:
                op = parts.pop()

            negate = False
            if parts[-1] == 'not':
                parts.pop()
                negate = True

            if _doc_cls:
                # Switch field names to proper names [set in Field(name='foo')]
                fields = QuerySet._lookup_field(_doc_cls, parts)
                parts = [field.db_field for field in fields]

                # Convert value to proper value
                field = fields[-1]
                singular_ops = [None, 'ne', 'gt', 'gte', 'lt', 'lte', 'not']
                singular_ops += match_operators
                if op in singular_ops:
                    value = field.prepare_query_value(op, value)
                elif op in ('in', 'nin', 'all', 'near'):
                    # 'in', 'nin' and 'all' require a list of values
                    value = [field.prepare_query_value(op, v) for v in value]

            # if op and op not in match_operators:
            if op:
                if op in geo_operators:
                    if op == "within_distance":
                        value = {'$within': {'$center': value}}
                    elif op == "within_spherical_distance":
                        value = {'$within': {'$centerSphere': value}}
                    elif op == "near":
                        value = {'$near': value}
                    elif op == "near_sphere":
                        value = {'$nearSphere': value}
                    elif op == 'within_box':
                        value = {'$within': {'$box': value}}
                    elif op == "geoWithin":
                        value = {'$geoWithin': value}
                    elif op == "geo_intersects":
                        value = {'$geoIntersects': value}
                    elif op == "geo_within_box":
                        value = {'$geoWithin': {'$box': value}}
                    elif op == "geo_within_polygon":
                        value = {'$geoWithin': {'$polygon': value}}
                    else:
                        raise NotImplementedError("Geo method '%s' has not "
                                                  "been implemented" % op)
                elif op not in match_operators:
                    value = {'$' + op: value}

            if negate:
                value = {'$not': value}

            for i, part in indices:
                parts.insert(i, part)
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

    def with_id(self, object_id):
        """Retrieve the object matching the id provided.

        :param object_id: the value for the id of the document to look up
        """
        id_field = self._document._meta['id_field']
        object_id = self._document._fields[id_field].to_mongo(object_id)

        try:
            result = self._collection.find_one({'_id': object_id})
        except pymongo.errors.AutoReconnect:
            # if the primary changes, sleep for 100ms and try again
            time.sleep(0.1)
            result = self._collection.find_one({'_id': object_id})

        if result is not None:
            result = self._document._from_augmented_son(result,
                                                        self._query_fields)
        return result

    def in_bulk(self, object_ids):
        """Retrieve a set of documents by their ids.

        :param object_ids: a list or tuple of ``ObjectId``\ s
        :rtype: dict of ObjectIds as keys and collection-specific
                Document subclasses as values.

        .. versionadded:: 0.3
        """
        doc_map = {}

        try:
            docs = self._collection.find({'_id': {'$in': object_ids}})
        except pymongo.errors.AutoReconnect:
            # if the primary changes, sleep for 100ms and try again
            time.sleep(0.1)
            docs = self._collection.find({'_id': {'$in': object_ids}})

        for doc in docs:
            doc_map[doc['_id']] = self._document._from_augmented_son(
                doc, self._query_fields)

        return doc_map

    def next(self):
        """Wrap the result in a :class:`~mongoengine.Document` object.
        """
        try:
            if self._limit == 0:
                raise StopIteration
            try:
                return self._document._from_augmented_son(self._cursor.next(),
                                                          self._query_fields)
            except pymongo.errors.AutoReconnect:
                # if the primary changes, sleep for 100ms and try again
                time.sleep(0.1)
                return self._document._from_augmented_son(self._cursor.next(),
                                                          self._query_fields)
        except StopIteration, e:
            self.rewind()
            raise e

    def rewind(self):
        """Rewind the cursor to its unevaluated state.

        .. versionadded:: 0.3
        """
        try:
            self._cursor.rewind()
        except pymongo.errors.AutoReconnect:
            # if the primary changes, sleep for 100ms and try again
            time.sleep(0.1)
            self._cursor.rewind()

    def count(self):
        """Count the selected elements in the query.
        """
        if self._limit == 0:
            return 0
        try:
            return self._cursor.count(with_limit_and_skip=True)
        except pymongo.errors.AutoReconnect:
            # if the primary changes, sleep for 100ms and try again
            time.sleep(0.1)
            return self._cursor.count(with_limit_and_skip=True)


    def __len__(self):
        return self.count()

    def limit(self, n):
        """Limit the number of returned documents to `n`. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[:5]``).

        :param n: the maximum number of objects to return
        """
        if n == 0:
            n = 1

        try:
            self._cursor.limit(n)
        except pymongo.errors.AutoReconnect:
            # if the primary changes, sleep for 100ms and try again
            time.sleep(0.1)
            self._cursor.limit(n)

        self._limit = n

        # Return self to allow chaining
        return self

    def skip(self, n):
        """Skip `n` documents before returning the results. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[5:]``).

        :param n: the number of objects to skip before returning results
        """
        try:
            self._cursor.skip(n)
        except pymongo.errors.AutoReconnect:
            # if the primary changes, sleep for 100ms and try again
            time.sleep(0.1)
            self._cursor.skip(n)

        self._skip = n
        return self

    def __getitem__(self, key):
        """Support skip and limit using getitem and slicing syntax.
        """
        # Slice provided
        if isinstance(key, slice):
            try:
                try:
                    self._cursor_obj = self._cursor[key]
                except pymongo.errors.AutoReconnect:
                    # if the primary changes, sleep for 100ms and try again
                    time.sleep(0.1)
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
            try:
                return self._document._from_augmented_son(self._cursor[key],
                                                          self._query_fields)
            except pymongo.errors.AutoReconnect:
                # if the primary changes, sleep for 100ms and try again
                time.sleep(0.1)
                return self._document._from_augmented_son(self._cursor[key],
                                                          self._query_fields)

        raise AttributeError

    def distinct(self, field):
        """Return a list of distinct values for a given field.

        :param field: the field to select distinct values from

        .. versionadded:: 0.4
        """
        try:
            return self._cursor.distinct(field)
        except pymongo.errors.AutoReconnect:
            # if the primary changes, sleep for 100ms and try again
            time.sleep(0.1)
            return self._cursor.distinct(field)

    def only(self, *fields):
        """Load only a subset of this document's fields. ::

            post = BlogPost.objects(...).only("title")

        :param fields: fields to include

        .. versionadded:: 0.3
        """
        self._query_fields = fields
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
            if not key: continue
            direction = pymongo.ASCENDING
            if key[0] == '-':
                direction = pymongo.DESCENDING
            if key[0] in ('-', '+'):
                key = key[1:]
            key = key.replace('__', '.')
            key_list.append((key, direction))

        self._ordering = key_list

        try:
            self._cursor.sort(key_list)
        except pymongo.errors.AutoReconnect:
            # if the primary changes, sleep for 100ms and try again
            time.sleep(0.1)
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
        """
        self._snapshot = enabled

    def timeout(self, enabled):
        """Enable or disable the default mongod timeout when querying.

        :param enabled: whether or not the timeout is used
        """
        self._timeout = enabled

    def delete(self, safe=False):
        """Delete the documents matched by the query.

        :param safe: check if the operation succeeded before returning
        """

        proxy_client = self._document._get_proxy_client()
        if proxy_client:
            from sweeper.model.decider_key import DeciderKeyRatio
            dkey = DeciderKeyRatio.get_by_name('mongo_proxy_write_service')
            if dkey and dkey.decide():
                proxy_client.instance().remove(self._document, self._query)
                return
        self._collection.delete_many(self._query)

    @classmethod
    def _transform_update(cls, _doc_cls=None, **update):
        """Transform an update spec from Django-style format to Mongo format.
        """
        operators = ['set', 'unset', 'inc', 'dec', 'pop', 'push', 'push_all',
                     'pull', 'pull_all', 'add_to_set']

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
                elif op == 'add_to_set':
                    op = op.replace('_to_set', 'ToSet')
            else:
                '''
                Updates must have an actual operator or else it will
                end up doing a save and borking your object
                '''
                raise InvalidQueryError('%s is invalid operator' % parts[0])

            if _doc_cls:
                # Switch field names to proper names [set in Field(name='foo')]
                fields = QuerySet._lookup_field(_doc_cls, parts)
                parts = [field.db_field for field in fields]

                # Convert value to proper value
                field = fields[-1]
                if op in (None, 'set', 'push', 'pull',
                          'addToSet'):
                    value = field.prepare_query_value(op, value)
                elif op in ('unset', 'pop'):
                    value = 1
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
        """Perform an atomic update on the fields matched by the query. When
        ``safe_update`` is used, the number of affected documents is returned.

        :param safe: check if the operation succeeded before returning
        :param update: Django-style update keyword arguments

        .. versionadded:: 0.2
        """
        if pymongo.version < '1.1.1':
            raise OperationError('update() method requires PyMongo 1.1.1+')

        update = QuerySet._transform_update(self._document, **update)

        if not update:
            raise OperationError(u"Update Malformed: No Command Found")

        try:
            ret = self._collection.update(self._query, update, multi=True,
                                          upsert=upsert, safe=safe_update)
            if ret is not None and 'n' in ret:
                return ret['n']
        except pymongo.errors.OperationFailure, err:
            if unicode(err) == u'multi not coded yet':
                message = u'update() method requires MongoDB 1.1.3+'
                raise OperationError(message)
            raise OperationError(u'Update failed (%s)' % unicode(err))

    def update_one(self, safe_update=True, upsert=False, **update):
        """Perform an atomic update on first field matched by the query. When
        ``safe_update`` is used, the number of affected documents is returned.

        :param safe: check if the operation succeeded before returning
        :param update: Django-style update keyword arguments

        .. versionadded:: 0.2
        """
        update = QuerySet._transform_update(self._document, **update)

        if not update:
            raise OperationError(u"Update Malformed: No Command Found")
        try:
            # Explicitly provide 'multi=False' to newer versions of PyMongo
            # as the default may change to 'True'
            if pymongo.version >= '1.1.1':
                ret = self._collection.update(self._query, update, multi=False,
                                              upsert=upsert, safe=safe_update)
            else:
                # Older versions of PyMongo don't support 'multi'
                ret = self._collection.update(self._query, update,
                                              safe=safe_update)
            if ret is not None and 'n' in ret:
                return ret['n']
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
        self._collections = {}

    def __get__(self, instance, owner):
        """Descriptor for instantiating a new QuerySet object when
        Document.objects is accessed.
        """
        if instance is not None:
            # Document class being used rather than a document object
            return self

        # we can't do async queries if we're on the root greenlet since we have
        # nothing to yield back to
        allow_async = bool(greenlet.getcurrent().parent)

        db = _get_db(owner._meta['db_name'], allow_async=allow_async)

        collection = owner._meta['collection']
        if (db, collection) not in self._collections:
            # Create collection as a capped collection if specified
            if owner._meta['max_size'] or owner._meta['max_documents']:
                # Get max document limit and max byte size from meta
                max_size = owner._meta['max_size'] or 10000000 # 10MB default
                max_documents = owner._meta['max_documents']

                if collection in db.collection_names():
                    self._collections[(db, collection)] = db[collection]
                    # The collection already exists, check if its capped
                    # options match the specified capped options
                    options = self._collections[(db, collection)].options()
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
                    self._collections[(db, collection)] = db.create_collection(
                        collection, **opts
                    )
            else:
                self._collections[(db, collection)] = db[collection]

        # owner is the document that contains the QuerySetManager
        queryset_class = owner._meta['queryset_class'] or QuerySet
        queryset = queryset_class(owner, self._collections[(db, collection)])
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
