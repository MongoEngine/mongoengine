from connection import _get_db

import pymongo


__all__ = ['queryset_manager', 'InvalidQueryError', 'InvalidCollectionError']


class InvalidQueryError(Exception):
    pass


class QuerySet(object):
    """A set of results returned from a query. Wraps a MongoDB cursor, 
    providing :class:`~mongoengine.Document` objects as the results.
    """
    
    def __init__(self, document, collection):
        self._document = document
        self._collection = collection
        self._query = {}

        # If inheritance is allowed, only return instances and instances of
        # subclasses of the class being used
        if document._meta.get('allow_inheritance'):
            self._query = {'_types': self._document._class_name}
        self._cursor_obj = None
        
    def ensure_index(self, key_or_list):
        """Ensure that the given indexes are in place.
        """
        if isinstance(key_or_list, basestring):
            key_or_list = [key_or_list]

        index_list = []
        # If _types is being used, prepend it to every specified index
        if self._document._meta.get('allow_inheritance'):
            index_list.append(('_types', 1))

        for key in key_or_list:
            # Get direction from + or -
            direction = pymongo.ASCENDING
            if key.startswith("-"):
                direction = pymongo.DESCENDING
            if key.startswith(("+", "-")):
                    key = key[1:]
            # Use real field name
            key = QuerySet._translate_field_name(self._document, key)
            index_list.append((key, direction))
        self._collection.ensure_index(index_list)
        return self

    def __call__(self, **query):
        """Filter the selected documents by calling the 
        :class:`~mongoengine.QuerySet` with a query.
        """
        query = QuerySet._transform_query(_doc_cls=self._document, **query)
        self._query.update(query)
        return self

    @property
    def _cursor(self):
        if not self._cursor_obj:
            # Ensure document-defined indexes are created
            if self._document._meta['indexes']:
                for key_or_list in self._document._meta['indexes']:
                    self.ensure_index(key_or_list)

            # If _types is being used (for polymorphism), it needs an index
            if '_types' in self._query:
                self._collection.ensure_index('_types')

            self._cursor_obj = self._collection.find(self._query)
            
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
        parts = [f.name for f in QuerySet._lookup_field(doc_cls, parts)]
        return '.'.join(parts)

    @classmethod
    def _transform_query(cls, _doc_cls=None, **query):
        """Transform a query from Django-style format to Mongo format.
        """
        operators = ['neq', 'gt', 'gte', 'lt', 'lte', 'in', 'nin', 'mod',
                     'all', 'size', 'exists']

        mongo_query = {}
        for key, value in query.items():
            parts = key.split('__')
            # Check for an operator and transform to mongo-style if there is
            op = None
            if parts[-1] in operators:
                op = parts.pop()

            if _doc_cls:
                # Switch field names to proper names [set in Field(name='foo')]
                fields = QuerySet._lookup_field(_doc_cls, parts)
                parts = [field.name for field in fields]

                # Convert value to proper value
                field = fields[-1]
                if op in (None, 'neq', 'gt', 'gte', 'lt', 'lte'):
                    value = field.prepare_query_value(value)
                elif op in ('in', 'nin', 'all'):
                    # 'in', 'nin' and 'all' require a list of values
                    value = [field.prepare_query_value(v) for v in value]

            if op:
                value = {'$' + op: value}

            key = '.'.join(parts)
            if op is None or key not in mongo_query:
                mongo_query[key] = value
            elif key in mongo_query and isinstance(mongo_query[key], dict):
                mongo_query[key].update(value)

        return mongo_query

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
        """
        if not isinstance(object_id, pymongo.objectid.ObjectId):
            object_id = pymongo.objectid.ObjectId(str(object_id))

        result = self._collection.find_one(object_id)
        if result is not None:
            result = self._document._from_son(result)
        return result

    def next(self):
        """Wrap the result in a :class:`~mongoengine.Document` object.
        """
        return self._document._from_son(self._cursor.next())

    def count(self):
        """Count the selected elements in the query.
        """
        return self._cursor.count()

    def __len__(self):
        return self.count()

    def limit(self, n):
        """Limit the number of returned documents to `n`. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[:5]``).
        """
        self._cursor.limit(n)
        # Return self to allow chaining
        return self

    def skip(self, n):
        """Skip `n` documents before returning the results. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[5:]``).
        """
        self._cursor.skip(n)
        return self

    def __getitem__(self, key):
        """Support skip and limit using getitem and slicing syntax.
        """
        # Slice provided
        if isinstance(key, slice):
            self._cursor_obj = self._cursor[key]
            # Allow further QuerySet modifications to be performed
            return self
        # Integer index provided
        elif isinstance(key, int):
            return self._document._from_son(self._cursor[key])

    def order_by(self, *keys):
        """Order the :class:`~mongoengine.queryset.QuerySet` by the keys. The
        order may be specified by prepending each of the keys by a + or a -.
        Ascending order is assumed.
        """
        key_list = []
        for key in keys:
            direction = pymongo.ASCENDING
            if key[0] == '-':
                direction = pymongo.DESCENDING
            if key[0] in ('-', '+'):
                key = key[1:]
            key_list.append((key, direction)) 

        self._cursor.sort(key_list)
        return self
        
    def explain(self, format=False):
        """Return an explain plan record for the 
        :class:`~mongoengine.queryset.QuerySet`\ 's cursor.
        """

        plan = self._cursor.explain()
        if format:
            import pprint
            plan = pprint.pformat(plan)
        return plan
        
    def delete(self):
        """Delete the documents matched by the query.
        """
        self._collection.remove(self._query)

    def __iter__(self):
        return self

    def exec_js(self, code, *fields, **options):
        """Execute a Javascript function on the server. A list of fields may be
        provided, which will be translated to their correct names and supplied
        as the arguments to the function. A few extra variables are added to
        the function's scope: ``collection``, which is the name of the 
        collection in use; ``query``, which is an object representing the 
        current query; and ``options``, which is an object containing any
        options specified as keyword arguments.
        """
        fields = [QuerySet._translate_field_name(self._document, f)
                  for f in fields]
        collection = self._document._meta['collection']
        scope = {
            'collection': collection,
            'query': self._query,
            'options': options or {},
        }
        code = pymongo.code.Code(code, scope=scope)

        db = _get_db()
        return db.eval(code, *fields)

    def sum(self, field):
        """Sum over the values of the specified field.
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


class InvalidCollectionError(Exception):
    pass


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
                    self._collection = db.create_collection(collection, opts)
            else:
                self._collection = db[collection]
        
        # owner is the document that contains the QuerySetManager
        queryset = QuerySet(owner, self._collection)
        if self._manager_func:
            queryset = self._manager_func(queryset)
        return queryset

def queryset_manager(func):
    """Decorator that allows you to define custom QuerySet managers on 
    :class:`~mongoengine.Document` classes. The manager must be a function that
    accepts a :class:`~mongoengine.queryset.QuerySet` as its only argument, and
    returns a :class:`~mongoengine.queryset.QuerySet`, probably the same one 
    but modified in some way.
    """
    return QuerySetManager(func)
