from connection import _get_db

import pymongo


class QuerySet(object):
    """A set of results returned from a query. Wraps a MongoDB cursor, 
    providing Document objects as the results.
    """
    
    def __init__(self, document, collection):
        self._document = document
        self._collection = collection
        self._query = {}
        self._cursor_obj = None
        self._ordering = []
        
    def ensure_index(self, key_or_list, direction=None):
        """Ensure that the given indexes are in place.
        """
        if isinstance(key_or_list, basestring):
            # single-field indexes needn't specify a direction
            if key_or_list.startswith("-"):
                key_or_list = key_or_list[1:]
            self._collection.ensure_index(key_or_list)
        elif isinstance(key_or_list, (list, tuple)):
            print key_or_list
            self._collection.ensure_index(key_or_list)
        return self

    def __call__(self, **query):
        """Filter the selected documents by calling the queryset with a query.
        """
        self._query.update(QuerySet._transform_query(**query))
        return self

    @property
    def _cursor(self):
        if not self._cursor_obj:
            self._cursor_obj = self._collection.find(self._query)
        return self._cursor_obj
       
    @classmethod
    def _transform_query(cls, **query):
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
        result = self._collection.find_one(self._query)
        if result is not None:
            result = self._document._from_son(result)
        return result

    def with_id(self, object_id):
        """Retrieve the object matching the _id provided.
        """
        if not isinstance(object_id, pymongo.objectid.ObjectId):
            object_id = pymongo.objectid.ObjectId(object_id)

        result = self._collection.find_one(object_id)
        if result is not None:
            result = self._document._from_son(result)
        return result

    def next(self):
        """Wrap the result in a Document object.
        """
        return self._document._from_son(self._cursor.next())

    def count(self):
        """Count the selected elements in the query.
        """
        return self._cursor.count()

    def limit(self, n):
        """Limit the number of returned documents to.
        """
        self._cursor.limit(n)
        # Return self to allow chaining
        return self

    def skip(self, n):
        """Skip n documents before returning the results.
        """
        self._cursor.skip(n)
        return self
        
    def order_by(self, *params):
        """Apply ordering conditions, Django-style.
        
        e.g., ``Model.objects.().order_by("-published_date", "ordering")``
        will order first by ``published_date DESC``, and then ``ordering ASC``.
        
        """
        if not params:
            self._ordering = []
        for param in params:
            if param.startswith("-"):
                param = param[1:]
                sort_dir = pymongo.DESCENDING
            else:
                sort_dir = pymongo.ASCENDING
            sort_rule = (param, sort_dir)

            if not sort_rule in self._ordering:
                self._ordering.append(sort_rule)
        self._cursor.sort(self._ordering)
        return self
        
    def explain(self, format=False):
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


class QuerySetManager(object):

    def __init__(self, document):
        db = _get_db()
        self._document = document
        self._collection_name = document._meta['collection']
        # This will create the collection if it doesn't exist
        self._collection = db[self._collection_name]

    def __get__(self, instance, owner):
        """Descriptor for instantiating a new QuerySet object when 
        Document.objects is accessed.
        """
        if instance is not None:
            # Document class being used rather than a document object
            return self
        
        # self._document should be the same as owner
        return QuerySet(self._document, self._collection)
