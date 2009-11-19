from connection import _get_db


class QuerySet(object):
    """A set of results returned from a query. Wraps a MongoDB cursor, 
    providing Document objects as the results.
    """
    
    def __init__(self, document, cursor):
        self._document = document
        self._cursor = cursor
       
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

    def __iter__(self):
        return self


class CollectionManager(object):
    
    def __init__(self, document):
        """Set up the collection manager for a specific document.
        """
        db = _get_db()
        self._document = document
        self._collection_name = document._meta['collection']
        # This will create the collection if it doesn't exist
        self._collection = db[self._collection_name]

    def _save_document(self, document):
        """Save the provided document to the collection.
        """
        _id = self._collection.save(document._to_mongo())
        document._id = _id

    def _transform_query(self, **query):
        """Transform a query from Django-style format to Mongo format.
        """
        operators = ['neq', 'gt', 'gte', 'lt', 'lte', 'in', 'nin', 'mod',
                     'all', 'size', 'exists']

        mongo_query = {}
        for key, value in query.items():
            parts = key.split('__')
            # Check for an operator and transform to mongo-style if there is
            if parts[-1] in operators:
                op = parts.pop()
                value = {'$' + op: value}

            key = '.'.join(parts)
            mongo_query[key] = value

        return mongo_query

    def find(self, **query):
        """Query the collection for document matching the provided query.
        """
        query = self._transform_query(**query)
        query['_types'] = self._document._class_name
        return QuerySet(self._document, self._collection.find(query))

    def find_one(self, **query):
        """Query the collection for document matching the provided query.
        """
        query = self._transform_query(**query)
        query['_types'] = self._document._class_name
        return self._document._from_son(self._collection.find_one(query))
