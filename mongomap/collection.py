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

    def find(self, query=None):
        """Query the collection for document matching the provided query.
        """
        return QuerySet(self._document, self._collection.find(query))

    def find_one(self, query=None):
        """Query the collection for document matching the provided query.
        """
        return self._document._from_son(self._collection.find_one(query))
