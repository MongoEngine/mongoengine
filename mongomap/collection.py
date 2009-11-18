from connection import _get_db

class CollectionManager(object):
    
    def __init__(self, document):
        """Set up the collection manager for a specific document.
        """
        db = _get_db()
        self._document = document
        self._collection_name = document._meta['collection']
        # This will create the collection if it doesn't exist
        self._collection = db[self._collection_name]
        self._id_field = document._meta['object_id_field']

    def _save_document(self, document):
        """Save the provided document to the collection.
        """
        _id = self._collection.save(document)
