from __future__ import absolute_import
from collections import defaultdict

from mongoengine.connection import get_connection, get_db
from pymongo.read_preferences import Primary
from pymongo.collection import Collection

__all__ = ['InvalidCollectionError', 'connection_manager']


class InvalidCollectionError(Exception):
    pass


# mockable function that controls whether indexes are automatically created
def should_auto_create_index(flag):
    return flag

class ConnectionManager(object):
    connections_registry = defaultdict(lambda: defaultdict(dict))

    def get_and_setup(self, doc_cls, alias=None, collection_name=None, read_preference=None):
        read_preference = read_preference or Primary()
        read_preference_str = str(read_preference)  # read_preference classes are unhashable in python 3
        
        if alias is None:
            alias = doc_cls._get_db_alias()

        if collection_name is None:
            registry_collection_name = getattr(doc_cls, '_class_name', doc_cls._get_collection_name())
            collection_name = doc_cls._get_collection_name()
        else:
            registry_collection_name = collection_name

        _collection = self.connections_registry[alias][registry_collection_name].get(read_preference_str)
        if not _collection:
            _collection = self.get_collection(doc_cls, alias, collection_name, read_preference=read_preference)
            if should_auto_create_index(doc_cls._meta.get('auto_create_index', False)):
                doc_cls.ensure_indexes(_collection)
            self.connections_registry[alias][registry_collection_name][read_preference_str] = _collection
        return self.connections_registry[alias][registry_collection_name][read_preference_str]

    @classmethod
    def _get_db(cls, alias):
        """Some Model using other db_alias"""
        return get_db(alias)

    @classmethod
    def get_collection(cls, doc_cls, alias=None, collection_name=None, read_preference=None):
        """Returns the collection for the document."""
        if alias is None:
            alias = doc_cls._get_db_alias()

        if collection_name is None:
            collection_name = doc_cls._get_collection_name()

        db = cls._get_db(alias=alias)

        # Create collection as a capped collection if specified
        if doc_cls._meta.get('max_size') or doc_cls._meta.get('max_documents'):
            # Get max document limit and max byte size from meta
            max_size = doc_cls._meta.get('max_size') or 10 * 2 ** 20  # 10MB default
            max_documents = doc_cls._meta.get('max_documents')
            # Round up to next 256 bytes as MongoDB would do it to avoid exception
            if max_size % 256:
                max_size = (max_size // 256 + 1) * 256

            if collection_name in db.collection_names():
                _collection = db[collection_name]
                # The collection already exists, check if its capped
                # options match the specified capped options
                options = _collection.options()
                if (
                    options.get('max') != max_documents or
                    options.get('size') != max_size
                ):
                    msg = (('Cannot create collection "%s" as a capped '
                            'collection as it already exists')
                           % _collection)
                    raise InvalidCollectionError(msg)
            else:
                # Create the collection as a capped collection
                opts = {'capped': True, 'size': max_size}
                if max_documents:
                    opts['max'] = max_documents
                _collection = db.create_collection(
                    collection_name, **opts
                )
        else:
            _collection = Collection(db, collection_name, read_preference=read_preference)
        return _collection

    def drop_collection(self, doc_cls, alias, collection_name):
        if alias is None:
            alias = doc_cls._get_db_alias()

        if collection_name is None:
            collection_name = doc_cls._get_collection_name()

        if not collection_name:
            from mongoengine.queryset import OperationError
            raise OperationError('Document %s has no collection defined '
                                 '(is it abstract ?)' % doc_cls)

        self.connections_registry[alias][collection_name] = {}
        db = self._get_db(alias=alias)
        db.drop_collection(collection_name)

    def drop_database(self, doc_cls, alias=None):
        if alias is None:
            alias = doc_cls._get_db_alias()

        self.connections_registry[alias] = defaultdict(dict)
        db = self._get_db(alias=alias)
        conn = get_connection(alias)
        conn.drop_database(db)

    def reset(self):
        self.connections_registry = defaultdict(lambda: defaultdict(dict))


connection_manager = ConnectionManager()
