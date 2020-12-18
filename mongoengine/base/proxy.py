from __future__ import absolute_import
import weakref

import copy
import lazy_object_proxy
from contextlib2 import contextmanager
from collections import defaultdict


def _get_field(doc, fields):
    for index in range(0, len(fields)):
        if doc is None:
            return None
        attname = fields[index].name
        if not (hasattr(doc, '_data') and attname in doc._data):
            return None
        doc = doc._data[attname]
    return getattr(doc, 'id', None)


class LazyPrefetchBase:
    _result_cache = None
    # mapping from field path to count of refereneces fetched
    _reference_cache_count = None
    # mapping from field path to map of id -> (data, is_doc) tuple
    # data is Document object if `is_doc`, else it is SON
    _reference_cache = None
    # decides whether to make the created DocumentProxy as being optimized using LazyPrefetchBase
    # ideally we should always mark them, but making it opt-in so that the flag can be used for optimization in very sepcific conitions
    _should_mark_document_proxy = False

    def _init_reference(self):
        if self._reference_cache_count is None:
            self._reference_cache_count = defaultdict(int)
        if self._reference_cache is None:
            self._reference_cache = defaultdict(dict)

    def lazy_prefetch_available(self):
        # make sure we have initialized reference dicts and have data in `_result_cache`
        return self._reference_cache_count is not None and self._result_cache

    def should_mark_document_proxy(self):
        return self._should_mark_document_proxy and self.lazy_prefetch_available()

    def try_fetch_document(self, value, cls, fields):
        # tries to fetch document of class `cls` at the path given by `fields` from the reference `value`
        # returns a tuple (is_valid, data): a bool whether fetched value is valid, and the fetched value
        field_path = '.'.join([f.name for f in fields])
        if self._is_prefetched(field_path, value):
            return (True, self._get_prefetched_document(value, cls, fields, field_path))

        # might have already fetched data for some prefix of `_result_cache`
        # `_reference_cache_count` stores the number of docs for which data was already fetched
        start, end = self._reference_cache_count[field_path], len(self._result_cache)

        ids = [_get_field(doc, fields) for doc in self._result_cache[start:end]]
        ids = [id for id in ids if id is not None]

        # This case usually happens when a queryset cache is not updated, like when cloning a queryset.
        if value.id not in ids:
            return (False, None)

        # Fetching is inevitable
        cursor = cls._get_db()[value.collection].find({"_id": {"$in": ids}})
        id_son_map = dict((son['_id'], son) for son in cursor)

        self._reference_cache[field_path].update(dict((id, (id_son_map.get(id, None), False)) for id in ids))

        # update `_reference_cache_count` so that we start slice from `end` the next time
        self._reference_cache_count[field_path] = end

        # we already checked `value.id` is present in `ids`
        return (True, self._get_prefetched_document(value, cls, fields, field_path))

    def _is_prefetched(self, field_path, value):
        # checks if data for `value` at `field_path` has already been fetched
        return value.id in self._reference_cache[field_path]

    def _get_prefetched_document(self, value, cls, fields, field_path):
        # get the prefetched data as document
        # only call when `is_prefetched(field_path, id)` is True
        d = self._reference_cache[field_path]
        data, is_doc = d[value.id]
        if data is None or is_doc:
            return data
        # else we have SON data. gotta convert it to doc before returning
        doc = cls._from_son(data, _lazy_prefetch_base=self, _fields=fields)
        d[value.id] = (doc, True)
        return doc


class ListFieldProxy(list, LazyPrefetchBase):
    def __init__(self, _list):
        self._result_cache = _list
        self._init_reference()


class DocumentProxy(lazy_object_proxy.Proxy):
    id = None
    collection = None
    wrapped = None
    _instance = None
    _lazy_prefetch_optimized = False

    def __init__(self, wrapped, id, collection, instance=None, lazy_prefetch_base=None):
        super(DocumentProxy, self).__init__(wrapped)
        self.id = id
        self.collection = collection
        if instance:
            self._instance = weakref.proxy(instance)
        if lazy_prefetch_base is not None:
            self._lazy_prefetch_optimized = lazy_prefetch_base.should_mark_document_proxy()

    def __call__(self, *args, **kwargs):
        # Hack as callable(lazy_object_proxy.Proxy) return True
        return self.__wrapped__

    def __eq__(self, other):
        if type(other) is DocumentProxy or hasattr(other, 'id'):
            return self.id == other.id
        return self.__wrapped__ == other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __nonzero__(self):
        return self.id is not None

    def __deepcopy__(self, memo):
        if getattr(DocumentProxy, 'should_ignore_deep_copy', False):
            return self
        return copy.deepcopy(self.__wrapped__, memo)

    @staticmethod
    @contextmanager
    def ignore_deep_copy():
        """
        Ignore deep copy for DocumentProxy for performance reasons where needed.
        """
        DocumentProxy.should_ignore_deep_copy = True
        yield
        DocumentProxy.should_ignore_deep_copy = False

    def __hash__(self):
        return hash(self.id) if self.id is not None else hash(self.__wrapped__)
