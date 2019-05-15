import weakref

import copy
import lazy_object_proxy
from contextlib2 import contextmanager


class DocumentProxy(lazy_object_proxy.Proxy):
    id = None
    collection = None
    wrapped = None
    _instance = None
    
    def __init__(self, wrapped, id, collection, instance=None):
        super(DocumentProxy, self).__init__(wrapped)
        self.id = id
        self.collection = collection
        if instance:
            self._instance = weakref.proxy(instance)

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
