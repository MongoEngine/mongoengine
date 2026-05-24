import weakref

from mongoengine.common import _import_class

from .helpers import mark_as_changed_wrapper, mark_key_as_changed_wrapper


class BaseDict(dict):
    """A special dict so we can watch any changes."""

    _instance = None
    _name = None

    def __init__(self, dict_items, instance, name):
        BaseDocument = _import_class("BaseDocument")

        if isinstance(instance, BaseDocument):
            self._instance = weakref.proxy(instance)
        self._name = name
        super().__init__(dict_items)

    def get(self, key, default=None):
        # get does not use __getitem__ by default so we must override it as well
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __getitem__(self, key):
        value = super().__getitem__(key)

        EmbeddedDocument = _import_class("EmbeddedDocument")
        if isinstance(value, EmbeddedDocument) and value._instance is None:
            value._instance = self._instance
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            value = BaseDict(value, None, f"{self._name}.{key}")
            super().__setitem__(key, value)
            value._instance = self._instance
        elif isinstance(value, list):
            from .base_list import BaseList

            if not isinstance(value, BaseList):
                value = BaseList(value, None, f"{self._name}.{key}")
                super().__setitem__(key, value)
                value._instance = self._instance
        return value

    def __getstate__(self):
        self.instance = None
        return self

    def __setstate__(self, state):
        self = state
        return self

    __setitem__ = mark_key_as_changed_wrapper(dict.__setitem__)
    __delattr__ = mark_key_as_changed_wrapper(dict.__delattr__)
    __delitem__ = mark_key_as_changed_wrapper(dict.__delitem__)
    pop = mark_as_changed_wrapper(dict.pop)
    clear = mark_as_changed_wrapper(dict.clear)
    update = mark_as_changed_wrapper(dict.update)
    popitem = mark_as_changed_wrapper(dict.popitem)
    setdefault = mark_as_changed_wrapper(dict.setdefault)

    def _mark_as_changed(self, key=None):
        if hasattr(self._instance, "_mark_as_changed"):
            if key:
                self._instance._mark_as_changed(f"{self._name}.{key}")
            else:
                self._instance._mark_as_changed(self._name)


__all__ = ("BaseDict",)
