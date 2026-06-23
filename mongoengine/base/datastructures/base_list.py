import weakref

from mongoengine.common import _import_class

from .helpers import mark_as_changed_wrapper


class BaseList(list):
    """A special list so we can watch any changes."""

    _instance = None
    _name = None

    def __init__(self, list_items, instance, name):
        BaseDocument = _import_class("BaseDocument")

        if isinstance(instance, BaseDocument):
            if isinstance(instance, weakref.ProxyTypes):
                self._instance = instance
            else:
                self._instance = weakref.proxy(instance)

        self._name = name
        super().__init__(list_items)

    def __await__(self):
        """Allow safely using `await` on BaseList (returns self immediately)."""

        async def _return_self():
            return self

        return _return_self().__await__()

    def __getitem__(self, key):
        # change index to positive value because MongoDB does not support negative one
        if isinstance(key, int) and key < 0:
            key = len(self) + key
        value = super().__getitem__(key)

        if isinstance(key, slice):
            # When receiving a slice operator, we don't convert the structure and bind
            # to parent's instance. This is buggy for now but would require more work to be handled properly
            return value

        EmbeddedDocument = _import_class("EmbeddedDocument")
        if isinstance(value, EmbeddedDocument) and value._instance is None:
            value._instance = self._instance
        elif isinstance(value, dict):
            from .base_dict import BaseDict

            if not isinstance(value, BaseDict):
                # Replace dict by BaseDict
                value = BaseDict(value, None, f"{self._name}.{key}")
                super().__setitem__(key, value)
                value._instance = self._instance
        elif isinstance(value, list) and not isinstance(value, BaseList):
            # Replace list by BaseList
            value = BaseList(value, None, f"{self._name}.{key}")
            super().__setitem__(key, value)
            value._instance = self._instance
        return value

    def __iter__(self):
        yield from super().__iter__()

    def __getstate__(self):
        self.instance = None
        return self

    def __setstate__(self, state):
        self = state
        return self

    def __setitem__(self, key, value):
        changed_key = key
        if isinstance(key, slice):
            # In case of slice, we don't bother to identify the exact elements being updated
            # instead, we simply marks the whole list as changed
            changed_key = None

        result = super().__setitem__(key, value)
        self._mark_as_changed(changed_key)
        return result

    append = mark_as_changed_wrapper(list.append)
    extend = mark_as_changed_wrapper(list.extend)
    insert = mark_as_changed_wrapper(list.insert)
    pop = mark_as_changed_wrapper(list.pop)
    remove = mark_as_changed_wrapper(list.remove)
    reverse = mark_as_changed_wrapper(list.reverse)
    sort = mark_as_changed_wrapper(list.sort)
    clear = mark_as_changed_wrapper(list.clear)
    __delitem__ = mark_as_changed_wrapper(list.__delitem__)
    __iadd__ = mark_as_changed_wrapper(list.__iadd__)
    __imul__ = mark_as_changed_wrapper(list.__imul__)

    def _mark_as_changed(self, key=None):
        if hasattr(self._instance, "_mark_as_changed"):
            if key is not None:
                self._instance._mark_as_changed(f"{self._name}.{key % len(self)}")
            else:
                self._instance._mark_as_changed(self._name)


__all__ = ("BaseList",)
