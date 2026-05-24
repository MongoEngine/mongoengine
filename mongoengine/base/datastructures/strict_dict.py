"""StrictDict implementation for efficient fixed-key dictionaries."""


class StrictDict:
    __slots__ = ()
    _special_fields = {"get", "pop", "iteritems", "items", "keys", "create"}
    _classes = {}

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __getitem__(self, key):
        key = "_reserved_" + key if key in self._special_fields else key
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __setitem__(self, key, value):
        key = "_reserved_" + key if key in self._special_fields else key
        return setattr(self, key, value)

    def __contains__(self, key):
        return hasattr(self, key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key, default=None):
        v = self.get(key, default)
        try:
            delattr(self, key)
        except AttributeError:
            pass
        return v

    def iteritems(self):
        for key in self:
            yield key, self[key]

    def items(self):
        return [(k, self[k]) for k in iter(self)]

    def iterkeys(self):
        return iter(self)

    def keys(self):
        return list(iter(self))

    def __iter__(self):
        return (key for key in self.__slots__ if hasattr(self, key))

    def __len__(self):
        return len(list(self.items()))

    def __eq__(self, other):
        return list(self.items()) == list(other.items())

    def __ne__(self, other):
        return not (self == other)

    @classmethod
    def create(cls, allowed_keys):
        allowed_keys_tuple = tuple(
            ("_reserved_" + k if k in cls._special_fields else k) for k in allowed_keys
        )
        allowed_keys = frozenset(allowed_keys_tuple)
        if allowed_keys not in cls._classes:

            class SpecificStrictDict(cls):
                __slots__ = allowed_keys_tuple

                def __repr__(self):
                    return "{%s}" % ", ".join(
                        f'"{k!s}": {v!r}' for k, v in self.items()
                    )

            cls._classes[allowed_keys] = SpecificStrictDict
        return cls._classes[allowed_keys]


__all__ = ("StrictDict",)
