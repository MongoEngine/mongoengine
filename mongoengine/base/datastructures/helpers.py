"""Helper decorators for tracking changes in datastructures."""


def mark_as_changed_wrapper(parent_method):
    """Decorator that ensures _mark_as_changed method gets called."""

    def wrapper(self, *args, **kwargs):
        # Can't use super() in the decorator.
        result = parent_method(self, *args, **kwargs)
        self._mark_as_changed()
        return result

    return wrapper


def mark_key_as_changed_wrapper(parent_method):
    """Decorator that ensures _mark_as_changed method gets called with the key argument"""

    def wrapper(self, key, *args, **kwargs):
        # Can't use super() in the decorator.
        if not args or key not in self or self[key] != args[0]:
            self._mark_as_changed(key)
        return parent_method(self, key, *args, **kwargs)

    return wrapper


__all__ = ("mark_as_changed_wrapper", "mark_key_as_changed_wrapper")
