from functools import partial

from mongoengine.synchronous.queryset import QuerySet

__all__ = ("queryset_manager", "QuerySetManager")


class QuerySetManager:
    """
    The default QuerySet Manager.

    Custom QuerySet Manager functions can extend this class and users can
    add extra queryset functionality.  Any custom manager methods must accept a
    :class:`~mongoengine.Document` class as its first argument, and a
    :class:`~mongoengine.queryset.QuerySet` as its second argument.

    The method function should return a :class:`~mongoengine.queryset.QuerySet`
    , probably the same one that was passed in, but modified in some way.
    """

    get_queryset = None

    def __init__(self, queryset_func=None, default=QuerySet):
        if queryset_func:
            self.get_queryset = queryset_func
        self.default = default

    def __get__(self, instance, owner):
        """Descriptor for instantiating a new QuerySet object when
        Document.objects is accessed.
        """
        if instance is not None:
            # Document object being used rather than a document class
            return self

        # owner is the document that contains the QuerySetManager
        queryset_class = owner._meta.get("queryset_class", self.default)
        if issubclass(queryset_class, QuerySet):
            queryset = queryset_class(owner)
        else:
            queryset = queryset_class(owner)
        if self.get_queryset:
            arg_count = self.get_queryset.__code__.co_argcount
            if arg_count == 1:
                queryset = self.get_queryset(queryset)
            elif arg_count == 2:
                queryset = self.get_queryset(owner, queryset)
            else:
                queryset = partial(self.get_queryset, owner, queryset)
        return queryset


def queryset_manager(func=None, *, queryset=QuerySet):
    """Decorator that allows you to define custom QuerySet managers on
    :class:`~mongoengine.Document` classes.

    The manager must be a function that accepts a
    :class:`~mongoengine.Document` class as its first argument, and either a
    :class:`~mongoengine.queryset.QuerySet` or
    :class:`~mongoengine.queryset.AsyncQuerySet` as its second argument.

    The method function should return a
    :class:`~mongoengine.queryset.QuerySet` or
    :class:`~mongoengine.queryset.AsyncQuerySet`, probably the same one that
    was passed in, but modified in some way.

    The ``default`` parameter determines which type of queryset manager is
    created (defaults to ``mongoengine.queryset.QuerySet``).
    """

    def decorator(f):
        return QuerySetManager(f, queryset)

    if func is not None:
        # Used as @queryset_manager
        return decorator(func)
    # Used as @queryset_manager(default=AsyncQuerySet)
    return decorator
