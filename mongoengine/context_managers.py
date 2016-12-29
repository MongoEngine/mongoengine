from mongoengine.common import _import_class
from mongoengine.connection import DEFAULT_CONNECTION_NAME, get_db


__all__ = ('no_dereference',
           'no_sub_classes', 'query_counter')



class no_dereference(object):
    """no_dereference context manager.

    Turns off all dereferencing in Documents for the duration of the context
    manager::

        with no_dereference(Group) as Group:
            Group.objects.find()
    """

    def __init__(self, cls):
        """Construct the no_dereference context manager.

        :param cls: the class to turn dereferencing off on
        """
        self.cls = cls

        ReferenceField = _import_class('ReferenceField')
        GenericReferenceField = _import_class('GenericReferenceField')
        ComplexBaseField = _import_class('ComplexBaseField')

        self.deref_fields = [k for k, v in self.cls._fields.iteritems()
                             if isinstance(v, (ReferenceField,
                                               GenericReferenceField,
                                               ComplexBaseField))]

    def __enter__(self):
        """Change the objects default and _auto_dereference values."""
        for field in self.deref_fields:
            self.cls._fields[field]._auto_dereference = False
        return self.cls

    def __exit__(self, t, value, traceback):
        """Reset the default and _auto_dereference values."""
        for field in self.deref_fields:
            self.cls._fields[field]._auto_dereference = True
        return self.cls


class no_sub_classes(object):
    """no_sub_classes context manager.

    Only returns instances of this class and no sub (inherited) classes::

        with no_sub_classes(Group) as Group:
            Group.objects.find()
    """

    def __init__(self, cls):
        """Construct the no_sub_classes context manager.

        :param cls: the class to turn querying sub classes on
        """
        self.cls = cls

    def __enter__(self):
        """Change the objects default and _auto_dereference values."""
        self.cls._all_subclasses = self.cls._subclasses
        self.cls._subclasses = (self.cls,)
        return self.cls

    def __exit__(self, t, value, traceback):
        """Reset the default and _auto_dereference values."""
        self.cls._subclasses = self.cls._all_subclasses
        delattr(self.cls, '_all_subclasses')
        return self.cls


class query_counter(object):
    """Query_counter context manager to get the number of queries."""

    def __init__(self):
        """Construct the query_counter."""
        self.counter = 0
        self.db = get_db()

    def __enter__(self):
        """On every with block we need to drop the profile collection."""
        self.db.set_profiling_level(0)
        self.db.system.profile.drop()
        self.db.set_profiling_level(2)
        return self

    def __exit__(self, t, value, traceback):
        """Reset the profiling level."""
        self.db.set_profiling_level(0)

    def __eq__(self, value):
        """== Compare querycounter."""
        counter = self._get_count()
        return value == counter

    def __ne__(self, value):
        """!= Compare querycounter."""
        return not self.__eq__(value)

    def __lt__(self, value):
        """< Compare querycounter."""
        return self._get_count() < value

    def __le__(self, value):
        """<= Compare querycounter."""
        return self._get_count() <= value

    def __gt__(self, value):
        """> Compare querycounter."""
        return self._get_count() > value

    def __ge__(self, value):
        """>= Compare querycounter."""
        return self._get_count() >= value

    def __int__(self):
        """int representation."""
        return self._get_count()

    def __repr__(self):
        """repr query_counter as the number of queries."""
        return u"%s" % self._get_count()

    def _get_count(self):
        """Get the number of queries."""
        ignore_query = {'ns': {'$ne': '%s.system.indexes' % self.db.name}}
        count = self.db.system.profile.find(ignore_query).count() - self.counter
        self.counter += 1
        return count
