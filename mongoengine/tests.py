from mongoengine.connection import get_db


class query_counter(object):
    """ Query_counter contextmanager to get the number of queries. """

    def __init__(self):
        """ Construct the query_counter. """
        self.counter = 0
        self.db = get_db()

    def __enter__(self):
        """ On every with block we need to drop the profile collection. """
        self.db.set_profiling_level(0)
        self.db.system.profile.drop()
        self.db.set_profiling_level(2)
        return self

    def __exit__(self, t, value, traceback):
        """ Reset the profiling level. """
        self.db.set_profiling_level(0)

    def __eq__(self, value):
        """ == Compare querycounter. """
        return value == self._get_count()

    def __ne__(self, value):
        """ != Compare querycounter. """
        return not self.__eq__(value)

    def __lt__(self, value):
        """ < Compare querycounter. """
        return self._get_count() < value

    def __le__(self, value):
        """ <= Compare querycounter. """
        return self._get_count() <= value

    def __gt__(self, value):
        """ > Compare querycounter. """
        return self._get_count() > value

    def __ge__(self, value):
        """ >= Compare querycounter. """
        return self._get_count() >= value

    def __int__(self):
        """ int representation. """
        return self._get_count()

    def __repr__(self):
        """ repr query_counter as the number of queries. """
        return u"%s" % self._get_count()

    def _get_count(self):
        """ Get the number of queries. """
        count = self.db.system.profile.find().count() - self.counter
        self.counter += 1
        return count
