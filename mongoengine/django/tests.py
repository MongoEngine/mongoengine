#coding: utf-8

from mongoengine import connect
from mongoengine.connection import get_db
from mongoengine.python_support import PY3

try:
    from django.test import TestCase
except Exception as err:
    if PY3:
        from unittest import TestCase
    else:
        raise err


class MongoTestCase(TestCase):
    """
    TestCase class that clear the collection between the tests
    """

    @property
    def db_name(self):
        from django.conf import settings
        return 'test_%s' % getattr(settings, 'MONGO_DATABASE_NAME', 'dummy')

    def __init__(self, methodName='runtest'):
        connect(self.db_name)
        self.db = get_db()
        super(MongoTestCase, self).__init__(methodName)

    def _post_teardown(self):
        super(MongoTestCase, self)._post_teardown()
        for collection in self.db.collection_names():
            if collection == 'system.indexes':
                continue
            self.db.drop_collection(collection)

    # prevent standard db init

    def _databases_names(self, *args, **kwargs):
        return []

    def _fixture_setup(self):
        pass

    def _fixture_teardown(self):
        pass
