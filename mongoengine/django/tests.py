#coding: utf-8
from django.conf import settings
from django.db.models.loading import cache
from django.test import TestCase
from django.test.simple import DjangoTestSuiteRunner
from django.test.simple import build_suite, build_test, reorder_suite
from django.utils import unittest
from mongoengine import connect
from mongoengine.connection import get_db


class MongoTestCase(TestCase):
    """
    TestCase class that clear the collection between the tests
    """
    db_name = 'test_%s' % settings.MONGO_DATABASE_NAME

    def __init__(self, methodName='runtest'):
        self.db = get_db()
        super(MongoTestCase, self).__init__(methodName)

    def _post_teardown(self):
        super(MongoTestCase, self)._post_teardown()
        for collection in self.db.collection_names():
            if collection == 'system.indexes':
                continue
            self.db.drop_collection(collection)


class MongoengineTestSuiteRunner(DjangoTestSuiteRunner):
    """
    TestRunner that could be set as TEST_RUNNER in Django settings module to
    test MongoEngine projects.

    This class uses the same logic as MongoTestCase for determining database
    name and closes connection gracefully after running tests.
    """
    db_name = 'test_%s' % settings.MONGO_DATABASE_NAME

    def run_tests(self, test_labels, extra_tests=None, **kwargs):
        self.setup_test_environment()
        suite = self.build_suite(test_labels, extra_tests)
        connection = self.setup_databases()
        result = self.run_suite(suite)
        self.teardown_databases(connection)
        self.teardown_test_environment()
        return self.suite_result(suite, result)

    def setup_databases(self):
        return connect(self.db_name)

    def teardown_databases(self, connection, **kwargs):
        connection.disconnect()

    def build_suite(self, test_labels, extra_tests=None, **kwargs):
        """
        Rewrite of the original build_suite method of the DjangoTestSuiteRunner
        to skip tests from the Django own test cases as they restricted to the
        Django ORM settings that we do not need to be set at all.
        """
        suite = unittest.TestSuite()

        if test_labels:
            for label in test_labels:
                if '.' in label:
                    suite.addTest(build_test(label))
                else:
                    app = cache.get_app(label)
                    suite.addTest(build_suite(app))
        else:
            for app in self.get_apps():
                suite.addTest(build_suite(app))

        if extra_tests:
            for test in extra_tests:
                suite.addTest(test)

        return reorder_suite(suite, (TestCase,))

    def get_apps(self):
        """
        Do not run Django own tests
        """
        return filter(
            lambda app: app.__name__.split('.', 1)[0] != 'django',
            cache.get_apps())
