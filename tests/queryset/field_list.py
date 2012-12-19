import sys
sys.path[0:0] = [""]

import unittest

from mongoengine import *
from mongoengine.queryset import QueryFieldList

__all__ = ("QueryFieldListTest",)

class QueryFieldListTest(unittest.TestCase):

    def test_empty(self):
        q = QueryFieldList()
        self.assertFalse(q)

        q = QueryFieldList(always_include=['_cls'])
        self.assertFalse(q)

    def test_include_include(self):
        q = QueryFieldList()
        q += QueryFieldList(fields=['a', 'b'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'a': True, 'b': True})
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'b': True})

    def test_include_exclude(self):
        q = QueryFieldList()
        q += QueryFieldList(fields=['a', 'b'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'a': True, 'b': True})
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.EXCLUDE)
        self.assertEqual(q.as_dict(), {'a': True})

    def test_exclude_exclude(self):
        q = QueryFieldList()
        q += QueryFieldList(fields=['a', 'b'], value=QueryFieldList.EXCLUDE)
        self.assertEqual(q.as_dict(), {'a': False, 'b': False})
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.EXCLUDE)
        self.assertEqual(q.as_dict(), {'a': False, 'b': False, 'c': False})

    def test_exclude_include(self):
        q = QueryFieldList()
        q += QueryFieldList(fields=['a', 'b'], value=QueryFieldList.EXCLUDE)
        self.assertEqual(q.as_dict(), {'a': False, 'b': False})
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'c': True})

    def test_always_include(self):
        q = QueryFieldList(always_include=['x', 'y'])
        q += QueryFieldList(fields=['a', 'b', 'x'], value=QueryFieldList.EXCLUDE)
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'x': True, 'y': True, 'c': True})

    def test_reset(self):
        q = QueryFieldList(always_include=['x', 'y'])
        q += QueryFieldList(fields=['a', 'b', 'x'], value=QueryFieldList.EXCLUDE)
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'x': True, 'y': True, 'c': True})
        q.reset()
        self.assertFalse(q)
        q += QueryFieldList(fields=['b', 'c'], value=QueryFieldList.ONLY)
        self.assertEqual(q.as_dict(), {'x': True, 'y': True, 'b': True, 'c': True})

    def test_using_a_slice(self):
        q = QueryFieldList()
        q += QueryFieldList(fields=['a'], value={"$slice": 5})
        self.assertEqual(q.as_dict(), {'a': {"$slice": 5}})
