from __future__ import absolute_import

from unittest import TestCase

from mongoengine.common import ReadOnlyContext


class TestReadOnlyContext(TestCase):

    def test_read_only_context(self):
        self.assertFalse(ReadOnlyContext.isActive())
        with ReadOnlyContext():
            self.assertTrue(ReadOnlyContext.isActive())
            with ReadOnlyContext():
                self.assertTrue(ReadOnlyContext.isActive())
            self.assertTrue(ReadOnlyContext.isActive())
        self.assertFalse(ReadOnlyContext.isActive())
