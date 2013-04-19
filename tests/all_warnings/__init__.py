"""
This test has been put into a module.  This is because it tests warnings that
only get triggered on first hit.  This way we can ensure its imported into the
top level and called first by the test suite.
"""
import sys
sys.path[0:0] = [""]
import unittest
import warnings

from mongoengine import *


__all__ = ('AllWarnings', )


class AllWarnings(unittest.TestCase):

    def setUp(self):
        conn = connect(db='mongoenginetest')
        self.warning_list = []
        self.showwarning_default = warnings.showwarning
        warnings.showwarning = self.append_to_warning_list

    def append_to_warning_list(self, message, category, *args):
        self.warning_list.append({"message": message,
                                  "category": category})

    def tearDown(self):
        # restore default handling of warnings
        warnings.showwarning = self.showwarning_default

    def test_dbref_reference_field_future_warning(self):

        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')

        Person.drop_collection()

        p1 = Person()
        p1.parent = None
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p2.save(cascade=False)

        self.assertTrue(len(self.warning_list) > 0)
        warning = self.warning_list[0]
        self.assertEqual(FutureWarning, warning["category"])
        self.assertTrue("ReferenceFields will default to using ObjectId"
                        in str(warning["message"]))

    def test_document_save_cascade_future_warning(self):

        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')

        Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p2.parent.name = "Poppa Wilson"
        p2.save()

        self.assertTrue(len(self.warning_list) > 0)
        if len(self.warning_list) > 1:
            print self.warning_list
        warning = self.warning_list[0]
        self.assertEqual(FutureWarning, warning["category"])
        self.assertTrue("Cascading saves will default to off in 0.8"
                        in str(warning["message"]))

    def test_document_collection_syntax_warning(self):

        class NonAbstractBase(Document):
            meta = {'allow_inheritance': True}

        class InheritedDocumentFailTest(NonAbstractBase):
            meta = {'collection': 'fail'}

        warning = self.warning_list[0]
        self.assertEqual(SyntaxWarning, warning["category"])
        self.assertEqual('non_abstract_base',
                         InheritedDocumentFailTest._get_collection_name())

import sys
sys.path[0:0] = [""]
