"""
This test has been put into a module.  This is because it tests warnings that
only get triggered on first hit.  This way we can ensure its imported into the
top level and called first by the test suite.
"""

import unittest
import warnings

from mongoengine import *
from mongoengine.base.common import _document_registry
from tests.asynchronous.utils import reset_async_connections
from tests.utils import MONGO_TEST_DB


class TestAllWarnings(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        await async_connect(db=MONGO_TEST_DB)
        self.warning_list = []
        self.showwarning_default = warnings.showwarning
        warnings.showwarning = self.append_to_warning_list

    async def asyncTearDown(self):
        warnings.showwarning = self.showwarning_default
        await async_disconnect_all()
        await reset_async_connections()

    def append_to_warning_list(self, message, category, *args):
        self.warning_list.append({"message": message, "category": category})

    def test_document_collection_syntax_warning(self):
        class NonAbstractBase(Document):
            meta = {"allow_inheritance": True}

        class InheritedDocumentFailTest(NonAbstractBase):
            meta = {"collection": "fail"}

        warning = self.warning_list[0]
        assert SyntaxWarning == warning["category"]
        assert "non_abstract_base" == InheritedDocumentFailTest._get_collection_name()
        _document_registry.pop(NonAbstractBase.__name__)
        _document_registry.pop(f"{NonAbstractBase.__name__}.{InheritedDocumentFailTest.__name__}")
