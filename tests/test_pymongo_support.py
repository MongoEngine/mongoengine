from mongoengine import Document
from mongoengine.pymongo_support import count_documents
from tests.utils import MongoDBTestCase


class TestPymongoSupport(MongoDBTestCase):
    def test_count_documents(self):
        class Test(Document):
            pass

        Test.drop_collection()
        Test().save()
        Test().save()
        assert count_documents(Test._get_collection(), filter={}) == 2
        assert count_documents(Test._get_collection(), filter={}, skip=1) == 1
        assert count_documents(Test._get_collection(), filter={}, limit=0) == 0
