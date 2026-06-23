import unittest

from mongoengine import (
    Document,
    IntField,
    ListField,
    StringField,
)
from tests.asynchronous.utils import MongoDBAsyncTestCase


class Doc(Document):
    id = IntField(primary_key=True)
    value = IntField()


class TestOnlyExcludeAll(MongoDBAsyncTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await Doc.adrop_collection()

    async def asyncTearDown(self):
        await super().asyncTearDown()

    async def _assert_db_equal(self, docs):
        assert await (await Doc._aget_collection()).find().sort("id").to_list() == docs

    async def test_modify(self):
        await Doc(id=0, value=0).asave()
        doc = await Doc(id=1, value=1).asave()

        old_doc = await Doc.aobjects(id=1).modify(set__value=-1)
        assert old_doc.to_json() == doc.to_json()
        await self._assert_db_equal([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    async def test_modify_with_new(self):
        await Doc(id=0, value=0).asave()
        doc = await Doc(id=1, value=1).asave()

        new_doc = await Doc.aobjects(id=1).modify(set__value=-1, new=True)
        doc.value = -1
        assert new_doc.to_json() == doc.to_json()
        await self._assert_db_equal([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    async def test_modify_not_existing(self):
        await Doc(id=0, value=0).asave()
        assert await Doc.aobjects(id=1).modify(set__value=-1) is None
        await self._assert_db_equal([{"_id": 0, "value": 0}])

    async def test_modify_with_upsert(self):
        await Doc(id=0, value=0).asave()
        old_doc = await Doc.aobjects(id=1).modify(set__value=1, upsert=True)
        assert old_doc is None
        await self._assert_db_equal([{"_id": 0, "value": 0}, {"_id": 1, "value": 1}])

    async def test_modify_with_upsert_existing(self):
        await Doc(id=0, value=0).asave()
        doc = await Doc(id=1, value=1).asave()

        old_doc = await Doc.aobjects(id=1).modify(set__value=-1, upsert=True)
        assert old_doc.to_json() == doc.to_json()
        await self._assert_db_equal([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    async def test_modify_with_upsert_with_new(self):
        await Doc(id=0, value=0).asave()
        new_doc = await Doc.aobjects(id=1).modify(upsert=True, new=True, set__value=1)
        assert new_doc.to_mongo() == {"_id": 1, "value": 1}
        await self._assert_db_equal([{"_id": 0, "value": 0}, {"_id": 1, "value": 1}])

    async def test_modify_with_remove(self):
        await Doc(id=0, value=0).asave()
        doc = await Doc(id=1, value=1).asave()

        old_doc = await Doc.aobjects(id=1).modify(remove=True)
        assert old_doc.to_json() == doc.to_json()
        await self._assert_db_equal([{"_id": 0, "value": 0}])

    async def test_find_and_modify_with_remove_not_existing(self):
        await Doc(id=0, value=0).asave()
        assert await Doc.aobjects(id=1).modify(remove=True) is None
        await self._assert_db_equal([{"_id": 0, "value": 0}])

    async def test_modify_with_order_by(self):
        await Doc(id=0, value=3).asave()
        await Doc(id=1, value=2).asave()
        await Doc(id=2, value=1).asave()
        doc = await Doc(id=3, value=0).asave()

        old_doc = await Doc.aobjects().order_by("-id").modify(set__value=-1)
        assert old_doc.to_json() == doc.to_json()
        await self._assert_db_equal(
            [
                {"_id": 0, "value": 3},
                {"_id": 1, "value": 2},
                {"_id": 2, "value": 1},
                {"_id": 3, "value": -1},
            ]
        )

    async def test_modify_with_fields(self):
        await Doc(id=0, value=0).asave()
        await Doc(id=1, value=1).asave()

        old_doc = await Doc.aobjects(id=1).only("id").modify(set__value=-1)
        assert old_doc.to_mongo() == {"_id": 1}
        await self._assert_db_equal([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    async def test_modify_with_push(self):
        class BlogPost(Document):
            tags = ListField(StringField())

        await BlogPost.adrop_collection()

        blog = await BlogPost.aobjects.create()

        # Push a new tag via modify with new=False (default).
        await BlogPost(id=blog.id).amodify(push__tags="code")
        assert blog.tags == []
        await blog.areload()
        assert blog.tags == ["code"]

        # Push a new tag via modify with new=True.
        blog = await BlogPost.aobjects(id=blog.id).modify(push__tags="java", new=True)
        assert blog.tags == ["code", "java"]

        # Push a new tag with a positional argument.
        blog = await BlogPost.aobjects(id=blog.id).modify(
            push__tags__0="python", new=True
        )
        assert blog.tags == ["python", "code", "java"]

        # Push multiple new tags with a positional argument.
        blog = await BlogPost.aobjects(id=blog.id).modify(
            push__tags__1=["go", "rust"], new=True
        )
        assert blog.tags == ["python", "go", "rust", "code", "java"]


if __name__ == "__main__":
    unittest.main()
