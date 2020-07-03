import unittest

from mongoengine import Document, IntField, ListField, StringField, connect


class Doc(Document):
    id = IntField(primary_key=True)
    value = IntField()


class TestFindAndModify(unittest.TestCase):
    def setUp(self):
        connect(db="mongoenginetest")
        Doc.drop_collection()

    def assertDbEqual(self, docs):
        assert list(Doc._collection.find().sort("id")) == docs

    def test_modify(self):
        Doc(id=0, value=0).save()
        doc = Doc(id=1, value=1).save()

        old_doc = Doc.objects(id=1).modify(set__value=-1)
        assert old_doc.to_json() == doc.to_json()
        self.assertDbEqual([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    def test_modify_with_new(self):
        Doc(id=0, value=0).save()
        doc = Doc(id=1, value=1).save()

        new_doc = Doc.objects(id=1).modify(set__value=-1, new=True)
        doc.value = -1
        assert new_doc.to_json() == doc.to_json()
        self.assertDbEqual([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    def test_modify_not_existing(self):
        Doc(id=0, value=0).save()
        assert Doc.objects(id=1).modify(set__value=-1) is None
        self.assertDbEqual([{"_id": 0, "value": 0}])

    def test_modify_with_upsert(self):
        Doc(id=0, value=0).save()
        old_doc = Doc.objects(id=1).modify(set__value=1, upsert=True)
        assert old_doc is None
        self.assertDbEqual([{"_id": 0, "value": 0}, {"_id": 1, "value": 1}])

    def test_modify_with_upsert_existing(self):
        Doc(id=0, value=0).save()
        doc = Doc(id=1, value=1).save()

        old_doc = Doc.objects(id=1).modify(set__value=-1, upsert=True)
        assert old_doc.to_json() == doc.to_json()
        self.assertDbEqual([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    def test_modify_with_upsert_with_new(self):
        Doc(id=0, value=0).save()
        new_doc = Doc.objects(id=1).modify(upsert=True, new=True, set__value=1)
        assert new_doc.to_mongo() == {"_id": 1, "value": 1}
        self.assertDbEqual([{"_id": 0, "value": 0}, {"_id": 1, "value": 1}])

    def test_modify_with_remove(self):
        Doc(id=0, value=0).save()
        doc = Doc(id=1, value=1).save()

        old_doc = Doc.objects(id=1).modify(remove=True)
        assert old_doc.to_json() == doc.to_json()
        self.assertDbEqual([{"_id": 0, "value": 0}])

    def test_find_and_modify_with_remove_not_existing(self):
        Doc(id=0, value=0).save()
        assert Doc.objects(id=1).modify(remove=True) is None
        self.assertDbEqual([{"_id": 0, "value": 0}])

    def test_modify_with_order_by(self):
        Doc(id=0, value=3).save()
        Doc(id=1, value=2).save()
        Doc(id=2, value=1).save()
        doc = Doc(id=3, value=0).save()

        old_doc = Doc.objects().order_by("-id").modify(set__value=-1)
        assert old_doc.to_json() == doc.to_json()
        self.assertDbEqual(
            [
                {"_id": 0, "value": 3},
                {"_id": 1, "value": 2},
                {"_id": 2, "value": 1},
                {"_id": 3, "value": -1},
            ]
        )

    def test_modify_with_fields(self):
        Doc(id=0, value=0).save()
        Doc(id=1, value=1).save()

        old_doc = Doc.objects(id=1).only("id").modify(set__value=-1)
        assert old_doc.to_mongo() == {"_id": 1}
        self.assertDbEqual([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    def test_modify_with_push(self):
        class BlogPost(Document):
            tags = ListField(StringField())

        BlogPost.drop_collection()

        blog = BlogPost.objects.create()

        # Push a new tag via modify with new=False (default).
        BlogPost(id=blog.id).modify(push__tags="code")
        assert blog.tags == []
        blog.reload()
        assert blog.tags == ["code"]

        # Push a new tag via modify with new=True.
        blog = BlogPost.objects(id=blog.id).modify(push__tags="java", new=True)
        assert blog.tags == ["code", "java"]

        # Push a new tag with a positional argument.
        blog = BlogPost.objects(id=blog.id).modify(push__tags__0="python", new=True)
        assert blog.tags == ["python", "code", "java"]

        # Push multiple new tags with a positional argument.
        blog = BlogPost.objects(id=blog.id).modify(
            push__tags__1=["go", "rust"], new=True
        )
        assert blog.tags == ["python", "go", "rust", "code", "java"]


if __name__ == "__main__":
    unittest.main()
