import unittest

from mongoengine import connect, Document, IntField, StringField, ListField

__all__ = ("FindAndModifyTest",)


class Doc(Document):
    id = IntField(primary_key=True)
    value = IntField()


class FindAndModifyTest(unittest.TestCase):

    def setUp(self):
        connect(db="mongoenginetest")
        Doc.drop_collection()

    def assertDbEqual(self, docs):
        self.assertEqual(list(Doc._collection.find().sort("id")), docs)

    def test_modify(self):
        Doc(id=0, value=0).save()
        doc = Doc(id=1, value=1).save()

        old_doc = Doc.objects(id=1).modify(set__value=-1)
        self.assertEqual(old_doc.to_json(), doc.to_json())
        self.assertDbEqual([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    def test_modify_with_new(self):
        Doc(id=0, value=0).save()
        doc = Doc(id=1, value=1).save()

        new_doc = Doc.objects(id=1).modify(set__value=-1, new=True)
        doc.value = -1
        self.assertEqual(new_doc.to_json(), doc.to_json())
        self.assertDbEqual([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    def test_modify_not_existing(self):
        Doc(id=0, value=0).save()
        self.assertEqual(Doc.objects(id=1).modify(set__value=-1), None)
        self.assertDbEqual([{"_id": 0, "value": 0}])

    def test_modify_with_upsert(self):
        Doc(id=0, value=0).save()
        old_doc = Doc.objects(id=1).modify(set__value=1, upsert=True)
        self.assertEqual(old_doc, None)
        self.assertDbEqual([{"_id": 0, "value": 0}, {"_id": 1, "value": 1}])

    def test_modify_with_upsert_existing(self):
        Doc(id=0, value=0).save()
        doc = Doc(id=1, value=1).save()

        old_doc = Doc.objects(id=1).modify(set__value=-1, upsert=True)
        self.assertEqual(old_doc.to_json(), doc.to_json())
        self.assertDbEqual([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    def test_modify_with_upsert_with_new(self):
        Doc(id=0, value=0).save()
        new_doc = Doc.objects(id=1).modify(upsert=True, new=True, set__value=1)
        self.assertEqual(new_doc.to_mongo(), {"_id": 1, "value": 1})
        self.assertDbEqual([{"_id": 0, "value": 0}, {"_id": 1, "value": 1}])

    def test_modify_with_remove(self):
        Doc(id=0, value=0).save()
        doc = Doc(id=1, value=1).save()

        old_doc = Doc.objects(id=1).modify(remove=True)
        self.assertEqual(old_doc.to_json(), doc.to_json())
        self.assertDbEqual([{"_id": 0, "value": 0}])

    def test_find_and_modify_with_remove_not_existing(self):
        Doc(id=0, value=0).save()
        self.assertEqual(Doc.objects(id=1).modify(remove=True), None)
        self.assertDbEqual([{"_id": 0, "value": 0}])

    def test_modify_with_order_by(self):
        Doc(id=0, value=3).save()
        Doc(id=1, value=2).save()
        Doc(id=2, value=1).save()
        doc = Doc(id=3, value=0).save()

        old_doc = Doc.objects().order_by("-id").modify(set__value=-1)
        self.assertEqual(old_doc.to_json(), doc.to_json())
        self.assertDbEqual([
            {"_id": 0, "value": 3}, {"_id": 1, "value": 2},
            {"_id": 2, "value": 1}, {"_id": 3, "value": -1}])

    def test_modify_with_fields(self):
        Doc(id=0, value=0).save()
        Doc(id=1, value=1).save()

        old_doc = Doc.objects(id=1).only("id").modify(set__value=-1)
        self.assertEqual(old_doc.to_mongo(), {"_id": 1})
        self.assertDbEqual([{"_id": 0, "value": 0}, {"_id": 1, "value": -1}])

    def test_modify_with_push(self):
        class BlogPost(Document):
            id = StringField(primary_key=True)
            tags = ListField(StringField())

        BlogPost.drop_collection()

        BlogPost(id="ABC").save()
        BlogPost(id="BCD").save()
        blog = BlogPost.objects(id="ABC").modify(push__tags="code")

        self.assertEqual(blog.to_mongo(), {"_id": "ABC", "tags": []})
        docs = [{"_id": "ABC", "tags":["code"]}, {"_id": "BCD", "tags":[]}]
        self.assertEqual(list(BlogPost._collection.find().sort("id")), docs)

        another_blog = BlogPost.objects(id="BCD").modify(push__tags="java")
        self.assertEqual(another_blog.to_mongo(), {"_id": "BCD", "tags": []})
        another_blog = BlogPost.objects(id="BCD").modify(push__tags__0=["python"])
        self.assertEqual(another_blog.to_mongo(), {"_id": "BCD", "tags": ["java"]})
        docs = [{"_id": "ABC", "tags":["code"]},
                {"_id": "BCD", "tags":["python", "java"]}]
        self.assertEqual(list(BlogPost._collection.find().sort("id")), docs)


if __name__ == '__main__':
    unittest.main()
