import unittest

from mongoengine import *
from mongoengine.pymongo_support import async_list_collection_names
from mongoengine.base.queryset import NULLIFY, PULL
from tests.asynchronous.utils import reset_async_connections


class TestClassMethods(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        await async_connect(db="mongoenginetest")
        self.db = await async_get_db()

        class Person(Document):
            name = StringField()
            age = IntField()

            non_field = True

            meta = {"allow_inheritance": True}

        self.Person = Person

    async def asyncTearDown(self):
        for collection in await async_list_collection_names(self.db):
            self.db.drop_collection(collection)
        await async_disconnect()
        await reset_async_connections()

    def test_definition(self):
        """Ensure that document may be defined using fields."""
        assert ["_cls", "age", "id", "name"] == sorted(self.Person._fields.keys())
        assert ["IntField", "ObjectIdField", "StringField", "StringField"] == sorted(
            x.__class__.__name__ for x in self.Person._fields.values()
        )

    async def test_get_db(self):
        """Ensure that get_db returns the expected db."""
        db = await async_get_db()
        assert self.db == db

    def test_get_collection_name(self):
        """Ensure that get_collection_name returns the expected collection
        name.
        """
        collection_name = "person"
        assert collection_name == self.Person._get_collection_name()

    async def test_get_collection(self):
        """Ensure that get_collection returns the expected collection."""
        collection_name = "person"
        collection = await self.Person._aget_collection()
        assert self.db[collection_name] == collection

    async def test_drop_collection(self):
        """Ensure that the collection may be dropped from the database."""
        collection_name = "person"
        await self.Person(name="Test").asave()
        assert collection_name in await async_list_collection_names(self.db)

        await self.Person.adrop_collection()
        assert collection_name not in await async_list_collection_names(self.db)

    def test_register_delete_rule(self):
        """Ensure that register delete rule adds a delete rule to the document
        meta.
        """

        class Job(Document):
            employee = ReferenceField(self.Person)

        assert self.Person._meta.get("delete_rules") is None

        self.Person.register_delete_rule(Job, "employee", NULLIFY)
        assert self.Person._meta["delete_rules"] == {(Job, "employee"): NULLIFY}

    async def test_compare_indexes(self):
        """Ensure that the indexes are properly created and that
        compare_indexes identifies the missing/extra indexes
        """

        class BlogPost(Document):
            author = StringField()
            title = StringField()
            description = StringField()
            tags = StringField()

            meta = {"indexes": [("author", "title")]}

        await BlogPost.adrop_collection()

        await BlogPost.aensure_indexes()
        assert await BlogPost.acompare_indexes() == {"missing": [], "extra": []}

        await BlogPost.acreate_index(["author", "description"])
        assert await BlogPost.acompare_indexes() == {
            "missing": [],
            "extra": [[("author", 1), ("description", 1)]],
        }

        await (await BlogPost._aget_collection()).drop_index("author_1_description_1")
        assert await BlogPost.acompare_indexes() == {"missing": [], "extra": []}

        await (await BlogPost._aget_collection()).drop_index("author_1_title_1")
        assert await BlogPost.acompare_indexes() == {
            "missing": [[("author", 1), ("title", 1)]],
            "extra": [],
        }

    async def test_compare_indexes_inheritance(self):
        """Ensure that the indexes are properly created and that
        compare_indexes identifies the missing/extra indexes for subclassed
        documents (_cls included)
        """

        class BlogPost(Document):
            author = StringField()
            title = StringField()
            description = StringField()

            meta = {"allow_inheritance": True}

        class BlogPostWithTags(BlogPost):
            tags = StringField()
            tag_list = ListField(StringField())

            meta = {"indexes": [("author", "tags")]}

        await BlogPost.adrop_collection()

        await BlogPost.aensure_indexes()
        await BlogPostWithTags.aensure_indexes()
        assert await BlogPost.acompare_indexes() == {"missing": [], "extra": []}

        await BlogPostWithTags.acreate_index(["author", "tag_list"])
        assert await BlogPost.acompare_indexes() == {
            "missing": [],
            "extra": [[("_cls", 1), ("author", 1), ("tag_list", 1)]],
        }

        await (await BlogPostWithTags._aget_collection()).drop_index("_cls_1_author_1_tag_list_1")
        assert await BlogPost.acompare_indexes() == {"missing": [], "extra": []}

        await (await BlogPostWithTags._aget_collection()).drop_index("_cls_1_author_1_tags_1")
        assert await BlogPost.acompare_indexes() == {
            "missing": [[("_cls", 1), ("author", 1), ("tags", 1)]],
            "extra": [],
        }

    async def test_compare_indexes_multiple_subclasses(self):
        """Ensure that compare_indexes behaves correctly if called from a
        class, which base class has multiple subclasses
        """

        class BlogPost(Document):
            author = StringField()
            title = StringField()
            description = StringField()

            meta = {"allow_inheritance": True}

        class BlogPostWithTags(BlogPost):
            tags = StringField()
            tag_list = ListField(StringField())

            meta = {"indexes": [("author", "tags")]}

        class BlogPostWithCustomField(BlogPost):
            custom = DictField()

            meta = {"indexes": [("author", "custom")]}

        await BlogPost.aensure_indexes()
        await BlogPostWithTags.aensure_indexes()
        await BlogPostWithCustomField.aensure_indexes()

        assert await BlogPost.acompare_indexes() == {"missing": [], "extra": []}
        assert await BlogPostWithTags.acompare_indexes() == {"missing": [], "extra": []}
        assert await BlogPostWithCustomField.acompare_indexes() == {"missing": [], "extra": []}

    async def test_compare_indexes_for_text_indexes(self):
        """Ensure that compare_indexes behaves correctly for text indexes"""

        class Doc(Document):
            a = StringField()
            b = StringField()
            meta = {
                "indexes": [
                    {
                        "fields": ["$a", "$b"],
                        "default_language": "english",
                        "weights": {"a": 10, "b": 2},
                    }
                ]
            }

        await Doc.adrop_collection()
        await Doc.aensure_indexes()
        actual = await Doc.acompare_indexes()
        expected = {"missing": [], "extra": []}
        assert actual == expected

    async def test_list_indexes_inheritance(self):
        """ensure that all of the indexes are listed regardless of the super-
        or sub-class that we call it from
        """

        class BlogPost(Document):
            author = StringField()
            title = StringField()
            description = StringField()

            meta = {"allow_inheritance": True}

        class BlogPostWithTags(BlogPost):
            tags = StringField()

            meta = {"indexes": [("author", "tags")]}

        class BlogPostWithTagsAndExtraText(BlogPostWithTags):
            extra_text = StringField()

            meta = {"indexes": [("author", "tags", "extra_text")]}

        await BlogPost.adrop_collection()

        await BlogPost.aensure_indexes()
        await BlogPostWithTags.aensure_indexes()
        await BlogPostWithTagsAndExtraText.aensure_indexes()

        assert await BlogPost.alist_indexes() == await BlogPostWithTags.alist_indexes()
        assert await BlogPost.alist_indexes() == await BlogPostWithTagsAndExtraText.alist_indexes()
        assert await BlogPost.alist_indexes() == [
            [("_cls", 1), ("author", 1), ("tags", 1)],
            [("_cls", 1), ("author", 1), ("tags", 1), ("extra_text", 1)],
            [("_id", 1)],
            [("_cls", 1)],
        ]

    def test_register_delete_rule_inherited(self):
        class Vaccine(Document):
            name = StringField(required=True)

            meta = {"indexes": ["name"]}

        class Animal(Document):
            family = StringField(required=True)
            vaccine_made = ListField(
                ReferenceField("Vaccine", reverse_delete_rule=PULL)
            )

            meta = {"allow_inheritance": True, "indexes": ["family"]}

        class Cat(Animal):
            name = StringField(required=True)

        assert Vaccine._meta["delete_rules"][(Animal, "vaccine_made")] == PULL
        assert Vaccine._meta["delete_rules"][(Cat, "vaccine_made")] == PULL

    def test_collection_naming(self):
        """Ensure that a collection with a specified name may be used."""

        class DefaultNamingTest(Document):
            pass

        assert "default_naming_test" == DefaultNamingTest._get_collection_name()

        class CustomNamingTest(Document):
            meta = {"collection": "pimp_my_collection"}

        assert "pimp_my_collection" == CustomNamingTest._get_collection_name()

        class DynamicNamingTest(Document):
            meta = {"collection": lambda c: "DYNAMO"}

        assert "DYNAMO" == DynamicNamingTest._get_collection_name()

        # Use Abstract class to handle backwards compatibility
        class BaseDocument(Document):
            meta = {"abstract": True, "collection": lambda c: c.__name__.lower()}

        class OldNamingConvention(BaseDocument):
            pass

        assert "oldnamingconvention" == OldNamingConvention._get_collection_name()

        class InheritedAbstractNamingTest(BaseDocument):
            meta = {"collection": "wibble"}

        assert "wibble" == InheritedAbstractNamingTest._get_collection_name()

        # Mixin tests
        class BaseMixin:
            meta = {"collection": lambda c: c.__name__.lower()}

        class OldMixinNamingConvention(Document, BaseMixin):
            pass

        assert (
                "oldmixinnamingconvention"
                == OldMixinNamingConvention._get_collection_name()
        )

        class BaseMixin:
            meta = {"collection": lambda c: c.__name__.lower()}

        class BaseDocument(Document, BaseMixin):
            meta = {"allow_inheritance": True}

        class MyDocument(BaseDocument):
            pass

        assert "basedocument" == MyDocument._get_collection_name()

    async def test_custom_collection_name_operations(self):
        """Ensure that a collection with a specified name is used as expected."""
        collection_name = "personCollTest"

        class Person(Document):
            name = StringField()
            meta = {"collection": collection_name}

        await Person(name="Test User").asave()
        assert collection_name in await async_list_collection_names(self.db)

        user_obj = await self.db[collection_name].find_one()
        assert user_obj["name"] == "Test User"

        user_obj = await Person.aobjects.first()
        assert user_obj.name == "Test User"

        await Person.adrop_collection()
        assert collection_name not in await async_list_collection_names(self.db)

    async def test_collection_name_and_primary(self):
        """Ensure that a collection with a specified name may be used."""

        class Person(Document):
            name = StringField(primary_key=True)
            meta = {"collection": "app"}

        await Person(name="Test User").asave()

        user_obj = await Person.aobjects.first()
        assert user_obj.name == "Test User"

        await Person.adrop_collection()


if __name__ == "__main__":
    unittest.main()
