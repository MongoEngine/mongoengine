import pytest

from mongoengine import *
from mongoengine.base import BaseDict
from tests.asynchronous.utils import MongoDBAsyncTestCase, async_get_as_pymongo


class TestDictField(MongoDBAsyncTestCase):
    async def test_storage(self):
        class BlogPost(Document):
            info = DictField()

        await BlogPost.adrop_collection()

        info = {"testkey": "testvalue"}
        post = await BlogPost(info=info).asave()
        assert await async_get_as_pymongo(post) == {"_id": post.id, "info": info}

    async def test_validate_invalid_type(self):
        class BlogPost(Document):
            info = DictField()

        await BlogPost.adrop_collection()

        invalid_infos = ["my post", ["test", "test"], {1: "test"}]
        for invalid_info in invalid_infos:
            with pytest.raises(ValidationError):
                BlogPost(info=invalid_info).validate()

    async def test_keys_with_dots_or_dollars(self):
        class BlogPost(Document):
            info = DictField()

        await BlogPost.adrop_collection()

        post = BlogPost()

        post.info = {"$title": "test"}
        with pytest.raises(ValidationError):
            post.validate()

        post.info = {"nested": {"$title": "test"}}
        with pytest.raises(ValidationError):
            post.validate()

        post.info = {"$title.test": "test"}
        with pytest.raises(ValidationError):
            post.validate()

        post.info = {"nested": {"the.title": "test"}}
        post.validate()

        post.info = {"dollar_and_dot": {"te$st.test": "test"}}
        post.validate()

    async def test_general_things(self):
        """Ensure that dict types work as expected."""

        class BlogPost(Document):
            info = DictField()

        await BlogPost.adrop_collection()  # todo

        post = BlogPost(info={"title": "test"})
        await post.asave()

        post = BlogPost()
        post.info = {"title": "dollar_sign", "details": {"te$t": "test"}}
        await post.asave()

        post = BlogPost()
        post.info = {"details": {"test": "test"}}
        await post.asave()

        post = BlogPost()
        post.info = {"details": {"test": 3}}
        await post.asave()

        assert await BlogPost.aobjects.count() == 4
        assert await BlogPost.aobjects.filter(info__title__exact="test").count() == 1
        assert (
            await BlogPost.aobjects.filter(info__details__test__exact="test").count()
            == 1
        )

        post = await BlogPost.aobjects.filter(info__title__exact="dollar_sign").first()
        assert "te$t" in post["info"]["details"]

        # Confirm handles non strings or non existing keys
        assert await BlogPost.aobjects.filter(info__details__test__exact=5).count() == 0
        assert (
            await BlogPost.aobjects.filter(info__made_up__test__exact="test").count()
            == 0
        )

        post = await BlogPost.aobjects.create(info={"title": "original"})
        post.info.update({"title": "updated"})
        await post.asave()
        await post.areload()
        assert "updated" == post.info["title"]

        post.info.setdefault("authors", [])
        await post.asave()
        await post.areload()
        assert post.info["authors"] == []

    async def test_dictfield_dump_document_with_inheritance__cls(self):
        """Ensure a DictField can handle another document's dump."""

        class Doc(Document):
            field = DictField()

        class ToEmbedParent(Document):
            id = IntField(primary_key=True)
            recursive = DictField()

            meta = {"allow_inheritance": True}

        class ToEmbedChild(ToEmbedParent):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

        await Doc.adrop_collection()
        await ToEmbedParent.adrop_collection()

        # with a Document with a _cls field
        to_embed_recursive = await ToEmbedChild(id=1).asave()
        to_embed_child = await ToEmbedChild(
            id=2, recursive=to_embed_recursive.to_mongo().to_dict()
        ).asave()

        doc_dump_as_dict = to_embed_child.to_mongo().to_dict()
        doc = Doc(field=doc_dump_as_dict)
        assert isinstance(doc.field, ToEmbedChild)
        await doc.asave()
        assert isinstance(doc.field, ToEmbedChild)
        assert doc.field == to_embed_child

    async def test_dictfield_dump_document_no_inheritance(self):
        """Ensure a DictField can handle another document's dump."""

        class Doc(Document):
            field = DictField()

        class ToEmbed(Document):
            id = IntField(primary_key=True)
            recursive = DictField()

        to_embed_recursive = await ToEmbed(id=1).asave()
        to_embed = await ToEmbed(
            id=2, recursive=(to_embed_recursive.to_mongo().to_dict())
        ).asave()
        doc = Doc(field=to_embed.to_mongo().to_dict())
        await doc.asave()
        assert isinstance(doc.field, dict)
        assert doc.field == {"_id": 2, "recursive": {"_id": 1, "recursive": {}}}

    async def test_dictfield_strict(self):
        """Ensure that dict field handles validation if provided a strict field type."""

        class Simple(Document):
            mapping = DictField(field=IntField())

        await Simple.adrop_collection()

        e = Simple()
        e.mapping["someint"] = 1
        await e.asave()

        # try creating an invalid mapping
        with pytest.raises(ValidationError):
            e.mapping["somestring"] = "abc"
            await e.asave()

    async def test_dictfield_complex(self):
        """Ensure that the dict field can handle the complex types."""

        class SettingBase(EmbeddedDocument):
            meta = {"allow_inheritance": True}

        class StringSetting(SettingBase):
            value = StringField()

        class IntegerSetting(SettingBase):
            value = IntField()

        class Simple(Document):
            mapping = DictField()

        await Simple.adrop_collection()

        e = Simple()
        e.mapping["somestring"] = StringSetting(value="foo")
        e.mapping["someint"] = IntegerSetting(value=42)
        e.mapping["nested_dict"] = {
            "number": 1,
            "string": "Hi!",
            "float": 1.001,
            "complex": IntegerSetting(value=42),
            "list": [IntegerSetting(value=42), StringSetting(value="foo")],
        }
        await e.asave()

        e2 = await Simple.aobjects.get(id=e.id)
        assert isinstance(e2.mapping["somestring"], StringSetting)
        assert isinstance(e2.mapping["someint"], IntegerSetting)

        # Test querying
        assert await Simple.aobjects.filter(mapping__someint__value=42).count() == 1
        assert await Simple.aobjects.filter(mapping__nested_dict__number=1).count() == 1
        assert (
            await Simple.aobjects.filter(
                mapping__nested_dict__complex__value=42
            ).count()
            == 1
        )
        assert (
            await Simple.aobjects.filter(
                mapping__nested_dict__list__0__value=42
            ).count()
            == 1
        )
        assert (
            await Simple.aobjects.filter(
                mapping__nested_dict__list__1__value="foo"
            ).count()
            == 1
        )

        # Confirm can update
        await Simple.aobjects().update(
            set__mapping={"someint": IntegerSetting(value=10)}
        )
        await Simple.aobjects().update(
            set__mapping__nested_dict__list__1=StringSetting(value="Boo")
        )
        assert (
            await Simple.aobjects.filter(
                mapping__nested_dict__list__1__value="foo"
            ).count()
            == 0
        )
        assert (
            await Simple.aobjects.filter(
                mapping__nested_dict__list__1__value="Boo"
            ).count()
            == 1
        )

    async def test_push_dict(self):
        class MyModel(Document):
            events = ListField(DictField())

        doc = await MyModel(events=[{"a": 1}]).asave()
        raw_doc = await async_get_as_pymongo(doc)
        expected_raw_doc = {"_id": doc.id, "events": [{"a": 1}]}
        assert raw_doc == expected_raw_doc

        await MyModel.aobjects(id=doc.id).update(push__events={})
        raw_doc = await async_get_as_pymongo(doc)
        expected_raw_doc = {"_id": doc.id, "events": [{"a": 1}, {}]}
        assert raw_doc == expected_raw_doc

    async def test_ensure_unique_default_instances(self):
        """Ensure that every field has it's own unique default instance."""

        class D(Document):
            data = DictField()
            data2 = DictField(default=lambda: {})

        d1 = D()
        d1.data["foo"] = "bar"
        d1.data2["foo"] = "bar"
        d2 = D()
        assert d2.data == {}
        assert d2.data2 == {}

    async def test_dict_field_invalid_dict_value(self):
        class DictFieldTest(Document):
            dictionary = DictField(required=True)

        await DictFieldTest.adrop_collection()

        test = DictFieldTest(dictionary=None)
        test.dictionary  # Just access to test getter
        with pytest.raises(ValidationError):
            test.validate()

        test = DictFieldTest(dictionary=False)
        test.dictionary  # Just access to test getter
        with pytest.raises(ValidationError):
            test.validate()

    async def test_dict_field_raises_validation_error_if_wrongly_assign_embedded_doc(
        self,
    ):
        class DictFieldTest(Document):
            dictionary = DictField(required=True)

        await DictFieldTest.adrop_collection()

        class Embedded(EmbeddedDocument):
            name = StringField()

        embed = Embedded(name="garbage")
        doc = DictFieldTest(dictionary=embed)
        with pytest.raises(ValidationError) as exc_info:
            doc.validate()

        error_msg = str(exc_info.value)
        assert "'dictionary'" in error_msg
        assert "Only dictionaries may be used in a DictField" in error_msg

    async def test_atomic_update_dict_field(self):
        """Ensure that the entire DictField can be atomically updated."""

        class Simple(Document):
            mapping = DictField(field=ListField(IntField(required=True)))

        await Simple.adrop_collection()

        e = Simple()
        e.mapping["someints"] = [1, 2]
        await e.asave()
        await e.aupdate(set__mapping={"ints": [3, 4]})
        await e.areload()
        assert isinstance(e.mapping, BaseDict)
        assert {"ints": [3, 4]} == e.mapping

        # try creating an invalid mapping
        with pytest.raises(ValueError):
            await e.aupdate(set__mapping={"somestrings": ["foo", "bar"]})

    async def test_dictfield_with_referencefield_complex_nesting_cases(self):
        """Ensure complex nesting inside DictField handles dereferencing of ReferenceField(dbref=True | False)"""

        # Relates to Issue #1453
        class Doc(Document):
            s = StringField()

        class Simple(Document):
            mapping0 = DictField(ReferenceField(Doc, dbref=True))
            mapping1 = DictField(ReferenceField(Doc, dbref=False))
            mapping2 = DictField(ListField(ReferenceField(Doc, dbref=True)))
            mapping3 = DictField(ListField(ReferenceField(Doc, dbref=False)))
            mapping4 = DictField(DictField(field=ReferenceField(Doc, dbref=True)))
            mapping5 = DictField(DictField(field=ReferenceField(Doc, dbref=False)))
            mapping6 = DictField(ListField(DictField(ReferenceField(Doc, dbref=True))))
            mapping7 = DictField(ListField(DictField(ReferenceField(Doc, dbref=False))))
            mapping8 = DictField(
                ListField(DictField(ListField(ReferenceField(Doc, dbref=True))))
            )
            mapping9 = DictField(
                ListField(DictField(ListField(ReferenceField(Doc, dbref=False))))
            )

        await Doc.adrop_collection()
        await Simple.adrop_collection()

        d = await Doc(s="aa").asave()
        e = Simple()
        e.mapping0["someint"] = e.mapping1["someint"] = d
        e.mapping2["someint"] = e.mapping3["someint"] = [d]
        e.mapping4["someint"] = e.mapping5["someint"] = {"d": d}
        e.mapping6["someint"] = e.mapping7["someint"] = [{"d": d}]
        e.mapping8["someint"] = e.mapping9["someint"] = [{"d": [d]}]
        await e.asave()

        s = await Simple.aobjects.select_related(
            "mapping0",
            "mapping1",
            "mapping2",
            "mapping3",
            "mapping4",
            "mapping5",
            "mapping6",
            "mapping7",
            "mapping8",
            "mapping9",
        ).first()
        assert isinstance(s.mapping0["someint"], Doc)
        assert isinstance(s.mapping1["someint"], Doc)
        assert isinstance(s.mapping2["someint"][0], Doc)
        assert isinstance(s.mapping3["someint"][0], Doc)
        assert isinstance(s.mapping4["someint"]["d"], Doc)
        assert isinstance(s.mapping5["someint"]["d"], Doc)
        assert isinstance(s.mapping6["someint"][0]["d"], Doc)
        assert isinstance(s.mapping7["someint"][0]["d"], Doc)
        assert isinstance(s.mapping8["someint"][0]["d"][0], Doc)
        assert isinstance(s.mapping9["someint"][0]["d"][0], Doc)
