import datetime

import pytest

from mongoengine import *
from tests.asynchronous.utils import MongoDBAsyncTestCase


class TestMapField(MongoDBAsyncTestCase):
    async def test_mapfield(self):
        """Ensure that the MapField handles the declared type."""

        class Simple(Document):
            mapping = MapField(IntField())

        await Simple.adrop_collection()

        e = Simple()
        e.mapping["someint"] = 1
        await e.asave()

        with pytest.raises(ValidationError):
            e.mapping["somestring"] = "abc"
            await e.asave()

        with pytest.raises(ValidationError):

            class NoDeclaredType(Document):
                mapping = MapField()

    async def test_complex_mapfield(self):
        """Ensure that the MapField can handle complex declared types."""

        class SettingBase(EmbeddedDocument):
            meta = {"allow_inheritance": True}

        class StringSetting(SettingBase):
            value = StringField()

        class IntegerSetting(SettingBase):
            value = IntField()

        class Extensible(Document):
            mapping = MapField(EmbeddedDocumentField(SettingBase))

        await Extensible.adrop_collection()

        e = Extensible()
        e.mapping["somestring"] = StringSetting(value="foo")
        e.mapping["someint"] = IntegerSetting(value=42)
        await e.asave()

        e2 = await Extensible.aobjects.get(id=e.id)
        assert isinstance(e2.mapping["somestring"], StringSetting)
        assert isinstance(e2.mapping["someint"], IntegerSetting)

        with pytest.raises(ValidationError):
            e.mapping["someint"] = 123
            await e.asave()

    async def test_embedded_mapfield_db_field(self):
        class Embedded(EmbeddedDocument):
            number = IntField(default=0, db_field="i")

        class Test(Document):
            my_map = MapField(field=EmbeddedDocumentField(Embedded), db_field="x")

        await Test.adrop_collection()

        test = Test()
        test.my_map["DICTIONARY_KEY"] = Embedded(number=1)
        await test.asave()

        await Test.aobjects.update_one(inc__my_map__DICTIONARY_KEY__number=1)

        test = await Test.aobjects.get()
        assert test.my_map["DICTIONARY_KEY"].number == 2
        doc = await self.db.test.find_one()
        assert doc["x"]["DICTIONARY_KEY"]["i"] == 2

    async def test_mapfield_numerical_index(self):
        """Ensure that MapField accept numeric strings as indexes."""

        class Embedded(EmbeddedDocument):
            name = StringField()

        class Test(Document):
            my_map = MapField(EmbeddedDocumentField(Embedded))

        await Test.adrop_collection()

        test = Test()
        test.my_map["1"] = Embedded(name="test")
        await test.asave()
        test.my_map["1"].name = "test updated"
        await test.asave()

    async def test_map_field_lookup(self):
        """Ensure MapField lookups succeed on Fields without a lookup
        method.
        """

        class Action(EmbeddedDocument):
            operation = StringField()
            object = StringField()

        class Log(Document):
            name = StringField()
            visited = MapField(DateTimeField())
            actions = MapField(EmbeddedDocumentField(Action))

        await Log.adrop_collection()
        await Log(
            name="wilson",
            visited={"friends": datetime.datetime.now()},
            actions={"friends": Action(operation="drink", object="beer")},
        ).asave()

        assert 1 == await Log.aobjects(visited__friends__exists=True).count()

        assert (
            1
            == await Log.aobjects(
                actions__friends__operation="drink", actions__friends__object="beer"
            ).count()
        )

    async def test_map_field_unicode(self):
        class Info(EmbeddedDocument):
            description = StringField()
            value_list = ListField(field=StringField())

        class BlogPost(Document):
            info_dict = MapField(field=EmbeddedDocumentField(Info))

        await BlogPost.adrop_collection()

        tree = BlogPost(info_dict={"éééé": {"description": "VALUE: éééé"}})

        await tree.asave()

        assert (
                (await BlogPost.aobjects.get(id=tree.id)).info_dict["éééé"].description
            == "VALUE: éééé"
        )
