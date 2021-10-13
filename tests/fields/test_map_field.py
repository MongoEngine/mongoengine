import datetime

import pytest

from mongoengine import *
from tests.utils import MongoDBTestCase


class TestMapField(MongoDBTestCase):
    def test_mapfield(self):
        """Ensure that the MapField handles the declared type."""

        class Simple(Document):
            mapping = MapField(IntField())

        Simple.drop_collection()

        e = Simple()
        e.mapping["someint"] = 1
        e.save()

        with pytest.raises(ValidationError):
            e.mapping["somestring"] = "abc"
            e.save()

        with pytest.raises(ValidationError):

            class NoDeclaredType(Document):
                mapping = MapField()

    def test_complex_mapfield(self):
        """Ensure that the MapField can handle complex declared types."""

        class SettingBase(EmbeddedDocument):
            meta = {"allow_inheritance": True}

        class StringSetting(SettingBase):
            value = StringField()

        class IntegerSetting(SettingBase):
            value = IntField()

        class Extensible(Document):
            mapping = MapField(EmbeddedDocumentField(SettingBase))

        Extensible.drop_collection()

        e = Extensible()
        e.mapping["somestring"] = StringSetting(value="foo")
        e.mapping["someint"] = IntegerSetting(value=42)
        e.save()

        e2 = Extensible.objects.get(id=e.id)
        assert isinstance(e2.mapping["somestring"], StringSetting)
        assert isinstance(e2.mapping["someint"], IntegerSetting)

        with pytest.raises(ValidationError):
            e.mapping["someint"] = 123
            e.save()

    def test_embedded_mapfield_db_field(self):
        class Embedded(EmbeddedDocument):
            number = IntField(default=0, db_field="i")

        class Test(Document):
            my_map = MapField(field=EmbeddedDocumentField(Embedded), db_field="x")

        Test.drop_collection()

        test = Test()
        test.my_map["DICTIONARY_KEY"] = Embedded(number=1)
        test.save()

        Test.objects.update_one(inc__my_map__DICTIONARY_KEY__number=1)

        test = Test.objects.get()
        assert test.my_map["DICTIONARY_KEY"].number == 2
        doc = self.db.test.find_one()
        assert doc["x"]["DICTIONARY_KEY"]["i"] == 2

    def test_mapfield_numerical_index(self):
        """Ensure that MapField accept numeric strings as indexes."""

        class Embedded(EmbeddedDocument):
            name = StringField()

        class Test(Document):
            my_map = MapField(EmbeddedDocumentField(Embedded))

        Test.drop_collection()

        test = Test()
        test.my_map["1"] = Embedded(name="test")
        test.save()
        test.my_map["1"].name = "test updated"
        test.save()

    def test_map_field_lookup(self):
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

        Log.drop_collection()
        Log(
            name="wilson",
            visited={"friends": datetime.datetime.now()},
            actions={"friends": Action(operation="drink", object="beer")},
        ).save()

        assert 1 == Log.objects(visited__friends__exists=True).count()

        assert (
            1
            == Log.objects(
                actions__friends__operation="drink", actions__friends__object="beer"
            ).count()
        )

    def test_map_field_unicode(self):
        class Info(EmbeddedDocument):
            description = StringField()
            value_list = ListField(field=StringField())

        class BlogPost(Document):
            info_dict = MapField(field=EmbeddedDocumentField(Info))

        BlogPost.drop_collection()

        tree = BlogPost(info_dict={"éééé": {"description": "VALUE: éééé"}})

        tree.save()

        assert (
            BlogPost.objects.get(id=tree.id).info_dict["éééé"].description
            == "VALUE: éééé"
        )
