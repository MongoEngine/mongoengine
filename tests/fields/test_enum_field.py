# -*- coding: utf-8 -*-
from enum import Enum

import pytest

from mongoengine import *
from tests.utils import MongoDBTestCase, get_as_pymongo


class Status(Enum):
    NEW = 'new'
    DONE = 'done'
    
    
class ModelWithEnum(Document):
    status = EnumField(Status)


class Color(Enum):
    RED = 1
    BLUE = 2


class ModelWithColor(Document):
    color = EnumField(Color, default=Color.RED)


class TestEnumField(MongoDBTestCase):
    def test_storage(self):
        model = ModelWithEnum(status=Status.NEW).save()
        assert get_as_pymongo(model) == {"_id": model.id, "status": 'new'}

    def test_set_enum(self):
        ModelWithEnum.drop_collection()
        m = ModelWithEnum(status=Status.NEW).save()
        assert ModelWithEnum.objects(status=Status.NEW).count() == 1
        assert ModelWithEnum.objects.first().status == Status.NEW
        m.validate()

    def test_set_by_value(self):
        ModelWithEnum.drop_collection()
        ModelWithEnum(status='new').save()
        assert ModelWithEnum.objects.first().status == Status.NEW

    def test_filter(self):
        ModelWithEnum.drop_collection()
        ModelWithEnum(status='new').save()
        assert ModelWithEnum.objects(status='new').count() == 1
        assert ModelWithEnum.objects(status=Status.NEW).count() == 1
        assert ModelWithEnum.objects(status=Status.DONE).count() == 0

    def test_change_value(self):
        m = ModelWithEnum(status='new')
        m.status = Status.DONE
        m.validate()
        assert m.status == Status.DONE

    def test_set_default(self):
        class ModelWithDefault(Document):
            status = EnumField(Status, default=Status.DONE)

        m = ModelWithDefault()
        m.validate()
        m.save()
        assert m.status == Status.DONE

    def test_enum_with_int(self):
        m = ModelWithColor()
        m.validate()
        m.save()
        assert m.color == Color.RED
        assert ModelWithColor.objects(color=Color.RED).count() == 1
        assert ModelWithColor.objects(color=1).count() == 1
        assert ModelWithColor.objects(color=2).count() == 0

    def test_storage_enum_with_int(self):
        model = ModelWithColor(color=Color.BLUE).save()
        assert get_as_pymongo(model) == {"_id": model.id, "color": "2"}

    def test_enum_field_can_be_empty(self):
        m = ModelWithEnum()
        m.validate()
        m.save()
        assert m.status is None
        assert ModelWithEnum.objects()[0].status is None
        assert ModelWithEnum.objects(status=None).count() == 1

    def test_cannot_create_model_with_wrong_enum_value(self):
        with pytest.raises(ValueError):
            ModelWithEnum(status='wrong_one')

    def test_cannot_create_model_with_wrong_enum_type(self):
        with pytest.raises(ValueError):
            ModelWithColor(color='wrong_type')

    def test_cannot_create_model_with_wrong_enum_value_int(self):
        with pytest.raises(ValueError):
            ModelWithColor(color=3)

    def test_cannot_set_wrong_enum_value(self):
        m = ModelWithEnum(status='new')
        with pytest.raises(ValueError):
            m.status = 'wrong'
