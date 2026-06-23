from datetime import datetime

try:
    # Python 3.11+
    from datetime import UTC
except ImportError:
    # Python ≤ 3.10
    from datetime import timezone

    UTC = timezone.utc

from mongoengine import *


class PickleEmbedded(EmbeddedDocument):
    date = DateTimeField(default=datetime.now)


class PickleTest(Document):
    number = IntField()
    string = StringField(choices=(("One", "1"), ("Two", "2")))
    embedded = EmbeddedDocumentField(PickleEmbedded)
    lists = ListField(StringField())
    photo = FileField()


class NewDocumentPickleTest(Document):
    number = IntField()
    string = StringField(choices=(("One", "1"), ("Two", "2")))
    embedded = EmbeddedDocumentField(PickleEmbedded)
    lists = ListField(StringField())
    photo = FileField()
    new_field = StringField()


class PickleDynamicEmbedded(DynamicEmbeddedDocument):
    date = DateTimeField(default=datetime.now(UTC))


class PickleDynamicTest(DynamicDocument):
    number = IntField()


class Mixin:
    name = StringField()


class Base(Document):
    meta = {"allow_inheritance": True}
