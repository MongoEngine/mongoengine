import pickle

from mongoengine import *
from mongoengine import signals
from tests.fixtures import PickleEmbedded


class PickleSignalsTest(Document):
    number = IntField()
    string = StringField(choices=(("One", "1"), ("Two", "2")))
    embedded = EmbeddedDocumentField(PickleEmbedded)
    lists = ListField(StringField())

    @classmethod
    def post_save(self, sender, document, created, **kwargs):
        pickle.dumps(document)

    @classmethod
    def post_delete(self, sender, document, **kwargs):
        pickle.dumps(document)


signals.post_save.connect(PickleSignalsTest.post_save, sender=PickleSignalsTest)
signals.post_delete.connect(PickleSignalsTest.post_delete, sender=PickleSignalsTest)


class Mixin:
    name = StringField()


class Base(Document):
    meta = {"allow_inheritance": True}
