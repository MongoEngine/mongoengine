import pickle
from datetime import datetime

from mongoengine import *
from mongoengine import signals


class PickleEmbedded(EmbeddedDocument):
    date = DateTimeField(default=datetime.now)


class PickleTest(Document):
    number = IntField()
    string = StringField(choices=(('One', '1'), ('Two', '2')))
    embedded = EmbeddedDocumentField(PickleEmbedded)
    lists = ListField(StringField())
    photo = FileField()


class NewDocumentPickleTest(Document):
    number = IntField()
    string = StringField(choices=(('One', '1'), ('Two', '2')))
    embedded = EmbeddedDocumentField(PickleEmbedded)
    lists = ListField(StringField())
    photo = FileField()
    new_field = StringField()


class PickleDyanmicEmbedded(DynamicEmbeddedDocument):
    date = DateTimeField(default=datetime.now)


class PickleDynamicTest(DynamicDocument):
    number = IntField()


class PickleSignalsTest(Document):
    number = IntField()
    string = StringField(choices=(('One', '1'), ('Two', '2')))
    embedded = EmbeddedDocumentField(PickleEmbedded)
    lists = ListField(StringField())

    @classmethod
    def post_save(self, sender, document, created, **kwargs):
        pickled = pickle.dumps(document)

    @classmethod
    def post_delete(self, sender, document, **kwargs):
        pickled = pickle.dumps(document)

signals.post_save.connect(PickleSignalsTest.post_save, sender=PickleSignalsTest)
signals.post_delete.connect(PickleSignalsTest.post_delete, sender=PickleSignalsTest)


class Mixin(object):
    name = StringField()


class Base(Document):
    meta = {'allow_inheritance': True}
