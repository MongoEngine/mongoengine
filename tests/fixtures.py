from datetime import datetime

from mongoengine import *


class PickleEmbedded(EmbeddedDocument):
    date = DateTimeField(default=datetime.now)


class PickleTest(Document):
    number = IntField()
    string = StringField(choices=(('One', '1'), ('Two', '2')))
    embedded = EmbeddedDocumentField(PickleEmbedded)
    lists = ListField(StringField())
    photo = FileField()


class Mixin(object):
    name = StringField()


class Base(Document):
    pass
