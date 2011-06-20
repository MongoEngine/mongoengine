from datetime import datetime
import pymongo

from mongoengine import *
from mongoengine.base import BaseField
from mongoengine.connection import _get_db


class PickleEmbedded(EmbeddedDocument):
    date = DateTimeField(default=datetime.now)


class PickleTest(Document):
    number = IntField()
    string = StringField(choices=(('One', '1'), ('Two', '2')))
    embedded = EmbeddedDocumentField(PickleEmbedded)
    lists = ListField(StringField())


class Mixin(object):
    name = StringField()


class Base(Document):
    pass
