import pymongo
import bson
import unittest
import mock
import cPickle

from datetime import datetime

from mongoengine import *
from mongoengine.base import _document_registry
from mongoengine.connection import _get_db, connect
import mongoengine.connection
from mock import MagicMock, Mock, call

import pymongo
mongoengine.connection.set_default_db("test")


class User(Document):
    email = StringField(db_field="e", required=True)
    first_name = StringField(db_field="fn", max_length=50)
    last_name = StringField(db_field="ln", max_length=50)
    value = FloatField(db_field="v", required=True, default=10)
    version = FloatField(db_field="_v")


class DocVersioningTest(unittest.TestCase):
    def setUp(self):
        connect()
        self.db = _get_db()

    def test_save(self):
        User.drop_collection()
        u = User(email='abc.def@gmail.com', first_name='abc',
                 last_name='def', value=10)
        u.save()
        u = [u for u in User.objects][0]
        # test basic insert
        assert u.version == 1
        u.email = 'abc.def2@gmail.com'
        u.save()
        u = [u for u in User.objects][0]
        # test upsert save
        assert u.version == 2
        User.drop_collection()
        print('save() passed.')

    def test_update(self):
        User.drop_collection()
        u = User(email='abc.def@gmail.com', first_name='abc', last_name='def')
        u.save()
        u = [u for u in User.objects][0]
        assert u.version == 1
        User.update({'_id': u.id}, {'$set': {'email': 'abc.def2@gmail.com'},
                                    '$inc': {'value': 10}}, multi=False)
        u = [u for u in User.objects][0]
        assert u.email == 'abc.def2@gmail.com' and u.version == 2 and \
            u.value == 20
        User.update({'_id': u.id}, {'$set': {'first_name': 'foobar'}},
                    multi=False)
        u = [u for u in User.objects][0]
        assert u.first_name == 'foobar' and u.version == 3
        User.drop_collection()
        print('update() passed.')

    def test_update_one(self):
        User.drop_collection()
        u = User(email='abc.def@gmail.com', first_name='abc', last_name='def')
        u.save()
        u = [u for u in User.objects][0]
        assert u.version == 1
        u.update_one({'$set': {'email': 'abc.def2@gmail.com'}})
        u = [u for u in User.objects][0]
        assert u.version == 2 and u.email == 'abc.def2@gmail.com'
        u.update_one({'$inc': {'value': 5}})
        u = [u for u in User.objects][0]
        assert u.version == 3 and u.value == 15
        User.drop_collection()
        print('update_one() passed.')


    def test_find_and_modify(self):
        User.drop_collection()
        u = User(email='abc.def@gmail.com', first_name='abc', last_name='def')
        u.save()
        u = [u for u in User.objects][0]
        User.find_and_modify({'id': u.id},
                             {'$set': {'email': 'abc.def2@gmail.com'},
                              '$inc': {'value': 20}})
        u = [u for u in User.objects][0]
        assert u.version == 2 and u.email == 'abc.def2@gmail.com' and \
            u.value == 30
        User.drop_collection()
        print('find_and_modify() passed.')


if __name__ == '__main__':
    unittest.main()