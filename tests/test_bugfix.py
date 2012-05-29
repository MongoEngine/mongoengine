# import pickle
# import pymongo
# import bson
# import warnings

# from datetime import datetime

# import tempfile
# import pymongo, gridfs

import unittest
from mongoengine import *
from bson.objectid import ObjectId

class BugFixTest(unittest.TestCase):


    def setUp(self):

        conn = connect(db='mongoenginetest')

    def test_items_list(self):

        class ActivityType1(EmbeddedDocument):
            activity_id = IntField()
            activity_name = StringField()

        class ActivityType2(EmbeddedDocument):
            activity_id = IntField()
            activity_status = StringField()

        class UserActivities(Document):
            user_id = IntField()
            activity = GenericEmbeddedDocumentField(choices=(ActivityType1, ActivityType2))


        UserActivities.drop_collection()

        user_id = 123
        activity_id = 321
        UserActivities(user_id=user_id, activity=ActivityType2(activity_id=activity_id, activity_status="A")).save()

        self.assertEquals(1, UserActivities.objects(user_id=user_id, __raw__={'activity.activity_status': 'A'}).count())




