import unittest

from pymongo import (ReadPreference,
                     read_preferences)

from mongoengine.python_support import IS_PYMONGO_3

if IS_PYMONGO_3:
    from pymongo import MongoClient
    CONN_CLASS = MongoClient
    READ_PREF = ReadPreference.SECONDARY
else:
    from pymongo import ReplicaSetConnection
    CONN_CLASS = ReplicaSetConnection
    READ_PREF = ReadPreference.SECONDARY_ONLY

import pymongo
import mongoengine
from mongoengine import *
from mongoengine.connection import (MongoEngineConnectionError,
                                    get_db)


class ConnectionTest(unittest.TestCase):

    def setUp(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def tearDown(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def test_replicaset_uri_passes_read_preference(self):
        """Requires a replica set called "rs" on port 27017
        """

        try:
            conn = connect(db='mongoenginetest',
                           host="mongodb://localhost/mongoenginetest?replicaSet=local-rs",
                           read_preference=READ_PREF)
        except MongoEngineConnectionError as e:
            return

        if not isinstance(conn, CONN_CLASS):
            # really???
            return

        self.assertEqual(conn.read_preference, READ_PREF)

    def test_connect_with_replicaset_via_kwargs(self):
        """Ensure connect() works when specifying a replicaSet via the
        connection kwargs
        """
        if IS_PYMONGO_3:
            c = connect(replicaset='local-rs')
            self.assertEqual(c._MongoClient__options.replica_set_name,
                             'local-rs')
            db = get_db()
            self.assertTrue(isinstance(db, pymongo.database.Database))
            self.assertEqual(db.name, 'test')
        else:
            # PyMongo < v3.x raises an exception:
            # "localhost:27017 is not a member of replica set local-rs"
            with self.assertRaises(MongoEngineConnectionError):
                c = connect(replicaset='local-rs')

    def test_connect_with_replicaset_via_uri(self):
        """Ensure connect() works when specifying a replicaSet via the
        MongoDB URI.
        """
        if IS_PYMONGO_3:
            c = connect(host='mongodb://localhost/test?replicaSet=local-rs')
            db = get_db()
            self.assertTrue(isinstance(db, pymongo.database.Database))
            self.assertEqual(db.name, 'test')
        else:
            # PyMongo < v3.x raises an exception:
            # "localhost:27017 is not a member of replica set local-rs"
            with self.assertRaises(MongoEngineConnectionError):
                c = connect(host='mongodb://localhost/test?replicaSet=local-rs')

    def test_read_preference_from_replica_set_in_uri_as_host(self):
        '''read preference from replica set cluster'''
        #test case about uri option overrides kwargs and read_preference with tag sets
        if IS_PYMONGO_3:
            conn_obj = connect(
                db='testrjx',
                host="mongodb://localhost:27017/?replicaset=dev_rs" +
                     "&readpreference=nearest&readpreferencetags=",
                read_preference=ReadPreference.SECONDARY
            )
            read_preference_obj = conn_obj.read_preference
            self.assertEqual(read_preference_obj.mode, ReadPreference.NEAREST.mode)
            self.assertEqual(read_preference_obj.tag_sets, [{}])
        else:
            if pymongo.version_tuple[1] < 9: # like v2.8
                with self.assertRaises(MongoEngineConnectionError):
                    c_obj = connect(
                        db='testrjx',
                        host="mongodb://localhost:27018/?replicaset=dev_rs"
                        + "&read_preference=nearest&readpreferencetags=dc:east,use:dev",
                        read_preference=ReadPreference.SECONDARY,
                        readpreferencetags=[{}] #tag_sets as an alternative
                    )
                    self.assertEqual(c_obj.read_preference, ReadPreference.NEAREST)
                    self.assertEqual(c_obj.tag_sets, [{'dc':'east','use':'dev'}])
            else:
                #for 2.9 as transition version, also accept read_preference object subclassing _ServerMode
                with self.assertRaises(MongoEngineConnectionError):
                    c_obj = connect(
                        db='testrjx',
                        host="mongodb://localhost:27018/?replicaset=dev_rs",
                        read_preference=read_preferences.SecondaryPreferred(
                            [{'dc':'east','use':'dev'}]
                        ),
                    )
                    self.assertEqual(c_obj.read_preference, ReadPreference.SECONDARY_PREFERRED)
                    self.assertEqual(c_obj.tag_sets, [{'dc':'east','use':'dev'}])

    def test_replica_set_as_uri_or_kwargs(self):
        '''replicaset as uri option or kwargs'''
        # dev_rs as real replica set name, while test_rs is false as comparision
        # replicaset case insensitive
        if not IS_PYMONGO_3: #like 2.9 and 2.8
            # uri opt overrides kwargs for connecting replica set
            with self.assertRaises(MongoEngineConnectionError):
                c_obj = connect(
                    db='testrjx',
                    host="mongodb://localhost:27018/?replicaset=dev_rs",
                    replicaset='test_rs'
                )
                self.assertEqual(c_obj._MongoReplicaSetClient__name, 'dev_rs')
        else: #like 3.3
            #here, kwargs overrides uri option
            #though wrong replica set to be connected, it is ok until issuing real data operation
            c_obj = connect(
                db='testrjx',
                host="mongodb://localhost:27017/?replicaSet=dev_rs",
                replicaSet='test_rs'
            )
            self.assertEqual(c_obj._MongoClient__options.replica_set_name, 'test_rs')

if __name__ == '__main__':
    unittest.main()
