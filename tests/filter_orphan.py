from mongoengine import *
from mongoengine.connection import connect
import mongoengine.connection
import pymongo
import unittest
import warnings


class Doc(Document):
    i = IntField(db_field="i", required=True)
    f = IntField(db_field="f", required=True)
    version = FloatField(db_field="_v")


class DocNoVer(Document):
    i = IntField(db_field="i", required=True)
    f = IntField(db_field="f", required=True)


class OrphanFilterTest(unittest.TestCase):
    def setup(self):
        try:
            self.client = pymongo.mongo_client.MongoClient(host='10.10.101.140',
                                                      port=27081)
            self.s1 = pymongo.mongo_client.MongoClient(host='10.10.101.34',
                                                  port=27017)
            self.s2 = pymongo.mongo_client.MongoClient(host='10.10.101.188',
                                                  port=27017)
            connect(host='10.10.101.140', port=27081)
        except:
            warnings.warn(
                "This unit test requires a mongo testing cluster setup. Test"
                " is skipped because mongo testing cluster is not available.")
        return

    def test_version_not_enabled(self):
        self.setup()
        db_name, coll_name = 'orphan_db', 'docnover'
        self.client.drop_database(db_name)
        ns = '{}.{}'.format(db_name, coll_name)
        mongoengine.connection.set_default_db(db_name)
        self.client.admin.command('enableSharding', db_name)
        self.client.admin.command('shardCollection', ns, key={'i': 1})
        # Create 6 docs with no versions.
        for i in xrange(6):
            d = DocNoVer(i=i, f=i)
            d.save()
        # Split docs to two chunks equally.
        self.client.admin.command({'split': ns, 'find': {'i': 3}})
        # Store the two chunks on two shards.
        self.client.admin.command('moveChunk', ns, find={'i': 3}, to='s1')
        # create synthetic orphan
        coll = self.client[db_name][coll_name]
        doc = coll.find_one({'i': 3})
        doc['f'] = 10
        self.s2[db_name][coll_name].insert(doc)

        # testing find w/o version
        res1 = [x.to_mongo() for x in DocNoVer.find({})]
        res2 = [x.to_mongo() for x in DocNoVer.find({}, slave_ok=True)]
        # make sure read from "secondary" return orphan.
        assert len(res1) + 1 == len(res2)
        try:
            [x.to_mongo() for x in DocNoVer.find({},
                                                 slave_ok=True,
                                                 filter_orphan=True)]
        except Exception, e:
            assert e.message == 'Filter orphan cannot be performed when ' \
                                'versioning is not enabled.'

        # testing find_iter w/o version
        res1 = [x.to_mongo() for x in DocNoVer.find_iter({})]
        res2 = [x.to_mongo() for x in DocNoVer.find_iter({}, slave_ok=True)]
        # make sure read from "secondary" return orphan.
        assert len(res1) + 1 == len(res2)
        try:
            [x.to_mongo() for x in \
             DocNoVer.find_iter({}, slave_ok=True, filter_orphan=True)]
        except Exception, e:
            assert e.message == 'Filter orphan cannot be performed when ' \
                                'versioning is not enabled.'
        self.client.drop_database(db_name)

    def test_version_enabled_empty_collection(self):
        self.setup()
        db_name, coll_name = 'orphan_db', 'doc'
        self.client.drop_database(db_name)
        ns = '{}.{}'.format(db_name, coll_name)
        mongoengine.connection.set_default_db(db_name)
        self.client.admin.command('enableSharding', db_name)
        self.client.admin.command('shardCollection', ns, key={'i': 1})
        # Create 6 docs with no versions.
        for i in xrange(6):
            d = Doc(i=i, f=i)
            d.save()
        # Split docs to two chunks equally.
        self.client.admin.command({'split': ns, 'find': {'i': 3}})
        # Store the two chunks on two shards.
        self.client.admin.command('moveChunk', ns, find={'i': 3}, to='s1')
        # create synthetic orphan, the orphan has an older version.
        coll = self.client[db_name][coll_name]
        doc = coll.find_one({'i': 3})
        doc['f'] = 10
        doc['_v'] = 0
        self.s2[db_name][coll_name].insert(doc)

        # testing find w/ version
        res1 = [x.to_mongo() for x in Doc.find({})]
        res2 = [x.to_mongo() for x in Doc.find({}, slave_ok=True)]
        assert len(res1) + 1 == len(res2)
        res3 = [x.to_mongo() for x in Doc.find({}, slave_ok=True,
                                               filter_orphan=True)]
        assert len(res1) == len(res3)

        # testing find_iter w/ version, w/o sorting
        res1 = [x.to_mongo() for x in Doc.find_iter({})]
        res2 = [x.to_mongo() for x in Doc.find_iter({}, slave_ok=True)]
        assert len(res1) + 1 == len(res2)
        try:
            [x.to_mongo() for x in \
             Doc.find_iter({}, slave_ok=True, filter_orphan=True)]
        except Exception, e:
            assert e.message == "Filter orphan requires to sort by _id or id as the highest sorting preference."

        # testing find_iter w/ version w/ sorting w/o _id as highest sorting
        # preference
        res1 = [x.to_mongo() for x in \
                Doc.find_iter({}, sort=[('i', pymongo.DESCENDING)])]
        res2 = [x.to_mongo() for x in Doc.find_iter({}, slave_ok=True)]
        assert len(res1) + 1 == len(res2)
        try:
            [x.to_mongo() for x in \
             Doc.find_iter({}, sort=[('i', pymongo.DESCENDING)],
                           slave_ok=True, filter_orphan=True)]
        except Exception, e:
            assert e.message == "Filter orphan requires to sort by _id or id as the highest sorting preference."

        # testing find_iter w/ version w/ sorting w/ _id as only sorting key
        res1 = [x.to_mongo() for x in \
                Doc.find_iter({}, sort=[('_id', pymongo.DESCENDING)])]
        res2 = [x.to_mongo() for x in Doc.find_iter({}, slave_ok=True)]
        assert len(res1) + 1 == len(res2)
        res3 = [x.to_mongo() for x in \
                Doc.find_iter({}, sort=[('_id', pymongo.DESCENDING)],
                              slave_ok=True, filter_orphan=True)]
        assert len(res1) == len(res3)
        for i in xrange(len(res1)):
            assert res1[i] == res3[i]

        # testing find_iter w/ version w/ sorting w/ _id as the highest sorting
        # preference
        res1 = [x.to_mongo() for x in \
                Doc.find_iter({}, sort=[('_id', pymongo.DESCENDING),
                                        ('i', pymongo.ASCENDING)])]
        res2 = [x.to_mongo() for x in Doc.find_iter({}, slave_ok=True)]
        assert len(res1) + 1 == len(res2)
        res3 = [x.to_mongo() for x in \
                Doc.find_iter({}, sort=[('_id', pymongo.DESCENDING),
                                        ('i', pymongo.ASCENDING)],
                              slave_ok=True, filter_orphan=True)]
        assert len(res1) == len(res3)
        for i in xrange(len(res1)):
            assert res1[i] == res3[i]
        self.client.drop_database(db_name)

if __name__ == '__main__':
    unittest.main()
