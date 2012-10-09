import pymongo
import unittest
import warnings

from mongoengine import *
from mongoengine.connection import _get_db

from bson import ObjectId, DBRef

class CustomQueryTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = _get_db()

        class Person(Document):
            meta = {'allow_inheritance': False}
            name = StringField()
            age = IntField(db_field='a')
            favourite_colour = EmbeddedDocumentField("Colour", db_field="c")
            other_colours = ListField(EmbeddedDocumentField("Colour"), db_field="o")
            number_list = ListField(IntField())
            arbitrary_dict = DictField()
            some_id = ObjectIdField()
            id_list = ListField(ObjectIdField())
            user = ReferenceField("User")
            friends = ListField(ReferenceField("Person"))

        class Colour(EmbeddedDocument):
            meta = {'allow_inheritance': False}
            name = StringField(db_field='n')

            def __str__(self):
                return self.name

            def __eq__(self, other):
                return isinstance(other, Colour) and self.name == other.name

        class User(Document):
            meta = {'allow_inheritance': False}
            id = ObjectIdField(primary_key=True)
            name = StringField()

        class Vehicle(Document):
            name = StringField()

        class Bike(Vehicle):
            has_bell = BooleanField()

        class Scooter(Vehicle):
            cc = IntField()

        class Shard(Document):
            hash = IntField(db_field='h')
            name = StringField()

            def _update_one_key(self):
                return {'_id': self.id, 'hash': 12}

        Person._pymongo().drop()
        User._pymongo().drop()
        Vehicle._pymongo().drop()
        Shard._pymongo().drop()

        self.Person = Person
        self.Colour = Colour
        self.User = User
        self.Vehicle = Vehicle
        self.Bike = Bike
        self.Scooter = Scooter
        self.Shard = Shard

    def tearDown(self):
        self.Person.objects.delete()

    def testInc(self):
        p = self.Person(name="Adam", age=12)
        p.save()

        self.assertEqual(p.name, "Adam")

        p.inc(age=1)
        self.assertEqual(p.age, 13)
        self.assertEqual(p.name, "Adam")
        p.reload()
        self.assertEqual(p.age, 13)
        self.assertEqual(p.name, "Adam")

        p.inc(age=-3)
        self.assertEqual(p.age, 10)
        p.reload()
        self.assertEqual(p.age, 10)

        p.update_one({'$inc': {'age': 7}})
        self.assertEqual(p.name, "Adam")
        self.assertEqual(p.age, 17)
        p.reload()
        self.assertEqual(p.name, "Adam")
        self.assertEqual(p.age, 17)

    def testSet(self):
        p = self.Person(name="Adam", age=12)
        p.save()

        self.assertEqual(p.name, "Adam")
        p.set(age=15)
        self.assertEqual(p.name, "Adam")
        self.assertEqual(p.age, 15)
        p.reload()
        self.assertEqual(p.age, 15)
        self.assertEqual(p.name, "Adam")

        p.set(age=19, name="Josh")
        self.assertEqual(p.age, 19)
        self.assertEqual(p.name, "Josh")
        p.reload()
        self.assertEqual(p.age, 19)
        self.assertEqual(p.name, "Josh")

    def testAddToSet(self):
        blue = self.Colour(name="Blue")
        red = self.Colour(name="Red")
        yellow = self.Colour(name="Yellow")
        p = self.Person(name="Adam", other_colours=[blue, yellow])
        p.save()

        self.assertEqual([c.name for c in p.other_colours], ['Blue', 'Yellow'])

        p.add_to_set(other_colours=blue)
        self.assertEqual([c.name for c in p.other_colours], ['Blue', 'Yellow'])
        p.reload()
        self.assertEqual([c.name for c in p.other_colours], ['Blue', 'Yellow'])

        p.add_to_set(other_colours=red)
        self.assertEqual([c.name for c in p.other_colours], ['Blue', 'Yellow', 'Red'])
        p.reload()
        self.assertEqual([c.name for c in p.other_colours], ['Blue', 'Yellow', 'Red'])

    def testSetEmbedded(self):
        blue = self.Colour(name="Blue")
        p = self.Person(name="Adam", age=12)
        p.save()

        self.assertEqual(p.favourite_colour, None)
        p.set(favourite_colour=blue)
        self.assertEqual(p.favourite_colour.name, blue.name)
        self.assertTrue(isinstance(p.favourite_colour, self.Colour))
        p.reload()
        self.assertEqual(p.favourite_colour.name, "Blue")

    def testInvalidField(self):
        p = self.Person(name="Adam", age=12)
        p.save()

        self.assertRaises(ValueError, p.set, doesntexist='foobar')

    def testTransformQuery(self):
        query = {'$set': {'name': 'Chu', 'age': 20}}
        result = self.Person._transform_value(query, self.Person)

        self.assertEqual(result, {'$set': {'name': 'Chu', 'a': 20}})

    def testTransformQueryEmbedded(self):
        blue = self.Colour(name='Blue')
        query = {'$set': {'name': 'Chu', 'age': 20, 'favourite_colour': blue}}
        result = self.Person._transform_value(query, self.Person)

        self.assertEqual(set(result['$set'].keys()), set(['name', 'a', 'c']))

        self.assertEqual(result['$set']['name'], 'Chu')
        self.assertEqual(result['$set']['c'], {'n': 'Blue'})

    def testTransformQueryList(self):
        blue = self.Colour(name='Blue')
        red = self.Colour(name='Red')
        query = {'$set': {'name': 'Chu', 'age': 20, 'other_colours': [red, blue]}}
        result = self.Person._transform_value(query, self.Person)

        self.assertEqual(set(result['$set'].keys()), set(['name', 'a', 'o']))

        self.assertEqual(result['$set']['name'], 'Chu')
        self.assertEqual(result['$set']['o'], [{'n': 'Red'}, {'n': 'Blue'}])

    def testTransformQueryListIndex(self):
        blue = self.Colour(name='Blue')
        query = {'$set': {'other_colours.1': blue}}
        result = self.Person._transform_value(query, self.Person)

        self.assertEqual(result, {'$set': {'o.1': {'n': 'Blue'}}})

        query = {'$set': {'other_colours.1.name': 'Red'}}
        result = self.Person._transform_value(query, self.Person)

        self.assertEqual(result, {'$set': {'o.1.n': 'Red'}})

    def testValidateQuery(self):
        p = self.Person(name="Adam", age=12)
        p.save()

        self.assertRaises(ValidationError, p.set, age="hello")

    def testDictEncoding(self):
        p = self.Person(name="Adam", age=21)
        p.save()
        val = {'x': 4, 'c': self.Colour(name="Blue")}
        p.set(arbitrary_dict=val)

        p.reload()
        self.assertEqual(p.arbitrary_dict, val)

        query = {'$set': {'arbitrary_dict.x': 42,
                          'age': 12,
                          'arbitrary_dict.c.n': 'Red'}}
        transformed = self.Person._transform_value(query, self.Person)

        self.assertEqual(transformed['$set']['arbitrary_dict.x'], 42)
        self.assertEqual(transformed['$set']['arbitrary_dict.c.n'], 'Red')
        self.assertEqual(transformed['$set']['a'], 12)
        self.assertEqual(len(transformed), 1)
        self.assertEqual(len(transformed['$set']), 3)

        p.update_one(query)
        self.assertEqual(p.age, 12)
        self.assertEqual(p.arbitrary_dict['x'], 4)
        self.assertEqual(p.arbitrary_dict['c']['n'], "Blue")

        p.reload()
        self.assertEqual(p.age, 12)
        self.assertEqual(p.arbitrary_dict['x'], 42)
        self.assertEqual(p.arbitrary_dict['c']['n'], "Red")

    def testObjectIdEncoding(self):
        p = self.Person(some_id='0' * 24)
        p.save()

        self.assertEqual(p.find({'some_id': '0' * 24}), [p])

        p.reload()

        self.assertEqual(p.some_id, ObjectId('0' * 24))

    def testDocumentIds(self):
        user = self.User(id=ObjectId())
        user.save()
        uid = user.id

        self.assertEqual(user.find({'_id': uid}), [user])
        self.assertEqual(user.find_one({'_id': uid}), user)

        p = self.Person()
        p.save()

        self.assertEqual(p.find({'_id': p.id}), [p])

    def testSort(self):
        p1 = self.Person(age=10)
        p1.save()
        p2 = self.Person(age=15)
        p2.save()
        p3 = self.Person(age=20)
        p3.save()

        self.assertEquals(self.Person.find({}, sort=[('age', 1)]), [p1, p2, p3])
        self.assertEquals(self.Person.find({}, sort=[('age', -1)]), [p3, p2, p1])
        self.assertRaises(TypeError, self.Person.find, {}, sort=[('age', 0)])
        self.assertRaises(ValueError, self.Person.find, {}, sort=['age'])
        self.assertRaises(ValueError, self.Person.find, {}, sort='age')

    def testIncludeFields(self):
        p = self.Person(age=10, name="Adam")
        p.save()

        p1 = self.Person.find_one({'age': 10})
        self.assertEquals(p1, p)

        p1 = self.Person.find_one({'age': 10}, fields=['age'])
        self.assertEquals(p1.age, 10)
        self.assertEquals(p1.name, None)

        p1 = self.Person.find_one({'age': 10}, fields=['age', 'name'])
        self.assertEquals(p1, p)

        p1 = self.Person.find_one({'age': 10}, fields={'age': 0})
        self.assertEquals(p1.age, None)
        self.assertEquals(p1.name, 'Adam')

        p1 = self.Person.find_one({'age': 10}, fields={'name': 0})
        self.assertEquals(p1.age, 10)
        self.assertEquals(p1.name, None)

        p1 = self.Person.find_one({'age': 10}, fields={'age': 1})
        self.assertEquals(p1.age, 10)
        self.assertEquals(p1.name, None)

    def testQueryIn(self):
        query = {'_id': {'$in': [ObjectId(), ObjectId(), ObjectId()] } }
        self.assertEquals(self.User._transform_value(query, self.User), query)

    def testAutoObjId(self):
        obj_ids = [ObjectId(), ObjectId(), ObjectId()]
        in_query = {'_id': {'$in': [str(o) for o in obj_ids]}}

        out_query = self.User._transform_value(in_query, self.User)

        self.assertEquals(out_query['_id']['$in'], obj_ids)

        u1 = self.User(id=ObjectId())
        u1.save()
        u2 = self.User(id=ObjectId())
        u2.save()

        users = self.User.find({'_id': {'$in': [str(u1.id), str(u2.id)]}})

        self.assertEquals(users, [u1, u2])

    def testOnlyId(self):
        p = self.Person(name="Adam")
        p.save()

        p1 = self.Person.find_one({'name': 'Adam'}, fields=['id'])
        self.assertEquals(p1.name, None)
        self.assertEquals(p1.id, p.id)

        p2 = self.Person.find_one({'name': 'Adam'}, fields=['_id'])
        self.assertEquals(p2.name, None)
        self.assertEquals(p2.id, p.id)

        # test where ID is defined explicitly
        u = self.User(name="Adam", id=ObjectId())
        u.save()

        u2 = self.User.find_one({'name': 'Adam'}, fields=['_id'])
        self.assertEquals(u2.name, None)
        self.assertEquals(u2.id, u.id)

        u2 = self.User.find_one({'name': 'Adam'}, fields=['id'])
        self.assertEquals(u2.name, None)
        self.assertEquals(u2.id, u.id)

    def testInheritanceBaseClass(self):
        v = self.Vehicle(name="Honda")
        b = self.Bike(name="Fixie")
        s = self.Scooter(name="Zoom", cc=110)
        v.save()
        b.save()
        s.save()

        self.assertEquals(self.Vehicle.count({}), 3)
        self.assertEquals(self.Vehicle.count({'name': "Fixie"}), 1)
        self.assertEquals(self.Vehicle.count({'name': "Honda"}), 1)
        ret = self.Scooter.update({'name': "Zoom"}, {'$set': {'cc': 400}})
        self.assertEquals(ret['n'], 1)
        s2 = self.Vehicle.find_one({'name': "Zoom"})
        self.assertEquals(s2.cc, 400)

    def testInheritanceSubClass(self):
        v = self.Vehicle(name="Honda")
        b = self.Bike(name="Fixie")
        b2 = self.Bike(name="Honda")
        s = self.Scooter(name="Zoom", cc=110)
        v.save()
        b.save()
        b2.save()
        s.save()

        self.assertEquals(self.Vehicle.count({}), 4)
        self.assertEquals(self.Bike.count({}), 2)
        self.assertEquals(self.Scooter.count({}), 1)
        self.assertEquals(self.Vehicle.count({'name': "Fixie"}), 1)
        self.assertEquals(self.Bike.count({'name': "Fixie"}), 1)
        self.assertEquals(self.Vehicle.count({'name': "Honda"}), 2)
        self.assertEquals(self.Bike.count({'name': "Honda"}), 1)

        ret = self.Bike.update({'name': "Honda"}, {'$set': {'name': 'Sarengetti'}})
        self.assertEquals(ret['n'], 1)
        self.assertEquals(self.Vehicle.count({'name': "Honda"}), 1)
        self.assertEquals(self.Vehicle.count({'name': "Sarengetti"}), 1)
        self.assertEquals(self.Bike.count({'name': "Honda"}), 0)
        self.assertEquals(self.Bike.count({'name': "Sarengetti"}), 1)

    def testShardKey(self):
        s1 = self.Shard(hash=14, name="s1")
        s2 = self.Shard(hash=12, name="s2")
        s1.save()
        s2.save()

        # hacked the shard key lookup to add hash=12 to spec
        ret = s1.update_one({'$set': {'name': 'changed'}})
        self.assertEquals(ret['n'], 0)

        ret = s2.update_one({'$set': {'name': 'changed'}})
        self.assertEquals(ret['n'], 1)

    def testFindIter(self):
        people = [self.Person(age=a) for a in [10, 15, 20]]
        for p in people:
            p.save()

        for i, p in enumerate(self.Person.find_iter({}, sort=[('age', 1)])):
            self.assertEquals(p, people[i])

        for i, p in enumerate(self.Person.find_iter({'age': {'$lte': 15}}, sort=[('age', 1)])):
            self.assertEquals(p, people[i])

    def testMultiUpdate(self):
        people = [self.Person(age=a) for a in [10, 15, 20]]
        for p in people:
            p.save()

        self.assertEquals(self.Person.count({'age': {'$lte': 15}}), 2)

        ret = self.Person.update({'age': {'$lte': 15}}, {'$set': {'name': 'Adam'}}, multi=False)
        self.assertEquals(ret['n'], 1)

        ret = self.Person.update({'age': {'$lte': 15}}, {'$set': {'name': 'Adam'}})
        self.assertEquals(ret['n'], 2)

        for p in people:
            p.reload()
            self.assertEquals(p.name, "Adam" if p.age <= 15 else None)

    def testCount(self):
        people = [self.Person(age=a) for a in [10, 15, 20]]
        for p in people:
            p.save()

        self.assertEquals(self.Person.count({'age': {'$lte': 15}}), 2)
        self.assertEquals(self.Person.count({'name': 'Adam'}), 0)
        self.assertEquals(self.Person.count({'age': {'$lte': 35}}), 3)
        self.assertEquals(self.Person.count({'age': {'$lte': 0}}), 0)
        self.assertEquals(self.Person.count({'age': 10}), 1)

    def testReferences(self):
        user = self.User(id=ObjectId(), name="Adam A Flynn")
        user.save()
        person = self.Person(user=user, name="Adam")
        person.save()

        spec = self.Person._transform_value({'user': user}, self.Person)
        self.assertEquals(spec, {'user': DBRef('user', user.id)})

        p = self.Person.find_one({'user': user})
        self.assertEquals(p.user.id, user.id)
        self.assertEquals(p.user.name, "Adam A Flynn")
        self.assertEquals(p.name, "Adam")

    def testReferenceList(self):
        adam = self.Person(name="Adam")
        adam.save()
        josh = self.Person(name="Josh")
        josh.save()
        chu = self.Person(name="Chu")
        chu.save()
        danny = self.Person(name="Danny", friends=[adam, josh])
        danny.save()

        q = self.Person._transform_value({'friends': chu}, self.Person)
        self.assertEquals(q['friends'], DBRef("person", chu.id))

        q = self.Person._transform_value({'friends': DBRef("person", chu.id)}, self.Person)
        self.assertEquals(q['friends'], DBRef("person", chu.id))

        q = self.Person._transform_value({'friends': {'$in': (DBRef("person", chu.id), adam)}}, self.Person)
        self.assertEquals(len(q['friends']['$in']), 2)
        self.assertTrue(DBRef("person", chu.id) in q['friends']['$in'])
        self.assertTrue(DBRef("person", adam.id) in q['friends']['$in'])

        # make sure tuple & list versions behave the same way
        q2 = self.Person._transform_value({'friends': {'$in': [DBRef("person", chu.id), adam]}}, self.Person)
        self.assertEquals(q2, q)

        self.assertEquals(self.Person.count({'friends': chu}), 0)
        self.assertEquals(self.Person.count({'friends': adam}), 1)
        self.assertEquals(self.Person.count({'friends': josh}), 1)
        self.assertEquals(self.Person.count({'friends': {'$ne': chu}}), 4)
        self.assertEquals(self.Person.count({'friends': {'$ne': adam}}), 3)
        self.assertEquals(self.Person.count({'friends': {'$ne': josh}}), 3)
        danny.add_to_set(friends=chu)
        self.assertEquals(self.Person.count({'friends': chu}), 1)
        self.assertEquals(self.Person.count({'friends': adam}), 1)
        self.assertEquals(self.Person.count({'friends': josh}), 1)

        danny.set(friends=[adam])
        self.assertEquals(self.Person.count({'friends': josh}), 0)
        self.assertEquals(self.Person.count({'friends': chu}), 0)
        self.assertEquals(self.Person.count({'friends': adam}), 1)

        danny.push(friends=DBRef("person", chu.id))
        self.assertEquals(self.Person.count({'friends': josh}), 0)
        self.assertEquals(self.Person.count({'friends': chu}), 1)
        self.assertEquals(self.Person.count({'friends': adam}), 1)

        danny.update_one({'$pull': {'friends': josh}})
        self.assertEquals(self.Person.count({'friends': josh}), 0)
        self.assertEquals(self.Person.count({'friends': chu}), 1)
        self.assertEquals(self.Person.count({'friends': adam}), 1)

        danny.update_one({'$pull': {'friends': DBRef("person", chu.id)}})
        self.assertEquals(self.Person.count({'friends': josh}), 0)
        self.assertEquals(self.Person.count({'friends': chu}), 0)
        self.assertEquals(self.Person.count({'friends': adam}), 1)

        danny.set(friends=[DBRef("person", adam.id), josh])
        self.assertEquals(self.Person.count({'friends': josh}), 1)
        self.assertEquals(self.Person.count({'friends': chu}), 0)
        self.assertEquals(self.Person.count({'friends': adam}), 1)

        d = self.Person.find_one({'_id': danny.id})
        self.assertEquals(d.friends, [adam, josh])

    def testIdToString(self):
        u = self.Person(id=ObjectId(), some_id=ObjectId())
        u.save()

        self.assertEquals(self.Person.find_one({'_id': u.id}), u)
        self.assertEquals(self.Person.find_one({'_id': str(u.id)}), u)
        self.assertEquals(self.Person.find_one({'_id': ObjectId()}), None)

        new_id = ObjectId()
        ret = self.Person.update({'_id': str(u.id)}, {'$set': {'some_id': str(new_id)} })
        self.assertEquals(ret['n'], 1)

        p = self.Person.find_one({'_id': u.id})
        self.assertEquals(p.some_id, new_id)

        self.assertEquals(self.Person.find_one({'some_id': u.some_id}), None)
        self.assertEquals(self.Person.count({'some_id': new_id}), 1)
        self.assertEquals(self.Person.count({'some_id': str(new_id)}), 1)
        p = self.Person.find_one({'some_id': new_id})
        self.assertTrue(isinstance(p.some_id, ObjectId))

        new_id2 = ObjectId()
        q = self.Person._transform_value({'$addToSet': {'id_list': str(new_id2)}}, self.Person)
        self.assertEquals(q['$addToSet']['id_list'], new_id2)
        ret = self.Person.update({'_id': u.id}, {'$addToSet': {'id_list': str(new_id2) } })

        self.assertEquals(ret['n'], 1)
        q = self.Person._transform_value({'$addToSet': {'id_list': new_id2}}, self.Person)
        self.assertEquals(q['$addToSet']['id_list'], new_id2)

        ret = self.Person.update({'_id': u.id}, {'$addToSet': {'id_list': new_id2 } })
        self.assertEquals(ret['n'], 1)

        ret = self.Person.update({'_id': u.id}, {'$addToSet': {'id_list': new_id } })
        self.assertEquals(ret['n'], 1)

        p = self.Person.find_one({'_id': u.id})
        self.assertEquals(len(p.id_list), 2)
        self.assertTrue(new_id in p.id_list)
        self.assertTrue(new_id2 in p.id_list)
        self.assertFalse(str(new_id2) in p.id_list)
        self.assertFalse(str(new_id) in p.id_list)

        new_id3 = ObjectId()
        new_id4 = ObjectId()
        ret = self.Person.update({'_id': u.id}, {'$set': {'id_list': [str(new_id3), new_id4]}})
        self.assertEquals(ret['n'], 1)

        self.assertEquals(self.Person.count({'id_list': new_id3}), 1)
        self.assertEquals(self.Person.count({'id_list': str(new_id4)}), 1)
        q = self.Person._transform_value({'id_list': str(new_id4)}, self.Person)
        self.assertEquals(q['id_list'], new_id4)

        p = self.Person.find_one({'_id': u.id})
        self.assertEquals(p.id_list, [new_id3, new_id4])

    def testListUpdate(self):
        u = self.Person(id=ObjectId(), some_id=ObjectId())
        u.save()

        ret = self.Person.update({'some_id': u.some_id}, {'some_id': u.some_id, 'number_list': range(3)}, multi=False)
        self.assertEquals(ret['n'], 1)

        u.reload()
        self.assertEquals(u.number_list, range(3))

        ret = self.Person.update({'some_id': u.some_id}, {'$set': {'number_list': range(5)}}, multi=False)
        self.assertEquals(ret['n'], 1)

        u.reload()
        self.assertEquals(u.number_list, range(5))

        ret = self.Person.update({'some_id': u.some_id}, {'some_id': u.some_id, 'name': 'Adam'}, multi=False)
        self.assertEquals(ret['n'], 1)

        u.reload()
        self.assertEquals(u.number_list, [])
        self.assertEquals(u.name, "Adam")

    def testPositionalOperator(self):
        red = self.Colour(name='Red')
        blue = self.Colour(name='Blue')
        green = self.Colour(name='Green')
        p = self.Person(other_colours=[red, blue, green])
        p.save()

        query = self.Person._transform_value({'$set': {'other_colours.$.name': 'Maroon'}}, self.Person)
        self.assertEquals(dict(query), {'$set': {'o.$.n': 'Maroon'}})

        query = self.Person._transform_value({'other_colours.name': 'Maroon'}, self.Person)
        self.assertEquals(dict(query), {'o.n': 'Maroon'})

        p2 = self.Person.find_one({'_id': p.id, 'other_colours.name': 'Red'})
        self.assertEquals(p.id, p2.id)

        resp = self.Person.update({'_id': p.id}, {'$set': {'other_colours.1.name': 'Aqua'}}, multi=False)
        self.assertEquals(resp['n'], 1)

        p.reload()
        self.assertEquals(p.other_colours[0].name, "Red")
        self.assertEquals(p.other_colours[1].name, "Aqua")
        self.assertEquals(p.other_colours[2].name, "Green")

        resp = self.Person.update({'_id': p.id, 'other_colours.name': 'Red'},
                                  {'$set': {'other_colours.$.name': 'Maroon'}},
                                  multi=False)
        self.assertEquals(resp['n'], 1)

        p.reload()
        self.assertEquals(p.other_colours[0].name, "Maroon")
        self.assertEquals(p.other_colours[1].name, "Aqua")
        self.assertEquals(p.other_colours[2].name, "Green")

    def testRemove(self):
        adam = self.Person(name="Adam")
        adam.save()
        josh = self.Person(name="Josh")
        josh.save()
        chu = self.Person(name="Chu")
        chu.save()
        danny = self.Person(name="Danny", friends=[adam, josh])
        danny.save()

        self.assertEquals(self.Person.count({}), 4)
        self.Person.remove({'name': {'$in': ['Adam', 'Josh']}})
        self.assertEquals(self.Person.count({}), 2)
        self.assertEquals(self.Person.count({'name': 'Adam'}), 0)

    def testAddToSet2(self):
        adam = self.Person(name="Adam")
        adam.save()
        red = self.Colour(name='Red')
        blue = self.Colour(name='Blue')
        green = self.Colour(name='Green')

        adam.add_to_set(other_colours=red)
        adam.reload()
        self.assertEquals(adam.other_colours, [red])
        adam.add_to_set(other_colours=red)
        adam.reload()
        self.assertEquals(adam.other_colours, [red])

        adam.add_to_set(other_colours=blue)
        adam.reload()
        self.assertEquals(adam.other_colours, [red, blue])

        adam.update_one({'$addToSet': {'other_colours': {'$each': [red, green, blue]}}})
        adam.reload()

        self.assertEquals(adam.other_colours, [red, blue, green])

    def testEmptyUpdates(self):
        adam = self.Person(name="Adam")
        adam.save()

        self.assertRaises(ValueError, adam.update_one, {})
        self.assertRaises(ValueError, self.Person.update, {'name': 'Adam'}, {})

        adam.reload()

        self.assertEquals(adam.name, "Adam")

    def testDictQueries(self):
        adam = self.Person(name="Adam", arbitrary_dict={'likes': 'boats'})
        adam.save()
        josh = self.Person(name="Josh", arbitrary_dict={'likes': 'fishing'})
        josh.save()

        p = self.Person.find({'arbitrary_dict.likes': 'boats'})
        self.assertEquals(len(p), 1)
        self.assertEquals(p[0].name, "Adam")

        p = self.Person.find({'arbitrary_dict.likes': 'fishing'})
        self.assertEquals(len(p), 1)
        self.assertEquals(p[0].name, "Josh")

        p = self.Person.find({'arbitrary_dict.likes': 'boring stuff'})
        self.assertEquals(len(p), 0)

        p = self.Person.find({'arbitrary_dict.hates': 'the world'})
        self.assertEquals(len(p), 0)

    def testExtraUpdateCriteria(self):
        adam = self.Person(name="Adam", age=23)
        adam.save()

        resp = adam.update_one({"$set": {"age": 27}}, criteria={"age": 25})
        self.assertEquals(resp['n'], 0)

        # confirm no in-mem transformation
        self.assertEquals(adam.age, 23)

        # confirm no DB change
        adam.reload()
        self.assertEquals(adam.age, 23)

        resp = adam.update_one({"$set": {"age": 27}}, criteria={"age": 23})
        self.assertEquals(resp['n'], 1)

        # confirm no in-mem transformation
        self.assertEquals(adam.age, 23)

        # confirm no DB change
        adam.reload()
        self.assertEquals(adam.age, 27)

if __name__ == '__main__':
    unittest.main()
