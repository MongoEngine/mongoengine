import pymongo
import unittest
import warnings

from mongoengine import *
from mongoengine.connection import _get_db

from bson import ObjectId

class DocumentTest(unittest.TestCase):

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

        class Colour(EmbeddedDocument):
            meta = {'allow_inheritance': False}
            name = StringField(db_field='n')

            def __str__(self):
                return self.name

        class User(Document):
            id = ObjectIdField(primary_key=True)
            name = StringField()

        self.Person = Person
        self.Colour = Colour
        self.User = User

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

if __name__ == '__main__':
    unittest.main()
