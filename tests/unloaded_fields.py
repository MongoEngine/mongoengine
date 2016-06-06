import unittest

import mongoengine.connection

from mongoengine import fields as f
from mongoengine.base import _document_registry
from mongoengine.document import Document, EmbeddedDocument

mongoengine.connection.set_default_db("test")
mongoengine.base.set_unloaded_field_handler(
    mongoengine.base.UnloadedFieldExceptionHandler())


class UnloadedFieldsTest(unittest.TestCase):

    def setUp(self):
        class Thought(EmbeddedDocument):
            name = f.StringField()
            contents = f.StringField()

        class Person(Document):
            name = f.StringField()
            age = f.IntField(default=30)
            coffee = f.IntField(db_field='s', default=5)
            fav_nums = f.ListField(f.IntField())
            userid = f.StringField()
            friends = f.ListField(f.ReferenceField('Person'))
            wildcard = f.ListField(f.GenericReferenceField())
            best_friend = f.ReferenceField('Person')
            best_blob = f.GenericReferenceField()
            thought = f.EmbeddedDocumentField('Thought')
            other_thoughts = f.ListField(f.EmbeddedDocumentField('Thought'))
            other_attrs = f.DictField()
        mongoengine.connection.connect()
        self.db = mongoengine.connection._get_db()
        self.person_cls = Person
        self.thought_cls = Thought

    def tearDown(self):
        _document_registry.clear()
        self.person_cls.drop_collection()

    def test_new_document_has_all_loaded(self):
        person = self.person_cls(name='Test Person')
        person.save()

        self.assertEqual(person.name, 'Test Person')
        self.assertEqual(person.age, 30)
        self.assertEqual(person.coffee, 5)
        self.assertEqual(person.userid, None)

    def test_retrieve_not_loaded(self):
        person = self.person_cls(name='A person')
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['name', 'age']
        )

        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'coffee')

    def test_set_then_retrieve(self):
        person = self.person_cls(name='Foo')
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['name', 'age']
        )

        person_loaded.coffee = 20

        self.assertEqual(person_loaded.coffee, 20)

    def test_update_then_retrieve(self):
        person = self.person_cls(name='Foo')
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['name', 'age']
        )

        person_loaded.set(coffee=20)

        self.assertEqual(person_loaded.coffee, 20)

    def test_reload_loads_all_fields(self):
        person = self.person_cls(name='Bar')
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['age']
        )

        person_loaded.reload()

        self.assertEqual(person.name, 'Bar')
        self.assertEqual(person.age, 30)
        self.assertEqual(person.coffee, 5)
        self.assertEqual(person.userid, None)

    def test_validate_only_loaded_fields(self):
        person = self.person_cls(name='Baz')
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['age']
        )

        person_loaded.validate()

    def test_loads_null_fields(self):
        person = self.person_cls(name='Foobar')
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['name', 'userid']
        )

        self.assertIsNone(person_loaded.userid)

    def test_implicit_none_loads_default(self):
        person = self.person_cls(name='Foobaz')
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['name', 'coffee']
        )

        self.assertEquals(person_loaded.coffee, 5)

    def test_unset_loads_default(self):
        person = self.person_cls(name='Barfoo', coffee=10)
        person.save()

        person.unset(coffee=None)

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['name', 'coffee']
        )

        self.assertEquals(person_loaded.coffee, 5)

    def test_excluded_fields(self):
        person = self.person_cls(name='Bazfoo', coffee=10)
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields={'coffee': 0}
        )

        self.assertEqual(person_loaded.id, person.id)
        self.assertEqual(person_loaded.name, 'Bazfoo')
        self.assertEqual(person_loaded.age, 30)
        self.assertEqual(person_loaded.userid, None)

        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'coffee')

    def test_explicit_included_fields_loaded(self):
        person = self.person_cls(name='Bazbar', coffee=10)
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields={'name': 1}
        )

        self.assertEqual(person_loaded.name, 'Bazbar')
        # verify id always loaded if not explicitly excluded
        self.assertEqual(person_loaded.id, person.id)
        for field in ['coffee', 'userid', 'age']:
            with self.assertRaises(mongoengine.base.FieldNotLoadedError):
                getattr(person_loaded, field)

    def test_exclude_id(self):
        person = self.person_cls(name='[object Object]')
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields={'name': 1, 'id': 0}
        )

        self.assertEqual(person_loaded.name, '[object Object]')
        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'id')

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields={'id': 0, 'name': 1}
        )

        self.assertEqual(person_loaded.name, '[object Object]')
        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'id')

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields={'id': 0}
        )

        self.assertEqual(person_loaded.name, '[object Object]')
        self.assertEqual(person_loaded.age, 30)
        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'id')

    def test_list_field_loaded(self):
        person = self.person_cls(fav_nums=[1, 2, 3])
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['fav_nums']
        )

        self.assertEqual(person_loaded.fav_nums, [1, 2, 3])

    def test_list_field_not_loaded(self):
        person = self.person_cls(fav_nums=[1, 2, 3])
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['age']
        )

        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'fav_nums')

    def test_list_ref_field_loaded(self):
        friend = self.person_cls()
        friend.save()
        person = self.person_cls(fav_nums=[1, 2, 3], friends=[friend])
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['friends']
        )

        self.assertEqual(person_loaded.friends[0].id, friend.id)

    def test_list_ref_field_not_loaded(self):
        friend = self.person_cls()
        friend.save()
        person = self.person_cls(friends=[friend])
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['age']
        )

        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'friends')

    def test_list_gen_ref_field_loaded(self):
        other = self.person_cls()
        other.save()
        person = self.person_cls(fav_nums=[1, 2, 3], wildcard=[other])
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['wildcard']
        )

        self.assertEqual(person_loaded.wildcard[0].id, other.id)

    def test_list_gen_ref_field_not_loaded(self):
        other = self.person_cls()
        other.save()
        person = self.person_cls(wildcard=[other])
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['age']
        )

        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'wildcard')

    def test_ref_field_loaded(self):
        other = self.person_cls()
        other.save()
        person = self.person_cls(best_friend=other)
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['best_friend']
        )

        self.assertEqual(person_loaded.best_friend.id, other.id)

    def test_ref_field_not_loaded(self):
        other = self.person_cls()
        other.save()
        person = self.person_cls(best_friend=other)
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['age']
        )

        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'best_friend')

    def test_gen_ref_field_loaded(self):
        other = self.person_cls()
        other.save()
        person = self.person_cls(best_blob=other)
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['best_blob']
        )

        self.assertEqual(person_loaded.best_blob.id, other.id)

    def test_gen_ref_field_not_loaded(self):
        other = self.person_cls()
        other.save()
        person = self.person_cls(best_blob=other)
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['age']
        )

        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'best_blob')

    def test_access_to_data_loaded_and_unloaded(self):
        person = self.person_cls(coffee=10, userid='person')
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['coffee']
        )

        _data = person_loaded._data

        self.assertEqual(_data['coffee'], 10)
        self.assertEqual(_data['userid'], None)
        self.assertEqual(_data['name'], None)
        self.assertEqual(_data['age'], 30)

    def test_direct_data_does_deep_copy(self):
        person = self.person_cls(
            coffee=10,
            fav_nums=[1, 2, 3],
            other_attrs={'happy': {'n': 1}}
        )
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id}
        )

        _data = person_loaded._data
        self.assertEqual(_data['fav_nums'], [1, 2, 3])
        self.assertEqual(person_loaded.fav_nums, [1, 2, 3])
        self.assertEqual(_data['other_attrs'], {'happy': {'n': 1}})
        self.assertEqual(person_loaded.other_attrs, {'happy': {'n': 1}})

        _data['fav_nums'][2] = 4
        _data['other_attrs']['happy']['n'] = 2

        self.assertEqual(person_loaded.fav_nums, [1, 2, 3])
        self.assertEqual(person_loaded.other_attrs, {'happy': {'n': 1}})

    def test_dereferenced_fields_have_all_fields(self):
        people = []
        for _ in range(0, 5):
            new_person = self.person_cls()
            new_person.save()
            people.append(new_person)

        person = self.person_cls(
            best_friend=people[0],
            best_blob=people[1],
            friends=[people[1], people[2]],
            wildcard=[people[3], people[4]]
        )
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id}
        )

        getattr(person_loaded.best_friend, 'name')
        getattr(person_loaded.best_blob, 'name')
        getattr(person_loaded.friends[0], 'name')
        getattr(person_loaded.wildcard[0], 'name')

    def test_embedded_fields_have_all_fields(self):
        person = self.person_cls(name='Descartes')
        thought = self.thought_cls(contents='I am')
        person.thought = thought
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id}
        )

        getattr(person_loaded.thought, 'name')

    def test_can_test_if_field_loaded(self):
        person = self.person_cls(name='A person')
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['name', 'age']
        )

        self.assertTrue(person_loaded.field_is_loaded('name'))
        self.assertFalse(person_loaded.field_is_loaded('coffee'))

    def test_can_load_id_with_empty_list(self):
        person = self.person_cls(name='A person')
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=[]
        )

        self.assertEqual(person.id, person_loaded.id)

    def test_can_load_id_with_empty_dict(self):
        person = self.person_cls(name='A person')
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields={}
        )

        self.assertEqual(person.id, person_loaded.id)

    def test_can_update_field_without_loading(self):
        person = self.person_cls(age=21)
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=[]
        )

        # happy birthday
        person_loaded.update_one({'$inc': {'age': 1}})

        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'age')

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['age']
        )

        self.assertEqual(person_loaded.age, 22)

        person_loaded.update_one({'$inc': {'age': 1}})

        self.assertEqual(person_loaded.age, 23)

    def test_subdoc_fields(self):
        thought = self.thought_cls(name='cat', contents='I\'m in a hat')
        person = self.person_cls(age=21, thought=thought)
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['thought.contents']
        )

        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'age')

        getattr(person_loaded, 'thought')

        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded.thought, 'name')
        self.assertEqual(person_loaded.thought.contents, 'I\'m in a hat')

    def test_list_subdoc_fields(self):
        person = self.person_cls(age=21, other_thoughts=[
            self.thought_cls(name='thing1', contents='one'),
            self.thought_cls(name='thing2', contents='two')])
        person.save()

        person_loaded = self.person_cls.find_one(
            {'_id': person.id},
            fields=['other_thoughts.contents']
        )

        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'age')

        getattr(person_loaded, 'other_thoughts')

        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded.other_thoughts[0], 'name')
        self.assertEqual(person_loaded.other_thoughts[1].contents, 'two')

    def test_queryset_loads_all_fields(self):
        person = self.person_cls(name='CLark')
        person.save()

        person_loaded = self.person_cls.objects(name='CLark').first()
        self.assertEqual(person_loaded.name, 'CLark')
        self.assertEqual(person_loaded.age, 30)

        # should not raise exception
        getattr(person_loaded, 'userid')

    def test_queryset_limits_fields(self):
        person = self.person_cls(name='CLaire')
        person.save()

        person_loaded = self.person_cls.objects(name='CLaire').only(
            'name').first()

        self.assertEqual(person_loaded.name, 'CLaire')
        with self.assertRaises(mongoengine.base.FieldNotLoadedError):
            getattr(person_loaded, 'age')


if __name__ == '__main__':
    unittest.main()
