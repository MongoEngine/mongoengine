# -*- coding: utf-8 -*-
import unittest

from bson import SON
from mongoengine import *
from mongoengine.connection import get_db

__all__ = ("DeltaTest",)


class DeltaTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

        class Person(Document):
            name = StringField()
            age = IntField()

            non_field = True

            meta = {"allow_inheritance": True}

        self.Person = Person

    def tearDown(self):
        for collection in self.db.collection_names():
            if 'system.' in collection:
                continue
            self.db.drop_collection(collection)

    def test_delta(self):
        self.delta(Document)
        self.delta(DynamicDocument)

    def delta(self, DocClass):

        class Doc(DocClass):
            string_field = StringField()
            int_field = IntField()
            dict_field = DictField()
            list_field = ListField()

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEqual(doc._get_changed_fields(), [])
        self.assertEqual(doc._delta(), ({}, {}))

        doc.string_field = 'hello'
        self.assertEqual(doc._get_changed_fields(), ['string_field'])
        self.assertEqual(doc._delta(), ({'string_field': 'hello'}, {}))

        doc._changed_fields = []
        doc.int_field = 1
        self.assertEqual(doc._get_changed_fields(), ['int_field'])
        self.assertEqual(doc._delta(), ({'int_field': 1}, {}))

        doc._changed_fields = []
        dict_value = {'hello': 'world', 'ping': 'pong'}
        doc.dict_field = dict_value
        self.assertEqual(doc._get_changed_fields(), ['dict_field'])
        self.assertEqual(doc._delta(), ({'dict_field': dict_value}, {}))

        doc._changed_fields = []
        list_value = ['1', 2, {'hello': 'world'}]
        doc.list_field = list_value
        self.assertEqual(doc._get_changed_fields(), ['list_field'])
        self.assertEqual(doc._delta(), ({'list_field': list_value}, {}))

        # Test unsetting
        doc._changed_fields = []
        doc.dict_field = {}
        self.assertEqual(doc._get_changed_fields(), ['dict_field'])
        self.assertEqual(doc._delta(), ({}, {'dict_field': 1}))

        doc._changed_fields = []
        doc.list_field = []
        self.assertEqual(doc._get_changed_fields(), ['list_field'])
        self.assertEqual(doc._delta(), ({}, {'list_field': 1}))

    def test_delta_recursive(self):
        self.delta_recursive(Document, EmbeddedDocument)
        self.delta_recursive(DynamicDocument, EmbeddedDocument)
        self.delta_recursive(Document, DynamicEmbeddedDocument)
        self.delta_recursive(DynamicDocument, DynamicEmbeddedDocument)

    def delta_recursive(self, DocClass, EmbeddedClass):

        class Embedded(EmbeddedClass):
            id = StringField()
            string_field = StringField()
            int_field = IntField()
            dict_field = DictField()
            list_field = ListField()

        class Doc(DocClass):
            string_field = StringField()
            int_field = IntField()
            dict_field = DictField()
            list_field = ListField()
            embedded_field = EmbeddedDocumentField(Embedded)

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEqual(doc._get_changed_fields(), [])
        self.assertEqual(doc._delta(), ({}, {}))

        embedded_1 = Embedded()
        embedded_1.id = "010101"
        embedded_1.string_field = 'hello'
        embedded_1.int_field = 1
        embedded_1.dict_field = {'hello': 'world'}
        embedded_1.list_field = ['1', 2, {'hello': 'world'}]
        doc.embedded_field = embedded_1

        self.assertEqual(doc._get_changed_fields(), ['embedded_field'])

        embedded_delta = {
            'id': "010101",
            'string_field': 'hello',
            'int_field': 1,
            'dict_field': {'hello': 'world'},
            'list_field': ['1', 2, {'hello': 'world'}]
        }
        self.assertEqual(doc.embedded_field._delta(), (embedded_delta, {}))
        self.assertEqual(doc._delta(),
                         ({'embedded_field': embedded_delta}, {}))

        doc.save()
        doc = doc.reload(10)

        doc.embedded_field.dict_field = {}
        self.assertEqual(doc._get_changed_fields(),
                         ['embedded_field.dict_field'])
        self.assertEqual(doc.embedded_field._delta(), ({}, {'dict_field': 1}))
        self.assertEqual(doc._delta(), ({}, {'embedded_field.dict_field': 1}))
        doc.save()
        doc = doc.reload(10)
        self.assertEqual(doc.embedded_field.dict_field, {})

        doc.embedded_field.list_field = []
        self.assertEqual(doc._get_changed_fields(),
                         ['embedded_field.list_field'])
        self.assertEqual(doc.embedded_field._delta(), ({}, {'list_field': 1}))
        self.assertEqual(doc._delta(), ({}, {'embedded_field.list_field': 1}))
        doc.save()
        doc = doc.reload(10)
        self.assertEqual(doc.embedded_field.list_field, [])

        embedded_2 = Embedded()
        embedded_2.string_field = 'hello'
        embedded_2.int_field = 1
        embedded_2.dict_field = {'hello': 'world'}
        embedded_2.list_field = ['1', 2, {'hello': 'world'}]

        doc.embedded_field.list_field = ['1', 2, embedded_2]
        self.assertEqual(doc._get_changed_fields(),
                         ['embedded_field.list_field'])

        self.assertEqual(doc.embedded_field._delta(), ({
            'list_field': ['1', 2, {
                '_cls': 'Embedded',
                'string_field': 'hello',
                'dict_field': {'hello': 'world'},
                'int_field': 1,
                'list_field': ['1', 2, {'hello': 'world'}],
            }]
        }, {}))

        self.assertEqual(doc._delta(), ({
            'embedded_field.list_field': ['1', 2, {
                '_cls': 'Embedded',
                'string_field': 'hello',
                'dict_field': {'hello': 'world'},
                'int_field': 1,
                'list_field': ['1', 2, {'hello': 'world'}],
            }]
        }, {}))
        doc.save()
        doc = doc.reload(10)

        self.assertEqual(doc.embedded_field.list_field[0], '1')
        self.assertEqual(doc.embedded_field.list_field[1], 2)
        for k in doc.embedded_field.list_field[2]._fields:
            self.assertEqual(doc.embedded_field.list_field[2][k],
                             embedded_2[k])

        doc.embedded_field.list_field[2].string_field = 'world'
        self.assertEqual(doc._get_changed_fields(),
                         ['embedded_field.list_field.2.string_field'])
        self.assertEqual(doc.embedded_field._delta(),
                         ({'list_field.2.string_field': 'world'}, {}))
        self.assertEqual(doc._delta(),
                         ({'embedded_field.list_field.2.string_field': 'world'}, {}))
        doc.save()
        doc = doc.reload(10)
        self.assertEqual(doc.embedded_field.list_field[2].string_field,
                         'world')

        # Test multiple assignments
        doc.embedded_field.list_field[2].string_field = 'hello world'
        doc.embedded_field.list_field[2] = doc.embedded_field.list_field[2]
        self.assertEqual(doc._get_changed_fields(),
                         ['embedded_field.list_field.2'])
        self.assertEqual(doc.embedded_field._delta(), ({'list_field.2': {
                '_cls': 'Embedded',
                'string_field': 'hello world',
                'int_field': 1,
                'list_field': ['1', 2, {'hello': 'world'}],
                'dict_field': {'hello': 'world'}}
            }, {}))
        self.assertEqual(doc._delta(), ({'embedded_field.list_field.2': {
                '_cls': 'Embedded',
                'string_field': 'hello world',
                'int_field': 1,
                'list_field': ['1', 2, {'hello': 'world'}],
                'dict_field': {'hello': 'world'}}
            }, {}))
        doc.save()
        doc = doc.reload(10)
        self.assertEqual(doc.embedded_field.list_field[2].string_field,
                         'hello world')

        # Test list native methods
        doc.embedded_field.list_field[2].list_field.pop(0)
        self.assertEqual(doc._delta(),
                         ({'embedded_field.list_field.2.list_field':
                          [2, {'hello': 'world'}]}, {}))
        doc.save()
        doc = doc.reload(10)

        doc.embedded_field.list_field[2].list_field.append(1)
        self.assertEqual(doc._delta(),
                         ({'embedded_field.list_field.2.list_field':
                          [2, {'hello': 'world'}, 1]}, {}))
        doc.save()
        doc = doc.reload(10)
        self.assertEqual(doc.embedded_field.list_field[2].list_field,
                         [2, {'hello': 'world'}, 1])

        doc.embedded_field.list_field[2].list_field.sort(key=str)
        doc.save()
        doc = doc.reload(10)
        self.assertEqual(doc.embedded_field.list_field[2].list_field,
                         [1, 2, {'hello': 'world'}])

        del doc.embedded_field.list_field[2].list_field[2]['hello']
        self.assertEqual(doc._delta(),
                         ({}, {'embedded_field.list_field.2.list_field.2.hello': 1}))
        doc.save()
        doc = doc.reload(10)

        del doc.embedded_field.list_field[2].list_field
        self.assertEqual(doc._delta(),
                         ({}, {'embedded_field.list_field.2.list_field': 1}))

        doc.save()
        doc = doc.reload(10)

        doc.dict_field['Embedded'] = embedded_1
        doc.save()
        doc = doc.reload(10)

        doc.dict_field['Embedded'].string_field = 'Hello World'
        self.assertEqual(doc._get_changed_fields(),
                         ['dict_field.Embedded.string_field'])
        self.assertEqual(doc._delta(),
                         ({'dict_field.Embedded.string_field': 'Hello World'}, {}))

    def test_circular_reference_deltas(self):
        self.circular_reference_deltas(Document, Document)
        self.circular_reference_deltas(Document, DynamicDocument)
        self.circular_reference_deltas(DynamicDocument, Document)
        self.circular_reference_deltas(DynamicDocument, DynamicDocument)

    def circular_reference_deltas(self, DocClass1, DocClass2):

        class Person(DocClass1):
            name = StringField()
            owns = ListField(ReferenceField('Organization'))

        class Organization(DocClass2):
            name = StringField()
            owner = ReferenceField('Person')

        Person.drop_collection()
        Organization.drop_collection()

        person = Person(name="owner").save()
        organization = Organization(name="company").save()

        person.owns.append(organization)
        organization.owner = person

        person.save()
        organization.save()

        p = Person.objects[0].select_related()
        o = Organization.objects.first()
        self.assertEqual(p.owns[0], o)
        self.assertEqual(o.owner, p)

    def test_circular_reference_deltas_2(self):
        self.circular_reference_deltas_2(Document, Document)
        self.circular_reference_deltas_2(Document, DynamicDocument)
        self.circular_reference_deltas_2(DynamicDocument, Document)
        self.circular_reference_deltas_2(DynamicDocument, DynamicDocument)

    def circular_reference_deltas_2(self, DocClass1, DocClass2, dbref=True):

        class Person(DocClass1):
            name = StringField()
            owns = ListField(ReferenceField('Organization', dbref=dbref))
            employer = ReferenceField('Organization', dbref=dbref)

        class Organization(DocClass2):
            name = StringField()
            owner = ReferenceField('Person', dbref=dbref)
            employees = ListField(ReferenceField('Person', dbref=dbref))

        Person.drop_collection()
        Organization.drop_collection()

        person = Person(name="owner").save()
        employee = Person(name="employee").save()
        organization = Organization(name="company").save()

        person.owns.append(organization)
        organization.owner = person

        organization.employees.append(employee)
        employee.employer = organization

        person.save()
        organization.save()
        employee.save()

        p = Person.objects.get(name="owner")
        e = Person.objects.get(name="employee")
        o = Organization.objects.first()

        self.assertEqual(p.owns[0], o)
        self.assertEqual(o.owner, p)
        self.assertEqual(e.employer, o)

        return person, organization, employee

    def test_delta_db_field(self):
        self.delta_db_field(Document)
        self.delta_db_field(DynamicDocument)

    def delta_db_field(self, DocClass):

        class Doc(DocClass):
            string_field = StringField(db_field='db_string_field')
            int_field = IntField(db_field='db_int_field')
            dict_field = DictField(db_field='db_dict_field')
            list_field = ListField(db_field='db_list_field')

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEqual(doc._get_changed_fields(), [])
        self.assertEqual(doc._delta(), ({}, {}))

        doc.string_field = 'hello'
        self.assertEqual(doc._get_changed_fields(), ['db_string_field'])
        self.assertEqual(doc._delta(), ({'db_string_field': 'hello'}, {}))

        doc._changed_fields = []
        doc.int_field = 1
        self.assertEqual(doc._get_changed_fields(), ['db_int_field'])
        self.assertEqual(doc._delta(), ({'db_int_field': 1}, {}))

        doc._changed_fields = []
        dict_value = {'hello': 'world', 'ping': 'pong'}
        doc.dict_field = dict_value
        self.assertEqual(doc._get_changed_fields(), ['db_dict_field'])
        self.assertEqual(doc._delta(), ({'db_dict_field': dict_value}, {}))

        doc._changed_fields = []
        list_value = ['1', 2, {'hello': 'world'}]
        doc.list_field = list_value
        self.assertEqual(doc._get_changed_fields(), ['db_list_field'])
        self.assertEqual(doc._delta(), ({'db_list_field': list_value}, {}))

        # Test unsetting
        doc._changed_fields = []
        doc.dict_field = {}
        self.assertEqual(doc._get_changed_fields(), ['db_dict_field'])
        self.assertEqual(doc._delta(), ({}, {'db_dict_field': 1}))

        doc._changed_fields = []
        doc.list_field = []
        self.assertEqual(doc._get_changed_fields(), ['db_list_field'])
        self.assertEqual(doc._delta(), ({}, {'db_list_field': 1}))

        # Test it saves that data
        doc = Doc()
        doc.save()

        doc.string_field = 'hello'
        doc.int_field = 1
        doc.dict_field = {'hello': 'world'}
        doc.list_field = ['1', 2, {'hello': 'world'}]
        doc.save()
        doc = doc.reload(10)

        self.assertEqual(doc.string_field, 'hello')
        self.assertEqual(doc.int_field, 1)
        self.assertEqual(doc.dict_field, {'hello': 'world'})
        self.assertEqual(doc.list_field, ['1', 2, {'hello': 'world'}])

    def test_delta_recursive_db_field(self):
        self.delta_recursive_db_field(Document, EmbeddedDocument)
        self.delta_recursive_db_field(Document, DynamicEmbeddedDocument)
        self.delta_recursive_db_field(DynamicDocument, EmbeddedDocument)
        self.delta_recursive_db_field(DynamicDocument, DynamicEmbeddedDocument)

    def delta_recursive_db_field(self, DocClass, EmbeddedClass):

        class Embedded(EmbeddedClass):
            string_field = StringField(db_field='db_string_field')
            int_field = IntField(db_field='db_int_field')
            dict_field = DictField(db_field='db_dict_field')
            list_field = ListField(db_field='db_list_field')

        class Doc(DocClass):
            string_field = StringField(db_field='db_string_field')
            int_field = IntField(db_field='db_int_field')
            dict_field = DictField(db_field='db_dict_field')
            list_field = ListField(db_field='db_list_field')
            embedded_field = EmbeddedDocumentField(Embedded,
                                    db_field='db_embedded_field')

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEqual(doc._get_changed_fields(), [])
        self.assertEqual(doc._delta(), ({}, {}))

        embedded_1 = Embedded()
        embedded_1.string_field = 'hello'
        embedded_1.int_field = 1
        embedded_1.dict_field = {'hello': 'world'}
        embedded_1.list_field = ['1', 2, {'hello': 'world'}]
        doc.embedded_field = embedded_1

        self.assertEqual(doc._get_changed_fields(), ['db_embedded_field'])

        embedded_delta = {
            'db_string_field': 'hello',
            'db_int_field': 1,
            'db_dict_field': {'hello': 'world'},
            'db_list_field': ['1', 2, {'hello': 'world'}]
        }
        self.assertEqual(doc.embedded_field._delta(), (embedded_delta, {}))
        self.assertEqual(doc._delta(),
            ({'db_embedded_field': embedded_delta}, {}))

        doc.save()
        doc = doc.reload(10)

        doc.embedded_field.dict_field = {}
        self.assertEqual(doc._get_changed_fields(),
            ['db_embedded_field.db_dict_field'])
        self.assertEqual(doc.embedded_field._delta(),
            ({}, {'db_dict_field': 1}))
        self.assertEqual(doc._delta(),
            ({}, {'db_embedded_field.db_dict_field': 1}))
        doc.save()
        doc = doc.reload(10)
        self.assertEqual(doc.embedded_field.dict_field, {})

        doc.embedded_field.list_field = []
        self.assertEqual(doc._get_changed_fields(),
            ['db_embedded_field.db_list_field'])
        self.assertEqual(doc.embedded_field._delta(),
            ({}, {'db_list_field': 1}))
        self.assertEqual(doc._delta(),
            ({}, {'db_embedded_field.db_list_field': 1}))
        doc.save()
        doc = doc.reload(10)
        self.assertEqual(doc.embedded_field.list_field, [])

        embedded_2 = Embedded()
        embedded_2.string_field = 'hello'
        embedded_2.int_field = 1
        embedded_2.dict_field = {'hello': 'world'}
        embedded_2.list_field = ['1', 2, {'hello': 'world'}]

        doc.embedded_field.list_field = ['1', 2, embedded_2]
        self.assertEqual(doc._get_changed_fields(),
            ['db_embedded_field.db_list_field'])
        self.assertEqual(doc.embedded_field._delta(), ({
            'db_list_field': ['1', 2, {
                '_cls': 'Embedded',
                'db_string_field': 'hello',
                'db_dict_field': {'hello': 'world'},
                'db_int_field': 1,
                'db_list_field': ['1', 2, {'hello': 'world'}],
            }]
        }, {}))

        self.assertEqual(doc._delta(), ({
            'db_embedded_field.db_list_field': ['1', 2, {
                '_cls': 'Embedded',
                'db_string_field': 'hello',
                'db_dict_field': {'hello': 'world'},
                'db_int_field': 1,
                'db_list_field': ['1', 2, {'hello': 'world'}],
            }]
        }, {}))
        doc.save()
        doc = doc.reload(10)

        self.assertEqual(doc.embedded_field.list_field[0], '1')
        self.assertEqual(doc.embedded_field.list_field[1], 2)
        for k in doc.embedded_field.list_field[2]._fields:
            self.assertEqual(doc.embedded_field.list_field[2][k],
                             embedded_2[k])

        doc.embedded_field.list_field[2].string_field = 'world'
        self.assertEqual(doc._get_changed_fields(),
            ['db_embedded_field.db_list_field.2.db_string_field'])
        self.assertEqual(doc.embedded_field._delta(),
            ({'db_list_field.2.db_string_field': 'world'}, {}))
        self.assertEqual(doc._delta(),
            ({'db_embedded_field.db_list_field.2.db_string_field': 'world'},
             {}))
        doc.save()
        doc = doc.reload(10)
        self.assertEqual(doc.embedded_field.list_field[2].string_field,
                        'world')

        # Test multiple assignments
        doc.embedded_field.list_field[2].string_field = 'hello world'
        doc.embedded_field.list_field[2] = doc.embedded_field.list_field[2]
        self.assertEqual(doc._get_changed_fields(),
            ['db_embedded_field.db_list_field.2'])
        self.assertEqual(doc.embedded_field._delta(), ({'db_list_field.2': {
            '_cls': 'Embedded',
            'db_string_field': 'hello world',
            'db_int_field': 1,
            'db_list_field': ['1', 2, {'hello': 'world'}],
            'db_dict_field': {'hello': 'world'}}}, {}))
        self.assertEqual(doc._delta(), ({
            'db_embedded_field.db_list_field.2': {
                '_cls': 'Embedded',
                'db_string_field': 'hello world',
                'db_int_field': 1,
                'db_list_field': ['1', 2, {'hello': 'world'}],
                'db_dict_field': {'hello': 'world'}}
            }, {}))
        doc.save()
        doc = doc.reload(10)
        self.assertEqual(doc.embedded_field.list_field[2].string_field,
                        'hello world')

        # Test list native methods
        doc.embedded_field.list_field[2].list_field.pop(0)
        self.assertEqual(doc._delta(),
            ({'db_embedded_field.db_list_field.2.db_list_field':
                [2, {'hello': 'world'}]}, {}))
        doc.save()
        doc = doc.reload(10)

        doc.embedded_field.list_field[2].list_field.append(1)
        self.assertEqual(doc._delta(),
            ({'db_embedded_field.db_list_field.2.db_list_field':
                [2, {'hello': 'world'}, 1]}, {}))
        doc.save()
        doc = doc.reload(10)
        self.assertEqual(doc.embedded_field.list_field[2].list_field,
            [2, {'hello': 'world'}, 1])

        doc.embedded_field.list_field[2].list_field.sort(key=str)
        doc.save()
        doc = doc.reload(10)
        self.assertEqual(doc.embedded_field.list_field[2].list_field,
            [1, 2, {'hello': 'world'}])

        del doc.embedded_field.list_field[2].list_field[2]['hello']
        self.assertEqual(doc._delta(),
            ({}, {'db_embedded_field.db_list_field.2.db_list_field.2.hello': 1}))
        doc.save()
        doc = doc.reload(10)

        del doc.embedded_field.list_field[2].list_field
        self.assertEqual(doc._delta(), ({},
            {'db_embedded_field.db_list_field.2.db_list_field': 1}))

    def test_delta_for_dynamic_documents(self):
        class Person(DynamicDocument):
            name = StringField()
            meta = {'allow_inheritance': True}

        Person.drop_collection()

        p = Person(name="James", age=34)
        self.assertEqual(p._delta(), (
            SON([('_cls', 'Person'), ('name', 'James'), ('age', 34)]), {}))

        p.doc = 123
        del p.doc
        self.assertEqual(p._delta(), (
            SON([('_cls', 'Person'), ('name', 'James'), ('age', 34)]), {}))

        p = Person()
        p.name = "Dean"
        p.age = 22
        p.save()

        p.age = 24
        self.assertEqual(p.age, 24)
        self.assertEqual(p._get_changed_fields(), ['age'])
        self.assertEqual(p._delta(), ({'age': 24}, {}))

        p = Person.objects(age=22).get()
        p.age = 24
        self.assertEqual(p.age, 24)
        self.assertEqual(p._get_changed_fields(), ['age'])
        self.assertEqual(p._delta(), ({'age': 24}, {}))

        p.save()
        self.assertEqual(1, Person.objects(age=24).count())

    def test_dynamic_delta(self):

        class Doc(DynamicDocument):
            pass

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEqual(doc._get_changed_fields(), [])
        self.assertEqual(doc._delta(), ({}, {}))

        doc.string_field = 'hello'
        self.assertEqual(doc._get_changed_fields(), ['string_field'])
        self.assertEqual(doc._delta(), ({'string_field': 'hello'}, {}))

        doc._changed_fields = []
        doc.int_field = 1
        self.assertEqual(doc._get_changed_fields(), ['int_field'])
        self.assertEqual(doc._delta(), ({'int_field': 1}, {}))

        doc._changed_fields = []
        dict_value = {'hello': 'world', 'ping': 'pong'}
        doc.dict_field = dict_value
        self.assertEqual(doc._get_changed_fields(), ['dict_field'])
        self.assertEqual(doc._delta(), ({'dict_field': dict_value}, {}))

        doc._changed_fields = []
        list_value = ['1', 2, {'hello': 'world'}]
        doc.list_field = list_value
        self.assertEqual(doc._get_changed_fields(), ['list_field'])
        self.assertEqual(doc._delta(), ({'list_field': list_value}, {}))

        # Test unsetting
        doc._changed_fields = []
        doc.dict_field = {}
        self.assertEqual(doc._get_changed_fields(), ['dict_field'])
        self.assertEqual(doc._delta(), ({}, {'dict_field': 1}))

        doc._changed_fields = []
        doc.list_field = []
        self.assertEqual(doc._get_changed_fields(), ['list_field'])
        self.assertEqual(doc._delta(), ({}, {'list_field': 1}))

    def test_delta_with_dbref_true(self):
        person, organization, employee = self.circular_reference_deltas_2(Document, Document, True)
        employee.name = 'test'

        self.assertEqual(organization._get_changed_fields(), [])

        updates, removals = organization._delta()
        self.assertEqual({}, removals)
        self.assertEqual({}, updates)

        organization.employees.append(person)
        updates, removals = organization._delta()
        self.assertEqual({}, removals)
        self.assertTrue('employees' in updates)

    def test_delta_with_dbref_false(self):
        person, organization, employee = self.circular_reference_deltas_2(Document, Document, False)
        employee.name = 'test'

        self.assertEqual(organization._get_changed_fields(), [])

        updates, removals = organization._delta()
        self.assertEqual({}, removals)
        self.assertEqual({}, updates)

        organization.employees.append(person)
        updates, removals = organization._delta()
        self.assertEqual({}, removals)
        self.assertTrue('employees' in updates)

    def test_nested_nested_fields_mark_as_changed(self):
        class EmbeddedDoc(EmbeddedDocument):
            name = StringField()

        class MyDoc(Document):
            subs = MapField(MapField(EmbeddedDocumentField(EmbeddedDoc)))
            name = StringField()

        MyDoc.drop_collection()

        mydoc = MyDoc(name='testcase1', subs={'a': {'b': EmbeddedDoc(name='foo')}}).save()

        mydoc = MyDoc.objects.first()
        subdoc = mydoc.subs['a']['b']
        subdoc.name = 'bar'

        self.assertEqual(["name"], subdoc._get_changed_fields())
        self.assertEqual(["subs.a.b.name"], mydoc._get_changed_fields())

        mydoc._clear_changed_fields()
        self.assertEqual([], mydoc._get_changed_fields())

    def test_lower_level_mark_as_changed(self):
        class EmbeddedDoc(EmbeddedDocument):
            name = StringField()

        class MyDoc(Document):
            subs = MapField(EmbeddedDocumentField(EmbeddedDoc))

        MyDoc.drop_collection()

        MyDoc().save()

        mydoc = MyDoc.objects.first()
        mydoc.subs['a'] = EmbeddedDoc()
        self.assertEqual(["subs.a"], mydoc._get_changed_fields())

        subdoc = mydoc.subs['a']
        subdoc.name = 'bar'

        self.assertEqual(["name"], subdoc._get_changed_fields())
        self.assertEqual(["subs.a"], mydoc._get_changed_fields())
        mydoc.save()

        mydoc._clear_changed_fields()
        self.assertEqual([], mydoc._get_changed_fields())

    def test_upper_level_mark_as_changed(self):
        class EmbeddedDoc(EmbeddedDocument):
            name = StringField()

        class MyDoc(Document):
            subs = MapField(EmbeddedDocumentField(EmbeddedDoc))

        MyDoc.drop_collection()

        MyDoc(subs={'a': EmbeddedDoc(name='foo')}).save()

        mydoc = MyDoc.objects.first()
        subdoc = mydoc.subs['a']
        subdoc.name = 'bar'

        self.assertEqual(["name"], subdoc._get_changed_fields())
        self.assertEqual(["subs.a.name"], mydoc._get_changed_fields())

        mydoc.subs['a'] = EmbeddedDoc()
        self.assertEqual(["subs.a"], mydoc._get_changed_fields())
        mydoc.save()

        mydoc._clear_changed_fields()
        self.assertEqual([], mydoc._get_changed_fields())

    def test_referenced_object_changed_attributes(self):
        """Ensures that when you save a new reference to a field, the referenced object isn't altered"""

        class Organization(Document):
            name = StringField()

        class User(Document):
            name = StringField()
            org = ReferenceField('Organization', required=True)

        Organization.drop_collection()
        User.drop_collection()

        org1 = Organization(name='Org 1')
        org1.save()

        org2 = Organization(name='Org 2')
        org2.save()

        user = User(name='Fred', org=org1)
        user.save()

        org1.reload()
        org2.reload()
        user.reload()
        self.assertEqual(org1.name, 'Org 1')
        self.assertEqual(org2.name, 'Org 2')
        self.assertEqual(user.name, 'Fred')

        user.name = 'Harold'
        user.org = org2

        org2.name = 'New Org 2'
        self.assertEqual(org2.name, 'New Org 2')

        user.save()
        org2.save()

        self.assertEqual(org2.name, 'New Org 2')
        org2.reload()
        self.assertEqual(org2.name, 'New Org 2')

    def test_delta_for_nested_map_fields(self):
        class UInfoDocument(Document):
            phone = StringField()

        class EmbeddedRole(EmbeddedDocument):
            type = StringField()

        class EmbeddedUser(EmbeddedDocument):
            name = StringField()
            roles = MapField(field=EmbeddedDocumentField(EmbeddedRole))
            rolist = ListField(field=EmbeddedDocumentField(EmbeddedRole))
            info = ReferenceField(UInfoDocument)

        class Doc(Document):
            users = MapField(field=EmbeddedDocumentField(EmbeddedUser))
            num = IntField(default=-1)

        Doc.drop_collection()

        doc = Doc(num=1)
        doc.users["007"] = EmbeddedUser(name="Agent007")
        doc.save()

        uinfo = UInfoDocument(phone="79089269066")
        uinfo.save()

        d = Doc.objects(num=1).first()
        d.users["007"]["roles"]["666"] = EmbeddedRole(type="superadmin")
        d.users["007"]["rolist"].append(EmbeddedRole(type="oops"))
        d.users["007"]["info"] = uinfo
        delta = d._delta()
        self.assertEqual(True, "users.007.roles.666" in delta[0])
        self.assertEqual(True, "users.007.rolist" in delta[0])
        self.assertEqual(True, "users.007.info" in delta[0])
        self.assertEqual('superadmin', delta[0]["users.007.roles.666"]["type"])
        self.assertEqual('oops', delta[0]["users.007.rolist"][0]["type"])
        self.assertEqual(uinfo.id, delta[0]["users.007.info"])

if __name__ == '__main__':
    unittest.main()
