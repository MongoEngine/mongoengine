import unittest

from mongoengine import *
from mongoengine.connection import get_db
from mongoengine.tests import query_counter


class FieldTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')
        self.db = get_db()

    def test_list_item_dereference(self):
        """Ensure that DBRef items in ListFields are dereferenced.
        """
        class User(Document):
            name = StringField()

        class Group(Document):
            members = ListField(ReferenceField(User))

        User.drop_collection()
        Group.drop_collection()

        for i in xrange(1, 51):
            user = User(name='user %s' % i)
            user.save()

        group = Group(members=User.objects)
        group.save()

        group = Group(members=User.objects)
        group.save()

        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first()
            self.assertEqual(q, 1)

            [m for m in group_obj.members]
            self.assertEqual(q, 2)

        # Document select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first().select_related()
            self.assertEqual(q, 2)
            [m for m in group_obj.members]
            self.assertEqual(q, 2)

        # Queryset select_related
        with query_counter() as q:
            self.assertEqual(q, 0)
            group_objs = Group.objects.select_related()
            self.assertEqual(q, 2)
            for group_obj in group_objs:
                [m for m in group_obj.members]
                self.assertEqual(q, 2)

        User.drop_collection()
        Group.drop_collection()

    def test_recursive_reference(self):
        """Ensure that ReferenceFields can reference their own documents.
        """
        class Employee(Document):
            name = StringField()
            boss = ReferenceField('self')
            friends = ListField(ReferenceField('self'))

        Employee.drop_collection()

        bill = Employee(name='Bill Lumbergh')
        bill.save()

        michael = Employee(name='Michael Bolton')
        michael.save()

        samir = Employee(name='Samir Nagheenanajar')
        samir.save()

        friends = [michael, samir]
        peter = Employee(name='Peter Gibbons', boss=bill, friends=friends)
        peter.save()

        Employee(name='Funky Gibbon', boss=bill, friends=friends).save()
        Employee(name='Funky Gibbon', boss=bill, friends=friends).save()
        Employee(name='Funky Gibbon', boss=bill, friends=friends).save()

        with query_counter() as q:
            self.assertEqual(q, 0)

            peter = Employee.objects.with_id(peter.id)
            self.assertEqual(q, 1)

            peter.boss
            self.assertEqual(q, 2)

            peter.friends
            self.assertEqual(q, 3)

        # Document select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            peter = Employee.objects.with_id(peter.id).select_related()
            self.assertEqual(q, 2)

            self.assertEquals(peter.boss, bill)
            self.assertEqual(q, 2)

            self.assertEquals(peter.friends, friends)
            self.assertEqual(q, 2)

        # Queryset select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            employees = Employee.objects(boss=bill).select_related()
            self.assertEqual(q, 2)

            for employee in employees:
                self.assertEquals(employee.boss, bill)
                self.assertEqual(q, 2)

                self.assertEquals(employee.friends, friends)
                self.assertEqual(q, 2)

    def test_circular_reference(self):
        """Ensure you can handle circular references
        """
        class Person(Document):
            name = StringField()
            relations = ListField(EmbeddedDocumentField('Relation'))

            def __repr__(self):
                return "<Person: %s>" % self.name

        class Relation(EmbeddedDocument):
            name = StringField()
            person = ReferenceField('Person')

        Person.drop_collection()
        mother = Person(name="Mother")
        daughter = Person(name="Daughter")

        mother.save()
        daughter.save()

        daughter_rel = Relation(name="Daughter", person=daughter)
        mother.relations.append(daughter_rel)
        mother.save()

        mother_rel = Relation(name="Daughter", person=mother)
        self_rel = Relation(name="Self", person=daughter)
        daughter.relations.append(mother_rel)
        daughter.relations.append(self_rel)
        daughter.save()

        self.assertEquals("[<Person: Mother>, <Person: Daughter>]", "%s" % Person.objects())

    def test_circular_reference_on_self(self):
        """Ensure you can handle circular references
        """
        class Person(Document):
            name = StringField()
            relations = ListField(ReferenceField('self'))

            def __repr__(self):
                return "<Person: %s>" % self.name

        Person.drop_collection()
        mother = Person(name="Mother")
        daughter = Person(name="Daughter")

        mother.save()
        daughter.save()

        mother.relations.append(daughter)
        mother.save()

        daughter.relations.append(mother)
        daughter.relations.append(daughter)
        daughter.save()

        self.assertEquals("[<Person: Mother>, <Person: Daughter>]", "%s" % Person.objects())

    def test_circular_tree_reference(self):
        """Ensure you can handle circular references with more than one level
        """
        class Other(EmbeddedDocument):
            name = StringField()
            friends = ListField(ReferenceField('Person'))

        class Person(Document):
            name = StringField()
            other = EmbeddedDocumentField(Other, default=lambda: Other())

            def __repr__(self):
                return "<Person: %s>" % self.name

        Person.drop_collection()
        paul = Person(name="Paul")
        paul.save()
        maria = Person(name="Maria")
        maria.save()
        julia = Person(name='Julia')
        julia.save()
        anna = Person(name='Anna')
        anna.save()

        paul.other.friends = [maria, julia, anna]
        paul.other.name = "Paul's friends"
        paul.save()

        maria.other.friends = [paul, julia, anna]
        maria.other.name = "Maria's friends"
        maria.save()

        julia.other.friends = [paul, maria, anna]
        julia.other.name = "Julia's friends"
        julia.save()

        anna.other.friends = [paul, maria, julia]
        anna.other.name = "Anna's friends"
        anna.save()

        self.assertEquals(
            "[<Person: Paul>, <Person: Maria>, <Person: Julia>, <Person: Anna>]",
            "%s" % Person.objects()
        )

    def test_generic_reference(self):

        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = ListField(GenericReferenceField())

        UserA.drop_collection()
        UserB.drop_collection()
        UserC.drop_collection()
        Group.drop_collection()

        members = []
        for i in xrange(1, 51):
            a = UserA(name='User A %s' % i)
            a.save()

            b = UserB(name='User B %s' % i)
            b.save()

            c = UserC(name='User C %s' % i)
            c.save()

            members += [a, b, c]

        group = Group(members=members)
        group.save()

        group = Group(members=members)
        group.save()

        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first()
            self.assertEqual(q, 1)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            for m in group_obj.members:
                self.assertTrue('User' in m.__class__.__name__)

        # Document select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first().select_related()
            self.assertEqual(q, 4)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            for m in group_obj.members:
                self.assertTrue('User' in m.__class__.__name__)

        # Queryset select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_objs = Group.objects.select_related()
            self.assertEqual(q, 4)

            for group_obj in group_objs:
                [m for m in group_obj.members]
                self.assertEqual(q, 4)

                [m for m in group_obj.members]
                self.assertEqual(q, 4)

                for m in group_obj.members:
                    self.assertTrue('User' in m.__class__.__name__)

        UserA.drop_collection()
        UserB.drop_collection()
        UserC.drop_collection()
        Group.drop_collection()

    def test_list_field_complex(self):

        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = ListField()

        UserA.drop_collection()
        UserB.drop_collection()
        UserC.drop_collection()
        Group.drop_collection()

        members = []
        for i in xrange(1, 51):
            a = UserA(name='User A %s' % i)
            a.save()

            b = UserB(name='User B %s' % i)
            b.save()

            c = UserC(name='User C %s' % i)
            c.save()

            members += [a, b, c]

        group = Group(members=members)
        group.save()

        group = Group(members=members)
        group.save()

        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first()
            self.assertEqual(q, 1)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            for m in group_obj.members:
                self.assertTrue('User' in m.__class__.__name__)

        # Document select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first().select_related()
            self.assertEqual(q, 4)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            for m in group_obj.members:
                self.assertTrue('User' in m.__class__.__name__)

        # Queryset select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_objs = Group.objects.select_related()
            self.assertEqual(q, 4)

            for group_obj in group_objs:
                [m for m in group_obj.members]
                self.assertEqual(q, 4)

                [m for m in group_obj.members]
                self.assertEqual(q, 4)

                for m in group_obj.members:
                    self.assertTrue('User' in m.__class__.__name__)

        UserA.drop_collection()
        UserB.drop_collection()
        UserC.drop_collection()
        Group.drop_collection()

    def test_map_field_reference(self):

        class User(Document):
            name = StringField()

        class Group(Document):
            members = MapField(ReferenceField(User))

        User.drop_collection()
        Group.drop_collection()

        members = []
        for i in xrange(1, 51):
            user = User(name='user %s' % i)
            user.save()
            members.append(user)

        group = Group(members=dict([(str(u.id), u) for u in members]))
        group.save()

        group = Group(members=dict([(str(u.id), u) for u in members]))
        group.save()

        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first()
            self.assertEqual(q, 1)

            [m for m in group_obj.members]
            self.assertEqual(q, 2)

            for k, m in group_obj.members.iteritems():
                self.assertTrue(isinstance(m, User))

        # Document select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first().select_related()
            self.assertEqual(q, 2)

            [m for m in group_obj.members]
            self.assertEqual(q, 2)

            for k, m in group_obj.members.iteritems():
                self.assertTrue(isinstance(m, User))

       # Queryset select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_objs = Group.objects.select_related()
            self.assertEqual(q, 2)

            for group_obj in group_objs:
                [m for m in group_obj.members]
                self.assertEqual(q, 2)

                for k, m in group_obj.members.iteritems():
                    self.assertTrue(isinstance(m, User))

        User.drop_collection()
        Group.drop_collection()

    def test_dict_field(self):

        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = DictField()

        UserA.drop_collection()
        UserB.drop_collection()
        UserC.drop_collection()
        Group.drop_collection()

        members = []
        for i in xrange(1, 51):
            a = UserA(name='User A %s' % i)
            a.save()

            b = UserB(name='User B %s' % i)
            b.save()

            c = UserC(name='User C %s' % i)
            c.save()

            members += [a, b, c]

        group = Group(members=dict([(str(u.id), u) for u in members]))
        group.save()
        group = Group(members=dict([(str(u.id), u) for u in members]))
        group.save()

        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first()
            self.assertEqual(q, 1)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            for k, m in group_obj.members.iteritems():
                self.assertTrue('User' in m.__class__.__name__)

        # Document select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first().select_related()
            self.assertEqual(q, 4)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            for k, m in group_obj.members.iteritems():
                self.assertTrue('User' in m.__class__.__name__)

        # Queryset select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_objs = Group.objects.select_related()
            self.assertEqual(q, 4)

            for group_obj in group_objs:
                [m for m in group_obj.members]
                self.assertEqual(q, 4)

                [m for m in group_obj.members]
                self.assertEqual(q, 4)

                for k, m in group_obj.members.iteritems():
                    self.assertTrue('User' in m.__class__.__name__)

        Group.objects.delete()
        Group().save()

        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first()
            self.assertEqual(q, 1)

            [m for m in group_obj.members]
            self.assertEqual(q, 1)
            self.assertEqual(group_obj.members, {})

        UserA.drop_collection()
        UserB.drop_collection()
        UserC.drop_collection()
        Group.drop_collection()

    def test_dict_field_no_field_inheritance(self):

        class UserA(Document):
            name = StringField()
            meta = {'allow_inheritance': False}

        class Group(Document):
            members = DictField()

        UserA.drop_collection()
        Group.drop_collection()

        members = []
        for i in xrange(1, 51):
            a = UserA(name='User A %s' % i)
            a.save()

            members += [a]

        group = Group(members=dict([(str(u.id), u) for u in members]))
        group.save()

        group = Group(members=dict([(str(u.id), u) for u in members]))
        group.save()

        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first()
            self.assertEqual(q, 1)

            [m for m in group_obj.members]
            self.assertEqual(q, 2)

            [m for m in group_obj.members]
            self.assertEqual(q, 2)

            for k, m in group_obj.members.iteritems():
                self.assertTrue(isinstance(m, UserA))

        # Document select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first().select_related()
            self.assertEqual(q, 2)

            [m for m in group_obj.members]
            self.assertEqual(q, 2)

            [m for m in group_obj.members]
            self.assertEqual(q, 2)

            for k, m in group_obj.members.iteritems():
                self.assertTrue(isinstance(m, UserA))

        # Queryset select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_objs = Group.objects.select_related()
            self.assertEqual(q, 2)

            for group_obj in group_objs:
                [m for m in group_obj.members]
                self.assertEqual(q, 2)

                [m for m in group_obj.members]
                self.assertEqual(q, 2)

                for k, m in group_obj.members.iteritems():
                    self.assertTrue(isinstance(m, UserA))

        UserA.drop_collection()
        Group.drop_collection()

    def test_generic_reference_map_field(self):

        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = MapField(GenericReferenceField())

        UserA.drop_collection()
        UserB.drop_collection()
        UserC.drop_collection()
        Group.drop_collection()

        members = []
        for i in xrange(1, 51):
            a = UserA(name='User A %s' % i)
            a.save()

            b = UserB(name='User B %s' % i)
            b.save()

            c = UserC(name='User C %s' % i)
            c.save()

            members += [a, b, c]

        group = Group(members=dict([(str(u.id), u) for u in members]))
        group.save()
        group = Group(members=dict([(str(u.id), u) for u in members]))
        group.save()

        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first()
            self.assertEqual(q, 1)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            for k, m in group_obj.members.iteritems():
                self.assertTrue('User' in m.__class__.__name__)

        # Document select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first().select_related()
            self.assertEqual(q, 4)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            [m for m in group_obj.members]
            self.assertEqual(q, 4)

            for k, m in group_obj.members.iteritems():
                self.assertTrue('User' in m.__class__.__name__)

        # Queryset select_related
        with query_counter() as q:
            self.assertEqual(q, 0)

            group_objs = Group.objects.select_related()
            self.assertEqual(q, 4)

            for group_obj in group_objs:
                [m for m in group_obj.members]
                self.assertEqual(q, 4)

                [m for m in group_obj.members]
                self.assertEqual(q, 4)

                for k, m in group_obj.members.iteritems():
                    self.assertTrue('User' in m.__class__.__name__)

        Group.objects.delete()
        Group().save()

        with query_counter() as q:
            self.assertEqual(q, 0)

            group_obj = Group.objects.first()
            self.assertEqual(q, 1)

            [m for m in group_obj.members]
            self.assertEqual(q, 1)

        UserA.drop_collection()
        UserB.drop_collection()
        UserC.drop_collection()
        Group.drop_collection()

    def test_multidirectional_lists(self):

        class Asset(Document):
            name = StringField(max_length=250, required=True)
            parent = GenericReferenceField(default=None)
            parents = ListField(GenericReferenceField())
            children = ListField(GenericReferenceField())

        Asset.drop_collection()

        root = Asset(name='', path="/", title="Site Root")
        root.save()

        company = Asset(name='company', title='Company', parent=root, parents=[root])
        company.save()

        root.children = [company]
        root.save()

        root = root.reload()
        self.assertEquals(root.children, [company])
        self.assertEquals(company.parents, [root])
