import sys
sys.path[0:0] = [""]
import unittest

from mongoengine import *
from mongoengine.connection import get_db
from mongoengine.context_managers import (switch_db, switch_collection,
                                          no_sub_classes, no_dereference,
                                          query_counter)


class ContextManagersTest(unittest.TestCase):

    def test_switch_db_context_manager(self):
        connect('mongoenginetest')
        register_connection('testdb-1', 'mongoenginetest2')

        class Group(Document):
            name = StringField()

        Group.drop_collection()

        Group(name="hello - default").save()
        self.assertEqual(1, Group.objects.count())

        with switch_db(Group, 'testdb-1') as Group:

            self.assertEqual(0, Group.objects.count())

            Group(name="hello").save()

            self.assertEqual(1, Group.objects.count())

            Group.drop_collection()
            self.assertEqual(0, Group.objects.count())

        self.assertEqual(1, Group.objects.count())

    def test_switch_collection_context_manager(self):
        connect('mongoenginetest')
        register_connection('testdb-1', 'mongoenginetest2')

        class Group(Document):
            name = StringField()

        Group.drop_collection()
        with switch_collection(Group, 'group1') as Group:
            Group.drop_collection()

        Group(name="hello - group").save()
        self.assertEqual(1, Group.objects.count())

        with switch_collection(Group, 'group1') as Group:

            self.assertEqual(0, Group.objects.count())

            Group(name="hello - group1").save()

            self.assertEqual(1, Group.objects.count())

            Group.drop_collection()
            self.assertEqual(0, Group.objects.count())

        self.assertEqual(1, Group.objects.count())

    def test_no_dereference_context_manager_object_id(self):
        """Ensure that DBRef items in ListFields aren't dereferenced.
        """
        connect('mongoenginetest')

        class User(Document):
            name = StringField()

        class Group(Document):
            ref = ReferenceField(User, dbref=False)
            generic = GenericReferenceField()
            members = ListField(ReferenceField(User, dbref=False))

        User.drop_collection()
        Group.drop_collection()

        for i in range(1, 51):
            User(name='user %s' % i).save()

        user = User.objects.first()
        Group(ref=user, members=User.objects, generic=user).save()

        with no_dereference(Group) as NoDeRefGroup:
            self.assertTrue(Group._fields['members']._auto_dereference)
            self.assertFalse(NoDeRefGroup._fields['members']._auto_dereference)

        with no_dereference(Group) as Group:
            group = Group.objects.first()
            self.assertTrue(all([not isinstance(m, User)
                                for m in group.members]))
            self.assertFalse(isinstance(group.ref, User))
            self.assertFalse(isinstance(group.generic, User))

        self.assertTrue(all([isinstance(m, User)
                             for m in group.members]))
        self.assertTrue(isinstance(group.ref, User))
        self.assertTrue(isinstance(group.generic, User))

    def test_no_dereference_context_manager_dbref(self):
        """Ensure that DBRef items in ListFields aren't dereferenced.
        """
        connect('mongoenginetest')

        class User(Document):
            name = StringField()

        class Group(Document):
            ref = ReferenceField(User, dbref=True)
            generic = GenericReferenceField()
            members = ListField(ReferenceField(User, dbref=True))

        User.drop_collection()
        Group.drop_collection()

        for i in range(1, 51):
            User(name='user %s' % i).save()

        user = User.objects.first()
        Group(ref=user, members=User.objects, generic=user).save()

        with no_dereference(Group) as NoDeRefGroup:
            self.assertTrue(Group._fields['members']._auto_dereference)
            self.assertFalse(NoDeRefGroup._fields['members']._auto_dereference)

        with no_dereference(Group) as Group:
            group = Group.objects.first()
            self.assertTrue(all([not isinstance(m, User)
                                for m in group.members]))
            self.assertFalse(isinstance(group.ref, User))
            self.assertFalse(isinstance(group.generic, User))

        self.assertTrue(all([isinstance(m, User)
                             for m in group.members]))
        self.assertTrue(isinstance(group.ref, User))
        self.assertTrue(isinstance(group.generic, User))

    def test_no_sub_classes(self):
        class A(Document):
            x = IntField()
            y = IntField()

            meta = {'allow_inheritance': True}

        class B(A):
            z = IntField()

        class C(B):
            zz = IntField()

        A.drop_collection()

        A(x=10, y=20).save()
        A(x=15, y=30).save()
        B(x=20, y=40).save()
        B(x=30, y=50).save()
        C(x=40, y=60).save()

        self.assertEqual(A.objects.count(), 5)
        self.assertEqual(B.objects.count(), 3)
        self.assertEqual(C.objects.count(), 1)

        with no_sub_classes(A) as A:
            self.assertEqual(A.objects.count(), 2)

            for obj in A.objects:
                self.assertEqual(obj.__class__, A)

        with no_sub_classes(B) as B:
            self.assertEqual(B.objects.count(), 2)

            for obj in B.objects:
                self.assertEqual(obj.__class__, B)

        with no_sub_classes(C) as C:
            self.assertEqual(C.objects.count(), 1)

            for obj in C.objects:
                self.assertEqual(obj.__class__, C)

        # Confirm context manager exit correctly
        self.assertEqual(A.objects.count(), 5)
        self.assertEqual(B.objects.count(), 3)
        self.assertEqual(C.objects.count(), 1)

    def test_query_counter(self):
        connect('mongoenginetest')
        db = get_db()
        db.test.find({})

        with query_counter() as q:
            self.assertEqual(0, q)

            for i in range(1, 51):
                db.test.find({}).count()

            self.assertEqual(50, q)

if __name__ == '__main__':
    unittest.main()
