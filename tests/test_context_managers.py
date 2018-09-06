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
            for m in group.members:
                self.assertNotIsInstance(m, User)
            self.assertNotIsInstance(group.ref, User)
            self.assertNotIsInstance(group.generic, User)

        for m in group.members:
            self.assertIsInstance(m, User)
        self.assertIsInstance(group.ref, User)
        self.assertIsInstance(group.generic, User)

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
            self.assertNotIsInstance(group.ref, User)
            self.assertNotIsInstance(group.generic, User)

        self.assertTrue(all([isinstance(m, User)
                             for m in group.members]))
        self.assertIsInstance(group.ref, User)
        self.assertIsInstance(group.generic, User)

    def test_no_sub_classes(self):
        class A(Document):
            x = IntField()
            meta = {'allow_inheritance': True}

        class B(A):
            z = IntField()

        class C(B):
            zz = IntField()

        A.drop_collection()

        A(x=10).save()
        A(x=15).save()
        B(x=20).save()
        B(x=30).save()
        C(x=40).save()

        self.assertEqual(A.objects.count(), 5)
        self.assertEqual(B.objects.count(), 3)
        self.assertEqual(C.objects.count(), 1)

        with no_sub_classes(A):
            self.assertEqual(A.objects.count(), 2)

            for obj in A.objects:
                self.assertEqual(obj.__class__, A)

        with no_sub_classes(B):
            self.assertEqual(B.objects.count(), 2)

            for obj in B.objects:
                self.assertEqual(obj.__class__, B)

        with no_sub_classes(C):
            self.assertEqual(C.objects.count(), 1)

            for obj in C.objects:
                self.assertEqual(obj.__class__, C)

        # Confirm context manager exit correctly
        self.assertEqual(A.objects.count(), 5)
        self.assertEqual(B.objects.count(), 3)
        self.assertEqual(C.objects.count(), 1)

    def test_no_sub_classes_modification_to_document_class_are_temporary(self):
        class A(Document):
            x = IntField()
            meta = {'allow_inheritance': True}

        class B(A):
            z = IntField()

        self.assertEqual(A._subclasses, ('A', 'A.B'))
        with no_sub_classes(A):
            self.assertEqual(A._subclasses, ('A',))
        self.assertEqual(A._subclasses, ('A', 'A.B'))

        self.assertEqual(B._subclasses, ('A.B',))
        with no_sub_classes(B):
            self.assertEqual(B._subclasses, ('A.B',))
        self.assertEqual(B._subclasses, ('A.B',))

    def test_no_subclass_context_manager_does_not_swallow_exception(self):
        class User(Document):
            name = StringField()

        with self.assertRaises(TypeError):
            with no_sub_classes(User):
                raise TypeError()

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
