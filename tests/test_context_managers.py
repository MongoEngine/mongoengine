import unittest

from mongoengine import *
from mongoengine.connection import get_db
from mongoengine.context_managers import (switch_db, switch_collection,
                                          no_sub_classes, no_dereference,
                                          query_counter)
from mongoengine.pymongo_support import count_documents


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
        register_connection(alias='testdb-1', db='mongoenginetest2')

        class Group(Document):
            name = StringField()

        Group.drop_collection()         # drops in default

        with switch_collection(Group, 'group1') as Group:
            Group.drop_collection()     # drops in group1

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

    def test_query_counter_does_not_swallow_exception(self):

        with self.assertRaises(TypeError):
            with query_counter() as q:
                raise TypeError()

    def test_query_counter_temporarily_modifies_profiling_level(self):
        connect('mongoenginetest')
        db = get_db()

        initial_profiling_level = db.profiling_level()

        try:
            NEW_LEVEL = 1
            db.set_profiling_level(NEW_LEVEL)
            self.assertEqual(db.profiling_level(), NEW_LEVEL)
            with query_counter() as q:
                self.assertEqual(db.profiling_level(), 2)
            self.assertEqual(db.profiling_level(), NEW_LEVEL)
        except Exception:
            db.set_profiling_level(initial_profiling_level)    # Ensures it gets reseted no matter the outcome of the test
            raise

    def test_query_counter(self):
        connect('mongoenginetest')
        db = get_db()

        collection = db.query_counter
        collection.drop()

        def issue_1_count_query():
            count_documents(collection, {})

        def issue_1_insert_query():
            collection.insert_one({'test': 'garbage'})

        def issue_1_find_query():
            collection.find_one()

        counter = 0
        with query_counter() as q:
            self.assertEqual(q, counter)
            self.assertEqual(q, counter)    # Ensures previous count query did not get counted

            for _ in range(10):
                issue_1_insert_query()
                counter += 1
            self.assertEqual(q, counter)

            for _ in range(4):
                issue_1_find_query()
                counter += 1
            self.assertEqual(q, counter)

            for _ in range(3):
                issue_1_count_query()
                counter += 1
            self.assertEqual(q, counter)

    def test_query_counter_counts_getmore_queries(self):
        connect('mongoenginetest')
        db = get_db()

        collection = db.query_counter
        collection.drop()

        many_docs = [{'test': 'garbage %s' % i} for i in range(150)]
        collection.insert_many(many_docs)   # first batch of documents contains 101 documents

        with query_counter() as q:
            self.assertEqual(q, 0)
            list(collection.find())
            self.assertEqual(q, 2)  # 1st select + 1 getmore

    def test_query_counter_ignores_particular_queries(self):
        connect('mongoenginetest')
        db = get_db()

        collection = db.query_counter
        collection.insert_many([{'test': 'garbage %s' % i} for i in range(10)])

        with query_counter() as q:
            self.assertEqual(q, 0)
            cursor = collection.find()
            self.assertEqual(q, 0)      # cursor wasn't opened yet
            _ = next(cursor)            # opens the cursor and fires the find query
            self.assertEqual(q, 1)

            cursor.close()              # issues a `killcursors` query that is ignored by the context
            self.assertEqual(q, 1)
            _ = db.system.indexes.find_one()    # queries on db.system.indexes are ignored as well
            self.assertEqual(q, 1)


if __name__ == '__main__':
    unittest.main()
