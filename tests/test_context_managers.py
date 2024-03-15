import random
import time
import unittest
from threading import Thread

import pytest
from bson import DBRef

from mongoengine import *
from mongoengine.connection import get_db
from mongoengine.context_managers import (
    no_dereference,
    no_sub_classes,
    query_counter,
    run_in_transaction,
    set_read_write_concern,
    set_write_concern,
    switch_collection,
    switch_db,
)
from mongoengine.pymongo_support import count_documents
from mongoengine.sessions import get_local_session
from tests.utils import MongoDBTestCase


class TestableThread(Thread):
    """
    Wrapper around `threading.Thread` that propagates exceptions.

    REF: https://gist.github.com/sbrugman/59b3535ebcd5aa0e2598293cfa58b6ab
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exc = None

    def run(self):
        try:
            super().run()
        except BaseException as e:
            self.exc = e

    def join(self, timeout=None):
        super().join(timeout)
        if self.exc:
            raise self.exc


class TestContextManagers(MongoDBTestCase):
    def test_set_write_concern(self):
        class User(Document):
            name = StringField()

        collection = User._get_collection()
        original_write_concern = collection.write_concern

        with set_write_concern(
            collection, {"w": "majority", "j": True, "wtimeout": 1234}
        ) as updated_collection:
            assert updated_collection.write_concern.document == {
                "w": "majority",
                "j": True,
                "wtimeout": 1234,
            }

        assert original_write_concern.document == collection.write_concern.document

    def test_set_read_write_concern(self):
        class User(Document):
            name = StringField()

        collection = User._get_collection()

        original_read_concern = collection.read_concern
        original_write_concern = collection.write_concern

        with set_read_write_concern(
            collection,
            {"w": "majority", "j": True, "wtimeout": 1234},
            {"level": "local"},
        ) as update_collection:
            assert update_collection.read_concern.document == {"level": "local"}
            assert update_collection.write_concern.document == {
                "w": "majority",
                "j": True,
                "wtimeout": 1234,
            }

        assert original_read_concern.document == collection.read_concern.document
        assert original_write_concern.document == collection.write_concern.document

    def test_run_in_transaction_context_manager(self):
        connect("mongoenginetest")
        register_connection("testdb-1", "mongoenginetest2")

        class Group(Document):
            name = StringField()

        Group.drop_collection()

        Group(name="hello - outside").save()
        assert 1 == Group.objects.count()

        with run_in_transaction():
            Group(name="hello - inside").save()
            assert 2 == Group.objects.count()

        assert 2 == Group.objects.count()
        assert get_local_session() is None

        Group.drop_collection()

        try:
            with run_in_transaction():
                Group(name="hello - inside").save()
                assert 1 == Group.objects.count()

                raise Exception("test")
        except Exception as e:
            assert str(e) == "test"

        assert 0 == Group.objects.count()
        assert get_local_session() is None

    def test_run_in_transaction_context_decorator(self):
        connect("mongoenginetest")
        register_connection("testdb-1", "mongoenginetest2")

        class Group(Document):
            name = StringField()

        Group.drop_collection()

        @run_in_transaction()
        def create_group(name):
            Group(name=name).save()
            assert 1 == Group.objects.count()

        create_group("hello - inside")
        assert 1 == Group.objects.count()

        @run_in_transaction()
        def create_group_and_then_fail(name):
            Group(name=name).save()
            raise Exception("test")

        Group.drop_collection()

        try:
            create_group_and_then_fail("hello - inside")
        except Exception as e:
            assert str(e) == "test"

        assert 0 == Group.objects.count()
        assert get_local_session() is None

    def test_switch_db_context_manager(self):
        register_connection("testdb-1", "mongoenginetest2")

        class Group(Document):
            name = StringField()

        Group.drop_collection()

        Group(name="hello - default").save()
        assert 1 == Group.objects.count()

        with switch_db(Group, "testdb-1") as Group:
            assert 0 == Group.objects.count()

            Group(name="hello").save()

            assert 1 == Group.objects.count()

            Group.drop_collection()
            assert 0 == Group.objects.count()

        assert 1 == Group.objects.count()

    def test_switch_collection_context_manager(self):
        register_connection(alias="testdb-1", db="mongoenginetest2")

        class Group(Document):
            name = StringField()

        Group.drop_collection()  # drops in default

        with switch_collection(Group, "group1") as Group:
            Group.drop_collection()  # drops in group1

        Group(name="hello - group").save()
        assert 1 == Group.objects.count()

        with switch_collection(Group, "group1") as Group:
            assert 0 == Group.objects.count()

            Group(name="hello - group1").save()

            assert 1 == Group.objects.count()

            Group.drop_collection()
            assert 0 == Group.objects.count()

        assert 1 == Group.objects.count()

    def test_no_dereference_context_manager_object_id(self):
        """Ensure that DBRef items in ListFields aren't dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            ref = ReferenceField(User, dbref=False)
            generic = GenericReferenceField()
            members = ListField(ReferenceField(User, dbref=False))

        User.drop_collection()
        Group.drop_collection()

        for i in range(1, 51):
            User(name="user %s" % i).save()

        user = User.objects.first()
        Group(ref=user, members=User.objects, generic=user).save()

        with no_dereference(Group):
            assert not Group._fields["members"]._auto_dereference

        with no_dereference(Group):
            group = Group.objects.first()
            for m in group.members:
                assert isinstance(m, DBRef)
            assert isinstance(group.ref, DBRef)
            assert isinstance(group.generic, dict)

        group = Group.objects.first()
        for m in group.members:
            assert isinstance(m, User)
        assert isinstance(group.ref, User)
        assert isinstance(group.generic, User)

    def test_no_dereference_context_manager_nested(self):
        """Ensure that DBRef items in ListFields aren't dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            ref = ReferenceField(User, dbref=False)

        User.drop_collection()
        Group.drop_collection()

        for i in range(1, 51):
            User(name="user %s" % i).save()

        user = User.objects.first()
        Group(ref=user).save()

        with no_dereference(Group):
            group = Group.objects.first()
            assert isinstance(group.ref, DBRef)

            with no_dereference(Group):
                group = Group.objects.first()
                assert isinstance(group.ref, DBRef)

            # make sure it's still off here
            group = Group.objects.first()
            assert isinstance(group.ref, DBRef)

        group = Group.objects.first()
        assert isinstance(group.ref, User)

        def run_in_thread(id):
            time.sleep(random.uniform(0.1, 0.5))  # Force desync of threads
            if id % 2 == 0:
                with no_dereference(Group):
                    group = Group.objects.first()
                    assert isinstance(group.ref, DBRef)
            else:
                group = Group.objects.first()
                assert isinstance(group.ref, User)

        threads = [TestableThread(target=run_in_thread, args=(id,)) for id in range(10)]
        _ = [th.start() for th in threads]
        _ = [th.join() for th in threads]

    def test_no_dereference_context_manager_dbref(self):
        """Ensure that DBRef items in ListFields aren't dereferenced"""

        class User(Document):
            name = StringField()

        class Group(Document):
            ref = ReferenceField(User, dbref=True)
            generic = GenericReferenceField()
            members = ListField(ReferenceField(User, dbref=True))

        User.drop_collection()
        Group.drop_collection()

        for i in range(1, 51):
            User(name="user %s" % i).save()

        user = User.objects.first()
        Group(ref=user, members=User.objects, generic=user).save()

        with no_dereference(Group):
            assert not Group._fields["members"]._auto_dereference

        with no_dereference(Group):
            qs = Group.objects
            assert qs._auto_dereference is False
            group = qs.first()
            assert not group._fields["members"]._auto_dereference
            assert all(not isinstance(m, User) for m in group.members)
            assert not isinstance(group.ref, User)
            assert not isinstance(group.generic, User)

        group = Group.objects.first()
        assert all(isinstance(m, User) for m in group.members)
        assert isinstance(group.ref, User)
        assert isinstance(group.generic, User)

    def test_no_sub_classes(self):
        class A(Document):
            x = IntField()
            meta = {"allow_inheritance": True}

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

        assert A.objects.count() == 5
        assert B.objects.count() == 3
        assert C.objects.count() == 1

        with no_sub_classes(A):
            assert A.objects.count() == 2

            for obj in A.objects:
                assert obj.__class__ == A

        with no_sub_classes(B):
            assert B.objects.count() == 2

            for obj in B.objects:
                assert obj.__class__ == B

        with no_sub_classes(C):
            assert C.objects.count() == 1

            for obj in C.objects:
                assert obj.__class__ == C

        # Confirm context manager exit correctly
        assert A.objects.count() == 5
        assert B.objects.count() == 3
        assert C.objects.count() == 1

    def test_no_sub_classes_modification_to_document_class_are_temporary(self):
        class A(Document):
            x = IntField()
            meta = {"allow_inheritance": True}

        class B(A):
            z = IntField()

        assert A._subclasses == ("A", "A.B")
        with no_sub_classes(A):
            assert A._subclasses == ("A",)
        assert A._subclasses == ("A", "A.B")

        assert B._subclasses == ("A.B",)
        with no_sub_classes(B):
            assert B._subclasses == ("A.B",)
        assert B._subclasses == ("A.B",)

    def test_no_subclass_context_manager_does_not_swallow_exception(self):
        class User(Document):
            name = StringField()

        with pytest.raises(TypeError):
            with no_sub_classes(User):
                raise TypeError()

    def test_query_counter_does_not_swallow_exception(self):
        with pytest.raises(TypeError):
            with query_counter():
                raise TypeError()

    def test_query_counter_temporarily_modifies_profiling_level(self):
        db = get_db()

        def _current_profiling_level():
            return db.command({"profile": -1})["was"]

        def _set_profiling_level(lvl):
            db.command({"profile": lvl})

        initial_profiling_level = _current_profiling_level()

        try:
            new_level = 1
            _set_profiling_level(new_level)
            assert _current_profiling_level() == new_level
            with query_counter():
                assert _current_profiling_level() == 2
            assert _current_profiling_level() == new_level
        except Exception:
            _set_profiling_level(
                initial_profiling_level
            )  # Ensures it gets reseted no matter the outcome of the test
            raise

    def test_query_counter(self):
        db = get_db()

        collection = db.query_counter
        collection.drop()

        def issue_1_count_query():
            count_documents(collection, {})

        def issue_1_insert_query():
            collection.insert_one({"test": "garbage"})

        def issue_1_find_query():
            collection.find_one()

        counter = 0
        with query_counter() as q:
            assert q == counter
            assert q == counter  # Ensures previous count query did not get counted

            for _ in range(10):
                issue_1_insert_query()
                counter += 1
            assert q == counter

            for _ in range(4):
                issue_1_find_query()
                counter += 1
            assert q == counter

            for _ in range(3):
                issue_1_count_query()
                counter += 1
            assert q == counter

            assert int(q) == counter  # test __int__
            assert repr(q) == str(int(q))  # test __repr__
            assert q > -1  # test __gt__
            assert q >= int(q)  # test __gte__
            assert q != -1
            assert q < 1000
            assert q <= int(q)

    def test_query_counter_alias(self):
        """query_counter works properly with db aliases?"""
        # Register a connection with db_alias testdb-1
        register_connection("testdb-1", "mongoenginetest2")

        class A(Document):
            """Uses default db_alias"""

            name = StringField()

        class B(Document):
            """Uses testdb-1 db_alias"""

            name = StringField()
            meta = {"db_alias": "testdb-1"}

        A.drop_collection()
        B.drop_collection()

        with query_counter() as q:
            assert q == 0
            A.objects.create(name="A")
            assert q == 1
            a = A.objects.first()
            assert q == 2
            a.name = "Test A"
            a.save()
            assert q == 3
            # querying the other db should'nt alter the counter
            B.objects().first()
            assert q == 3

        with query_counter(alias="testdb-1") as q:
            assert q == 0
            B.objects.create(name="B")
            assert q == 1
            b = B.objects.first()
            assert q == 2
            b.name = "Test B"
            b.save()
            assert b.name == "Test B"
            assert q == 3
            # querying the other db should'nt alter the counter
            A.objects().first()
            assert q == 3

    def test_query_counter_counts_getmore_queries(self):
        db = get_db()

        collection = db.query_counter
        collection.drop()

        many_docs = [{"test": "garbage %s" % i} for i in range(150)]
        collection.insert_many(
            many_docs
        )  # first batch of documents contains 101 documents

        with query_counter() as q:
            assert q == 0
            list(collection.find())
            assert q == 2  # 1st select + 1 getmore

    def test_query_counter_ignores_particular_queries(self):
        db = get_db()

        collection = db.query_counter
        collection.insert_many([{"test": "garbage %s" % i} for i in range(10)])

        with query_counter() as q:
            assert q == 0
            cursor = collection.find()
            assert q == 0  # cursor wasn't opened yet
            _ = next(cursor)  # opens the cursor and fires the find query
            assert q == 1

            cursor.close()  # issues a `killcursors` query that is ignored by the context
            assert q == 1
            _ = (
                db.system.indexes.find_one()
            )  # queries on db.system.indexes are ignored as well
            assert q == 1


if __name__ == "__main__":
    unittest.main()
