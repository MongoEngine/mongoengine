import random
import threading
import time
import unittest

import pymongo
import pytest

from mongoengine import *
from mongoengine.connection import _get_session, get_db
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

from .utils import requires_mongodb_gte_40, requires_mongodb_gte_44


class TestContextManagers:
    def test_set_write_concern(self):
        connect("mongoenginetest")

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
        connect("mongoenginetest")

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

    def test_switch_db_context_manager(self):
        connect("mongoenginetest")
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
        connect("mongoenginetest")
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
        connect("mongoenginetest")

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

        with no_dereference(Group) as NoDeRefGroup:
            assert Group._fields["members"]._auto_dereference
            assert not NoDeRefGroup._fields["members"]._auto_dereference

        with no_dereference(Group) as Group:
            group = Group.objects.first()
            for m in group.members:
                assert not isinstance(m, User)
            assert not isinstance(group.ref, User)
            assert not isinstance(group.generic, User)

        for m in group.members:
            assert isinstance(m, User)
        assert isinstance(group.ref, User)
        assert isinstance(group.generic, User)

    def test_no_dereference_context_manager_dbref(self):
        """Ensure that DBRef items in ListFields aren't dereferenced."""
        connect("mongoenginetest")

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

        with no_dereference(Group) as NoDeRefGroup:
            assert Group._fields["members"]._auto_dereference
            assert not NoDeRefGroup._fields["members"]._auto_dereference

        with no_dereference(Group) as Group:
            group = Group.objects.first()
            assert all(not isinstance(m, User) for m in group.members)
            assert not isinstance(group.ref, User)
            assert not isinstance(group.generic, User)

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
        connect("mongoenginetest")
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
        connect("mongoenginetest")
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
        connect("mongoenginetest")
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
        connect("mongoenginetest")
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

    @requires_mongodb_gte_40
    def test_updating_a_document_within_a_transaction(self):
        connect("mongoenginetest")

        class A(Document):
            name = StringField()

        A.drop_collection()

        a_doc = A.objects.create(name="a")

        with run_in_transaction():
            a_doc.update(name="b")
            assert "b" == A.objects.get(id=a_doc.id).name

    @requires_mongodb_gte_40
    def test_transaction_updates_across_databases(self):
        connect("mongoenginetest")
        connect("test2", "test2")

        class A(Document):
            name = StringField()

        A.objects.all().delete()
        a_doc = A.objects.create(name="a")

        class B(Document):
            meta = {"db_alias": "test2"}
            name = StringField()

        B.objects.all().delete()
        b_doc = B.objects.create(name="b")

        with run_in_transaction():
            a_doc.update(name="a2")
            b_doc.update(name="b2")

        assert "a2" == A.objects.get(id=a_doc.id).name
        assert "b2" == B.objects.get(id=b_doc.id).name

    @requires_mongodb_gte_44
    def test_collection_creation_via_upsersts_across_databases_in_transaction(self):
        connect("mongoenginetest")
        connect("test2", "test2")

        class A(Document):
            name = StringField()

        A.drop_collection()

        a_doc = A.objects.create(name="a")

        class B(Document):
            meta = {"db_alias": "test2"}
            name = StringField()

        B.drop_collection()

        b_doc = B.objects.create(name="b")

        with run_in_transaction():
            a_doc.update(name="a3")
            with switch_db(A, "test2"):
                a_doc.update(name="a4", upsert=True)
                b_doc.update(name="b3")
            b_doc.update(name="b4")

        assert "a3" == A.objects.get(id=a_doc.id).name
        assert "b4" == B.objects.get(id=b_doc.id).name
        with switch_db(A, "test2"):
            assert "a4" == A.objects.get(id=a_doc.id).name

    @requires_mongodb_gte_40
    def test_an_exception_raised_in_transactions_across_databases_rolls_back_updates(
        self,
    ):
        connect("mongoenginetest")
        connect("test2", "test2")

        class A(Document):
            name = StringField()

        A.drop_collection()
        with switch_db(A, "test2"):
            A.drop_collection()

        a_doc = A.objects.create(name="a")

        class B(Document):
            meta = {"db_alias": "test2"}
            name = StringField()

        B.drop_collection()

        b_doc = B.objects.create(name="b")

        try:
            with run_in_transaction():
                a_doc.update(name="a3")
                with switch_db(A, "test2"):
                    a_doc.update(name="a4", upsert=True)
                    b_doc.update(name="b3")
                    b_doc.update(name="b4")
                raise Exception
        except Exception:
            pass

        assert "a" == A.objects.get(id=a_doc.id).name
        assert "b" == B.objects.get(id=b_doc.id).name
        with switch_db(A, "test2"):
            assert 0 == A.objects.all().count()

    @requires_mongodb_gte_40
    def test_exception_in_child_of_a_nested_transaction_rolls_parent_back(self):
        connect("mongoenginetest")

        class A(Document):
            name = StringField()

        A.drop_collection()
        a_doc = A.objects.create(name="a")

        class B(Document):
            name = StringField()

        B.drop_collection()
        b_doc = B.objects.create(name="b")

        try:
            with run_in_transaction():
                a_doc.update(name="trx-parent")
                with run_in_transaction():
                    b_doc.update(name="trx-child")
                    raise Exception
        except Exception:
            pass

        assert "a" == A.objects.get(id=a_doc.id).name
        assert "b" == B.objects.get(id=b_doc.id).name

    @requires_mongodb_gte_40
    def test_exception_in_parent_of_nested_transaction_after_child_completed_only_rolls_parent_back(
        self,
    ):
        connect("mongoenginetest")

        class A(Document):
            name = StringField()

        A.drop_collection()
        a_doc = A.objects.create(name="a")

        class B(Document):
            name = StringField()

        B.drop_collection()
        b_doc = B.objects.create(name="b")

        class TestExc(Exception):
            pass

        def run_tx():
            try:
                with run_in_transaction():
                    a_doc.update(name="trx-parent")
                    with run_in_transaction():
                        b_doc.update(name="trx-child")
                    raise TestExc
            except TestExc:
                pass
            except OperationError as op_failure:
                """
                See thread safety test below for more details about TransientTransctionError handling
                """
                if "TransientTransactionError" in str(op_failure):
                    run_tx()
                else:
                    raise op_failure

        run_tx()
        assert "a" == A.objects.get(id=a_doc.id).name
        assert "trx-child" == B.objects.get(id=b_doc.id).name

    @requires_mongodb_gte_40
    def test_nested_transactions_create_and_release_sessions_accordingly(self):
        connect("mongoenginetest")
        with run_in_transaction():
            s1 = _get_session()
            with run_in_transaction():
                s2 = _get_session()
                assert s1 != s2
                with run_in_transaction():
                    pass
                assert s2 == _get_session()
            assert s1 == _get_session()
        assert _get_session() is None

    @requires_mongodb_gte_40
    def test_thread_safety_of_transactions(self):
        """
        Make sure transactions don't step over each other. Each
        session should be isolated to each thread. If this is the
        case, then no amount of runtime variability should have
        an effect on the output.

        This test sets up 10 records, each with an integer field
        of value 0 - 9.

        We then spin up 10 threads and attempt to update a target
        record by multiplying it's integer value by 10. Then, if
        the target record is even, throw an exception, which
        should then roll the transaction back. The odd rows always
        succeed.

        If the sessions are properly thread safe, we should ALWAYS
        net out with the following sum across the integer fields
        of the 10 records:

        0 + 10 + 2 + 30 + 4 + 50 + 6 + 70 + 8 + 90 = 270
        """
        connect("mongoenginetest")

        class A(Document):
            i = IntField()

        # Ensure the collection is created
        A.objects.create(i=0)

        class TestExc(Exception):
            pass

        def thread_fn(idx):
            # Open the transaction at some unknown interval
            time.sleep(random.uniform(0.01, 0.1))
            try:
                with run_in_transaction():
                    a = A.objects.get(i=idx)
                    a.i = idx * 10
                    # Save at some unknown interval
                    time.sleep(random.uniform(0.01, 0.1))
                    a.save()

                    # Force roll backs for the even runs...
                    if idx % 2 == 0:
                        raise TestExc
            except TestExc:
                pass
            except pymongo.errors.OperationFailure as op_failure:
                """
                If there's a TransientTransactionError, retry - the lock could not be acquired.

                Per MongoDB docs: The core transaction API does not incorporate retry logic for
                "TransientTransactionError". To handle "TransientTransactionError", applications
                should explicitly incorporate retry logic for the error.

                See: https://www.mongodb.com/docs/manual/core/transactions-in-applications/#-transienttransactionerror-
                """
                error_labels = op_failure.details.get("errorLabels", [])
                if "TransientTransactionError" in error_labels:
                    thread_fn(idx)
                else:
                    raise op_failure

        for r in range(10):
            """
            Threads & randomization are tricky - run it multiple times
            """

            # Clear out the collection for a fresh run
            A.objects.all().delete()

            # Prepopulate the data for reads
            thread_count = 10
            for i in range(thread_count):
                A.objects.create(i=i)

            # Prime the threads
            threads = [
                threading.Thread(target=thread_fn, args=(i,))
                for i in range(thread_count)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            # Check the sum
            expected_sum = 0
            for i in range(thread_count):
                if i % 2 == 0:
                    expected_sum += i
                else:
                    expected_sum += i * 10
            assert expected_sum == 270
            assert expected_sum == sum(a.i for a in A.objects.all())


if __name__ == "__main__":
    unittest.main()
