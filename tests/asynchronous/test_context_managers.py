import asyncio
import logging
import random

import pytest
from pymongo.errors import OperationFailure, InvalidOperation

from mongoengine import *
from mongoengine.asynchronous import async_register_connection, async_get_db, async_connect
from mongoengine.session import _get_session
from mongoengine.context_managers import (
    no_sub_classes,
    set_read_write_concern,
    set_write_concern,
    switch_collection,
    switch_db, async_query_counter, run_in_transaction,
)
from mongoengine.pymongo_support import async_count_documents
from tests.asynchronous.utils import MongoDBAsyncTestCase
from tests.utils import (
    requires_mongodb_gte_44,
)


class TestRollbackError(Exception):
    __test__ = False  # Silence pytest warning


class TestContextManagers(MongoDBAsyncTestCase):
    async def test_set_write_concern(self):
        class User(Document):
            name = StringField()

        collection = await User._aget_collection()
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

    async def test_set_read_write_concern(self):
        class User(Document):
            name = StringField()

        collection = await User._aget_collection()

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

    async def test_switch_db_context_manager(self):
        await async_register_connection("testdb-1", "mongoenginetest2")

        class Group(Document):
            name = StringField()

        await Group.adrop_collection()
        with switch_db(Group, "testdb-1") as Group:
            await Group.adrop_collection()

        await Group(name="hello - default").asave()
        assert 1 == await Group.aobjects.count()

        with switch_db(Group, "testdb-1") as Group:
            assert 0 == await Group.aobjects.count()

            await Group(name="hello").asave()

            assert 1 == await Group.aobjects.count()

            await Group.adrop_collection()
            assert 0 == await Group.aobjects.count()

        assert 1 == await Group.aobjects.count()

    async def test_switch_collection_context_manager(self):
        await async_register_connection(alias="testdb-1", db="mongoenginetest2")

        class Group(Document):
            name = StringField()

        await Group.adrop_collection()  # drops in default

        with switch_collection(Group, "group1") as Group:
            await Group.adrop_collection()  # drops in group1

        await Group(name="hello - group").asave()
        assert 1 == await Group.aobjects.count()

        with switch_collection(Group, "group1") as Group:
            assert 0 == await Group.aobjects.count()

            await Group(name="hello - group1").asave()

            assert 1 == await Group.aobjects.count()

            await Group.adrop_collection()
            assert 0 == await Group.aobjects.count()

        assert 1 == await Group.aobjects.count()

    async def test_no_sub_classes(self):
        class A(Document):
            x = IntField()
            meta = {"allow_inheritance": True}

        class B(A):
            z = IntField()

        class C(B):
            zz = IntField()

        await A.adrop_collection()

        await A(x=10).asave()
        await A(x=15).asave()
        await B(x=20).asave()
        await B(x=30).asave()
        await C(x=40).asave()

        assert await A.aobjects.count() == 5
        assert await B.aobjects.count() == 3
        assert await C.aobjects.count() == 1

        with no_sub_classes(A):
            assert await A.aobjects.count() == 2

            async for obj in A.aobjects:
                assert obj.__class__ == A

        with no_sub_classes(B):
            assert await B.aobjects.count() == 2

            async for obj in B.aobjects:
                assert obj.__class__ == B

        with no_sub_classes(C):
            assert await C.aobjects.count() == 1

            async for obj in C.aobjects:
                assert obj.__class__ == C

        # Confirm context manager exit correctly
        assert await A.aobjects.count() == 5
        assert await B.aobjects.count() == 3
        assert await C.aobjects.count() == 1

    async def test_no_sub_classes_modification_to_document_class_are_temporary(self):
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

    async def test_no_subclass_context_manager_does_not_swallow_exception(self):
        class User(Document):
            name = StringField()

        with pytest.raises(TypeError):
            with no_sub_classes(User):
                raise TypeError()

    async def test_query_counter_does_not_swallow_exception(self):
        with pytest.raises(TypeError):
            async with async_query_counter():
                raise TypeError()

    async def test_query_counter_temporarily_modifies_profiling_level(self):
        db = await async_get_db()

        async def _current_profiling_level():
            return (await db.command({"profile": -1}))["was"]

        async def _set_profiling_level(lvl):
            await db.command({"profile": lvl})

        initial_profiling_level = await _current_profiling_level()

        try:
            new_level = 1
            await _set_profiling_level(new_level)
            assert await _current_profiling_level() == new_level
            async with async_query_counter():
                assert await _current_profiling_level() == 2
            assert await _current_profiling_level() == new_level
        except Exception:
            await _set_profiling_level(
                initial_profiling_level
            )  # Ensures it gets reseted no matter the outcome of the test
            raise

    async def test_query_counter(self):
        db = await async_get_db()

        collection = db.query_counter
        await collection.drop()

        async def issue_1_count_query():
            await async_count_documents(collection, {})

        async def issue_1_insert_query():
            await collection.insert_one({"test": "garbage"})

        async def issue_1_find_query():
            await collection.find_one()

        counter = 0
        async with async_query_counter() as q:
            assert await q.eq(counter)
            assert await q.eq(counter)  # Ensures previous count query did not get counted

            for _ in range(10):
                await issue_1_insert_query()
                counter += 1
            assert await q.eq(counter)

            for _ in range(4):
                await issue_1_find_query()
                counter += 1
            assert await q.eq(counter)

            for _ in range(3):
                await issue_1_count_query()
                counter += 1
            assert await q.eq(counter)

            assert await q.int() == counter  # test __int__
            assert await q.repr() == str(await q.int())  # test __repr__
            assert await q.gt(-1)  # test __gt__
            assert await q.ge(await q.int())  # test __gte__
            assert await q.ne(-1)
            assert await q.lt(1000)
            assert await q.le(await q.int())

    async def test_query_counter_alias(self):
        """query_counter works properly with db aliases?"""
        # Register a connection with db_alias testdb-1
        await async_register_connection("testdb-1", "mongoenginetest2")

        class A(Document):
            """Uses default db_alias"""

            name = StringField()

        class B(Document):
            """Uses testdb-1 db_alias"""

            name = StringField()
            meta = {"db_alias": "testdb-1"}

        await A.adrop_collection()
        await B.adrop_collection()

        async with async_query_counter() as q:
            assert await q.eq(0)
            await A.aobjects.create(name="A")
            assert await q.eq(1)
            a = await A.aobjects.first()
            assert await q.eq(2)
            a.name = "Test A"
            await a.asave()
            assert await q.eq(3)
            # querying the other db shouldn't alter the counter
            await B.aobjects().first()
            assert await q.eq(3)

        async with async_query_counter(alias="testdb-1") as q:
            assert await q.eq(0)
            await B.aobjects.create(name="B")
            assert await q.eq(1)
            b = await B.aobjects.first()
            assert await q.eq(2)
            b.name = "Test B"
            await b.asave()
            assert b.name == "Test B"
            assert await q.eq(3)
            # querying the other db shouldn't alter the counter
            await A.aobjects().first()
            assert await q.eq(3)

    async def test_query_counter_counts_getmore_queries(self):
        db = await async_get_db()

        collection = db.query_counter
        await collection.drop()

        many_docs = [{"test": "garbage %s" % i} for i in range(150)]
        await collection.insert_many(
            many_docs
        )  # the first batch of documents contains 101 documents

        async with async_query_counter() as q:
            assert await q.eq(0)
            await collection.find().to_list()
            assert await q.eq(2)  # 1st select + 1 getmore

    async def test_query_counter_ignores_particular_queries(self):
        db = await async_get_db()

        collection = db.query_counter
        await collection.insert_many([{"test": "garbage %s" % i} for i in range(10)])

        async with async_query_counter() as q:
            assert await q.eq(0)
            cursor = collection.find()
            assert await q.eq(0)  # cursor wasn't opened yet
            _ = await cursor.__anext__()  # opens the cursor and fires the find query
            assert await q.eq(1)

            await cursor.close()  # issues a `kill cursors` query ignored by the context
            assert await q.eq(1)
            _ = (
                await db.system.indexes.find_one()
            )  # queries on db.system.indexes are ignored as well
            assert await q.eq(1)

    async def test_updating_a_document_within_a_transaction(self):
        class A(Document):
            name = StringField()

        await A.adrop_collection()

        a_doc = await A.aobjects.create(name="a")

        async with run_in_transaction():
            await a_doc.aupdate(name="b")
            assert (await A.aobjects.get(id=a_doc.id)).name == "b"
            assert await A.aobjects.count() == 1

        assert await A.aobjects.count() == 1
        assert (await A.aobjects.get(id=a_doc.id)).name == "b"

    async def test_updating_a_document_within_a_transaction_that_fails(self):
        class A(Document):
            name = StringField()

        await A.adrop_collection()

        a_doc = await A.aobjects.create(name="a")

        with pytest.raises(TestRollbackError):
            async with run_in_transaction():
                await a_doc.aupdate(name="b")
                assert (await A.aobjects.get(id=a_doc.id)).name == "b"
                raise TestRollbackError()

        assert await A.aobjects.count() == 1
        assert (await A.aobjects.get(id=a_doc.id)).name == "a"

    async def test_creating_a_document_within_a_transaction(self):

        class A(Document):
            name = StringField()

        await A.adrop_collection()

        # ensure the collection is created (needed for transaction with MongoDB <= 4.2)
        await A.aobjects.create(name="test")
        await A.aobjects.delete()

        async with run_in_transaction():
            a_doc = await A.aobjects.create(name="a")
            another_doc = await A(name="b").asave()
            assert (await A.aobjects.get(id=a_doc.id)).name == "a"
            assert (await A.aobjects.get(id=another_doc.id)).name == "b"
            assert await A.aobjects.count() == 2

        assert await A.aobjects.count() == 2
        assert (await A.aobjects.get(id=a_doc.id)).name == "a"
        assert (await A.aobjects.get(id=another_doc.id)).name == "b"

    async def test_creating_a_document_within_a_transaction_that_fails(self):

        class A(Document):
            name = StringField()

        await A.adrop_collection()
        # ensure a collection is created (needed for transaction with MongoDB <= 4.2)
        await A.aobjects.create(name="test")
        await A.aobjects.delete()

        with pytest.raises(TestRollbackError):
            async with run_in_transaction():
                a_doc = await A.aobjects.create(name="a")
                another_doc = await A(name="b").asave()
                assert (await A.aobjects.get(id=a_doc.id)).name == "a"
                assert (await A.aobjects.get(id=another_doc.id)).name == "b"
                assert await A.aobjects.count() == 2
                raise TestRollbackError()

        assert await A.aobjects.count() == 0

    async def test_transaction_updates_across_databases(self):
        await async_connect("mongoenginetest")
        await async_connect("test2", "test2")

        class A(Document):
            name = StringField()

        await A.aobjects.all().delete()
        a_doc = await A.aobjects.create(name="a")

        class B(Document):
            meta = {"db_alias": "test2"}
            name = StringField()

        await B.aobjects.all().delete()
        b_doc = await B.aobjects.create(name="b")

        async with run_in_transaction():
            await a_doc.aupdate(name="a2")
            await b_doc.aupdate(name="b2")

        assert "a2" == (await A.aobjects.get(id=a_doc.id)).name
        assert "b2" == (await B.aobjects.get(id=b_doc.id)).name

    @requires_mongodb_gte_44
    async def test_collection_creation_via_upserts_across_databases_in_transaction(self):
        await async_connect("mongoenginetest")
        await async_connect("test2", "test2")

        class A(Document):
            name = StringField()

        await A.adrop_collection()

        a_doc = await A.aobjects.create(name="a")

        class B(Document):
            meta = {"db_alias": "test2"}
            name = StringField()

        await B.adrop_collection()

        b_doc = await B.aobjects.create(name="b")

        async with run_in_transaction():
            await a_doc.aupdate(name="a3")
            with switch_db(A, "test2"):
                await a_doc.aupdate(name="a4", upsert=True)
                await b_doc.aupdate(name="b3")
            await b_doc.aupdate(name="b4")

        assert "a3" == (await A.aobjects.get(id=a_doc.id)).name
        assert "b4" == (await B.aobjects.get(id=b_doc.id)).name
        with switch_db(A, "test2"):
            assert "a4" == (await A.aobjects.get(id=a_doc.id)).name

    async def test_an_exception_raised_in_transactions_across_databases_rolls_back_updates(
            self,
    ):
        await async_connect("mongoenginetest")
        await async_connect("test2", "test2")

        class A(Document):
            name = StringField()

        await A.adrop_collection()
        with switch_db(A, "test2"):
            await A.adrop_collection()

        a_doc = await A.aobjects.create(name="a")

        class B(Document):
            meta = {"db_alias": "test2"}
            name = StringField()

        await B.adrop_collection()

        b_doc = await B.aobjects.create(name="b")

        try:
            async with run_in_transaction():
                await a_doc.aupdate(name="a3")
                with switch_db(A, "test2"):
                    await a_doc.aupdate(name="a4", upsert=True)
                    await b_doc.aupdate(name="b3")
                    await b_doc.aupdate(name="b4")
                raise Exception
        except Exception:
            pass

        assert "a" == (await A.aobjects.get(id=a_doc.id)).name
        assert "b" == (await B.aobjects.get(id=b_doc.id)).name
        with switch_db(A, "test2"):
            assert 0 == await A.aobjects.all().count()

    async def test_exception_in_child_of_a_nested_transaction_rolls_parent_back(self):
        class A(Document):
            name = StringField()

        await A.adrop_collection()
        a_doc = await A.aobjects.create(name="a")

        class B(Document):
            name = StringField()

        await B.adrop_collection()
        b_doc = await B.aobjects.create(name="b")

        async def run_tx():
            try:
                async with run_in_transaction():
                    await a_doc.aupdate(name="trx-parent")
                    try:
                        async with run_in_transaction():
                            await b_doc.aupdate(name="trx-child")
                            raise TestRollbackError()
                    except TestRollbackError as exc:
                        # at this stage, the parent transaction is still there
                        assert (await A.aobjects.get(id=a_doc.id)).name == "trx-parent"
                        raise exc
            except OperationError as op_failure:
                """
                See thread safety test below for more details about TransientTransactionError handling
                """
                if "TransientTransactionError" in str(op_failure):
                    logging.warning("TransientTransactionError - retrying...")
                    await run_tx()
                else:
                    raise op_failure

        with pytest.raises(TestRollbackError):
            await run_tx()

        assert (await A.aobjects.get(id=a_doc.id)).name == "a"
        assert (await B.aobjects.get(id=b_doc.id)).name == "b"

    async def test_exception_in_parent_of_nested_transaction_after_child_completed_only_rolls_parent_back(
            self,
    ):
        class A(Document):
            name = StringField()

        await A.adrop_collection()
        a_doc = await A.aobjects.create(name="a")

        class B(Document):
            name = StringField()

        await B.adrop_collection()
        b_doc = await B.aobjects.create(name="b")

        async def run_tx():
            try:
                async with run_in_transaction():
                    await a_doc.aupdate(name="trx-parent")
                    async with run_in_transaction():
                        await b_doc.aupdate(name="trx-child")

                    raise TestRollbackError()

            except TestRollbackError:
                pass
            except OperationError as op_failure:
                """
                See thread safety test below for more details about TransientTransactionError handling
                """
                if "TransientTransactionError" in str(op_failure):
                    logging.warning("TransientTransactionError - retrying...")
                    await run_tx()
                else:
                    raise op_failure

        await run_tx()
        assert "a" == (await A.aobjects.get(id=a_doc.id)).name
        assert "trx-child" == (await B.aobjects.get(id=b_doc.id)).name

    async def test_nested_transactions_create_and_release_sessions_accordingly(self):
        async with run_in_transaction():
            s1 = _get_session()
            async with run_in_transaction():
                s2 = _get_session()
                assert s1 is not s2
                async with run_in_transaction():
                    pass
                assert _get_session() is s2
            assert _get_session() is s1

        assert _get_session() is None

    async def test_task_safety_of_transactions(self):
        """
        Async equivalent of the thread-safety test: ensure concurrent *tasks*
        using run_in_transaction() don't step over each other.

        NOTE: This tests task/context isolation (ContextVars), not thread isolation.
        """

        class A(Document):
            i = IntField(unique=True)

        await A.adrop_collection()
        _ = await A.aobjects.first()  # ensure collection exists

        task_count = 20

        async def worker(idx: int):
            # Open the transaction at some unknown interval
            await asyncio.sleep(random.uniform(0.1, 0.5))

            # Retry loop (instead of recursive retry)
            max_retries = 50
            for attempt in range(max_retries):
                try:
                    async with run_in_transaction():
                        a = await A.aobjects.get(i=idx)
                        a.i = idx * task_count

                        # Save at some unknown interval
                        await asyncio.sleep(random.uniform(0.1, 0.5))
                        await a.asave()

                        # Force rollbacks for the even runs...
                        if idx % 2 == 0:
                            raise TestRollbackError()

                    return  # success

                except TestRollbackError:
                    return  # rollback intended

                except OperationFailure as op_failure:
                    # Retry TransientTransactionError
                    labels = (op_failure.details or {}).get("errorLabels", [])
                    if "TransientTransactionError" in labels:
                        logging.warning(
                            "TransientTransactionError (idx=%s attempt=%s/%s) - retrying...",
                            idx, attempt + 1, max_retries,
                        )
                        await asyncio.sleep(0.01 * (attempt + 1))
                        continue
                    raise

                except (OperationError, InvalidOperation) as err:
                    # MongoEngine may wrap pymongo errors (OperationError loses labels/details)
                    msg = str(err)
                    if (
                            "TransientTransactionError" in msg
                            or "NoSuchTransaction" in msg
                            or "code 251" in msg
                            or "Cannot use ended session" in msg
                    ):
                        logging.warning(
                            "Transient/wrapped txn error (idx=%s attempt=%s/%s) - retrying...",
                            idx, attempt + 1, max_retries,
                        )
                        await asyncio.sleep(0.01 * (attempt + 1))
                        continue
                    raise

            raise AssertionError(f"Exceeded transient retries for idx={idx}")

        for _ in range(5):
            # Clear out the collection for a fresh run
            await A.aobjects.all().delete()

            # Prepopulate the data for reads
            for i in range(task_count):
                await A.aobjects.create(i=i)

            # Run workers concurrently (tasks, not threads)
            await asyncio.gather(*(worker(i) for i in range(task_count)))

            # Check the sum
            expected_sum = sum(i if i % 2 == 0 else i * task_count for i in range(task_count))
            assert expected_sum == 2090

            total = 0
            async for a in A.aobjects.all():
                total += a.i
            assert expected_sum == total
