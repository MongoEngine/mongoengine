import unittest

from bson import DBRef, ObjectId

from mongoengine import *
from mongoengine.asynchronous import (
    async_connect,
    async_register_connection,
    async_disconnect_all,
)
from mongoengine.context_managers import async_query_counter
from tests.asynchronous.utils import reset_async_connections
from tests.utils import MONGO_TEST_DB


class FieldTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db = await async_connect(db=MONGO_TEST_DB)

    async def asyncTearDown(self):
        await self.db.drop_database(MONGO_TEST_DB)
        await async_disconnect_all()
        await reset_async_connections()

    async def test_list_item_dereference(self):
        """Ensure that DBRef items in ListFields are dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            members = ListField(ReferenceField(User))

        await User.adrop_collection()
        await Group.adrop_collection()

        for i in range(1, 51):
            user = User(name="user %s" % i)
            await user.asave()

        group = Group(members=await User.aobjects.all().to_list())
        await group.asave()

        group = Group(members=await User.aobjects.all().to_list())
        await group.asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            len(group_obj._data["members"])
            assert await q.eq(1)

            len((await group_obj.aselect_related("members")).members)
            assert await q.eq(2)

            _ = [m for m in group_obj.members]
            assert await q.eq(2)

        # Document select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await (await Group.aobjects.first()).aselect_related("members")
            assert await q.eq(2)
            _ = [m for m in group_obj.members]
            assert await q.eq(2)

        # Queryset select_related
        async with async_query_counter() as q:
            assert await q.eq(0)
            group_objs = Group.aobjects.select_related("members")
            assert await q.eq(0)
            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]
                assert await q.eq(1)

        await User.adrop_collection()
        await Group.adrop_collection()

    async def test_list_item_dereference_dref_false(self):
        """Ensure that DBRef items in ListFields are dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            members = ListField(ReferenceField(User, dbref=False))

        await User.adrop_collection()
        await Group.adrop_collection()

        for i in range(1, 51):
            user = User(name="user %s" % i)
            await user.asave()

        group = Group(members=User.aobjects)
        await group.asave()
        await group.areload()  # Confirm reload works

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            _ = [m for m in (await group_obj.aselect_related("members")).members]
            assert await q.eq(2)
            assert group_obj._data["members"]

            # verifies that no additional queries gets executed
            # if we re-iterate over the ListField once it is
            # dereferenced
            _ = [m for m in group_obj.members]
            assert await q.eq(2)
            assert group_obj._data["members"]

        # Document select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await (await Group.aobjects.first()).aselect_related("members")

            assert await q.eq(2)
            _ = [m for m in group_obj.members]
            assert await q.eq(2)

        # Queryset select_related
        async with async_query_counter() as q:
            assert await q.eq(0)
            group_objs = Group.aobjects.select_related("members")
            assert await q.eq(0)
            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]
                assert await q.eq(1)

    async def test_list_item_dereference_orphan_dbref(self):
        """Ensure that orphan DBRef items in ListFields are dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            members = ListField(ReferenceField(User, dbref=False))

        await User.adrop_collection()
        await Group.adrop_collection()

        for i in range(1, 51):
            user = User(name="user %s" % i)
            await user.asave()

        group = Group(members=User.aobjects)
        await group.asave()
        await group.areload()  # Confirm reload works

        # Delete one User so one of the references in the
        # Group.members list is an orphan DBRef
        await (await User.aobjects.first()).adelete()
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            _ = [m for m in (await group_obj.aselect_related("members")).members]
            assert await q.eq(2)
            assert group_obj._data["members"]

            # verifies that no additional queries gets executed
            # if we re-iterate over the ListField once it is
            # dereferenced
            _ = [m for m in group_obj.members]
            assert await q.eq(2)
            assert group_obj._data["members"]

        await User.adrop_collection()
        await Group.adrop_collection()

    async def test_list_item_dereference_dref_false_stores_as_type(self):
        """Ensure that DBRef items are stored as their type"""

        class User(Document):
            my_id = IntField(primary_key=True)
            name = StringField()

        class Group(Document):
            members = ListField(ReferenceField(User, dbref=False))

        await User.adrop_collection()
        await Group.adrop_collection()

        user = await User(my_id=1, name="user 1").asave()

        await Group(members=User.aobjects).asave()
        group = await Group.aobjects.first()

        assert (await (await Group._aget_collection()).find_one())["members"] == [1]
        assert group.members == [user]

    async def test_handle_old_style_references(self):
        """Ensure that DBRef items in ListFields are dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            members = ListField(ReferenceField(User, dbref=True))

        await User.adrop_collection()
        await Group.adrop_collection()

        for i in range(1, 26):
            user = User(name="user %s" % i)
            await user.asave()

        group = Group(members=User.aobjects)
        await group.asave()

        group = await (await Group._aget_collection()).find_one()

        # Update the model to change the reference
        class Group(Document):
            members = ListField(ReferenceField(User, dbref=False))

        group = await Group.aobjects.first()
        group.members.append(await User(name="String!").asave())
        await group.asave()

        group = await Group.aobjects.select_related("members").first()
        assert group.members[0].name == "user 1"
        assert group.members[-1].name == "String!"

    async def test_migrate_references(self):
        """Example of migrating ReferenceField storage"""

        # Create some sample data
        class User(Document):
            name = StringField()

        class Group(Document):
            author = ReferenceField(User, dbref=True)
            members = ListField(ReferenceField(User, dbref=True))

        await User.adrop_collection()
        await Group.adrop_collection()

        user = await User(name="Ross").asave()
        group = await Group(author=user, members=[user]).asave()

        raw_data = await (await Group._aget_collection()).find_one()
        assert isinstance(raw_data["author"], DBRef)
        assert isinstance(raw_data["members"][0], DBRef)
        group = await Group.aobjects.select_related("author", "members").first()

        assert group.author == user
        assert group.members == [user]

        # Migrate the model definition
        class Group(Document):
            author = ReferenceField(User, dbref=False)
            members = ListField(ReferenceField(User, dbref=False))

        # Migrate the data
        async for g in Group.aobjects():
            # Explicitly mark as changed so resets
            g._mark_as_changed("author")
            g._mark_as_changed("members")
            await g.asave()

        group = await Group.aobjects.select_related("author", "members").first()
        assert group.author == user
        assert group.members == [user]

        raw_data = await (await Group._aget_collection()).find_one()
        assert isinstance(raw_data["author"], ObjectId)
        assert isinstance(raw_data["members"][0], ObjectId)

    async def test_recursive_reference(self):
        """Ensure that ReferenceFields can reference their own documents."""

        class Employee(Document):
            name = StringField()
            boss = ReferenceField("self")
            friends = ListField(ReferenceField("self"))

        await Employee.adrop_collection()

        bill = Employee(name="Bill Lumbergh")
        await bill.asave()

        michael = Employee(name="Michael Bolton")
        await michael.asave()

        samir = Employee(name="Samir Nagheenanajar")
        await samir.asave()

        friends = [michael, samir]
        peter = Employee(name="Peter Gibbons", boss=bill, friends=friends)
        await peter.asave()

        await Employee(name="Funky Gibbon", boss=bill, friends=friends).asave()
        await Employee(name="Funky Gibbon", boss=bill, friends=friends).asave()
        await Employee(name="Funky Gibbon", boss=bill, friends=friends).asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            peter = await Employee.aobjects.select_related("boss", "friends").with_id(
                peter.id
            )
            assert await q.eq(1)

            peter.boss
            assert await q.eq(1)

            peter.friends
            assert await q.eq(1)

        # Document select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            peter = await (await Employee.aobjects.with_id(peter.id)).aselect_related(
                "boss", "friends"
            )
            assert await q.eq(2)

            assert peter.boss == bill
            assert await q.eq(2)

            assert peter.friends == friends
            assert await q.eq(2)

        # Queryset select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            employees = Employee.aobjects(boss=bill).select_related("boss", "friends")
            assert await q.eq(0)

            async for employee in employees:
                assert employee.boss == bill
                assert await q.eq(1)

                assert employee.friends == friends
                assert await q.eq(1)

    async def test_list_of_lists_of_references(self):
        class User(Document):
            name = StringField()

        class Post(Document):
            user_lists = ListField(ListField(ReferenceField(User)))

        class SimpleList(Document):
            users = ListField(ReferenceField(User))

        await User.adrop_collection()
        await Post.adrop_collection()
        await SimpleList.adrop_collection()

        u1 = await User.aobjects.create(name="u1")
        u2 = await User.aobjects.create(name="u2")
        u3 = await User.aobjects.create(name="u3")

        await SimpleList.aobjects.create(users=[u1, u2, u3])
        assert (
            await SimpleList.aobjects.all().select_related("users").first()
        ).users == [u1, u2, u3]

        await Post.aobjects.create(user_lists=[[u1, u2], [u3]])
        assert (
            await Post.aobjects.all().select_related("user_lists").first()
        ).user_lists == [[u1, u2], [u3]]

    async def test_circular_reference(self):
        """Ensure you can handle circular references"""

        class Relation(EmbeddedDocument):
            name = StringField()
            person = ReferenceField("Person")

        class Person(Document):
            name = StringField()
            relations = ListField(EmbeddedDocumentField("Relation"))

            def __repr__(self):
                return "<Person: %s>" % self.name

        await Person.adrop_collection()
        mother = Person(name="Mother")
        daughter = Person(name="Daughter")

        await mother.asave()
        await daughter.asave()

        daughter_rel = Relation(name="Daughter", person=daughter)
        mother.relations.append(daughter_rel)
        await mother.asave()

        mother_rel = Relation(name="Daughter", person=mother)
        self_rel = Relation(name="Self", person=daughter)
        daughter.relations.append(mother_rel)
        daughter.relations.append(self_rel)
        await daughter.asave()

        assert (
            "[<Person: Mother>, <Person: Daughter>]"
            == "%s" % await Person.aobjects().to_list()
        )

    async def test_circular_reference_on_self(self):
        """Ensure you can handle circular references"""

        class Person(Document):
            name = StringField()
            relations = ListField(ReferenceField("self"))

            def __repr__(self):
                return "<Person: %s>" % self.name

        await Person.adrop_collection()
        mother = Person(name="Mother")
        daughter = Person(name="Daughter")

        await mother.asave()
        await daughter.asave()

        mother.relations.append(daughter)
        await mother.asave()

        daughter.relations.append(mother)
        daughter.relations.append(daughter)
        assert daughter._get_changed_fields() == ["relations"]
        await daughter.asave()

        assert (
            "[<Person: Mother>, <Person: Daughter>]"
            == "%s" % await Person.aobjects().to_list()
        )

    async def test_circular_tree_reference(self):
        """Ensure you can handle circular references with more than one level"""

        class Other(EmbeddedDocument):
            name = StringField()
            friends = ListField(ReferenceField("Person"))

        class Person(Document):
            name = StringField()
            other = EmbeddedDocumentField(Other, default=lambda: Other())

            def __repr__(self):
                return "<Person: %s>" % self.name

        await Person.adrop_collection()
        paul = await Person(name="Paul").asave()
        maria = await Person(name="Maria").asave()
        julia = await Person(name="Julia").asave()
        anna = await Person(name="Anna").asave()

        paul.other.friends = [maria, julia, anna]
        paul.other.name = "Paul's friends"
        await paul.asave()

        maria.other.friends = [paul, julia, anna]
        maria.other.name = "Maria's friends"
        await maria.asave()

        julia.other.friends = [paul, maria, anna]
        julia.other.name = "Julia's friends"
        await julia.asave()

        anna.other.friends = [paul, maria, julia]
        anna.other.name = "Anna's friends"
        await anna.asave()

        assert (
            "[<Person: Paul>, <Person: Maria>, <Person: Julia>, <Person: Anna>]"
            == "%s" % await Person.aobjects().to_list()
        )

    async def test_generic_reference(self):
        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = ListField(
                GenericReferenceField(
                    choices=(
                        UserA,
                        UserB,
                        UserC,
                    )
                )
            )

        await UserA.adrop_collection()
        await UserB.adrop_collection()
        await UserC.adrop_collection()
        await Group.adrop_collection()

        members = []
        for i in range(1, 51):
            a = UserA(name="User A %s" % i)
            await a.asave()

            b = UserB(name="User B %s" % i)
            await b.asave()

            c = UserC(name="User C %s" % i)
            await c.asave()

            members += [a, b, c]

        group = Group(members=members)
        await group.asave()

        group = Group(members=members)
        await group.asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            _ = [m for m in group_obj.members]
            assert await q.eq(1)

            for m in group_obj.members:
                assert "User" in m["_cls"]

        # Document select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await (await Group.aobjects.first()).aselect_related("members")
            assert await q.eq(2)

            _ = [m for m in group_obj.members]
            assert await q.eq(2)

            for m in group_obj.members:
                assert "User" in m.__class__.__name__

        # Queryset select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_objs = await Group.aobjects.select_related("members").to_list()
            assert await q.eq(1)

            for group_obj in group_objs:
                _ = [m for m in group_obj.members]
                assert await q.eq(1)

                for m in group_obj.members:
                    assert "User" in m.__class__.__name__

    async def test_generic_reference_orphan_dbref(self):
        """Ensure that generic orphan DBRef items in ListFields are dereferenced."""

        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = ListField(
                GenericReferenceField(
                    choices=(
                        UserA,
                        UserB,
                        UserC,
                    )
                )
            )

        await UserA.adrop_collection()
        await UserB.adrop_collection()
        await UserC.adrop_collection()
        await Group.adrop_collection()

        members = []
        for i in range(1, 51):
            a = UserA(name="User A %s" % i)
            await a.asave()

            b = UserB(name="User B %s" % i)
            await b.asave()

            c = UserC(name="User C %s" % i)
            await c.asave()

            members += [a, b, c]

        group = Group(members=members)
        await group.asave()

        # Delete one UserA instance so that there is
        # an orphan DBRef in the GenericReference ListField
        user = await UserA.aobjects.first()
        await user.adelete()
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.select_related("members").first()
            assert await q.eq(1)

            for m in group_obj.members:
                if not isinstance(
                    m,
                    (
                        UserA,
                        UserB,
                        UserC,
                    ),
                ):
                    assert m == {
                        "_cls": "UserA",
                        "_missing_reference": True,
                        "_ref": DBRef("user_a", user.pk),
                    }
            assert await q.eq(1)
            assert group_obj._data["members"]

        await UserA.adrop_collection()
        await UserB.adrop_collection()
        await UserC.adrop_collection()
        await Group.adrop_collection()

    async def test_list_field_complex(self):
        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = ListField(
                GenericReferenceField(
                    choices=(
                        UserA,
                        UserB,
                        UserC,
                    )
                )
            )

        await UserA.adrop_collection()
        await UserB.adrop_collection()
        await UserC.adrop_collection()
        await Group.adrop_collection()

        members = []
        for i in range(1, 51):
            a = UserA(name="User A %s" % i)
            await a.asave()

            b = UserB(name="User B %s" % i)
            await b.asave()

            c = UserC(name="User C %s" % i)
            await c.asave()

            members += [a, b, c]

        group = Group(members=members)
        await group.asave()

        group = Group(members=members)
        await group.asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            _ = [m for m in group_obj.members]
            assert await q.eq(1)

            for m in group_obj.members:
                assert "User" in m["_cls"]

        # Document select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await (await Group.aobjects.first()).aselect_related("members")
            assert await q.eq(2)

            _ = [m for m in group_obj.members]
            assert await q.eq(2)

            for m in group_obj.members:
                assert "User" in m.__class__.__name__

        # Queryset select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_objs = await Group.aobjects.select_related("members").to_list()
            assert await q.eq(1)

            for group_obj in group_objs:
                _ = [m for m in group_obj.members]
                assert await q.eq(1)

                for m in group_obj.members:
                    assert "User" in m.__class__.__name__

        await UserA.adrop_collection()
        await UserB.adrop_collection()
        await UserC.adrop_collection()
        await Group.adrop_collection()

    async def test_map_field_reference(self):
        class User(Document):
            name = StringField()

        class Group(Document):
            members = MapField(ReferenceField(User))

        await User.adrop_collection()
        await Group.adrop_collection()

        members = []
        for i in range(1, 51):
            user = User(name="user %s" % i)
            await user.asave()
            members.append(user)

        group = Group(members={str(u.id): u for u in members})
        await group.asave()

        group = Group(members={str(u.id): u for u in members})
        await group.asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            _ = [m for m in group_obj.members]
            assert await q.eq(1)

            for _, m in group_obj.members.items():
                assert "User" in m.document_type.__name__

        # Document select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await (await Group.aobjects.first()).aselect_related("members")
            assert await q.eq(2)

            _ = [m for m in group_obj.members]
            assert await q.eq(2)

            for k, m in group_obj.members.items():
                assert isinstance(m, User)

        # Queryset select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_objs = Group.aobjects.select_related("members")
            assert await q.eq(0)

            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]
                assert await q.eq(1)

                for k, m in group_obj.members.items():
                    assert isinstance(m, User)

        await User.adrop_collection()
        await Group.adrop_collection()

    async def test_dict_field(self):
        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = DictField(
                GenericReferenceField(
                    choices=(
                        UserA,
                        UserB,
                        UserC,
                    )
                )
            )

        await UserA.adrop_collection()
        await UserB.adrop_collection()
        await UserC.adrop_collection()
        await Group.adrop_collection()

        members = []
        for i in range(1, 51):
            a = UserA(name="User A %s" % i)
            await a.asave()

            b = UserB(name="User B %s" % i)
            await b.asave()

            c = UserC(name="User C %s" % i)
            await c.asave()

            members += [a, b, c]

        group = Group(members={str(u.id): u for u in members})
        await group.asave()
        group = Group(members={str(u.id): u for u in members})
        await group.asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            _ = [m for m in group_obj.members]
            assert await q.eq(1)

            _ = [m for m in group_obj.members]
            assert await q.eq(1)

            for k, m in group_obj.members.items():
                assert "User" in m["_cls"]

        # Document select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)
            await group_obj.aselect_related("members")
            assert await q.eq(2)

            _ = [m for m in group_obj.members]
            assert await q.eq(2)

            for k, m in group_obj.members.items():
                assert "User" in m.__class__.__name__

        # Queryset select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_objs = Group.aobjects.select_related("members")
            assert await q.eq(0)

            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]
                assert await q.eq(1)

                _ = [m for m in group_obj.members]
                assert await q.eq(1)

                for k, m in group_obj.members.items():
                    assert "User" in m.__class__.__name__

        await Group.aobjects.delete()
        await Group().asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            _ = [m for m in group_obj.members]
            assert await q.eq(1)
            assert group_obj.members == {}

        await UserA.adrop_collection()
        await UserB.adrop_collection()
        await UserC.adrop_collection()
        await Group.adrop_collection()

    async def test_dict_field_no_field_inheritance(self):
        class UserA(Document):
            name = StringField()
            meta = {"allow_inheritance": False}

        class Group(Document):
            members = DictField(ReferenceField(UserA))

        await UserA.adrop_collection()
        await Group.adrop_collection()

        members = []
        for i in range(1, 51):
            a = UserA(name="User A %s" % i)
            await a.asave()

            members += [a]

        group = Group(members={str(u.id): u for u in members})
        await group.asave()

        group = Group(members={str(u.id): u for u in members})
        await group.asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            _ = [m for m in group_obj.members]
            assert await q.eq(1)

            group_obj = await Group.aobjects.first()

            for k, m in group_obj.members.items():
                assert "User" in m.document_type.__name__

        # Document select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await (await Group.aobjects.first()).aselect_related("members")
            assert await q.eq(2)

            _ = [m for m in group_obj.members]
            assert await q.eq(2)

            for k, m in group_obj.members.items():
                assert isinstance(m, UserA)

        # Queryset select_related
        async with async_query_counter() as q:
            assert await q.eq(0)

            group_objs = await Group.aobjects.select_related("members").to_list()
            assert await q.eq(1)

            for group_obj in group_objs:
                _ = [m for m in group_obj.members]
                assert await q.eq(1)

                for _, m in group_obj.members.items():
                    assert isinstance(m, UserA)

        await UserA.adrop_collection()
        await Group.adrop_collection()

    async def test_generic_reference_map_field(self):
        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            members = MapField(
                GenericReferenceField(
                    choices=(
                        UserA,
                        UserB,
                        UserC,
                    )
                )
            )

        await UserA.adrop_collection()
        await UserB.adrop_collection()
        await UserC.adrop_collection()
        await Group.adrop_collection()

        members = []
        for i in range(1, 51):
            a = UserA(name="User A %s" % i)
            await a.asave()

            b = UserB(name="User B %s" % i)
            await b.asave()

            c = UserC(name="User C %s" % i)
            await c.asave()

            members += [a, b, c]

        group = Group(members={str(u.id): u for u in members})
        await group.asave()
        group = Group(members={str(u.id): u for u in members})
        await group.asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            _ = [m for m in group_obj.members]
            assert await q.eq(1)

            for _, m in group_obj.members.items():
                assert "User" in m.document_type.__name__

        # Document select_related
        async with async_query_counter() as q:
            assert await q.eq(0)
            group_obj = await Group.aobjects.first()
            assert await q.eq(1)
            await group_obj.aselect_related("members")
            assert await q.eq(2)

            _ = [m for m in group_obj.members]
            assert await q.eq(2)

            for _, m in group_obj.members.items():
                assert "User" in m.__class__.__name__

        # Queryset select_related
        async with async_query_counter() as q:
            assert await q.eq(0)
            group_objs = Group.aobjects.select_related("members")
            assert await q.eq(0)

            async for group_obj in group_objs:
                _ = [m for m in group_obj.members]
                assert await q.eq(1)

                for _, m in group_obj.members.items():
                    assert "User" in m.__class__.__name__

        await Group.aobjects.delete()
        await Group().asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            _ = [m for m in group_obj.members]
            assert await q.eq(1)

        await UserA.adrop_collection()
        await UserB.adrop_collection()
        await UserC.adrop_collection()
        await Group.adrop_collection()

    async def test_multidirectional_lists(self):
        class Asset(Document):
            name = StringField(max_length=250, required=True)
            path = StringField()
            title = StringField()
            parent = GenericReferenceField(default=None, choices=("Self",))
            parents = ListField(GenericReferenceField(choices=("Self",)))
            children = ListField(GenericReferenceField(choices=("Self",)))

        await Asset.adrop_collection()

        root = Asset(name="", path="/", title="Site Root")
        await root.asave()

        company = Asset(name="company", title="Company", parent=root, parents=[root])
        await company.asave()

        root.children = [company]
        await root.asave()

        root = await root.aselect_related("children")
        assert root.children == [company]
        assert company.parents == [root]

    async def test_dict_in_dbref_instance(self):
        class Person(Document):
            name = StringField(max_length=250, required=True)

        class Room(Document):
            number = StringField(max_length=250, required=True)
            staffs_with_position = ListField(DictField())

        await Person.adrop_collection()
        await Room.adrop_collection()

        bob = await Person.aobjects.create(name="Bob")
        await bob.asave()
        sarah = await Person.aobjects.create(name="Sarah")
        await sarah.asave()

        room_101 = await Room.aobjects.create(number="101")
        room_101.staffs_with_position = [
            {"position_key": "window", "staff": sarah},
            {"position_key": "door", "staff": bob.to_dbref()},
        ]
        await room_101.asave()

        room = await Room.aobjects.first()
        assert room.staffs_with_position[0]["staff"]["_ref"].id == sarah.pk
        assert room.staffs_with_position[1]["staff"].id == bob.pk

    async def test_document_reload_no_inheritance(self):
        class Foo(Document):
            meta = {"allow_inheritance": False}
            bar = ReferenceField("Bar")
            baz = ReferenceField("Baz")

        class Bar(Document):
            meta = {"allow_inheritance": False}
            msg = StringField(required=True, default="Blammo!")

        class Baz(Document):
            meta = {"allow_inheritance": False}
            msg = StringField(required=True, default="Kaboom!")

        await Foo.adrop_collection()
        await Bar.adrop_collection()
        await Baz.adrop_collection()

        bar = Bar()
        await bar.asave()
        baz = Baz()
        await baz.asave()
        foo = Foo()
        foo.bar = bar
        foo.baz = baz
        await foo.asave()
        await foo.aselect_related("bar", "baz")

        assert isinstance(foo.bar, Bar)
        assert isinstance(foo.baz, Baz)

    async def test_document_reload_reference_integrity(self):
        """
        Ensure reloading a document with multiple similar id
        in different collections doesn't mix them.
        """

        class Topic(Document):
            id = IntField(primary_key=True)

        class User(Document):
            id = IntField(primary_key=True)
            name = StringField()

        class Message(Document):
            id = IntField(primary_key=True)
            topic = ReferenceField(Topic)
            author = ReferenceField(User)

        await Topic.adrop_collection()
        await User.adrop_collection()
        await Message.adrop_collection()

        # All objects share the same id, but each in a different collection
        topic = await Topic(id=1).asave()
        user = await User(id=1, name="user-name").asave()
        await Message(id=1, topic=topic, author=user).asave()

        concurrent_change_user = await User.aobjects.get(id=1)
        concurrent_change_user.name = "new-name"
        await concurrent_change_user.asave()
        assert user.name != "new-name"

        msg = await Message.aobjects.get(id=1)
        await msg.aselect_related("author")
        assert msg.topic == topic
        assert msg.author == user
        assert msg.author.name == "new-name"

    async def test_list_lookup_not_checked_in_map(self):
        """Ensure we dereference list data correctly"""

        class Comment(Document):
            id = IntField(primary_key=True)
            text = StringField()

        class Message(Document):
            id = IntField(primary_key=True)
            comments = ListField(ReferenceField(Comment))

        await Comment.adrop_collection()
        await Message.adrop_collection()

        c1 = await Comment(id=0, text="zero").asave()
        c2 = await Comment(id=1, text="one").asave()
        await Message(id=1, comments=[c1, c2]).asave()

        msg = await Message.aobjects.get(id=1)
        assert 0 == msg.comments[0].id
        assert 1 == msg.comments[1].id

    async def test_list_item_dereference_dref_false_save_doesnt_cause_extra_queries(
        self,
    ):
        """Ensure that DBRef items in ListFields are dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            name = StringField()
            members = ListField(ReferenceField(User, dbref=False))

        await User.adrop_collection()
        await Group.adrop_collection()

        for i in range(1, 51):
            await User(name="user %s" % i).asave()

        await Group(name="Test", members=User.aobjects).asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            group_obj.name = "new test"
            await group_obj.asave()

            assert await q.eq(2)

    async def test_list_item_dereference_dref_true_save_doesnt_cause_extra_queries(
        self,
    ):
        """Ensure that DBRef items in ListFields are dereferenced."""

        class User(Document):
            name = StringField()

        class Group(Document):
            name = StringField()
            members = ListField(ReferenceField(User, dbref=True))

        await User.adrop_collection()
        await Group.adrop_collection()

        for i in range(1, 51):
            await User(name="user %s" % i).asave()

        await Group(name="Test", members=User.aobjects).asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            group_obj.name = "new test"
            await group_obj.asave()

            assert await q.eq(2)

    async def test_generic_reference_save_doesnt_cause_extra_queries(self):
        class UserA(Document):
            name = StringField()

        class UserB(Document):
            name = StringField()

        class UserC(Document):
            name = StringField()

        class Group(Document):
            name = StringField()
            members = ListField(
                GenericReferenceField(
                    choices=(
                        UserA,
                        UserB,
                        UserC,
                    )
                )
            )

        await UserA.adrop_collection()
        await UserB.adrop_collection()
        await UserC.adrop_collection()
        await Group.adrop_collection()

        members = []
        for i in range(1, 51):
            a = await UserA(name="User A %s" % i).asave()
            b = await UserB(name="User B %s" % i).asave()
            c = await UserC(name="User C %s" % i).asave()

            members += [a, b, c]

        await Group(name="test", members=members).asave()

        async with async_query_counter() as q:
            assert await q.eq(0)

            group_obj = await Group.aobjects.first()
            assert await q.eq(1)

            group_obj.name = "new test"
            await group_obj.asave()

            assert await q.eq(2)

    async def test_objectid_reference_across_databases(self):
        # mongoenginetest - Is default connection alias from setUp()
        # Register Aliases
        await async_register_connection("testdb-1", f"{MONGO_TEST_DB}_2")

        class User(Document):
            name = StringField()
            meta = {"db_alias": "testdb-1"}

        class Book(Document):
            name = StringField()
            author = ReferenceField(User)

        # Drops
        await User.adrop_collection()
        await Book.adrop_collection()

        user = await User(name="Ross").asave()
        await Book(name="MongoEngine for pros", author=user).asave()

        # Can't use query_counter across databases - so test the _data object
        book = await Book.aobjects.first()
        assert not isinstance(book._data["author"], User)

        assert isinstance(await book.author.afetch(), User)

    async def test_non_ascii_pk(self):
        """
        Ensure that dbref conversion to string does not fail when
        non-ascii characters are used in primary key
        """

        class Brand(Document):
            title = StringField(max_length=255, primary_key=True)

        class BrandGroup(Document):
            title = StringField(max_length=255, primary_key=True)
            brands = ListField(ReferenceField("Brand", dbref=True))

        await Brand.adrop_collection()
        await BrandGroup.adrop_collection()

        brand1 = await Brand(title="Moschino").asave()
        brand2 = await Brand(title="Денис Симачёв").asave()

        await BrandGroup(title="top_brands", brands=[brand1, brand2]).asave()
        brand_groups = BrandGroup.aobjects().all()

        assert 2 == len([brand async for bg in brand_groups for brand in bg.brands])

    async def test_dereferencing_embedded_listfield_referencefield(self):
        class Tag(Document):
            meta = {"collection": "tags"}
            name = StringField()

        class Post(EmbeddedDocument):
            body = StringField()
            tags = ListField(ReferenceField("Tag", dbref=True))

        class Page(Document):
            meta = {"collection": "pages"}
            tags = ListField(ReferenceField("Tag", dbref=True))
            posts = ListField(EmbeddedDocumentField(Post))

        await Tag.adrop_collection()
        await Page.adrop_collection()

        tag = await Tag(name="test").asave()
        post = Post(body="test body", tags=[tag])
        await Page(tags=[tag], posts=[post]).asave()

        page = await Page.aobjects.first()
        assert page.tags[0] == page.posts[0].tags[0]

    async def test_select_related_follows_embedded_referencefields(self):
        class Song(Document):
            title = StringField()

        class PlaylistItem(EmbeddedDocument):
            song = ReferenceField("Song")

        class Playlist(Document):
            items = ListField(EmbeddedDocumentField("PlaylistItem"))

        await Playlist.adrop_collection()
        await Song.adrop_collection()

        songs = [await Song.aobjects.create(title="song %d" % i) for i in range(3)]
        items = [PlaylistItem(song=song) for song in songs]
        playlist = await Playlist.aobjects.create(items=items)

        async with async_query_counter() as q:
            assert await q.eq(0)

            playlist = await Playlist.aobjects.select_related("items__song").first()
            songs = [item.song for item in playlist.items]

            assert await q.eq(1)


if __name__ == "__main__":
    unittest.main()
