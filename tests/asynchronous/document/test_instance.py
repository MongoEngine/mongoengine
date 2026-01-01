import copy
import os
import pickle
import uuid
import weakref
from datetime import datetime
try:
    # Python 3.11+
    from datetime import UTC
except ImportError:
    # Python ≤ 3.10
    from datetime import timezone
    UTC = timezone.utc
from unittest.mock import AsyncMock

import bson
import pytest
from bson import DBRef, ObjectId

from mongoengine import *
from mongoengine import signals
from mongoengine.asynchronous import async_get_db, async_disconnect, async_register_connection, async_disconnect_all
from mongoengine.base import _DocumentRegistry
from mongoengine.base.queryset.pipeline_builder import PipelineBuilder
from mongoengine.context_managers import switch_db, async_query_counter, switch_collection
from mongoengine.errors import (
    FieldDoesNotExist,
    InvalidDocumentError,
    InvalidQueryError,
    NotRegistered,
    NotUniqueError,
    SaveConditionError,
)
from mongoengine.pymongo_support import (
    async_list_collection_names,
)
from mongoengine.base.queryset import NULLIFY, Q, CASCADE, PULL, DENY
from mongoengine.registry import _CollectionRegistry
from tests import fixtures
from tests.fixtures import (
    PickleDynamicEmbedded,
    PickleDynamicTest,
    PickleEmbedded,
    PickleTest,
)
from tests.asynchronous.fixtures import PickleSignalsTest
from tests.asynchronous.utils import (
    MongoDBAsyncTestCase,
    async_db_ops_tracker,
    async_get_as_pymongo,
    requires_mongodb_gte_44, reset_async_connections,
)
from tests.utils import MONGO_TEST_DB

TEST_IMAGE_PATH = os.path.join(os.path.dirname(__file__), "../fields/mongoengine.png")


class TestDocumentInstance(MongoDBAsyncTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()

        class Job(EmbeddedDocument):
            name = StringField()
            years = IntField()

        class Person(Document):
            name = StringField()
            age = IntField()
            job = EmbeddedDocumentField(Job)

            non_field = True

            meta = {"allow_inheritance": True}

        self.Person = Person
        self.Job = Job

    async def asyncTearDown(self):
        for collection in await async_list_collection_names(self.db):
            self.db.drop_collection(collection)
        await super().asyncTearDown()
        await reset_async_connections()
        _CollectionRegistry.clear()

    async def _assert_db_equal(self, docs):
        assert await (await self.Person._aget_collection()).find().sort("id").to_list() == sorted(
            docs, key=lambda doc: doc["_id"]
        )

    def _assert_has_instance(self, field, instance):
        assert hasattr(field, "_instance")
        assert field._instance is not None
        if isinstance(field._instance, weakref.ProxyType):
            assert field._instance.__eq__(instance)
        else:
            assert field._instance == instance

    async def test_capped_collection(self):
        """Ensure that capped collections work properly."""

        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {"max_documents": 10, "max_size": 4096}

        await Log.adrop_collection()

        # Ensure that the collection handles up to its maximum
        for _ in range(10):
            await Log().asave()

        assert await Log.aobjects.count() == 10

        # Check that extra documents don't increase the size
        await Log().asave()
        assert await Log.aobjects.count() == 10

        options = await (await Log.aobjects._collection).options()
        assert options["capped"] is True
        assert options["max"] == 10
        assert options["size"] == 4096

        # Check that the document cannot be redefined with different options
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {"max_documents": 11}

        # Accessing Document.objects creates the collection
        with pytest.raises(InvalidCollectionError):
            await Log.aobjects.count()

    async def test_capped_collection_default(self):
        """Ensure that capped collections defaults work properly."""

        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {"max_documents": 10}

        await Log.adrop_collection()

        # Create a doc to create the collection
        await Log().asave()

        options = await (await Log.aobjects._collection).options()
        assert options["capped"] is True
        assert options["max"] == 10
        assert options["size"] == 10 * 2 ** 20

        # Check that the document with default value can be recreated
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {"max_documents": 10}

        # Create the collection by accessing Document.aobjects.count()
        await Log.aobjects.count()

    async def test_capped_collection_no_max_size_problems(self):
        """Ensure that capped collections with odd max_size work properly.
        MongoDB rounds up max_size to next multiple of 256, recreating a doc
        with the same spec failed in mongoengine <0.10
        """

        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {"max_size": 10000}

        await Log.adrop_collection()

        # Create a doc to create the collection
        await Log().asave()

        options = await (await Log.aobjects._collection).options()
        assert options["capped"] is True
        assert options["size"] >= 10000

        # Check that the document with odd max_size value can be recreated
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {"max_size": 10000}

        # Create the collection by accessing Document.aobjects.count()
        await Log.aobjects.count()

    async def test_repr(self):
        """Ensure that unicode representation works"""

        class Article(Document):
            title = StringField()

            def __unicode__(self):
                return self.title

        doc = Article(title="привет мир")

        assert "<Article: привет мир>" == repr(doc)

    async def test_repr_none(self):
        """Ensure None values are handled correctly."""

        class Article(Document):
            title = StringField()

            def __str__(self):
                return None

        doc = Article(title="привет мир")
        assert "<Article: None>" == repr(doc)

    async def test_queryset_resurrects_dropped_collection(self):
        await self.Person.adrop_collection()
        assert await self.Person.aobjects().to_list() == []

        # Ensure works correctly with inherited classes
        class Actor(self.Person):
            pass

        Actor.aobjects()
        await self.Person.adrop_collection()
        assert await Actor.aobjects.to_list() == []

    async def test_polymorphic_references(self):
        """Ensure that the correct subclasses are returned from a query
        when using references / generic references
        """

        class Animal(Document):
            meta = {"allow_inheritance": True}

        class Fish(Animal):
            pass

        class Mammal(Animal):
            pass

        class Dog(Mammal):
            pass

        class Human(Mammal):
            pass

        class Zoo(Document):
            animals = ListField(ReferenceField(Animal))

        await Zoo.adrop_collection()
        await Animal.adrop_collection()

        await Animal().asave()
        await Fish().asave()
        await Mammal().asave()
        await Dog().asave()
        await Human().asave()

        # Save a reference to each animal
        zoo = Zoo(animals=Animal.aobjects)
        await zoo.asave()
        await zoo.areload()

        classes = [a.__class__ for a in (await Zoo.aobjects.select_related("animals").first()).animals]
        assert classes == [Animal, Fish, Mammal, Dog, Human]

        await Zoo.adrop_collection()

        class Zoo(Document):
            animals = ListField(GenericReferenceField(choices=(Animal,)))

        # Save a reference to each animal
        zoo = Zoo(animals=Animal.aobjects)
        await zoo.asave()
        await zoo.areload()

        classes = [a.__class__ for a in (await Zoo.aobjects.select_related("animals").first()).animals]
        assert classes == [Animal, Fish, Mammal, Dog, Human]

    async def test_reference_inheritance(self):
        class Stats(Document):
            created = DateTimeField(default=datetime.now)

            meta = {"allow_inheritance": False}

        class CompareStats(Document):
            generated = DateTimeField(default=datetime.now(UTC))
            stats = ListField(ReferenceField(Stats))

        await Stats.adrop_collection()
        await CompareStats.adrop_collection()

        list_stats = []

        for i in range(10):
            s = Stats()
            await s.asave()
            list_stats.append(s)

        cmp_stats = CompareStats(stats=list_stats)
        await cmp_stats.asave()

        assert list_stats == (await CompareStats.aobjects.first()).stats

    async def test_db_field_load(self):
        """Ensure we load data correctly from the right db field."""

        class Person(Document):
            name = StringField(required=True)
            _rank = StringField(required=False, db_field="rank")

            @property
            def rank(self):
                return self._rank or "Private"

        await Person.adrop_collection()

        await Person(name="Jack", _rank="Corporal").asave()

        await Person(name="Fred").asave()

        assert (await Person.aobjects.get(name="Jack")).rank == "Corporal"
        assert (await Person.aobjects.get(name="Fred")).rank == "Private"

    async def test_db_embedded_doc_field_load(self):
        """Ensure we load embedded document data correctly."""

        class Rank(EmbeddedDocument):
            title = StringField(required=True)

        class Person(Document):
            name = StringField(required=True)
            rank_ = EmbeddedDocumentField(Rank, required=False, db_field="rank")

            @property
            def rank(self):
                if self.rank_ is None:
                    return "Private"
                return self.rank_.title

        await Person.adrop_collection()

        await Person(name="Jack", rank_=Rank(title="Corporal")).asave()
        await Person(name="Fred").asave()

        assert (await Person.aobjects.get(name="Jack")).rank == "Corporal"
        assert (await Person.aobjects.get(name="Fred")).rank == "Private"

    async def test_custom_id_field(self):
        """Ensure that documents may be created with custom primary keys."""

        class User(Document):
            username = StringField(primary_key=True)
            name = StringField()

            meta = {"allow_inheritance": True}

        await User.adrop_collection()

        assert User._fields["username"].db_field == "_id"
        assert User._meta["id_field"] == "username"

        await User.aobjects.create(username="test", name="test user")
        user = await User.aobjects.first()
        assert user.id == "test"
        assert user.pk == "test"
        user_dict = await (await User.aobjects._collection).find_one()
        assert user_dict["_id"] == "test"

    async def test_change_custom_id_field_in_subclass(self):
        """Subclasses cannot override which field is the primary key."""

        class User(Document):
            username = StringField(primary_key=True)
            name = StringField()
            meta = {"allow_inheritance": True}

        with pytest.raises(ValueError, match="Cannot override primary key field"):
            class EmailUser(User):
                email = StringField(primary_key=True)

    async def test_custom_id_field_is_required(self):
        """Ensure the custom primary key field is required."""

        class User(Document):
            username = StringField(primary_key=True)
            name = StringField()

        with pytest.raises(ValidationError) as exc_info:
            await User(name="test").asave()
        assert "Field is required: ['username']" in str(exc_info.value)

    async def test_document_not_registered(self):
        class Place(Document):
            name = StringField()

            meta = {"allow_inheritance": True}

        class NicePlace(Place):
            pass

        await Place.adrop_collection()

        await Place(name="London").asave()
        await NicePlace(name="Buckingham Palace").asave()

        # Mimic Place and NicePlace definitions being in a different file
        # and the NicePlace model not being imported in at query time.
        _DocumentRegistry.unregister("Place.NicePlace")

        with pytest.raises(NotRegistered):
            await Place.aobjects.all().to_list()

    async def test_document_registry_regressions(self):
        class Location(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        class Area(Location):
            location = ReferenceField("Location", dbref=True)

        await Location.adrop_collection()

        assert Area == _DocumentRegistry.get("Area")
        assert Area == _DocumentRegistry.get("Location.Area")

    async def test_creation(self):
        """Ensure that document may be created using keyword arguments."""
        person = self.Person(name="Test User", age=30)
        assert person.name == "Test User"
        assert person.age == 30

    async def test__qs_property_does_not_raise(self):
        # ensures no regression of #2500
        class MyDocument(Document):
            pass

        await MyDocument.adrop_collection()
        object = MyDocument()
        await object._aqs().insert([MyDocument()])
        assert await MyDocument.aobjects.count() == 1

    async def test_to_dbref(self):
        """Ensure that you can get a dbref of a document."""
        person = self.Person(name="Test User", age=30)
        with pytest.raises(OperationError):
            person.to_dbref()
        await person.asave()
        person.to_dbref()

    async def test_key_like_attribute_access(self):
        person = self.Person(age=30)
        assert person["age"] == 30
        with pytest.raises(KeyError):
            person["unknown_attr"]

    async def test_save_abstract_document(self):
        """Saving an abstract document should fail."""

        class Doc(Document):
            name = StringField()
            meta = {"abstract": True}

        with pytest.raises(InvalidDocumentError):
            await Doc(name="aaa").asave()

    async def test_reload(self):
        """Ensure that attributes may be reloaded."""
        person = self.Person(name="Test User", age=20)
        await person.asave()

        person_obj = await self.Person.aobjects.first()
        person_obj.name = "Mr Test User"
        person_obj.age = 21
        await person_obj.asave()

        assert person.name == "Test User"
        assert person.age == 20

        await person.areload("age")
        assert person.name == "Test User"
        assert person.age == 21

        await person.areload()
        assert person.name == "Mr Test User"
        assert person.age == 21

        await person.areload()
        assert person.name == "Mr Test User"
        assert person.age == 21

    async def test_reload_sharded(self):
        class Animal(Document):
            superphylum = StringField()
            meta = {"shard_key": ("superphylum",)}

        await Animal.adrop_collection()
        doc = await Animal.aobjects.create(superphylum="Deuterostomia")

        CMD_QUERY_KEY = "command"
        async with async_query_counter() as q:
            await doc.areload()
            query_op = (await ((await q.db).system.profile.find({"ns": f"{MONGO_TEST_DB}.animal"})).to_list())[0]
            assert set(query_op[CMD_QUERY_KEY]["filter"].keys()) == {
                "_id",
                "superphylum",
            }

    async def test_reload_sharded_with_db_field(self):
        class Person(Document):
            nationality = StringField(db_field="country")
            meta = {"shard_key": ("nationality",)}

        await Person.adrop_collection()
        doc = await Person.aobjects.create(nationality="Poland")

        CMD_QUERY_KEY = "command"
        async with async_query_counter() as q:
            await doc.areload()
            query_op = (await ((await q.db).system.profile.find({"ns": f"{MONGO_TEST_DB}.person"})).to_list())[0]
            assert set(query_op[CMD_QUERY_KEY]["filter"].keys()) == {"_id", "country"}

    async def test_reload_sharded_nested(self):
        class SuperPhylum(EmbeddedDocument):
            name = StringField()

        class Animal(Document):
            superphylum = EmbeddedDocumentField(SuperPhylum)
            meta = {"shard_key": ("superphylum.name",)}

        await Animal.adrop_collection()
        doc = Animal(superphylum=SuperPhylum(name="Deuterostomia"))
        await doc.asave()
        await doc.areload()
        await Animal.adrop_collection()

    async def test_save_update_shard_key_routing(self):
        """Ensures updating a doc with a specified shard_key includes it in
        the query.
        """

        class Animal(Document):
            is_mammal = BooleanField()
            name = StringField()
            meta = {"shard_key": ("is_mammal", "id")}

        await Animal.adrop_collection()
        doc = Animal(is_mammal=True, name="Dog")
        await doc.asave()

        async with async_query_counter() as q:
            doc.name = "Cat"
            await doc.asave()
            query_op = (await ((await q.db).system.profile.find({"ns": f"{MONGO_TEST_DB}.animal"})).to_list())[0]
            assert query_op["op"] == "update"
            assert set(query_op["command"]["q"].keys()) == {"_id", "is_mammal"}

        await Animal.adrop_collection()

    async def test_save_create_shard_key_routing(self):
        """Ensures inserting a doc with a specified shard_key includes it in
        the query.
        """

        class Animal(Document):
            _id = UUIDField(binary=False, primary_key=True, default=uuid.uuid4)
            is_mammal = BooleanField()
            name = StringField()
            meta = {"shard_key": ("is_mammal",)}

        await Animal.adrop_collection()
        doc = Animal(is_mammal=True, name="Dog")

        async with async_query_counter() as q:
            await doc.asave()
            query_op = (await ((await q.db).system.profile.find({"ns": f"{MONGO_TEST_DB}.animal"})).to_list())[0]
            assert query_op["op"] == "command"
            assert query_op["command"]["findAndModify"] == "animal"
            assert set(query_op["command"]["query"].keys()) == {"_id", "is_mammal"}

        await Animal.adrop_collection()

    async def test_reload_with_changed_fields(self):
        """Ensures reloading will not affect changed fields"""

        class User(Document):
            name = StringField()
            number = IntField()

        await User.adrop_collection()

        user = await User(name="Bob", number=1).asave()
        user.name = "John"
        user.number = 2

        assert user._get_changed_fields() == ["name", "number"]
        await user.areload("number")
        assert user._get_changed_fields() == ["name"]
        await user.asave()
        await user.areload()
        assert user.name == "John"

    async def test_reload_referencing(self):
        """Ensures reloading updates weakrefs correctly."""

        class Embedded(EmbeddedDocument):
            dict_field = DictField()
            list_field = ListField()

        class Doc(Document):
            dict_field = DictField()
            list_field = ListField()
            embedded_field = EmbeddedDocumentField(Embedded)

        await Doc.adrop_collection()
        doc = Doc()
        doc.dict_field = {"hello": "world"}
        doc.list_field = ["1", 2, {"hello": "world"}]

        embedded_1 = Embedded()
        embedded_1.dict_field = {"hello": "world"}
        embedded_1.list_field = ["1", 2, {"hello": "world"}]
        doc.embedded_field = embedded_1
        await doc.asave()

        doc = await doc.areload(10)
        doc.list_field.append(1)
        doc.dict_field["woot"] = "woot"
        doc.embedded_field.list_field.append(1)
        doc.embedded_field.dict_field["woot"] = "woot"

        changed = doc._get_changed_fields()
        assert changed == [
            "list_field",
            "dict_field.woot",
            "embedded_field.list_field",
            "embedded_field.dict_field.woot",
        ]
        await doc.asave()

        assert len(doc.list_field) == 4
        doc = await doc.areload(10)
        assert doc._get_changed_fields() == []
        assert len(doc.list_field) == 4
        assert len(doc.dict_field) == 2
        assert len(doc.embedded_field.list_field) == 4
        assert len(doc.embedded_field.dict_field) == 2

        doc.list_field.append(1)
        await doc.asave()
        doc.dict_field["extra"] = 1
        doc = await doc.areload(10, "list_field")
        assert doc._get_changed_fields() == ["dict_field.extra"]
        assert len(doc.list_field) == 5
        assert len(doc.dict_field) == 3
        assert len(doc.embedded_field.list_field) == 4
        assert len(doc.embedded_field.dict_field) == 2

    async def test_reload_doesnt_exist(self):
        class Foo(Document):
            pass

        f = Foo()
        with pytest.raises(DoesNotExist):
            await f.areload()

        await f.asave()
        await f.adelete()

        with pytest.raises(DoesNotExist):
            await f.areload()

    async def test_reload_of_non_strict_with_special_field_name(self):
        """Ensures reloading works for documents with meta strict is False."""

        class Post(Document):
            meta = {"strict": False}
            title = StringField()
            items = ListField()

        await Post.adrop_collection()

        await (await Post._aget_collection()).insert_one(
            {"title": "Items eclipse", "items": ["more lorem", "even more ipsum"]}
        )

        post = await Post.aobjects.first()
        await post.areload()
        assert post.title == "Items eclipse"
        assert post.items == ["more lorem", "even more ipsum"]

    async def test_dictionary_access(self):
        """Ensure that dictionary-style field access works properly."""
        person = self.Person(name="Test User", age=30, job=self.Job())
        assert person["name"] == "Test User"

        with pytest.raises(KeyError):
            person.__getitem__("salary")
        with pytest.raises(KeyError):
            person.__setitem__("salary", 50)

        person["name"] = "Another User"
        assert person["name"] == "Another User"

        # Length = length(assigned fields + id)
        assert len(person) == 5

        assert "age" in person
        person.age = None
        assert "age" not in person
        assert "nationality" not in person

    async def test_embedded_document_to_mongo(self):
        class Person(EmbeddedDocument):
            name = StringField()
            age = IntField()

            meta = {"allow_inheritance": True}

        class Employee(Person):
            salary = IntField()

        assert sorted(Person(name="Bob", age=35).to_mongo().keys()) == [
            "_cls",
            "age",
            "name",
        ]
        assert sorted(Employee(name="Bob", age=35, salary=0).to_mongo().keys()) == [
            "_cls",
            "age",
            "name",
            "salary",
        ]

    async def test_embedded_document_to_mongo_id(self):
        class SubDoc(EmbeddedDocument):
            id = StringField(required=True)

        sub_doc = SubDoc(id="abc")
        assert list(sub_doc.to_mongo().keys()) == ["id"]

    async def test_embedded_document(self):
        """Ensure that embedded documents are set up correctly."""

        class Comment(EmbeddedDocument):
            content = StringField()

        assert "content" in Comment._fields
        assert "id" not in Comment._fields

    async def test_embedded_document_instance(self):
        """Ensure that embedded documents can reference parent instance."""

        class Embedded(EmbeddedDocument):
            string = StringField()

        class Doc(Document):
            embedded_field = EmbeddedDocumentField(Embedded)

        await Doc.adrop_collection()

        doc = Doc(embedded_field=Embedded(string="Hi"))
        self._assert_has_instance(doc.embedded_field, doc)

        await doc.asave()
        doc = await Doc.aobjects.get()
        self._assert_has_instance(doc.embedded_field, doc)

    async def test_embedded_document_complex_instance(self):
        """Ensure that embedded documents in complex fields can reference
        parent instance.
        """

        class Embedded(EmbeddedDocument):
            string = StringField()

        class Doc(Document):
            embedded_field = ListField(EmbeddedDocumentField(Embedded))

        await Doc.adrop_collection()
        doc = Doc(embedded_field=[Embedded(string="Hi")])
        self._assert_has_instance(doc.embedded_field[0], doc)

        await doc.asave()
        doc = await Doc.aobjects.get()
        self._assert_has_instance(doc.embedded_field[0], doc)

    async def test_embedded_document_complex_instance_no_use_db_field(self):
        """Ensure that use_db_field is propagated to list of Emb Docs."""

        class Embedded(EmbeddedDocument):
            string = StringField(db_field="s")

        class Doc(Document):
            embedded_field = ListField(EmbeddedDocumentField(Embedded))

        d = (
            Doc(embedded_field=[Embedded(string="Hi")])
            .to_mongo(use_db_field=False)
            .to_dict()
        )
        assert d["embedded_field"] == [{"string": "Hi"}]

    async def test_instance_is_set_on_setattr(self):
        class Email(EmbeddedDocument):
            email = EmailField()

        class Account(Document):
            email = EmbeddedDocumentField(Email)

        await Account.adrop_collection()

        acc = Account()
        acc.email = Email(email="test@example.com")
        self._assert_has_instance(acc._data["email"], acc)
        await acc.asave()

        acc1 = await Account.aobjects.first()
        self._assert_has_instance(acc1._data["email"], acc1)

    async def test_instance_is_set_on_setattr_on_embedded_document_list(self):
        class Email(EmbeddedDocument):
            email = EmailField()

        class Account(Document):
            emails = EmbeddedDocumentListField(Email)

        await Account.adrop_collection()
        acc = Account()
        acc.emails = [Email(email="test@example.com")]
        self._assert_has_instance(acc._data["emails"][0], acc)
        await acc.asave()

        acc1 = await Account.aobjects.first()
        self._assert_has_instance(acc1._data["emails"][0], acc1)

    async def test_save_checks_that_clean_is_called(self):
        class CustomError(Exception):
            pass

        class TestDocument(Document):
            def clean(self):
                raise CustomError()

        with pytest.raises(CustomError):
            await TestDocument().asave()

        await TestDocument().asave(clean=False)

    async def test_save_signal_pre_save_post_validation_makes_change_to_doc(self):
        class BlogPost(Document):
            content = StringField()

            @classmethod
            async def pre_save_post_validation(cls, sender, document, **kwargs):
                document.content = "checked"

        signals.pre_save_post_validation.connect(
            BlogPost.pre_save_post_validation, sender=BlogPost
        )

        await BlogPost.adrop_collection()

        post = await BlogPost(content="unchecked").asave()
        assert post.content == "checked"
        # Make sure pre_save_post_validation changes makes it to the db
        raw_doc = await async_get_as_pymongo(post)
        assert raw_doc == {"content": "checked", "_id": post.id}

        # Important to disconnect as it could cause some assertions in test_signals
        # to fail (due to the garbage collection timing of this signal)
        signals.pre_save_post_validation.disconnect(BlogPost.pre_save_post_validation)

    async def test_document_clean(self):
        class TestDocument(Document):
            status = StringField()
            cleaned = BooleanField(default=False)

            def clean(self):
                self.cleaned = True

        await TestDocument.adrop_collection()

        t = TestDocument(status="draft")

        # Ensure clean=False prevent call to clean
        t = TestDocument(status="published")
        await t.asave(clean=False)
        assert t.status == "published"
        assert t.cleaned is False

        t = TestDocument(status="published")
        assert t.cleaned is False
        await t.asave(clean=True)
        assert t.status == "published"
        assert t.cleaned is True
        raw_doc = await async_get_as_pymongo(t)
        # Make sure clean changes makes it to the db
        assert raw_doc == {"status": "published", "cleaned": True, "_id": t.id}

    async def test_document_embedded_clean(self):
        class TestEmbeddedDocument(EmbeddedDocument):
            x = IntField(required=True)
            y = IntField(required=True)
            z = IntField(required=True)

            meta = {"allow_inheritance": False}

            def clean(self):
                if self.z:
                    if self.z != self.x + self.y:
                        raise ValidationError("Value of z != x + y")
                else:
                    self.z = self.x + self.y

        class TestDocument(Document):
            doc = EmbeddedDocumentField(TestEmbeddedDocument)
            status = StringField()

        await TestDocument.adrop_collection()

        t = TestDocument(doc=TestEmbeddedDocument(x=10, y=25, z=15))

        with pytest.raises(ValidationError) as exc_info:
            await t.asave()

        expected_msg = "Value of z != x + y"
        assert expected_msg in str(exc_info.value)
        assert exc_info.value.to_dict() == {"doc": {"__all__": expected_msg}}

        t = await TestDocument(doc=TestEmbeddedDocument(x=10, y=25)).asave()
        assert t.doc.z == 35

        # Asserts not raises
        t = TestDocument(doc=TestEmbeddedDocument(x=15, y=35, z=5))
        await t.asave(clean=False)

    async def test_modify_empty(self):
        doc = await self.Person(name="bob", age=10).asave()

        with pytest.raises(InvalidDocumentError):
            await self.Person().amodify(set__age=10)

        await self._assert_db_equal([dict(doc.to_mongo())])

    async def test_modify_invalid_query(self):
        doc1 = await self.Person(name="bob", age=10).asave()
        doc2 = await self.Person(name="jim", age=20).asave()
        docs = [dict(doc1.to_mongo()), dict(doc2.to_mongo())]

        with pytest.raises(InvalidQueryError):
            await doc1.amodify({"id": doc2.id}, set__value=20)

        await self._assert_db_equal(docs)

    async def test_modify_match_another_document(self):
        doc1 = await self.Person(name="bob", age=10).asave()
        doc2 = await self.Person(name="jim", age=20).asave()
        docs = [dict(doc1.to_mongo()), dict(doc2.to_mongo())]

        n_modified = await doc1.amodify({"name": doc2.name}, set__age=100)
        assert n_modified == 0

        await self._assert_db_equal(docs)

    async def test_modify_not_exists(self):
        doc1 = await self.Person(name="bob", age=10).asave()
        doc2 = self.Person(id=ObjectId(), name="jim", age=20)
        docs = [dict(doc1.to_mongo())]

        n_modified = await doc2.amodify({"name": doc2.name}, set__age=100)
        assert n_modified == 0

        await self._assert_db_equal(docs)

    async def test_modify_update(self):
        other_doc = await self.Person(name="bob", age=10).asave()
        doc = await self.Person(
            name="jim", age=20, job=self.Job(name="10gen", years=3)
        ).asave()

        doc_copy = doc._from_son(doc.to_mongo())

        # these changes must go away
        doc.name = "liza"
        doc.job.name = "Google"
        doc.job.years = 3

        n_modified = await doc.amodify(
            set__age=21, set__job__name="MongoDB", unset__job__years=True
        )
        assert n_modified == 1
        doc_copy.age = 21
        doc_copy.job.name = "MongoDB"
        del doc_copy.job.years

        assert doc.to_json() == doc_copy.to_json()
        assert doc._get_changed_fields() == []

        await self._assert_db_equal([dict(other_doc.to_mongo()), dict(doc.to_mongo())])

    async def test_modify_with_positional_push(self):
        class Content(EmbeddedDocument):
            keywords = ListField(StringField())

        class BlogPost(Document):
            tags = ListField(StringField())
            content = EmbeddedDocumentField(Content)

        post = await BlogPost.aobjects.create(
            tags=["python"], content=Content(keywords=["ipsum"])
        )

        assert post.tags == ["python"]
        await post.amodify(push__tags__0=["code", "mongo"])
        assert post.tags == ["code", "mongo", "python"]

        # Assert same order of the list items is maintained in the db
        assert (await (await BlogPost._aget_collection()).find_one({"_id": post.pk}))["tags"] == [
            "code",
            "mongo",
            "python",
        ]

        assert post.content.keywords == ["ipsum"]
        await post.amodify(push__content__keywords__0=["lorem"])
        assert post.content.keywords == ["lorem", "ipsum"]

        # Assert same order of the list items is maintained in the db
        assert (await (await BlogPost._aget_collection()).find_one({"_id": post.pk}))["content"][
                   "keywords"
               ] == ["lorem", "ipsum"]

    async def test_save(self):
        """Ensure that a document may be saved in the database."""

        # Create person object and save it to the database
        person = self.Person(name="Test User", age=30)
        await person.asave()

        # Ensure that the object is in the database
        raw_doc = await async_get_as_pymongo(person)
        assert raw_doc == {
            "_cls": "Person",
            "name": "Test User",
            "age": 30,
            "_id": person.id,
        }

    async def test_save_write_concern(self):
        class Recipient(Document):
            email = EmailField(required=True)

        rec = Recipient(email="garbage@garbage.com")

        fn = AsyncMock()
        rec._asave_create = fn
        await rec.asave(write_concern={"w": 0})
        assert fn.call_args[1]["write_concern"] == {"w": 0}

    async def test_save_skip_validation(self):
        class Recipient(Document):
            email = EmailField(required=True)

        recipient = Recipient(email="not-an-email")
        with pytest.raises(ValidationError):
            await recipient.asave()

        await recipient.asave(validate=False)
        raw_doc = await async_get_as_pymongo(recipient)
        assert raw_doc == {"email": "not-an-email", "_id": recipient.id}

    async def test_save_with_bad_id(self):
        class Clown(Document):
            id = IntField(primary_key=True)

        with pytest.raises(ValidationError):
            await Clown(id="not_an_int").asave()

    async def test_save_to_a_value_that_equates_to_false(self):
        class Thing(EmbeddedDocument):
            count = IntField()

        class User(Document):
            thing = EmbeddedDocumentField(Thing)

        await User.adrop_collection()

        user = User(thing=Thing(count=1))
        await user.asave()
        await user.areload()

        user.thing.count = 0
        await user.asave()

        await user.areload()
        assert user.thing.count == 0

    async def test_save_max_recursion_not_hit(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField("self")
            friend = ReferenceField("self")

        await Person.adrop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        await p1.asave()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        await p2.asave()

        p1.friend = p2
        await p1.asave()

        # Confirm can save and it resets the changed fields without hitting
        # max recursion error
        p0 = await Person.aobjects.first()
        p0.name = "wpjunior"
        await p0.asave()

    async def test_save_max_recursion_not_hit_with_file_field(self):
        class Foo(Document):
            name = StringField()
            picture = FileField()
            bar = ReferenceField("self")

        await Foo.adrop_collection()

        a = await Foo(name="hello").asave()

        a.bar = a
        with open(TEST_IMAGE_PATH, "rb") as test_image:
            await a.picture.aput(test_image)
            await a.asave()

            # Confirm can save, and it resets the changed fields without hitting
            # max recursion error
            b = await Foo.aobjects.select_related("bar").with_id(a.id)
            b.name = "world"
            await b.asave()

            assert b.picture == b.bar.picture, b.bar.bar.picture

    async def test_save_cascades(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField("self")

        await Person.adrop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        await p1.asave()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        await p2.asave()

        p = await Person.aobjects(name="Wilson Jr").select_related("parent").get()
        p.parent.name = "Daddy Wilson"
        await p.asave(cascade=True)

        await p1.areload()
        assert p1.name == p.parent.name

    async def test_save_cascade_kwargs(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField("self")

        await Person.adrop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        await p1.asave()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p1.name = "Daddy Wilson"
        await p2.asave(force_insert=True, cascade_kwargs={"force_insert": False})

        await p1.areload()
        await p2.aselect_related("parent")
        assert p1.name == p2.parent.name

    async def test_save_cascade_meta_false(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField("self")

            meta = {"cascade": False}

        await Person.adrop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        await p1.asave()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        await p2.asave()

        p = await Person.aobjects(name="Wilson Jr").select_related("parent").get()
        p.parent.name = "Daddy Wilson"
        await p.asave()

        await p1.areload()
        assert p1.name != p.parent.name

        await p.asave(cascade=True)
        await p1.areload()
        assert p1.name == p.parent.name

    async def test_save_cascade_meta_true(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField("self")

            meta = {"cascade": False}

        await Person.adrop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        await p1.asave()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        await p2.asave(cascade=True)

        p = await Person.aobjects(name="Wilson Jr").select_related("parent").get()
        p.parent.name = "Daddy Wilson"
        await p.asave()

        await p1.areload()
        assert p1.name != p.parent.name

    async def test_save_cascades_generically(self):
        class Person(Document):
            name = StringField()
            parent = GenericReferenceField(choices=("Self",))

        await Person.adrop_collection()

        p1 = Person(name="Wilson Snr")
        await p1.asave()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        await p2.asave()

        p = await Person.aobjects(name="Wilson Jr").select_related("parent").get()
        p.parent.name = "Daddy Wilson"
        await p.asave()

        await p1.areload()
        assert p1.name != p.parent.name

        await p.asave(cascade=True)
        await p1.areload()
        assert p1.name == p.parent.name

    async def test_save_atomicity_condition(self):
        class Widget(Document):
            toggle = BooleanField(default=False)
            count = IntField(default=0)
            save_id = UUIDField()

        def flip(widget):
            widget.toggle = not widget.toggle
            widget.count += 1

        def UUID(i):
            return uuid.UUID(int=i)

        await Widget.adrop_collection()

        w1 = Widget(toggle=False, save_id=UUID(1))

        # ignore save_condition on new record creation
        await w1.asave(save_condition={"save_id": UUID(42)})
        await w1.areload()
        assert not w1.toggle
        assert w1.save_id == UUID(1)
        assert w1.count == 0

        # mismatch in save_condition prevents save and raise exception
        flip(w1)
        assert w1.toggle
        assert w1.count == 1
        with pytest.raises(SaveConditionError):
            await w1.asave(save_condition={"save_id": UUID(42)})
        await w1.areload()
        assert not w1.toggle
        assert w1.count == 0

        # matched save_condition allows save
        flip(w1)
        assert w1.toggle
        assert w1.count == 1
        await w1.asave(save_condition={"save_id": UUID(1)})
        await w1.areload()
        assert w1.toggle
        assert w1.count == 1

        # save_condition can be used to ensure atomic read & updates
        # i.e., prevent interleaved reads and writes from separate contexts
        w2 = await Widget.aobjects.get()
        assert w1 == w2
        old_id = w1.save_id

        flip(w1)
        w1.save_id = UUID(2)
        await w1.asave(save_condition={"save_id": old_id})
        await w1.areload()
        assert not w1.toggle
        assert w1.count == 2
        flip(w2)
        flip(w2)
        with pytest.raises(SaveConditionError):
            await w2.asave(save_condition={"save_id": old_id})
        await w2.areload()
        assert not w2.toggle
        assert w2.count == 2

        # save_condition uses mongoengine-style operator syntax
        flip(w1)
        await w1.asave(save_condition={"count__lt": w1.count})
        await w1.areload()
        assert w1.toggle
        assert w1.count == 3
        flip(w1)
        with pytest.raises(SaveConditionError):
            await w1.asave(save_condition={"count__gte": w1.count})
        await w1.areload()
        assert w1.toggle
        assert w1.count == 3

    async def test_save_update_selectively(self):
        class WildBoy(Document):
            age = IntField()
            name = StringField()

        await WildBoy.adrop_collection()

        await WildBoy(age=12, name="John").asave()

        boy1 = await WildBoy.aobjects().first()
        boy2 = await WildBoy.aobjects().first()

        boy1.age = 99
        await boy1.asave()
        boy2.name = "Bob"
        await boy2.asave()

        fresh_boy = await WildBoy.aobjects().first()
        assert fresh_boy.age == 99
        assert fresh_boy.name == "Bob"

    async def test_save_update_selectively_with_custom_pk(self):
        # Prevents regression of #2082
        class WildBoy(Document):
            pk_id = StringField(primary_key=True)
            age = IntField()
            name = StringField()

        await WildBoy.adrop_collection()

        await WildBoy(pk_id="A", age=12, name="John").asave()

        boy1 = await WildBoy.aobjects().first()
        boy2 = await WildBoy.aobjects().first()

        boy1.age = 99
        await boy1.asave()
        boy2.name = "Bob"
        await boy2.asave()

        fresh_boy = await WildBoy.aobjects().first()
        assert fresh_boy.age == 99
        assert fresh_boy.name == "Bob"

    async def test_update(self):
        """Ensure that an existing document is updated instead of be
        overwritten.
        """
        # Create person object and save it to the database
        person = self.Person(name="Test User", age=30)
        await person.asave()

        # Create same person object, with same id, without age
        same_person = self.Person(name="Test")
        same_person.id = person.id
        await same_person.asave()

        # Confirm only one object
        assert await self.Person.aobjects.count() == 1

        # reload
        await person.areload()
        await same_person.areload()

        # Confirm the same
        assert person == same_person
        assert person.name == same_person.name
        assert person.age == same_person.age

        # Confirm the saved values
        assert person.name == "Test"
        assert person.age == 30

        # Test only / exclude only updates included fields
        person = await self.Person.aobjects.only("name").get()
        person.name = "User"
        await person.asave()

        await person.areload()
        assert person.name == "User"
        assert person.age == 30

        # test exclude only updates set fields
        person = await self.Person.aobjects.exclude("name").get()
        person.age = 21
        await person.asave()

        await person.areload()
        assert person.name == "User"
        assert person.age == 21

        # Test only / exclude can set non excluded / included fields
        person = await self.Person.aobjects.only("name").get()
        person.name = "Test"
        person.age = 30
        await person.asave()

        await person.areload()
        assert person.name == "Test"
        assert person.age == 30

        # test exclude only updates set fields
        person = await self.Person.aobjects.exclude("name").get()
        person.name = "User"
        person.age = 21
        await person.asave()

        await person.areload()
        assert person.name == "User"
        assert person.age == 21

        # Confirm does remove unrequired fields
        person = await self.Person.aobjects.exclude("name").get()
        person.age = None
        await person.asave()

        await person.areload()
        assert person.name == "User"
        assert person.age is None

        person = await self.Person.aobjects.get()
        person.name = None
        person.age = None
        await person.asave()

        await person.areload()
        assert person.name is None
        assert person.age is None

    async def test_update_rename_operator(self):
        """Test the $rename operator."""
        coll = await self.Person._aget_collection()
        doc = await self.Person(name="John").asave()
        raw_doc = await coll.find_one({"_id": doc.pk})
        assert set(raw_doc.keys()) == {"_id", "_cls", "name"}

        await doc.aupdate(rename__name="first_name")
        raw_doc = await coll.find_one({"_id": doc.pk})
        assert set(raw_doc.keys()) == {"_id", "_cls", "first_name"}
        assert raw_doc["first_name"] == "John"

    async def test_inserts_if_you_set_the_pk(self):
        _ = await self.Person(name="p1", id=bson.ObjectId()).asave()
        p2 = self.Person(name="p2")
        p2.id = bson.ObjectId()
        await p2.asave()

        assert 2 == await self.Person.aobjects.count()

    async def test_can_save_if_not_included(self):
        class EmbeddedDoc(EmbeddedDocument):
            pass

        class Simple(Document):
            pass

        simple = Simple()
        await simple.asave()

        class Doc(Document):
            string_field = StringField(default="1")
            int_field = IntField(default=1)
            float_field = FloatField(default=1.1)
            boolean_field = BooleanField(default=True)
            datetime_field = DateTimeField(default=datetime.now)
            embedded_document_field = EmbeddedDocumentField(
                EmbeddedDoc, default=lambda: EmbeddedDoc()
            )
            list_field = ListField(default=lambda: [1, 2, 3])
            dict_field = DictField(default=lambda: {"hello": "world"})
            objectid_field = ObjectIdField(default=bson.ObjectId)
            reference_field = ReferenceField(Simple, default=simple)
            map_field = MapField(IntField(), default=lambda: {"simple": 1})
            decimal_field = DecimalField(default=1.0)
            complex_datetime_field = ComplexDateTimeField(default=datetime.now)
            url_field = URLField(default="http://mongoengine.org")
            dynamic_field = DynamicField(default=1)
            generic_reference_field = GenericReferenceField(
                default=simple, choices=(Simple,)
            )
            sorted_list_field = SortedListField(IntField(), default=lambda: [1, 2, 3])
            email_field = EmailField(default="ross@example.com")
            geo_point_field = GeoPointField(default=lambda: [1, 2])
            sequence_field = SequenceField()
            uuid_field = UUIDField(default=uuid.uuid4)
            generic_embedded_document_field = GenericEmbeddedDocumentField(
                default=lambda: EmbeddedDoc()
            )

        await Simple.adrop_collection()
        await Doc.adrop_collection()

        await Doc().asave()
        my_doc = await Doc.aobjects.only("string_field").first()
        my_doc.string_field = "string"
        await my_doc.asave()

        my_doc = await Doc.aobjects.get(string_field="string")
        assert my_doc.string_field == "string"
        assert my_doc.int_field == 1

    async def test_document_update(self):
        # try updating a non-saved document
        with pytest.raises(OperationError):
            person = self.Person(name="dcrosta")
            await person.aupdate(set__name="Dan Crosta")

        author = self.Person(name="dcrosta")
        await author.asave()

        await author.aupdate(set__name="Dan Crosta")
        await author.areload()

        p1 = await self.Person.aobjects.first()
        assert p1.name == author.name

        # try sending an empty update
        with pytest.raises(OperationError):
            person = await self.Person.aobjects.first()
            await person.aupdate()

        # update that doesn't explicitly specify an operator should default
        # to 'set__'
        person = await self.Person.aobjects.first()
        await person.aupdate(name="Dan")
        await person.areload()
        assert "Dan" == person.name

    async def test_update_unique_field(self):
        class Doc(Document):
            name = StringField(unique=True)

        doc1 = await Doc(name="first").asave()
        doc2 = await Doc(name="second").asave()

        with pytest.raises(NotUniqueError):
            await doc2.aupdate(set__name=doc1.name)

    async def test_embedded_update(self):
        """Test update on `EmbeddedDocumentField` fields."""

        class Page(EmbeddedDocument):
            log_message = StringField(verbose_name="Log message", required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)

        await Site.adrop_collection()
        site = Site(page=Page(log_message="Warning: Dummy message"))
        await site.asave()

        # Update
        site = await Site.aobjects.first()
        site.page.log_message = "Error: Dummy message"
        await site.asave()

        site = await Site.aobjects.first()
        assert site.page.log_message == "Error: Dummy message"

    async def test_update_list_field(self):
        """Test update on `ListField` with $pull + $in."""

        class Doc(Document):
            foo = ListField(StringField())

        await Doc.adrop_collection()
        doc = Doc(foo=["a", "b", "c"])
        await doc.asave()

        # Update
        doc = await Doc.aobjects.first()
        await doc.aupdate(pull__foo__in=["a", "c"])

        doc = await Doc.aobjects.first()
        assert doc.foo == ["b"]

    async def test_embedded_update_db_field(self):
        """Test update on `EmbeddedDocumentField` fields when db_field
        is other than default.
        """

        class Page(EmbeddedDocument):
            log_message = StringField(
                verbose_name="Log message", db_field="page_log_message", required=True
            )

        class Site(Document):
            page = EmbeddedDocumentField(Page)

        await Site.adrop_collection()

        site = Site(page=Page(log_message="Warning: Dummy message"))
        await site.asave()

        # Update
        site = await Site.aobjects.first()
        site.page.log_message = "Error: Dummy message"
        await site.asave()

        site = await Site.aobjects.first()
        assert site.page.log_message == "Error: Dummy message"

    async def test_save_only_changed_fields(self):
        """Ensure save only sets / unsets changed fields."""

        class User(self.Person):
            active = BooleanField(default=True)

        await User.adrop_collection()

        # Create person object and save it to the database
        user = User(name="Test User", age=30, active=True)
        await user.asave()
        await user.areload()

        # Simulated Race condition
        same_person = await self.Person.aobjects.get()
        same_person.active = False

        user.age = 21
        await user.asave()

        same_person.name = "User"
        await same_person.asave()

        person = await self.Person.aobjects.get()
        assert person.name == "User"
        assert person.age == 21
        assert person.active is False

    async def test__get_changed_fields_same_ids_reference_field_does_not_enters_infinite_loop_embedded_doc(
            self,
    ):
        # Refers to Issue #1685
        class EmbeddedChildModel(EmbeddedDocument):
            id = DictField(primary_key=True)

        class ParentModel(Document):
            child = EmbeddedDocumentField(EmbeddedChildModel)

        emb = EmbeddedChildModel(id={"1": [1]})
        changed_fields = ParentModel(child=emb)._get_changed_fields()
        assert changed_fields == []

    async def test__get_changed_fields_same_ids_reference_field_does_not_enters_infinite_loop_different_doc(
            self,
    ):
        # Refers to Issue #1685
        class User(Document):
            id = IntField(primary_key=True)
            name = StringField()

        class Message(Document):
            id = IntField(primary_key=True)
            author = ReferenceField(User)

        await Message.adrop_collection()

        # All objects share the same id, but each in a different collection
        user = await User(id=1, name="user-name").asave()
        message = await Message(id=1, author=user).asave()

        message.author.name = "tutu"
        assert message._get_changed_fields() == []
        assert user._get_changed_fields() == ["name"]

    async def test__get_changed_fields_same_ids_embedded(self):
        # Refers to Issue #1768
        class User(EmbeddedDocument):
            id = IntField()
            name = StringField()

        class Message(Document):
            id = IntField(primary_key=True)
            author = EmbeddedDocumentField(User)

        await Message.adrop_collection()

        # All objects share the same id, but each in a different collection
        user = User(id=1, name="user-name")  # .save()
        message = await Message(id=1, author=user).asave()

        message.author.name = "tutu"
        assert message._get_changed_fields() == ["author.name"]
        await message.asave()

        message_fetched = await Message.aobjects.with_id(message.id)
        assert message_fetched.author.name == "tutu"

    async def test_query_count_when_saving(self):
        """Ensure references to don't cause extra fetches when saving"""

        class Organization(Document):
            name = StringField()

        class User(Document):
            name = StringField()
            orgs = ListField(ReferenceField("Organization"))

        class Feed(Document):
            name = StringField()

        class UserSubscription(Document):
            name = StringField()
            user = ReferenceField(User)
            feed = ReferenceField(Feed)

        await Organization.adrop_collection()
        await User.adrop_collection()
        await Feed.adrop_collection()
        await UserSubscription.adrop_collection()

        o1 = await Organization(name="o1").asave()
        o2 = await Organization(name="o2").asave()

        u1 = await User(name="Ross", orgs=[o1, o2]).asave()
        f1 = await Feed(name="MongoEngine").asave()

        sub = await UserSubscription(user=u1, feed=f1).asave()

        user = await User.aobjects.select_related("orgs").first()
        assert isinstance(user._data["orgs"][0], Organization)
        assert isinstance(user.orgs[0], Organization)
        assert isinstance(user._data["orgs"][0], Organization)

        # Changing a value
        async with async_query_counter() as q:
            assert await q.eq(0)
            sub = await UserSubscription.aobjects.select_related("user").first()
            assert await q.eq(1)
            sub.name = "Test Sub"
            await sub.asave()
            assert await q.eq(2)

        # Changing a value that will cascade
        async with async_query_counter() as q:
            assert await q.eq(0)
            sub = await UserSubscription.aobjects.select_related("user").first()
            assert await q.eq(1)
            sub.user.name = "Test"
            assert await q.eq(1)
            await sub.asave(cascade=True)
            assert await q.eq(2)

        # Changing a value and one that will cascade
        async with async_query_counter() as q:
            assert await q.eq(0)
            sub = await UserSubscription.aobjects.select_related("user").first()
            sub.name = "Test Sub 2"
            assert await q.eq(1)
            sub.user.name = "Test 2"
            assert await q.eq(1)
            await sub.asave(cascade=True)
            assert await q.eq(3)  # One for the UserSub and one for the User

        # Saving with just the refs
        async with async_query_counter() as q:
            assert await q.eq(0)
            sub = UserSubscription(user=u1.pk, feed=f1.pk)
            assert await q.eq(0)
            await sub.asave()
            assert await q.eq(1)

        # Saving with just the refs on a ListField
        async with async_query_counter() as q:
            assert await q.eq(0)
            await User(name="Bob", orgs=[o1.pk, o2.pk]).asave()
            assert await q.eq(1)

        # Saving new objects
        async with async_query_counter() as q:
            assert await q.eq(0)
            user = await User.aobjects.first()
            assert await q.eq(1)
            feed = await Feed.aobjects.first()
            assert await q.eq(2)
            sub = UserSubscription(user=user, feed=feed)
            assert await q.eq(2)  # Check no change
            await sub.asave()
            assert await q.eq(3)

    async def test_set_unset_one_operation(self):
        """Ensure that $set and $unset actions are performed in the
        same operation.
        """

        class FooBar(Document):
            foo = StringField(default=None)
            bar = StringField(default=None)

        await FooBar.adrop_collection()

        # write an entity with a single prop
        foo = await FooBar(foo="foo").asave()

        assert foo.foo == "foo"
        del foo.foo
        foo.bar = "bar"

        async with async_query_counter() as q:
            assert await q.eq(0)
            await foo.asave()
            assert await q.eq(1)

    async def test_save_only_changed_fields_recursive(self):
        """Ensure save only sets / unsets changed fields."""

        class Comment(EmbeddedDocument):
            published = BooleanField(default=True)

        class User(self.Person):
            comments_dict = DictField()
            comments = ListField(EmbeddedDocumentField(Comment))
            active = BooleanField(default=True)

        await User.adrop_collection()

        # Create person object and save it to the database
        person = User(name="Test User", age=30, active=True)
        person.comments.append(Comment())
        await person.asave()
        await person.areload()

        person = await self.Person.aobjects.get()
        assert person.comments[0].published

        person.comments[0].published = False
        await person.asave()

        person = await self.Person.aobjects.get()
        assert not person.comments[0].published

        # Simple dict w
        person.comments_dict["first_post"] = Comment()
        await person.asave()

        person = await self.Person.aobjects.get()
        assert person.comments_dict["first_post"].published

        person.comments_dict["first_post"].published = False
        await person.asave()

        person = await self.Person.aobjects.get()
        assert not person.comments_dict["first_post"].published

    @requires_mongodb_gte_44
    async def test_update_propagates_hint_collation_and_comment(self):
        """Make sure adding a hint/comment/collation to the query gets added to the query"""
        base = {"locale": "en", "strength": 2}
        index_name = "name_1"

        class AggPerson(Document):
            name = StringField()
            meta = {
                "indexes": [{"fields": ["name"], "name": index_name, "collation": base}]
            }

        await AggPerson.adrop_collection()
        _ = await AggPerson.aobjects.first()

        comment = "test_comment"

        async with async_db_ops_tracker() as q:
            _ = await AggPerson.aobjects.comment(comment).update_one(name="something")
            query_op = (await ((await q.db).system.profile.find({"ns": f"{MONGO_TEST_DB}.agg_person"})).to_list())[0]
            CMD_QUERY_KEY = "command"
            assert "hint" not in query_op[CMD_QUERY_KEY]
            assert query_op[CMD_QUERY_KEY]["comment"] == comment
            assert "collation" not in query_op[CMD_QUERY_KEY]

        async with async_db_ops_tracker() as q:
            _ = await AggPerson.aobjects.hint(index_name).update_one(name="something")
            query_op = (await ((await q.db).system.profile.find({"ns": f"{MONGO_TEST_DB}.agg_person"})).to_list())[0]
            CMD_QUERY_KEY = "command"
            assert query_op[CMD_QUERY_KEY]["hint"] == {"$hint": index_name}
            assert "comment" not in query_op[CMD_QUERY_KEY]
            assert "collation" not in query_op[CMD_QUERY_KEY]

        async with async_db_ops_tracker() as q:
            _ = await AggPerson.aobjects.collation(base).update_one(name="something")
            query_op = (await ((await q.db).system.profile.find({"ns": f"{MONGO_TEST_DB}.agg_person"})).to_list())[0]
            CMD_QUERY_KEY = "command"
            assert "hint" not in query_op[CMD_QUERY_KEY]
            assert "comment" not in query_op[CMD_QUERY_KEY]
            assert query_op[CMD_QUERY_KEY]["collation"] == base

    async def test_delete(self):
        """Ensure that document may be deleted using the delete method."""
        person = self.Person(name="Test User", age=30)
        await person.asave()
        assert await self.Person.aobjects.count() == 1
        await person.adelete()
        assert await self.Person.aobjects.count() == 0

    @requires_mongodb_gte_44
    async def test_delete_propagates_hint_collation_and_comment(self):
        """Make sure adding a hint/comment/collation to the query gets added to the query"""
        base = {"locale": "en", "strength": 2}
        index_name = "name_1"

        class AggPerson(Document):
            name = StringField()
            meta = {
                "indexes": [{"fields": ["name"], "name": index_name, "collation": base}]
            }

        await AggPerson.adrop_collection()
        _ = await AggPerson.aobjects.first()

        comment = "test_comment"

        async with async_db_ops_tracker() as q:
            _ = await AggPerson.aobjects().comment(comment).delete()
            query_op = (await ((await q.db).system.profile.find({"ns": f"{MONGO_TEST_DB}.agg_person"})).to_list())[0]
            CMD_QUERY_KEY = "command"
            assert "hint" not in query_op[CMD_QUERY_KEY]
            assert query_op[CMD_QUERY_KEY]["comment"] == comment
            assert "collation" not in query_op[CMD_QUERY_KEY]

        async with async_db_ops_tracker() as q:
            _ = await AggPerson.aobjects.hint(index_name).delete()
            query_op = (await ((await q.db).system.profile.find({"ns": f"{MONGO_TEST_DB}.agg_person"})).to_list())[0]
            CMD_QUERY_KEY = "command"
            assert query_op[CMD_QUERY_KEY]["hint"] == {"$hint": index_name}
            assert "comment" not in query_op[CMD_QUERY_KEY]
            assert "collation" not in query_op[CMD_QUERY_KEY]

        async with async_db_ops_tracker() as q:
            _ = await AggPerson.aobjects.collation(base).delete()
            query_op = (await ((await q.db).system.profile.find({"ns": f"{MONGO_TEST_DB}.agg_person"})).to_list())[0]
            CMD_QUERY_KEY = "command"
            assert "hint" not in query_op[CMD_QUERY_KEY]
            assert "comment" not in query_op[CMD_QUERY_KEY]
            assert query_op[CMD_QUERY_KEY]["collation"] == base

    async def test_save_custom_id(self):
        """Ensure that a document may be saved with a custom _id."""

        # Create person object and save it to the database
        person = self.Person(name="Test User", age=30, id="497ce96f395f2f052a494fd4")
        await person.asave()

        # Ensure that the object is in the database with the correct _id
        collection = self.db[self.Person._get_collection_name()]
        person_obj = await collection.find_one({"name": "Test User"})
        assert str(person_obj["_id"]) == "497ce96f395f2f052a494fd4"

    async def test_save_custom_pk(self):
        """Ensure that a document may be saved with a custom _id using
        pk alias.
        """
        # Create person object and save it to the database
        person = self.Person(name="Test User", age=30, pk="497ce96f395f2f052a494fd4")
        await person.asave()

        # Ensure that the object is in the database with the correct _id
        collection = self.db[self.Person._get_collection_name()]
        person_obj = await collection.find_one({"name": "Test User"})
        assert str(person_obj["_id"]) == "497ce96f395f2f052a494fd4"

    async def test_save_list(self):
        """Ensure that a list field may be properly saved."""

        class Comment(EmbeddedDocument):
            content = StringField()

        class BlogPost(Document):
            content = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))
            tags = ListField(StringField())

        await BlogPost.adrop_collection()

        post = BlogPost(content="Went for a walk today...")
        post.tags = tags = ["fun", "leisure"]
        comments = [Comment(content="Good for you"), Comment(content="Yay.")]
        post.comments = comments
        await post.asave()

        collection = self.db[BlogPost._get_collection_name()]
        post_obj = await collection.find_one()
        assert post_obj["tags"] == tags
        for comment_obj, comment in zip(post_obj["comments"], comments):
            assert comment_obj["content"] == comment["content"]

    async def test_list_search_by_embedded(self):
        class User(Document):
            username = StringField(required=True)

            meta = {"allow_inheritance": False}

        class Comment(EmbeddedDocument):
            comment = StringField()
            user = ReferenceField(User, required=True)

            meta = {"allow_inheritance": False}

        class Page(Document):
            comments = ListField(EmbeddedDocumentField(Comment))
            meta = {
                "allow_inheritance": False,
                "indexes": [{"fields": ["comments.user"]}],
            }

        await User.adrop_collection()
        await Page.adrop_collection()

        u1 = User(username="wilson")
        await u1.asave()

        u2 = User(username="rozza")
        await u2.asave()

        u3 = User(username="hmarr")
        await u3.asave()

        p1 = Page(
            comments=[
                Comment(user=u1, comment="Its very good"),
                Comment(user=u2, comment="Hello world"),
                Comment(user=u3, comment="Ping Pong"),
                Comment(user=u1, comment="I like a beer"),
            ]
        )
        await p1.asave()

        p2 = Page(
            comments=[
                Comment(user=u1, comment="Its very good"),
                Comment(user=u2, comment="Hello world"),
            ]
        )
        await p2.asave()

        p3 = Page(comments=[Comment(user=u3, comment="Its very good")])
        await p3.asave()

        p4 = Page(comments=[Comment(user=u2, comment="Heavy Metal song")])
        await p4.asave()

        assert [p1, p2] == await Page.aobjects.filter(comments__user=u1).to_list()
        assert [p1, p2, p4] == await Page.aobjects.filter(comments__user=u2).to_list()
        assert [p1, p3] == await Page.aobjects.filter(comments__user=u3).to_list()

    async def test_save_embedded_document(self):
        """Ensure that a document with an embedded document field may
        be saved in the database.
        """

        class EmployeeDetails(EmbeddedDocument):
            position = StringField()

        class Employee(self.Person):
            salary = IntField()
            details = EmbeddedDocumentField(EmployeeDetails)

        # Create employee object and save it to the database
        employee = Employee(name="Test Employee", age=50, salary=20000)
        employee.details = EmployeeDetails(position="Developer")
        await employee.asave()

        # Ensure that the object is in the database
        collection = self.db[self.Person._get_collection_name()]
        employee_obj = await collection.find_one({"name": "Test Employee"})
        assert employee_obj["name"] == "Test Employee"
        assert employee_obj["age"] == 50

        # Ensure that the 'details' embedded object saved correctly
        assert employee_obj["details"]["position"] == "Developer"

    async def test_embedded_update_after_save(self):
        """Test update of `EmbeddedDocumentField` attached to a newly
        saved document.
        """

        class Page(EmbeddedDocument):
            log_message = StringField(verbose_name="Log message", required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)

        await Site.adrop_collection()
        site = Site(page=Page(log_message="Warning: Dummy message"))
        await site.asave()

        # Update
        site.page.log_message = "Error: Dummy message"
        await site.asave()

        site = await Site.aobjects.first()
        assert site.page.log_message == "Error: Dummy message"

    async def test_updating_an_embedded_document(self):
        """Ensure that a document with an embedded document field may
        be saved in the database.
        """

        class EmployeeDetails(EmbeddedDocument):
            position = StringField()

        class Employee(self.Person):
            salary = IntField()
            details = EmbeddedDocumentField(EmployeeDetails)

        # Create employee object and save it to the database
        employee = Employee(name="Test Employee", age=50, salary=20000)
        employee.details = EmployeeDetails(position="Developer")
        await employee.asave()

        # Test updating an embedded document
        promoted_employee = await Employee.aobjects.get(name="Test Employee")
        promoted_employee.details.position = "Senior Developer"
        await promoted_employee.asave()

        await promoted_employee.areload()
        assert promoted_employee.name == "Test Employee"
        assert promoted_employee.age == 50

        # Ensure that the 'details' embedded object saved correctly
        assert promoted_employee.details.position == "Senior Developer"

        # Test removal
        promoted_employee.details = None
        await promoted_employee.asave()

        await promoted_employee.areload()
        assert promoted_employee.details is None

    async def test_object_mixins(self):
        class NameMixin:
            name = StringField()

        class Foo(EmbeddedDocument, NameMixin):
            quantity = IntField()

        assert ["name", "quantity"] == sorted(Foo._fields.keys())

        class Bar(Document, NameMixin):
            widgets = StringField()

        assert ["id", "name", "widgets"] == sorted(Bar._fields.keys())

    async def test_mixin_inheritance(self):
        class BaseMixIn:
            count = IntField()
            data = StringField()

        class DoubleMixIn(BaseMixIn):
            comment = StringField()

        class TestDoc(Document, DoubleMixIn):
            age = IntField()

        await TestDoc.adrop_collection()
        t = TestDoc(count=12, data="test", comment="great!", age=19)

        await t.asave()

        t = await TestDoc.aobjects.first()

        assert t.age == 19
        assert t.comment == "great!"
        assert t.data == "test"
        assert t.count == 12

    async def test_save_reference(self):
        """Ensure that a document reference field may be saved in the
        database.
        """

        class BlogPost(Document):
            meta = {"collection": "blogpost_1"}
            content = StringField()
            author = ReferenceField(self.Person)

        await BlogPost.adrop_collection()

        author = self.Person(name="Test User")
        await author.asave()

        post = BlogPost(content="Watched some TV today... how exciting.")
        # Should only reference author when saving
        post.author = author
        await post.asave()

        post_obj = await BlogPost.aobjects.select_related("author").first()

        # Test laziness
        assert isinstance(post_obj._data["author"], self.Person)
        assert isinstance(post_obj.author, self.Person)
        assert post_obj.author.name == "Test User"

        # Ensure that the dereferenced object may be changed and saved
        post_obj.author.age = 25
        await post_obj.author.asave()

        author = (await self.Person.aobjects(name="Test User").to_list())[-1]
        assert author.age == 25

    def test_duplicate_db_fields_raise_invalid_document_error(self):
        """Ensure a InvalidDocumentError is thrown if duplicate fields
        declare the same db_field.
        """
        with pytest.raises(InvalidDocumentError):
            class Foo(Document):
                name = StringField()
                name2 = StringField(db_field="name")

    async def test_invalid_son(self):
        """Raise an error if loading invalid data."""

        class Occurrence(EmbeddedDocument):
            number = IntField()

        class Word(Document):
            stem = StringField()
            count = IntField(default=1)
            forms = ListField(StringField(), default=list)
            occurs = ListField(EmbeddedDocumentField(Occurrence), default=list)

        with pytest.raises(InvalidDocumentError):
            Word._from_son(
                {
                    "stem": [1, 2, 3],
                    "forms": 1,
                    "count": "one",
                    "occurs": {"hello": None},
                }
            )

        # Tests for issue #1438: https://github.com/MongoEngine/mongoengine/issues/1438
        with pytest.raises(ValueError):
            Word._from_son("this is not a valid SON dict")

    async def test_reverse_delete_rule_cascade_and_nullify(self):
        """Ensure that a referenced document is also deleted upon
        deletion.
        """

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)
            reviewer = ReferenceField(self.Person, reverse_delete_rule=NULLIFY)

        await self.Person.adrop_collection()
        await BlogPost.adrop_collection()

        author = self.Person(name="Test User")
        await author.asave()

        reviewer = self.Person(name="Re Viewer")
        await reviewer.asave()

        post = BlogPost(content="Watched some TV")
        post.author = author
        post.reviewer = reviewer
        await post.asave()

        await reviewer.adelete()
        # No effect on the BlogPost
        assert await BlogPost.aobjects.count() == 1
        assert (await BlogPost.aobjects.get()).reviewer is None

        # Delete the Person, which should lead to deletion of the BlogPost, too
        await author.adelete()
        assert await BlogPost.aobjects.count() == 0

    async def test_reverse_delete_rule_pull(self):
        """Ensure that a referenced document is also deleted with
        pull.
        """

        class Record(Document):
            name = StringField()
            children = ListField(ReferenceField("self", reverse_delete_rule=PULL))

        await Record.adrop_collection()

        parent_record = await Record(name="parent").asave()
        child_record = await Record(name="child").asave()
        parent_record.children.append(child_record)
        await parent_record.asave()

        await child_record.adelete()
        assert (await Record.aobjects(name="parent").get()).children == []

    async def test_reverse_delete_rule_with_custom_id_field(self):
        """Ensure that a referenced document with custom primary key
        is also deleted upon deletion.
        """

        class User(Document):
            name = StringField(primary_key=True)

        class Book(Document):
            author = ReferenceField(User, reverse_delete_rule=CASCADE)
            reviewer = ReferenceField(User, reverse_delete_rule=NULLIFY)

        await User.adrop_collection()
        await Book.adrop_collection()

        user = await User(name="Mike").asave()
        reviewer = await User(name="John").asave()
        _ = await Book(author=user, reviewer=reviewer).asave()

        await reviewer.adelete()
        assert await Book.aobjects.count() == 1
        assert (await Book.aobjects.get()).reviewer is None

        await user.adelete()
        assert await Book.aobjects.count() == 0

    async def test_reverse_delete_rule_with_shared_id_among_collections(self):
        """Ensure that cascade delete rule doesn't mix id among
        collections.
        """

        class User(Document):
            id = IntField(primary_key=True)

        class Book(Document):
            id = IntField(primary_key=True)
            author = ReferenceField(User, reverse_delete_rule=CASCADE)

        await User.adrop_collection()
        await Book.adrop_collection()

        user_1 = await User(id=1).asave()
        user_2 = await User(id=2).asave()
        _ = await Book(id=1, author=user_2).asave()
        book_2 = await Book(id=2, author=user_1).asave()

        await user_2.adelete()
        # Deleting user_2 should also delete book_1 but not book_2
        assert await Book.aobjects.count() == 1
        assert await Book.aobjects.get() == book_2

        user_3 = await User(id=3).asave()
        _ = await Book(id=3, author=user_3).asave()

        await user_3.adelete()
        # Deleting user_3 should also delete book_3
        assert await Book.aobjects.count() == 1
        assert await Book.aobjects.get() == book_2

    async def test_reverse_delete_rule_with_document_inheritance(self):
        """Ensure that a referenced document is also deleted upon
        deletion of a child document.
        """

        class Writer(self.Person):
            pass

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)
            reviewer = ReferenceField(self.Person, reverse_delete_rule=NULLIFY)

        await self.Person.adrop_collection()
        await BlogPost.adrop_collection()

        author = Writer(name="Test User")
        await author.asave()

        reviewer = Writer(name="Re Viewer")
        await reviewer.asave()

        post = BlogPost(content="Watched some TV")
        post.author = author
        post.reviewer = reviewer
        await post.asave()

        await reviewer.adelete()
        assert await BlogPost.aobjects.count() == 1
        assert (await BlogPost.aobjects.get()).reviewer is None

        # Delete the Writer should lead to deletion of the BlogPost
        await author.adelete()
        assert await BlogPost.aobjects.count() == 0

    async def test_reverse_delete_rule_cascade_and_nullify_complex_field(self):
        """Ensure that a referenced document is also deleted upon
        deletion for complex fields.
        """

        class BlogPost(Document):
            content = StringField()
            authors = ListField(
                ReferenceField(self.Person, reverse_delete_rule=CASCADE)
            )
            reviewers = ListField(
                ReferenceField(self.Person, reverse_delete_rule=NULLIFY)
            )

        await self.Person.adrop_collection()
        await BlogPost.adrop_collection()

        author = self.Person(name="Test User")
        await author.asave()

        reviewer = self.Person(name="Re Viewer")
        await reviewer.asave()

        post = BlogPost(content="Watched some TV")
        post.authors = [author]
        post.reviewers = [reviewer]
        await post.asave()

        # Deleting the reviewer should have no effect on the BlogPost
        await reviewer.adelete()
        assert await BlogPost.aobjects.count() == 1
        assert (await BlogPost.aobjects.get()).reviewers == []

        # Delete the Person, which should lead to deletion of the BlogPost, too
        await author.adelete()
        assert await BlogPost.aobjects.count() == 0

    async def test_reverse_delete_rule_cascade_triggers_pre_delete_signal(self):
        """Ensure the pre_delete signal is triggered upon a cascading
        deletion setup a blog post with content, an author and editor
        delete the author which triggers deletion of blogpost via
        cascade blog post's pre_delete signal alters an editor attribute.
        """

        class Editor(self.Person):
            review_queue = IntField(default=0)

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)
            editor = ReferenceField(Editor)

            @classmethod
            async def pre_delete(cls, sender, document, **kwargs):
                # decrement the docs-to-review count
                await Editor.aobjects(pk=document.editor.pk).update(dec__review_queue=1)

        signals.pre_delete.connect(BlogPost.pre_delete, sender=BlogPost)

        await self.Person.adrop_collection()
        await BlogPost.adrop_collection()
        await Editor.adrop_collection()

        author = await self.Person(name="Will S.").asave()
        editor = await Editor(name="Max P.", review_queue=1).asave()
        await BlogPost(content="wrote some books", author=author, editor=editor).asave()

        # delete the author, the post is also deleted due to the CASCADE rule
        await author.adelete()

        # the pre-delete signal should have decremented the editor's queue
        editor = await Editor.aobjects(name="Max P.").get()
        assert editor.review_queue == 0

    async def test_two_way_reverse_delete_rule(self):
        """Ensure that Bi-Directional relationships work with
        reverse_delete_rule
        """

        class Bar(Document):
            content = StringField()
            foo = ReferenceField("Foo")

        class Foo(Document):
            content = StringField()
            bar = ReferenceField(Bar)

        Bar.register_delete_rule(Foo, "bar", NULLIFY)
        Foo.register_delete_rule(Bar, "foo", NULLIFY)

        await Bar.adrop_collection()
        await Foo.adrop_collection()

        b = Bar(content="Hello")
        await b.asave()

        f = Foo(content="world", bar=b)
        await f.asave()

        b.foo = f
        await b.asave()

        await f.adelete()

        assert await Bar.aobjects.count() == 1  # No effect on the BlogPost
        assert (await Bar.aobjects.get()).foo is None

    async def test_invalid_reverse_delete_rule_raise_errors(self):
        with pytest.raises(InvalidDocumentError):
            class Blog(Document):
                content = StringField()
                authors = MapField(
                    ReferenceField(self.Person, reverse_delete_rule=CASCADE)
                )
                reviewers = DictField(
                    field=ReferenceField(self.Person, reverse_delete_rule=NULLIFY)
                )

        with pytest.raises(InvalidDocumentError):
            class Parents(EmbeddedDocument):
                father = ReferenceField("Person", reverse_delete_rule=DENY)
                mother = ReferenceField("Person", reverse_delete_rule=DENY)

    async def test_reverse_delete_rule_cascade_recurs(self):
        """Ensure that a chain of documents is also deleted upon
        cascaded deletion.
        """

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)

        class Comment(Document):
            text = StringField()
            post = ReferenceField(BlogPost, reverse_delete_rule=CASCADE)

        await self.Person.adrop_collection()
        await BlogPost.adrop_collection()
        await Comment.adrop_collection()

        author = self.Person(name="Test User")
        await author.asave()

        post = BlogPost(content="Watched some TV")
        post.author = author
        await post.asave()

        comment = Comment(text="Kudos.")
        comment.post = post
        await comment.asave()

        # Delete the Person, which should lead to deletion of the BlogPost,
        # and, recursively to the Comment, too
        await author.adelete()
        assert await Comment.aobjects.count() == 0

    async def test_reverse_delete_rule_deny(self):
        """Ensure that a document cannot be referenced if there are
        still documents referring to it.
        """

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=DENY)

        await self.Person.adrop_collection()
        await BlogPost.adrop_collection()

        author = self.Person(name="Test User")
        await author.asave()

        post = BlogPost(content="Watched some TV")
        post.author = author
        await post.asave()

        # Delete the Person should be denied
        with pytest.raises(OperationError):
            await author.adelete()  # Should raise denied error
        assert await BlogPost.aobjects.count() == 1  # No objects may have been deleted
        assert await self.Person.aobjects.count() == 1

        # Other users, that don't have BlogPosts must be removable, like normal
        author = self.Person(name="Another User")
        await author.asave()

        assert await self.Person.aobjects.count() == 2
        await author.adelete()
        assert await self.Person.aobjects.count() == 1

    async def subclasses_and_unique_keys_works(self):
        class A(Document):
            pass

        class B(A):
            foo = BooleanField(unique=True)

        await A.adrop_collection()
        await B.adrop_collection()

        await A().asave()
        await A().asave()
        await B(foo=True).asave()

        assert await A.aobjects.count() == 2
        assert await B.aobjects.count() == 1

    async def test_document_hash(self):
        """Test document in list, dict, set."""

        class User(Document):
            pass

        class BlogPost(Document):
            pass

        # Clear old data
        await User.adrop_collection()
        await BlogPost.adrop_collection()

        u1 = await User.aobjects.create()
        u2 = await User.aobjects.create()
        u3 = await User.aobjects.create()
        u4 = User()  # New object

        b1 = await BlogPost.aobjects.create()
        b2 = await BlogPost.aobjects.create()

        # Make sure docs are properly identified in a list (__eq__ is used
        # for the comparison).
        all_user_list = await User.aobjects.all().to_list()
        assert u1 in all_user_list
        assert u2 in all_user_list
        assert u3 in all_user_list
        assert u4 not in all_user_list  # New object
        assert b1 not in all_user_list  # Other object
        assert b2 not in all_user_list  # Other object

        # Make sure docs can be used as keys in a dict (__hash__ is used
        # for hashing the docs).
        all_user_dic = {}
        async for u in User.aobjects.all():
            all_user_dic[u] = "OK"

        assert all_user_dic.get(u1, False) == "OK"
        assert all_user_dic.get(u2, False) == "OK"
        assert all_user_dic.get(u3, False) == "OK"
        assert all_user_dic.get(u4, False) is False  # New object
        assert all_user_dic.get(b1, False) is False  # Other object
        assert all_user_dic.get(b2, False) is False  # Other object

        # Make sure docs are properly identified in a set (__hash__ is used
        # for hashing the docs).
        all_user_set = set(await User.aobjects.all().to_list())
        assert u1 in all_user_set
        assert u4 not in all_user_set
        assert b1 not in all_user_list
        assert b2 not in all_user_list

        # Make sure duplicate docs aren't accepted in the set
        assert len(all_user_set) == 3
        all_user_set.add(u1)
        all_user_set.add(u2)
        all_user_set.add(u3)
        assert len(all_user_set) == 3

    async def test_picklable(self):
        pickle_doc = PickleTest(number=1, string="One", lists=["1", "2"])
        pickle_doc.embedded = PickleEmbedded()
        pickled_doc = pickle.dumps(
            pickle_doc
        )  # make sure pickling works even before the doc is saved
        await pickle_doc.asave()

        pickled_doc = pickle.dumps(pickle_doc)
        resurrected = pickle.loads(pickled_doc)

        assert resurrected == pickle_doc

        # Test pickling changed data
        pickle_doc.lists.append("3")
        pickled_doc = pickle.dumps(pickle_doc)
        resurrected = pickle.loads(pickled_doc)

        assert resurrected == pickle_doc
        resurrected.string = "Two"
        await resurrected.asave()

        pickle_doc = await PickleTest.aobjects.first()
        assert resurrected == pickle_doc
        assert pickle_doc.string == "Two"
        assert pickle_doc.lists == ["1", "2", "3"]

    async def test_regular_document_pickle(self):
        pickle_doc = PickleTest(number=1, string="One", lists=["1", "2"])
        pickled_doc = pickle.dumps(
            pickle_doc
        )  # make sure pickling works even before the doc is saved
        await pickle_doc.asave()

        pickled_doc = pickle.dumps(pickle_doc)

        # Test that when a document's definition changes the new
        # definition is used
        fixtures.PickleTest = fixtures.NewDocumentPickleTest

        resurrected = pickle.loads(pickled_doc)
        assert resurrected.__class__ == fixtures.NewDocumentPickleTest
        assert (
                resurrected._fields_ordered
                == fixtures.NewDocumentPickleTest._fields_ordered
        )
        assert resurrected._fields_ordered != pickle_doc._fields_ordered

        # The local PickleTest is still a ref to the original
        fixtures.PickleTest = PickleTest

    async def test_dynamic_document_pickle(self):
        pickle_doc = PickleDynamicTest(
            name="test", number=1, string="One", lists=["1", "2"]
        )
        pickle_doc.embedded = PickleDynamicEmbedded(foo="Bar")
        pickled_doc = pickle.dumps(
            pickle_doc
        )  # make sure pickling works even before the doc is saved

        await pickle_doc.asave()

        pickled_doc = pickle.dumps(pickle_doc)
        resurrected = pickle.loads(pickled_doc)

        assert resurrected == pickle_doc
        assert resurrected._fields_ordered == pickle_doc._fields_ordered
        assert resurrected._dynamic_fields.keys() == pickle_doc._dynamic_fields.keys()

        assert resurrected.embedded == pickle_doc.embedded
        assert (
                resurrected.embedded._fields_ordered == pickle_doc.embedded._fields_ordered
        )
        assert (
                resurrected.embedded._dynamic_fields.keys()
                == pickle_doc.embedded._dynamic_fields.keys()
        )

    async def test_picklable_on_signals(self):
        pickle_doc = PickleSignalsTest(number=1, string="One", lists=["1", "2"])
        pickle_doc.embedded = PickleEmbedded()
        await pickle_doc.asave()
        await pickle_doc.adelete()

    async def test_override_method_with_field(self):
        """Test creating a field with a field name that would override
        the "validate" method.
        """
        with pytest.raises(InvalidDocumentError):
            class Blog(Document):
                validate = DictField()

    async def test_mutating_documents(self):
        class B(EmbeddedDocument):
            field1 = StringField(default="field1")

        class A(Document):
            b = EmbeddedDocumentField(B, default=lambda: B())

        await A.adrop_collection()

        a = A()
        await a.asave()
        await a.areload()
        assert a.b.field1 == "field1"

        class C(EmbeddedDocument):
            c_field = StringField(default="cfield")

        class B(EmbeddedDocument):
            field1 = StringField(default="field1")
            field2 = EmbeddedDocumentField(C, default=lambda: C())

        class A(Document):
            b = EmbeddedDocumentField(B, default=lambda: B())

        a = await A.aobjects.first()
        a.b.field2.c_field = "new value"
        await a.asave()

        await a.areload()
        assert a.b.field2.c_field == "new value"

    async def test_can_save_false_values(self):
        """Ensures you can save False values on save."""

        class Doc(Document):
            foo = StringField()
            archived = BooleanField(default=False, required=True)

        await Doc.adrop_collection()

        d = Doc()
        await d.asave()
        d.archived = False
        await d.asave()

        assert await Doc.aobjects(archived=False).count() == 1

    async def test_can_save_false_values_dynamic(self):
        """Ensures you can save False values on dynamic docs."""

        class Doc(DynamicDocument):
            foo = StringField()

        await Doc.adrop_collection()

        d = Doc()
        await d.asave()
        d.archived = False
        await d.asave()

        assert await Doc.aobjects(archived=False).count() == 1

    async def test_do_not_save_unchanged_references(self):
        """Ensures cascading saves dont auto update"""

        class Job(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            age = IntField()
            job = ReferenceField(Job)

        await Job.adrop_collection()
        await Person.adrop_collection()

        job = Job(name="Job 1")
        # job should not have any changed fields after the save
        await job.asave()

        person = Person(name="name", age=10, job=job)

        from pymongo.asynchronous.collection import AsyncCollection

        orig_update_one = AsyncCollection.update_one
        try:

            def fake_update_one(*args, **kwargs):
                self.fail("Unexpected update for %s" % args[0].name)
                return orig_update_one(*args, **kwargs)

            AsyncCollection.update_one = fake_update_one
            await person.asave()
        finally:
            AsyncCollection.update_one = orig_update_one

    async def test_db_alias_tests(self):
        """DB Alias tests."""
        # mongoenginetest - Is default connection alias from setUp()
        # Register Aliases
        await async_register_connection("testdb-1", f"{MONGO_TEST_DB}_2")
        await async_register_connection("testdb-2", f"{MONGO_TEST_DB}_3")
        await async_register_connection("testdb-3", f"{MONGO_TEST_DB}_4")

        class User(Document):
            name = StringField()
            meta = {"db_alias": "testdb-1"}

        class Book(Document):
            name = StringField()
            meta = {"db_alias": "testdb-2"}

        # Drops
        await User.adrop_collection()
        await Book.adrop_collection()

        # Create
        bob = await User.aobjects.create(name="Bob")
        hp = await Book.aobjects.create(name="Harry Potter")

        # Selects
        assert await User.aobjects.first() == bob
        assert await Book.aobjects.first() == hp

        # DeReference
        class AuthorBooks(Document):
            author = ReferenceField(User)
            book = ReferenceField(Book)
            meta = {"db_alias": "testdb-3"}

        # Drops
        await AuthorBooks.adrop_collection()

        ab = await AuthorBooks.aobjects.create(author=bob, book=hp)

        # select
        assert await AuthorBooks.aobjects.select_related("book").first() == ab
        # qs = AuthorBooks.aobjects.select_related("book")
        # pipeline = PipelineBuilder(qs).build()
        with pytest.raises(DoesNotExist):
            (await AuthorBooks.aobjects.select_related("book").first()).book

        with pytest.raises(DoesNotExist):
            (await AuthorBooks.aobjects.select_related("author").first()).author
        assert await AuthorBooks.aobjects.filter(author=bob).first() == ab
        assert await AuthorBooks.aobjects.filter(book=hp).first() == ab

        # DB Alias
        assert await User._async_get_db() == await async_get_db("testdb-1")
        assert await Book._async_get_db() == await async_get_db("testdb-2")
        assert await AuthorBooks._async_get_db() == await async_get_db("testdb-3")

        # Collections
        assert await User._aget_collection() == (await async_get_db("testdb-1"))[User._get_collection_name()]
        assert await Book._aget_collection() == (await async_get_db("testdb-2"))[Book._get_collection_name()]
        assert (
                await AuthorBooks._aget_collection()
                == (await async_get_db("testdb-3"))[AuthorBooks._get_collection_name()]
        )
        await async_disconnect("testdb-1")
        await async_disconnect("testdb-2")
        await async_disconnect("testdb-3")

    async def test_db_alias_overrides(self):
        """Test db_alias can be overriden."""
        # Register a connection with db_alias testdb-2
        await async_register_connection("testdb-2", f"{MONGO_TEST_DB}_2")

        class A(Document):
            """Uses default db_alias"""

            name = StringField()
            meta = {"allow_inheritance": True}

        class B(A):
            """Uses testdb-2 db_alias"""

            meta = {"db_alias": "testdb-2"}

        A.aobjects.all()

        assert "testdb-2" == B._meta.get("db_alias")
        assert MONGO_TEST_DB == (await A._aget_collection()).database.name
        assert f"{MONGO_TEST_DB}_2" == (await B._aget_collection()).database.name
        await async_disconnect("testdb-2")

    async def test_db_alias_propagates(self):
        """db_alias propagates?"""
        await async_register_connection("testdb-1", f"{MONGO_TEST_DB}_2")

        class A(Document):
            name = StringField()
            meta = {"db_alias": "testdb-1", "allow_inheritance": True}

        class B(A):
            pass

        assert "testdb-1" == B._meta.get("db_alias")
        await async_disconnect("testdb-1")

    async def test_db_ref_usage(self):
        """DB Ref usage in dict_fields."""

        class User(Document):
            name = StringField()

        class Book(Document):
            name = StringField()
            author = ReferenceField(User)
            extra = DictField()
            meta = {"ordering": ["+name"]}

            def __unicode__(self):
                return self.name

            def __str__(self):
                return self.name

        # Drops
        await User.adrop_collection()
        await Book.adrop_collection()

        # Authors
        bob = await User.aobjects.create(name="Bob")
        jon = await User.aobjects.create(name="Jon")

        # Redactors
        karl = await User.aobjects.create(name="Karl")
        susan = await User.aobjects.create(name="Susan")
        peter = await User.aobjects.create(name="Peter")

        # Bob
        await Book.aobjects.create(
            name="1",
            author=bob,
            extra={"a": bob.to_dbref(), "b": [karl.to_dbref(), susan.to_dbref()]},
        )
        await Book.aobjects.create(
            name="2", author=bob, extra={"a": bob.to_dbref(), "b": karl.to_dbref()}
        )
        await Book.aobjects.create(
            name="3",
            author=bob,
            extra={"a": bob.to_dbref(), "c": [jon.to_dbref(), peter.to_dbref()]},
        )
        await Book.aobjects.create(name="4", author=bob)

        # Jon
        await Book.aobjects.create(name="5", author=jon)
        await Book.aobjects.create(name="6", author=peter)
        await Book.aobjects.create(name="7", author=jon)
        await Book.aobjects.create(name="8", author=jon)
        await Book.aobjects.create(name="9", author=jon, extra={"a": peter.to_dbref()})

        # Checks
        assert ",".join([str(b) async for b in Book.aobjects.all()]) == "1,2,3,4,5,6,7,8,9"
        # bob related books
        bob_books_qs = Book.aobjects.filter(
            Q(extra__a=bob) | Q(author=bob) | Q(extra__b=bob)
        )
        assert [str(b) async for b in bob_books_qs] == ["1", "2", "3", "4"]
        assert await bob_books_qs.count() == 4

        # Susan & Karl related books
        susan_karl_books_qs = Book.aobjects.filter(
            Q(extra__a__all=[karl, susan])
            | Q(author__all=[karl, susan])
            | Q(extra__b__all=[karl.to_dbref(), susan.to_dbref()])
        )
        assert [str(b) async for b in susan_karl_books_qs] == ["1"]
        assert await susan_karl_books_qs.count() == 1

        # $Where
        custom_qs = Book.aobjects.filter(
            __raw__={
                "$where": """
                                            function(){
                                                return this.name == '1' ||
                                                       this.name == '2';}"""
            }
        )
        assert [str(b) async for b in custom_qs] == ["1", "2"]

    async def test_switch_db_instance(self):
        await async_register_connection("testdb-1", f"{MONGO_TEST_DB}_2")

        class Group(Document):
            name = StringField()

        await Group.adrop_collection()
        with switch_db(Group, "testdb-1") as Group:
            await Group.adrop_collection()
        await Group(name="hello - default").asave()
        assert 1 == await Group.aobjects.count()

        group = await Group.aobjects.first()
        group.switch_db("testdb-1")
        group.name = "hello - testdb!"
        await group.asave()

        with switch_db(Group, "testdb-1") as Group:
            group = await Group.aobjects.first()
            assert "hello - testdb!" == group.name

        group = await Group.aobjects.first()
        assert "hello - default" == group.name

        # Slightly contrived now - perform an update
        # Only works as they have the same object_id
        group.switch_db("testdb-1")
        await group.aupdate(set__name="hello - update")

        with switch_db(Group, "testdb-1") as Group:
            group = await Group.aobjects.first()
            assert "hello - update" == group.name
            await Group.adrop_collection()
            assert 0 == await Group.aobjects.count()

        group = await Group.aobjects.first()
        assert "hello - default" == group.name

        # Totally contrived now - perform a deleted
        # Only works as they have the same object_id
        group.switch_db("testdb-1")
        await group.adelete()

        with switch_db(Group, "testdb-1") as Group:
            assert 0 == await Group.aobjects.count()

        group = await Group.aobjects.first()
        assert "hello - default" == group.name

    async def test_switch_db_multiple_documents_same_context(self):
        await async_register_connection("testdb-1", f"{MONGO_TEST_DB}_2")
        await async_register_connection("testdb-2", f"{MONGO_TEST_DB}_3")

        class Group(Document):
            name = StringField()

        class Post(Document):
            title = StringField()

        # --- clean default db ---
        await Group.adrop_collection()
        await Post.adrop_collection()

        # --- clean testdb-1 for Group ---
        with switch_db(Group, "testdb-1") as Group_1:
            await Group_1.adrop_collection()

        # --- clean testdb-2 for Post ---
        with switch_db(Post, "testdb-2") as Post_2:
            await Post_2.adrop_collection()

        # Seed default DB
        await Group(name="group-default").asave()
        await Post(title="post-default").asave()

        assert 1 == await Group.aobjects.count()
        assert 1 == await Post.aobjects.count()

        # Seed each DB within a *single* combined context
        async with switch_db(Group, "testdb-1"), switch_db(Post, "testdb-2"):
            await Group(name="group-testdb-1").asave()
            await Post(title="post-testdb-2").asave()

            assert 1 == await Group.aobjects.count()
            assert 1 == await Post.aobjects.count()

            g = await Group.aobjects.first()
            p = await Post.aobjects.first()
            assert g.name == "group-testdb-1"
            assert p.title == "post-testdb-2"

        # Outside combined context -> default DB again
        g0 = await Group.aobjects.first()
        p0 = await Post.aobjects.first()
        assert g0.name == "group-default"
        assert p0.title == "post-default"

        # Prove we can still read each switched DB independently
        async with switch_db(Group, "testdb-1"):
            g1 = await Group.aobjects.first()
            assert g1.name == "group-testdb-1"

        async with switch_db(Post, "testdb-2"):
            p2 = await Post.aobjects.first()
            assert p2.title == "post-testdb-2"

    async def test_switch_db_and_switch_collection_instance(self):
        await async_register_connection("testdb-1", f"{MONGO_TEST_DB}_2")

        class Group(Document):
            name = StringField()

        # Clean default + switched locations
        await Group.adrop_collection()
        async with switch_db(Group, "testdb-1"):
            async with switch_collection(Group, "group_alt"):
                await Group.adrop_collection()

        # Seed default (default DB + default collection)
        await Group(name="hello - default").asave()
        assert 1 == await Group.aobjects.count()

        # Switch instance to db+collection and save there
        group = await Group.aobjects.first()
        group.switch_db("testdb-1")
        group.switch_collection("group_alt")
        group.name = "hello - testdb-1/group_alt"
        await group.asave()

        # Read back from switched db+collection
        async with switch_db(Group, "testdb-1"):
            async with switch_collection(Group, "group_alt"):
                g = await Group.aobjects.first()
                assert "hello - testdb-1/group_alt" == g.name

        # Default still unchanged
        g0 = await Group.aobjects.first()
        assert "hello - default" == g0.name

        # Update only in switched db+collection (same object_id assumption)
        g0.switch_db("testdb-1")
        g0.switch_collection("group_alt")
        await g0.aupdate(set__name="hello - update")

        async with switch_db(Group, "testdb-1"):
            async with switch_collection(Group, "group_alt"):
                g = await Group.aobjects.first()
                assert "hello - update" == g.name
                # cleanup switched target only
                await Group.adrop_collection()
                assert 0 == await Group.aobjects.count()

        # Default still intact after dropping switched collection
        g0 = await Group.aobjects.first()
        assert "hello - default" == g0.name

        # Delete in switched target only (same object_id assumption)
        g0.switch_db("testdb-1")
        g0.switch_collection("group_alt")
        await g0.adelete()

        async with switch_db(Group, "testdb-1"):
            async with switch_collection(Group, "group_alt"):
                assert 0 == await Group.aobjects.count()

        # Default still intact
        g0 = await Group.aobjects.first()
        assert "hello - default" == g0.name

    async def test_switch_multiple_db_and_multiple_collection_same_time(self):
        await async_register_connection("tenantA", f"{MONGO_TEST_DB}_2")
        await async_register_connection("tenantB", f"{MONGO_TEST_DB}_2")

        class User(Document):
            name = StringField()

        class Post(Document):
            title = StringField()

        # Clean defaults
        await User.adrop_collection()
        await Post.adrop_collection()

        # Clean switched targets (two different db+collection combos)
        async with switch_db(User, "tenantA"), switch_collection(User, "users_A"):
            await User.adrop_collection()

        async with switch_db(Post, "tenantB"), switch_collection(Post, "posts_B"):
            await Post.adrop_collection()

        # Seed defaults (default DB + default collections)
        await User(name="user-default").asave()
        await Post(title="post-default").asave()

        assert 1 == await User.aobjects.count()
        assert 1 == await Post.aobjects.count()

        # Write to BOTH overrides in the SAME context block
        async with switch_db(User, "tenantA"), switch_collection(User, "users_A"), \
                switch_db(Post, "tenantB"), switch_collection(Post, "posts_B"):
            await User(name="user-A").asave()
            await Post(title="post-B").asave()

            assert 1 == await User.aobjects.count()
            assert 1 == await Post.aobjects.count()

            u = await User.aobjects.first()
            p = await Post.aobjects.first()
            assert u.name == "user-A"
            assert p.title == "post-B"

        # Verify defaults are unchanged after leaving the block
        u0 = await User.aobjects.first()
        p0 = await Post.aobjects.first()
        assert u0.name == "user-default"
        assert p0.title == "post-default"

        # Verify switched locations still have their own data (independently)
        async with switch_db(User, "tenantA"), switch_collection(User, "users_A"):
            assert 1 == await User.aobjects.count()
            u = await User.aobjects.first()
            assert u.name == "user-A"

        async with switch_db(Post, "tenantB"), switch_collection(Post, "posts_B"):
            assert 1 == await Post.aobjects.count()
            p = await Post.aobjects.first()
            assert p.title == "post-B"

        # Cleanup only switched targets (defaults remain)
        async with switch_db(User, "tenantA"), switch_collection(User, "users_A"):
            await User.adrop_collection()
            assert 0 == await User.aobjects.count()

        async with switch_db(Post, "tenantB"), switch_collection(Post, "posts_B"):
            await Post.adrop_collection()
            assert 0 == await Post.aobjects.count()

        # Defaults still intact
        assert 1 == await User.aobjects.count()
        assert 1 == await Post.aobjects.count()
        assert (await User.aobjects.first()).name == "user-default"
        assert (await Post.aobjects.first()).title == "post-default"

    async def test_load_undefined_fields(self):
        class User(Document):
            name = StringField()

        await User.adrop_collection()

        await (await User._aget_collection()).insert_one(
            {"name": "John", "foo": "Bar", "data": [1, 2, 3]}
        )

        with pytest.raises(FieldDoesNotExist):
            await User.aobjects.first()

    async def test_load_undefined_fields_with_strict_false(self):
        class User(Document):
            name = StringField()

            meta = {"strict": False}

        await User.adrop_collection()

        await (await User._aget_collection()).insert_one(
            {"name": "John", "foo": "Bar", "data": [1, 2, 3]}
        )

        user = await User.aobjects.first()
        assert user.name == "John"
        assert not hasattr(user, "foo")
        assert user._data["foo"] == "Bar"
        assert not hasattr(user, "data")
        assert user._data["data"] == [1, 2, 3]

    async def test_load_undefined_fields_on_embedded_document(self):
        class Thing(EmbeddedDocument):
            name = StringField()

        class User(Document):
            name = StringField()
            thing = EmbeddedDocumentField(Thing)

        await User.adrop_collection()

        await (await User._aget_collection()).insert_one(
            {
                "name": "John",
                "thing": {"name": "My thing", "foo": "Bar", "data": [1, 2, 3]},
            }
        )

        with pytest.raises(FieldDoesNotExist):
            await User.aobjects.first()

    async def test_load_undefined_fields_on_embedded_document_with_strict_false_on_doc(self):
        class Thing(EmbeddedDocument):
            name = StringField()

        class User(Document):
            name = StringField()
            thing = EmbeddedDocumentField(Thing)

            meta = {"strict": False}

        await User.adrop_collection()

        await (await User._aget_collection()).insert_one(
            {
                "name": "John",
                "thing": {"name": "My thing", "foo": "Bar", "data": [1, 2, 3]},
            }
        )

        with pytest.raises(FieldDoesNotExist):
            await User.aobjects.first()

    async def test_load_undefined_fields_on_embedded_document_with_strict_false(self):
        class Thing(EmbeddedDocument):
            name = StringField()

            meta = {"strict": False}

        class User(Document):
            name = StringField()
            thing = EmbeddedDocumentField(Thing)

        await User.adrop_collection()

        await (await User._aget_collection()).insert_one(
            {
                "name": "John",
                "thing": {"name": "My thing", "foo": "Bar", "data": [1, 2, 3]},
            }
        )

        user = await User.aobjects.first()
        assert user.name == "John"
        assert user.thing.name == "My thing"
        assert not hasattr(user.thing, "foo")
        assert user.thing._data["foo"] == "Bar"
        assert not hasattr(user.thing, "data")
        assert user.thing._data["data"] == [1, 2, 3]

    async def test_spaces_in_keys(self):
        class Embedded(DynamicEmbeddedDocument):
            pass

        class Doc(DynamicDocument):
            pass

        await Doc.adrop_collection()
        doc = Doc()
        setattr(doc, "hello world", 1)
        await doc.asave()

        one = await Doc.aobjects.filter(**{"hello world": 1}).count()
        assert 1 == one

    async def test_shard_key(self):
        class LogEntry(Document):
            machine = StringField()
            log = StringField()

            meta = {"shard_key": ("machine",)}

        await LogEntry.adrop_collection()

        log = LogEntry()
        log.machine = "Localhost"
        await log.asave()

        assert log.id is not None

        log.log = "Saving"
        await log.asave()

        # try to change the shard key
        with pytest.raises(OperationError):
            log.machine = "127.0.0.1"

    async def test_shard_key_in_embedded_document(self):
        class Foo(EmbeddedDocument):
            foo = StringField()

        class Bar(Document):
            meta = {"shard_key": ("foo.foo",)}
            foo = EmbeddedDocumentField(Foo)
            bar = StringField()

        foo_doc = Foo(foo="hello")
        bar_doc = Bar(foo=foo_doc, bar="world")
        await bar_doc.asave()

        assert bar_doc.id is not None

        bar_doc.bar = "baz"
        await bar_doc.asave()

        # try to change the shard key
        with pytest.raises(OperationError):
            bar_doc.foo.foo = "something"
            await bar_doc.asave()

    async def test_shard_key_primary(self):
        class LogEntry(Document):
            machine = StringField(primary_key=True)
            log = StringField()

            meta = {"shard_key": ("machine",)}

        await LogEntry.adrop_collection()

        log = LogEntry()
        log.machine = "Localhost"
        await log.asave()

        assert log.id is not None

        log.log = "Saving"
        await log.asave()

        # try to change the shard key
        with pytest.raises(OperationError):
            log.machine = "127.0.0.1"

    def test_kwargs_simple(self):
        class Embedded(EmbeddedDocument):
            name = StringField()

        class Doc(Document):
            doc_name = StringField()
            doc = EmbeddedDocumentField(Embedded)

            def __eq__(self, other):
                return self.doc_name == other.doc_name and self.doc == other.doc

        classic_doc = Doc(doc_name="my doc", doc=Embedded(name="embedded doc"))
        dict_doc = Doc(**{"doc_name": "my doc", "doc": {"name": "embedded doc"}})

        assert classic_doc == dict_doc
        assert classic_doc._data == dict_doc._data

    def test_kwargs_complex(self):
        class Embedded(EmbeddedDocument):
            name = StringField()

        class Doc(Document):
            doc_name = StringField()
            docs = ListField(EmbeddedDocumentField(Embedded))

            def __eq__(self, other):
                return self.doc_name == other.doc_name and self.docs == other.docs

        classic_doc = Doc(
            doc_name="my doc",
            docs=[Embedded(name="embedded doc1"), Embedded(name="embedded doc2")],
        )
        dict_doc = Doc(
            **{
                "doc_name": "my doc",
                "docs": [{"name": "embedded doc1"}, {"name": "embedded doc2"}],
            }
        )

        assert classic_doc == dict_doc
        assert classic_doc._data == dict_doc._data

    def test_positional_creation(self):
        """Document cannot be instantiated using positional arguments."""
        with pytest.raises(TypeError) as exc_info:
            self.Person("Test User", 42)

        expected_msg = (
            "Instantiating a document with positional arguments is not "
            "supported. Please use `field_name=value` keyword arguments."
        )
        assert str(exc_info.value) == expected_msg

    def test_mixed_creation(self):
        """Document cannot be instantiated using mixed arguments."""
        with pytest.raises(TypeError) as exc_info:
            self.Person("Test User", age=42)

        expected_msg = (
            "Instantiating a document with positional arguments is not "
            "supported. Please use `field_name=value` keyword arguments."
        )
        assert str(exc_info.value) == expected_msg

    def test_positional_creation_embedded(self):
        """Embedded document cannot be created using positional arguments."""
        with pytest.raises(TypeError) as exc_info:
            self.Job("Test Job", 4)

        expected_msg = (
            "Instantiating a document with positional arguments is not "
            "supported. Please use `field_name=value` keyword arguments."
        )
        assert str(exc_info.value) == expected_msg

    def test_mixed_creation_embedded(self):
        """Embedded document cannot be created using mixed arguments."""
        with pytest.raises(TypeError) as exc_info:
            self.Job("Test Job", years=4)

        expected_msg = (
            "Instantiating a document with positional arguments is not "
            "supported. Please use `field_name=value` keyword arguments."
        )
        assert str(exc_info.value) == expected_msg

    async def test_data_contains_id_field(self):
        """Ensure that asking for _data returns 'id'."""

        class Person(Document):
            name = StringField()

        await Person.adrop_collection()
        await Person(name="Harry Potter").asave()

        person = await Person.aobjects.first()
        assert "id" in person._data.keys()
        assert person._data.get("id") == person.id

    async def test_complex_nesting_document_and_embedded_document(self):
        class Macro(EmbeddedDocument):
            value = DynamicField(default="UNDEFINED")

        class Parameter(EmbeddedDocument):
            macros = MapField(EmbeddedDocumentField(Macro))

            def expand(self):
                self.macros["test"] = Macro()

        class Node(Document):
            parameters = MapField(EmbeddedDocumentField(Parameter))

            def expand(self):
                self.flattened_parameter = {}
                for parameter_name, parameter in self.parameters.items():
                    parameter.expand()

        class NodesSystem(Document):
            name = StringField(required=True)
            nodes = MapField(ReferenceField(Node, dbref=False))

            async def asave(self, *args, **kwargs):
                for node_name, node in self.nodes.items():
                    node.expand()
                    await node.asave(*args, **kwargs)
                await super().asave(*args, **kwargs)

        await NodesSystem.adrop_collection()
        await Node.adrop_collection()

        system = NodesSystem(name="system")
        system.nodes["node"] = Node()
        await system.asave()
        system.nodes["node"].parameters["param"] = Parameter()
        await system.asave()

        system = await NodesSystem.aobjects.select_related("nodes").first()
        assert (
                "UNDEFINED" == system.nodes["node"].parameters["param"].macros["test"].value
        )

    async def test_embedded_document_equality(self):
        class Test(Document):
            field = StringField(required=True)

        class Embedded(EmbeddedDocument):
            ref = ReferenceField(Test)

        await Test.adrop_collection()
        test = await Test(field="123").asave()  # has id

        e = Embedded(ref=test)
        f1 = Embedded._from_son(e.to_mongo())
        f2 = Embedded._from_son(e.to_mongo())

        assert f1 == f2
        f1.ref  # Dereferences lazily
        assert f1 == f2

    async def test_dbref_equality(self):
        class Test2(Document):
            name = StringField()

        class Test3(Document):
            name = StringField()

        class Test(Document):
            name = StringField()
            test2 = ReferenceField("Test2")
            test3 = ReferenceField("Test3")

        await Test.adrop_collection()
        await Test2.adrop_collection()
        await Test3.adrop_collection()

        t2 = Test2(name="a")
        await t2.asave()

        t3 = Test3(name="x")
        t3.id = t2.id
        await t3.asave()

        t = Test(name="b", test2=t2, test3=t3)

        f = Test._from_son(t.to_mongo())

        dbref2 = f._data["test2"]
        obj2 = f.test2
        assert isinstance(dbref2, DBRef)
        assert isinstance(await obj2.afetch(), Test2)
        assert obj2.id == dbref2.id
        assert obj2 == dbref2
        assert dbref2 == obj2

        dbref3 = f._data["test3"]
        obj3 = f.test3
        assert isinstance(dbref3, DBRef)
        assert isinstance(await obj3.afetch(), Test3)
        assert obj3.id == dbref3.id
        assert obj3 == dbref3
        assert dbref3 == obj3

        assert obj2.id == obj3.id
        assert dbref2.id == dbref3.id
        assert dbref2 != dbref3
        assert dbref3 != dbref2
        assert dbref2 != dbref3
        assert dbref3 != dbref2

        assert obj2 != dbref3
        assert dbref3 != obj2
        assert obj2 != dbref3
        assert dbref3 != obj2

        assert obj3 != dbref2
        assert dbref2 != obj3
        assert obj3 != dbref2
        assert dbref2 != obj3

    async def test_default_values_dont_get_override_upon_save_when_only_is_used(self):
        class Person(Document):
            created_on = DateTimeField(default=lambda: datetime.utcnow())
            name = StringField()

        p = Person(name="alon")
        await p.asave()
        orig_created_on = (await Person.aobjects().only("created_on").first()).created_on

        p2 = await Person.aobjects().only("name").first()
        p2.name = "alon2"
        await p2.asave()
        p3 = await Person.aobjects().only("created_on").first()
        assert orig_created_on == p3.created_on

        class Person(Document):
            created_on = DateTimeField(default=lambda: datetime.utcnow())
            name = StringField()
            height = IntField(default=189)

        p4 = await Person.aobjects.first()
        await p4.asave()
        assert p4.height == 189

        # However the default will not be fixed in DB
        assert await Person.aobjects(height=189).count() == 0

        # alter DB for the new default
        coll = await Person._aget_collection()
        async for person in Person.aobjects.as_pymongo():
            if "height" not in person:
                await coll.update_one({"_id": person["_id"]}, {"$set": {"height": 189}})

        assert await Person.aobjects(height=189).count() == 1

    def test_shard_key_mutability_after_from_json(self):
        """Ensure that a document ID can be modified after from_json.

        If you instantiate a document by using from_json/_from_son and you
        indicate that this should be considered a new document (vs a doc that
        already exists in the database), then you should be able to modify
        fields that are part of its shard key (note that this is not permitted
        on docs that are already persisted).

        See https://github.com/mongoengine/mongoengine/issues/771 for details.
        """

        class Person(Document):
            name = StringField()
            age = IntField()
            meta = {"shard_key": ("id", "name")}

        p = Person.from_json('{"name": "name", "age": 27}', created=True)
        assert p._created is True
        p.name = "new name"
        p.id = "12345"
        assert p.name == "new name"
        assert p.id == "12345"

    def test_shard_key_mutability_after_from_son(self):
        """Ensure that a document ID can be modified after _from_son.

        See `test_shard_key_mutability_after_from_json` above for more details.
        """

        class Person(Document):
            name = StringField()
            age = IntField()
            meta = {"shard_key": ("id", "name")}

        p = Person._from_son({"name": "name", "age": 27}, created=True)
        assert p._created is True
        p.name = "new name"
        p.id = "12345"
        assert p.name == "new name"
        assert p.id == "12345"

    async def test_from_json_created_false_without_an_id(self):
        class Person(Document):
            name = StringField()

        await Person.aobjects.delete()

        p = Person.from_json('{"name": "name"}', created=False)
        assert p._created is False
        assert p.id is None

        # Make sure the document is subsequently persisted correctly.
        await p.asave()
        assert p.id is not None
        saved_p = await Person.aobjects.get(id=p.id)
        assert saved_p.name == "name"

    async def test_from_json_created_false_with_an_id(self):
        """See https://github.com/mongoengine/mongoengine/issues/1854"""

        class Person(Document):
            name = StringField()

        await Person.aobjects.delete()

        p = Person.from_json(
            '{"_id": "5b85a8b04ec5dc2da388296e", "name": "name"}', created=False
        )
        assert p._created is False
        assert p._changed_fields == []
        assert p.name == "name"
        assert p.id == ObjectId("5b85a8b04ec5dc2da388296e")
        await p.asave()

        with pytest.raises(DoesNotExist):
            # Since the object is considered as already persisted (thanks to
            # `created=False` and an existing ID), and we haven't changed any
            # fields (i.e. `_changed_fields` is empty), the document is
            # considered unchanged and hence the `save()` call above did
            # nothing.
            await Person.aobjects.get(id=p.id)

        assert not p._created
        p.name = "a new name"
        assert p._changed_fields == ["name"]
        await p.asave()
        saved_p = await Person.aobjects.get(id=p.id)
        assert saved_p.name == p.name

    async def test_from_json_created_true_with_an_id(self):
        class Person(Document):
            name = StringField()

        await Person.aobjects.delete()

        p = Person.from_json(
            '{"_id": "5b85a8b04ec5dc2da388296e", "name": "name"}', created=True
        )
        assert p._created
        assert p._changed_fields == []
        assert p.name == "name"
        assert p.id == ObjectId("5b85a8b04ec5dc2da388296e")
        await p.asave()

        saved_p = await Person.aobjects.get(id=p.id)
        assert saved_p == p
        assert saved_p.name == "name"

    async def test_null_field(self):
        # 734
        class User(Document):
            name = StringField()
            height = IntField(default=184, null=True)
            str_fld = StringField(null=True)
            int_fld = IntField(null=True)
            flt_fld = FloatField(null=True)
            dt_fld = DateTimeField(null=True)
            cdt_fld = ComplexDateTimeField(null=True)

        await User.aobjects.delete()
        u = await User(name="user").asave()
        u_from_db = await User.aobjects.get(name="user")
        u_from_db.height = None
        await u_from_db.asave()
        assert u_from_db.height is None
        # 864
        assert u_from_db.str_fld is None
        assert u_from_db.int_fld is None
        assert u_from_db.flt_fld is None
        assert u_from_db.dt_fld is None
        assert u_from_db.cdt_fld is None

        # 735
        await User.aobjects.delete()
        u = User(name="user")
        await u.asave()
        await User.aobjects(name="user").update_one(set__height=None, upsert=True)
        u_from_db = await User.aobjects.get(name="user")
        assert u_from_db.height is None

    def test_not_saved_eq(self):
        """Ensure we can compare documents not saved."""

        class Person(Document):
            pass

        p = Person()
        p1 = Person()
        assert p != p1
        assert p == p

    async def test_list_iter(self):
        # 914
        class B(EmbeddedDocument):
            v = StringField()

        class A(Document):
            array = ListField(EmbeddedDocumentField(B))

        await A.aobjects.delete()
        await A(array=[B(v="1"), B(v="2"), B(v="3")]).asave()
        a = await A.aobjects.get()
        assert a.array._instance == a
        for idx, b in enumerate(a.array):
            assert b._instance == a
        assert idx == 2

    async def test_updating_listfield_manipulate_list(self):
        class Company(Document):
            name = StringField()
            employees = ListField(field=DictField())

        await Company.adrop_collection()

        comp = Company(name="BigBank", employees=[{"name": "John"}])
        await comp.asave()
        comp.employees.append({"name": "Bill"})
        await comp.asave()

        stored_comp = await async_get_as_pymongo(comp)
        self.assertEqual(
            stored_comp,
            {
                "_id": comp.id,
                "employees": [{"name": "John"}, {"name": "Bill"}],
                "name": "BigBank",
            },
        )

        comp = await comp.areload()
        comp.employees[0]["color"] = "red"
        comp.employees[-1]["color"] = "blue"
        comp.employees[-1].update({"size": "xl"})
        await comp.asave()

        assert len(comp.employees) == 2
        assert comp.employees[0] == {"name": "John", "color": "red"}
        assert comp.employees[1] == {"name": "Bill", "size": "xl", "color": "blue"}

        stored_comp = await async_get_as_pymongo(comp)
        self.assertEqual(
            stored_comp,
            {
                "_id": comp.id,
                "employees": [
                    {"name": "John", "color": "red"},
                    {"size": "xl", "color": "blue", "name": "Bill"},
                ],
                "name": "BigBank",
            },
        )

    async def test_falsey_pk(self):
        """Ensure that we can create and update a document with Falsey PK."""

        class Person(Document):
            age = IntField(primary_key=True)
            height = FloatField()

        person = Person()
        person.age = 0
        person.height = 1.89
        await person.asave()

        await person.aupdate(set__height=2.0)

    async def test_push_with_position(self):
        """Ensure that push with position works properly for an instance."""

        class BlogPost(Document):
            slug = StringField()
            tags = ListField(StringField())

        blog = BlogPost()
        blog.slug = "ABC"
        blog.tags = ["python"]
        await blog.asave()

        await blog.aupdate(push__tags__0=["mongodb", "code"])
        await blog.areload()
        assert blog.tags == ["mongodb", "code", "python"]

    async def test_push_nested_list(self):
        """Ensure that push update works in nested list"""

        class BlogPost(Document):
            slug = StringField()
            tags = ListField()

        blog = await BlogPost(slug="test").asave()
        await blog.aupdate(push__tags=["value1", 123])
        await blog.areload()
        assert blog.tags == [["value1", 123]]

    async def test_accessing_objects_with_indexes_error(self):
        insert_result = await self.db.company.insert_many(
            [{"name": "Foo"}, {"name": "Foo"}]
        )  # Force 2 doc with the same name
        REF_OID = insert_result.inserted_ids[0]
        await self.db.user.insert_one({"company": REF_OID})  # Force 2 doc with same name

        class Company(Document):
            name = StringField(unique=True)

        class User(Document):
            company = ReferenceField(Company)

        # # Ensure index creation exception aren't swallowed (#1688) #todo
        # with pytest.raises(DuplicateKeyError):
        #     await User.aobjects().select_related()

    def test_deepcopy(self):
        regex_field = StringField(regex=r"(^ABC\d\d\d\d$)")
        no_regex_field = StringField()
        # Copy copied field object
        copy.deepcopy(copy.deepcopy(regex_field))
        copy.deepcopy(copy.deepcopy(no_regex_field))
        # Copy same field object multiple times to make sure we restore __deepcopy__ correctly
        copy.deepcopy(regex_field)
        copy.deepcopy(regex_field)
        copy.deepcopy(no_regex_field)
        copy.deepcopy(no_regex_field)

    async def test_deepcopy_with_reference_itself(self):
        class User(Document):
            name = StringField(regex=r"(.*)")
            other_user = ReferenceField("self")

        user1 = await User(name="John").asave()
        await User(name="Bob", other_user=user1).asave()

        user1.other_user = user1
        await user1.asave()
        async for u in User.aobjects.all():
            copied_u = copy.deepcopy(u)
            assert copied_u is not u
            assert copied_u._fields["name"] is u._fields["name"]
            assert (
                    copied_u._fields["name"].regex is u._fields["name"].regex
            )  # Compiled regex objects are atomic

    async def test_embedded_document_failed_while_loading_instance_when_it_is_not_a_dict(
            self,
    ):
        class LightSaber(EmbeddedDocument):
            color = StringField()

        class Jedi(Document):
            light_saber = EmbeddedDocumentField(LightSaber)

        coll = await Jedi._aget_collection()
        await Jedi(light_saber=LightSaber(color="red")).asave()
        _ = await Jedi.aobjects.to_list()  # Ensure a proper document loads without errors

        # Forces a document with a wrong shape (may occur in case of migration)
        value = "I_should_be_a_dict"
        await coll.insert_one({"light_saber": value})

        with pytest.raises(InvalidDocumentError) as exc_info:
            await Jedi.aobjects.to_list()

        assert str(
            exc_info.value
        ) == "Invalid data to create a `Jedi` instance.\nField 'light_saber' - The source SON object needs to be of type 'dict' but a '%s' was found" % type(
            value
        )


class ObjectKeyTestCase(MongoDBAsyncTestCase):
    def test_object_key_simple_document(self):
        class Book(Document):
            title = StringField()

        book = Book(title="Whatever")
        assert book._object_key == {"pk": None}

        book.pk = ObjectId()
        assert book._object_key == {"pk": book.pk}

    def test_object_key_with_custom_primary_key(self):
        class Book(Document):
            isbn = StringField(primary_key=True)
            title = StringField()

        book = Book(title="Sapiens")
        assert book._object_key == {"pk": None}

        book = Book(pk="0062316117")
        assert book._object_key == {"pk": "0062316117"}

    def test_object_key_in_a_sharded_collection(self):
        class Book(Document):
            title = StringField()
            meta = {"shard_key": ("pk", "title")}

        book = Book()
        assert book._object_key == {"pk": None, "title": None}
        book = Book(pk=ObjectId(), title="Sapiens")
        assert book._object_key == {"pk": book.pk, "title": "Sapiens"}

    def test_object_key_with_custom_db_field(self):
        class Book(Document):
            author = StringField(db_field="creator")
            meta = {"shard_key": ("pk", "author")}

        book = Book(pk=ObjectId(), author="Author")
        assert book._object_key == {"pk": book.pk, "author": "Author"}

    def test_object_key_with_nested_shard_key(self):
        class Author(EmbeddedDocument):
            name = StringField()

        class Book(Document):
            author = EmbeddedDocumentField(Author)
            meta = {"shard_key": ("pk", "author.name")}

        book = Book(pk=ObjectId(), author=Author(name="Author"))
        assert book._object_key == {"pk": book.pk, "author__name": "Author"}


class DBFieldMappingTest(MongoDBAsyncTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()

        class Fields:
            w1 = BooleanField(db_field="w2")

            x1 = BooleanField(db_field="x2")
            x2 = BooleanField(db_field="x3")

            y1 = BooleanField(db_field="y0")
            y2 = BooleanField(db_field="y1")

            z1 = BooleanField(db_field="z2")
            z2 = BooleanField(db_field="z1")

        class Doc(Fields, Document):
            pass

        class DynDoc(Fields, DynamicDocument):
            pass

        self.Doc = Doc
        self.DynDoc = DynDoc

    async def asyncTearDown(self):
        for collection in await async_list_collection_names(self.db):
            await self.db.drop_collection(collection)
        await super().asyncTearDown()

    async def test_setting_fields_in_constructor_of_strict_doc_uses_model_names(self):
        doc = self.Doc(z1=True, z2=False)
        assert doc.z1 is True
        assert doc.z2 is False

    async def test_setting_fields_in_constructor_of_dyn_doc_uses_model_names(self):
        doc = self.DynDoc(z1=True, z2=False)
        assert doc.z1 is True
        assert doc.z2 is False

    async def test_setting_unknown_field_in_constructor_of_dyn_doc_does_not_overwrite_model_fields(
            self,
    ):
        doc = self.DynDoc(w2=True)
        assert doc.w1 is None
        assert doc.w2 is True

    async def test_unknown_fields_of_strict_doc_do_not_overwrite_dbfields_1(self):
        doc = self.Doc()
        doc.w2 = True
        doc.x3 = True
        doc.y0 = True
        await doc.asave()
        reloaded = await self.Doc.aobjects.get(id=doc.id)
        assert reloaded.w1 is None
        assert reloaded.x1 is None
        assert reloaded.x2 is None
        assert reloaded.y1 is None
        assert reloaded.y2 is None

    async def test_dbfields_are_loaded_to_the_right_modelfield_for_strict_doc_2(self):
        doc = self.Doc()
        doc.x2 = True
        doc.y2 = True
        doc.z2 = True
        await doc.asave()
        reloaded = await self.Doc.aobjects.get(id=doc.id)
        assert (
                   reloaded.x1,
                   reloaded.x2,
                   reloaded.y1,
                   reloaded.y2,
                   reloaded.z1,
                   reloaded.z2,
               ) == (doc.x1, doc.x2, doc.y1, doc.y2, doc.z1, doc.z2)

    async def test_dbfields_are_loaded_to_the_right_modelfield_for_dyn_doc_2(self):
        doc = self.DynDoc()
        doc.x2 = True
        doc.y2 = True
        doc.z2 = True
        await doc.asave()
        reloaded = await self.DynDoc.aobjects.get(id=doc.id)
        assert (
                   reloaded.x1,
                   reloaded.x2,
                   reloaded.y1,
                   reloaded.y2,
                   reloaded.z1,
                   reloaded.z2,
               ) == (doc.x1, doc.x2, doc.y1, doc.y2, doc.z1, doc.z2)
