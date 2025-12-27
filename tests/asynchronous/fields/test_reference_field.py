import pytest
from bson import SON, DBRef

from mongoengine import *
from tests.asynchronous.utils import MongoDBAsyncTestCase


class TestReferenceField(MongoDBAsyncTestCase):
    async def test_reference_field_fails_init_wrong_document_type(self):
        class User(Document):
            name = StringField()

        ERROR_MSG = "Argument to ReferenceField constructor must be a document class or a string"
        # fails if given an instance
        with pytest.raises(ValidationError, match=ERROR_MSG):
            class Test(Document):
                author = ReferenceField(User())

        class NonDocumentSubClass:
            pass

        # fails if given a non Document subclass
        with pytest.raises(ValidationError, match=ERROR_MSG):
            class Test(Document):  # noqa: F811
                author = ReferenceField(NonDocumentSubClass)

    async def test_reference_validation(self):
        """Ensure that invalid document objects cannot be assigned to
        reference fields.
        """

        class User(Document):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(User)

        await User.adrop_collection()
        await BlogPost.adrop_collection()

        # Make sure ReferenceField only accepts a document class or a string
        # with a document class name.
        with pytest.raises(ValidationError):
            ReferenceField(EmbeddedDocument)

        unsaved_user = User(name="Test User")

        # Ensure that the referenced object must have been saved
        post1 = BlogPost(content="Chips and gravy taste good.")
        post1.author = unsaved_user
        expected_error = (
            "The instance of the document 'User' you are "
            "trying to reference has an empty 'id'. You can only reference "
            "documents once they have been saved to the database"
        )
        with pytest.raises(ValidationError, match=expected_error):
            await post1.asave()

        # Check that an invalid object type cannot be used
        post2 = BlogPost(content="Chips and chilli taste good.")
        post1.author = post2
        with pytest.raises(ValidationError):
            post1.validate()

        # Ensure ObjectID's are accepted as references
        user = User(name="Test User")
        user_object_id = user.pk
        post3 = BlogPost(content="Chips and curry sauce taste good.")
        post3.author = user_object_id
        await post3.asave()

        # Make sure referencing a saved document of the right type works
        await user.asave()
        post1.author = user
        await post1.asave()

        # Make sure referencing a saved document of the *wrong* type fails
        await post2.asave()
        post1.author = post2
        with pytest.raises(ValidationError):
            post1.validate()

    async def test_dbref_reference_fields(self):
        """Make sure storing references as bson.dbref.DBRef works."""

        class Person(Document):
            name = StringField()
            parent = ReferenceField("self", dbref=True)

        await Person.adrop_collection()

        p1 = await Person(name="John").asave()
        await Person(name="Ross", parent=p1).asave()

        assert (await (await Person._aget_collection()).find_one({"name": "Ross"}))["parent"] == DBRef(
            "person", p1.pk
        )

        p = await Person.aobjects.get(name="Ross")
        assert p.parent == p1

    async def test_dbref_to_mongo(self):
        """Make sure that calling to_mongo on a ReferenceField which
        has dbref=False, but actually actually contains a DBRef returns
        an ID of that DBRef.
        """

        class Person(Document):
            name = StringField()
            parent = ReferenceField("self", dbref=False)

        p = Person(name="Steve", parent=DBRef("person", "abcdefghijklmnop"))
        assert p.to_mongo() == SON([("name", "Steve"), ("parent", "abcdefghijklmnop")])

    async def test_objectid_reference_fields(self):
        class Person(Document):
            name = StringField()
            parent = ReferenceField("self", dbref=False)

        await Person.adrop_collection()

        p1 = await Person(name="John").asave()
        await Person(name="Ross", parent=p1).asave()

        col = await Person._aget_collection()
        data = await col.find_one({"name": "Ross"})
        assert data["parent"] == p1.pk

        p = await Person.aobjects.get(name="Ross")
        assert p.parent == p1

    async def test_undefined_reference(self):
        """Ensure that ReferenceFields may reference undefined Documents."""

        class Product(Document):
            name = StringField()
            company = ReferenceField("Company")

        class Company(Document):
            name = StringField()

        await Product.adrop_collection()
        await Company.adrop_collection()

        ten_gen = Company(name="10gen")
        await ten_gen.asave()
        mongodb = Product(name="MongoDB", company=ten_gen)
        await mongodb.asave()

        me = Product(name="MongoEngine")
        await me.asave()

        obj = await Product.aobjects(company=ten_gen).first()
        assert obj == mongodb
        assert obj.company == ten_gen

        obj = await Product.aobjects(company=None).first()
        assert obj == me

        obj = await Product.aobjects.get(company=None)
        assert obj == me

    async def test_reference_query_conversion_dbref(self):
        """Ensure that ReferenceFields can be queried using objects and values
        of the type of the primary key of the referenced object.
        """

        class Member(Document):
            user_num = IntField(primary_key=True)

        class BlogPost(Document):
            title = StringField()
            author = ReferenceField(Member, dbref=True)

        await Member.adrop_collection()
        await BlogPost.adrop_collection()

        m1 = Member(user_num=1)
        await m1.asave()
        m2 = Member(user_num=2)
        await m2.asave()

        post1 = BlogPost(title="post 1", author=m1)
        await post1.asave()

        post2 = BlogPost(title="post 2", author=m2)
        await post2.asave()

        post = await BlogPost.aobjects(author=m1).first()
        assert post.id == post1.id

        post = await BlogPost.aobjects(author=m2).first()
        assert post.id == post2.id
