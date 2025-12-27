import pytest
from bson import DBRef, ObjectId

from mongoengine import (
    Document,
    GenericReferenceField,
    ListField,
    NotRegistered,
    StringField,
    ValidationError,
)
from mongoengine.base import _DocumentRegistry
from tests.asynchronous.utils import MongoDBAsyncTestCase, async_get_as_pymongo


class TestField(MongoDBAsyncTestCase):

    async def test_generic_reference_field_basics(self):
        """Ensure that a GenericReferenceField properly dereferences items."""

        class Link(Document):
            title = StringField()
            meta = {"allow_inheritance": False}

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=(Post, Link,))

        await Link.adrop_collection()
        await Post.adrop_collection()
        await Bookmark.adrop_collection()

        link_1 = Link(title="Pitchfork")
        await link_1.asave()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.asave()

        bm = Bookmark(bookmark_object=post_1)
        await bm.asave()

        bm = await Bookmark.aobjects(bookmark_object=post_1).select_related("bookmark_object").first()
        assert await async_get_as_pymongo(bm) == {
            "_id": bm.id,
            "bookmark_object": {
                "_cls": "Post",
                "_ref": post_1.to_dbref(),
            },
        }
        assert bm.bookmark_object == post_1
        assert isinstance(bm.bookmark_object, Post)

        bm.bookmark_object = link_1
        await bm.asave()

        bm = await Bookmark.aobjects(bookmark_object=link_1).select_related("bookmark_object").first()
        assert await async_get_as_pymongo(bm, select_related="bookmark_object") == {
            "_id": bm.id,
            "bookmark_object": {'_cls': 'Link', '_id': link_1.pk,
                                '_ref': link_1.to_dbref(),
                                'title': 'Pitchfork'}
        }

        assert bm.bookmark_object == link_1
        assert isinstance(bm.bookmark_object, Link)

    async def test_generic_reference_works_with_in_operator(self):
        class SomeObj(Document):
            pass

        class OtherObj(Document):
            obj = GenericReferenceField(choices=(SomeObj,))

        await SomeObj.adrop_collection()
        await OtherObj.adrop_collection()

        s1 = await SomeObj().asave()
        await OtherObj(obj=s1).asave()

        # Query using to_dbref
        assert await OtherObj.aobjects(obj__in=[s1.to_dbref()]).count() == 1

        # Query using id
        assert await OtherObj.aobjects(obj__in=[s1.id]).count() == 1

        # Query using document instance
        assert await OtherObj.aobjects(obj__in=[s1]).count() == 1

    async def test_generic_reference_list(self):
        """Ensure that a ListField properly dereferences generic references."""

        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class User(Document):
            bookmarks = ListField(GenericReferenceField(choices=(Post, Link,)))

        await Link.adrop_collection()
        await Post.adrop_collection()
        await User.adrop_collection()

        link_1 = Link(title="Pitchfork")
        await link_1.asave()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.asave()

        user = User(bookmarks=[post_1, link_1])
        await user.asave()

        user = await User.aobjects(bookmarks__all=[post_1, link_1]).select_related("bookmarks").first()

        assert user.bookmarks[0] == post_1
        assert user.bookmarks[1] == link_1

    async def test_generic_reference_document_not_registered(self):
        """Ensure dereferencing out of the document registry throws a
        `NotRegistered` error.
        """

        class Link(Document):
            title = StringField()

        class User(Document):
            bookmarks = ListField(GenericReferenceField(choices=(Link,)))

        await Link.adrop_collection()
        await User.adrop_collection()

        link_1 = Link(title="Pitchfork")
        await link_1.asave()

        user = User(bookmarks=[link_1])
        await user.asave()

        # Mimic User and Link definitions being in a different file
        # and the Link model not being imported in the User file.
        _DocumentRegistry.unregister("Link")

        try:
            await User.aobjects.select_related("bookmarks").first()
            raise AssertionError("Link was removed from the registry")
        except NotRegistered:
            pass

    async def test_generic_reference_is_none(self):

        class City(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            city = GenericReferenceField(choices=(City,))

        await Person.adrop_collection()

        person = await Person(name="Wilson Jr").asave()
        assert await Person.aobjects(city=None).to_list() == [person]

    async def test_generic_reference_choices(self):
        """Ensure that a GenericReferenceField can handle choices."""

        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=(Post,))

        await Link.adrop_collection()
        await Post.adrop_collection()
        await Bookmark.adrop_collection()

        link_1 = Link(title="Pitchfork")
        await link_1.asave()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.asave()

        bm = Bookmark(bookmark_object=link_1)
        with pytest.raises(ValidationError):
            bm.validate()

        bm = Bookmark(bookmark_object=post_1)
        await bm.asave()

        bm = await Bookmark.aobjects.select_related("bookmark_object").first()
        assert bm.bookmark_object == post_1

    async def test_generic_reference_string_choices(self):
        """Ensure that a GenericReferenceField can handle choices as strings"""

        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=("Post", Link))

        await Link.adrop_collection()
        await Post.adrop_collection()
        await Bookmark.adrop_collection()

        link_1 = Link(title="Pitchfork")
        await link_1.asave()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.asave()

        bm = Bookmark(bookmark_object=link_1)
        await bm.asave()

        bm = Bookmark(bookmark_object=post_1)
        await bm.asave()

        bm = Bookmark(bookmark_object=bm)
        with pytest.raises(ValidationError):
            bm.validate()

    async def test_generic_reference_choices_no_dereference(self):
        """Ensure that a GenericReferenceField can handle choices on
        non-derefenreced (i.e. DBRef) elements
        """

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=(Post,))
            other_field = StringField()

        await Post.adrop_collection()
        await Bookmark.adrop_collection()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.asave()

        bm_ = Bookmark(bookmark_object=post_1)
        await bm_.asave()

        bm = await Bookmark.aobjects.get(id=bm_.id)
        assert bm.bookmark_object.value == {"_ref": DBRef("post", post_1.id), "_cls": "Post"}
        bm.other_field = "dummy_change"
        await bm.asave()

    async def test_generic_reference_list_choices(self):
        """Ensure that a ListField properly dereferences generic references and
        respects choices.
        """

        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class User(Document):
            bookmarks = ListField(GenericReferenceField(choices=(Post,)))

        await Link.adrop_collection()
        await Post.adrop_collection()
        await User.adrop_collection()

        link_1 = Link(title="Pitchfork")
        await link_1.asave()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.asave()

        user = User(bookmarks=[link_1])
        with pytest.raises(ValidationError):
            user.validate()

        user = User(bookmarks=[post_1])
        await user.asave()

        user = await User.aobjects.select_related("bookmarks").first()
        assert user.bookmarks == [post_1]

    async def test_generic_reference_list_item_modification(self):
        """Ensure that modifications of related documents (through generic reference) don't influence on querying"""

        class Post(Document):
            title = StringField()

        class User(Document):
            username = StringField()
            bookmarks = ListField(GenericReferenceField(choices=(Post,)))

        await Post.adrop_collection()
        await User.adrop_collection()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        await post_1.asave()

        user = User(bookmarks=[post_1])
        await user.asave()

        post_1.title = "Title was modified"
        user.username = "New username"
        await user.asave()

        user = await User.aobjects(bookmarks__all=[post_1]).select_related("bookmarks").first()

        assert user is not None
        assert user.bookmarks[0] == post_1

    async def test_generic_reference_filter_by_dbref(self):
        """Ensure we can search for a specific generic reference by
        providing its ObjectId.
        """

        class Doc(Document):
            ref = GenericReferenceField(choices=('Doc',))

        await Doc.adrop_collection()

        doc1 = await Doc.aobjects.create()
        doc2 = await Doc.aobjects.create(ref=doc1)

        doc = await Doc.aobjects.get(ref=DBRef("doc", doc1.pk))
        assert doc == doc2

    async def test_generic_reference_is_not_tracked_in_parent_doc(self):
        """Ensure that modifications of related documents (through generic reference) don't influence
        the owner changed fields (#1934)
        """

        class Doc1(Document):
            name = StringField()

        class Doc2(Document):
            ref = GenericReferenceField(choices=(Doc1,))
            refs = ListField(GenericReferenceField(choices=(Doc1,)))

        await Doc1.adrop_collection()
        await Doc2.adrop_collection()

        doc1 = await Doc1(name="garbage1").asave()
        doc11 = await Doc1(name="garbage11").asave()
        doc2 = await Doc2(ref=doc1, refs=[doc11]).asave()

        doc2.ref.name = "garbage2"
        assert doc2._get_changed_fields() == []

        doc2.refs[0].name = "garbage3"
        assert doc2._get_changed_fields() == []
        assert doc2._delta() == ({}, {})

    async def test_generic_reference_field(self):
        """Ensure we can search for a specific generic reference by
        providing its DBRef.
        """

        class Doc(Document):
            ref = GenericReferenceField(choices=('Doc',))

        await Doc.adrop_collection()

        doc1 = await Doc.aobjects.create()
        doc2 = await Doc.aobjects.create(ref=doc1)

        assert isinstance(doc1.pk, ObjectId)

        doc = await Doc.aobjects.get(ref=doc1.pk)
        assert doc == doc2
