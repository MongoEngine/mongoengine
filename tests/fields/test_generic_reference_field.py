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
from tests.utils import MongoDBTestCase, get_as_pymongo


class TestField(MongoDBTestCase):

    def test_generic_reference_field_basics(self):
        """Ensure that a GenericReferenceField properly dereferences items."""

        class Link(Document):
            title = StringField()
            meta = {"allow_inheritance": False}

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField()

        Link.drop_collection()
        Post.drop_collection()
        Bookmark.drop_collection()

        link_1 = Link(title="Pitchfork")
        link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()

        bm = Bookmark(bookmark_object=post_1)
        bm.save()

        bm = Bookmark.objects(bookmark_object=post_1).first()
        assert get_as_pymongo(bm) == {
            "_id": bm.id,
            "bookmark_object": {
                "_cls": "Post",
                "_ref": post_1.to_dbref(),
            },
        }
        assert bm.bookmark_object == post_1
        assert isinstance(bm.bookmark_object, Post)

        bm.bookmark_object = link_1
        bm.save()

        bm = Bookmark.objects(bookmark_object=link_1).first()
        assert get_as_pymongo(bm) == {
            "_id": bm.id,
            "bookmark_object": {
                "_cls": "Link",
                "_ref": link_1.to_dbref(),
            },
        }

        assert bm.bookmark_object == link_1
        assert isinstance(bm.bookmark_object, Link)

    def test_generic_reference_works_with_in_operator(self):
        class SomeObj(Document):
            pass

        class OtherObj(Document):
            obj = GenericReferenceField()

        SomeObj.drop_collection()
        OtherObj.drop_collection()

        s1 = SomeObj().save()
        OtherObj(obj=s1).save()

        # Query using to_dbref
        assert OtherObj.objects(obj__in=[s1.to_dbref()]).count() == 1

        # Query using id
        assert OtherObj.objects(obj__in=[s1.id]).count() == 1

        # Query using document instance
        assert OtherObj.objects(obj__in=[s1]).count() == 1

    def test_generic_reference_list(self):
        """Ensure that a ListField properly dereferences generic references."""

        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class User(Document):
            bookmarks = ListField(GenericReferenceField())

        Link.drop_collection()
        Post.drop_collection()
        User.drop_collection()

        link_1 = Link(title="Pitchfork")
        link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()

        user = User(bookmarks=[post_1, link_1])
        user.save()

        user = User.objects(bookmarks__all=[post_1, link_1]).first()

        assert user.bookmarks[0] == post_1
        assert user.bookmarks[1] == link_1

    def test_generic_reference_document_not_registered(self):
        """Ensure dereferencing out of the document registry throws a
        `NotRegistered` error.
        """

        class Link(Document):
            title = StringField()

        class User(Document):
            bookmarks = ListField(GenericReferenceField())

        Link.drop_collection()
        User.drop_collection()

        link_1 = Link(title="Pitchfork")
        link_1.save()

        user = User(bookmarks=[link_1])
        user.save()

        # Mimic User and Link definitions being in a different file
        # and the Link model not being imported in the User file.
        _DocumentRegistry.unregister("Link")

        user = User.objects.first()
        try:
            user.bookmarks
            raise AssertionError("Link was removed from the registry")
        except NotRegistered:
            pass

    def test_generic_reference_is_none(self):
        class Person(Document):
            name = StringField()
            city = GenericReferenceField()

        Person.drop_collection()

        Person(name="Wilson Jr").save()
        assert repr(Person.objects(city=None)) == "[<Person: Person object>]"

    def test_generic_reference_choices(self):
        """Ensure that a GenericReferenceField can handle choices."""

        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=(Post,))

        Link.drop_collection()
        Post.drop_collection()
        Bookmark.drop_collection()

        link_1 = Link(title="Pitchfork")
        link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()

        bm = Bookmark(bookmark_object=link_1)
        with pytest.raises(ValidationError):
            bm.validate()

        bm = Bookmark(bookmark_object=post_1)
        bm.save()

        bm = Bookmark.objects.first()
        assert bm.bookmark_object == post_1

    def test_generic_reference_string_choices(self):
        """Ensure that a GenericReferenceField can handle choices as strings"""

        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=("Post", Link))

        Link.drop_collection()
        Post.drop_collection()
        Bookmark.drop_collection()

        link_1 = Link(title="Pitchfork")
        link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()

        bm = Bookmark(bookmark_object=link_1)
        bm.save()

        bm = Bookmark(bookmark_object=post_1)
        bm.save()

        bm = Bookmark(bookmark_object=bm)
        with pytest.raises(ValidationError):
            bm.validate()

    def test_generic_reference_choices_no_dereference(self):
        """Ensure that a GenericReferenceField can handle choices on
        non-derefenreced (i.e. DBRef) elements
        """

        class Post(Document):
            title = StringField()

        class Bookmark(Document):
            bookmark_object = GenericReferenceField(choices=(Post,))
            other_field = StringField()

        Post.drop_collection()
        Bookmark.drop_collection()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()

        bm = Bookmark(bookmark_object=post_1)
        bm.save()

        bm = Bookmark.objects.get(id=bm.id)
        # bookmark_object is now a DBRef
        bm.other_field = "dummy_change"
        bm.save()

    def test_generic_reference_list_choices(self):
        """Ensure that a ListField properly dereferences generic references and
        respects choices.
        """

        class Link(Document):
            title = StringField()

        class Post(Document):
            title = StringField()

        class User(Document):
            bookmarks = ListField(GenericReferenceField(choices=(Post,)))

        Link.drop_collection()
        Post.drop_collection()
        User.drop_collection()

        link_1 = Link(title="Pitchfork")
        link_1.save()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()

        user = User(bookmarks=[link_1])
        with pytest.raises(ValidationError):
            user.validate()

        user = User(bookmarks=[post_1])
        user.save()

        user = User.objects.first()
        assert user.bookmarks == [post_1]

    def test_generic_reference_list_item_modification(self):
        """Ensure that modifications of related documents (through generic reference) don't influence on querying"""

        class Post(Document):
            title = StringField()

        class User(Document):
            username = StringField()
            bookmarks = ListField(GenericReferenceField())

        Post.drop_collection()
        User.drop_collection()

        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
        post_1.save()

        user = User(bookmarks=[post_1])
        user.save()

        post_1.title = "Title was modified"
        user.username = "New username"
        user.save()

        user = User.objects(bookmarks__all=[post_1]).first()

        assert user is not None
        assert user.bookmarks[0] == post_1

    def test_generic_reference_filter_by_dbref(self):
        """Ensure we can search for a specific generic reference by
        providing its ObjectId.
        """

        class Doc(Document):
            ref = GenericReferenceField()

        Doc.drop_collection()

        doc1 = Doc.objects.create()
        doc2 = Doc.objects.create(ref=doc1)

        doc = Doc.objects.get(ref=DBRef("doc", doc1.pk))
        assert doc == doc2

    def test_generic_reference_is_not_tracked_in_parent_doc(self):
        """Ensure that modifications of related documents (through generic reference) don't influence
        the owner changed fields (#1934)
        """

        class Doc1(Document):
            name = StringField()

        class Doc2(Document):
            ref = GenericReferenceField()
            refs = ListField(GenericReferenceField())

        Doc1.drop_collection()
        Doc2.drop_collection()

        doc1 = Doc1(name="garbage1").save()
        doc11 = Doc1(name="garbage11").save()
        doc2 = Doc2(ref=doc1, refs=[doc11]).save()

        doc2.ref.name = "garbage2"
        assert doc2._get_changed_fields() == []

        doc2.refs[0].name = "garbage3"
        assert doc2._get_changed_fields() == []
        assert doc2._delta() == ({}, {})

    def test_generic_reference_field(self):
        """Ensure we can search for a specific generic reference by
        providing its DBRef.
        """

        class Doc(Document):
            ref = GenericReferenceField()

        Doc.drop_collection()

        doc1 = Doc.objects.create()
        doc2 = Doc.objects.create(ref=doc1)

        assert isinstance(doc1.pk, ObjectId)

        doc = Doc.objects.get(ref=doc1.pk)
        assert doc == doc2
