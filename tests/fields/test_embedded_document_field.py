import pytest

from mongoengine import (
    Document,
    EmbeddedDocument,
    EmbeddedDocumentField,
    GenericEmbeddedDocumentField,
    IntField,
    InvalidQueryError,
    ListField,
    LookUpError,
    StringField,
    ValidationError,
)

from tests.utils import MongoDBTestCase


class TestEmbeddedDocumentField(MongoDBTestCase):
    def test___init___(self):
        class MyDoc(EmbeddedDocument):
            name = StringField()

        field = EmbeddedDocumentField(MyDoc)
        assert field.document_type_obj == MyDoc

        field2 = EmbeddedDocumentField("MyDoc")
        assert field2.document_type_obj == "MyDoc"

    def test___init___throw_error_if_document_type_is_not_EmbeddedDocument(self):
        with pytest.raises(ValidationError):
            EmbeddedDocumentField(dict)

    def test_document_type_throw_error_if_not_EmbeddedDocument_subclass(self):
        class MyDoc(Document):
            name = StringField()

        emb = EmbeddedDocumentField("MyDoc")
        with pytest.raises(ValidationError) as exc_info:
            emb.document_type
        assert (
            "Invalid embedded document class provided to an EmbeddedDocumentField"
            in str(exc_info.value)
        )

    def test_embedded_document_field_only_allow_subclasses_of_embedded_document(self):
        # Relates to #1661
        class MyDoc(Document):
            name = StringField()

        with pytest.raises(ValidationError):

            class MyFailingDoc(Document):
                emb = EmbeddedDocumentField(MyDoc)

        with pytest.raises(ValidationError):

            class MyFailingdoc2(Document):
                emb = EmbeddedDocumentField("MyDoc")

    def test_query_embedded_document_attribute(self):
        class AdminSettings(EmbeddedDocument):
            foo1 = StringField()
            foo2 = StringField()

        class Person(Document):
            settings = EmbeddedDocumentField(AdminSettings)
            name = StringField()

        Person.drop_collection()

        p = Person(settings=AdminSettings(foo1="bar1", foo2="bar2"), name="John").save()

        # Test non exiting attribute
        with pytest.raises(InvalidQueryError) as exc_info:
            Person.objects(settings__notexist="bar").first()
        assert str(exc_info.value) == 'Cannot resolve field "notexist"'

        with pytest.raises(LookUpError):
            Person.objects.only("settings.notexist")

        # Test existing attribute
        assert Person.objects(settings__foo1="bar1").first().id == p.id
        only_p = Person.objects.only("settings.foo1").first()
        assert only_p.settings.foo1 == p.settings.foo1
        assert only_p.settings.foo2 is None
        assert only_p.name is None

        exclude_p = Person.objects.exclude("settings.foo1").first()
        assert exclude_p.settings.foo1 is None
        assert exclude_p.settings.foo2 == p.settings.foo2
        assert exclude_p.name == p.name

    def test_query_embedded_document_attribute_with_inheritance(self):
        class BaseSettings(EmbeddedDocument):
            meta = {"allow_inheritance": True}
            base_foo = StringField()

        class AdminSettings(BaseSettings):
            sub_foo = StringField()

        class Person(Document):
            settings = EmbeddedDocumentField(BaseSettings)

        Person.drop_collection()

        p = Person(settings=AdminSettings(base_foo="basefoo", sub_foo="subfoo"))
        p.save()

        # Test non exiting attribute
        with pytest.raises(InvalidQueryError) as exc_info:
            assert Person.objects(settings__notexist="bar").first().id == p.id
        assert str(exc_info.value) == 'Cannot resolve field "notexist"'

        # Test existing attribute
        assert Person.objects(settings__base_foo="basefoo").first().id == p.id
        assert Person.objects(settings__sub_foo="subfoo").first().id == p.id

        only_p = Person.objects.only("settings.base_foo", "settings._cls").first()
        assert only_p.settings.base_foo == "basefoo"
        assert only_p.settings.sub_foo is None

    def test_query_list_embedded_document_with_inheritance(self):
        class Post(EmbeddedDocument):
            title = StringField(max_length=120, required=True)
            meta = {"allow_inheritance": True}

        class TextPost(Post):
            content = StringField()

        class MoviePost(Post):
            author = StringField()

        class Record(Document):
            posts = ListField(EmbeddedDocumentField(Post))

        record_movie = Record(posts=[MoviePost(author="John", title="foo")]).save()
        record_text = Record(posts=[TextPost(content="a", title="foo")]).save()

        records = list(Record.objects(posts__author=record_movie.posts[0].author))
        assert len(records) == 1
        assert records[0].id == record_movie.id

        records = list(Record.objects(posts__content=record_text.posts[0].content))
        assert len(records) == 1
        assert records[0].id == record_text.id

        assert Record.objects(posts__title="foo").count() == 2


class TestGenericEmbeddedDocumentField(MongoDBTestCase):
    def test_generic_embedded_document(self):
        class Car(EmbeddedDocument):
            name = StringField()

        class Dish(EmbeddedDocument):
            food = StringField(required=True)
            number = IntField()

        class Person(Document):
            name = StringField()
            like = GenericEmbeddedDocumentField()

        Person.drop_collection()

        person = Person(name="Test User")
        person.like = Car(name="Fiat")
        person.save()

        person = Person.objects.first()
        assert isinstance(person.like, Car)

        person.like = Dish(food="arroz", number=15)
        person.save()

        person = Person.objects.first()
        assert isinstance(person.like, Dish)

    def test_generic_embedded_document_choices(self):
        """Ensure you can limit GenericEmbeddedDocument choices."""

        class Car(EmbeddedDocument):
            name = StringField()

        class Dish(EmbeddedDocument):
            food = StringField(required=True)
            number = IntField()

        class Person(Document):
            name = StringField()
            like = GenericEmbeddedDocumentField(choices=(Dish,))

        Person.drop_collection()

        person = Person(name="Test User")
        person.like = Car(name="Fiat")
        with pytest.raises(ValidationError):
            person.validate()

        person.like = Dish(food="arroz", number=15)
        person.save()

        person = Person.objects.first()
        assert isinstance(person.like, Dish)

    def test_generic_list_embedded_document_choices(self):
        """Ensure you can limit GenericEmbeddedDocument choices inside
        a list field.
        """

        class Car(EmbeddedDocument):
            name = StringField()

        class Dish(EmbeddedDocument):
            food = StringField(required=True)
            number = IntField()

        class Person(Document):
            name = StringField()
            likes = ListField(GenericEmbeddedDocumentField(choices=(Dish,)))

        Person.drop_collection()

        person = Person(name="Test User")
        person.likes = [Car(name="Fiat")]
        with pytest.raises(ValidationError):
            person.validate()

        person.likes = [Dish(food="arroz", number=15)]
        person.save()

        person = Person.objects.first()
        assert isinstance(person.likes[0], Dish)

    def test_choices_validation_documents(self):
        """
        Ensure fields with document choices validate given a valid choice.
        """

        class UserComments(EmbeddedDocument):
            author = StringField()
            message = StringField()

        class BlogPost(Document):
            comments = ListField(GenericEmbeddedDocumentField(choices=(UserComments,)))

        # Ensure Validation Passes
        BlogPost(comments=[UserComments(author="user2", message="message2")]).save()

    def test_choices_validation_documents_invalid(self):
        """
        Ensure fields with document choices validate given an invalid choice.
        This should throw a ValidationError exception.
        """

        class UserComments(EmbeddedDocument):
            author = StringField()
            message = StringField()

        class ModeratorComments(EmbeddedDocument):
            author = StringField()
            message = StringField()

        class BlogPost(Document):
            comments = ListField(GenericEmbeddedDocumentField(choices=(UserComments,)))

        # Single Entry Failure
        post = BlogPost(comments=[ModeratorComments(author="mod1", message="message1")])
        with pytest.raises(ValidationError):
            post.save()

        # Mixed Entry Failure
        post = BlogPost(
            comments=[
                ModeratorComments(author="mod1", message="message1"),
                UserComments(author="user2", message="message2"),
            ]
        )
        with pytest.raises(ValidationError):
            post.save()

    def test_choices_validation_documents_inheritance(self):
        """
        Ensure fields with document choices validate given subclass of choice.
        """

        class Comments(EmbeddedDocument):
            meta = {"abstract": True}
            author = StringField()
            message = StringField()

        class UserComments(Comments):
            pass

        class BlogPost(Document):
            comments = ListField(GenericEmbeddedDocumentField(choices=(Comments,)))

        # Save Valid EmbeddedDocument Type
        BlogPost(comments=[UserComments(author="user2", message="message2")]).save()

    def test_query_generic_embedded_document_attribute(self):
        class AdminSettings(EmbeddedDocument):
            foo1 = StringField()

        class NonAdminSettings(EmbeddedDocument):
            foo2 = StringField()

        class Person(Document):
            settings = GenericEmbeddedDocumentField(
                choices=(AdminSettings, NonAdminSettings)
            )

        Person.drop_collection()

        p1 = Person(settings=AdminSettings(foo1="bar1")).save()
        p2 = Person(settings=NonAdminSettings(foo2="bar2")).save()

        # Test non exiting attribute
        with pytest.raises(InvalidQueryError) as exc_info:
            Person.objects(settings__notexist="bar").first()
        assert str(exc_info.value) == 'Cannot resolve field "notexist"'

        with pytest.raises(LookUpError):
            Person.objects.only("settings.notexist")

        # Test existing attribute
        assert Person.objects(settings__foo1="bar1").first().id == p1.id
        assert Person.objects(settings__foo2="bar2").first().id == p2.id

    def test_query_generic_embedded_document_attribute_with_inheritance(self):
        class BaseSettings(EmbeddedDocument):
            meta = {"allow_inheritance": True}
            base_foo = StringField()

        class AdminSettings(BaseSettings):
            sub_foo = StringField()

        class Person(Document):
            settings = GenericEmbeddedDocumentField(choices=[BaseSettings])

        Person.drop_collection()

        p = Person(settings=AdminSettings(base_foo="basefoo", sub_foo="subfoo"))
        p.save()

        # Test non exiting attribute
        with pytest.raises(InvalidQueryError) as exc_info:
            assert Person.objects(settings__notexist="bar").first().id == p.id
        assert str(exc_info.value) == 'Cannot resolve field "notexist"'

        # Test existing attribute
        assert Person.objects(settings__base_foo="basefoo").first().id == p.id
        assert Person.objects(settings__sub_foo="subfoo").first().id == p.id
