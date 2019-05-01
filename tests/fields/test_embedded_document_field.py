# -*- coding: utf-8 -*-
from mongoengine import Document, StringField, ValidationError, EmbeddedDocument, EmbeddedDocumentField, \
    InvalidQueryError, LookUpError, IntField, GenericEmbeddedDocumentField, ListField, EmbeddedDocumentListField

from tests.utils import MongoDBTestCase


class TestEmbeddedDocumentField(MongoDBTestCase):
    def test___init___(self):
        class MyDoc(EmbeddedDocument):
            name = StringField()

        field = EmbeddedDocumentField(MyDoc)
        self.assertEqual(field.document_type_obj, MyDoc)

        field2 = EmbeddedDocumentField('MyDoc')
        self.assertEqual(field2.document_type_obj, 'MyDoc')

    def test___init___throw_error_if_document_type_is_not_EmbeddedDocument(self):
        with self.assertRaises(ValidationError):
            EmbeddedDocumentField(dict)

    def test_document_type_throw_error_if_not_EmbeddedDocument_subclass(self):

        class MyDoc(Document):
            name = StringField()

        emb = EmbeddedDocumentField('MyDoc')
        with self.assertRaises(ValidationError) as ctx:
            emb.document_type
        self.assertIn('Invalid embedded document class provided to an EmbeddedDocumentField', str(ctx.exception))

    def test_embedded_document_field_only_allow_subclasses_of_embedded_document(self):
        # Relates to #1661
        class MyDoc(Document):
            name = StringField()

        with self.assertRaises(ValidationError):
            class MyFailingDoc(Document):
                emb = EmbeddedDocumentField(MyDoc)

        with self.assertRaises(ValidationError):
            class MyFailingdoc2(Document):
                emb = EmbeddedDocumentField('MyDoc')

    def test_query_embedded_document_attribute(self):
        class AdminSettings(EmbeddedDocument):
            foo1 = StringField()
            foo2 = StringField()

        class Person(Document):
            settings = EmbeddedDocumentField(AdminSettings)
            name = StringField()

        Person.drop_collection()

        p = Person(
            settings=AdminSettings(foo1='bar1', foo2='bar2'),
            name='John',
        ).save()

        # Test non exiting attribute
        with self.assertRaises(InvalidQueryError) as ctx_err:
            Person.objects(settings__notexist='bar').first()
        self.assertEqual(unicode(ctx_err.exception), u'Cannot resolve field "notexist"')

        with self.assertRaises(LookUpError):
            Person.objects.only('settings.notexist')

        # Test existing attribute
        self.assertEqual(Person.objects(settings__foo1='bar1').first().id, p.id)
        only_p = Person.objects.only('settings.foo1').first()
        self.assertEqual(only_p.settings.foo1, p.settings.foo1)
        self.assertIsNone(only_p.settings.foo2)
        self.assertIsNone(only_p.name)

        exclude_p = Person.objects.exclude('settings.foo1').first()
        self.assertIsNone(exclude_p.settings.foo1)
        self.assertEqual(exclude_p.settings.foo2, p.settings.foo2)
        self.assertEqual(exclude_p.name, p.name)

    def test_query_embedded_document_attribute_with_inheritance(self):
        class BaseSettings(EmbeddedDocument):
            meta = {'allow_inheritance': True}
            base_foo = StringField()

        class AdminSettings(BaseSettings):
            sub_foo = StringField()

        class Person(Document):
            settings = EmbeddedDocumentField(BaseSettings)

        Person.drop_collection()

        p = Person(settings=AdminSettings(base_foo='basefoo', sub_foo='subfoo'))
        p.save()

        # Test non exiting attribute
        with self.assertRaises(InvalidQueryError) as ctx_err:
            self.assertEqual(Person.objects(settings__notexist='bar').first().id, p.id)
        self.assertEqual(unicode(ctx_err.exception), u'Cannot resolve field "notexist"')

        # Test existing attribute
        self.assertEqual(Person.objects(settings__base_foo='basefoo').first().id, p.id)
        self.assertEqual(Person.objects(settings__sub_foo='subfoo').first().id, p.id)

        only_p = Person.objects.only('settings.base_foo', 'settings._cls').first()
        self.assertEqual(only_p.settings.base_foo, 'basefoo')
        self.assertIsNone(only_p.settings.sub_foo)

    def test_query_list_embedded_document_with_inheritance(self):
        class BaseEmbeddedDoc(EmbeddedDocument):
            s = StringField()
            meta = {'allow_inheritance': True}

        class EmbeddedDoc(BaseEmbeddedDoc):
            s2 = StringField()

        class MyDoc(Document):
            embeds = EmbeddedDocumentListField(BaseEmbeddedDoc)

        doc = MyDoc(embeds=[EmbeddedDoc(s='foo', s2='bar')]).save()

        self.assertEqual(MyDoc.objects(embeds__s='foo').first(), doc)
        self.assertEqual(MyDoc.objects(embeds__s2='bar').first(), doc)


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

        person = Person(name='Test User')
        person.like = Car(name='Fiat')
        person.save()

        person = Person.objects.first()
        self.assertIsInstance(person.like, Car)

        person.like = Dish(food="arroz", number=15)
        person.save()

        person = Person.objects.first()
        self.assertIsInstance(person.like, Dish)

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

        person = Person(name='Test User')
        person.like = Car(name='Fiat')
        self.assertRaises(ValidationError, person.validate)

        person.like = Dish(food="arroz", number=15)
        person.save()

        person = Person.objects.first()
        self.assertIsInstance(person.like, Dish)

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

        person = Person(name='Test User')
        person.likes = [Car(name='Fiat')]
        self.assertRaises(ValidationError, person.validate)

        person.likes = [Dish(food="arroz", number=15)]
        person.save()

        person = Person.objects.first()
        self.assertIsInstance(person.likes[0], Dish)

    def test_choices_validation_documents(self):
        """
        Ensure fields with document choices validate given a valid choice.
        """
        class UserComments(EmbeddedDocument):
            author = StringField()
            message = StringField()

        class BlogPost(Document):
            comments = ListField(
                GenericEmbeddedDocumentField(choices=(UserComments,))
            )

        # Ensure Validation Passes
        BlogPost(comments=[
            UserComments(author='user2', message='message2'),
        ]).save()

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
            comments = ListField(
                GenericEmbeddedDocumentField(choices=(UserComments,))
            )

        # Single Entry Failure
        post = BlogPost(comments=[
            ModeratorComments(author='mod1', message='message1'),
        ])
        self.assertRaises(ValidationError, post.save)

        # Mixed Entry Failure
        post = BlogPost(comments=[
            ModeratorComments(author='mod1', message='message1'),
            UserComments(author='user2', message='message2'),
        ])
        self.assertRaises(ValidationError, post.save)

    def test_choices_validation_documents_inheritance(self):
        """
        Ensure fields with document choices validate given subclass of choice.
        """
        class Comments(EmbeddedDocument):
            meta = {
                'abstract': True
            }
            author = StringField()
            message = StringField()

        class UserComments(Comments):
            pass

        class BlogPost(Document):
            comments = ListField(
                GenericEmbeddedDocumentField(choices=(Comments,))
            )

        # Save Valid EmbeddedDocument Type
        BlogPost(comments=[
            UserComments(author='user2', message='message2'),
        ]).save()

    def test_query_generic_embedded_document_attribute(self):
        class AdminSettings(EmbeddedDocument):
            foo1 = StringField()

        class NonAdminSettings(EmbeddedDocument):
            foo2 = StringField()

        class Person(Document):
            settings = GenericEmbeddedDocumentField(choices=(AdminSettings, NonAdminSettings))

        Person.drop_collection()

        p1 = Person(settings=AdminSettings(foo1='bar1')).save()
        p2 = Person(settings=NonAdminSettings(foo2='bar2')).save()

        # Test non exiting attribute
        with self.assertRaises(InvalidQueryError) as ctx_err:
            Person.objects(settings__notexist='bar').first()
        self.assertEqual(unicode(ctx_err.exception), u'Cannot resolve field "notexist"')

        with self.assertRaises(LookUpError):
            Person.objects.only('settings.notexist')

        # Test existing attribute
        self.assertEqual(Person.objects(settings__foo1='bar1').first().id, p1.id)
        self.assertEqual(Person.objects(settings__foo2='bar2').first().id, p2.id)

    def test_query_generic_embedded_document_attribute_with_inheritance(self):
        class BaseSettings(EmbeddedDocument):
            meta = {'allow_inheritance': True}
            base_foo = StringField()

        class AdminSettings(BaseSettings):
            sub_foo = StringField()

        class Person(Document):
            settings = GenericEmbeddedDocumentField(choices=[BaseSettings])

        Person.drop_collection()

        p = Person(settings=AdminSettings(base_foo='basefoo', sub_foo='subfoo'))
        p.save()

        # Test non exiting attribute
        with self.assertRaises(InvalidQueryError) as ctx_err:
            self.assertEqual(Person.objects(settings__notexist='bar').first().id, p.id)
        self.assertEqual(unicode(ctx_err.exception), u'Cannot resolve field "notexist"')

        # Test existing attribute
        self.assertEqual(Person.objects(settings__base_foo='basefoo').first().id, p.id)
        self.assertEqual(Person.objects(settings__sub_foo='subfoo').first().id, p.id)
