from bson import DBRef, ObjectId
import pytest

from mongoengine import *
from mongoengine.base import LazyReference
from mongoengine.context_managers import query_counter

from tests.utils import MongoDBTestCase


class TestLazyReferenceField(MongoDBTestCase):
    def test_lazy_reference_config(self):
        # Make sure ReferenceField only accepts a document class or a string
        # with a document class name.
        with pytest.raises(ValidationError):
            LazyReferenceField(EmbeddedDocument)

    def test___repr__(self):
        class Animal(Document):
            pass

        class Ocurrence(Document):
            animal = LazyReferenceField(Animal)

        Animal.drop_collection()
        Ocurrence.drop_collection()

        animal = Animal()
        oc = Ocurrence(animal=animal)
        assert "LazyReference" in repr(oc.animal)

    def test___getattr___unknown_attr_raises_attribute_error(self):
        class Animal(Document):
            pass

        class Ocurrence(Document):
            animal = LazyReferenceField(Animal)

        Animal.drop_collection()
        Ocurrence.drop_collection()

        animal = Animal().save()
        oc = Ocurrence(animal=animal)
        with pytest.raises(AttributeError):
            oc.animal.not_exist

    def test_lazy_reference_simple(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = LazyReferenceField(Animal)

        Animal.drop_collection()
        Ocurrence.drop_collection()

        animal = Animal(name="Leopard", tag="heavy").save()
        Ocurrence(person="test", animal=animal).save()
        p = Ocurrence.objects.get()
        assert isinstance(p.animal, LazyReference)
        fetched_animal = p.animal.fetch()
        assert fetched_animal == animal
        # `fetch` keep cache on referenced document by default...
        animal.tag = "not so heavy"
        animal.save()
        double_fetch = p.animal.fetch()
        assert fetched_animal is double_fetch
        assert double_fetch.tag == "heavy"
        # ...unless specified otherwise
        fetch_force = p.animal.fetch(force=True)
        assert fetch_force is not fetched_animal
        assert fetch_force.tag == "not so heavy"

    def test_lazy_reference_fetch_invalid_ref(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = LazyReferenceField(Animal)

        Animal.drop_collection()
        Ocurrence.drop_collection()

        animal = Animal(name="Leopard", tag="heavy").save()
        Ocurrence(person="test", animal=animal).save()
        animal.delete()
        p = Ocurrence.objects.get()
        assert isinstance(p.animal, LazyReference)
        with pytest.raises(DoesNotExist):
            p.animal.fetch()

    def test_lazy_reference_set(self):
        class Animal(Document):
            meta = {"allow_inheritance": True}

            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = LazyReferenceField(Animal)

        Animal.drop_collection()
        Ocurrence.drop_collection()

        class SubAnimal(Animal):
            nick = StringField()

        animal = Animal(name="Leopard", tag="heavy").save()
        sub_animal = SubAnimal(nick="doggo", name="dog").save()
        for ref in (
            animal,
            animal.pk,
            DBRef(animal._get_collection_name(), animal.pk),
            LazyReference(Animal, animal.pk),
            sub_animal,
            sub_animal.pk,
            DBRef(sub_animal._get_collection_name(), sub_animal.pk),
            LazyReference(SubAnimal, sub_animal.pk),
        ):
            p = Ocurrence(person="test", animal=ref).save()
            p.reload()
            assert isinstance(p.animal, LazyReference)
            p.animal.fetch()

    def test_lazy_reference_bad_set(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = LazyReferenceField(Animal)

        Animal.drop_collection()
        Ocurrence.drop_collection()

        class BadDoc(Document):
            pass

        animal = Animal(name="Leopard", tag="heavy").save()
        baddoc = BadDoc().save()
        for bad in (
            42,
            "foo",
            baddoc,
            DBRef(baddoc._get_collection_name(), animal.pk),
            LazyReference(BadDoc, animal.pk),
        ):
            with pytest.raises(ValidationError):
                Ocurrence(person="test", animal=bad).save()

    def test_lazy_reference_query_conversion(self):
        """Ensure that LazyReferenceFields can be queried using objects and values
        of the type of the primary key of the referenced object.
        """

        class Member(Document):
            user_num = IntField(primary_key=True)

        class BlogPost(Document):
            title = StringField()
            author = LazyReferenceField(Member, dbref=False)

        Member.drop_collection()
        BlogPost.drop_collection()

        m1 = Member(user_num=1)
        m1.save()
        m2 = Member(user_num=2)
        m2.save()

        post1 = BlogPost(title="post 1", author=m1)
        post1.save()

        post2 = BlogPost(title="post 2", author=m2)
        post2.save()

        post = BlogPost.objects(author=m1).first()
        assert post.id == post1.id

        post = BlogPost.objects(author=m2).first()
        assert post.id == post2.id

        # Same thing by passing a LazyReference instance
        post = BlogPost.objects(author=LazyReference(Member, m2.pk)).first()
        assert post.id == post2.id

    def test_lazy_reference_query_conversion_dbref(self):
        """Ensure that LazyReferenceFields can be queried using objects and values
        of the type of the primary key of the referenced object.
        """

        class Member(Document):
            user_num = IntField(primary_key=True)

        class BlogPost(Document):
            title = StringField()
            author = LazyReferenceField(Member, dbref=True)

        Member.drop_collection()
        BlogPost.drop_collection()

        m1 = Member(user_num=1)
        m1.save()
        m2 = Member(user_num=2)
        m2.save()

        post1 = BlogPost(title="post 1", author=m1)
        post1.save()

        post2 = BlogPost(title="post 2", author=m2)
        post2.save()

        post = BlogPost.objects(author=m1).first()
        assert post.id == post1.id

        post = BlogPost.objects(author=m2).first()
        assert post.id == post2.id

        # Same thing by passing a LazyReference instance
        post = BlogPost.objects(author=LazyReference(Member, m2.pk)).first()
        assert post.id == post2.id

    def test_lazy_reference_passthrough(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            animal = LazyReferenceField(Animal, passthrough=False)
            animal_passthrough = LazyReferenceField(Animal, passthrough=True)

        Animal.drop_collection()
        Ocurrence.drop_collection()

        animal = Animal(name="Leopard", tag="heavy").save()
        Ocurrence(animal=animal, animal_passthrough=animal).save()
        p = Ocurrence.objects.get()
        assert isinstance(p.animal, LazyReference)
        with pytest.raises(KeyError):
            p.animal["name"]
        with pytest.raises(AttributeError):
            p.animal.name
        assert p.animal.pk == animal.pk

        assert p.animal_passthrough.name == "Leopard"
        assert p.animal_passthrough["name"] == "Leopard"

        # Should not be able to access referenced document's methods
        with pytest.raises(AttributeError):
            p.animal.save
        with pytest.raises(KeyError):
            p.animal["save"]

    def test_lazy_reference_not_set(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = LazyReferenceField(Animal)

        Animal.drop_collection()
        Ocurrence.drop_collection()

        Ocurrence(person="foo").save()
        p = Ocurrence.objects.get()
        assert p.animal is None

    def test_lazy_reference_equality(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        Animal.drop_collection()

        animal = Animal(name="Leopard", tag="heavy").save()
        animalref = LazyReference(Animal, animal.pk)
        assert animal == animalref
        assert animalref == animal

        other_animalref = LazyReference(Animal, ObjectId("54495ad94c934721ede76f90"))
        assert animal != other_animalref
        assert other_animalref != animal

    def test_lazy_reference_embedded(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class EmbeddedOcurrence(EmbeddedDocument):
            in_list = ListField(LazyReferenceField(Animal))
            direct = LazyReferenceField(Animal)

        class Ocurrence(Document):
            in_list = ListField(LazyReferenceField(Animal))
            in_embedded = EmbeddedDocumentField(EmbeddedOcurrence)
            direct = LazyReferenceField(Animal)

        Animal.drop_collection()
        Ocurrence.drop_collection()

        animal1 = Animal(name="doggo").save()
        animal2 = Animal(name="cheeta").save()

        def check_fields_type(occ):
            assert isinstance(occ.direct, LazyReference)
            for elem in occ.in_list:
                assert isinstance(elem, LazyReference)
            assert isinstance(occ.in_embedded.direct, LazyReference)
            for elem in occ.in_embedded.in_list:
                assert isinstance(elem, LazyReference)

        occ = Ocurrence(
            in_list=[animal1, animal2],
            in_embedded={"in_list": [animal1, animal2], "direct": animal1},
            direct=animal1,
        ).save()
        check_fields_type(occ)
        occ.reload()
        check_fields_type(occ)
        occ.direct = animal1.id
        occ.in_list = [animal1.id, animal2.id]
        occ.in_embedded.direct = animal1.id
        occ.in_embedded.in_list = [animal1.id, animal2.id]
        check_fields_type(occ)

    def test_lazy_reference_embedded_dereferencing(self):
        # Test case for #2375

        # -- Test documents

        class Author(Document):
            name = StringField()

        class AuthorReference(EmbeddedDocument):
            author = LazyReferenceField(Author)

        class Book(Document):
            authors = EmbeddedDocumentListField(AuthorReference)

        # -- Cleanup

        Author.drop_collection()
        Book.drop_collection()

        # -- Create test data

        author_1 = Author(name="A1").save()
        author_2 = Author(name="A2").save()
        author_3 = Author(name="A3").save()
        book = Book(
            authors=[
                AuthorReference(author=author_1),
                AuthorReference(author=author_2),
                AuthorReference(author=author_3),
            ]
        ).save()

        with query_counter() as qc:
            book = Book.objects.first()
            # Accessing the list must not trigger dereferencing.
            book.authors
            assert qc == 1

        for ref in book.authors:
            with pytest.raises(AttributeError):
                ref["author"].name
            assert isinstance(ref.author, LazyReference)
            assert isinstance(ref.author.id, ObjectId)


class TestGenericLazyReferenceField(MongoDBTestCase):
    def test_generic_lazy_reference_simple(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = GenericLazyReferenceField()

        Animal.drop_collection()
        Ocurrence.drop_collection()

        animal = Animal(name="Leopard", tag="heavy").save()
        Ocurrence(person="test", animal=animal).save()
        p = Ocurrence.objects.get()
        assert isinstance(p.animal, LazyReference)
        fetched_animal = p.animal.fetch()
        assert fetched_animal == animal
        # `fetch` keep cache on referenced document by default...
        animal.tag = "not so heavy"
        animal.save()
        double_fetch = p.animal.fetch()
        assert fetched_animal is double_fetch
        assert double_fetch.tag == "heavy"
        # ...unless specified otherwise
        fetch_force = p.animal.fetch(force=True)
        assert fetch_force is not fetched_animal
        assert fetch_force.tag == "not so heavy"

    def test_generic_lazy_reference_choices(self):
        class Animal(Document):
            name = StringField()

        class Vegetal(Document):
            name = StringField()

        class Mineral(Document):
            name = StringField()

        class Ocurrence(Document):
            living_thing = GenericLazyReferenceField(choices=[Animal, Vegetal])
            thing = GenericLazyReferenceField()

        Animal.drop_collection()
        Vegetal.drop_collection()
        Mineral.drop_collection()
        Ocurrence.drop_collection()

        animal = Animal(name="Leopard").save()
        vegetal = Vegetal(name="Oak").save()
        mineral = Mineral(name="Granite").save()

        occ_animal = Ocurrence(living_thing=animal, thing=animal).save()
        _ = Ocurrence(living_thing=vegetal, thing=vegetal).save()
        with pytest.raises(ValidationError):
            Ocurrence(living_thing=mineral).save()

        occ = Ocurrence.objects.get(living_thing=animal)
        assert occ == occ_animal
        assert isinstance(occ.thing, LazyReference)
        assert isinstance(occ.living_thing, LazyReference)

        occ.thing = vegetal
        occ.living_thing = vegetal
        occ.save()

        occ.thing = mineral
        occ.living_thing = mineral
        with pytest.raises(ValidationError):
            occ.save()

    def test_generic_lazy_reference_set(self):
        class Animal(Document):
            meta = {"allow_inheritance": True}

            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = GenericLazyReferenceField()

        Animal.drop_collection()
        Ocurrence.drop_collection()

        class SubAnimal(Animal):
            nick = StringField()

        animal = Animal(name="Leopard", tag="heavy").save()
        sub_animal = SubAnimal(nick="doggo", name="dog").save()
        for ref in (
            animal,
            LazyReference(Animal, animal.pk),
            {"_cls": "Animal", "_ref": DBRef(animal._get_collection_name(), animal.pk)},
            sub_animal,
            LazyReference(SubAnimal, sub_animal.pk),
            {
                "_cls": "SubAnimal",
                "_ref": DBRef(sub_animal._get_collection_name(), sub_animal.pk),
            },
        ):
            p = Ocurrence(person="test", animal=ref).save()
            p.reload()
            assert isinstance(p.animal, (LazyReference, Document))
            p.animal.fetch()

    def test_generic_lazy_reference_bad_set(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = GenericLazyReferenceField(choices=["Animal"])

        Animal.drop_collection()
        Ocurrence.drop_collection()

        class BadDoc(Document):
            pass

        animal = Animal(name="Leopard", tag="heavy").save()
        baddoc = BadDoc().save()
        for bad in (42, "foo", baddoc, LazyReference(BadDoc, animal.pk)):
            with pytest.raises(ValidationError):
                Ocurrence(person="test", animal=bad).save()

    def test_generic_lazy_reference_query_conversion(self):
        class Member(Document):
            user_num = IntField(primary_key=True)

        class BlogPost(Document):
            title = StringField()
            author = GenericLazyReferenceField()

        Member.drop_collection()
        BlogPost.drop_collection()

        m1 = Member(user_num=1)
        m1.save()
        m2 = Member(user_num=2)
        m2.save()

        post1 = BlogPost(title="post 1", author=m1)
        post1.save()

        post2 = BlogPost(title="post 2", author=m2)
        post2.save()

        post = BlogPost.objects(author=m1).first()
        assert post.id == post1.id

        post = BlogPost.objects(author=m2).first()
        assert post.id == post2.id

        # Same thing by passing a LazyReference instance
        post = BlogPost.objects(author=LazyReference(Member, m2.pk)).first()
        assert post.id == post2.id

    def test_generic_lazy_reference_not_set(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = GenericLazyReferenceField()

        Animal.drop_collection()
        Ocurrence.drop_collection()

        Ocurrence(person="foo").save()
        p = Ocurrence.objects.get()
        assert p.animal is None

    def test_generic_lazy_reference_accepts_string_instead_of_class(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = GenericLazyReferenceField("Animal")

        Animal.drop_collection()
        Ocurrence.drop_collection()

        animal = Animal().save()
        Ocurrence(animal=animal).save()
        p = Ocurrence.objects.get()
        assert p.animal == animal

    def test_generic_lazy_reference_embedded(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class EmbeddedOcurrence(EmbeddedDocument):
            in_list = ListField(GenericLazyReferenceField())
            direct = GenericLazyReferenceField()

        class Ocurrence(Document):
            in_list = ListField(GenericLazyReferenceField())
            in_embedded = EmbeddedDocumentField(EmbeddedOcurrence)
            direct = GenericLazyReferenceField()

        Animal.drop_collection()
        Ocurrence.drop_collection()

        animal1 = Animal(name="doggo").save()
        animal2 = Animal(name="cheeta").save()

        def check_fields_type(occ):
            assert isinstance(occ.direct, LazyReference)
            for elem in occ.in_list:
                assert isinstance(elem, LazyReference)
            assert isinstance(occ.in_embedded.direct, LazyReference)
            for elem in occ.in_embedded.in_list:
                assert isinstance(elem, LazyReference)

        occ = Ocurrence(
            in_list=[animal1, animal2],
            in_embedded={"in_list": [animal1, animal2], "direct": animal1},
            direct=animal1,
        ).save()
        check_fields_type(occ)
        occ.reload()
        check_fields_type(occ)
        animal1_ref = {
            "_cls": "Animal",
            "_ref": DBRef(animal1._get_collection_name(), animal1.pk),
        }
        animal2_ref = {
            "_cls": "Animal",
            "_ref": DBRef(animal2._get_collection_name(), animal2.pk),
        }
        occ.direct = animal1_ref
        occ.in_list = [animal1_ref, animal2_ref]
        occ.in_embedded.direct = animal1_ref
        occ.in_embedded.in_list = [animal1_ref, animal2_ref]
        check_fields_type(occ)
