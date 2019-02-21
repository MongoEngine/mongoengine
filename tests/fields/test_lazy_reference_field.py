# -*- coding: utf-8 -*-
from bson import DBRef, ObjectId

from mongoengine import *
from mongoengine.base import LazyReference

from tests.utils import MongoDBTestCase


class TestLazyReferenceField(MongoDBTestCase):
    def test_lazy_reference_config(self):
        # Make sure ReferenceField only accepts a document class or a string
        # with a document class name.
        self.assertRaises(ValidationError, LazyReferenceField, EmbeddedDocument)

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
        self.assertIsInstance(p.animal, LazyReference)
        fetched_animal = p.animal.fetch()
        self.assertEqual(fetched_animal, animal)
        # `fetch` keep cache on referenced document by default...
        animal.tag = "not so heavy"
        animal.save()
        double_fetch = p.animal.fetch()
        self.assertIs(fetched_animal, double_fetch)
        self.assertEqual(double_fetch.tag, "heavy")
        # ...unless specified otherwise
        fetch_force = p.animal.fetch(force=True)
        self.assertIsNot(fetch_force, fetched_animal)
        self.assertEqual(fetch_force.tag, "not so heavy")

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
        self.assertIsInstance(p.animal, LazyReference)
        with self.assertRaises(DoesNotExist):
            p.animal.fetch()

    def test_lazy_reference_set(self):
        class Animal(Document):
            meta = {'allow_inheritance': True}

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
        sub_animal = SubAnimal(nick='doggo', name='dog').save()
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
            self.assertIsInstance(p.animal, LazyReference)
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
                'foo',
                baddoc,
                DBRef(baddoc._get_collection_name(), animal.pk),
                LazyReference(BadDoc, animal.pk)
                ):
            with self.assertRaises(ValidationError):
                p = Ocurrence(person="test", animal=bad).save()

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

        post1 = BlogPost(title='post 1', author=m1)
        post1.save()

        post2 = BlogPost(title='post 2', author=m2)
        post2.save()

        post = BlogPost.objects(author=m1).first()
        self.assertEqual(post.id, post1.id)

        post = BlogPost.objects(author=m2).first()
        self.assertEqual(post.id, post2.id)

        # Same thing by passing a LazyReference instance
        post = BlogPost.objects(author=LazyReference(Member, m2.pk)).first()
        self.assertEqual(post.id, post2.id)

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

        post1 = BlogPost(title='post 1', author=m1)
        post1.save()

        post2 = BlogPost(title='post 2', author=m2)
        post2.save()

        post = BlogPost.objects(author=m1).first()
        self.assertEqual(post.id, post1.id)

        post = BlogPost.objects(author=m2).first()
        self.assertEqual(post.id, post2.id)

        # Same thing by passing a LazyReference instance
        post = BlogPost.objects(author=LazyReference(Member, m2.pk)).first()
        self.assertEqual(post.id, post2.id)

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
        self.assertIsInstance(p.animal, LazyReference)
        with self.assertRaises(KeyError):
            p.animal['name']
        with self.assertRaises(AttributeError):
            p.animal.name
        self.assertEqual(p.animal.pk, animal.pk)

        self.assertEqual(p.animal_passthrough.name, "Leopard")
        self.assertEqual(p.animal_passthrough['name'], "Leopard")

        # Should not be able to access referenced document's methods
        with self.assertRaises(AttributeError):
            p.animal.save
        with self.assertRaises(KeyError):
            p.animal['save']

    def test_lazy_reference_not_set(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = LazyReferenceField(Animal)

        Animal.drop_collection()
        Ocurrence.drop_collection()

        Ocurrence(person='foo').save()
        p = Ocurrence.objects.get()
        self.assertIs(p.animal, None)

    def test_lazy_reference_equality(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        Animal.drop_collection()

        animal = Animal(name="Leopard", tag="heavy").save()
        animalref = LazyReference(Animal, animal.pk)
        self.assertEqual(animal, animalref)
        self.assertEqual(animalref, animal)

        other_animalref = LazyReference(Animal, ObjectId("54495ad94c934721ede76f90"))
        self.assertNotEqual(animal, other_animalref)
        self.assertNotEqual(other_animalref, animal)

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

        animal1 = Animal('doggo').save()
        animal2 = Animal('cheeta').save()

        def check_fields_type(occ):
            self.assertIsInstance(occ.direct, LazyReference)
            for elem in occ.in_list:
                self.assertIsInstance(elem, LazyReference)
            self.assertIsInstance(occ.in_embedded.direct, LazyReference)
            for elem in occ.in_embedded.in_list:
                self.assertIsInstance(elem, LazyReference)

        occ = Ocurrence(
            in_list=[animal1, animal2],
            in_embedded={'in_list': [animal1, animal2], 'direct': animal1},
            direct=animal1
        ).save()
        check_fields_type(occ)
        occ.reload()
        check_fields_type(occ)
        occ.direct = animal1.id
        occ.in_list = [animal1.id, animal2.id]
        occ.in_embedded.direct = animal1.id
        occ.in_embedded.in_list = [animal1.id, animal2.id]
        check_fields_type(occ)


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
        self.assertIsInstance(p.animal, LazyReference)
        fetched_animal = p.animal.fetch()
        self.assertEqual(fetched_animal, animal)
        # `fetch` keep cache on referenced document by default...
        animal.tag = "not so heavy"
        animal.save()
        double_fetch = p.animal.fetch()
        self.assertIs(fetched_animal, double_fetch)
        self.assertEqual(double_fetch.tag, "heavy")
        # ...unless specified otherwise
        fetch_force = p.animal.fetch(force=True)
        self.assertIsNot(fetch_force, fetched_animal)
        self.assertEqual(fetch_force.tag, "not so heavy")

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
        occ_vegetal = Ocurrence(living_thing=vegetal, thing=vegetal).save()
        with self.assertRaises(ValidationError):
            Ocurrence(living_thing=mineral).save()

        occ = Ocurrence.objects.get(living_thing=animal)
        self.assertEqual(occ, occ_animal)
        self.assertIsInstance(occ.thing, LazyReference)
        self.assertIsInstance(occ.living_thing, LazyReference)

        occ.thing = vegetal
        occ.living_thing = vegetal
        occ.save()

        occ.thing = mineral
        occ.living_thing = mineral
        with self.assertRaises(ValidationError):
            occ.save()

    def test_generic_lazy_reference_set(self):
        class Animal(Document):
            meta = {'allow_inheritance': True}

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
        sub_animal = SubAnimal(nick='doggo', name='dog').save()
        for ref in (
                animal,
                LazyReference(Animal, animal.pk),
                {'_cls': 'Animal', '_ref': DBRef(animal._get_collection_name(), animal.pk)},

                sub_animal,
                LazyReference(SubAnimal, sub_animal.pk),
                {'_cls': 'SubAnimal', '_ref': DBRef(sub_animal._get_collection_name(), sub_animal.pk)},
                ):
            p = Ocurrence(person="test", animal=ref).save()
            p.reload()
            self.assertIsInstance(p.animal, (LazyReference, Document))
            p.animal.fetch()

    def test_generic_lazy_reference_bad_set(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = GenericLazyReferenceField(choices=['Animal'])

        Animal.drop_collection()
        Ocurrence.drop_collection()

        class BadDoc(Document):
            pass

        animal = Animal(name="Leopard", tag="heavy").save()
        baddoc = BadDoc().save()
        for bad in (
                42,
                'foo',
                baddoc,
                LazyReference(BadDoc, animal.pk)
                ):
            with self.assertRaises(ValidationError):
                p = Ocurrence(person="test", animal=bad).save()

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

        post1 = BlogPost(title='post 1', author=m1)
        post1.save()

        post2 = BlogPost(title='post 2', author=m2)
        post2.save()

        post = BlogPost.objects(author=m1).first()
        self.assertEqual(post.id, post1.id)

        post = BlogPost.objects(author=m2).first()
        self.assertEqual(post.id, post2.id)

        # Same thing by passing a LazyReference instance
        post = BlogPost.objects(author=LazyReference(Member, m2.pk)).first()
        self.assertEqual(post.id, post2.id)

    def test_generic_lazy_reference_not_set(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocurrence(Document):
            person = StringField()
            animal = GenericLazyReferenceField()

        Animal.drop_collection()
        Ocurrence.drop_collection()

        Ocurrence(person='foo').save()
        p = Ocurrence.objects.get()
        self.assertIs(p.animal, None)

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

        animal1 = Animal('doggo').save()
        animal2 = Animal('cheeta').save()

        def check_fields_type(occ):
            self.assertIsInstance(occ.direct, LazyReference)
            for elem in occ.in_list:
                self.assertIsInstance(elem, LazyReference)
            self.assertIsInstance(occ.in_embedded.direct, LazyReference)
            for elem in occ.in_embedded.in_list:
                self.assertIsInstance(elem, LazyReference)

        occ = Ocurrence(
            in_list=[animal1, animal2],
            in_embedded={'in_list': [animal1, animal2], 'direct': animal1},
            direct=animal1
        ).save()
        check_fields_type(occ)
        occ.reload()
        check_fields_type(occ)
        animal1_ref = {'_cls': 'Animal', '_ref': DBRef(animal1._get_collection_name(), animal1.pk)}
        animal2_ref = {'_cls': 'Animal', '_ref': DBRef(animal2._get_collection_name(), animal2.pk)}
        occ.direct = animal1_ref
        occ.in_list = [animal1_ref, animal2_ref]
        occ.in_embedded.direct = animal1_ref
        occ.in_embedded.in_list = [animal1_ref, animal2_ref]
        check_fields_type(occ)
