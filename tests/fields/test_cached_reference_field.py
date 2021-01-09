from decimal import Decimal

import pytest

from mongoengine import (
    CachedReferenceField,
    DecimalField,
    Document,
    EmbeddedDocument,
    EmbeddedDocumentField,
    InvalidDocumentError,
    ListField,
    ReferenceField,
    StringField,
    ValidationError,
)
from tests.utils import MongoDBTestCase


class TestCachedReferenceField(MongoDBTestCase):
    def test_constructor_fail_bad_document_type(self):
        with pytest.raises(
            ValidationError, match="must be a document class or a string"
        ):
            CachedReferenceField(document_type=0)

    def test_get_and_save(self):
        """
        Tests #1047: CachedReferenceField creates DBRefs on to_python,
        but can't save them on to_mongo.
        """

        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocorrence(Document):
            person = StringField()
            animal = CachedReferenceField(Animal)

        Animal.drop_collection()
        Ocorrence.drop_collection()

        Ocorrence(
            person="testte", animal=Animal(name="Leopard", tag="heavy").save()
        ).save()
        p = Ocorrence.objects.get()
        p.person = "new_testte"
        p.save()

    def test_general_things(self):
        class Animal(Document):
            name = StringField()
            tag = StringField()

        class Ocorrence(Document):
            person = StringField()
            animal = CachedReferenceField(Animal, fields=["tag"])

        Animal.drop_collection()
        Ocorrence.drop_collection()

        a = Animal(name="Leopard", tag="heavy")
        a.save()

        assert Animal._cached_reference_fields == [Ocorrence.animal]
        o = Ocorrence(person="teste", animal=a)
        o.save()

        p = Ocorrence(person="Wilson")
        p.save()

        assert Ocorrence.objects(animal=None).count() == 1

        assert a.to_mongo(fields=["tag"]) == {"tag": "heavy", "_id": a.pk}

        assert o.to_mongo()["animal"]["tag"] == "heavy"

        # counts
        Ocorrence(person="teste 2").save()
        Ocorrence(person="teste 3").save()

        count = Ocorrence.objects(animal__tag="heavy").count()
        assert count == 1

        ocorrence = Ocorrence.objects(animal__tag="heavy").first()
        assert ocorrence.person == "teste"
        assert isinstance(ocorrence.animal, Animal)

    def test_with_decimal(self):
        class PersonAuto(Document):
            name = StringField()
            salary = DecimalField()

        class SocialTest(Document):
            group = StringField()
            person = CachedReferenceField(PersonAuto, fields=("salary",))

        PersonAuto.drop_collection()
        SocialTest.drop_collection()

        p = PersonAuto(name="Alberto", salary=Decimal("7000.00"))
        p.save()

        s = SocialTest(group="dev", person=p)
        s.save()

        assert SocialTest.objects._collection.find_one({"person.salary": 7000.00}) == {
            "_id": s.pk,
            "group": s.group,
            "person": {"_id": p.pk, "salary": 7000.00},
        }

    def test_cached_reference_field_reference(self):
        class Group(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            group = ReferenceField(Group)

        class SocialData(Document):
            obs = StringField()
            tags = ListField(StringField())
            person = CachedReferenceField(Person, fields=("group",))

        Group.drop_collection()
        Person.drop_collection()
        SocialData.drop_collection()

        g1 = Group(name="dev")
        g1.save()

        g2 = Group(name="designers")
        g2.save()

        p1 = Person(name="Alberto", group=g1)
        p1.save()

        p2 = Person(name="Andre", group=g1)
        p2.save()

        p3 = Person(name="Afro design", group=g2)
        p3.save()

        s1 = SocialData(obs="testing 123", person=p1, tags=["tag1", "tag2"])
        s1.save()

        s2 = SocialData(obs="testing 321", person=p3, tags=["tag3", "tag4"])
        s2.save()

        assert SocialData.objects._collection.find_one({"tags": "tag2"}) == {
            "_id": s1.pk,
            "obs": "testing 123",
            "tags": ["tag1", "tag2"],
            "person": {"_id": p1.pk, "group": g1.pk},
        }

        assert SocialData.objects(person__group=g2).count() == 1
        assert SocialData.objects(person__group=g2).first() == s2

    def test_cached_reference_field_push_with_fields(self):
        class Product(Document):
            name = StringField()

        Product.drop_collection()

        class Basket(Document):
            products = ListField(CachedReferenceField(Product, fields=["name"]))

        Basket.drop_collection()
        product1 = Product(name="abc").save()
        product2 = Product(name="def").save()
        basket = Basket(products=[product1]).save()
        assert Basket.objects._collection.find_one() == {
            "_id": basket.pk,
            "products": [{"_id": product1.pk, "name": product1.name}],
        }
        # push to list
        basket.update(push__products=product2)
        basket.reload()
        assert Basket.objects._collection.find_one() == {
            "_id": basket.pk,
            "products": [
                {"_id": product1.pk, "name": product1.name},
                {"_id": product2.pk, "name": product2.name},
            ],
        }

    def test_cached_reference_field_update_all(self):
        class Person(Document):
            TYPES = (("pf", "PF"), ("pj", "PJ"))
            name = StringField()
            tp = StringField(choices=TYPES)
            father = CachedReferenceField("self", fields=("tp",))

        Person.drop_collection()

        a1 = Person(name="Wilson Father", tp="pj")
        a1.save()

        a2 = Person(name="Wilson Junior", tp="pf", father=a1)
        a2.save()

        a2 = Person.objects.with_id(a2.id)
        assert a2.father.tp == a1.tp

        assert dict(a2.to_mongo()) == {
            "_id": a2.pk,
            "name": "Wilson Junior",
            "tp": "pf",
            "father": {"_id": a1.pk, "tp": "pj"},
        }

        assert Person.objects(father=a1)._query == {"father._id": a1.pk}
        assert Person.objects(father=a1).count() == 1

        Person.objects.update(set__tp="pf")
        Person.father.sync_all()

        a2.reload()
        assert dict(a2.to_mongo()) == {
            "_id": a2.pk,
            "name": "Wilson Junior",
            "tp": "pf",
            "father": {"_id": a1.pk, "tp": "pf"},
        }

    def test_cached_reference_fields_on_embedded_documents(self):
        with pytest.raises(InvalidDocumentError):

            class Test(Document):
                name = StringField()

            type(
                "WrongEmbeddedDocument",
                (EmbeddedDocument,),
                {"test": CachedReferenceField(Test)},
            )

    def test_cached_reference_auto_sync(self):
        class Person(Document):
            TYPES = (("pf", "PF"), ("pj", "PJ"))
            name = StringField()
            tp = StringField(choices=TYPES)

            father = CachedReferenceField("self", fields=("tp",))

        Person.drop_collection()

        a1 = Person(name="Wilson Father", tp="pj")
        a1.save()

        a2 = Person(name="Wilson Junior", tp="pf", father=a1)
        a2.save()

        a1.tp = "pf"
        a1.save()

        a2.reload()
        assert dict(a2.to_mongo()) == {
            "_id": a2.pk,
            "name": "Wilson Junior",
            "tp": "pf",
            "father": {"_id": a1.pk, "tp": "pf"},
        }

    def test_cached_reference_auto_sync_disabled(self):
        class Persone(Document):
            TYPES = (("pf", "PF"), ("pj", "PJ"))
            name = StringField()
            tp = StringField(choices=TYPES)

            father = CachedReferenceField("self", fields=("tp",), auto_sync=False)

        Persone.drop_collection()

        a1 = Persone(name="Wilson Father", tp="pj")
        a1.save()

        a2 = Persone(name="Wilson Junior", tp="pf", father=a1)
        a2.save()

        a1.tp = "pf"
        a1.save()

        assert Persone.objects._collection.find_one({"_id": a2.pk}) == {
            "_id": a2.pk,
            "name": "Wilson Junior",
            "tp": "pf",
            "father": {"_id": a1.pk, "tp": "pj"},
        }

    def test_cached_reference_embedded_fields(self):
        class Owner(EmbeddedDocument):
            TPS = (("n", "Normal"), ("u", "Urgent"))
            name = StringField()
            tp = StringField(verbose_name="Type", db_field="t", choices=TPS)

        class Animal(Document):
            name = StringField()
            tag = StringField()

            owner = EmbeddedDocumentField(Owner)

        class Ocorrence(Document):
            person = StringField()
            animal = CachedReferenceField(Animal, fields=["tag", "owner.tp"])

        Animal.drop_collection()
        Ocorrence.drop_collection()

        a = Animal(
            name="Leopard", tag="heavy", owner=Owner(tp="u", name="Wilson Júnior")
        )
        a.save()

        o = Ocorrence(person="teste", animal=a)
        o.save()
        assert dict(a.to_mongo(fields=["tag", "owner.tp"])) == {
            "_id": a.pk,
            "tag": "heavy",
            "owner": {"t": "u"},
        }
        assert o.to_mongo()["animal"]["tag"] == "heavy"
        assert o.to_mongo()["animal"]["owner"]["t"] == "u"

        # Check to_mongo with fields
        assert "animal" not in o.to_mongo(fields=["person"])

        # counts
        Ocorrence(person="teste 2").save()
        Ocorrence(person="teste 3").save()

        count = Ocorrence.objects(animal__tag="heavy", animal__owner__tp="u").count()
        assert count == 1

        ocorrence = Ocorrence.objects(
            animal__tag="heavy", animal__owner__tp="u"
        ).first()
        assert ocorrence.person == "teste"
        assert isinstance(ocorrence.animal, Animal)

    def test_cached_reference_embedded_list_fields(self):
        class Owner(EmbeddedDocument):
            name = StringField()
            tags = ListField(StringField())

        class Animal(Document):
            name = StringField()
            tag = StringField()

            owner = EmbeddedDocumentField(Owner)

        class Ocorrence(Document):
            person = StringField()
            animal = CachedReferenceField(Animal, fields=["tag", "owner.tags"])

        Animal.drop_collection()
        Ocorrence.drop_collection()

        a = Animal(
            name="Leopard",
            tag="heavy",
            owner=Owner(tags=["cool", "funny"], name="Wilson Júnior"),
        )
        a.save()

        o = Ocorrence(person="teste 2", animal=a)
        o.save()
        assert dict(a.to_mongo(fields=["tag", "owner.tags"])) == {
            "_id": a.pk,
            "tag": "heavy",
            "owner": {"tags": ["cool", "funny"]},
        }

        assert o.to_mongo()["animal"]["tag"] == "heavy"
        assert o.to_mongo()["animal"]["owner"]["tags"] == ["cool", "funny"]

        # counts
        Ocorrence(person="teste 2").save()
        Ocorrence(person="teste 3").save()

        query = Ocorrence.objects(
            animal__tag="heavy", animal__owner__tags="cool"
        )._query
        assert query == {"animal.owner.tags": "cool", "animal.tag": "heavy"}

        ocorrence = Ocorrence.objects(
            animal__tag="heavy", animal__owner__tags="cool"
        ).first()
        assert ocorrence.person == "teste 2"
        assert isinstance(ocorrence.animal, Animal)
