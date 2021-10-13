import unittest
import warnings

import pytest

from mongoengine import (
    BooleanField,
    Document,
    EmbeddedDocument,
    EmbeddedDocumentField,
    GenericReferenceField,
    IntField,
    ReferenceField,
    StringField,
)
from mongoengine.pymongo_support import list_collection_names
from tests.fixtures import Base
from tests.utils import MongoDBTestCase


class TestInheritance(MongoDBTestCase):
    def tearDown(self):
        for collection in list_collection_names(self.db):
            self.db.drop_collection(collection)

    def test_constructor_cls(self):
        # Ensures _cls is properly set during construction
        # and when object gets reloaded (prevent regression of #1950)
        class EmbedData(EmbeddedDocument):
            data = StringField()
            meta = {"allow_inheritance": True}

        class DataDoc(Document):
            name = StringField()
            embed = EmbeddedDocumentField(EmbedData)
            meta = {"allow_inheritance": True}

        test_doc = DataDoc(name="test", embed=EmbedData(data="data"))
        assert test_doc._cls == "DataDoc"
        assert test_doc.embed._cls == "EmbedData"
        test_doc.save()
        saved_doc = DataDoc.objects.with_id(test_doc.id)
        assert test_doc._cls == saved_doc._cls
        assert test_doc.embed._cls == saved_doc.embed._cls
        test_doc.delete()

    def test_superclasses(self):
        """Ensure that the correct list of superclasses is assembled."""

        class Animal(Document):
            meta = {"allow_inheritance": True}

        class Fish(Animal):
            pass

        class Guppy(Fish):
            pass

        class Mammal(Animal):
            pass

        class Dog(Mammal):
            pass

        class Human(Mammal):
            pass

        assert Animal._superclasses == ()
        assert Fish._superclasses == ("Animal",)
        assert Guppy._superclasses == ("Animal", "Animal.Fish")
        assert Mammal._superclasses == ("Animal",)
        assert Dog._superclasses == ("Animal", "Animal.Mammal")
        assert Human._superclasses == ("Animal", "Animal.Mammal")

    def test_external_superclasses(self):
        """Ensure that the correct list of super classes is assembled when
        importing part of the model.
        """

        class Animal(Base):
            pass

        class Fish(Animal):
            pass

        class Guppy(Fish):
            pass

        class Mammal(Animal):
            pass

        class Dog(Mammal):
            pass

        class Human(Mammal):
            pass

        assert Animal._superclasses == ("Base",)
        assert Fish._superclasses == ("Base", "Base.Animal")
        assert Guppy._superclasses == ("Base", "Base.Animal", "Base.Animal.Fish")
        assert Mammal._superclasses == ("Base", "Base.Animal")
        assert Dog._superclasses == ("Base", "Base.Animal", "Base.Animal.Mammal")
        assert Human._superclasses == ("Base", "Base.Animal", "Base.Animal.Mammal")

    def test_subclasses(self):
        """Ensure that the correct list of _subclasses (subclasses) is
        assembled.
        """

        class Animal(Document):
            meta = {"allow_inheritance": True}

        class Fish(Animal):
            pass

        class Guppy(Fish):
            pass

        class Mammal(Animal):
            pass

        class Dog(Mammal):
            pass

        class Human(Mammal):
            pass

        assert Animal._subclasses == (
            "Animal",
            "Animal.Fish",
            "Animal.Fish.Guppy",
            "Animal.Mammal",
            "Animal.Mammal.Dog",
            "Animal.Mammal.Human",
        )
        assert Fish._subclasses == ("Animal.Fish", "Animal.Fish.Guppy")
        assert Guppy._subclasses == ("Animal.Fish.Guppy",)
        assert Mammal._subclasses == (
            "Animal.Mammal",
            "Animal.Mammal.Dog",
            "Animal.Mammal.Human",
        )
        assert Human._subclasses == ("Animal.Mammal.Human",)

    def test_external_subclasses(self):
        """Ensure that the correct list of _subclasses (subclasses) is
        assembled when importing part of the model.
        """

        class Animal(Base):
            pass

        class Fish(Animal):
            pass

        class Guppy(Fish):
            pass

        class Mammal(Animal):
            pass

        class Dog(Mammal):
            pass

        class Human(Mammal):
            pass

        assert Animal._subclasses == (
            "Base.Animal",
            "Base.Animal.Fish",
            "Base.Animal.Fish.Guppy",
            "Base.Animal.Mammal",
            "Base.Animal.Mammal.Dog",
            "Base.Animal.Mammal.Human",
        )
        assert Fish._subclasses == ("Base.Animal.Fish", "Base.Animal.Fish.Guppy")
        assert Guppy._subclasses == ("Base.Animal.Fish.Guppy",)
        assert Mammal._subclasses == (
            "Base.Animal.Mammal",
            "Base.Animal.Mammal.Dog",
            "Base.Animal.Mammal.Human",
        )
        assert Human._subclasses == ("Base.Animal.Mammal.Human",)

    def test_dynamic_declarations(self):
        """Test that declaring an extra class updates meta data"""

        class Animal(Document):
            meta = {"allow_inheritance": True}

        assert Animal._superclasses == ()
        assert Animal._subclasses == ("Animal",)

        # Test dynamically adding a class changes the meta data
        class Fish(Animal):
            pass

        assert Animal._superclasses == ()
        assert Animal._subclasses == ("Animal", "Animal.Fish")

        assert Fish._superclasses == ("Animal",)
        assert Fish._subclasses == ("Animal.Fish",)

        # Test dynamically adding an inherited class changes the meta data
        class Pike(Fish):
            pass

        assert Animal._superclasses == ()
        assert Animal._subclasses == ("Animal", "Animal.Fish", "Animal.Fish.Pike")

        assert Fish._superclasses == ("Animal",)
        assert Fish._subclasses == ("Animal.Fish", "Animal.Fish.Pike")

        assert Pike._superclasses == ("Animal", "Animal.Fish")
        assert Pike._subclasses == ("Animal.Fish.Pike",)

    def test_inheritance_meta_data(self):
        """Ensure that document may inherit fields from a superclass document."""

        class Person(Document):
            name = StringField()
            age = IntField()

            meta = {"allow_inheritance": True}

        class Employee(Person):
            salary = IntField()

        assert ["_cls", "age", "id", "name", "salary"] == sorted(
            Employee._fields.keys()
        )
        assert Employee._get_collection_name() == Person._get_collection_name()

    def test_inheritance_to_mongo_keys(self):
        """Ensure that document may inherit fields from a superclass document."""

        class Person(Document):
            name = StringField()
            age = IntField()

            meta = {"allow_inheritance": True}

        class Employee(Person):
            salary = IntField()

        assert ["_cls", "age", "id", "name", "salary"] == sorted(
            Employee._fields.keys()
        )
        assert Person(name="Bob", age=35).to_mongo().keys() == ["_cls", "name", "age"]
        assert Employee(name="Bob", age=35, salary=0).to_mongo().keys() == [
            "_cls",
            "name",
            "age",
            "salary",
        ]
        assert Employee._get_collection_name() == Person._get_collection_name()

    def test_indexes_and_multiple_inheritance(self):
        """Ensure that all of the indexes are created for a document with
        multiple inheritance.
        """

        class A(Document):
            a = StringField()

            meta = {"allow_inheritance": True, "indexes": ["a"]}

        class B(Document):
            b = StringField()

            meta = {"allow_inheritance": True, "indexes": ["b"]}

        class C(A, B):
            pass

        A.drop_collection()
        B.drop_collection()
        C.drop_collection()

        C.ensure_indexes()

        assert sorted(
            idx["key"] for idx in C._get_collection().index_information().values()
        ) == sorted([[("_cls", 1), ("b", 1)], [("_id", 1)], [("_cls", 1), ("a", 1)]])

    def test_polymorphic_queries(self):
        """Ensure that the correct subclasses are returned from a query"""

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

        Animal.drop_collection()

        Animal().save()
        Fish().save()
        Mammal().save()
        Dog().save()
        Human().save()

        classes = [obj.__class__ for obj in Animal.objects]
        assert classes == [Animal, Fish, Mammal, Dog, Human]

        classes = [obj.__class__ for obj in Mammal.objects]
        assert classes == [Mammal, Dog, Human]

        classes = [obj.__class__ for obj in Human.objects]
        assert classes == [Human]

    def test_allow_inheritance(self):
        """Ensure that inheritance is disabled by default on simple
        classes and that _cls will not be used.
        """

        class Animal(Document):
            name = StringField()

        # can't inherit because Animal didn't explicitly allow inheritance
        with pytest.raises(ValueError, match="Document Animal may not be subclassed"):

            class Dog(Animal):
                pass

        # Check that _cls etc aren't present on simple documents
        dog = Animal(name="dog").save()
        assert dog.to_mongo().keys() == ["_id", "name"]

        collection = self.db[Animal._get_collection_name()]
        obj = collection.find_one()
        assert "_cls" not in obj

    def test_cant_turn_off_inheritance_on_subclass(self):
        """Ensure if inheritance is on in a subclass you cant turn it off."""

        class Animal(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        with pytest.raises(ValueError) as exc_info:

            class Mammal(Animal):
                meta = {"allow_inheritance": False}

        assert (
            str(exc_info.value)
            == 'Only direct subclasses of Document may set "allow_inheritance" to False'
        )

    def test_allow_inheritance_abstract_document(self):
        """Ensure that abstract documents can set inheritance rules and that
        _cls will not be used.
        """

        class FinalDocument(Document):
            meta = {"abstract": True, "allow_inheritance": False}

        class Animal(FinalDocument):
            name = StringField()

        with pytest.raises(ValueError):

            class Mammal(Animal):
                pass

        # Check that _cls isn't present in simple documents
        doc = Animal(name="dog")
        assert "_cls" not in doc.to_mongo()

    def test_using_abstract_class_in_reference_field(self):
        # Ensures no regression of #1920
        class AbstractHuman(Document):
            meta = {"abstract": True}

        class Dad(AbstractHuman):
            name = StringField()

        class Home(Document):
            dad = ReferenceField(AbstractHuman)  # Referencing the abstract class
            address = StringField()

        dad = Dad(name="5").save()
        Home(dad=dad, address="street").save()

        home = Home.objects.first()
        home.address = "garbage"
        home.save()  # Was failing with ValidationError

    def test_abstract_class_referencing_self(self):
        # Ensures no regression of #1920
        class Human(Document):
            meta = {"abstract": True}
            creator = ReferenceField("self", dbref=True)

        class User(Human):
            name = StringField()

        user = User(name="John").save()
        user2 = User(name="Foo", creator=user).save()

        user2 = User.objects.with_id(user2.id)
        user2.name = "Bar"
        user2.save()  # Was failing with ValidationError

    def test_abstract_handle_ids_in_metaclass_properly(self):
        class City(Document):
            continent = StringField()
            meta = {"abstract": True, "allow_inheritance": False}

        class EuropeanCity(City):
            name = StringField()

        berlin = EuropeanCity(name="Berlin", continent="Europe")
        assert len(berlin._db_field_map) == len(berlin._fields_ordered)
        assert len(berlin._reverse_db_field_map) == len(berlin._fields_ordered)
        assert len(berlin._fields_ordered) == 3
        assert berlin._fields_ordered[0] == "id"

    def test_auto_id_not_set_if_specific_in_parent_class(self):
        class City(Document):
            continent = StringField()
            city_id = IntField(primary_key=True)
            meta = {"abstract": True, "allow_inheritance": False}

        class EuropeanCity(City):
            name = StringField()

        berlin = EuropeanCity(name="Berlin", continent="Europe")
        assert len(berlin._db_field_map) == len(berlin._fields_ordered)
        assert len(berlin._reverse_db_field_map) == len(berlin._fields_ordered)
        assert len(berlin._fields_ordered) == 3
        assert berlin._fields_ordered[0] == "city_id"

    def test_auto_id_vs_non_pk_id_field(self):
        class City(Document):
            continent = StringField()
            id = IntField()
            meta = {"abstract": True, "allow_inheritance": False}

        class EuropeanCity(City):
            name = StringField()

        berlin = EuropeanCity(name="Berlin", continent="Europe")
        assert len(berlin._db_field_map) == len(berlin._fields_ordered)
        assert len(berlin._reverse_db_field_map) == len(berlin._fields_ordered)
        assert len(berlin._fields_ordered) == 4
        assert berlin._fields_ordered[0] == "auto_id_0"
        berlin.save()
        assert berlin.pk == berlin.auto_id_0

    def test_abstract_document_creation_does_not_fail(self):
        class City(Document):
            continent = StringField()
            meta = {"abstract": True, "allow_inheritance": False}

        city = City(continent="asia")
        assert city.pk is None
        # TODO: expected error? Shouldn't we create a new error type?
        with pytest.raises(KeyError):
            city.pk = 1

    def test_allow_inheritance_embedded_document(self):
        """Ensure embedded documents respect inheritance."""

        class Comment(EmbeddedDocument):
            content = StringField()

        with pytest.raises(ValueError):

            class SpecialComment(Comment):
                pass

        doc = Comment(content="test")
        assert "_cls" not in doc.to_mongo()

        class Comment(EmbeddedDocument):
            content = StringField()
            meta = {"allow_inheritance": True}

        doc = Comment(content="test")
        assert "_cls" in doc.to_mongo()

    def test_document_inheritance(self):
        """Ensure mutliple inheritance of abstract documents"""

        class DateCreatedDocument(Document):
            meta = {"allow_inheritance": True, "abstract": True}

        class DateUpdatedDocument(Document):
            meta = {"allow_inheritance": True, "abstract": True}

        class MyDocument(DateCreatedDocument, DateUpdatedDocument):
            pass

    def test_abstract_documents(self):
        """Ensure that a document superclass can be marked as abstract
        thereby not using it as the name for the collection."""

        defaults = {
            "index_background": True,
            "index_opts": {"hello": "world"},
            "allow_inheritance": True,
            "queryset_class": "QuerySet",
            "db_alias": "myDB",
            "shard_key": ("hello", "world"),
        }

        meta_settings = {"abstract": True}
        meta_settings.update(defaults)

        class Animal(Document):
            name = StringField()
            meta = meta_settings

        class Fish(Animal):
            pass

        class Guppy(Fish):
            pass

        class Mammal(Animal):
            meta = {"abstract": True}

        class Human(Mammal):
            pass

        for k, v in defaults.items():
            for cls in [Animal, Fish, Guppy]:
                assert cls._meta[k] == v

        assert "collection" not in Animal._meta
        assert "collection" not in Mammal._meta

        assert Animal._get_collection_name() is None
        assert Mammal._get_collection_name() is None

        assert Fish._get_collection_name() == "fish"
        assert Guppy._get_collection_name() == "fish"
        assert Human._get_collection_name() == "human"

        # ensure that a subclass of a non-abstract class can't be abstract
        with pytest.raises(ValueError):

            class EvilHuman(Human):
                evil = BooleanField(default=True)
                meta = {"abstract": True}

    def test_abstract_embedded_documents(self):
        # 789: EmbeddedDocument shouldn't inherit abstract
        class A(EmbeddedDocument):
            meta = {"abstract": True}

        class B(A):
            pass

        assert not B._meta["abstract"]

    def test_inherited_collections(self):
        """Ensure that subclassed documents don't override parents'
        collections
        """

        class Drink(Document):
            name = StringField()
            meta = {"allow_inheritance": True}

        class Drinker(Document):
            drink = GenericReferenceField()

        try:
            warnings.simplefilter("error")

            class AcloholicDrink(Drink):
                meta = {"collection": "booze"}

        except SyntaxWarning:
            warnings.simplefilter("ignore")

            class AlcoholicDrink(Drink):
                meta = {"collection": "booze"}

        else:
            raise AssertionError("SyntaxWarning should be triggered")

        warnings.resetwarnings()

        Drink.drop_collection()
        AlcoholicDrink.drop_collection()
        Drinker.drop_collection()

        red_bull = Drink(name="Red Bull")
        red_bull.save()

        programmer = Drinker(drink=red_bull)
        programmer.save()

        beer = AlcoholicDrink(name="Beer")
        beer.save()
        real_person = Drinker(drink=beer)
        real_person.save()

        assert Drinker.objects[0].drink.name == red_bull.name
        assert Drinker.objects[1].drink.name == beer.name


if __name__ == "__main__":
    unittest.main()
