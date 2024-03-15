import unittest

import pytest

from mongoengine import *
from mongoengine.context_managers import run_in_transaction
from tests.utils import MongoDBTestCase

__all__ = ("TestDynamicDocument",)


class TestDynamicDocument(MongoDBTestCase):
    def setUp(self):
        super().setUp()

        class Person(DynamicDocument):
            name = StringField()
            meta = {"allow_inheritance": True}

        Person.drop_collection()

        self.Person = Person

    def test_simple_dynamic_document(self):
        """Ensures simple dynamic documents are saved correctly"""

        p = self.Person()
        p.name = "James"
        p.age = 34

        assert p.to_mongo() == {"_cls": "Person", "name": "James", "age": 34}
        assert sorted(p.to_mongo().keys()) == ["_cls", "age", "name"]
        p.save()
        assert sorted(p.to_mongo().keys()) == ["_cls", "_id", "age", "name"]

        assert self.Person.objects.first().age == 34

        # Confirm no changes to self.Person
        assert not hasattr(self.Person, "age")

    def test_dynamic_document_parse_values_in_constructor_like_document_do(self):
        class ProductDynamicDocument(DynamicDocument):
            title = StringField()
            price = FloatField()

        class ProductDocument(Document):
            title = StringField()
            price = FloatField()

        product = ProductDocument(title="Blabla", price="12.5")
        dyn_product = ProductDynamicDocument(title="Blabla", price="12.5")
        assert product.price == dyn_product.price == 12.5

    def test_change_scope_of_variable(self):
        """Test changing the scope of a dynamic field has no adverse effects"""
        p = self.Person()
        p.name = "Dean"
        p.misc = 22
        p.save()

        p = self.Person.objects.get()
        p.misc = {"hello": "world"}
        p.save()

        p = self.Person.objects.get()
        assert p.misc == {"hello": "world"}

    def test_delete_dynamic_field(self):
        """Test deleting a dynamic field works"""
        self.Person.drop_collection()
        p = self.Person()
        p.name = "Dean"
        p.misc = 22
        p.save()

        p = self.Person.objects.get()
        p.misc = {"hello": "world"}
        p.save()

        p = self.Person.objects.get()
        assert p.misc == {"hello": "world"}
        collection = self.db[self.Person._get_collection_name()]
        obj = collection.find_one()
        assert sorted(obj.keys()) == ["_cls", "_id", "misc", "name"]

        del p.misc
        p.save()

        p = self.Person.objects.get()
        assert not hasattr(p, "misc")

        obj = collection.find_one()
        assert sorted(obj.keys()) == ["_cls", "_id", "name"]

    def test_reload_run_in_transaction(self):
        p = self.Person()
        p.misc = 22
        p.save()

        with run_in_transaction():
            p.reload()
            assert 22 == p.misc
            p.misc = 122
            p.save()
            p.reload()

        assert 122 == p.misc

        with pytest.raises(Exception, match="test"):
            with run_in_transaction():
                p.reload()
                assert 122 == p.misc
                p.misc = 22
                p.save()
                p.reload()
                assert 22 == p.misc
                raise Exception("test")

        p.reload()
        assert 122 == p.misc

    def test_reload_after_unsetting(self):
        p = self.Person()
        p.misc = 22
        p.save()
        p.update(unset__misc=1)
        p.reload()

    def test_reload_dynamic_field(self):
        self.Person.objects.delete()
        p = self.Person.objects.create()
        p.update(age=1)

        assert len(p._data) == 3
        assert sorted(p._data.keys()) == ["_cls", "id", "name"]

        p.reload()
        assert len(p._data) == 4
        assert sorted(p._data.keys()) == ["_cls", "age", "id", "name"]

    def test_fields_without_underscore(self):
        """Ensure we can query dynamic fields"""
        Person = self.Person

        p = self.Person(name="Dean")
        p.save()

        raw_p = Person.objects.as_pymongo().get(id=p.id)
        assert raw_p == {"_cls": "Person", "_id": p.id, "name": "Dean"}

        p.name = "OldDean"
        p.newattr = "garbage"
        p.save()
        raw_p = Person.objects.as_pymongo().get(id=p.id)
        assert raw_p == {
            "_cls": "Person",
            "_id": p.id,
            "name": "OldDean",
            "newattr": "garbage",
        }

    def test_fields_containing_underscore(self):
        """Ensure we can query dynamic fields"""

        class WeirdPerson(DynamicDocument):
            name = StringField()
            _name = StringField()

        WeirdPerson.drop_collection()

        p = WeirdPerson(name="Dean", _name="Dean")
        p.save()

        raw_p = WeirdPerson.objects.as_pymongo().get(id=p.id)
        assert raw_p == {"_id": p.id, "_name": "Dean", "name": "Dean"}

        p.name = "OldDean"
        p._name = "NewDean"
        p._newattr1 = "garbage"  # Unknown fields won't be added
        p.save()
        raw_p = WeirdPerson.objects.as_pymongo().get(id=p.id)
        assert raw_p == {"_id": p.id, "_name": "NewDean", "name": "OldDean"}

    def test_dynamic_document_queries(self):
        """Ensure we can query dynamic fields"""
        p = self.Person()
        p.name = "Dean"
        p.age = 22
        p.save()

        assert 1 == self.Person.objects(age=22).count()
        p = self.Person.objects(age=22)
        p = p.get()
        assert 22 == p.age

    def test_complex_dynamic_document_queries(self):
        class Person(DynamicDocument):
            name = StringField()

        Person.drop_collection()

        p = Person(name="test")
        p.age = "ten"
        p.save()

        p1 = Person(name="test1")
        p1.age = "less then ten and a half"
        p1.save()

        p2 = Person(name="test2")
        p2.age = 10
        p2.save()

        assert Person.objects(age__icontains="ten").count() == 2
        assert Person.objects(age__gte=10).count() == 1

    def test_complex_data_lookups(self):
        """Ensure you can query dynamic document dynamic fields"""
        p = self.Person()
        p.misc = {"hello": "world"}
        p.save()

        assert 1 == self.Person.objects(misc__hello="world").count()

    def test_three_level_complex_data_lookups(self):
        """Ensure you can query three level document dynamic fields"""
        self.Person.objects.create(misc={"hello": {"hello2": "world"}})
        assert 1 == self.Person.objects(misc__hello__hello2="world").count()

    def test_complex_embedded_document_validation(self):
        """Ensure embedded dynamic documents may be validated"""

        class Embedded(DynamicEmbeddedDocument):
            content = URLField()

        class Doc(DynamicDocument):
            pass

        Doc.drop_collection()
        doc = Doc()

        embedded_doc_1 = Embedded(content="http://mongoengine.org")
        embedded_doc_1.validate()

        embedded_doc_2 = Embedded(content="this is not a url")
        with pytest.raises(ValidationError):
            embedded_doc_2.validate()

        doc.embedded_field_1 = embedded_doc_1
        doc.embedded_field_2 = embedded_doc_2
        with pytest.raises(ValidationError):
            doc.validate()

    def test_inheritance(self):
        """Ensure that dynamic document plays nice with inheritance"""

        class Employee(self.Person):
            salary = IntField()

        Employee.drop_collection()

        assert "name" in Employee._fields
        assert "salary" in Employee._fields
        assert Employee._get_collection_name() == self.Person._get_collection_name()

        joe_bloggs = Employee()
        joe_bloggs.name = "Joe Bloggs"
        joe_bloggs.salary = 10
        joe_bloggs.age = 20
        joe_bloggs.save()

        assert 1 == self.Person.objects(age=20).count()
        assert 1 == Employee.objects(age=20).count()

        joe_bloggs = self.Person.objects.first()
        assert isinstance(joe_bloggs, Employee)

    def test_embedded_dynamic_document(self):
        """Test dynamic embedded documents"""

        class Embedded(DynamicEmbeddedDocument):
            pass

        class Doc(DynamicDocument):
            pass

        Doc.drop_collection()
        doc = Doc()

        embedded_1 = Embedded()
        embedded_1.string_field = "hello"
        embedded_1.int_field = 1
        embedded_1.dict_field = {"hello": "world"}
        embedded_1.list_field = ["1", 2, {"hello": "world"}]
        doc.embedded_field = embedded_1

        assert doc.to_mongo() == {
            "embedded_field": {
                "_cls": "Embedded",
                "string_field": "hello",
                "int_field": 1,
                "dict_field": {"hello": "world"},
                "list_field": ["1", 2, {"hello": "world"}],
            }
        }
        doc.save()

        doc = Doc.objects.first()
        assert doc.embedded_field.__class__ == Embedded
        assert doc.embedded_field.string_field == "hello"
        assert doc.embedded_field.int_field == 1
        assert doc.embedded_field.dict_field == {"hello": "world"}
        assert doc.embedded_field.list_field == ["1", 2, {"hello": "world"}]

    def test_complex_embedded_documents(self):
        """Test complex dynamic embedded documents setups"""

        class Embedded(DynamicEmbeddedDocument):
            pass

        class Doc(DynamicDocument):
            pass

        Doc.drop_collection()
        doc = Doc()

        embedded_1 = Embedded()
        embedded_1.string_field = "hello"
        embedded_1.int_field = 1
        embedded_1.dict_field = {"hello": "world"}

        embedded_2 = Embedded()
        embedded_2.string_field = "hello"
        embedded_2.int_field = 1
        embedded_2.dict_field = {"hello": "world"}
        embedded_2.list_field = ["1", 2, {"hello": "world"}]

        embedded_1.list_field = ["1", 2, embedded_2]
        doc.embedded_field = embedded_1

        assert doc.to_mongo() == {
            "embedded_field": {
                "_cls": "Embedded",
                "string_field": "hello",
                "int_field": 1,
                "dict_field": {"hello": "world"},
                "list_field": [
                    "1",
                    2,
                    {
                        "_cls": "Embedded",
                        "string_field": "hello",
                        "int_field": 1,
                        "dict_field": {"hello": "world"},
                        "list_field": ["1", 2, {"hello": "world"}],
                    },
                ],
            }
        }
        doc.save()
        doc = Doc.objects.first()
        assert doc.embedded_field.__class__ == Embedded
        assert doc.embedded_field.string_field == "hello"
        assert doc.embedded_field.int_field == 1
        assert doc.embedded_field.dict_field == {"hello": "world"}
        assert doc.embedded_field.list_field[0] == "1"
        assert doc.embedded_field.list_field[1] == 2

        embedded_field = doc.embedded_field.list_field[2]

        assert embedded_field.__class__ == Embedded
        assert embedded_field.string_field == "hello"
        assert embedded_field.int_field == 1
        assert embedded_field.dict_field == {"hello": "world"}
        assert embedded_field.list_field == ["1", 2, {"hello": "world"}]

    def test_dynamic_and_embedded(self):
        """Ensure embedded documents play nicely"""

        class Address(EmbeddedDocument):
            city = StringField()

        class Person(DynamicDocument):
            name = StringField()

        Person.drop_collection()

        Person(name="Ross", address=Address(city="London")).save()

        person = Person.objects.first()
        person.address.city = "Lundenne"
        person.save()

        assert Person.objects.first().address.city == "Lundenne"

        person = Person.objects.first()
        person.address = Address(city="Londinium")
        person.save()

        assert Person.objects.first().address.city == "Londinium"

        person = Person.objects.first()
        person.age = 35
        person.save()
        assert Person.objects.first().age == 35

    def test_dynamic_embedded_works_with_only(self):
        """Ensure custom fieldnames on a dynamic embedded document are found by qs.only()"""

        class Address(DynamicEmbeddedDocument):
            city = StringField()

        class Person(DynamicDocument):
            address = EmbeddedDocumentField(Address)

        Person.drop_collection()

        Person(
            name="Eric", address=Address(city="San Francisco", street_number="1337")
        ).save()

        assert Person.objects.first().address.street_number == "1337"
        assert (
            Person.objects.only("address__street_number").first().address.street_number
            == "1337"
        )

    def test_dynamic_and_embedded_dict_access(self):
        """Ensure embedded dynamic documents work with dict[] style access"""

        class Address(EmbeddedDocument):
            city = StringField()

        class Person(DynamicDocument):
            name = StringField()

        Person.drop_collection()

        Person(name="Ross", address=Address(city="London")).save()

        person = Person.objects.first()
        person.attrval = "This works"

        person["phone"] = "555-1212"  # but this should too

        # Same thing two levels deep
        person["address"]["city"] = "Lundenne"
        person.save()

        assert Person.objects.first().address.city == "Lundenne"

        assert Person.objects.first().phone == "555-1212"

        person = Person.objects.first()
        person.address = Address(city="Londinium")
        person.save()

        assert Person.objects.first().address.city == "Londinium"

        person = Person.objects.first()
        person["age"] = 35
        person.save()
        assert Person.objects.first().age == 35


if __name__ == "__main__":
    unittest.main()
