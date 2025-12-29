from mongoengine import (
    Document,
    EmbeddedDocument,
    EmbeddedDocumentField,
    EmbeddedDocumentListField,
    GenericReferenceField,
    IntField,
    ListField,
    ReferenceField,
    StringField,
    DictField,
    MapField,
)
from mongoengine.base.queryset.pipeline_builder import PipelineBuilder
from tests.asynchronous.utils import MongoDBAsyncTestCase


class TestQuerysetLookupMatch(MongoDBAsyncTestCase):
    # ============================================================
    # 1) ReferenceField (scalar) -> attribute
    # ============================================================
    async def test_queryset_lookup_on_reference_field_attribute(self):
        class Parent(Document):
            age = IntField(required=True)

        class Child(Document):
            parent = ReferenceField(Parent, required=True)
            name = StringField()

        await Parent.adrop_collection()
        await Child.adrop_collection()

        p_young = await Parent(age=30).asave()
        p_old = await Parent(age=65).asave()

        await Child(name="c1", parent=p_young).asave()
        await Child(name="c2", parent=p_old).asave()
        await Child(name="c3", parent=p_old).asave()

        qs = Child.aobjects(parent__age__gt=50)
        assert sorted([c.name async for c in qs]) == ["c2", "c3"]

    async def test_reference_field_missing_reference_does_not_match(self):
        class Parent(Document):
            age = IntField(required=True)

        class Child(Document):
            parent = ReferenceField(Parent, required=True)
            name = StringField()

        await Parent.adrop_collection()
        await Child.adrop_collection()

        p_old = await Parent(age=65).asave()
        await Child(name="c_missing", parent=p_old).asave()

        await Parent.aobjects(id=p_old.id).delete()

        qs = Child.aobjects(parent__age__gt=50)
        assert [c.name async for c in qs] == []

    # ============================================================
    # 2) ListField(ReferenceField) -> attribute
    # ============================================================
    async def test_queryset_lookup_on_list_reference_field_attribute(self):
        class Parent(Document):
            age = IntField(required=True)

        class Child(Document):
            parents = ListField(ReferenceField(Parent))
            name = StringField()

        await Parent.adrop_collection()
        await Child.adrop_collection()

        p_young = await Parent(age=25).asave()
        p_old = await Parent(age=75).asave()

        await Child(name="c1", parents=[p_young]).asave()
        await Child(name="c2", parents=[p_old]).asave()
        await Child(name="c3", parents=[p_young, p_old]).asave()

        qs = Child.aobjects(parents__age__gt=50)
        assert sorted([c.name async for c in qs]) == ["c2", "c3"]

    async def test_list_reference_field_missing_reference_does_not_match(self):
        class Parent(Document):
            age = IntField(required=True)

        class Child(Document):
            parents = ListField(ReferenceField(Parent))
            name = StringField()

        await Parent.adrop_collection()
        await Child.adrop_collection()

        p_old = await Parent(age=70).asave()
        await Child(name="c_missing_list", parents=[p_old]).asave()

        await Parent.aobjects(id=p_old.id).delete()

        qs = Child.aobjects(parents__age__gt=50)
        assert [c.name async for c in qs] == []

    async def test_list_reference_field_mixed_missing_and_matching_still_matches(self):
        class Parent(Document):
            age = IntField(required=True)

        class Child(Document):
            parents = ListField(ReferenceField(Parent))
            name = StringField()

        await Parent.adrop_collection()
        await Child.adrop_collection()

        p_missing = await Parent(age=99).asave()
        p_ok = await Parent(age=60).asave()

        await Child(name="c_mixed", parents=[p_missing, p_ok]).asave()
        await Parent.aobjects(id=p_missing.id).delete()

        qs = Child.aobjects(parents__age__gt=50)
        assert [c.name async for c in qs] == ["c_mixed"]

    # ============================================================
    # 3) GenericReferenceField (scalar) -> attribute
    # ============================================================
    async def test_queryset_lookup_on_generic_reference_field_attribute(self):
        class Person(Document):
            age = IntField(required=True)

        class Animal(Document):
            age = IntField(required=True)

        class Holder(Document):
            target = GenericReferenceField(choices=(Person, Animal), required=True)
            name = StringField()

        await Person.adrop_collection()
        await Animal.adrop_collection()
        await Holder.adrop_collection()

        p_young = await Person(age=20).asave()
        p_old = await Person(age=80).asave()
        a_old = await Animal(age=55).asave()

        await Holder(name="h1", target=p_young).asave()
        await Holder(name="h2", target=p_old).asave()
        await Holder(name="h3", target=a_old).asave()

        qs = Holder.aobjects(target__age__gt=50)
        assert sorted([h.name async for h in qs]) == ["h2", "h3"]

    async def test_generic_reference_field_missing_reference_does_not_match(self):
        class Person(Document):
            age = IntField(required=True)

        class Holder(Document):
            target = GenericReferenceField(choices=(Person,), required=True)
            name = StringField()

        await Person.adrop_collection()
        await Holder.adrop_collection()

        old_person = await Person(age=80).asave()
        await Holder(name="h_missing", target=old_person).asave()

        await Person.aobjects(id=old_person.id).delete()

        qs = Holder.aobjects(target__age__gt=50)
        assert [h.name async for h in qs] == []

    # ============================================================
    # 4) ListField(GenericReferenceField) -> attribute
    # ============================================================
    async def test_queryset_lookup_on_list_generic_reference_field(self):
        class Person(Document):
            age = IntField(required=True)

        class Animal(Document):
            age = IntField(required=True)

        class Group(Document):
            members = ListField(GenericReferenceField(choices=(Person, Animal)))
            name = StringField()

        await Person.adrop_collection()
        await Animal.adrop_collection()
        await Group.adrop_collection()

        p_young = await Person(age=10).asave()
        p_old = await Person(age=70).asave()
        a_old = await Animal(age=80).asave()

        await Group(name="g1", members=[p_young]).asave()
        await Group(name="g2", members=[p_old]).asave()
        await Group(name="g3", members=[a_old]).asave()
        await Group(name="g4", members=[p_young, a_old]).asave()

        qs = Group.aobjects(members__age__gt=50)
        assert sorted([g.name async for g in qs]) == ["g2", "g3", "g4"]

    async def test_list_generic_reference_missing_reference_does_not_match(self):
        class Person(Document):
            age = IntField(required=True)

        class Group(Document):
            members = ListField(GenericReferenceField(choices=(Person,)))
            name = StringField()

        await Person.adrop_collection()
        await Group.adrop_collection()

        p_old = await Person(age=80).asave()
        await Group(name="g_missing", members=[p_old]).asave()

        await Person.aobjects(id=p_old.id).delete()

        qs = Group.aobjects(members__age__gt=50)
        assert [g.name async for g in qs] == []

    async def test_list_generic_reference_mixed_missing_and_matching(self):
        class Person(Document):
            age = IntField(required=True)

        class Group(Document):
            members = ListField(GenericReferenceField(choices=(Person,)))
            name = StringField()

        await Person.adrop_collection()
        await Group.adrop_collection()

        p_missing = await Person(age=90).asave()
        p_ok = await Person(age=60).asave()

        await Group(name="g_ok", members=[p_missing, p_ok]).asave()
        await Person.aobjects(id=p_missing.id).delete()

        qs = Group.aobjects(members__age__gt=50)
        assert [g.name async for g in qs] == ["g_ok"]

    # ============================================================
    # 5) EmbeddedDocumentField -> Reference/Generic -> attribute
    # ============================================================
    async def test_queryset_lookup_on_embedded_reference_field_attribute(self):
        class Parent(Document):
            age = IntField(required=True)

        class Meta(EmbeddedDocument):
            parent = ReferenceField(Parent, required=True)

        class Child(Document):
            info = EmbeddedDocumentField(Meta, required=True)
            name = StringField()

        await Parent.adrop_collection()
        await Child.adrop_collection()

        p_young = await Parent(age=10).asave()
        p_old = await Parent(age=90).asave()

        await Child(name="c1", info=Meta(parent=p_young)).asave()
        await Child(name="c2", info=Meta(parent=p_old)).asave()

        qs = Child.aobjects(info__parent__age__gt=50)
        assert [c.name async for c in qs] == ["c2"]

    async def test_embedded_reference_field_missing_reference_does_not_match(self):
        class Parent(Document):
            age = IntField(required=True)

        class Meta(EmbeddedDocument):
            parent = ReferenceField(Parent, required=True)

        class Child(Document):
            info = EmbeddedDocumentField(Meta, required=True)
            name = StringField()

        await Parent.adrop_collection()
        await Child.adrop_collection()

        p_old = await Parent(age=70).asave()
        await Child(name="c_missing", info=Meta(parent=p_old)).asave()

        await Parent.aobjects(id=p_old.id).delete()

        qs = Child.aobjects(info__parent__age__gt=50)
        assert [c.name async for c in qs] == []

    async def test_queryset_lookup_on_embedded_generic_reference_field_attribute(self):
        class Person(Document):
            age = IntField(required=True)

        class Animal(Document):
            age = IntField(required=True)

        class Meta(EmbeddedDocument):
            target = GenericReferenceField(choices=(Person, Animal), required=True)

        class Holder(Document):
            info = EmbeddedDocumentField(Meta, required=True)
            name = StringField()

        await Person.adrop_collection()
        await Animal.adrop_collection()
        await Holder.adrop_collection()

        p_young = await Person(age=15).asave()
        p_old = await Person(age=70).asave()
        a_old = await Animal(age=55).asave()

        await Holder(name="h1", info=Meta(target=p_young)).asave()
        await Holder(name="h2", info=Meta(target=p_old)).asave()
        await Holder(name="h3", info=Meta(target=a_old)).asave()

        qs = Holder.aobjects(info__target__age__gt=50)
        assert sorted([h.name async for h in qs]) == ["h2", "h3"]

    async def test_embedded_generic_reference_field_missing_reference_does_not_match(self):
        class Person(Document):
            age = IntField(required=True)

        class Meta(EmbeddedDocument):
            target = GenericReferenceField(choices=(Person,), required=True)

        class Holder(Document):
            info = EmbeddedDocumentField(Meta, required=True)
            name = StringField()

        await Person.adrop_collection()
        await Holder.adrop_collection()

        p_old = await Person(age=80).asave()
        await Holder(name="h_missing", info=Meta(target=p_old)).asave()

        await Person.aobjects(id=p_old.id).delete()

        qs = Holder.aobjects(info__target__age__gt=50)
        assert [h.name async for h in qs] == []

    # ============================================================
    # 6) EmbeddedDocumentListField(Item) -> Reference/Generic -> attribute
    # ============================================================
    async def test_queryset_lookup_on_embedded_list_reference_field_attribute(self):
        class Parent(Document):
            age = IntField(required=True)

        class Item(EmbeddedDocument):
            parent = ReferenceField(Parent, required=True)

        class Child(Document):
            items = EmbeddedDocumentListField(Item)
            name = StringField()

        await Parent.adrop_collection()
        await Child.adrop_collection()

        p_young = await Parent(age=20).asave()
        p_old = await Parent(age=80).asave()

        await Child(name="c1", items=[Item(parent=p_young)]).asave()
        await Child(name="c2", items=[Item(parent=p_old)]).asave()
        await Child(name="c3", items=[Item(parent=p_young), Item(parent=p_old)]).asave()

        qs = Child.aobjects(items__parent__age__gt=50)
        assert sorted([c.name async for c in qs]) == ["c2", "c3"]

    async def test_embedded_list_reference_field_missing_reference_does_not_match(self):
        class Parent(Document):
            age = IntField(required=True)

        class Item(EmbeddedDocument):
            parent = ReferenceField(Parent, required=True)

        class Child(Document):
            items = EmbeddedDocumentListField(Item)
            name = StringField()

        await Parent.adrop_collection()
        await Child.adrop_collection()

        p_old = await Parent(age=80).asave()
        await Child(name="c_missing", items=[Item(parent=p_old)]).asave()

        await Parent.aobjects(id=p_old.id).delete()

        qs = Child.aobjects(items__parent__age__gt=50)
        assert [c.name async for c in qs] == []

    async def test_queryset_lookup_on_embedded_list_generic_reference_field_attribute(self):
        class Person(Document):
            age = IntField(required=True)

        class Animal(Document):
            age = IntField(required=True)

        class Item(EmbeddedDocument):
            target = GenericReferenceField(choices=(Person, Animal), required=True)

        class Group(Document):
            items = EmbeddedDocumentListField(Item)
            name = StringField()

        await Person.adrop_collection()
        await Animal.adrop_collection()
        await Group.adrop_collection()

        p_young = await Person(age=12).asave()
        a_old = await Animal(age=77).asave()

        await Group(name="g1", items=[Item(target=p_young)]).asave()
        await Group(name="g2", items=[Item(target=a_old)]).asave()
        await Group(name="g3", items=[Item(target=p_young), Item(target=a_old)]).asave()

        qs = Group.aobjects(items__target__age__gt=50)
        assert sorted([g.name async for g in qs]) == ["g2", "g3"]

    async def test_embedded_list_generic_reference_field_missing_reference_does_not_match(self):
        class Animal(Document):
            age = IntField(required=True)

        class Item(EmbeddedDocument):
            target = GenericReferenceField(choices=(Animal,), required=True)

        class Group(Document):
            items = EmbeddedDocumentListField(Item)
            name = StringField()

        await Animal.adrop_collection()
        await Group.adrop_collection()

        a_old = await Animal(age=70).asave()
        await Group(name="g_missing", items=[Item(target=a_old)]).asave()

        await Animal.aobjects(id=a_old.id).delete()

        qs = Group.aobjects(items__target__age__gt=50)
        assert [g.name async for g in qs] == []

    # ============================================================
    # 7) Deep nesting (Embedded -> Embedded -> Ref/Generic)
    # ============================================================
    async def test_deeply_nested_embedded_reference_and_generic_reference(self):
        class Parent(Document):
            age = IntField(required=True)

        class Person(Document):
            age = IntField(required=True)

        class Animal(Document):
            age = IntField(required=True)

        class Inner(EmbeddedDocument):
            parent = ReferenceField(Parent, required=True)
            target = GenericReferenceField(choices=(Person, Animal), required=True)

        class Outer(EmbeddedDocument):
            inner = EmbeddedDocumentField(Inner, required=True)

        class Child(Document):
            outer = EmbeddedDocumentField(Outer, required=True)
            name = StringField()

        await Parent.adrop_collection()
        await Person.adrop_collection()
        await Animal.adrop_collection()
        await Child.adrop_collection()

        p_young = await Parent(age=10).asave()
        p_old = await Parent(age=80).asave()

        per_young = await Person(age=20).asave()
        per_old = await Person(age=70).asave()
        ani_old = await Animal(age=55).asave()

        await Child(name="c1", outer=Outer(inner=Inner(parent=p_young, target=per_young))).asave()
        await Child(name="c2", outer=Outer(inner=Inner(parent=p_old, target=per_old))).asave()
        await Child(name="c3", outer=Outer(inner=Inner(parent=p_old, target=ani_old))).asave()

        qs1 = Child.aobjects(outer__inner__parent__age__gt=50)
        assert sorted([c.name async for c in qs1]) == ["c2", "c3"]

        qs2 = Child.aobjects(outer__inner__target__age__gt=50)
        assert sorted([c.name async for c in qs2]) == ["c2", "c3"]

    async def test_deeply_nested_missing_reference_does_not_match(self):
        class Parent(Document):
            age = IntField(required=True)

        class Person(Document):
            age = IntField(required=True)

        class Inner(EmbeddedDocument):
            parent = ReferenceField(Parent, required=True)
            target = GenericReferenceField(choices=(Person,), required=True)

        class Outer(EmbeddedDocument):
            inner = EmbeddedDocumentField(Inner, required=True)

        class Child(Document):
            outer = EmbeddedDocumentField(Outer, required=True)
            name = StringField()

        await Parent.adrop_collection()
        await Person.adrop_collection()
        await Child.adrop_collection()

        # missing ReferenceField should not match
        p_old = await Parent(age=90).asave()
        per_old = await Person(age=80).asave()
        await Child(name="c_missing_ref", outer=Outer(inner=Inner(parent=p_old, target=per_old))).asave()
        await Parent.aobjects(id=p_old.id).delete()

        qs = Child.aobjects(outer__inner__parent__age__gt=50)
        assert [c.name async for c in qs] == []

        # isolate next scenario
        await Child.aobjects.delete()

        # missing GenericReferenceField should not match
        p_ok = await Parent(age=90).asave()
        per_missing = await Person(age=80).asave()
        await Child(name="c_missing_generic", outer=Outer(inner=Inner(parent=p_ok, target=per_missing))).asave()
        await Person.aobjects(id=per_missing.id).delete()

        qs2 = Child.aobjects(outer__inner__target__age__gt=50)
        assert [c.name async for c in qs2] == []

    # ============================================================
    # 8) Deep nesting with EmbeddedDocumentListField + Ref/Generic
    # ============================================================
    async def test_deeply_nested_embedded_list_reference_and_generic_reference(self):
        class Parent(Document):
            age = IntField(required=True)

        class Person(Document):
            age = IntField(required=True)

        class Animal(Document):
            age = IntField(required=True)

        class Inner(EmbeddedDocument):
            parent = ReferenceField(Parent, required=True)
            target = GenericReferenceField(choices=(Person, Animal), required=True)

        class Outer(EmbeddedDocument):
            inners = EmbeddedDocumentListField(Inner)

        class Child(Document):
            outer = EmbeddedDocumentField(Outer, required=True)
            name = StringField()

        await Parent.adrop_collection()
        await Person.adrop_collection()
        await Animal.adrop_collection()
        await Child.adrop_collection()

        p_young = await Parent(age=5).asave()
        p_old = await Parent(age=90).asave()

        per_young = await Person(age=10).asave()
        ani_old = await Animal(age=77).asave()

        await Child(name="c1", outer=Outer(inners=[Inner(parent=p_young, target=per_young)])).asave()
        await Child(name="c2", outer=Outer(inners=[Inner(parent=p_old, target=ani_old)])).asave()
        await Child(name="c3", outer=Outer(
            inners=[Inner(parent=p_young, target=per_young), Inner(parent=p_old, target=ani_old)])).asave()

        qs1 = Child.aobjects(outer__inners__parent__age__gt=50).select_related("outer__inners__target",
                                                                               "outer__inners__parent")
        pipeline = PipelineBuilder(qs1).build()
        b = [c async for c in qs1]
        assert sorted([c.name async for c in qs1]) == ["c2", "c3"]

        qs2 = Child.aobjects(outer__inners__target__age__gt=50)
        a = [c async for c in qs2]
        assert sorted([c.name async for c in qs2]) == ["c2", "c3"]

    # ============================================================
    # 9) Deep nesting: embedded list -> (List(Ref) + List(Generic))
    # ============================================================
    async def test_deeply_nested_list_of_refs_and_generics_inside_embedded_list(self):
        class Parent(Document):
            age = IntField(required=True)

        class Person(Document):
            age = IntField(required=True)

        class Animal(Document):
            age = IntField(required=True)

        class Inner(EmbeddedDocument):
            parents = ListField(ReferenceField(Parent))
            members = ListField(GenericReferenceField(choices=(Person, Animal)))

        class Outer(EmbeddedDocument):
            inners = EmbeddedDocumentListField(Inner)

        class Child(Document):
            outer = EmbeddedDocumentField(Outer, required=True)
            name = StringField()

        await Parent.adrop_collection()
        await Person.adrop_collection()
        await Animal.adrop_collection()
        await Child.adrop_collection()

        p_young = await Parent(age=10).asave()
        p_old = await Parent(age=70).asave()

        per_young = await Person(age=10).asave()
        ani_old = await Animal(age=80).asave()

        await Child(name="c1", outer=Outer(inners=[Inner(parents=[p_young], members=[per_young])])).asave()
        await Child(name="c2", outer=Outer(inners=[Inner(parents=[p_old], members=[ani_old])])).asave()
        await Child(name="c3",
                    outer=Outer(inners=[Inner(parents=[p_young, p_old], members=[per_young, ani_old])])).asave()

        qs1 = Child.aobjects(outer__inners__parents__age__gt=50)
        assert sorted([c.name async for c in qs1]) == ["c2", "c3"]

        qs2 = Child.aobjects(outer__inners__members__age__gt=50)
        assert sorted([c.name async for c in qs2]) == ["c2", "c3"]

    # ============================================================
    # 10) Reference-of-Reference and multi-hop combos
    # ============================================================
    async def test_queryset_lookup_on_reference_of_reference_attribute(self):
        class GrandParent(Document):
            age = IntField(required=True)

        class Parent(Document):
            gp = ReferenceField(GrandParent, required=True)

        class Child(Document):
            parent = ReferenceField(Parent, required=True)
            name = StringField()

        await GrandParent.adrop_collection()
        await Parent.adrop_collection()
        await Child.adrop_collection()

        gp_young = await GrandParent(age=10).asave()
        gp_old = await GrandParent(age=80).asave()

        p1 = await Parent(gp=gp_young).asave()
        p2 = await Parent(gp=gp_old).asave()

        await Child(name="c1", parent=p1).asave()
        await Child(name="c2", parent=p2).asave()
        await Child(name="c3", parent=p2).asave()

        qs = Child.aobjects(parent__gp__age__gt=50)
        assert sorted([c.name async for c in qs]) == ["c2", "c3"]

    async def test_reference_of_reference_missing_reference_does_not_match(self):
        class GrandParent(Document):
            age = IntField(required=True)

        class Parent(Document):
            gp = ReferenceField(GrandParent, required=True)

        class Child(Document):
            parent = ReferenceField(Parent, required=True)
            name = StringField()

        await GrandParent.adrop_collection()
        await Parent.adrop_collection()
        await Child.adrop_collection()

        gp_old = await GrandParent(age=80).asave()
        p = await Parent(gp=gp_old).asave()
        await Child(name="c_missing_gp", parent=p).asave()

        await GrandParent.aobjects(id=gp_old.id).delete()

        qs = Child.aobjects(parent__gp__age__gt=50)
        assert [c.name async for c in qs] == []

    async def test_queryset_lookup_on_reference_to_list_reference_attribute(self):
        class GrandParent(Document):
            age = IntField(required=True)

        class Parent(Document):
            gps = ListField(ReferenceField(GrandParent))

        class Child(Document):
            parent = ReferenceField(Parent, required=True)
            name = StringField()

        await GrandParent.adrop_collection()
        await Parent.adrop_collection()
        await Child.adrop_collection()

        gp_young = await GrandParent(age=10).asave()
        gp_old = await GrandParent(age=90).asave()

        p1 = await Parent(gps=[gp_young]).asave()
        p2 = await Parent(gps=[gp_old]).asave()
        p3 = await Parent(gps=[gp_young, gp_old]).asave()

        await Child(name="c1", parent=p1).asave()
        await Child(name="c2", parent=p2).asave()
        await Child(name="c3", parent=p3).asave()

        qs = Child.aobjects(parent__gps__age__gt=50)
        assert sorted([c.name async for c in qs]) == ["c2", "c3"]

    async def test_queryset_lookup_on_reference_to_generic_reference_attribute(self):
        class Person(Document):
            age = IntField(required=True)

        class Animal(Document):
            age = IntField(required=True)

        class Parent(Document):
            target = GenericReferenceField(choices=(Person, Animal), required=True)

        class Child(Document):
            parent = ReferenceField(Parent, required=True)
            name = StringField()

        await Person.adrop_collection()
        await Animal.adrop_collection()
        await Parent.adrop_collection()
        await Child.adrop_collection()

        p_young = await Person(age=10).asave()
        p_old = await Person(age=80).asave()
        a_old = await Animal(age=70).asave()

        par1 = await Parent(target=p_young).asave()
        par2 = await Parent(target=p_old).asave()
        par3 = await Parent(target=a_old).asave()

        await Child(name="c1", parent=par1).asave()
        await Child(name="c2", parent=par2).asave()
        await Child(name="c3", parent=par3).asave()

        qs = Child.aobjects(parent__target__age__gt=50)
        assert sorted([c.name async for c in qs]) == ["c2", "c3"]

    async def test_reference_to_generic_reference_missing_reference_does_not_match(self):
        class Person(Document):
            age = IntField(required=True)

        class Parent(Document):
            target = GenericReferenceField(choices=(Person,), required=True)

        class Child(Document):
            parent = ReferenceField(Parent, required=True)
            name = StringField()

        await Person.adrop_collection()
        await Parent.adrop_collection()
        await Child.adrop_collection()

        per_old = await Person(age=80).asave()
        par = await Parent(target=per_old).asave()
        await Child(name="c_missing_generic", parent=par).asave()

        await Person.aobjects(id=per_old.id).delete()

        qs = Child.aobjects(parent__target__age__gt=50)
        assert [c.name async for c in qs] == []

    # ============================================================
    # 11) Generic -> Reference (multi-hop deref after generic)
    # ============================================================
    async def test_queryset_lookup_on_generic_then_reference_attribute(self):
        class GrandParent(Document):
            age = IntField(required=True)

        class Person(Document):
            gp = ReferenceField(GrandParent, required=True)

        class Animal(Document):
            gp = ReferenceField(GrandParent, required=True)

        class Holder(Document):
            target = GenericReferenceField(choices=(Person, Animal), required=True)
            name = StringField()

        await GrandParent.adrop_collection()
        await Person.adrop_collection()
        await Animal.adrop_collection()
        await Holder.adrop_collection()

        gp_young = await GrandParent(age=10).asave()
        gp_old = await GrandParent(age=80).asave()

        per = await Person(gp=gp_young).asave()
        ani = await Animal(gp=gp_old).asave()

        await Holder(name="h1", target=per).asave()
        await Holder(name="h2", target=ani).asave()

        qs = Holder.aobjects(target__gp__age__gt=50)
        assert [h.name async for h in qs] == ["h2"]

    # ============================================================
    # 12) Nested list shapes (ListField(ListField(ReferenceField)))
    # ============================================================
    async def test_queryset_lookup_on_nested_list_of_reference_field_attribute(self):
        class Parent(Document):
            age = IntField(required=True)

        class Child(Document):
            # nested lists of refs
            parents = ListField(ListField(ReferenceField(Parent)))
            name = StringField()

        await Parent.adrop_collection()
        await Child.adrop_collection()

        p_young = await Parent(age=10).asave()
        p_old = await Parent(age=80).asave()

        await Child(name="c1", parents=[[p_young]]).asave()
        await Child(name="c2", parents=[[p_old]]).asave()
        await Child(name="c3", parents=[[p_young, p_old]]).asave()

        qs = Child.aobjects(parents__age__gt=50)
        assert sorted([c.name async for c in qs]) == ["c2", "c3"]

    # ============================================================
    # 13) DictField / MapField reference & generic (stress coverage)
    # ============================================================
    async def test_queryset_lookup_on_map_reference_field_attribute(self):
        class Parent(Document):
            age = IntField(required=True)

        class Child(Document):
            by_key = MapField(ReferenceField(Parent))
            name = StringField()

        await Parent.adrop_collection()
        await Child.adrop_collection()

        p_young = await Parent(age=10).asave()
        p_old = await Parent(age=70).asave()

        await Child(name="c1", by_key={"a": p_young}).asave()
        await Child(name="c2", by_key={"a": p_old}).asave()
        await Child(name="c3", by_key={"a": p_young, "b": p_old}).asave()

        qs = Child.aobjects(by_key__age__gt=50)
        assert sorted([c.name async for c in qs]) == ["c2", "c3"]

    async def test_queryset_lookup_on_dict_generic_reference_field_attribute(self):
        class Person(Document):
            age = IntField(required=True)

        class Animal(Document):
            age = IntField(required=True)

        class Holder(Document):
            d = DictField(GenericReferenceField(choices=(Person, Animal)))
            name = StringField()

        await Person.adrop_collection()
        await Animal.adrop_collection()
        await Holder.adrop_collection()

        p_young = await Person(age=10).asave()
        a_old = await Animal(age=80).asave()

        await Holder(name="h1", d={"x": p_young}).asave()
        await Holder(name="h2", d={"x": a_old}).asave()
        await Holder(name="h3", d={"x": p_young, "y": a_old}).asave()

        qs = Holder.aobjects(d__age__gt=50)
        assert sorted([h.name async for h in qs]) == ["h2", "h3"]
