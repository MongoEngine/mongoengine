import unittest
import warnings

from pymongo.read_preferences import ReadPreference

from mongoengine import *
from tests.utils import MongoDBTestCase


class TestQuerysetAggregate(MongoDBTestCase):
    def test_read_preference_aggregation_framework(self):
        class Bar(Document):
            txt = StringField()

            meta = {"indexes": ["txt"]}

        # Aggregates with read_preference
        pipeline = []
        bars = Bar.objects.read_preference(
            ReadPreference.SECONDARY_PREFERRED
        ).aggregate(pipeline)
        assert (
            bars._CommandCursor__collection.read_preference
            == ReadPreference.SECONDARY_PREFERRED
        )

    def test_queryset_aggregation_framework(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = Person.objects(age__lte=22).aggregate(pipeline)

        assert list(data) == [
            {"_id": p1.pk, "name": "ISABELLA LUANNA"},
            {"_id": p2.pk, "name": "WILSON JUNIOR"},
        ]

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = Person.objects(age__lte=22).order_by("-name").aggregate(pipeline)

        assert list(data) == [
            {"_id": p2.pk, "name": "WILSON JUNIOR"},
            {"_id": p1.pk, "name": "ISABELLA LUANNA"},
        ]

        pipeline = [
            {"$group": {"_id": None, "total": {"$sum": 1}, "avg": {"$avg": "$age"}}}
        ]
        data = (
            Person.objects(age__gte=17, age__lte=40)
            .order_by("-age")
            .aggregate(pipeline)
        )
        assert list(data) == [{"_id": None, "avg": 29, "total": 2}]

        pipeline = [{"$match": {"name": "Isabella Luanna"}}]
        data = Person.objects().aggregate(pipeline)
        assert list(data) == [{"_id": p1.pk, "age": 16, "name": "Isabella Luanna"}]

    def test_queryset_aggregation_with_skip(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = Person.objects.skip(1).aggregate(pipeline)

        assert list(data) == [
            {"_id": p2.pk, "name": "WILSON JUNIOR"},
            {"_id": p3.pk, "name": "SANDRA MARA"},
        ]

    def test_queryset_aggregation_with_limit(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = Person.objects.limit(1).aggregate(pipeline)

        assert list(data) == [{"_id": p1.pk, "name": "ISABELLA LUANNA"}]

    def test_queryset_aggregation_with_sort(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = Person.objects.order_by("name").aggregate(pipeline)

        assert list(data) == [
            {"_id": p1.pk, "name": "ISABELLA LUANNA"},
            {"_id": p3.pk, "name": "SANDRA MARA"},
            {"_id": p2.pk, "name": "WILSON JUNIOR"},
        ]

    def test_queryset_aggregation_with_skip_with_limit(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = list(Person.objects.skip(1).limit(1).aggregate(pipeline))

        assert list(data) == [{"_id": p2.pk, "name": "WILSON JUNIOR"}]

        # Make sure limit/skip chaining order has no impact
        data2 = Person.objects.limit(1).skip(1).aggregate(pipeline)

        assert data == list(data2)

    def test_queryset_aggregation_with_sort_with_limit(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = Person.objects.order_by("name").limit(2).aggregate(pipeline)

        assert list(data) == [
            {"_id": p1.pk, "name": "ISABELLA LUANNA"},
            {"_id": p3.pk, "name": "SANDRA MARA"},
        ]

        # Verify adding limit/skip steps works as expected
        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}, {"$limit": 1}]
        data = Person.objects.order_by("name").limit(2).aggregate(pipeline)

        assert list(data) == [{"_id": p1.pk, "name": "ISABELLA LUANNA"}]

        pipeline = [
            {"$project": {"name": {"$toUpper": "$name"}}},
            {"$skip": 1},
            {"$limit": 1},
        ]
        data = Person.objects.order_by("name").limit(2).aggregate(pipeline)

        assert list(data) == [{"_id": p3.pk, "name": "SANDRA MARA"}]

    def test_queryset_aggregation_with_sort_with_skip(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = Person.objects.order_by("name").skip(2).aggregate(pipeline)

        assert list(data) == [{"_id": p2.pk, "name": "WILSON JUNIOR"}]

    def test_queryset_aggregation_with_sort_with_skip_with_limit(self):
        class Person(Document):
            name = StringField()
            age = IntField()

        Person.drop_collection()

        p1 = Person(name="Isabella Luanna", age=16)
        p2 = Person(name="Wilson Junior", age=21)
        p3 = Person(name="Sandra Mara", age=37)
        Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]
        data = Person.objects.order_by("name").skip(1).limit(1).aggregate(pipeline)

        assert list(data) == [{"_id": p3.pk, "name": "SANDRA MARA"}]

    def test_queryset_aggregation_deprecated_interface(self):
        class Person(Document):
            name = StringField()

        Person.drop_collection()

        p1 = Person(name="Isabella Luanna")
        p2 = Person(name="Wilson Junior")
        p3 = Person(name="Sandra Mara")
        Person.objects.insert([p1, p2, p3])

        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}]

        # Make sure a warning is emitted
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            with self.assertRaises(DeprecationWarning):
                Person.objects.order_by("name").limit(2).aggregate(*pipeline)

        # Make sure old interface works as expected with a 1-step pipeline
        data = Person.objects.order_by("name").limit(2).aggregate(*pipeline)

        assert list(data) == [
            {"_id": p1.pk, "name": "ISABELLA LUANNA"},
            {"_id": p3.pk, "name": "SANDRA MARA"},
        ]

        # Make sure old interface works as expected with a 2-steps pipeline
        pipeline = [{"$project": {"name": {"$toUpper": "$name"}}}, {"$limit": 1}]
        data = Person.objects.order_by("name").limit(2).aggregate(*pipeline)

        assert list(data) == [{"_id": p1.pk, "name": "ISABELLA LUANNA"}]

    def test_queryset_aggregation_geonear_aggregation_on_pointfield(self):
        """test ensures that $geonear can be used as a 1-stage pipeline and that
        MongoEngine does not interfer with such pipeline (#2473)
        """

        class Aggr(Document):
            name = StringField()
            c = PointField()

        Aggr.drop_collection()

        agg1 = Aggr(name="X", c=[10.634584, 35.8245029]).save()
        agg2 = Aggr(name="Y", c=[10.634584, 35.8245029]).save()

        pipeline = [
            {
                "$geoNear": {
                    "near": {"type": "Point", "coordinates": [10.634584, 35.8245029]},
                    "distanceField": "c",
                    "spherical": True,
                }
            }
        ]
        assert list(Aggr.objects.aggregate(*pipeline)) == [
            {"_id": agg1.id, "c": 0.0, "name": "X"},
            {"_id": agg2.id, "c": 0.0, "name": "Y"},
        ]


if __name__ == "__main__":
    unittest.main()
