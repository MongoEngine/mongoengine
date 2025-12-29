from mongoengine import Document, IntField, StringField, ReferenceField
from mongoengine.base.queryset.pipeline_builder import PipelineBuilder

from tests.asynchronous.utils import MongoDBAsyncTestCase


class TestQuerysetPipelineBuilder(MongoDBAsyncTestCase):
    async def test_pipeline_reference_field_attribute_match(self):
        class Parent(Document):
            age = IntField(required=True)

        class Child(Document):
            parent = ReferenceField(Parent, required=True)
            name = StringField()

        qs = Child.aobjects(parent__age__gt=50)
        pipeline = PipelineBuilder(qs).build()

        expected = [
            {
                "$lookup": {
                    "as": "parent__docs",
                    "from": Parent._get_collection_name(),
                    "let": {
                        "refIds": {
                            "$cond": [
                                {"$isArray": "$parent"},
                                "$parent",
                                {
                                    "$cond": [
                                        {"$ifNull": ["$parent", False]},
                                        ["$parent"],
                                        [],
                                    ]
                                },
                            ]
                        }
                    },
                    "pipeline": [
                        {"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}},
                    ],
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$gt": [
                            {
                                "$size": {
                                    "$filter": {
                                        "input": "$parent__docs",
                                        "as": "d",
                                        "cond": {"$gt": ["$$d.age", 50]},
                                    }
                                }
                            },
                            0,
                        ]
                    }
                }
            },
            {"$project": {"parent__docs": 0}},
        ]

        assert pipeline == expected
