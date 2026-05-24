from mongoengine import (
    Document,
    EmbeddedDocument,
    EmbeddedDocumentField,
    EmbeddedDocumentListField,
    IntField,
    StringField,
    ReferenceField,
    ListField,
    DictField,
    MapField,
    GenericReferenceField,
)
from mongoengine.base import _DocumentRegistry
from mongoengine.base.queryset.pipeline_builder import PipelineBuilder
from mongoengine.base.queryset.pipeline_builder.schema import Schema
from mongoengine.registry import _CollectionRegistry

from tests.asynchronous.utils import MongoDBAsyncTestCase


class TestQuerysetPipelineBuilderStress(MongoDBAsyncTestCase):

    def tearDown(self):
        _DocumentRegistry.clear()

    def test_reference_field_attribute_match(self):
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
                    "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
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

    def test_reference_field_select_related_scalar_hydrate(self):
        class Book(Document):
            title = StringField()

        class AuthorBook(Document):
            book = ReferenceField(Book)

        qs = AuthorBook.aobjects.select_related("book")
        pipeline = PipelineBuilder(qs).build()

        expected = [
            {
                "$lookup": {
                    "from": Book._get_collection_name(),
                    "let": {
                        "refIds": {
                            "$cond": [
                                {"$isArray": "$book"},
                                "$book",
                                {"$cond": [{"$ifNull": ["$book", False]}, ["$book"], []]},
                            ]
                        }
                    },
                    "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                    "as": "book__docs",
                }
            },
            {
                "$addFields": {
                    "book": {
                        "$let": {
                            "vars": {"orig": "$book"},
                            "in": {
                                "$cond": [
                                    {"$ifNull": ["$$orig", False]},
                                    {
                                        "$let": {
                                            "vars": {
                                                "rid": {
                                                    "$cond": [
                                                        {"$eq": [{"$type": "$$orig"}, "object"]},
                                                        "$$orig.$id",
                                                        "$$orig",
                                                    ]
                                                }
                                            },
                                            "in": {
                                                "$let": {
                                                    "vars": {
                                                        "docs": {
                                                            "$cond": [
                                                                {"$isArray": "$book__docs"},
                                                                "$book__docs",
                                                                [],
                                                            ]
                                                        },
                                                        "idx": {
                                                            "$indexOfArray": [
                                                                {
                                                                    "$map": {
                                                                        "input": {
                                                                            "$cond": [
                                                                                {"$isArray": "$book__docs"},
                                                                                "$book__docs",
                                                                                [],
                                                                            ]
                                                                        },
                                                                        "as": "d",
                                                                        "in": "$$d._id",
                                                                    }
                                                                },
                                                                "$$rid",
                                                            ]
                                                        },
                                                    },
                                                    "in": {
                                                        "$cond": [
                                                            {"$gte": ["$$idx", 0]},
                                                            {"$arrayElemAt": ["$$docs", "$$idx"]},
                                                            {"_missing_reference": True, "_ref": "$$rid"},
                                                        ]
                                                    },
                                                }
                                            },
                                        }
                                    },
                                    None,
                                ]
                            },
                        }
                    }
                }
            },
            {"$project": {"book__docs": 0}},
        ]
        assert pipeline == expected

    def test_listfield_reference_select_related(self):
        class Book(Document):
            title = StringField()

        class Shelf(Document):
            books = ListField(ReferenceField(Book))

        qs = Shelf.aobjects.select_related("books")
        pipeline = PipelineBuilder(qs).build()

        expected = [
            {
                "$lookup": {
                    "from": Book._get_collection_name(),
                    "let": {
                        "refIds": {
                            "$cond": [
                                {"$isArray": "$books"},
                                {
                                    "$reduce": {
                                        "input": "$books",
                                        "initialValue": [],
                                        "in": {
                                            "$concatArrays": [
                                                "$$value",
                                                {
                                                    "$cond": [
                                                        {"$isArray": "$$this"},
                                                        "$$this",
                                                        {
                                                            "$cond": [
                                                                {"$ifNull": ["$$this", False]},
                                                                ["$$this"],
                                                                [],
                                                            ]
                                                        },
                                                    ]
                                                },
                                            ]
                                        },
                                    }
                                },
                                [],
                            ]
                        }
                    },
                    "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                    "as": "books__docs",
                }
            },
            # keep your $addFields block exactly as you wrote it (it already matches)
            {
                "$addFields": {
                    "books": {
                        "$cond": [
                            {"$isArray": "$books"},
                            {
                                "$map": {
                                    "input": "$books",
                                    "as": "item",
                                    "in": {
                                        "$let": {
                                            "vars": {"orig": "$$item"},
                                            "in": {
                                                "$cond": [
                                                    {"$ifNull": ["$$orig", False]},
                                                    {
                                                        "$let": {
                                                            "vars": {
                                                                "rid": {
                                                                    "$cond": [
                                                                        {"$eq": [{"$type": "$$orig"}, "object"]},
                                                                        "$$orig.$id",
                                                                        "$$orig",
                                                                    ]
                                                                }
                                                            },
                                                            "in": {
                                                                "$let": {
                                                                    "vars": {
                                                                        "docs": {
                                                                            "$cond": [
                                                                                {"$isArray": "$books__docs"},
                                                                                "$books__docs",
                                                                                [],
                                                                            ]
                                                                        },
                                                                        "idx": {
                                                                            "$indexOfArray": [
                                                                                {
                                                                                    "$map": {
                                                                                        "input": {
                                                                                            "$cond": [
                                                                                                {
                                                                                                    "$isArray": "$books__docs"},
                                                                                                "$books__docs",
                                                                                                [],
                                                                                            ]
                                                                                        },
                                                                                        "as": "d",
                                                                                        "in": "$$d._id",
                                                                                    }
                                                                                },
                                                                                "$$rid",
                                                                            ]
                                                                        },
                                                                    },
                                                                    "in": {
                                                                        "$cond": [
                                                                            {"$gte": ["$$idx", 0]},
                                                                            {"$arrayElemAt": ["$$docs", "$$idx"]},
                                                                            {"_missing_reference": True,
                                                                             "_ref": "$$rid"},
                                                                        ]
                                                                    },
                                                                }
                                                            },
                                                        }
                                                    },
                                                    None,
                                                ]
                                            },
                                        }
                                    },
                                }
                            },
                            "$books",
                        ]
                    }
                }
            },
            {"$project": {"books__docs": 0}},
        ]

        assert pipeline == expected

    def test_dictfield_reference_select_related(self):
        class Book(Document):
            title = StringField()

        class Box(Document):
            by_key = DictField(field=ReferenceField(Book))

        qs = Box.aobjects.select_related("by_key")
        pipeline = PipelineBuilder(qs).build()

        expected = [
            {
                "$lookup": {
                    "from": Book._get_collection_name(),
                    "let": {
                        "refIds": {
                            "$reduce": {
                                "input": {"$objectToArray": "$by_key"},
                                "initialValue": [],
                                "in": {
                                    "$concatArrays": [
                                        "$$value",
                                        {
                                            "$cond": [
                                                {"$isArray": "$$this.v"},
                                                "$$this.v",
                                                {"$cond": [{"$ifNull": ["$$this.v", False]}, ["$$this.v"], []]},
                                            ]
                                        },
                                    ]
                                },
                            }
                        }
                    },
                    "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                    "as": "by_key__docs",
                }
            },
            {
                "$addFields": {
                    "by_key": {
                        "$arrayToObject": {
                            "$map": {
                                "input": {"$objectToArray": "$by_key"},
                                "as": "kv",
                                "in": {
                                    "k": "$$kv.k",
                                    "v": {
                                        "$let": {
                                            "vars": {"orig": "$$kv.v"},
                                            "in": {
                                                "$cond": [
                                                    {"$ifNull": ["$$orig", False]},
                                                    {
                                                        "$let": {
                                                            "vars": {
                                                                "rid": {
                                                                    "$cond": [
                                                                        {"$eq": [{"$type": "$$orig"}, "object"]},
                                                                        "$$orig.$id",
                                                                        "$$orig",
                                                                    ]
                                                                }
                                                            },
                                                            "in": {
                                                                "$let": {
                                                                    "vars": {
                                                                        "docs": {
                                                                            "$cond": [
                                                                                {"$isArray": "$by_key__docs"},
                                                                                "$by_key__docs",
                                                                                [],
                                                                            ]
                                                                        },
                                                                        "idx": {
                                                                            "$indexOfArray": [
                                                                                {
                                                                                    "$map": {
                                                                                        "input": {
                                                                                            "$cond": [
                                                                                                {
                                                                                                    "$isArray": "$by_key__docs"},
                                                                                                "$by_key__docs",
                                                                                                [],
                                                                                            ]
                                                                                        },
                                                                                        "as": "d",
                                                                                        "in": "$$d._id",
                                                                                    }
                                                                                },
                                                                                "$$rid",
                                                                            ]
                                                                        },
                                                                    },
                                                                    "in": {
                                                                        "$cond": [
                                                                            {"$gte": ["$$idx", 0]},
                                                                            {"$arrayElemAt": ["$$docs", "$$idx"]},
                                                                            {"_missing_reference": True,
                                                                             "_ref": "$$rid"},
                                                                        ]
                                                                    },
                                                                }
                                                            },
                                                        }
                                                    },
                                                    None,
                                                ]
                                            },
                                        }
                                    },
                                },
                            }
                        }
                    }
                }
            },
            {"$project": {"by_key__docs": 0}},
        ]
        assert pipeline == expected

    def test_mapfield_reference_select_related(self):
        class Book(Document):
            title = StringField()

        class Store(Document):
            by_key = MapField(field=ReferenceField(Book))

        qs = Store.aobjects.select_related("by_key")
        pipeline = PipelineBuilder(qs).build()

        expected = [
            {
                "$lookup": {
                    "from": Book._get_collection_name(),
                    "let": {
                        "refIds": {
                            "$reduce": {
                                "input": {"$objectToArray": "$by_key"},
                                "initialValue": [],
                                "in": {
                                    "$concatArrays": [
                                        "$$value",
                                        {
                                            "$cond": [
                                                {"$isArray": "$$this.v"},
                                                "$$this.v",
                                                {
                                                    "$cond": [
                                                        {"$ifNull": ["$$this.v", False]},
                                                        ["$$this.v"],
                                                        [],
                                                    ]
                                                },
                                            ]
                                        },
                                    ]
                                },
                            }
                        }
                    },
                    "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                    "as": "by_key__docs",
                }
            },
            {
                "$addFields": {
                    "by_key": {
                        "$arrayToObject": {
                            "$map": {
                                "input": {"$objectToArray": "$by_key"},
                                "as": "kv",
                                "in": {
                                    "k": "$$kv.k",
                                    "v": {
                                        "$let": {
                                            "vars": {"orig": "$$kv.v"},
                                            "in": {
                                                "$cond": [
                                                    {"$ifNull": ["$$orig", False]},
                                                    {
                                                        "$let": {
                                                            "vars": {
                                                                "rid": {
                                                                    "$cond": [
                                                                        {"$eq": [{"$type": "$$orig"}, "object"]},
                                                                        "$$orig.$id",
                                                                        "$$orig",
                                                                    ]
                                                                }
                                                            },
                                                            "in": {
                                                                "$let": {
                                                                    "vars": {
                                                                        "docs": {
                                                                            "$cond": [
                                                                                {"$isArray": "$by_key__docs"},
                                                                                "$by_key__docs",
                                                                                [],
                                                                            ]
                                                                        },
                                                                        "idx": {
                                                                            "$indexOfArray": [
                                                                                {
                                                                                    "$map": {
                                                                                        "input": {
                                                                                            "$cond": [
                                                                                                {
                                                                                                    "$isArray": "$by_key__docs"},
                                                                                                "$by_key__docs",
                                                                                                [],
                                                                                            ]
                                                                                        },
                                                                                        "as": "d",
                                                                                        "in": "$$d._id",
                                                                                    }
                                                                                },
                                                                                "$$rid",
                                                                            ]
                                                                        },
                                                                    },
                                                                    "in": {
                                                                        "$cond": [
                                                                            {"$gte": ["$$idx", 0]},
                                                                            {"$arrayElemAt": ["$$docs", "$$idx"]},
                                                                            {"_missing_reference": True,
                                                                             "_ref": "$$rid"},
                                                                        ]
                                                                    },
                                                                }
                                                            },
                                                        }
                                                    },
                                                    None,
                                                ]
                                            },
                                        }
                                    },
                                },
                            }
                        }
                    }
                }
            },
            {"$project": {"by_key__docs": 0}},
        ]

        assert pipeline == expected

    def test_generic_reference_scalar_select_related_exact(self):
        class A(Document):
            name = StringField()

        class B(Document):
            title = StringField()

        class Host(Document):
            obj = GenericReferenceField(choices=(A, B))

        qs = Host.aobjects.select_related("obj")
        pipeline = PipelineBuilder(qs).build()

        # Exact expected (but relies on Schema.regex_match for the class-test expression)
        def alias_for(cls):
            return f"obj__{cls.__name__}"

        # Matches StageBuilder._generic_value_transform_expr(...) logic
        expr = "$$orig"
        for cls in reversed([A, B]):  # reversed choices
            alias_arr = f"${alias_for(cls)}"
            class_test = Schema.regex_match("$$orig._cls", cls)
            branch = {
                "$let": {
                    "vars": {
                        "matches": {
                            "$filter": {
                                "input": alias_arr,
                                "as": "doc",
                                "cond": {"$eq": ["$$doc._id", "$$orig._ref.$id"]},
                            }
                        }
                    },
                    "in": {
                        "$cond": [
                            {"$gt": [{"$size": "$$matches"}, 0]},
                            {"$mergeObjects": [{"$first": "$$matches"},
                                               {"_ref": "$$orig._ref", "_cls": "$$orig._cls"}]},
                            {"_missing_reference": True, "_ref": "$$orig._ref", "_cls": "$$orig._cls"},
                        ]
                    },
                }
            }
            expr = {"$cond": [class_test, branch, expr]}

        expected = [
            {
                "$lookup": {
                    "from": A._get_collection_name(),
                    "localField": "obj._ref.$id",
                    "foreignField": "_id",
                    "as": alias_for(A),
                }
            },
            {
                "$lookup": {
                    "from": B._get_collection_name(),
                    "localField": "obj._ref.$id",
                    "foreignField": "_id",
                    "as": alias_for(B),
                }
            },
            {
                "$addFields": {
                    "obj": {
                        "$let": {
                            "vars": {"orig": "$obj"},
                            "in": expr,
                        }
                    }
                }
            },
            {"$project": {alias_for(A): 0, alias_for(B): 0}},
        ]

        assert pipeline == expected

    def test_embedded_list_double_select_related_and_filter_via_join(self):
        class Parent(Document):
            age = IntField(required=True)

        class Target(Document):
            name = StringField()

        class Inner(EmbeddedDocument):
            parent = ReferenceField(Parent)
            target = ReferenceField(Target)

        class Outer(EmbeddedDocument):
            inners = EmbeddedDocumentListField(Inner)

        class Child(Document):
            outer = EmbeddedDocumentField(Outer)

        qs = (
            Child.aobjects(outer__inners__parent__age__gt=50)
            .select_related("outer__inners__target", "outer__inners__parent")
        )
        pipeline = PipelineBuilder(qs).build()

        # The exact expected pipeline for embedded list hydration depends on your builder’s
        # chosen alias naming for embedded lookups. If your StageBuilder uses:
        # docs_alias = f"{list_path.replace('.', '_')}_{embedded_key.replace('.', '_')}__docs"
        # then for outer.inners.parent it becomes: "outer_inners_parent__docs"
        #
        # Below expected matches the current naming pattern in your StageBuilder.
        parent_docs = "outer_inners_parent__docs"
        target_docs = "outer_inners_target__docs"

        expected = [
            # lookup parents
            {
                "$lookup": {
                    "from": Parent._get_collection_name(),
                    "let": {
                        "refIds": {
                            "$cond": [
                                {"$isArray": "$outer.inners"},
                                {
                                    "$reduce": {
                                        "input": {"$ifNull": ["$outer.inners.parent", []]},
                                        "initialValue": [],
                                        "in": {
                                            "$concatArrays": [
                                                "$$value",
                                                {
                                                    "$cond": [
                                                        {"$isArray": "$$this"},
                                                        "$$this",
                                                        {"$cond": [{"$ifNull": ["$$this", False]}, ["$$this"], []]},
                                                    ]
                                                },
                                            ]
                                        },
                                    }
                                },
                                [],
                            ]
                        }
                    },
                    "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                    "as": parent_docs,
                }
            },
            # filter via join (parent.age > 50)
            {
                "$match": {
                    "$expr": {
                        "$gt": [
                            {
                                "$size": {
                                    "$filter": {
                                        "input": f"${parent_docs}",
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
            # hydrate outer.inners.parent
            {
                "$addFields": {
                    "outer.inners": {
                        "$cond": [
                            {"$isArray": "$outer.inners"},
                            {
                                "$map": {
                                    "input": "$outer.inners",
                                    "as": "it",
                                    "in": {
                                        "$mergeObjects": [
                                            "$$it",
                                            {
                                                "parent": {
                                                    "$let": {
                                                        "vars": {"orig": "$$it.parent"},
                                                        "in": {
                                                            "$cond": [
                                                                {"$ifNull": ["$$orig", False]},
                                                                {
                                                                    "$let": {
                                                                        "vars": {
                                                                            "rid": {
                                                                                "$cond": [
                                                                                    {"$eq": [{"$type": "$$orig"},
                                                                                             "object"]},
                                                                                    "$$orig.$id",
                                                                                    "$$orig",
                                                                                ]
                                                                            }
                                                                        },
                                                                        "in": {
                                                                            "$let": {
                                                                                "vars": {
                                                                                    "docs": {
                                                                                        "$cond": [
                                                                                            {
                                                                                                "$isArray": f"${parent_docs}"},
                                                                                            f"${parent_docs}",
                                                                                            [],
                                                                                        ]
                                                                                    },
                                                                                    "idx": {
                                                                                        "$indexOfArray": [
                                                                                            {
                                                                                                "$map": {
                                                                                                    "input": {
                                                                                                        "$cond": [
                                                                                                            {
                                                                                                                "$isArray": f"${parent_docs}"},
                                                                                                            f"${parent_docs}",
                                                                                                            [],
                                                                                                        ]
                                                                                                    },
                                                                                                    "as": "d",
                                                                                                    "in": "$$d._id",
                                                                                                }
                                                                                            },
                                                                                            "$$rid",
                                                                                        ]
                                                                                    },
                                                                                },
                                                                                "in": {
                                                                                    "$cond": [
                                                                                        {"$gte": ["$$idx", 0]},
                                                                                        {"$arrayElemAt": ["$$docs",
                                                                                                          "$$idx"]},
                                                                                        {"_missing_reference": True,
                                                                                         "_ref": "$$rid"},
                                                                                    ]
                                                                                },
                                                                            }
                                                                        },
                                                                    }
                                                                },
                                                                None,
                                                            ]
                                                        },
                                                    }
                                                }
                                            },
                                        ]
                                    },
                                }
                            },
                            "$outer.inners",
                        ]
                    }
                }
            },
            {"$project": {parent_docs: 0}},
            # lookup targets
            {
                "$lookup": {
                    "from": Target._get_collection_name(),
                    "let": {
                        "refIds": {
                            "$cond": [
                                {"$isArray": "$outer.inners"},
                                {
                                    "$reduce": {
                                        "input": {"$ifNull": ["$outer.inners.target", []]},
                                        "initialValue": [],
                                        "in": {
                                            "$concatArrays": [
                                                "$$value",
                                                {
                                                    "$cond": [
                                                        {"$isArray": "$$this"},
                                                        "$$this",
                                                        {"$cond": [{"$ifNull": ["$$this", False]}, ["$$this"], []]},
                                                    ]
                                                },
                                            ]
                                        },
                                    }
                                },
                                [],
                            ]
                        }
                    },
                    "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                    "as": target_docs,
                }
            },
            # hydrate outer.inners.target
            {
                "$addFields": {
                    "outer.inners": {
                        "$cond": [
                            {"$isArray": "$outer.inners"},
                            {
                                "$map": {
                                    "input": "$outer.inners",
                                    "as": "it",
                                    "in": {
                                        "$mergeObjects": [
                                            "$$it",
                                            {
                                                "target": {
                                                    "$let": {
                                                        "vars": {"orig": "$$it.target"},
                                                        "in": {
                                                            "$cond": [
                                                                {"$ifNull": ["$$orig", False]},
                                                                {
                                                                    "$let": {
                                                                        "vars": {
                                                                            "rid": {
                                                                                "$cond": [
                                                                                    {"$eq": [{"$type": "$$orig"},
                                                                                             "object"]},
                                                                                    "$$orig.$id",
                                                                                    "$$orig",
                                                                                ]
                                                                            }
                                                                        },
                                                                        "in": {
                                                                            "$let": {
                                                                                "vars": {
                                                                                    "docs": {
                                                                                        "$cond": [
                                                                                            {
                                                                                                "$isArray": f"${target_docs}"},
                                                                                            f"${target_docs}",
                                                                                            [],
                                                                                        ]
                                                                                    },
                                                                                    "idx": {
                                                                                        "$indexOfArray": [
                                                                                            {
                                                                                                "$map": {
                                                                                                    "input": {
                                                                                                        "$cond": [
                                                                                                            {
                                                                                                                "$isArray": f"${target_docs}"},
                                                                                                            f"${target_docs}",
                                                                                                            [],
                                                                                                        ]
                                                                                                    },
                                                                                                    "as": "d",
                                                                                                    "in": "$$d._id",
                                                                                                }
                                                                                            },
                                                                                            "$$rid",
                                                                                        ]
                                                                                    },
                                                                                },
                                                                                "in": {
                                                                                    "$cond": [
                                                                                        {"$gte": ["$$idx", 0]},
                                                                                        {"$arrayElemAt": ["$$docs",
                                                                                                          "$$idx"]},
                                                                                        {"_missing_reference": True,
                                                                                         "_ref": "$$rid"},
                                                                                    ]
                                                                                },
                                                                            }
                                                                        },
                                                                    }
                                                                },
                                                                None,
                                                            ]
                                                        },
                                                    }
                                                }
                                            },
                                        ]
                                    },
                                }
                            },
                            "$outer.inners",
                        ]
                    }
                }
            },
            {"$project": {target_docs: 0}},
        ]

        assert pipeline == expected
