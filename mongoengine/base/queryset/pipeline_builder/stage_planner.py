from __future__ import annotations

from typing import Optional

from .schema import Schema
from .match_planner import MatchPlanner


class StageBuilder:
    """
    Emits Mongo aggregation stages from a lookup tree.
    If interleave=True, applies bucket $match right after the corresponding deref stage.
    """

    def emit(self, doc_cls, prefix: str, tree: dict, buckets: Optional[dict], interleave: bool,
             embedded_list_path=None):
        stages = []
        self._pipeline = stages  # internal append target

        self._walk_lookups(
            doc_cls=doc_cls,
            prefix=prefix,
            tree=tree,
            buckets=buckets,
            embedded_list_path=embedded_list_path,
            interleave=interleave,
        )
        return stages

    # ----------------- core walk -----------------

    def _walk_lookups(self, doc_cls, prefix, tree, buckets, embedded_list_path=None, interleave=False):
        from mongoengine.fields import (
            ReferenceField, GenericReferenceField,
            ListField, DictField, MapField, EmbeddedDocumentField, FileField,
        )

        def apply_bucket(full_path):
            if not interleave or buckets is None:
                return
            bucket = buckets.pop(full_path, None)
            if bucket:
                self._pipeline.append({"$match": bucket})

        for field_name, subtree in tree.items():
            if field_name == "":
                continue

            field = doc_cls._fields.get(field_name)
            if not field:
                continue

            full_path = f"{prefix}{field.db_field}" if prefix else field.db_field

            # ReferenceField
            if isinstance(field, ReferenceField):
                target = field.document_type_obj
                if embedded_list_path:
                    self._add_embedded_list_structured_ref_lookup(target, field, embedded_list_path, field.db_field)
                else:
                    if target and target._meta.get("abstract", False):
                        self._add_abstract_dbref_lookup(target, field, full_path)
                    else:
                        self._add_structured_ref_lookup(target, field, full_path)

                apply_bucket(full_path)

                if subtree:
                    self._walk_lookups(target, f"{full_path}.", subtree, buckets, embedded_list_path, interleave)
                continue

            # ListField(...)
            if isinstance(field, ListField):
                leaf, _depth = Schema.unwrap_list_field(field)

                if leaf is not None and isinstance(leaf, ReferenceField):
                    target = leaf.document_type
                    if embedded_list_path:
                        self._add_embedded_list_structured_ref_lookup(target, field, embedded_list_path, field.db_field)
                    else:
                        self._add_structured_ref_lookup(target, field, full_path)

                    apply_bucket(full_path)

                    if subtree:
                        self._walk_lookups(target, f"{full_path}.", subtree, buckets, embedded_list_path, interleave)
                    continue

                if leaf is not None and isinstance(leaf, GenericReferenceField):
                    if leaf.choices:
                        if embedded_list_path:
                            self._add_embedded_list_generic_lookup(leaf, embedded_list_path, field.db_field)
                        else:
                            self._add_generic_lookup(leaf, full_path, is_list=True)

                    apply_bucket(full_path)
                    continue

            # MapField(ReferenceField)
            if isinstance(field, MapField) and isinstance(field.field, ReferenceField):
                if not embedded_list_path:
                    self._add_map_ref_lookup(field.field.document_type, field, full_path)
                apply_bucket(full_path)
                continue

            # MapField(GenericReferenceField)
            if isinstance(field, MapField) and isinstance(field.field, GenericReferenceField) and getattr(field.field,
                                                                                                          "choices",
                                                                                                          None):
                if not embedded_list_path:
                    self._add_object_generic_lookup(field.field, full_path)
                apply_bucket(full_path)
                continue

            # DictField(GenericReferenceField)
            if isinstance(field, DictField) and isinstance(field.field, GenericReferenceField) and getattr(field.field,
                                                                                                           "choices",
                                                                                                           None):
                if not embedded_list_path:
                    self._add_object_generic_lookup(field.field, full_path)
                apply_bucket(full_path)
                continue

            # Embedded doc list: descend
            if self._is_list_of_embedded(field):
                embedded_doc = self._embedded_doc_type(field)
                if subtree and embedded_doc:
                    self._walk_lookups(
                        embedded_doc,
                        f"{full_path}.",
                        subtree,
                        buckets,
                        embedded_list_path=full_path,
                        interleave=interleave,
                    )
                continue

            # DictField(Reference-only)
            if isinstance(field, DictField):
                refs = self._collect_ref_document_types(field.field)
                if len(refs) == 1:
                    target = list(refs)[0]
                    if not embedded_list_path:
                        self._add_structured_ref_lookup(target, field, full_path)

                    apply_bucket(full_path)

                    if subtree and not embedded_list_path:
                        self._walk_lookups(target, f"{full_path}.", subtree, buckets, embedded_list_path, interleave)
                continue

            # GenericReferenceField scalar
            if isinstance(field, GenericReferenceField) and field.choices:
                if embedded_list_path:
                    self._add_embedded_list_generic_lookup(field, embedded_list_path, field.db_field)
                else:
                    self._add_generic_lookup(field, full_path)

                apply_bucket(full_path)

                # safe traversal under generic (target.gp...)
                if subtree:
                    for sub_name, sub_tree in subtree.items():
                        if sub_name == "":
                            continue

                        common_ref_field, common_target = MatchPlanner.generic_common_ref(field, sub_name)
                        if common_ref_field is None or common_target is None:
                            continue

                        gp_path = f"{full_path}.{common_ref_field.db_field}"
                        self._add_structured_ref_lookup(common_target, common_ref_field, gp_path)
                        apply_bucket(gp_path)

                        if sub_tree:
                            self._walk_lookups(common_target, f"{gp_path}.", sub_tree, buckets, embedded_list_path,
                                               interleave)
                continue

            if isinstance(field, EmbeddedDocumentField):
                if subtree:
                    self._walk_lookups(field.document_type, f"{full_path}.", subtree, buckets, embedded_list_path,
                                       interleave)
                continue

            if isinstance(field, FileField):
                continue

    # ----------------- shared small helpers -----------------

    @staticmethod
    def _is_list_of_embedded(field):
        from mongoengine.fields import EmbeddedDocumentListField, ListField, EmbeddedDocumentField
        return (
                isinstance(field, EmbeddedDocumentListField)
                or (isinstance(field, ListField) and isinstance(getattr(field, "field", None), EmbeddedDocumentField))
        )

    @staticmethod
    def _embedded_doc_type(field):
        dt = getattr(field, "document_type", None)
        if dt:
            return dt
        inner = getattr(field, "field", None)
        return getattr(inner, "document_type", None) if inner else None

    @staticmethod
    def _collect_ref_document_types(field):
        from mongoengine.fields import ReferenceField, ListField, DictField, GenericReferenceField

        if isinstance(field, ReferenceField):
            return {field.document_type} if field.document_type is not None else set()
        if isinstance(field, ListField):
            return StageBuilder._collect_ref_document_types(field.field)
        if isinstance(field, DictField):
            return StageBuilder._collect_ref_document_types(field.field) if field.field is not None else set()
        if isinstance(field, GenericReferenceField):
            return set()
        return set()

    @staticmethod
    def _project_remove(*paths):
        return {"$project": {p: 0 for p in paths if p}}

    # ----------------- structured ref lookup -----------------

    @staticmethod
    def _build_ref_ids_expr(field, source_expr):
        from mongoengine.fields import ReferenceField, ListField, DictField, GenericReferenceField

        if isinstance(field, ReferenceField):
            if field.dbref:
                array_ids = {"$map": {"input": source_expr, "as": "r", "in": "$$r.$id"}}
                scalar_id = f"{source_expr}.$id"
            else:
                array_ids = source_expr
                scalar_id = source_expr

            return {
                "$cond": [
                    {"$isArray": source_expr},
                    array_ids,
                    {"$cond": [{"$ifNull": [source_expr, False]}, [scalar_id], []]},
                ]
            }

        if isinstance(field, GenericReferenceField):
            return []

        if isinstance(field, ListField):
            return {
                "$cond": [
                    {"$isArray": source_expr},
                    {
                        "$reduce": {
                            "input": source_expr,
                            "initialValue": [],
                            "in": {
                                "$concatArrays": ["$$value", StageBuilder._build_ref_ids_expr(field.field, "$$this")]},
                        }
                    },
                    [],
                ]
            }

        if isinstance(field, DictField):
            obj_array = {"$objectToArray": source_expr}
            return {
                "$reduce": {
                    "input": obj_array,
                    "initialValue": [],
                    "in": {"$concatArrays": ["$$value", StageBuilder._build_ref_ids_expr(field.field, "$$this.v")]},
                }
            }

        return []

    @staticmethod
    def _build_value_expr(field, source_expr, docs_expr):
        from mongoengine.fields import ReferenceField, ListField, DictField, GenericReferenceField

        if isinstance(field, ReferenceField):
            id_expr = f"{source_expr}.$id" if field.dbref else source_expr

            cls_name = None
            try:
                dt = field.document_type
                if dt is not None:
                    cls_name = getattr(dt, "_class_name", dt.__name__)
            except Exception:
                cls_name = None

            return {
                "$cond": [
                    {"$ifNull": [source_expr, False]},
                    {
                        "$let": {
                            "vars": {
                                "matches": {"$filter": {"input": docs_expr, "as": "doc",
                                                        "cond": {"$eq": ["$$doc._id", id_expr]}}},
                                "refId": id_expr,
                            },
                            "in": {
                                "$cond": [
                                    {"$gt": [{"$size": "$$matches"}, 0]},
                                    {"$first": "$$matches"},
                                    {"_missing_reference": True, "_ref": "$$refId", "_cls": cls_name},
                                ]
                            },
                        }
                    },
                    None,
                ]
            }

        if isinstance(field, GenericReferenceField):
            return source_expr

        if isinstance(field, ListField):
            return {
                "$cond": [
                    {"$isArray": source_expr},
                    {"$map": {"input": source_expr, "as": "item",
                              "in": StageBuilder._build_value_expr(field.field, "$$item", docs_expr)}},
                    source_expr,
                ]
            }

        if isinstance(field, DictField):
            return {
                "$arrayToObject": {
                    "$map": {
                        "input": {"$objectToArray": source_expr},
                        "as": "kv",
                        "in": {"k": "$$kv.k", "v": StageBuilder._build_value_expr(field.field, "$$kv.v", docs_expr)},
                    }
                }
            }

        return source_expr

    def _add_structured_ref_lookup(self, target_cls, field_shape, local_field):
        if not target_cls:
            return

        docs_alias = f"{local_field}__docs"
        ref_ids_expr = self._build_ref_ids_expr(field_shape, f"${local_field}")

        self._pipeline.append(
            {
                "$lookup": {
                    "from": target_cls._get_collection_name(),
                    "let": {"refIds": ref_ids_expr},
                    "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                    "as": docs_alias,
                }
            }
        )

        docs_expr = f"${docs_alias}"
        transformed_expr = self._build_value_expr(field_shape, f"${local_field}", docs_expr)

        self._pipeline.append({"$addFields": {local_field: transformed_expr}})
        self._pipeline.append({"$project": {docs_alias: 0}})

    # ----------------- embedded list structured ref lookup -----------------

    def _add_embedded_list_structured_ref_lookup(self, target_cls, field_shape, list_path, embedded_key):
        if not target_cls:
            return

        safe_list = list_path.replace(".", "_")
        docs_alias = f"{safe_list}_{embedded_key}__docs"
        raw_values_expr = {"$ifNull": [f"${list_path}.{embedded_key}", []]}

        ref_ids_expr = {
            "$cond": [
                {"$isArray": f"${list_path}"},
                {
                    "$reduce": {
                        "input": raw_values_expr,
                        "initialValue": [],
                        "in": {"$concatArrays": ["$$value", self._build_ref_ids_expr(field_shape, "$$this")]},
                    }
                },
                [],
            ]
        }

        self._pipeline.append(
            {
                "$lookup": {
                    "from": target_cls._get_collection_name(),
                    "let": {"refIds": ref_ids_expr},
                    "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                    "as": docs_alias,
                }
            }
        )

        docs_expr = f"${docs_alias}"
        per_item_value_expr = self._build_value_expr(field_shape, f"$$it.{embedded_key}", docs_expr)

        self._pipeline.append(
            {
                "$addFields": {
                    list_path: {
                        "$cond": [
                            {"$isArray": f"${list_path}"},
                            {"$map": {"input": f"${list_path}", "as": "it",
                                      "in": {"$mergeObjects": ["$$it", {embedded_key: per_item_value_expr}]}}},
                            f"${list_path}",
                        ]
                    }
                }
            }
        )
        self._pipeline.append({"$project": {docs_alias: 0}})

    # ----------------- map ref lookup -----------------

    def _add_map_ref_lookup(self, target_cls, map_field, local_field):
        if not target_cls:
            return

        safe = local_field.replace(".", "_")
        docs_alias = f"{safe}__docs"
        is_dbref = bool(getattr(map_field.field, "dbref", False))
        id_from_value_expr = "$$kv.v.$id" if is_dbref else "$$kv.v"

        ref_ids_expr = {
            "$cond": [
                {"$eq": [{"$type": f"${local_field}"}, "object"]},
                {"$map": {"input": {"$objectToArray": f"${local_field}"}, "as": "kv", "in": id_from_value_expr}},
                [],
            ]
        }

        self._pipeline.append(
            {
                "$lookup": {
                    "from": target_cls._get_collection_name(),
                    "let": {"refIds": ref_ids_expr},
                    "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                    "as": docs_alias,
                }
            }
        )

        cls_name = getattr(target_cls, "_class_name", target_cls.__name__)

        self._pipeline.append(
            {
                "$addFields": {
                    local_field: {
                        "$cond": [
                            {"$eq": [{"$type": f"${local_field}"}, "object"]},
                            {
                                "$arrayToObject": {
                                    "$map": {
                                        "input": {"$objectToArray": f"${local_field}"},
                                        "as": "kv",
                                        "in": {
                                            "k": "$$kv.k",
                                            "v": {
                                                "$let": {
                                                    "vars": {
                                                        "refId": {"$ifNull": [id_from_value_expr, None]},
                                                        "matches": {"$filter": {"input": f"${docs_alias}", "as": "doc",
                                                                                "cond": {"$eq": ["$$doc._id",
                                                                                                 id_from_value_expr]}}},
                                                    },
                                                    "in": {
                                                        "$cond": [
                                                            {"$ifNull": ["$$refId", False]},
                                                            {
                                                                "$cond": [
                                                                    {"$gt": [{"$size": "$$matches"}, 0]},
                                                                    {"$first": "$$matches"},
                                                                    {"_missing_reference": True, "_ref": "$$refId",
                                                                     "_cls": cls_name},
                                                                ]
                                                            },
                                                            None,
                                                        ]
                                                    },
                                                }
                                            },
                                        },
                                    }
                                }
                            },
                            f"${local_field}",
                        ]
                    }
                }
            }
        )

        self._pipeline.append(self._project_remove(docs_alias))

    # ----------------- generic lookups -----------------

    @staticmethod
    def _missing_generic_expr(ref_expr, cls_expr):
        return {"_missing_reference": True, "_ref": ref_expr, "_cls": cls_expr}

    @staticmethod
    def _generic_value_transform_expr(doc_classes, alias_for_cls, val_var="$$val"):
        expr = val_var
        for cls in reversed(doc_classes):
            alias_arr = f"${alias_for_cls(cls)}"
            class_test = Schema.regex_match(f"{val_var}._cls", cls)

            branch = {
                "$let": {
                    "vars": {
                        "matches": {"$filter": {"input": alias_arr, "as": "doc",
                                                "cond": {"$eq": ["$$doc._id", f"{val_var}._ref.$id"]}}}
                    },
                    "in": {
                        "$cond": [
                            {"$gt": [{"$size": "$$matches"}, 0]},
                            {"$mergeObjects": [{"$first": "$$matches"},
                                               {"_ref": f"{val_var}._ref", "_cls": f"{val_var}._cls"}]},
                            StageBuilder._missing_generic_expr(f"{val_var}._ref", f"{val_var}._cls"),
                        ]
                    },
                }
            }

            expr = {"$cond": [class_test, branch, expr]}
        return expr

    def _add_object_generic_lookup(self, generic_field, local_field):
        doc_classes = Schema.resolve_generic_choices(generic_field)
        if not doc_classes:
            return

        safe = local_field.replace(".", "_")

        def alias_for(cls):
            return f"{safe}__{cls.__name__}"

        # lookups per class
        for cls in doc_classes:
            ref_ids_expr = {
                "$cond": [
                    {"$eq": [{"$type": f"${local_field}"}, "object"]},
                    {
                        "$map": {
                            "input": {"$filter": {"input": {"$objectToArray": f"${local_field}"}, "as": "kv",
                                                  "cond": Schema.regex_match("$$kv.v._cls", cls)}},
                            "as": "kv",
                            "in": "$$kv.v._ref.$id",
                        }
                    },
                    [],
                ]
            }

            self._pipeline.append(
                {
                    "$lookup": {
                        "from": cls._get_collection_name(),
                        "let": {"refIds": ref_ids_expr},
                        "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                        "as": alias_for(cls),
                    }
                }
            )

        value_expr = self._generic_value_transform_expr(doc_classes, alias_for_cls=alias_for, val_var="$$val")

        self._pipeline.append(
            {
                "$addFields": {
                    local_field: {
                        "$cond": [
                            {"$eq": [{"$type": f"${local_field}"}, "object"]},
                            {
                                "$arrayToObject": {
                                    "$map": {
                                        "input": {"$objectToArray": f"${local_field}"},
                                        "as": "kv",
                                        "in": {"k": "$$kv.k",
                                               "v": {"$let": {"vars": {"val": "$$kv.v"}, "in": value_expr}}},
                                    }
                                }
                            },
                            f"${local_field}",
                        ]
                    }
                }
            }
        )

        self._pipeline.append(self._project_remove(*[alias_for(cls) for cls in doc_classes]))

    def _add_generic_lookup(self, field, local_field, is_list=False):
        doc_classes = Schema.resolve_generic_choices(field)
        if not doc_classes:
            return

        def alias_for(cls):
            return f"{local_field}__{cls.__name__}"

        # scalar generic
        if not is_list:
            for cls in doc_classes:
                self._pipeline.append(
                    {"$lookup": {"from": cls._get_collection_name(), "localField": f"{local_field}._ref.$id",
                                 "foreignField": "_id", "as": alias_for(cls)}}
                )

            transformed = self._generic_value_transform_expr(doc_classes, alias_for_cls=alias_for,
                                                             val_var=f"${local_field}")
            self._pipeline.append({"$addFields": {local_field: transformed}})
            self._pipeline.append(self._project_remove(*[alias_for(cls) for cls in doc_classes]))
            return

        # list generic
        for cls in doc_classes:
            self._pipeline.append(
                {"$lookup": {"from": cls._get_collection_name(), "localField": f"{local_field}._ref.$id",
                             "foreignField": "_id", "as": alias_for(cls)}}
            )

        item_expr = self._generic_value_transform_expr(doc_classes, alias_for_cls=alias_for, val_var="$$item")

        self._pipeline.append(
            {"$addFields": {local_field: {"$map": {"input": f"${local_field}", "as": "item", "in": item_expr}}}})
        self._pipeline.append(self._project_remove(*[alias_for(cls) for cls in doc_classes]))

    # ----------------- embedded list generic lookup (kept close to your working version) -----------------

    def _add_embedded_list_generic_lookup(self, generic_field, list_path, embedded_key):
        doc_classes = Schema.resolve_generic_choices(generic_field)
        if not doc_classes:
            return

        safe_list = list_path.replace(".", "_")
        raw_values_expr = {"$ifNull": [f"${list_path}.{embedded_key}", []]}

        def alias_for(cls):
            return f"{safe_list}_{embedded_key}__{cls.__name__}"

        def regex_match(input_expr, cls):
            return Schema.regex_match(input_expr, cls)

        aliases = []
        for cls in doc_classes:
            alias = alias_for(cls)
            aliases.append(alias)

            class_test_m = regex_match("$$m._cls", cls)
            class_test_this = regex_match("$$this._cls", cls)

            ref_ids_expr = {
                "$cond": [
                    {"$isArray": f"${list_path}"},
                    {
                        "$reduce": {
                            "input": raw_values_expr,
                            "initialValue": [],
                            "in": {
                                "$concatArrays": [
                                    "$$value",
                                    {
                                        "$cond": [
                                            {"$isArray": "$$this"},
                                            {
                                                "$map": {
                                                    "input": {"$filter": {"input": "$$this", "as": "m",
                                                                          "cond": class_test_m}},
                                                    "as": "m",
                                                    "in": "$$m._ref.$id",
                                                }
                                            },
                                            {"$cond": [class_test_this, ["$$this._ref.$id"], []]},
                                        ]
                                    },
                                ]
                            },
                        }
                    },
                    [],
                ]
            }

            self._pipeline.append(
                {"$lookup": {"from": cls._get_collection_name(), "let": {"refIds": ref_ids_expr},
                             "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}], "as": alias}}
            )

        def value_transform_expr():
            expr = "$$val"
            for cls in reversed(doc_classes):
                alias_arr = f"${alias_for(cls)}"
                class_test_val = regex_match("$$val._cls", cls)

                branch = {
                    "$let": {
                        "vars": {"matches": {"$filter": {"input": alias_arr, "as": "doc",
                                                         "cond": {"$eq": ["$$doc._id", "$$val._ref.$id"]}}}},
                        "in": {
                            "$cond": [
                                {"$gt": [{"$size": "$$matches"}, 0]},
                                {"$mergeObjects": [{"$first": "$$matches"},
                                                   {"_ref": "$$val._ref", "_cls": "$$val._cls"}]},
                                {"_missing_reference": True, "_ref": "$$val._ref", "_cls": "$$val._cls"},
                            ]
                        },
                    }
                }
                expr = {"$cond": [class_test_val, branch, expr]}
            return expr

        self._pipeline.append(
            {
                "$addFields": {
                    list_path: {
                        "$cond": [
                            {"$isArray": f"${list_path}"},
                            {
                                "$map": {
                                    "input": f"${list_path}",
                                    "as": "it",
                                    "in": {
                                        "$mergeObjects": [
                                            "$$it",
                                            {
                                                embedded_key: {
                                                    "$cond": [
                                                        {"$isArray": f"$$it.{embedded_key}"},
                                                        {"$map": {"input": f"$$it.{embedded_key}", "as": "val", "in": {
                                                            "$let": {"vars": {"val": "$$val"},
                                                                     "in": value_transform_expr()}}}},
                                                        {"$let": {"vars": {"val": f"$$it.{embedded_key}"},
                                                                  "in": value_transform_expr()}},
                                                    ]
                                                }
                                            },
                                        ]
                                    },
                                }
                            },
                            f"${list_path}",
                        ]
                    }
                }
            }
        )

        for alias in aliases:
            self._pipeline.append({"$project": {alias: 0}})

    # ----------------- abstract dbref lookup -----------------

    @staticmethod
    def _concrete_subclasses(doc_cls):
        result = set()

        def _walk(c):
            for sub in c.__subclasses__():
                meta = getattr(sub, "_meta", {})
                if meta.get("abstract"):
                    _walk(sub)
                else:
                    result.add(sub)
                    _walk(sub)

        _walk(doc_cls)
        return list(result)

    def _add_abstract_dbref_lookup(self, abstract_cls, field, local_field):
        subclasses = self._concrete_subclasses(abstract_cls)
        if not subclasses:
            return

        for cls in subclasses:
            try:
                coll = cls._get_collection_name()
            except Exception:
                coll = None
            if not coll:
                continue

            temp = f"{local_field}__{cls.__name__}"

            self._pipeline.append(
                {"$lookup": {"from": coll, "localField": f"{local_field}.$id", "foreignField": "_id", "as": temp}})

            self._pipeline.append(
                {
                    "$addFields": {
                        local_field: {
                            "$cond": [
                                {"$and": [{"$ifNull": [f"${local_field}", False]},
                                          {"$eq": [f"${local_field}.$ref", coll]}]},
                                {
                                    "$let": {
                                        "vars": {"matches": f"${temp}", "refId": f"${local_field}"},
                                        "in": {
                                            "$cond": [
                                                {"$gt": [{"$size": "$$matches"}, 0]},
                                                {"$mergeObjects": [{"$first": "$$matches"}, {"_ref": "$$refId"}]},
                                                {"_missing_reference": True, "_ref": "$$refId"},
                                            ]
                                        },
                                    }
                                },
                                f"${local_field}",
                            ]
                        }
                    }
                }
            )

            self._pipeline.append({"$project": {temp: 0}})
