"""
MongoDB Aggregation Pipeline Builder for MongoEngine QuerySets.

This module provides the PipelineBuilder class that converts MongoEngine QuerySets
into MongoDB aggregation pipelines with automatic dereferencing support for various
field types including ReferenceFields, GenericReferenceFields, and nested structures.
"""

import re

__all__ = ("PipelineBuilder", "needs_aggregation")


class PipelineBuilder:
    def __init__(self, queryset, max_depth=3):
        self.queryset = queryset
        self.document = queryset._document
        self.max_depth = max_depth
        self.pipeline = []

    # ======================================================================
    # PUBLIC API
    # ======================================================================
    def build(self):
        self._match_stage()

        if self.queryset._select_related:
            tree = self._build_related_tree(self.queryset._select_related)
            self._lookup_walk(self.document, "", tree)

        self._projection_stage()
        self._sort_stage()
        self._skip_stage()
        self._limit_stage()

        return self.pipeline

    # ======================================================================
    # STAGE BUILDERS: MATCH
    # ======================================================================
    @staticmethod
    def _convert_where_to_function(query: dict):
        if "$where" not in query:
            return query, None

        raw_js = query["$where"].strip()
        m = re.match(r"function\s*\(\s*\)\s*\{(.*)\}", raw_js, re.S)
        inner = m.group(1).strip() if m else raw_js
        inner = re.sub(r"\bthis\b", "doc", inner)

        cleaned = {k: v for k, v in query.items() if k != "$where"}
        function_expr = {
            "$expr": {
                "$function": {
                    "body": f"function(doc) {{ {inner} }}",
                    "args": ["$$ROOT"],
                    "lang": "js",
                }
            }
        }
        return cleaned, function_expr

    def _match_stage(self):
        mongo_query = self.queryset._query
        if not mongo_query:
            return

        mongo_query = self._walk_and_convert_regex(mongo_query)
        cleaned, function_expr = self._convert_where_to_function(mongo_query)

        if function_expr:
            if cleaned:
                self.pipeline.append({"$match": cleaned})
            self.pipeline.append({"$match": function_expr})
        else:
            self.pipeline.append({"$match": cleaned})

    # ======================================================================
    # LOOKUP TREE
    # ======================================================================
    @staticmethod
    def _build_related_tree(fields):
        tree = {}
        for f in fields:
            parts = f.split("__")
            node = tree
            for p in parts:
                node = node.setdefault(p, {})
            node[""] = True
        return tree

    # ======================================================================
    # Embedded doc helpers (robust across mongoengine versions)
    # ======================================================================
    @staticmethod
    def _is_list_of_embedded(field):
        from mongoengine.fields import EmbeddedDocumentListField, ListField, EmbeddedDocumentField

        return (
                isinstance(field, EmbeddedDocumentListField)
                or (
                        isinstance(field, ListField)
                        and isinstance(getattr(field, "field", None), EmbeddedDocumentField)
                )
        )

    @staticmethod
    def _embedded_doc_type(field):
        """
        Safely extract embedded document type from:
          - EmbeddedDocumentField
          - EmbeddedDocumentListField
          - ListField(EmbeddedDocumentField)
        """
        dt = getattr(field, "document_type", None)
        if dt:
            return dt
        inner = getattr(field, "field", None)
        dt = getattr(inner, "document_type", None) if inner else None
        if dt:
            return dt
        return None

    # ======================================================================
    # ListField helpers (supports nested ListField(ListField(...)))
    # ======================================================================
    @staticmethod
    def _unwrap_list_field(fld):
        """
        If fld is ListField(...ListField(...X)), return (leaf, depth).
        Otherwise return (None, 0).
        """
        from mongoengine.fields import ListField

        if not isinstance(fld, ListField):
            return None, 0

        depth = 0
        cur = fld
        while isinstance(cur, ListField):
            depth += 1
            cur = cur.field
        return cur, depth

    # ======================================================================
    # Embedded-list deref helper (ListField(EmbeddedDocument) containing refs)
    # ======================================================================
    def _add_embedded_list_structured_ref_lookup(
            self,
            target_cls,
            field_shape,  # ReferenceField or ListField(ReferenceField) etc
            list_path,  # e.g. "items"
            embedded_key,  # e.g. "song"
    ):
        """
        Produces the *working* pattern:
          - $lookup with refIds = [] unless $isArray("$items")
          - $addFields items = $map(...) only if $isArray("$items"), else keep "$items"
          - temp alias WITHOUT dots
        """
        if not target_cls:
            return

        safe_list = list_path.replace(".", "_")
        docs_alias = f"{safe_list}_{embedded_key}__docs"

        # IMPORTANT:
        # when items is array-of-embedded-docs, "$items.song" is already an array of ids
        ref_ids_expr = {
            "$cond": [
                {"$isArray": f"${list_path}"},
                {"$ifNull": [f"${list_path}.{embedded_key}", []]},
                [],
            ]
        }

        self.pipeline.append(
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
        per_item_value_expr = self._build_value_expr(
            field_shape,
            f"$$it.{embedded_key}",
            docs_expr,
        )

        self.pipeline.append(
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
                                            {embedded_key: per_item_value_expr},
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

        self.pipeline.append({"$project": {docs_alias: 0}})

    # ======================================================================
    # MapField deref helpers
    # ======================================================================
    def _add_map_ref_lookup(self, target_cls, map_field, local_field):
        """
        MapField(ReferenceField)
        Stored: {k: ObjectId} (or {k: DBRef} if dbref=True)
        """
        if not target_cls:
            return

        safe = local_field.replace(".", "_")
        docs_alias = f"{safe}__docs"
        is_dbref = bool(getattr(map_field.field, "dbref", False))
        id_from_value_expr = "$$kv.v.$id" if is_dbref else "$$kv.v"

        ref_ids_expr = {
            "$cond": [
                {"$eq": [{"$type": f"${local_field}"}, "object"]},
                {
                    "$map": {
                        "input": {"$objectToArray": f"${local_field}"},
                        "as": "kv",
                        "in": id_from_value_expr,
                    }
                },
                [],
            ]
        }

        self.pipeline.append({
            "$lookup": {
                "from": target_cls._get_collection_name(),
                "let": {"refIds": ref_ids_expr},
                "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                "as": docs_alias,
            }
        })

        cls_name = getattr(target_cls, "_class_name", target_cls.__name__)

        self.pipeline.append({
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
                                                    "matches": {
                                                        "$filter": {
                                                            "input": f"${docs_alias}",
                                                            "as": "doc",
                                                            "cond": {"$eq": ["$$doc._id", id_from_value_expr]},
                                                        }
                                                    },
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
        })

        self.pipeline.append({"$project": {docs_alias: 0}})

    def _add_map_generic_lookup(self, generic_field, local_field):
        """
        MapField(GenericReferenceField(choices=...))
        Stored: { k: { _ref: DBRef, _cls: "..." }, ... }
        """
        from mongoengine.document import _DocumentRegistry

        doc_classes = []
        for ch in getattr(generic_field, "choices", None) or ():
            if isinstance(ch, str):
                cls = _DocumentRegistry.get(ch)
            elif isinstance(ch, type):
                cls = _DocumentRegistry.get(ch.__name__)
            else:
                continue
            if cls:
                doc_classes.append(cls)
        if not doc_classes:
            return

        safe = local_field.replace(".", "_")

        for cls in doc_classes:
            alias = f"{safe}__{cls.__name__}"

            ref_ids_expr = {
                "$cond": [
                    {"$eq": [{"$type": f"${local_field}"}, "object"]},
                    {
                        "$map": {
                            "input": {
                                "$filter": {
                                    "input": {"$objectToArray": f"${local_field}"},
                                    "as": "kv",
                                    "cond": {
                                        "$regexMatch": {
                                            "input": "$$kv.v._cls",
                                            "regex": f"^{cls._class_name}(\\.|$)",
                                        }
                                    },
                                }
                            },
                            "as": "kv",
                            "in": "$$kv.v._ref.$id",
                        }
                    },
                    [],
                ]
            }

            self.pipeline.append({
                "$lookup": {
                    "from": cls._get_collection_name(),
                    "let": {"refIds": ref_ids_expr},
                    "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                    "as": alias,
                }
            })

        def value_transform_expr():
            expr = "$$val"
            for cls in reversed(doc_classes):
                alias_arr = f"${safe}__{cls.__name__}"
                class_test = {"$regexMatch": {"input": "$$val._cls", "regex": f"^{cls._class_name}(\\.|$)"}}
                branch = {
                    "$let": {
                        "vars": {
                            "matches": {
                                "$filter": {
                                    "input": alias_arr,
                                    "as": "doc",
                                    "cond": {"$eq": ["$$doc._id", "$$val._ref.$id"]},
                                }
                            }
                        },
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
                expr = {"$cond": [class_test, branch, expr]}
            return expr

        self.pipeline.append({
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
                                           "v": {"$let": {"vars": {"val": "$$kv.v"}, "in": value_transform_expr()}}},
                                }
                            }
                        },
                        f"${local_field}",
                    ]
                }
            }
        })

        for cls in doc_classes:
            self.pipeline.append({"$project": {f"{safe}__{cls.__name__}": 0}})

    # ======================================================================
    # DictField(GenericReferenceField) deref helper
    # ======================================================================
    def _add_dict_generic_lookup(self, generic_field, local_field):
        """
        DictField(GenericReferenceField(choices=...))
        Stored: { k: { _ref: DBRef, _cls: "..." }, ... }
        """
        from mongoengine.document import _DocumentRegistry

        doc_classes = []
        for ch in getattr(generic_field, "choices", None) or ():
            if isinstance(ch, str):
                cls = _DocumentRegistry.get(ch)
            elif isinstance(ch, type):
                cls = _DocumentRegistry.get(ch.__name__)
            else:
                continue
            if cls:
                doc_classes.append(cls)
        if not doc_classes:
            return

        safe = local_field.replace(".", "_")

        for cls in doc_classes:
            alias = f"{safe}__{cls.__name__}"

            ref_ids_expr = {
                "$cond": [
                    {"$eq": [{"$type": f"${local_field}"}, "object"]},
                    {
                        "$map": {
                            "input": {
                                "$filter": {
                                    "input": {"$objectToArray": f"${local_field}"},
                                    "as": "kv",
                                    "cond": {
                                        "$regexMatch": {
                                            "input": "$$kv.v._cls",
                                            "regex": f"^{cls._class_name}(\\.|$)",
                                        }
                                    },
                                }
                            },
                            "as": "kv",
                            "in": "$$kv.v._ref.$id",
                        }
                    },
                    [],
                ]
            }

            self.pipeline.append({
                "$lookup": {
                    "from": cls._get_collection_name(),
                    "let": {"refIds": ref_ids_expr},
                    "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                    "as": alias,
                }
            })

        def value_transform_expr():
            expr = "$$val"
            for cls in reversed(doc_classes):
                alias_arr = f"${safe}__{cls.__name__}"
                class_test = {"$regexMatch": {"input": "$$val._cls", "regex": f"^{cls._class_name}(\\.|$)"}}
                branch = {
                    "$let": {
                        "vars": {
                            "matches": {
                                "$filter": {
                                    "input": alias_arr,
                                    "as": "doc",
                                    "cond": {"$eq": ["$$doc._id", "$$val._ref.$id"]},
                                }
                            }
                        },
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
                expr = {"$cond": [class_test, branch, expr]}
            return expr

        self.pipeline.append({
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
                                           "v": {"$let": {"vars": {"val": "$$kv.v"}, "in": value_transform_expr()}}},
                                }
                            }
                        },
                        f"${local_field}",
                    ]
                }
            }
        })

        for cls in doc_classes:
            self.pipeline.append({"$project": {f"{safe}__{cls.__name__}": 0}})

    # ======================================================================
    # LOOKUP WALKER
    # ======================================================================
    def _lookup_walk(self, doc_cls, prefix, tree, embedded_list_path=None):
        from mongoengine.fields import (
            ReferenceField, GenericReferenceField,
            ListField, DictField, MapField, EmbeddedDocumentField, FileField,
        )

        for field_name, subtree in tree.items():
            if field_name == "":
                continue

            field = doc_cls._fields.get(field_name)
            if not field:
                continue

            full_path = f"{prefix}{field.db_field}" if prefix else field.db_field

            # =========================  REF FIELD  =========================
            if isinstance(field, ReferenceField):
                target = field.document_type_obj

                if embedded_list_path:
                    self._add_embedded_list_structured_ref_lookup(
                        target_cls=target,
                        field_shape=field,
                        list_path=embedded_list_path,
                        embedded_key=field.db_field,
                    )
                else:
                    if target and target._meta.get("abstract", False):
                        self._add_abstract_dbref_lookup(target, field, full_path)
                    else:
                        self._add_structured_ref_lookup(target, field, full_path)

                if subtree:
                    self._lookup_walk(
                        target,
                        prefix=f"{full_path}.",
                        tree=subtree,
                        embedded_list_path=embedded_list_path,
                    )
                continue

            # ==================== LIST (possibly nested) =====================
            if isinstance(field, ListField):
                leaf, _depth = self._unwrap_list_field(field)

                # nested list -> ReferenceField leaf
                if leaf is not None and isinstance(leaf, ReferenceField):
                    target = leaf.document_type

                    if embedded_list_path:
                        self._add_embedded_list_structured_ref_lookup(
                            target_cls=target,
                            field_shape=field,
                            list_path=embedded_list_path,
                            embedded_key=field.db_field,
                        )
                    else:
                        self._add_structured_ref_lookup(target, field, full_path)

                    if subtree:
                        self._lookup_walk(
                            target,
                            prefix=f"{full_path}.",
                            tree=subtree,
                            embedded_list_path=embedded_list_path,
                        )
                    continue

                # nested list -> GenericReferenceField leaf (flat list only)
                if leaf is not None and isinstance(leaf, GenericReferenceField):
                    if leaf.choices and not embedded_list_path:
                        self._add_generic_lookup(leaf, full_path, is_list=True)
                    continue

            # ==================== MapField(ReferenceField) =====================
            if isinstance(field, MapField) and isinstance(field.field, ReferenceField):
                if not embedded_list_path:
                    self._add_map_ref_lookup(
                        target_cls=field.field.document_type,
                        map_field=field,
                        local_field=full_path,
                    )
                continue

            # ==================== MapField(GenericReferenceField) =====================
            if isinstance(field, MapField) and isinstance(field.field, GenericReferenceField) and getattr(field.field,
                                                                                                          "choices",
                                                                                                          None):
                if not embedded_list_path:
                    self._add_map_generic_lookup(field.field, full_path)
                continue

            # ==================== DictField(GenericReferenceField) =====================
            if isinstance(field, DictField) and isinstance(field.field, GenericReferenceField) and getattr(field.field,
                                                                                                           "choices",
                                                                                                           None):
                if not embedded_list_path:
                    self._add_dict_generic_lookup(field.field, full_path)
                continue

            # ==================== LIST of EmbeddedDocument (descend) =====================
            if self._is_list_of_embedded(field):
                embedded_doc = self._embedded_doc_type(field)
                if subtree and embedded_doc:
                    self._lookup_walk(
                        embedded_doc,
                        prefix=f"{full_path}.",
                        tree=subtree,
                        embedded_list_path=full_path,
                    )
                continue

            # ==================== DictField References (ReferenceField only) ========================
            if isinstance(field, DictField):
                refs = self._collect_ref_document_types(field.field)
                if len(refs) == 1:
                    target = list(refs)[0]
                    if not embedded_list_path:
                        self._add_dictfield_lookup(target, field, full_path)

                    if subtree and not embedded_list_path:
                        self._lookup_walk(
                            target,
                            prefix=f"{full_path}.",
                            tree=subtree,
                            embedded_list_path=embedded_list_path,
                        )
                continue

            # ==================== Generic Reference (scalar) ===========================
            if isinstance(field, GenericReferenceField) and field.choices:
                if not embedded_list_path:
                    self._add_generic_lookup(field, full_path)
                continue

            if isinstance(field, EmbeddedDocumentField):
                if subtree:
                    self._lookup_walk(
                        field.document_type,
                        f"{full_path}.",
                        subtree,
                        embedded_list_path=embedded_list_path,
                    )
                continue

            if isinstance(field, FileField):
                continue

    # ======================================================================
    # HELPER: collect leaf ReferenceField document types under a field
    # ======================================================================
    def _collect_ref_document_types(self, field):
        from mongoengine.fields import ReferenceField, ListField, DictField, GenericReferenceField

        doc_types = set()

        if isinstance(field, ReferenceField):
            if field.document_type is not None:
                doc_types.add(field.document_type)
            return doc_types

        if isinstance(field, ListField):
            doc_types |= self._collect_ref_document_types(field.field)
            return doc_types

        if isinstance(field, DictField):
            if field.field is not None:
                doc_types |= self._collect_ref_document_types(field.field)
            return doc_types

        # We skip GenericReferenceField here (multi-collection)
        if isinstance(field, GenericReferenceField):
            return doc_types

        return doc_types

    # ======================================================================
    # HELPER: concrete subclasses for abstract Document classes
    # ======================================================================
    def _concrete_subclasses(self, doc_cls):
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

    # ======================================================================
    # HELPER: build expression that returns an array of ObjectIds
    # ======================================================================
    def _build_ref_ids_expr(self, field, source_expr):
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
                                "$concatArrays": [
                                    "$$value",
                                    self._build_ref_ids_expr(field.field, "$$this"),
                                ]
                            },
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
                    "in": {
                        "$concatArrays": [
                            "$$value",
                            self._build_ref_ids_expr(field.field, "$$this.v"),
                        ]
                    },
                }
            }

        return []

    # ======================================================================
    # HELPER: build expression that reconstructs value with docs instead of refs
    # ======================================================================
    def _build_value_expr(self, field, source_expr, docs_expr):
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
                                "matches": {
                                    "$filter": {
                                        "input": docs_expr,
                                        "as": "doc",
                                        "cond": {"$eq": ["$$doc._id", id_expr]},
                                    }
                                },
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
                    {
                        "$map": {
                            "input": source_expr,
                            "as": "item",
                            "in": self._build_value_expr(field.field, "$$item", docs_expr),
                        }
                    },
                    source_expr,
                ]
            }

        if isinstance(field, DictField):
            return {
                "$arrayToObject": {
                    "$map": {
                        "input": {"$objectToArray": source_expr},
                        "as": "kv",
                        "in": {
                            "k": "$$kv.k",
                            "v": self._build_value_expr(field.field, "$$kv.v", docs_expr),
                        },
                    }
                }
            }

        return source_expr

    # ======================================================================
    # STRUCTURED LOOKUP FOR ANY REFERENCE SHAPE (scalar, list, dict)
    # ======================================================================
    def _add_structured_ref_lookup(self, target_cls, field_shape, local_field):
        if not target_cls:
            return

        docs_alias = f"{local_field}__docs"
        ref_ids_expr = self._build_ref_ids_expr(field_shape, f"${local_field}")

        self.pipeline.append(
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

        self.pipeline.append({"$addFields": {local_field: transformed_expr}})
        self.pipeline.append({"$project": {docs_alias: 0}})

    def _add_dictfield_lookup(self, target_cls, dict_field, local_field):
        self._add_structured_ref_lookup(target_cls, dict_field, local_field)

    # ======================================================================
    # ABSTRACT DBRef REFERENCE LOOKUP (scalar ReferenceField → abstract base)
    # ======================================================================
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

            self.pipeline.append({
                "$lookup": {
                    "from": coll,
                    "localField": f"{local_field}.$id",
                    "foreignField": "_id",
                    "as": temp,
                }
            })

            self.pipeline.append({
                "$addFields": {
                    local_field: {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ifNull": [f"${local_field}", False]},
                                    {"$eq": [f"${local_field}.$ref", coll]},
                                ]
                            },
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
            })

            self.pipeline.append({"$project": {temp: 0}})

    # ======================================================================
    # GENERIC REFERENCE LOOKUP (scalar fields + list generic)
    # ======================================================================
    def _add_generic_lookup(self, field, local_field, is_list=False):
        from mongoengine.document import _DocumentRegistry

        doc_classes = []
        for ch in field.choices:
            if isinstance(ch, str):
                cls = _DocumentRegistry.get(ch)
            elif isinstance(ch, type):
                cls = _DocumentRegistry.get(ch.__name__)
            else:
                continue
            if cls:
                doc_classes.append(cls)
        if not doc_classes:
            return

        # SCALAR GENERIC
        if not is_list:
            for cls in doc_classes:
                temp = f"{local_field}__{cls.__name__}"

                self.pipeline.append({
                    "$lookup": {
                        "from": cls._get_collection_name(),
                        "localField": f"{local_field}._ref.$id",
                        "foreignField": "_id",
                        "as": temp,
                    }
                })

                class_test = {
                    "$regexMatch": {
                        "input": f"${local_field}._cls",
                        "regex": f"^{cls._class_name}(\\.|$)"
                    }
                }

                self.pipeline.append({
                    "$addFields": {
                        local_field: {
                            "$cond": [
                                class_test,
                                {
                                    "$let": {
                                        "vars": {
                                            "matches": f"${temp}",
                                            "refVal": f"${local_field}._ref",
                                            "clsVal": f"${local_field}._cls",
                                        },
                                        "in": {
                                            "$cond": [
                                                {"$gt": [{"$size": "$$matches"}, 0]},
                                                {"$mergeObjects": [{"$first": "$$matches"},
                                                                   {"_ref": "$$refVal", "_cls": "$$clsVal"}]},
                                                {"_missing_reference": True, "_ref": "$$refVal", "_cls": "$$clsVal"},
                                            ]
                                        }
                                    }
                                },
                                f"${local_field}"
                            ]
                        }
                    }
                })

                self.pipeline.append({"$project": {temp: 0}})
            return

        # LIST GENERIC (flat list only)
        for cls in doc_classes:
            self.pipeline.append({
                "$lookup": {
                    "from": cls._get_collection_name(),
                    "localField": f"{local_field}._ref.$id",
                    "foreignField": "_id",
                    "as": f"{local_field}__{cls.__name__}",
                }
            })

        def item_expr_for(cls):
            return {
                "$cond": [
                    {"$regexMatch": {"input": "$$item._cls", "regex": f"^{cls._class_name}(\\.|$)"}},
                    {
                        "$let": {
                            "vars": {
                                "matches": {
                                    "$filter": {
                                        "input": f"${local_field}__{cls.__name__}",
                                        "as": "doc",
                                        "cond": {"$eq": ["$$doc._id", "$$item._ref.$id"]},
                                    }
                                }
                            },
                            "in": {
                                "$cond": [
                                    {"$gt": [{"$size": "$$matches"}, 0]},
                                    {"$mergeObjects": [{"$first": "$$matches"},
                                                       {"_ref": "$$item._ref", "_cls": "$$item._cls"}]},
                                    {"_missing_reference": True, "_ref": "$$item._ref", "_cls": "$$item._cls"},
                                ]
                            }
                        }
                    },
                    "$$item"
                ]
            }

        def build_item_expr():
            expr = "$$item"
            for cls in reversed(doc_classes):
                expr = {"$cond": [
                    {"$regexMatch": {"input": "$$item._cls", "regex": f"^{cls._class_name}(\\.|$)"}},
                    item_expr_for(cls),
                    expr
                ]}
            return expr

        self.pipeline.append({
            "$addFields": {
                local_field: {
                    "$map": {
                        "input": f"${local_field}",
                        "as": "item",
                        "in": build_item_expr(),
                    }
                }
            }
        })

        for cls in doc_classes:
            self.pipeline.append({"$project": {f"{local_field}__{cls.__name__}": 0}})

    # ======================================================================
    # PROJECTION / SORT / LIMIT / ETC
    # ======================================================================
    def _projection_stage(self):
        lf = self.queryset._loaded_fields
        if not lf:
            return

        proj = lf.as_dict()
        if "_id" not in proj:
            proj["_id"] = 1

        self.pipeline.append({"$project": proj})

    def _sort_stage(self):
        ordering = self.queryset._ordering
        if not ordering:
            return
        sort_dict = {field: direction for field, direction in ordering}
        self.pipeline.append({"$sort": sort_dict})

    def _skip_stage(self):
        if self.queryset._skip:
            self.pipeline.append({"$skip": self.queryset._skip})

    def _limit_stage(self):
        if self.queryset._limit is not None:
            self.pipeline.append({"$limit": self.queryset._limit})

    # ======================================================================
    # HELPERS: projection check (kept for compatibility)
    # ======================================================================
    def _field_selected_by_projection(self, full_path):
        lf = self.queryset._loaded_fields
        if not lf:
            return True

        proj = lf.as_dict()
        if not proj:
            return True

        parts = full_path.split(".")

        for i in range(1, len(parts) + 1):
            key = ".".join(parts[:i])
            if key in proj and proj[key] == 0:
                return False

        has_include = any(v == 1 for v in proj.values())
        if not has_include:
            return True

        for i in range(len(parts), 0, -1):
            key = ".".join(parts[:i])
            if key in proj:
                return proj[key] == 1

        return False

    # ======================================================================
    # HELPERS: regex conversion
    # ======================================================================
    def _convert_regex(self, value):
        if isinstance(value, re.Pattern):
            pattern = value.pattern
            opts = ""
            if value.flags & re.IGNORECASE:
                opts += "i"
            if value.flags & re.MULTILINE:
                opts += "m"
            if value.flags & re.DOTALL:
                opts += "s"
            return {"$regex": pattern, "$options": opts} if opts else {"$regex": pattern}
        return value

    def _walk_and_convert_regex(self, obj):
        if isinstance(obj, dict):
            return {k: self._walk_and_convert_regex(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._walk_and_convert_regex(v) for v in obj]
        return self._convert_regex(obj)


def needs_aggregation(queryset):
    doc = queryset._document
    lf = queryset._loaded_fields
    projections = lf.as_dict() if lf else None

    from mongoengine.fields import (
        ReferenceField,
        EmbeddedDocumentField,
        EmbeddedDocumentListField,
        ListField,
        GenericReferenceField,
        DictField,
        MapField,
    )

    def is_list_of_embedded(fld):
        return (
                isinstance(fld, EmbeddedDocumentListField)
                or (
                        isinstance(fld, ListField)
                        and isinstance(getattr(fld, "field", None), EmbeddedDocumentField)
                )
        )

    def embedded_doc_type(fld):
        dt = getattr(fld, "document_type", None)
        if dt:
            return dt
        inner = getattr(fld, "field", None)
        dt = getattr(inner, "document_type", None) if inner else None
        if dt:
            return dt
        return None

    def unwrap_list(fld):
        cur = fld
        while isinstance(cur, ListField):
            cur = cur.field
        return cur

    def field_path_requires_lookup(parts):
        cls = doc
        for p in parts:
            if not cls:
                return False

            fld = cls._fields.get(p)
            if not fld:
                return False

            if isinstance(fld, (DictField, MapField)):
                sub = fld.field
                if isinstance(sub, ReferenceField):
                    return True
                if isinstance(sub, GenericReferenceField):
                    return bool(getattr(sub, "choices", None))
                if isinstance(sub, ListField):
                    leaf = unwrap_list(sub)
                    if isinstance(leaf, ReferenceField):
                        return True
                    if isinstance(leaf, GenericReferenceField):
                        return bool(getattr(leaf, "choices", None))

            if isinstance(fld, GenericReferenceField):
                return bool(getattr(fld, "choices", None))

            if isinstance(fld, ReferenceField):
                return True

            if isinstance(fld, ListField):
                leaf = unwrap_list(fld)
                if isinstance(leaf, ReferenceField):
                    return True
                if isinstance(leaf, GenericReferenceField):
                    return bool(getattr(leaf, "choices", None))

            if isinstance(fld, EmbeddedDocumentField) or is_list_of_embedded(fld):
                cls = embedded_doc_type(fld)
                continue

            cls = None

        return False

    mongo_query = queryset._query or {}
    for key in mongo_query.keys():
        if field_path_requires_lookup(key.split("__")):
            return True

    ordering = queryset._ordering or []
    for item in ordering:
        field = item[0] if isinstance(item, (tuple, list)) else item
        clean = field.lstrip("-").lstrip("+")
        if field_path_requires_lookup(clean.split("__")):
            return True

    def field_is_projected(name):
        if projections is None or projections == {}:
            return True
        if name in projections:
            return True
        return any(k.startswith(name + ".") for k in projections)

    def needs_lookup_for_field(field, seen_embedded=None):
        if seen_embedded is None:
            seen_embedded = set()

        if isinstance(field, (DictField, MapField)):
            sub = field.field
            if isinstance(sub, ReferenceField):
                return True
            if isinstance(sub, GenericReferenceField):
                return bool(getattr(sub, "choices", None))
            if isinstance(sub, ListField):
                leaf = unwrap_list(sub)
                if isinstance(leaf, ReferenceField):
                    return True
                if isinstance(leaf, GenericReferenceField):
                    return bool(getattr(leaf, "choices", None))

        if isinstance(field, GenericReferenceField):
            return bool(getattr(field, "choices", None))

        if isinstance(field, ReferenceField):
            return True

        if isinstance(field, ListField):
            leaf = unwrap_list(field)
            if isinstance(leaf, ReferenceField):
                return True
            if isinstance(leaf, GenericReferenceField):
                return bool(getattr(leaf, "choices", None))

        if isinstance(field, EmbeddedDocumentField) or is_list_of_embedded(field):
            dt = embedded_doc_type(field)
            if not dt or dt in seen_embedded:
                return False
            seen2 = set(seen_embedded)
            seen2.add(dt)
            return any(needs_lookup_for_field(sub, seen2) for sub in dt._fields.values())

        return False

    for name, field in doc._fields.items():
        if field_is_projected(name) and needs_lookup_for_field(field):
            return True

    return False
