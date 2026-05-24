from __future__ import annotations

from typing import Any

from .schema import Schema
from .match_planner import MatchPlanner


class StageBuilder:
    """
    Emit MongoDB aggregation stages for "select_related" and join-based filtering.

    Key goals:
      - Preserve raw reference values (ObjectId / DBRef) unless hydration is requested.
      - Allow "filter via join" without hydrating (root filtering uses joined docs).
      - Support nested traversal (embedded docs / embedded lists).
      - Handle "missing reference" by emitting a MongoEngine-friendly marker dict:
            {"_missing_reference": True, "_ref": <ObjectId-or-DBRef-id>}
        IMPORTANT: for ReferenceField this marker MUST NOT include "_cls",
        otherwise MapField(ReferenceField).__get__ may treat it like a GenericReference wrapper.
      - MongoDB version aware:
          * MongoDB >= 5.0 uses $getField for O(1) doc lookup by id (faster for large joined arrays).
          * MongoDB 4.2/4.4 uses $indexOfArray + $arrayElemAt for compatibility.
    """

    def __init__(self, mongo_version=None):
        self._pipeline: list[dict] = []
        self._mongo_version = mongo_version
        # $getField requires MongoDB >= 5.0 — gives O(1) ref hydration vs O(n) $indexOfArray scan.
        self._use_getfield = bool(mongo_version) and tuple(mongo_version)[:2] >= (5, 0)

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #

    def emit(
        self,
        doc_cls,
        prefix: str,
        tree: dict,
        buckets: dict | None,
        interleave: bool,
        embedded_list_path=None,
        hydrate_tree: dict | None = None,
    ) -> list[dict]:
        self._pipeline = []

        self._walk_lookups(
            doc_cls=doc_cls,
            prefix=prefix,
            tree=tree,
            buckets=buckets,
            embedded_list_path=embedded_list_path,
            interleave=interleave,
            hydrate_tree=hydrate_tree or {},
        )
        return self._pipeline

    # --------------------------------------------------------------------- #
    # Core traversal
    # --------------------------------------------------------------------- #

    def _walk_lookups(
        self,
        doc_cls,
        prefix: str,
        tree: dict,
        buckets: dict | None,
        embedded_list_path=None,
        interleave: bool = False,
        hydrate_tree: dict | None = None,
    ):
        from mongoengine.fields import (
            ReferenceField,
            GenericReferenceField,
            ListField,
            DictField,
            MapField,
            EmbeddedDocumentField,
            FileField,
        )

        hydrate_tree = hydrate_tree or {}

        def apply_bucket(full_path: str):
            if not interleave or buckets is None:
                return
            bucket = buckets.pop(full_path, None)
            if bucket:
                self._pipeline.append({"$match": bucket})

        for field_name, subtree in (tree or {}).items():
            if not field_name or field_name == "":
                continue

            field = doc_cls._fields.get(field_name)
            if not field:
                continue

            full_path = f"{prefix}{field.db_field}" if prefix else field.db_field

            requested_hydrate = field_name in hydrate_tree
            subtree_hydrate_tree = (
                hydrate_tree.get(field_name, {}) if requested_hydrate else {}
            )

            needs_traversal = bool(subtree) and not embedded_list_path
            hydrate_effective = requested_hydrate or needs_traversal
            preserve_orig = needs_traversal and not requested_hydrate
            orig_alias = (
                f"__orig__{full_path.replace('.', '_')}" if preserve_orig else None
            )

            # ---------------- ReferenceField ----------------
            if isinstance(field, ReferenceField):
                target = field.document_type_obj

                if embedded_list_path:
                    foreign_match = None
                    if interleave and buckets is not None:
                        foreign_match = self._pop_foreign_match_for_prefix(
                            buckets, full_path
                        )

                    self._add_embedded_list_structured_ref_lookup(
                        target_cls=target,
                        field_shape=field,
                        list_path=embedded_list_path,
                        embedded_key=field.db_field,
                        foreign_match=foreign_match,
                        hydrate=hydrate_effective,
                    )

                    if foreign_match is None:
                        apply_bucket(full_path)

                else:
                    if preserve_orig:
                        self._pipeline.append(
                            {"$addFields": {orig_alias: f"${full_path}"}}
                        )

                    foreign_match = None
                    if interleave and buckets is not None:
                        foreign_match = self._pop_foreign_match_for_prefix(
                            buckets, full_path
                        )

                    if target and target._meta.get("abstract", False):
                        self._add_abstract_dbref_lookup(target, full_path)
                        if foreign_match is not None:
                            self._pipeline.append({"$match": foreign_match})
                    else:
                        self._add_structured_ref_lookup(
                            target_cls=target,
                            field_shape=field,
                            local_field=full_path,
                            foreign_match=foreign_match,
                            hydrate=hydrate_effective,
                        )
                        if foreign_match is None:
                            apply_bucket(full_path)

                # descend
                if subtree and not embedded_list_path and target is not None:
                    self._walk_lookups(
                        target,
                        f"{full_path}.",
                        subtree,
                        buckets,
                        embedded_list_path,
                        interleave,
                        subtree_hydrate_tree,
                    )

                if preserve_orig:
                    self._pipeline.append({"$addFields": {full_path: f"${orig_alias}"}})
                    self._pipeline.append(self._project_remove(orig_alias))

                continue

            # ---------------- ListField ----------------
            if isinstance(field, ListField):
                if Schema.is_list_of_embedded(field):
                    embedded_doc = Schema.embedded_doc_type(field)
                    if subtree and embedded_doc:
                        self._walk_lookups(
                            embedded_doc,
                            f"{full_path}.",
                            subtree,
                            buckets,
                            embedded_list_path=full_path,
                            interleave=interleave,
                            hydrate_tree=subtree_hydrate_tree,
                        )
                    continue

                leaf, _depth = Schema.unwrap_list_field(field)

                # List[ReferenceField]
                if leaf is not None and isinstance(leaf, ReferenceField):
                    target = leaf.document_type

                    if embedded_list_path:
                        foreign_match = None
                        if interleave and buckets is not None:
                            foreign_match = self._pop_foreign_match_for_prefix(
                                buckets, full_path
                            )

                        self._add_embedded_list_structured_ref_lookup(
                            target_cls=target,
                            field_shape=field,
                            list_path=embedded_list_path,
                            embedded_key=field.db_field,
                            foreign_match=foreign_match,
                            hydrate=hydrate_effective,
                        )

                        if foreign_match is None:
                            apply_bucket(full_path)

                    else:
                        if preserve_orig:
                            self._pipeline.append(
                                {"$addFields": {orig_alias: f"${full_path}"}}
                            )

                        foreign_match = None
                        if interleave and buckets is not None:
                            foreign_match = self._pop_foreign_match_for_prefix(
                                buckets, full_path
                            )

                        self._add_structured_ref_lookup(
                            target_cls=target,
                            field_shape=field,
                            local_field=full_path,
                            foreign_match=foreign_match,
                            hydrate=hydrate_effective,
                        )

                        if foreign_match is None:
                            apply_bucket(full_path)

                    if subtree and not embedded_list_path and target is not None:
                        self._walk_lookups(
                            target,
                            f"{full_path}.",
                            subtree,
                            buckets,
                            full_path,  # walk inside list elements, not as a flat path
                            interleave,
                            subtree_hydrate_tree,
                        )

                    if preserve_orig:
                        self._pipeline.append(
                            {"$addFields": {full_path: f"${orig_alias}"}}
                        )
                        self._pipeline.append(self._project_remove(orig_alias))

                    continue

                # List[GenericReferenceField]
                if (
                    leaf is not None
                    and isinstance(leaf, GenericReferenceField)
                    and leaf.choices
                ):
                    if embedded_list_path:
                        foreign_match = None
                        if interleave and buckets is not None:
                            foreign_match = self._pop_foreign_match_for_prefix(
                                buckets, full_path
                            )

                        self._add_embedded_list_generic_lookup(
                            generic_field=leaf,
                            list_path=embedded_list_path,
                            embedded_key=field.db_field,
                            foreign_match=foreign_match,
                            hydrate=requested_hydrate,
                        )
                        if foreign_match is None:
                            apply_bucket(full_path)
                    else:
                        self._add_generic_lookup(leaf, full_path, is_list=True)
                        apply_bucket(full_path)
                    continue

            # ---------------- EmbeddedDocumentField ----------------
            if isinstance(field, EmbeddedDocumentField):
                if subtree:
                    self._walk_lookups(
                        field.document_type,
                        f"{full_path}.",
                        subtree,
                        buckets,
                        embedded_list_path,
                        interleave,
                        subtree_hydrate_tree,
                    )
                continue

            # ---------------- MapField(ReferenceField) ----------------
            if isinstance(field, MapField) and isinstance(field.field, ReferenceField):
                if embedded_list_path:
                    apply_bucket(full_path)
                    continue

                foreign_match = None
                if interleave and buckets is not None:
                    foreign_match = self._pop_foreign_match_for_prefix(
                        buckets, full_path
                    )

                target = field.field.document_type_obj or field.field.document_type

                self._add_structured_ref_lookup(
                    target_cls=target,
                    field_shape=field,
                    local_field=full_path,
                    foreign_match=foreign_match,
                    hydrate=requested_hydrate,
                )

                if foreign_match is None:
                    apply_bucket(full_path)
                continue

            # ---------------- DictField ----------------
            if isinstance(field, DictField):
                if embedded_list_path:
                    apply_bucket(full_path)
                    continue

                if isinstance(field.field, GenericReferenceField) and getattr(
                    field.field, "choices", None
                ):
                    foreign_match = None
                    if interleave and buckets is not None:
                        foreign_match = self._pop_foreign_match_for_prefix(
                            buckets, full_path
                        )

                    self._add_object_generic_lookup(
                        generic_field=field.field,
                        local_field=full_path,
                        foreign_match=foreign_match,
                        hydrate=requested_hydrate,
                    )

                    if foreign_match is None:
                        apply_bucket(full_path)
                    continue

                target = self._resolve_single_ref_target(field)
                if target is not None:
                    foreign_match = None
                    if interleave and buckets is not None:
                        foreign_match = self._pop_foreign_match_for_prefix(
                            buckets, full_path
                        )

                    self._add_structured_ref_lookup(
                        target_cls=target,
                        field_shape=field,
                        local_field=full_path,
                        foreign_match=foreign_match,
                        hydrate=requested_hydrate,
                    )

                    if foreign_match is None:
                        apply_bucket(full_path)
                    continue

            # ---------------- GenericReferenceField scalar ----------------
            if isinstance(field, GenericReferenceField) and field.choices:
                if embedded_list_path:
                    foreign_match = None
                    if interleave and buckets is not None:
                        foreign_match = self._pop_foreign_match_for_prefix(
                            buckets, full_path
                        )

                    self._add_embedded_list_generic_lookup(
                        generic_field=field,
                        list_path=embedded_list_path,
                        embedded_key=field.db_field,
                        foreign_match=foreign_match,
                        hydrate=requested_hydrate,
                    )
                    if foreign_match is None:
                        apply_bucket(full_path)

                else:
                    if preserve_orig:
                        self._pipeline.append(
                            {"$addFields": {orig_alias: f"${full_path}"}}
                        )

                    self._add_generic_lookup(field, full_path)
                    apply_bucket(full_path)

                    if subtree:
                        for sub_name, sub_tree in subtree.items():
                            if not sub_name or sub_name == "":
                                continue

                            common_ref_field, common_target = (
                                MatchPlanner.generic_common_ref(field, sub_name)
                            )
                            if common_ref_field is None or common_target is None:
                                continue

                            gp_path = f"{full_path}.{common_ref_field.db_field}"

                            foreign_match = None
                            if interleave and buckets is not None:
                                foreign_match = self._pop_foreign_match_for_prefix(
                                    buckets, gp_path
                                )

                            hydrate_gp = bool(subtree_hydrate_tree.get(sub_name))
                            hydrate_gp_effective = hydrate_gp or bool(sub_tree)

                            orig_gp_alias = None
                            if bool(sub_tree) and not hydrate_gp:
                                orig_gp_alias = f"__orig__{gp_path.replace('.', '_')}"
                                self._pipeline.append(
                                    {"$addFields": {orig_gp_alias: f"${gp_path}"}}
                                )

                            self._add_structured_ref_lookup(
                                target_cls=common_target,
                                field_shape=common_ref_field,
                                local_field=gp_path,
                                foreign_match=foreign_match,
                                hydrate=hydrate_gp_effective,
                            )

                            if foreign_match is None:
                                apply_bucket(gp_path)

                            if sub_tree:
                                self._walk_lookups(
                                    common_target,
                                    f"{gp_path}.",
                                    sub_tree,
                                    buckets,
                                    embedded_list_path,
                                    interleave,
                                    subtree_hydrate_tree.get(sub_name, {}),
                                )

                            if orig_gp_alias:
                                self._pipeline.append(
                                    {"$addFields": {gp_path: f"${orig_gp_alias}"}}
                                )
                                self._pipeline.append(
                                    self._project_remove(orig_gp_alias)
                                )

                    if preserve_orig:
                        self._pipeline.append(
                            {"$addFields": {full_path: f"${orig_alias}"}}
                        )
                        self._pipeline.append(self._project_remove(orig_alias))

                continue

            # ---------------- FileField ----------------
            if isinstance(field, FileField):
                continue

    # --------------------------------------------------------------------- #
    # Bucketing helpers
    # --------------------------------------------------------------------- #

    def _pop_foreign_match_for_prefix(self, buckets: dict, prefix: str) -> dict | None:
        if prefix not in buckets:
            return None
        candidate = buckets[prefix]
        foreign = self._to_foreign_match(candidate, prefix)
        if foreign is None:
            return None
        buckets.pop(prefix, None)
        return foreign

    def _to_foreign_match(self, match: Any, prefix: str) -> dict | None:
        if not isinstance(match, dict):
            return None

        for bad in ("$expr", "$where", "$function"):
            if bad in match:
                return None

        out: dict[str, Any] = {}
        want = prefix + "."

        for k, v in match.items():
            if not isinstance(k, str):
                return None

            if k in ("$and", "$or", "$nor"):
                if not isinstance(v, list):
                    return None
                sub = []
                for clause in v:
                    clause_foreign = self._to_foreign_match(clause, prefix)
                    if clause_foreign is None:
                        return None
                    sub.append(clause_foreign)
                out[k] = sub
                continue

            if k.startswith("$"):
                return None

            if not k.startswith(want):
                return None

            out[k[len(want) :]] = v

        return out or None

    # --------------------------------------------------------------------- #
    # Small utilities
    # --------------------------------------------------------------------- #

    @staticmethod
    def _project_remove(*paths: str) -> dict:
        return {"$project": {p: 0 for p in paths if p}}

    @staticmethod
    def _resolve_single_ref_target(field_shape):
        from mongoengine.fields import ReferenceField, ListField, DictField, MapField

        targets = set()

        def walk(f):
            if isinstance(f, ReferenceField):
                t = getattr(f, "document_type_obj", None) or getattr(
                    f, "document_type", None
                )
                if t is not None:
                    targets.add(t)
                return

            if isinstance(f, (ListField, DictField, MapField)):
                inner = getattr(f, "field", None)
                if inner is not None:
                    walk(inner)
                return

        walk(field_shape)
        if len(targets) == 1:
            return next(iter(targets))
        return None

    # --------------------------------------------------------------------- #
    # Ref-id extraction
    # --------------------------------------------------------------------- #

    @staticmethod
    def _build_ref_ids_expr(field, source_expr):
        from mongoengine.fields import (
            ReferenceField,
            ListField,
            DictField,
            GenericReferenceField,
            MapField,
        )

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
                                    StageBuilder._build_ref_ids_expr(
                                        field.field, "$$this"
                                    ),
                                ]
                            },
                        }
                    },
                    [],
                ]
            }

        if isinstance(field, (DictField, MapField)):
            obj_array = {"$objectToArray": source_expr}
            return {
                "$reduce": {
                    "input": obj_array,
                    "initialValue": [],
                    "in": {
                        "$concatArrays": [
                            "$$value",
                            StageBuilder._build_ref_ids_expr(field.field, "$$this.v"),
                        ]
                    },
                }
            }

        return []

    # --------------------------------------------------------------------- #
    # Hydration
    # --------------------------------------------------------------------- #

    @staticmethod
    def _missing_ref_expr(ref_id_expr: str) -> dict:
        # DO NOT include "_cls" here.
        return {"_missing_reference": True, "_ref": ref_id_expr}

    def _build_value_expr(self, field, source_expr, docs_expr):
        """
        Hydrate ReferenceField leaves inside an arbitrary field shape (scalar/list/dict/map).

        IMPORTANT for missing refs:
          - For ReferenceField we MUST emit:
                {"_missing_reference": True, "_ref": <ObjectId>}
            (NO "_cls")
          - If the stored value is a DBRef-like object, _ref must be its $id, not the object itself.

        Performance:
          - When _use_getfield is True (MongoDB >= 5.0), the docs array is converted ONCE
            into a hash {id_str: doc, ...} in an outer $let, then each ref leaf does an
            O(1) $getField lookup. This is dramatically faster for List/Map/Dict of refs
            against large joined collections (O(n+m) vs O(n*m)).
          - When _use_getfield is False, falls back to the legacy $indexOfArray scan path.
        """
        if self._use_getfield:
            return {
                "$let": {
                    "vars": {"docsHash": self._build_docs_hash_expr(docs_expr)},
                    "in": self._build_value_expr_inner(
                        field, source_expr, "$$docsHash", use_hash=True
                    ),
                }
            }
        return self._build_value_expr_inner(
            field, source_expr, docs_expr, use_hash=False
        )

    @staticmethod
    def _build_docs_hash_expr(docs_expr):
        """Build {$toString(_id): doc, ...} from a docs array — done ONCE per hydration."""
        docs_arr = {"$cond": [{"$isArray": docs_expr}, docs_expr, []]}
        return {
            "$arrayToObject": {
                "$map": {
                    "input": docs_arr,
                    "as": "d",
                    "in": {"k": {"$toString": "$$d._id"}, "v": "$$d"},
                }
            }
        }

    def _build_value_expr_inner(self, field, source_expr, lookup_expr, use_hash):
        """
        Recursive worker.
          - When use_hash=True: lookup_expr is a docs hash {id_str: doc}; leaf uses $getField.
          - When use_hash=False: lookup_expr is the raw docs array; leaf uses $indexOfArray.
        """
        from mongoengine.fields import (
            ReferenceField,
            ListField,
            DictField,
            GenericReferenceField,
            MapField,
        )

        # ---- ReferenceField (leaf) ----
        if isinstance(field, ReferenceField):
            return self._build_ref_leaf_expr(source_expr, lookup_expr, use_hash)

        # ---- GenericReferenceField leaf is handled elsewhere ----
        if isinstance(field, GenericReferenceField):
            return source_expr

        # ---- ListField ----
        if isinstance(field, ListField):
            return {
                "$cond": [
                    {"$isArray": source_expr},
                    {
                        "$map": {
                            "input": source_expr,
                            "as": "item",
                            "in": self._build_value_expr_inner(
                                field.field, "$$item", lookup_expr, use_hash
                            ),
                        }
                    },
                    source_expr,
                ]
            }

        # ---- DictField / MapField ----
        if isinstance(field, (DictField, MapField)):
            return {
                "$arrayToObject": {
                    "$map": {
                        "input": {"$objectToArray": source_expr},
                        "as": "kv",
                        "in": {
                            "k": "$$kv.k",
                            "v": self._build_value_expr_inner(
                                field.field, "$$kv.v", lookup_expr, use_hash
                            ),
                        },
                    }
                }
            }

        return source_expr

    @staticmethod
    def _build_ref_leaf_expr(source_expr, lookup_expr, use_hash):
        """Resolve a single ReferenceField — hash path uses $getField, legacy uses $indexOfArray."""
        if use_hash:
            return {
                "$let": {
                    "vars": {"orig": source_expr},
                    "in": {
                        "$cond": [
                            {"$ifNull": ["$$orig", False]},
                            {
                                "$let": {
                                    "vars": {
                                        "rid": {
                                            "$cond": [
                                                {
                                                    "$eq": [
                                                        {"$type": "$$orig"},
                                                        "object",
                                                    ]
                                                },
                                                "$$orig.$id",
                                                "$$orig",
                                            ]
                                        }
                                    },
                                    "in": {
                                        "$let": {
                                            "vars": {
                                                "found": {
                                                    "$getField": {
                                                        "field": {"$toString": "$$rid"},
                                                        "input": lookup_expr,
                                                    }
                                                }
                                            },
                                            "in": {
                                                "$ifNull": [
                                                    "$$found",
                                                    {
                                                        "_missing_reference": True,
                                                        "_ref": "$$rid",
                                                    },
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

        # Legacy $indexOfArray path (MongoDB < 5.0)
        docs_arr = {"$cond": [{"$isArray": lookup_expr}, lookup_expr, []]}
        return {
            "$let": {
                "vars": {"orig": source_expr},
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
                                            "docs": docs_arr,
                                            "idx": {
                                                "$indexOfArray": [
                                                    {
                                                        "$map": {
                                                            "input": docs_arr,
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
                                                {
                                                    "_missing_reference": True,
                                                    "_ref": "$$rid",
                                                },
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

    # --------------------------------------------------------------------- #
    # foreign-match translation for local filtering
    # --------------------------------------------------------------------- #

    @staticmethod
    def _foreign_match_to_expr(match: Any, var: str = "$$d") -> dict | None:
        if not isinstance(match, dict):
            return None

        for bad in ("$expr", "$where", "$function"):
            if bad in match:
                return None

        def field_expr(field_path: str, predicate: Any) -> dict | None:
            path = f"{var}.{field_path}" if field_path else var

            if not isinstance(predicate, dict) or not predicate:
                return {"$eq": [path, predicate]}

            parts: list[dict] = []
            regex_pat = None
            regex_opt = None

            for op, val in predicate.items():
                if op == "$eq":
                    parts.append({"$eq": [path, val]})
                elif op == "$ne":
                    parts.append({"$ne": [path, val]})
                elif op == "$gt":
                    parts.append({"$gt": [path, val]})
                elif op == "$gte":
                    parts.append({"$gte": [path, val]})
                elif op == "$lt":
                    parts.append({"$lt": [path, val]})
                elif op == "$lte":
                    parts.append({"$lte": [path, val]})
                elif op == "$in":
                    if not isinstance(val, list):
                        return None
                    parts.append({"$in": [path, val]})
                elif op == "$nin":
                    if not isinstance(val, list):
                        return None
                    parts.append({"$not": [{"$in": [path, val]}]})
                elif op == "$regex":
                    regex_pat = val
                elif op == "$options":
                    regex_opt = val
                elif op == "$exists":
                    return None
                else:
                    return None

            if regex_pat is not None:
                rm = {"input": path, "regex": regex_pat}
                if isinstance(regex_opt, str) and regex_opt:
                    rm["options"] = regex_opt
                parts.append({"$regexMatch": rm})

            if not parts:
                return None
            return parts[0] if len(parts) == 1 else {"$and": parts}

        def walk(node: Any) -> dict | None:
            if not isinstance(node, dict):
                return None

            for bad in ("$expr", "$where", "$function"):
                if bad in node:
                    return None

            exprs: list[dict] = []
            for k, v in node.items():
                if not isinstance(k, str):
                    return None

                if k in ("$and", "$or", "$nor"):
                    if not isinstance(v, list):
                        return None
                    sub_exprs: list[dict] = []
                    for clause in v:
                        ce = walk(clause)
                        if ce is None:
                            return None
                        sub_exprs.append(ce)

                    if k == "$and":
                        exprs.append(
                            sub_exprs[0] if len(sub_exprs) == 1 else {"$and": sub_exprs}
                        )
                    elif k == "$or":
                        exprs.append(
                            sub_exprs[0] if len(sub_exprs) == 1 else {"$or": sub_exprs}
                        )
                    else:
                        inner = (
                            sub_exprs[0] if len(sub_exprs) == 1 else {"$or": sub_exprs}
                        )
                        exprs.append({"$not": [inner]})
                    continue

                if k.startswith("$"):
                    return None

                fe = field_expr(k, v)
                if fe is None:
                    return None
                exprs.append(fe)

            if not exprs:
                return None
            return exprs[0] if len(exprs) == 1 else {"$and": exprs}

        return walk(match)

    # --------------------------------------------------------------------- #
    # Structured Reference lookup
    # --------------------------------------------------------------------- #

    def _add_structured_ref_lookup(
        self,
        target_cls,
        field_shape,
        local_field: str,
        foreign_match: dict | None = None,
        hydrate: bool = False,
    ):
        if not target_cls:
            return

        safe = local_field.replace(".", "_")
        docs_alias = f"{safe}__docs"

        ref_ids_expr = self._build_ref_ids_expr(field_shape, f"${local_field}")
        base_pipeline = [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}]

        self._pipeline.append(
            {
                "$lookup": {
                    "from": target_cls._get_collection_name(),
                    "let": {"refIds": ref_ids_expr},
                    "pipeline": list(base_pipeline),
                    "as": docs_alias,
                }
            }
        )

        filter_cond = None
        if foreign_match:
            cond = self._foreign_match_to_expr(foreign_match, var="$$d")
            if cond is not None:
                filter_cond = cond
                self._pipeline.append(
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {
                                        "$size": {
                                            "$filter": {
                                                "input": f"${docs_alias}",
                                                "as": "d",
                                                "cond": cond,
                                            }
                                        }
                                    },
                                    0,
                                ]
                            }
                        }
                    }
                )
            else:
                fallback_alias = f"{safe}__match_fallback"
                self._pipeline.append(
                    {
                        "$lookup": {
                            "from": target_cls._get_collection_name(),
                            "let": {"refIds": ref_ids_expr},
                            "pipeline": base_pipeline + [{"$match": foreign_match}],
                            "as": fallback_alias,
                        }
                    }
                )
                self._pipeline.append({"$match": {fallback_alias: {"$ne": []}}})
                self._pipeline.append({"$project": {fallback_alias: 0}})

        if hydrate:
            if filter_cond is not None:
                # Hydrate with only the docs that pass the filter, not the full docs_alias.
                # This ensures the field contains only the matched sub-documents.
                from mongoengine.fields import ListField

                filtered_expr = {
                    "$filter": {
                        "input": {
                            "$cond": [
                                {"$isArray": f"${docs_alias}"},
                                f"${docs_alias}",
                                [],
                            ]
                        },
                        "as": "d",
                        "cond": filter_cond,
                    }
                }
                if isinstance(field_shape, ListField):
                    self._pipeline.append({"$addFields": {local_field: filtered_expr}})
                else:
                    self._pipeline.append(
                        {
                            "$addFields": {
                                local_field: {
                                    "$let": {
                                        "vars": {"matches": filtered_expr},
                                        "in": {
                                            "$cond": [
                                                {"$gt": [{"$size": "$$matches"}, 0]},
                                                {"$arrayElemAt": ["$$matches", 0]},
                                                None,
                                            ]
                                        },
                                    }
                                }
                            }
                        }
                    )
            else:
                transformed = self._build_value_expr(
                    field_shape, f"${local_field}", f"${docs_alias}"
                )
                self._pipeline.append({"$addFields": {local_field: transformed}})

        self._pipeline.append({"$project": {docs_alias: 0}})

    # --------------------------------------------------------------------- #
    # Embedded list structured ref lookup
    # --------------------------------------------------------------------- #

    def _add_embedded_list_structured_ref_lookup(
        self,
        target_cls,
        field_shape,
        list_path: str,
        embedded_key: str,
        foreign_match: dict | None = None,
        hydrate: bool = True,
    ):
        if not target_cls:
            return

        safe_list = list_path.replace(".", "_")
        safe_key = embedded_key.replace(".", "_")
        docs_alias = f"{safe_list}_{safe_key}__docs"

        raw_values_expr = {"$ifNull": [f"${list_path}.{embedded_key}", []]}

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
                                self._build_ref_ids_expr(field_shape, "$$this"),
                            ]
                        },
                    }
                },
                [],
            ]
        }

        base_pipeline = [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}]

        self._pipeline.append(
            {
                "$lookup": {
                    "from": target_cls._get_collection_name(),
                    "let": {"refIds": ref_ids_expr},
                    "pipeline": list(base_pipeline),
                    "as": docs_alias,
                }
            }
        )

        if foreign_match:
            cond = self._foreign_match_to_expr(foreign_match, var="$$d")
            if cond is not None:
                self._pipeline.append(
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {
                                        "$size": {
                                            "$filter": {
                                                "input": f"${docs_alias}",
                                                "as": "d",
                                                "cond": cond,
                                            }
                                        }
                                    },
                                    0,
                                ]
                            }
                        }
                    }
                )
            else:
                match_alias = f"{safe_list}_{safe_key}__match_fallback"
                self._pipeline.append(
                    {
                        "$lookup": {
                            "from": target_cls._get_collection_name(),
                            "let": {"refIds": ref_ids_expr},
                            "pipeline": base_pipeline + [{"$match": foreign_match}],
                            "as": match_alias,
                        }
                    }
                )
                self._pipeline.append({"$match": {match_alias: {"$ne": []}}})
                self._pipeline.append({"$project": {match_alias: 0}})

        if hydrate:
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
                                                    embedded_key: self._build_value_expr(
                                                        field_shape,
                                                        f"$$it.{embedded_key}",
                                                        f"${docs_alias}",
                                                    )
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

        self._pipeline.append({"$project": {docs_alias: 0}})

    # --------------------------------------------------------------------- #
    # GenericReference support (unchanged behavior, no db checks)
    # --------------------------------------------------------------------- #

    def _add_object_generic_lookup(
        self,
        generic_field,
        local_field: str,
        foreign_match: dict | None = None,
        hydrate: bool = False,
    ):
        doc_classes = Schema.resolve_generic_choices(generic_field)
        if not doc_classes:
            return

        safe = local_field.replace(".", "_")

        def alias_for(cls):
            return f"{safe}__{cls.__name__}"

        aliases = []
        for cls in doc_classes:
            alias = alias_for(cls)
            aliases.append(alias)

            ref_ids_expr = {
                "$cond": [
                    {"$eq": [{"$type": f"${local_field}"}, "object"]},
                    {
                        "$map": {
                            "input": {
                                "$filter": {
                                    "input": {"$objectToArray": f"${local_field}"},
                                    "as": "kv",
                                    "cond": Schema.regex_match("$$kv.v._cls", cls),
                                }
                            },
                            "as": "kv",
                            "in": "$$kv.v._ref.$id",
                        }
                    },
                    [],
                ]
            }

            pipeline = [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}]
            if foreign_match:
                pipeline.append({"$match": foreign_match})

            self._pipeline.append(
                {
                    "$lookup": {
                        "from": cls._get_collection_name(),
                        "let": {"refIds": ref_ids_expr},
                        "pipeline": pipeline,
                        "as": alias,
                    }
                }
            )

        if foreign_match:
            self._pipeline.append(
                {"$match": {"$or": [{a: {"$ne": []}} for a in aliases]}}
            )

        if hydrate:
            value_expr = self._generic_value_transform_expr(
                doc_classes, alias_for_cls=alias_for, val_var="$$kv.v"
            )
            self._pipeline.append(
                {
                    "$addFields": {
                        local_field: {
                            "$cond": [
                                {"$eq": [{"$type": f"${local_field}"}, "object"]},
                                {
                                    "$arrayToObject": {
                                        "$map": {
                                            "input": {
                                                "$objectToArray": f"${local_field}"
                                            },
                                            "as": "kv",
                                            "in": {"k": "$$kv.k", "v": value_expr},
                                        }
                                    }
                                },
                                f"${local_field}",
                            ]
                        }
                    }
                }
            )

        self._pipeline.append(self._project_remove(*aliases))

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
                        "matches": {
                            "$filter": {
                                "input": alias_arr,
                                "as": "doc",
                                "cond": {"$eq": ["$$doc._id", f"{val_var}._ref.$id"]},
                            }
                        }
                    },
                    "in": {
                        "$cond": [
                            {"$gt": [{"$size": "$$matches"}, 0]},
                            {
                                "$mergeObjects": [
                                    {"$first": "$$matches"},
                                    {
                                        "_ref": f"{val_var}._ref",
                                        "_cls": f"{val_var}._cls",
                                    },
                                ]
                            },
                            StageBuilder._missing_generic_expr(
                                f"{val_var}._ref", f"{val_var}._cls"
                            ),
                        ]
                    },
                }
            }

            expr = {"$cond": [class_test, branch, expr]}
        return expr

    def _add_embedded_list_generic_lookup(
        self,
        generic_field,
        list_path: str,
        embedded_key: str,
        foreign_match: dict | None = None,
        hydrate: bool = True,
    ):
        # keep your existing implementation (db alias removed)
        doc_classes = Schema.resolve_generic_choices(generic_field)
        if not doc_classes:
            return

        safe_list = list_path.replace(".", "_")
        safe_key = embedded_key.replace(".", "_")
        raw_values_expr = {"$ifNull": [f"${list_path}.{embedded_key}", []]}

        def alias_docs(cls):
            return f"{safe_list}_{safe_key}__{cls.__name__}"

        def alias_match(cls):
            return f"{safe_list}_{safe_key}__{cls.__name__}__match"

        def regex_match(input_expr, cls):
            return Schema.regex_match(input_expr, cls)

        def ref_ids_expr_for(cls):
            class_test_m = regex_match("$$m._cls", cls)
            class_test_this = regex_match("$$this._cls", cls)

            return {
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
                                                    "input": {
                                                        "$filter": {
                                                            "input": "$$this",
                                                            "as": "m",
                                                            "cond": class_test_m,
                                                        }
                                                    },
                                                    "as": "m",
                                                    "in": "$$m._ref.$id",
                                                }
                                            },
                                            {
                                                "$cond": [
                                                    class_test_this,
                                                    ["$$this._ref.$id"],
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

        base = [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}]

        docs_aliases = []
        for cls in doc_classes:
            a_docs = alias_docs(cls)
            docs_aliases.append(a_docs)
            self._pipeline.append(
                {
                    "$lookup": {
                        "from": cls._get_collection_name(),
                        "let": {"refIds": ref_ids_expr_for(cls)},
                        "pipeline": list(base),
                        "as": a_docs,
                    }
                }
            )

        match_aliases = []
        if foreign_match:
            cond = self._foreign_match_to_expr(foreign_match, var="$$d")
            if cond is not None:
                self._pipeline.append(
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {
                                        "$gt": [
                                            {
                                                "$size": {
                                                    "$filter": {
                                                        "input": f"${alias_docs(cls)}",
                                                        "as": "d",
                                                        "cond": cond,
                                                    }
                                                }
                                            },
                                            0,
                                        ]
                                    }
                                    for cls in doc_classes
                                ]
                            }
                        }
                    }
                )
            else:
                for cls in doc_classes:
                    a_match = alias_match(cls)
                    match_aliases.append(a_match)
                    self._pipeline.append(
                        {
                            "$lookup": {
                                "from": cls._get_collection_name(),
                                "let": {"refIds": ref_ids_expr_for(cls)},
                                "pipeline": base + [{"$match": foreign_match}],
                                "as": a_match,
                            }
                        }
                    )
                self._pipeline.append(
                    {"$match": {"$or": [{a: {"$ne": []}} for a in match_aliases]}}
                )

                if not hydrate:
                    self._pipeline.append(
                        self._project_remove(*(match_aliases + docs_aliases))
                    )
                    return

        if hydrate:

            def vbase(cls):
                n = cls.__name__
                return n[:1].lower() + n[1:]

            docs_vars = {
                f"{vbase(cls)}Docs": {
                    "$cond": [
                        {"$isArray": f"${alias_docs(cls)}"},
                        f"${alias_docs(cls)}",
                        [],
                    ]
                }
                for cls in doc_classes
            }
            ids_vars = {
                f"{vbase(cls)}Ids": {
                    "$map": {"input": f"$${vbase(cls)}Docs", "as": "d", "in": "$$d._id"}
                }
                for cls in doc_classes
            }

            def hydrate_one_value(val_expr: str):
                expr = val_expr
                for cls in reversed(doc_classes):
                    vb = vbase(cls)
                    docs_var = f"$${vb}Docs"
                    ids_var = f"$${vb}Ids"
                    class_test_val = regex_match(f"{val_expr}._cls", cls)

                    branch = {
                        "$let": {
                            "vars": {
                                "ref": f"{val_expr}._ref",
                                "idx": {
                                    "$indexOfArray": [ids_var, f"{val_expr}._ref.$id"]
                                },
                            },
                            "in": {
                                "$cond": [
                                    {"$gte": ["$$idx", 0]},
                                    {
                                        "$mergeObjects": [
                                            {"$arrayElemAt": [docs_var, "$$idx"]},
                                            {
                                                "_ref": f"{val_expr}._ref",
                                                "_cls": f"{val_expr}._cls",
                                            },
                                        ]
                                    },
                                    {
                                        "_missing_reference": True,
                                        "_ref": "$$ref",
                                        "_cls": f"{val_expr}._cls",
                                    },
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
                                    "$let": {
                                        "vars": docs_vars,
                                        "in": {
                                            "$let": {
                                                "vars": ids_vars,
                                                "in": {
                                                    "$map": {
                                                        "input": f"${list_path}",
                                                        "as": "it",
                                                        "in": {
                                                            "$mergeObjects": [
                                                                "$$it",
                                                                {
                                                                    embedded_key: {
                                                                        "$cond": [
                                                                            {
                                                                                "$isArray": f"$$it.{embedded_key}"
                                                                            },
                                                                            {
                                                                                "$map": {
                                                                                    "input": f"$$it.{embedded_key}",
                                                                                    "as": "val",
                                                                                    "in": hydrate_one_value(
                                                                                        "$$val"
                                                                                    ),
                                                                                }
                                                                            },
                                                                            hydrate_one_value(
                                                                                f"$$it.{embedded_key}"
                                                                            ),
                                                                        ]
                                                                    }
                                                                },
                                                            ]
                                                        },
                                                    }
                                                },
                                            }
                                        },
                                    }
                                },
                                f"${list_path}",
                            ]
                        }
                    }
                }
            )

        self._pipeline.append(self._project_remove(*(docs_aliases + match_aliases)))

    def _add_generic_lookup(self, field, local_field, is_list=False):
        doc_classes = Schema.resolve_generic_choices(field)
        if not doc_classes:
            return

        def alias_for(cls):
            return f"{local_field}__{cls.__name__}"

        # ---------------- scalar GenericReferenceField ----------------
        if not is_list:
            for cls in doc_classes:
                self._pipeline.append(
                    {
                        "$lookup": {
                            "from": cls._get_collection_name(),
                            "localField": f"{local_field}._ref.$id",
                            "foreignField": "_id",
                            "as": alias_for(cls),
                        }
                    }
                )

            transformed = self._generic_value_transform_expr(
                doc_classes,
                alias_for_cls=alias_for,
                val_var="$$orig",  # IMPORTANT: never "$<field>" inside same $addFields
            )

            self._pipeline.append(
                {
                    "$addFields": {
                        local_field: {
                            "$let": {
                                "vars": {"orig": f"${local_field}"},
                                "in": transformed,
                            }
                        }
                    }
                }
            )

            self._pipeline.append(
                self._project_remove(*[alias_for(cls) for cls in doc_classes])
            )
            return

        # ---------------- list GenericReferenceField ----------------
        for cls in doc_classes:
            self._pipeline.append(
                {
                    "$lookup": {
                        "from": cls._get_collection_name(),
                        "localField": f"{local_field}._ref.$id",
                        "foreignField": "_id",
                        "as": alias_for(cls),
                    }
                }
            )

        item_expr = self._generic_value_transform_expr(
            doc_classes, alias_for_cls=alias_for, val_var="$$item"
        )
        self._pipeline.append(
            {
                "$addFields": {
                    local_field: {
                        "$map": {
                            "input": f"${local_field}",
                            "as": "item",
                            "in": item_expr,
                        }
                    }
                }
            }
        )
        self._pipeline.append(
            self._project_remove(*[alias_for(cls) for cls in doc_classes])
        )

    # --------------------------------------------------------------------- #
    # Abstract DBRef lookup
    # --------------------------------------------------------------------- #

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

    def _add_abstract_dbref_lookup(self, abstract_cls, local_field: str):
        subclasses = self._concrete_subclasses(abstract_cls)
        if not subclasses:
            return

        safe_local = local_field.replace(".", "_")

        ref_id_expr = {
            "$cond": [
                {"$eq": [{"$type": f"${local_field}"}, "object"]},
                f"${local_field}.$id",
                f"${local_field}",
            ]
        }

        for cls in subclasses:
            try:
                coll = cls._get_collection_name()
            except Exception:
                continue

            tmp = f"{safe_local}__{cls.__name__}"
            self._pipeline.append(
                {
                    "$lookup": {
                        "from": coll,
                        "let": {"rid": ref_id_expr},
                        "pipeline": [{"$match": {"$expr": {"$eq": ["$_id", "$$rid"]}}}],
                        "as": tmp,
                    }
                }
            )

            cls_name = getattr(cls, "_class_name", cls.__name__)

            self._pipeline.append(
                {
                    "$addFields": {
                        local_field: {
                            "$let": {
                                "vars": {"m": f"${tmp}", "v": f"${local_field}"},
                                "in": {
                                    "$cond": [
                                        {"$gt": [{"$size": "$$m"}, 0]},
                                        {
                                            "$mergeObjects": [
                                                {"$first": "$$m"},
                                                {"_cls": cls_name},
                                            ]
                                        },
                                        "$$v",
                                    ]
                                },
                            }
                        }
                    }
                }
            )

            self._pipeline.append({"$project": {tmp: 0}})
