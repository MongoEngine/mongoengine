from __future__ import annotations

from typing import Any, Dict, Optional, List

from .schema import Schema
from .match_planner import MatchPlanner


class StageBuilder:
    """
    Emits Mongo aggregation stages from a lookup tree.

    Policy:
      - Only hydrate ($addFields overwrite reference) when select_related asked for it.
      - Otherwise: lookup is filter-only (keeps ObjectId/DBRef unchanged) BUT still filters via join results.
      - If deeper traversal is required to evaluate lookups, we may hydrate temporarily and restore original.
    """

    def emit(
            self,
            doc_cls,
            prefix: str,
            tree: dict,
            buckets: Optional[dict],
            interleave: bool,
            embedded_list_path=None,
            hydrate_tree: Optional[dict] = None,
    ):
        stages: List[dict] = []
        self._pipeline = stages

        self._walk_lookups(
            doc_cls=doc_cls,
            prefix=prefix,
            tree=tree,
            buckets=buckets,
            embedded_list_path=embedded_list_path,
            interleave=interleave,
            hydrate_tree=hydrate_tree or {},
        )
        return stages

    def _walk_lookups(
            self,
            doc_cls,
            prefix,
            tree,
            buckets,
            embedded_list_path=None,
            interleave=False,
            hydrate_tree=None,
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
            if field_name == "":
                continue

            field = doc_cls._fields.get(field_name)
            if not field:
                continue

            full_path = f"{prefix}{field.db_field}" if prefix else field.db_field

            requested_hydrate = field_name in hydrate_tree
            subtree_hydrate_tree = hydrate_tree.get(field_name, {}) if requested_hydrate else {}

            # If we need to deref deeper (subtree) and we're not in embedded_list_path mode,
            # we must hydrate to traverse. If hydrate wasn’t requested, we preserve+restore.
            needs_traversal = bool(subtree) and not embedded_list_path
            hydrate_effective = requested_hydrate or needs_traversal
            preserve_orig = needs_traversal and not requested_hydrate
            orig_alias = f"__orig__{full_path.replace('.', '_')}" if preserve_orig else None

            # ---------------- ReferenceField ----------------
            if isinstance(field, ReferenceField):
                target = field.document_type_obj

                if embedded_list_path:
                    foreign_match = None
                    if interleave and buckets is not None:
                        foreign_match = self._pop_foreign_match_for_prefix(buckets, full_path)

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
                        self._pipeline.append({"$addFields": {orig_alias: f"${full_path}"}})

                    foreign_match = None
                    if interleave and buckets is not None:
                        foreign_match = self._pop_foreign_match_for_prefix(buckets, full_path)

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

                # descend (only meaningful outside embedded-list mode)
                if subtree and not embedded_list_path:
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

            # ---------------- ListField(...) ----------------
            if isinstance(field, ListField):
                # handle list of embedded docs BEFORE unwrap refs/generics
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
                            foreign_match = self._pop_foreign_match_for_prefix(buckets, full_path)

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
                            self._pipeline.append({"$addFields": {orig_alias: f"${full_path}"}})

                        foreign_match = None
                        if interleave and buckets is not None:
                            foreign_match = self._pop_foreign_match_for_prefix(buckets, full_path)

                        self._add_structured_ref_lookup(
                            target_cls=target,
                            field_shape=field,
                            local_field=full_path,
                            foreign_match=foreign_match,
                            hydrate=hydrate_effective,
                        )

                        if foreign_match is None:
                            apply_bucket(full_path)

                    if subtree and not embedded_list_path:
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

                # List[GenericReferenceField]
                if leaf is not None and isinstance(leaf, GenericReferenceField):
                    if leaf.choices:
                        if embedded_list_path:
                            foreign_match = None
                            if interleave and buckets is not None:
                                foreign_match = self._pop_foreign_match_for_prefix(buckets, full_path)

                            self._add_embedded_list_generic_lookup(
                                generic_field=leaf,
                                list_path=embedded_list_path,
                                embedded_key=field.db_field,
                                foreign_match=foreign_match,
                                hydrate=requested_hydrate,  # select_related only
                            )

                            if foreign_match is None:
                                apply_bucket(full_path)

                        else:
                            # scalar list-of-generic lookup builder currently hydrates; keep behavior
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
                    foreign_match = self._pop_foreign_match_for_prefix(buckets, full_path)

                target = field.field.document_type_obj or field.field.document_type

                # hydrate only if select_related asked for it
                self._add_structured_ref_lookup(
                    target_cls=target,
                    field_shape=field,  # <-- MapField shape
                    local_field=full_path,  # <-- "nodes"
                    foreign_match=foreign_match,
                    hydrate=requested_hydrate,  # <-- THIS is the key
                )

                if foreign_match is None:
                    apply_bucket(full_path)

                continue

            # ---------------- DictField(... ReferenceField ...) ----------------
            if isinstance(field, DictField):
                if embedded_list_path:
                    apply_bucket(full_path)
                    continue

                target = self._resolve_single_ref_target(field)
                if target is not None:
                    foreign_match = None
                    if interleave and buckets is not None:
                        foreign_match = self._pop_foreign_match_for_prefix(buckets, full_path)

                    # hydrate ONLY if select_related asked for it (or traversal)
                    self._add_structured_ref_lookup(
                        target_cls=target,
                        field_shape=field,  # DictField shape (may nest lists/dicts/refs)
                        local_field=full_path,  # "mapping0"
                        foreign_match=foreign_match,
                        hydrate=requested_hydrate,  # <-- key for this test
                    )

                    if foreign_match is None:
                        apply_bucket(full_path)

                    continue

            # ---------------- DictField(GenericReferenceField) ----------------
            if (
                    isinstance(field, DictField)
                    and isinstance(field.field, GenericReferenceField)
                    and getattr(field.field, "choices", None)
            ):
                if embedded_list_path:
                    apply_bucket(full_path)
                    continue

                foreign_match = None
                if interleave and buckets is not None:
                    foreign_match = self._pop_foreign_match_for_prefix(buckets, full_path)

                # IMPORTANT: hydrate only when select_related asked for it
                self._add_object_generic_lookup(
                    generic_field=field.field,
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
                    # IMPORTANT: embedded-list scalar generic also needs foreign_match pop
                    foreign_match = None
                    if interleave and buckets is not None:
                        foreign_match = self._pop_foreign_match_for_prefix(buckets, full_path)

                    self._add_embedded_list_generic_lookup(
                        generic_field=field,
                        list_path=embedded_list_path,
                        embedded_key=field.db_field,
                        foreign_match=foreign_match,
                        hydrate=requested_hydrate,  # select_related only
                    )

                    if foreign_match is None:
                        apply_bucket(full_path)

                else:
                    # For traversal under generic, we may hydrate temporarily, then restore.
                    if preserve_orig:
                        self._pipeline.append({"$addFields": {orig_alias: f"${full_path}"}})

                    # NOTE: _add_generic_lookup always hydrates this field; that's fine.
                    self._add_generic_lookup(field, full_path)
                    apply_bucket(full_path)

                    # ---- SAFE traversal under generic (target__gp__...)
                    if subtree:
                        for sub_name, sub_tree in subtree.items():
                            if sub_name == "":
                                continue

                            common_ref_field, common_target = MatchPlanner.generic_common_ref(field, sub_name)
                            if common_ref_field is None or common_target is None:
                                continue

                            gp_path = f"{full_path}.{common_ref_field.db_field}"

                            foreign_match = None
                            if interleave and buckets is not None:
                                foreign_match = self._pop_foreign_match_for_prefix(buckets, gp_path)

                            hydrate_gp = bool(subtree_hydrate_tree.get(sub_name))
                            hydrate_gp_effective = hydrate_gp or bool(sub_tree)

                            orig_gp_alias = None
                            if bool(sub_tree) and not hydrate_gp:
                                orig_gp_alias = f"__orig__{gp_path.replace('.', '_')}"
                                self._pipeline.append({"$addFields": {orig_gp_alias: f"${gp_path}"}})

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
                                self._pipeline.append({"$addFields": {gp_path: f"${orig_gp_alias}"}})
                                self._pipeline.append(self._project_remove(orig_gp_alias))

                    if preserve_orig:
                        self._pipeline.append({"$addFields": {full_path: f"${orig_alias}"}})
                        self._pipeline.append(self._project_remove(orig_alias))

                continue

            if isinstance(field, FileField):
                continue

    # ----------------- OPTIMIZATION HELPERS -----------------

    def _pop_foreign_match_for_prefix(self, buckets: dict, prefix: str) -> Optional[dict]:
        if prefix not in buckets:
            return None
        candidate = buckets[prefix]
        foreign = self._to_foreign_match(candidate, prefix)
        if foreign is None:
            return None
        buckets.pop(prefix, None)
        return foreign

    def _to_foreign_match(self, match: Any, prefix: str) -> Optional[dict]:
        if not isinstance(match, dict):
            return None

        for bad in ("$expr", "$where", "$function"):
            if bad in match:
                return None

        out: Dict[str, Any] = {}
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

            out[k[len(want):]] = v

        return out or None

    # ----------------- helpers -----------------

    @staticmethod
    def _project_remove(*paths: str) -> dict:
        return {"$project": {p: 0 for p in paths if p}}

    @staticmethod
    def _is_list_of_embedded(field) -> bool:
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
    def _resolve_single_ref_target(field_shape):
        """
        If field_shape contains ReferenceField leaves that all point to the same target,
        return that target_cls, else None.
        """
        from mongoengine.fields import ReferenceField, ListField, DictField, MapField

        targets = set()

        def walk(f):
            if isinstance(f, ReferenceField):
                t = getattr(f, "document_type_obj", None) or getattr(f, "document_type", None)
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

    @staticmethod
    def _docs_to_id_map_expr(docs_expr):
        """
        Build { "<_id str>": <doc> } from an array of docs.
        """
        return {
            "$arrayToObject": {
                "$map": {
                    "input": docs_expr,
                    "as": "d",
                    "in": {"k": {"$toString": "$$d._id"}, "v": "$$d"},
                }
            }
        }

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
        """4.2-safe hydrate expression for ReferenceField/MapField/ListField/DictField."""
        from mongoengine.fields import ReferenceField, ListField, DictField, GenericReferenceField

        # ---- ReferenceField ----
        if isinstance(field, ReferenceField):
            id_expr = f"{source_expr}.$id" if field.dbref else source_expr

            # docs_expr must always be an array
            docs_arr = {
                "$cond": [{"$isArray": docs_expr}, docs_expr, []]
            }
            ids_arr = {"$map": {"input": docs_arr, "as": "d", "in": "$$d._id"}}

            return {
                "$cond": [
                    {"$ifNull": [source_expr, False]},
                    {
                        "$let": {
                            "vars": {
                                "docs": docs_arr,
                                "ids": ids_arr,
                                "refId": id_expr,
                                "idx": {"$indexOfArray": [ids_arr, id_expr]},
                            },
                            "in": {
                                "$cond": [
                                    {"$gte": ["$$idx", 0]},
                                    # hydrated doc
                                    {"$arrayElemAt": ["$$docs", "$$idx"]},
                                    # explicit missing marker
                                    {
                                        "_missing_reference": True,
                                        "_ref": "$$refId",
                                    },
                                ]
                            },
                        }
                    },
                    None,
                ]
            }

        # ---- GenericReferenceField ----
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
                            "in": StageBuilder._build_value_expr(field.field, "$$item", docs_expr),
                        }
                    },
                    source_expr,
                ]
            }

        # ---- DictField / MapField ----
        if isinstance(field, DictField):
            return {
                "$arrayToObject": {
                    "$map": {
                        "input": {"$objectToArray": source_expr},
                        "as": "kv",
                        "in": {
                            "k": "$$kv.k",
                            "v": StageBuilder._build_value_expr(field.field, "$$kv.v", docs_expr),
                        },
                    }
                }
            }

        return source_expr

    @staticmethod
    def _foreign_match_to_expr(match: Any, var: str = "$$d") -> Optional[dict]:
        """
        Convert a foreign-doc match dict (keys relative to the foreign doc) into an $expr condition
        usable inside $filter cond.

        Supported:
          - field predicates with scalar equality
          - field predicates with ops: $eq,$ne,$gt,$gte,$lt,$lte,$in,$nin
          - $and/$or/$nor recursively
          - $regex (+ optional $options) via $regexMatch

        Rejects (returns None):
          - $expr/$where/$function anywhere
          - unknown operators ($geo*, $elemMatch, etc.)
          - $exists (ambiguous under $expr if nulls are allowed)
        """
        if not isinstance(match, dict):
            return None

        # hard reject unsafe
        for bad in ("$expr", "$where", "$function"):
            if bad in match:
                return None

        def field_expr(field_path: str, predicate: Any) -> Optional[dict]:
            path = f"{var}.{field_path}" if field_path else var

            # scalar => equality
            if not isinstance(predicate, dict) or not predicate:
                return {"$eq": [path, predicate]}

            parts: List[dict] = []
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
                    # val must be an array for $in
                    if not isinstance(val, list):
                        return None
                    parts.append({"$in": [path, val]})
                elif op == "$nin":
                    if not isinstance(val, list):
                        return None
                    parts.append({"$not": [{"$in": [path, val]}]})
                elif op == "$regex":
                    # translate to $regexMatch; handle $options if present
                    regex_pat = val
                elif op == "$options":
                    regex_opt = val
                elif op == "$exists":
                    # can't translate safely in general (null vs missing ambiguity)
                    return None
                else:
                    # unknown / unsupported operator
                    return None

            if regex_pat is not None:
                rm = {"input": path, "regex": regex_pat}
                if isinstance(regex_opt, str) and regex_opt:
                    rm["options"] = regex_opt
                parts.append({"$regexMatch": rm})

            if not parts:
                return None
            if len(parts) == 1:
                return parts[0]
            return {"$and": parts}

        def walk(node: Any) -> Optional[dict]:
            if not isinstance(node, dict):
                return None

            for bad in ("$expr", "$where", "$function"):
                if bad in node:
                    return None

            exprs: List[dict] = []

            for k, v in node.items():
                if not isinstance(k, str):
                    return None

                if k in ("$and", "$or", "$nor"):
                    if not isinstance(v, list):
                        return None
                    sub_exprs: List[dict] = []
                    for clause in v:
                        ce = walk(clause)
                        if ce is None:
                            return None
                        sub_exprs.append(ce)

                    if k == "$and":
                        exprs.append(sub_exprs[0] if len(sub_exprs) == 1 else {"$and": sub_exprs})
                    elif k == "$or":
                        exprs.append(sub_exprs[0] if len(sub_exprs) == 1 else {"$or": sub_exprs})
                    else:  # $nor
                        # nor(a,b) == not(or(a,b))
                        inner = sub_exprs[0] if len(sub_exprs) == 1 else {"$or": sub_exprs}
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
            if len(exprs) == 1:
                return exprs[0]
            return {"$and": exprs}

        return walk(match)

    def _add_structured_ref_lookup(
            self,
            target_cls,
            field_shape,
            local_field: str,
            foreign_match: Optional[dict] = None,
            hydrate: bool = False,
    ):
        if not target_cls:
            return

        safe = local_field.replace(".", "_")
        docs_alias = f"{safe}__docs"

        ref_ids_expr = self._build_ref_ids_expr(field_shape, f"${local_field}")
        base_pipeline = [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}]

        # 1) Always do a SINGLE unfiltered lookup (only by refIds)
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

        # 2) If we have a foreign_match, filter roots LOCALLY against joined docs
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
                # fallback: if we can't safely translate, keep old behavior by pushing down
                # (optional; you can remove this fallback if you prefer strict)
                self._pipeline.append(
                    {
                        "$lookup": {
                            "from": target_cls._get_collection_name(),
                            "let": {"refIds": ref_ids_expr},
                            "pipeline": base_pipeline + [{"$match": foreign_match}],
                            "as": f"{safe}__match_fallback",
                        }
                    }
                )
                self._pipeline.append({"$match": {f"{safe}__match_fallback": {"$ne": []}}})
                self._pipeline.append({"$project": {f"{safe}__match_fallback": 0}})

        # 3) Hydrate (select_related) using unfiltered docs_alias so no false "missing"
        if hydrate:
            # Use array (Mongo 4.2 safe)
            transformed_expr = self._build_value_expr(field_shape, f"${local_field}", f"${docs_alias}")
            self._pipeline.append({"$addFields": {local_field: transformed_expr}})

        # cleanup
        self._pipeline.append({"$project": {docs_alias: 0}})

    # ----------------- embedded list structured ref lookup -----------------

    def _add_embedded_list_structured_ref_lookup(
            self,
            target_cls,
            field_shape,
            list_path: str,
            embedded_key: str,
            foreign_match: Optional[dict] = None,
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
                        "in": {"$concatArrays": ["$$value", self._build_ref_ids_expr(field_shape, "$$this")]},
                    }
                },
                [],
            ]
        }

        base_pipeline = [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}]

        # 1) Single unfiltered lookup
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

        # 2) Local root filtering if foreign_match exists
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
                # optional fallback, same idea as above
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

        # 3) Hydrate embedded list items if requested
        if hydrate:
            # ensure docs_alias is treated as array
            docs_arr = {"$cond": [{"$isArray": f"${docs_alias}"}, f"${docs_alias}", []]}
            ids_arr = {"$map": {"input": docs_arr, "as": "d", "in": "$$d._id"}}

            # NOTE: build_value_expr expects docs_expr to be an array expression
            # so pass "$$docs" (from $let below) to avoid recomputing map for each item.
            per_item_value_expr = self._build_value_expr(field_shape, f"$$it.{embedded_key}", "$$docs")

            self._pipeline.append(
                {
                    "$addFields": {
                        list_path: {
                            "$cond": [
                                {"$isArray": f"${list_path}"},
                                {
                                    "$let": {
                                        "vars": {
                                            "docs": docs_arr,
                                            "ids": ids_arr,
                                        },
                                        "in": {
                                            "$map": {
                                                "input": f"${list_path}",
                                                "as": "it",
                                                "in": {
                                                    "$mergeObjects": [
                                                        "$$it",
                                                        {
                                                            embedded_key: {
                                                                # Inline hydration using the precomputed arrays:
                                                                "$cond": [
                                                                    {"$ifNull": [f"$$it.{embedded_key}", False]},
                                                                    {
                                                                        "$let": {
                                                                            "vars": {
                                                                                "refId": (
                                                                                    f"$$it.{embedded_key}.$id"
                                                                                    if getattr(field_shape, "dbref",
                                                                                               False)
                                                                                    else f"$$it.{embedded_key}"
                                                                                ),
                                                                                "idx": {
                                                                                    "$indexOfArray": [
                                                                                        "$$ids",
                                                                                        (
                                                                                            f"$$it.{embedded_key}.$id"
                                                                                            if getattr(field_shape,
                                                                                                       "dbref", False)
                                                                                            else f"$$it.{embedded_key}"
                                                                                        ),
                                                                                    ]
                                                                                },
                                                                            },
                                                                            "in": {
                                                                                "$cond": [
                                                                                    {"$gte": ["$$idx", 0]},
                                                                                    {"$arrayElemAt": ["$$docs",
                                                                                                      "$$idx"]},
                                                                                    {
                                                                                        "_missing_reference": True,
                                                                                        "_ref": "$$refId",
                                                                                        "_cls": getattr(
                                                                                            getattr(target_cls,
                                                                                                    "_class_name",
                                                                                                    None),
                                                                                            "__str__",
                                                                                            lambda: getattr(target_cls,
                                                                                                            "__name__",
                                                                                                            "Unknown")
                                                                                        )(),
                                                                                    },
                                                                                ]
                                                                            },
                                                                        }
                                                                    },
                                                                    None,
                                                                ]
                                                            }
                                                        },
                                                    ]
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

        self._pipeline.append({"$project": {docs_alias: 0}})

    # ----------------- MapField(ReferenceField) filter-only -----------------

    def _add_map_ref_lookup(self, target_cls, map_field, local_field: str, foreign_match: Optional[dict] = None):
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

        pipeline = [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}]
        if foreign_match:
            pipeline.append({"$match": foreign_match})

        self._pipeline.append(
            {"$lookup": {"from": target_cls._get_collection_name(), "let": {"refIds": ref_ids_expr},
                         "pipeline": pipeline, "as": docs_alias}}
        )

        if foreign_match:
            self._pipeline.append({"$match": {docs_alias: {"$ne": []}}})

        self._pipeline.append(self._project_remove(docs_alias))

    # ----------------- DictField(GenericReferenceField) filter-only -----------------

    def _add_object_generic_lookup(
            self,
            generic_field,
            local_field: str,
            foreign_match: Optional[dict] = None,
            hydrate: bool = False,
    ):
        """
        DictField(GenericReferenceField) support.

        - Always does lookups into each choice collection based on ids found in the dict values.
        - If foreign_match: filters root docs (keeps old behavior).
        - If hydrate: rewrites dict values from {"_cls","_ref"} into hydrated document dicts
          (same shape as scalar GenericReferenceField hydration: merged doc + _ref/_cls).
        """
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

        # root filtering when query had members__... predicates
        if foreign_match:
            self._pipeline.append({"$match": {"$or": [{a: {"$ne": []}} for a in aliases]}})

        if hydrate:
            # Transform each dict value using the same logic as scalar generic hydration
            value_expr = self._generic_value_transform_expr(
                doc_classes,
                alias_for_cls=alias_for,
                val_var="$$kv.v",
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
                                            "input": {"$objectToArray": f"${local_field}"},
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

    # ----------------- generic lookup helpers -----------------

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
                    "vars": {"matches": {"$filter": {"input": alias_arr, "as": "doc",
                                                     "cond": {"$eq": ["$$doc._id", f"{val_var}._ref.$id"]}}}},
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

    # ----------------- embedded list GenericReferenceField -----------------

    def _add_embedded_list_generic_lookup(
            self,
            generic_field,
            list_path: str,
            embedded_key: str,
            foreign_match: Optional[dict] = None,
            hydrate: bool = True,
    ):
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

        base = [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}]

        # ---------------- 1) Unfiltered lookups (needed for correct hydration) ----------------
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

        # ---------------- 2) Root filtering for foreign_match ----------------
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
                # fallback: filtered match lookups
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
                self._pipeline.append({"$match": {"$or": [{a: {"$ne": []}} for a in match_aliases]}})

                if not hydrate:
                    self._pipeline.append(self._project_remove(*(match_aliases + docs_aliases)))
                    return

        # ---------------- 3) Hydrate (fast, MongoDB 4.2-safe) ----------------
        if hydrate:
            # Make MongoDB-friendly $let var names (must start with lowercase)
            def vbase(cls):
                n = cls.__name__
                return n[:1].lower() + n[1:]  # Person -> person, Animal -> animal

            # Outer let: define <cls> Docs arrays
            docs_vars = {}
            for cls in doc_classes:
                vb = vbase(cls)
                docs_vars[f"{vb}Docs"] = {
                    "$cond": [
                        {"$isArray": f"${alias_docs(cls)}"},
                        f"${alias_docs(cls)}",
                        [],
                    ]
                }

            # Inner let: define <cls>Ids arrays from <cls>Docs
            ids_vars = {}
            for cls in doc_classes:
                vb = vbase(cls)
                ids_vars[f"{vb}Ids"] = {
                    "$map": {"input": f"$${vb}Docs", "as": "d", "in": "$$d._id"}
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
                                "rid": f"{val_expr}._ref.$id",
                                "idx": {"$indexOfArray": [ids_var, f"{val_expr}._ref.$id"]},
                            },
                            "in": {
                                "$cond": [
                                    {"$gte": ["$$idx", 0]},
                                    {
                                        "$mergeObjects": [
                                            {"$arrayElemAt": [docs_var, "$$idx"]},
                                            {"_ref": f"{val_expr}._ref", "_cls": f"{val_expr}._cls"},
                                        ]
                                    },
                                    {"_missing_reference": True, "_ref": "$$ref", "_cls": f"{val_expr}._cls"},
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
                                                                            {"$isArray": f"$$it.{embedded_key}"},
                                                                            {
                                                                                "$map": {
                                                                                    "input": f"$$it.{embedded_key}",
                                                                                    "as": "val",
                                                                                    "in": hydrate_one_value("$$val"),
                                                                                }
                                                                            },
                                                                            hydrate_one_value(f"$$it.{embedded_key}"),
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

        # ---------------- cleanup ----------------
        self._pipeline.append(self._project_remove(*(docs_aliases + match_aliases)))

    # ----------------- existing generic lookup -----------------

    def _add_generic_lookup(self, field, local_field, is_list=False):
        doc_classes = Schema.resolve_generic_choices(field)
        if not doc_classes:
            return

        def alias_for(cls):
            return f"{local_field}__{cls.__name__}"

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

        for cls in doc_classes:
            self._pipeline.append(
                {"$lookup": {"from": cls._get_collection_name(), "localField": f"{local_field}._ref.$id",
                             "foreignField": "_id", "as": alias_for(cls)}}
            )

        item_expr = self._generic_value_transform_expr(doc_classes, alias_for_cls=alias_for, val_var="$$item")
        self._pipeline.append(
            {"$addFields": {local_field: {"$map": {"input": f"${local_field}", "as": "item", "in": item_expr}}}})
        self._pipeline.append(self._project_remove(*[alias_for(cls) for cls in doc_classes]))

    # ----------------- abstract dbref lookup  -----------------

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
        """
        Hydrate ReferenceField pointing to an abstract base class.

        Ensures the hydrated dict is MongoEngine-friendly:
          - includes `_cls` (so it can resolve subclass)
          - converts `_id` -> `id` and removes `_id`
        """
        subclasses = self._concrete_subclasses(abstract_cls)
        if not subclasses:
            return

        safe_local = local_field.replace(".", "_")

        # resolve referenced id for DBRef or ObjectId
        ref_id_expr = {
            "$cond": [
                {"$eq": [{"$type": f"${local_field}"}, "object"]},  # DBRef
                f"${local_field}.$id",
                f"${local_field}",  # ObjectId
            ]
        }

        for cls in subclasses:
            try:
                coll = cls._get_collection_name()
            except Exception:
                coll = None
            if not coll:
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

            # overwrite local_field with hydrated doc ONLY if:
            # - we matched, and
            # - if DBRef: $ref matches this collection
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
                                            "$let": {
                                                "vars": {"doc": {"$first": "$$m"}},
                                                "in": {
                                                    "$cond": [
                                                        {"$eq": [{"$type": "$$v"}, "object"]},  # DBRef
                                                        {
                                                            "$cond": [
                                                                {"$eq": ["$$v.$ref", coll]},
                                                                {
                                                                    "$setField": {
                                                                        "field": "_id",
                                                                        "input": {
                                                                            "$mergeObjects": [
                                                                                "$$doc",
                                                                                {"id": "$$doc._id", "_cls": cls_name},
                                                                            ]
                                                                        },
                                                                        "value": "$$REMOVE",
                                                                    }
                                                                },
                                                                "$$v",
                                                            ]
                                                        },
                                                        # ObjectId storage: any match is valid
                                                        {
                                                            "$setField": {
                                                                "field": "_id",
                                                                "input": {
                                                                    "$mergeObjects": [
                                                                        "$$doc",
                                                                        {"id": "$$doc._id", "_cls": cls_name},
                                                                    ]
                                                                },
                                                                "value": "$$REMOVE",
                                                            }
                                                        },
                                                    ]
                                                },
                                            }
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
