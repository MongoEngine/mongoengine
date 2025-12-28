"""
MongoDB Aggregation Pipeline Builder for MongoEngine QuerySets.

This module provides the PipelineBuilder class that converts MongoEngine QuerySets
into MongoDB aggregation pipelines with automatic dereferencing support for various
field types including ReferenceFields, GenericReferenceFields, and nested structures.
"""

import re

__all__ = ("PipelineBuilder", "needs_aggregation")

from collections import defaultdict


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
        """
        Build a pipeline with progressive lookup + match pushdown.

        We split queryset._query into "buckets" keyed by the deref-prefix required.
        Example:
            - {"name": "x"} -> bucket ""
            - {"parent.age": {"$gt":50}} -> bucket "parent"
            - {"parent.company.n": 1} -> bucket "parent.company"

        Then:
            1) $match bucket "" (local only)
            2) do lookups in a tree order
            3) after each lookup stage, $match the bucket for that deref prefix
            4) finally apply $where/$function (cannot be bucketed safely)
            5) projection/sort/skip/limit
        """
        mongo_query = self.queryset._query or {}

        # No query: keep the original behavior
        if not mongo_query:
            if self.queryset._select_related:
                tree = self._build_related_tree(self.queryset._select_related)
                # original lookup walk (no interleaving needed)
                self._lookup_walk(self.document, "", tree)

            self._projection_stage()
            self._sort_stage()
            self._skip_stage()
            self._limit_stage()
            return self.pipeline

        # Convert regex and extract $where
        mongo_query = self._walk_and_convert_regex(mongo_query)
        cleaned, function_expr = self._convert_where_to_function(mongo_query)

        # Bucket queries by required lookup prefix
        buckets = self._bucket_query_by_lookup_prefix(self.document, cleaned)

        # Root/local match first
        root_match = buckets.pop("", None)
        if root_match:
            self.pipeline.append({"$match": root_match})

        # Build lookup tree from:
        #   - explicit select_related
        #   - implicit lookup needs from bucket prefixes
        tree = {}
        if self.queryset._select_related:
            tree = self._merge_lookup_trees(tree, self._build_related_tree(self.queryset._select_related))
        tree = self._merge_lookup_trees(tree, self._auto_lookup_tree_from_buckets(buckets))

        # Walk lookups with interleaved matches
        if tree:
            self._lookup_walk_interleaved(
                doc_cls=self.document,
                prefix="",
                tree=tree,
                buckets=buckets,
                embedded_list_path=None,
            )

        # Any leftover buckets (safety net)
        if buckets:
            leftovers = [q for q in buckets.values() if q]
            if leftovers:
                self.pipeline.append({"$match": leftovers[0] if len(leftovers) == 1 else {"$and": leftovers}})

        # $where/$function last
        if function_expr:
            self.pipeline.append({"$match": function_expr})

        # Tail stages
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

    def _merge_lookup_trees(self, a: dict, b: dict) -> dict:
        if not a:
            return dict(b or {})
        if not b:
            return dict(a)
        out = dict(a)
        for k, v in b.items():
            if k not in out:
                out[k] = v
            else:
                if isinstance(out[k], dict) and isinstance(v, dict):
                    out[k] = self._merge_lookup_trees(out[k], v)
        return out

    def _auto_lookup_tree_from_buckets(self, buckets: dict) -> dict:
        """
        Build a lookup tree using *python field names* from bucket prefixes that are
        in *db_field dotted form* (e.g. "info.target", "parent.company").

        This keeps _lookup_walk_interleaved() working even when db_field != attr_name.
        """

        def resolve_field_name(doc_cls, db_part: str):
            # direct attribute name match
            if db_part in doc_cls._fields:
                return db_part, doc_cls._fields[db_part]

            # db_field match
            for name, fld in doc_cls._fields.items():
                if getattr(fld, "db_field", None) == db_part:
                    return name, fld

            return None, None

        tree = {}

        for dotted_prefix in buckets.keys():
            if not dotted_prefix:
                continue

            parts = dotted_prefix.split(".")
            node = tree
            cur = self.document

            for db_part in parts:
                if cur is None:
                    break

                field_name, fld = resolve_field_name(cur, db_part)
                if not fld:
                    break

                node = node.setdefault(field_name, {})

                # advance cur when prefix continues through embedded/ref
                from mongoengine.fields import (
                    ListField, EmbeddedDocumentField, EmbeddedDocumentListField,
                    ReferenceField, GenericReferenceField
                )

                leaf = fld
                while isinstance(leaf, ListField):
                    leaf = leaf.field

                if isinstance(leaf, ReferenceField):
                    cur = getattr(leaf, "document_type_obj", None) or getattr(leaf, "document_type", None)
                    continue

                if isinstance(leaf, GenericReferenceField):
                    # can't safely descend into concrete class here; stop
                    cur = None
                    continue

                if isinstance(fld, (EmbeddedDocumentField, EmbeddedDocumentListField)) or getattr(leaf, "document_type",
                                                                                                  None):
                    cur = getattr(leaf, "document_type", None) or getattr(leaf, "document_type_obj", None)
                    continue

                cur = None

            # mark leaf
            node[""] = True

        return tree

    # ======================================================================
    # QUERY BUCKETING (match pushdown)
    # ======================================================================
    def _bucket_query_by_lookup_prefix(self, doc_cls, query: dict) -> dict:
        """
        Bucket by required deref prefix, but store keys in *dotted* form so Mongo can match.
        """
        buckets = {}

        def merge(prefix: str, frag: dict):
            if not frag:
                return
            if prefix not in buckets:
                buckets[prefix] = frag
            else:
                existing = buckets[prefix]
                if existing == frag:
                    return
                buckets[prefix] = {"$and": [existing, frag]}

        def walk(q):
            if not isinstance(q, dict):
                merge("", q)
                return

            # logical ops: bucket each clause and reassemble per-prefix
            for op in ("$and", "$or", "$nor"):
                if op in q:
                    clauses = q.get(op) or []
                    per_prefix = defaultdict(list)
                    for clause in clauses:
                        sub = self._bucket_query_by_lookup_prefix(doc_cls, clause)
                        for pfx, frag in sub.items():
                            per_prefix[pfx].append(frag)
                    for pfx, frags in per_prefix.items():
                        merge(pfx, frags[0] if len(frags) == 1 else {op: frags})

            for k, v in q.items():
                if isinstance(k, str) and k.startswith("$"):
                    if k not in ("$and", "$or", "$nor"):
                        merge("", {k: v})
                    continue

                # normalize to dotted form for pipeline matching
                fk = k.replace("__", ".") if ("__" in k and "." not in k) else k
                prefix = self._required_lookup_prefix_for_field(doc_cls, fk)

                # IMPORTANT: store fk (not k)
                merge(prefix, {fk: v})

        walk(query)
        return buckets

    def _required_lookup_prefix_for_field(self, doc_cls, field_key: str) -> str:
        """
        Return the *deepest* deref prefix required for a dotted path.

        Examples:
          "name" -> ""
          "parent.age" -> "parent"
          "parent.company.name" -> "parent.company"
          "info.target.age" (embedded generic ref) -> "info.target"
        """
        from mongoengine.fields import (
            ListField, ReferenceField, GenericReferenceField,
            EmbeddedDocumentField, EmbeddedDocumentListField
        )

        parts = field_key.split(".")
        cur = doc_cls
        db_path = []
        last_deref_prefix = ""

        for i, part in enumerate(parts):
            if cur is None:
                break

            fld = cur._fields.get(part)
            if fld is None:
                for name, f in cur._fields.items():
                    if getattr(f, "db_field", None) == part:
                        fld = f
                        break
            if fld is None:
                break

            db_part = getattr(fld, "db_field", part)
            db_path.append(db_part)

            leaf = fld
            while isinstance(leaf, ListField):
                leaf = leaf.field

            is_terminal = (i == len(parts) - 1)

            # ReferenceField: only deref if there are more path parts
            if isinstance(leaf, ReferenceField):
                if not is_terminal:
                    last_deref_prefix = ".".join(db_path)
                    cur = getattr(leaf, "document_type_obj", None) or getattr(leaf, "document_type", None)
                    continue
                # terminal ref equality => no lookup required
                return last_deref_prefix

            # GenericReferenceField: only deref if there are more path parts
            if isinstance(leaf, GenericReferenceField):
                if not is_terminal:
                    last_deref_prefix = ".".join(db_path)
                    return last_deref_prefix
                # terminal generic equality => no lookup required
                return last_deref_prefix

            # Embedded docs: keep walking
            if isinstance(fld, (EmbeddedDocumentField, EmbeddedDocumentListField)) or getattr(leaf, "document_type",
                                                                                              None):
                cur = getattr(leaf, "document_type", None) or getattr(leaf, "document_type_obj", None)
                continue

            cur = None

        return last_deref_prefix

    # ======================================================================
    # INTERLEAVED LOOKUP WALK (lookup -> match -> lookup -> match)
    # ======================================================================
    def _lookup_walk_interleaved(self, doc_cls, prefix, tree, buckets, embedded_list_path=None):
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

                # apply bucket for this deref prefix now
                bucket = buckets.pop(full_path, None)
                if bucket:
                    self.pipeline.append({"$match": bucket})

                if subtree:
                    self._lookup_walk_interleaved(
                        target,
                        prefix=f"{full_path}.",
                        tree=subtree,
                        buckets=buckets,
                        embedded_list_path=embedded_list_path,
                    )
                continue

            # ==================== LIST (possibly nested) =====================
            if isinstance(field, ListField):
                leaf, _depth = self._unwrap_list_field(field)

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

                    bucket = buckets.pop(full_path, None)
                    if bucket:
                        self.pipeline.append({"$match": bucket})

                    if subtree:
                        self._lookup_walk_interleaved(
                            target,
                            prefix=f"{full_path}.",
                            tree=subtree,
                            buckets=buckets,
                            embedded_list_path=embedded_list_path,
                        )
                    continue

                if leaf is not None and isinstance(leaf, GenericReferenceField):
                    if leaf.choices:
                        if embedded_list_path:
                            # GenericRef (or list-of-genericref) inside EmbeddedDocumentListField(...)
                            self._add_embedded_list_generic_lookup(
                                generic_field=leaf,
                                list_path=embedded_list_path,
                                embedded_key=field.db_field,
                            )
                        else:
                            self._add_generic_lookup(leaf, full_path, is_list=True)

                    bucket = buckets.pop(full_path, None)
                    if bucket:
                        self.pipeline.append({"$match": bucket})
                    continue

            # ==================== MapField(ReferenceField) =====================
            if isinstance(field, MapField) and isinstance(field.field, ReferenceField):
                if not embedded_list_path:
                    self._add_map_ref_lookup(
                        target_cls=field.field.document_type,
                        map_field=field,
                        local_field=full_path,
                    )

                bucket = buckets.pop(full_path, None)
                if bucket:
                    self.pipeline.append({"$match": bucket})
                continue

            # ==================== MapField(GenericReferenceField) =====================
            if isinstance(field, MapField) and isinstance(field.field, GenericReferenceField) and getattr(field.field,
                                                                                                          "choices",
                                                                                                          None):
                if not embedded_list_path:
                    self._add_map_generic_lookup(field.field, full_path)

                bucket = buckets.pop(full_path, None)
                if bucket:
                    self.pipeline.append({"$match": bucket})
                continue

            # ==================== DictField(GenericReferenceField) =====================
            if isinstance(field, DictField) and isinstance(field.field, GenericReferenceField) and getattr(field.field,
                                                                                                           "choices",
                                                                                                           None):
                if not embedded_list_path:
                    self._add_dict_generic_lookup(field.field, full_path)

                bucket = buckets.pop(full_path, None)
                if bucket:
                    self.pipeline.append({"$match": bucket})
                continue

            # ==================== LIST of EmbeddedDocument (descend) =====================
            if self._is_list_of_embedded(field):
                embedded_doc = self._embedded_doc_type(field)
                if subtree and embedded_doc:
                    self._lookup_walk_interleaved(
                        embedded_doc,
                        prefix=f"{full_path}.",
                        tree=subtree,
                        buckets=buckets,
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

                    bucket = buckets.pop(full_path, None)
                    if bucket:
                        self.pipeline.append({"$match": bucket})

                    if subtree and not embedded_list_path:
                        self._lookup_walk_interleaved(
                            target,
                            prefix=f"{full_path}.",
                            tree=subtree,
                            buckets=buckets,
                            embedded_list_path=embedded_list_path,
                        )
                continue

            # ==================== Generic Reference (scalar) ===========================
            if isinstance(field, GenericReferenceField) and field.choices:
                if embedded_list_path:
                    # GenericReferenceField inside EmbeddedDocumentListField(...)
                    self._add_embedded_list_generic_lookup(
                        generic_field=field,
                        list_path=embedded_list_path,
                        embedded_key=field.db_field,
                    )
                else:
                    self._add_generic_lookup(field, full_path)

                bucket = buckets.pop(full_path, None)
                if bucket:
                    self.pipeline.append({"$match": bucket})
                continue

            if isinstance(field, EmbeddedDocumentField):
                if subtree:
                    self._lookup_walk_interleaved(
                        field.document_type,
                        f"{full_path}.",
                        subtree,
                        buckets,
                        embedded_list_path=embedded_list_path,
                    )
                continue

            if isinstance(field, FileField):
                continue

    # ======================================================================
    # Embedded doc helpers (robust across mongoengine versions)
    # ======================================================================
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
        dt = getattr(inner, "document_type", None) if inner else None
        if dt:
            return dt
        return None

    # ======================================================================
    # ListField helpers (supports nested ListField(ListField(...)))
    # ======================================================================
    @staticmethod
    def _unwrap_list_field(fld):
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
            field_shape,  # ReferenceField OR ListField(ReferenceField) OR nested lists
            list_path,  # e.g. "items" (array of embedded docs)
            embedded_key,  # e.g. "parents" or "parent"
    ):
        """
        Supports embedded list refs where embedded_key can be:
          - ReferenceField
          - ListField(ReferenceField) (including nested lists)
        by flattening refIds correctly for $lookup.
        """
        if not target_cls:
            return

        safe_list = list_path.replace(".", "_")
        docs_alias = f"{safe_list}_{embedded_key}__docs"

        # This is the array you get from a dotted projection on an array-of-objects:
        # - for scalar ref: [ObjectId, ObjectId, ...]
        # - for list ref:   [[ObjectId,...], [ObjectId,...], ...]
        raw_values_expr = {"$ifNull": [f"${list_path}.{embedded_key}", []]}

        # Flatten and normalize into a single array of ObjectIds, regardless of shape.
        # We delegate actual "extract ids" logic to _build_ref_ids_expr (handles dbref, lists, etc).
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
        per_item_value_expr = self._build_value_expr(field_shape, f"$$it.{embedded_key}", docs_expr)

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

        cls_name = getattr(target_cls, "_class_name", target_cls.__name__)

        self.pipeline.append(
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
            }
        )

        self.pipeline.append({"$project": {docs_alias: 0}})

    def _add_map_generic_lookup(self, generic_field, local_field):
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

            self.pipeline.append(
                {
                    "$lookup": {
                        "from": cls._get_collection_name(),
                        "let": {"refIds": ref_ids_expr},
                        "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                        "as": alias,
                    }
                }
            )

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

        self.pipeline.append(
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
                                            "v": {"$let": {"vars": {"val": "$$kv.v"}, "in": value_transform_expr()}},
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

        for cls in doc_classes:
            self.pipeline.append({"$project": {f"{safe}__{cls.__name__}": 0}})

    def _add_dict_generic_lookup(self, generic_field, local_field):
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

            self.pipeline.append(
                {
                    "$lookup": {
                        "from": cls._get_collection_name(),
                        "let": {"refIds": ref_ids_expr},
                        "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                        "as": alias,
                    }
                }
            )

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

        self.pipeline.append(
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
                                            "v": {"$let": {"vars": {"val": "$$kv.v"}, "in": value_transform_expr()}},
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

        for cls in doc_classes:
            self.pipeline.append({"$project": {f"{safe}__{cls.__name__}": 0}})

    def _add_embedded_list_generic_lookup(self, generic_field, list_path, embedded_key):
        """
        EmbeddedDocumentListField(Item) where Item.<embedded_key> is either:
          - GenericReferenceField
          - ListField(GenericReferenceField)

        This:
          - does per-choice lookups
          - rewrites each embedded item to replace <embedded_key> with the joined doc(s)
          - keeps missing refs as {_missing_reference: True, _ref:..., _cls:...}
        """
        from mongoengine.document import _DocumentRegistry

        doc_classes = []
        for ch in getattr(generic_field, "choices", None) or ():
            if isinstance(ch, str):
                cls = _DocumentRegistry.get(ch)
            elif isinstance(ch, type):
                cls = _DocumentRegistry.get(ch.__name__)
            else:
                cls = None
            if cls:
                doc_classes.append(cls)
        if not doc_classes:
            return

        safe_list = list_path.replace(".", "_")
        aliases = []

        # dotted projection on array-of-objects:
        # - scalar generic:  [ {_ref,_cls}, {_ref,_cls}, ... ]
        # - list generic:    [ [ {_ref,_cls}, ... ], [ {_ref,_cls}, ... ], ... ]
        raw_values_expr = {"$ifNull": [f"${list_path}.{embedded_key}", []]}

        # 1) lookups per concrete class (collect ids across ALL embedded docs, flattening lists)
        for cls in doc_classes:
            alias = f"{safe_list}_{embedded_key}__{cls.__name__}"
            aliases.append(alias)

            # IMPORTANT: $$m is only valid inside $filter/$map scopes.
            class_test_m = {"$regexMatch": {"input": "$$m._cls", "regex": f"^{cls._class_name}(\\.|$)"}}
            class_test_this = {"$regexMatch": {"input": "$$this._cls", "regex": f"^{cls._class_name}(\\.|$)"}}

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
                                        # $$this is either a dict (scalar generic) or an array (list generic)
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

            self.pipeline.append(
                {
                    "$lookup": {
                        "from": cls._get_collection_name(),
                        "let": {"refIds": ref_ids_expr},
                        "pipeline": [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}],
                        "as": alias,
                    }
                }
            )

        # 2) transform one generic value in $$val (scalar)
        def value_transform_expr():
            expr = "$$val"
            for cls in reversed(doc_classes):
                alias_arr = f"${safe_list}_{embedded_key}__{cls.__name__}"
                class_test_val = {"$regexMatch": {"input": "$$val._cls", "regex": f"^{cls._class_name}(\\.|$)"}}

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
                                {
                                    "$mergeObjects": [
                                        {"$first": "$$matches"},
                                        {"_ref": "$$val._ref", "_cls": "$$val._cls"},
                                    ]
                                },
                                {"_missing_reference": True, "_ref": "$$val._ref", "_cls": "$$val._cls"},
                            ]
                        },
                    }
                }

                expr = {"$cond": [class_test_val, branch, expr]}
            return expr

        # 3) rewrite embedded list items; handle scalar OR list at embedded_key
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
                                            {
                                                embedded_key: {
                                                    "$cond": [
                                                        {"$isArray": f"$$it.{embedded_key}"},
                                                        {
                                                            "$map": {
                                                                "input": f"$$it.{embedded_key}",
                                                                "as": "val",
                                                                "in": {
                                                                    "$let": {
                                                                        "vars": {"val": "$$val"},
                                                                        "in": value_transform_expr(),
                                                                    }
                                                                },
                                                            }
                                                        },
                                                        {
                                                            "$let": {
                                                                "vars": {"val": f"$$it.{embedded_key}"},
                                                                "in": value_transform_expr(),
                                                            }
                                                        },
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

        # 4) cleanup temp arrays
        for alias in aliases:
            self.pipeline.append({"$project": {alias: 0}})

    # ======================================================================
    # LOOKUP WALKER (your existing one, kept as-is for select_related paths)
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
                    self._lookup_walk(target, prefix=f"{full_path}.", tree=subtree,
                                      embedded_list_path=embedded_list_path)
                continue

            if isinstance(field, ListField):
                leaf, _depth = self._unwrap_list_field(field)

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
                        self._lookup_walk(target, prefix=f"{full_path}.", tree=subtree,
                                          embedded_list_path=embedded_list_path)
                    continue

                if leaf is not None and isinstance(leaf, GenericReferenceField):
                    if leaf.choices:
                        if embedded_list_path:
                            self._add_embedded_list_generic_lookup(
                                generic_field=leaf,
                                list_path=embedded_list_path,
                                embedded_key=field.db_field,
                            )
                        else:
                            self._add_generic_lookup(leaf, full_path, is_list=True)
                    continue

            if isinstance(field, MapField) and isinstance(field.field, ReferenceField):
                if not embedded_list_path:
                    self._add_map_ref_lookup(target_cls=field.field.document_type, map_field=field,
                                             local_field=full_path)
                continue

            if isinstance(field, MapField) and isinstance(field.field, GenericReferenceField) and getattr(field.field,
                                                                                                          "choices",
                                                                                                          None):
                if not embedded_list_path:
                    self._add_map_generic_lookup(field.field, full_path)
                continue

            if isinstance(field, DictField) and isinstance(field.field, GenericReferenceField) and getattr(field.field,
                                                                                                           "choices",
                                                                                                           None):
                if not embedded_list_path:
                    self._add_dict_generic_lookup(field.field, full_path)
                continue

            if self._is_list_of_embedded(field):
                embedded_doc = self._embedded_doc_type(field)
                if subtree and embedded_doc:
                    self._lookup_walk(embedded_doc, prefix=f"{full_path}.", tree=subtree, embedded_list_path=full_path)
                continue

            if isinstance(field, DictField):
                refs = self._collect_ref_document_types(field.field)
                if len(refs) == 1:
                    target = list(refs)[0]
                    if not embedded_list_path:
                        self._add_dictfield_lookup(target, field, full_path)

                    if subtree and not embedded_list_path:
                        self._lookup_walk(target, prefix=f"{full_path}.", tree=subtree,
                                          embedded_list_path=embedded_list_path)
                continue

            if isinstance(field, GenericReferenceField) and field.choices:
                if not embedded_list_path:
                    self._add_generic_lookup(field, full_path)
                continue

            if isinstance(field, EmbeddedDocumentField):
                if subtree:
                    self._lookup_walk(field.document_type, f"{full_path}.", subtree,
                                      embedded_list_path=embedded_list_path)
                continue

            if isinstance(field, FileField):
                continue

    # ======================================================================
    # HELPERS: collect leaf ReferenceField document types under a field
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
                            "in": {"$concatArrays": ["$$value", self._build_ref_ids_expr(field.field, "$$this")]},
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
                    "in": {"$concatArrays": ["$$value", self._build_ref_ids_expr(field.field, "$$this.v")]},
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
                    {"$map": {"input": source_expr, "as": "item",
                              "in": self._build_value_expr(field.field, "$$item", docs_expr)}},
                    source_expr,
                ]
            }

        if isinstance(field, DictField):
            return {
                "$arrayToObject": {
                    "$map": {
                        "input": {"$objectToArray": source_expr},
                        "as": "kv",
                        "in": {"k": "$$kv.k", "v": self._build_value_expr(field.field, "$$kv.v", docs_expr)},
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

            self.pipeline.append(
                {
                    "$lookup": {
                        "from": coll,
                        "localField": f"{local_field}.$id",
                        "foreignField": "_id",
                        "as": temp,
                    }
                }
            )

            self.pipeline.append(
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

                self.pipeline.append(
                    {
                        "$lookup": {
                            "from": cls._get_collection_name(),
                            "localField": f"{local_field}._ref.$id",
                            "foreignField": "_id",
                            "as": temp,
                        }
                    }
                )

                class_test = {"$regexMatch": {"input": f"${local_field}._cls", "regex": f"^{cls._class_name}(\\.|$)"}}

                self.pipeline.append(
                    {
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
                                                    {"_missing_reference": True, "_ref": "$$refVal",
                                                     "_cls": "$$clsVal"},
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

                self.pipeline.append({"$project": {temp: 0}})
            return

        # LIST GENERIC (flat list only)
        for cls in doc_classes:
            self.pipeline.append(
                {
                    "$lookup": {
                        "from": cls._get_collection_name(),
                        "localField": f"{local_field}._ref.$id",
                        "foreignField": "_id",
                        "as": f"{local_field}__{cls.__name__}",
                    }
                }
            )

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
                            },
                        }
                    },
                    "$$item",
                ]
            }

        def build_item_expr():
            expr = "$$item"
            for cls in reversed(doc_classes):
                expr = {"$cond": [{"$regexMatch": {"input": "$$item._cls", "regex": f"^{cls._class_name}(\\.|$)"}},
                                  item_expr_for(cls), expr]}
            return expr

        self.pipeline.append(
            {"$addFields": {local_field: {"$map": {"input": f"${local_field}", "as": "item", "in": build_item_expr()}}}}
        )

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
