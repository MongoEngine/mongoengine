"""
MongoDB Aggregation Pipeline Builder for MongoEngine QuerySets.

This module provides the PipelineBuilder class that converts MongoEngine QuerySets
into MongoDB aggregation pipelines with automatic dereferencing support for various
field types, including ReferenceFields, GenericReferenceFields, and nested structures.
"""

__all__ = ("PipelineBuilder", "needs_aggregation")

import re
from collections import defaultdict


class PipelineBuilder:
    def __init__(self, queryset):
        self.queryset = queryset
        self.document = queryset._document
        self.pipeline = []

    # PUBLIC API
    def build(self):
        mongo_query = self.queryset._query or {}

        # No query: original behavior
        if not mongo_query:
            if self.queryset._select_related:
                tree = PipelineBuilder._build_related_tree(self.queryset._select_related)
                self._walk_lookups(self.document, "", tree, buckets=None, embedded_list_path=None, interleave=False)

            self._tail_stages()
            return self.pipeline

        # Convert regex and extract $where
        mongo_query = self._walk_and_convert_regex(mongo_query)
        cleaned, function_expr = PipelineBuilder._convert_where_to_function(mongo_query)

        # Bucket queries by required lookup prefix
        buckets = PipelineBuilder._bucket_query_by_lookup_prefix(self.document, cleaned)

        # Root/local match first
        root_match = buckets.pop("", None)
        if root_match:
            self.pipeline.append({"$match": root_match})

        # Build lookup tree from:
        #   - explicit select_related
        #   - implicit lookup needs from bucket prefixes
        tree = {}
        if self.queryset._select_related:
            tree = PipelineBuilder._merge_lookup_trees(tree, PipelineBuilder._build_related_tree(
                self.queryset._select_related))
        tree = PipelineBuilder._merge_lookup_trees(tree, self._auto_lookup_tree_from_buckets(buckets))

        # Walk lookups interleaved with bucket matches
        if tree:
            self._walk_lookups(self.document, "", tree, buckets=buckets, embedded_list_path=None, interleave=True)

        # Safety net: leftover buckets
        if buckets:
            leftovers = [q for q in buckets.values() if q]
            if leftovers:
                self.pipeline.append({"$match": leftovers[0] if len(leftovers) == 1 else {"$and": leftovers}})

        # $where/$function last
        if function_expr:
            self.pipeline.append({"$match": function_expr})

        self._tail_stages()
        return self.pipeline

    def _tail_stages(self):
        self._projection_stage()
        self._sort_stage()
        self._skip_stage()
        self._limit_stage()

    # WHERE -> $function
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

    # LOOKUP TREE
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

    @staticmethod
    def _merge_lookup_trees(a: dict, b: dict) -> dict:
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
                    out[k] = PipelineBuilder._merge_lookup_trees(out[k], v)
        return out

    def _auto_lookup_tree_from_buckets(self, buckets: dict) -> dict:
        """
        Build lookup tree using python field names from bucket prefixes (bucket keys are db_field dotted paths).
        Includes safe traversal through GenericReferenceField when the next segment is a common ReferenceField.
        """
        tree = {}
        for dotted_prefix in list(buckets.keys()):
            if not dotted_prefix:
                continue

            parts = dotted_prefix.split(".")
            node = tree
            cur = self.document

            for idx, db_part in enumerate(parts):
                if cur is None:
                    break

                field_name, fld = PipelineBuilder._resolve_field_name(cur, db_part)
                if not fld:
                    break

                node = node.setdefault(field_name, {})

                from mongoengine.fields import ReferenceField, GenericReferenceField, EmbeddedDocumentField, \
                    EmbeddedDocumentListField
                leaf = PipelineBuilder._unwrap_list_leaf(fld)

                if isinstance(leaf, ReferenceField):
                    cur = getattr(leaf, "document_type_obj", None) or getattr(leaf, "document_type", None)
                    continue

                if isinstance(leaf, GenericReferenceField):
                    if idx < len(parts) - 1:
                        next_part = parts[idx + 1]
                        common_ref_field, _common_target = PipelineBuilder._generic_common_ref(leaf, next_part)
                        if common_ref_field is None:
                            cur = None
                            break

                        # advance using representative concrete class to keep resolving names
                        from mongoengine.document import _DocumentRegistry
                        ch0 = (leaf.choices or ())[0]
                        cur = _DocumentRegistry.get(ch0 if isinstance(ch0, str) else ch0.__name__)
                        continue

                    cur = None
                    break

                if isinstance(fld, (EmbeddedDocumentField, EmbeddedDocumentListField)) or getattr(leaf, "document_type",
                                                                                                  None):
                    cur = getattr(leaf, "document_type", None) or getattr(leaf, "document_type_obj", None)
                    continue

                cur = None

            node[""] = True
        return tree

    # FIELD RESOLUTION HELPERS
    @staticmethod
    def _resolve_field_name(doc_cls, db_part: str):
        """Return (python_field_name, field_obj) by attr-name or db_field match."""
        if db_part in doc_cls._fields:
            return db_part, doc_cls._fields[db_part]
        for name, fld in doc_cls._fields.items():
            if getattr(fld, "db_field", None) == db_part:
                return name, fld
        return None, None

    @staticmethod
    def _unwrap_list_leaf(field):
        """If field is ListField(...ListField(x)...), return the deepest leaf."""
        from mongoengine.fields import ListField
        leaf = field
        while isinstance(leaf, ListField):
            leaf = leaf.field
        return leaf

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

    # GENERIC traversal helper
    @staticmethod
    def _generic_common_ref(generic_field, next_part: str):
        """
        If all GenericReferenceField choices share next_part as ReferenceField to same doc type -> return it.
        """
        from mongoengine.fields import ReferenceField, ListField

        doc_classes = PipelineBuilder._resolve_generic_choices(generic_field)
        if not doc_classes:
            return None, None

        targets = []
        fld = None
        for cls in doc_classes:
            fld = cls._fields.get(next_part)
            if fld is None:
                for _n, f in cls._fields.items():
                    if getattr(f, "db_field", None) == next_part:
                        fld = f
                        break
            if fld is None:
                return None, None

            leaf = fld
            while isinstance(leaf, ListField):
                leaf = leaf.field
            if not isinstance(leaf, ReferenceField):
                return None, None

            targets.append(getattr(leaf, "document_type_obj", None) or getattr(leaf, "document_type", None))

        if any(t is None for t in targets):
            return None, None
        if len({id(t) for t in targets}) != 1:
            return None, None

        return doc_classes[0]._fields.get(next_part) or fld, targets[0]  # any representative field + common target

    # QUERY BUCKETING (your working version; kept as-is except tiny helpers)
    @staticmethod
    def _bucket_query_by_lookup_prefix(doc_cls, query: dict) -> dict:
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

        def dotted(k: str) -> str:
            return k.replace("__", ".") if ("__" in k and "." not in k) else k

        def get_field_by_db_part(cur, part):
            fld = cur._fields.get(part)
            if fld:
                return fld
            for _name, f in cur._fields.items():
                if getattr(f, "db_field", None) == part:
                    return f
            return None

        def path_expr(base_var: str, parts: list[str]) -> str:
            return base_var + "".join(f".{p}" for p in parts)

        def expr_for_any_map_value(field_db: str, rest: list[str], cond: dict):
            if len(cond) != 1:
                return None
            op, value = next(iter(cond.items()))
            value_expr = path_expr("$$kv.v", rest)
            return {
                "$expr": {
                    "$anyElementTrue": {
                        "$map": {
                            "input": {"$objectToArray": f"${field_db}"},
                            "as": "kv",
                            "in": {op: [value_expr, value]},
                        }
                    }
                }
            }

        def expr_for_nested_list(field_db: str, rest: list[str], cond: dict):
            if len(cond) != 1:
                return None
            op, value = next(iter(cond.items()))
            value_expr = path_expr("$$it", rest)
            return {
                "$expr": {
                    "$anyElementTrue": {
                        "$map": {
                            "input": {
                                "$reduce": {
                                    "input": f"${field_db}",
                                    "initialValue": [],
                                    "in": {"$concatArrays": ["$$value", "$$this"]},
                                }
                            },
                            "as": "it",
                            "in": {op: [value_expr, value]},
                        }
                    }
                }
            }

        def walk(q, cur_doc=doc_cls):
            if not isinstance(q, dict):
                merge("", q)
                return

            for op in ("$and", "$or", "$nor"):
                if op in q:
                    clauses = q.get(op) or []
                    per_prefix = defaultdict(list)
                    for clause in clauses:
                        sub = PipelineBuilder._bucket_query_by_lookup_prefix(cur_doc, clause)
                        for pfx, frag in sub.items():
                            per_prefix[pfx].append(frag)
                    for pfx, frags in per_prefix.items():
                        merge(pfx, frags[0] if len(frags) == 1 else {op: frags})

            for k, v in q.items():
                if isinstance(k, str) and k.startswith("$"):
                    if k not in ("$and", "$or", "$nor"):
                        merge("", {k: v})
                    continue

                fk = dotted(k)
                parts = fk.split(".")
                if not parts:
                    continue

                first = parts[0]
                fld0 = get_field_by_db_part(cur_doc, first)

                if fld0 is not None and len(parts) >= 2:
                    from mongoengine.fields import ListField, MapField, DictField, ReferenceField, GenericReferenceField

                    field_db = getattr(fld0, "db_field", first)
                    rest = parts[1:]

                    if isinstance(fld0, MapField) and isinstance(getattr(fld0, "field", None), ReferenceField):
                        prefix = field_db
                        rewritten = expr_for_any_map_value(field_db, rest, v if isinstance(v, dict) else {"$eq": v})
                        if rewritten:
                            merge(prefix, rewritten)
                            continue

                    if isinstance(fld0, DictField) and isinstance(getattr(fld0, "field", None), GenericReferenceField):
                        prefix = field_db
                        rewritten = expr_for_any_map_value(field_db, rest, v if isinstance(v, dict) else {"$eq": v})
                        if rewritten:
                            merge(prefix, rewritten)
                            continue

                    if isinstance(fld0, ListField):
                        leaf = fld0
                        depth = 0
                        while isinstance(leaf, ListField):
                            depth += 1
                            leaf = leaf.field
                        if depth >= 2 and isinstance(leaf, ReferenceField):
                            prefix = field_db
                            rewritten = expr_for_nested_list(field_db, rest, v if isinstance(v, dict) else {"$eq": v})
                            if rewritten:
                                merge(prefix, rewritten)
                                continue

                prefix = PipelineBuilder._required_lookup_prefix_for_field(cur_doc, fk)
                merge(prefix, {fk: v})

        walk(query)
        return buckets

    @staticmethod
    def _required_lookup_prefix_for_field(doc_cls, field_key: str) -> str:
        from mongoengine.fields import (
            ListField, ReferenceField, GenericReferenceField,
            EmbeddedDocumentField, EmbeddedDocumentListField,
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
                for _name, f in cur._fields.items():
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

            if isinstance(leaf, ReferenceField):
                if not is_terminal:
                    last_deref_prefix = ".".join(db_path)
                    cur = getattr(leaf, "document_type_obj", None) or getattr(leaf, "document_type", None)
                    continue
                return last_deref_prefix

            if isinstance(leaf, GenericReferenceField):
                if not is_terminal:
                    next_part = parts[i + 1]
                    common_ref_field, _common_target = PipelineBuilder._generic_common_ref(leaf, next_part)
                    if common_ref_field is not None:
                        last_deref_prefix = ".".join(db_path)
                        from mongoengine.document import _DocumentRegistry
                        ch0 = (leaf.choices or ())[0]
                        cur = _DocumentRegistry.get(ch0 if isinstance(ch0, str) else ch0.__name__)
                        continue

                    last_deref_prefix = ".".join(db_path)
                    return last_deref_prefix
                return last_deref_prefix

            if isinstance(fld, (EmbeddedDocumentField, EmbeddedDocumentListField)) or getattr(leaf, "document_type",
                                                                                              None):
                cur = getattr(leaf, "document_type", None) or getattr(leaf, "document_type_obj", None)
                continue

            cur = None

        return last_deref_prefix

    # LOOKUP WALKER (single implementation)
    def _walk_lookups(self, doc_cls, prefix, tree, buckets, embedded_list_path=None, interleave=False):
        """
        If interleave=True: after each deref stage, apply bucket match for that full_path.
        If interleave=False: just do lookups (used for select_related without query pushdown).
        """
        from mongoengine.fields import (
            ReferenceField, GenericReferenceField,
            ListField, DictField, MapField, EmbeddedDocumentField, FileField,
        )

        def apply_bucket(full_path):
            if not interleave or buckets is None:
                return
            bucket = buckets.pop(full_path, None)
            if bucket:
                self.pipeline.append({"$match": bucket})

        for field_name, subtree in tree.items():
            if field_name == "":
                continue

            field = doc_cls._fields.get(field_name)
            if not field:
                continue

            full_path = f"{prefix}{field.db_field}" if prefix else field.db_field

            # --------------------- ReferenceField ---------------------
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

            # --------------------- ListField(...) ---------------------
            if isinstance(field, ListField):
                leaf, _depth = PipelineBuilder._unwrap_list_field(field)

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

            # --------------------- MapField(ReferenceField) ---------------------
            if isinstance(field, MapField) and isinstance(field.field, ReferenceField):
                if not embedded_list_path:
                    self._add_map_ref_lookup(field.field.document_type, field, full_path)
                apply_bucket(full_path)
                continue

            # --------------------- MapField(GenericReferenceField) ---------------------
            if isinstance(field, MapField) and isinstance(field.field, GenericReferenceField) and getattr(field.field,
                                                                                                          "choices",
                                                                                                          None):
                if not embedded_list_path:
                    self._add_object_generic_lookup(field.field, full_path)
                apply_bucket(full_path)
                continue

            # --------------------- DictField(GenericReferenceField) ---------------------
            if isinstance(field, DictField) and isinstance(field.field, GenericReferenceField) and getattr(field.field,
                                                                                                           "choices",
                                                                                                           None):
                if not embedded_list_path:
                    self._add_object_generic_lookup(field.field, full_path)
                apply_bucket(full_path)
                continue

            # --------------------- Embedded doc list (descend) ---------------------
            if PipelineBuilder._is_list_of_embedded(field):
                embedded_doc = PipelineBuilder._embedded_doc_type(field)
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

            # --------------------- DictField(Reference-only) ---------------------
            if isinstance(field, DictField):
                refs = PipelineBuilder._collect_ref_document_types(field.field)
                if len(refs) == 1:
                    target = list(refs)[0]
                    if not embedded_list_path:
                        self._add_structured_ref_lookup(target, field, full_path)

                    apply_bucket(full_path)

                    if subtree and not embedded_list_path:
                        self._walk_lookups(target, f"{full_path}.", subtree, buckets, embedded_list_path, interleave)
                continue

            # --------------------- GenericReferenceField scalar ---------------------
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
                        common_ref_field, common_target = PipelineBuilder._generic_common_ref(field, sub_name)
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

    # Embedded doc helpers
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
        return dt

    # HELPERS: collect leaf ReferenceField document types under a field
    @staticmethod
    def _collect_ref_document_types(field):
        from mongoengine.fields import ReferenceField, ListField, DictField, GenericReferenceField

        doc_types = set()
        if isinstance(field, ReferenceField):
            if field.document_type is not None:
                doc_types.add(field.document_type)
            return doc_types
        if isinstance(field, ListField):
            return PipelineBuilder._collect_ref_document_types(field.field)
        if isinstance(field, DictField):
            return PipelineBuilder._collect_ref_document_types(field.field) if field.field is not None else set()
        if isinstance(field, GenericReferenceField):
            return set()
        return set()

    # LOOKUP IMPLEMENTATIONS (your working code kept, but generic object lookup unified)
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
                        "in": {
                            "$concatArrays": [
                                "$$value",
                                PipelineBuilder._build_ref_ids_expr(field_shape, "$$this"),
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
                                    "in": {"$mergeObjects": ["$$it", {embedded_key: per_item_value_expr}]},
                                }
                            },
                            f"${list_path}",
                        ]
                    }
                }
            }
        )

        self.pipeline.append({"$project": {docs_alias: 0}})

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

        # 1) lookup all referenced docs
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

        # 2) rewrite each value with joined doc (or missing marker)
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
                                                                    {
                                                                        "_missing_reference": True,
                                                                        "_ref": "$$refId",
                                                                        "_cls": cls_name,
                                                                    },
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

        # 3) cleanup
        self.pipeline.append(PipelineBuilder._project_remove(docs_alias))

    def _add_object_generic_lookup(self, generic_field, local_field):
        """
        For MapField(GenericReferenceField) or DictField(GenericReferenceField) where the stored value is an object:
            {k: {_ref, _cls}, ...}
        Replaces each value with the joined doc (merged with {_ref,_cls}) or missing marker.
        """
        doc_classes = PipelineBuilder._resolve_generic_choices(generic_field)
        if not doc_classes:
            return

        safe = local_field.replace(".", "_")

        def alias_for(cls):
            return f"{safe}__{cls.__name__}"

        # 1) lookups per class (collect IDs across objectToArray filtered by _cls)
        for cls in doc_classes:
            ref_ids_expr = {
                "$cond": [
                    {"$eq": [{"$type": f"${local_field}"}, "object"]},
                    {
                        "$map": {
                            "input": {
                                "$filter": {
                                    "input": {"$objectToArray": f"${local_field}"},
                                    "as": "kv",
                                    "cond": self._regex_match("$$kv.v._cls", cls),
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
                        "as": alias_for(cls),
                    }
                }
            )

        # 2) rewrite values using shared transform
        value_expr = PipelineBuilder._generic_value_transform_expr(
            doc_classes=doc_classes,
            alias_for_cls=alias_for,
            val_var="$$val",
        )

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
                                            "v": {"$let": {"vars": {"val": "$$kv.v"}, "in": value_expr}},
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

        # 3) cleanup
        self.pipeline.append(PipelineBuilder._project_remove(*[alias_for(cls) for cls in doc_classes]))

    # (kept exactly as your working version)
    def _add_dict_generic_lookup(self, generic_field, local_field):
        # Backward-compat shim: keep callers working
        return self._add_object_generic_lookup(generic_field, local_field)

    def _add_map_generic_lookup(self, generic_field, local_field):
        # Backward-compat shim: keep callers working
        return self._add_object_generic_lookup(generic_field, local_field)

    # GenericReferenceField lookup (scalar + list) - keep your working code
    @staticmethod
    def _resolve_generic_choices(generic_field):
        """Return concrete document classes for a GenericReferenceField's choices."""
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
        return doc_classes

    @staticmethod
    def _cls_regex(cls):
        return f"^{cls._class_name}(\\.|$)"

    @staticmethod
    def _regex_match(input_expr, cls):
        return {"$regexMatch": {"input": input_expr, "regex": PipelineBuilder._cls_regex(cls)}}

    @staticmethod
    def _project_remove(*paths):
        """Build a $project stage that removes each path/field."""
        return {"$project": {p: 0 for p in paths if p}}

    @staticmethod
    def _missing_generic_expr(ref_expr, cls_expr):
        return {"_missing_reference": True, "_ref": ref_expr, "_cls": cls_expr}

    @staticmethod
    def _generic_value_transform_expr(doc_classes, alias_for_cls, val_var="$$val"):
        """
        Build an expression that transforms one generic-ref dict in <val_var> into:
          - the joined document merged with {_ref, _cls}, OR
          - {_missing_reference: True, _ref, _cls}
        Requires per-class $lookup arrays named by alias_for_cls(cls).
        """
        expr = val_var
        for cls in reversed(doc_classes):
            alias_arr = f"${alias_for_cls(cls)}"
            class_test = PipelineBuilder._regex_match(f"{val_var}._cls", cls)

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
                                    {"_ref": f"{val_var}._ref", "_cls": f"{val_var}._cls"},
                                ]
                            },
                            PipelineBuilder._missing_generic_expr(f"{val_var}._ref", f"{val_var}._cls"),
                        ]
                    },
                }
            }

            expr = {"$cond": [class_test, branch, expr]}
        return expr

    @staticmethod
    def _generic_item_transform_expr(doc_classes, alias_for_cls, item_var="$$item"):
        """Same as _generic_value_transform_expr, but for list items."""
        return PipelineBuilder._generic_value_transform_expr(
            doc_classes=doc_classes,
            alias_for_cls=alias_for_cls,
            val_var=item_var,
        )

    def _add_generic_lookup(self, field, local_field, is_list=False):
        doc_classes = PipelineBuilder._resolve_generic_choices(field)
        if not doc_classes:
            return

        def alias_for(cls):
            return f"{local_field}__{cls.__name__}"

        # ----------------------------
        # SCALAR GENERIC
        # ----------------------------
        if not is_list:
            # 1) do all lookups
            for cls in doc_classes:
                self.pipeline.append(
                    {
                        "$lookup": {
                            "from": cls._get_collection_name(),
                            "localField": f"{local_field}._ref.$id",
                            "foreignField": "_id",
                            "as": alias_for(cls),
                        }
                    }
                )

            # 2) single addFields: nested cond over _cls
            transformed = PipelineBuilder._generic_value_transform_expr(
                doc_classes=doc_classes,
                alias_for_cls=alias_for,
                val_var=f"${local_field}",
            )

            self.pipeline.append({"$addFields": {local_field: transformed}})

            # 3) cleanup temps
            self.pipeline.append(PipelineBuilder._project_remove(*[alias_for(cls) for cls in doc_classes]))
            return

        # ----------------------------
        # LIST GENERIC (flat list)
        # ----------------------------
        for cls in doc_classes:
            self.pipeline.append(
                {
                    "$lookup": {
                        "from": cls._get_collection_name(),
                        "localField": f"{local_field}._ref.$id",
                        "foreignField": "_id",
                        "as": alias_for(cls),
                    }
                }
            )

        item_expr = PipelineBuilder._generic_item_transform_expr(
            doc_classes=doc_classes,
            alias_for_cls=alias_for,
            item_var="$$item",
        )

        self.pipeline.append(
            {
                "$addFields": {
                    local_field: {
                        "$map": {"input": f"${local_field}", "as": "item", "in": item_expr}
                    }
                }
            }
        )

        self.pipeline.append(PipelineBuilder._project_remove(*[alias_for(cls) for cls in doc_classes]))

    # Embedded list generic lookup (left as your working version)

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
        doc_classes = PipelineBuilder._resolve_generic_choices(generic_field)
        if not doc_classes:
            return

        safe_list = list_path.replace(".", "_")
        raw_values_expr = {"$ifNull": [f"${list_path}.{embedded_key}", []]}

        def alias_for(cls):
            return f"{safe_list}_{embedded_key}__{cls.__name__}"

        def cls_regex(cls):
            return f"^{cls._class_name}(\\.|$)"

        def regex_match(input_expr, cls):
            return {"$regexMatch": {"input": input_expr, "regex": cls_regex(cls)}}

        # 1) lookups per concrete class (collect ids across ALL embedded docs, flattening lists)
        aliases = []
        for cls in doc_classes:
            alias = alias_for(cls)
            aliases.append(alias)

            # IMPORTANT: $$m is only valid inside $filter/$map scopes.
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
                alias_arr = f"${alias_for(cls)}"
                class_test_val = regex_match("$$val._cls", cls)

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

    # Structured ref lookup (kept)
    def _add_structured_ref_lookup(self, target_cls, field_shape, local_field):
        if not target_cls:
            return

        docs_alias = f"{local_field}__docs"
        ref_ids_expr = PipelineBuilder._build_ref_ids_expr(field_shape, f"${local_field}")

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

    # Abstract DBRef lookup (kept)
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
                {"$lookup": {"from": coll, "localField": f"{local_field}.$id", "foreignField": "_id", "as": temp}}
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

    # Ref id + value reconstruction (kept)
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
                            "in": {"$concatArrays": ["$$value",
                                                     PipelineBuilder._build_ref_ids_expr(field.field, "$$this")]},
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
                    "in": {"$concatArrays": ["$$value", PipelineBuilder._build_ref_ids_expr(field.field, "$$this.v")]},
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
                              "in": PipelineBuilder._build_value_expr(field.field, "$$item", docs_expr)}},
                    source_expr,
                ]
            }

        if isinstance(field, DictField):
            return {
                "$arrayToObject": {
                    "$map": {
                        "input": {"$objectToArray": source_expr},
                        "as": "kv",
                        "in": {"k": "$$kv.k", "v": PipelineBuilder._build_value_expr(field.field, "$$kv.v", docs_expr)},
                    }
                }
            }

        return source_expr

    # PROJECTION / SORT / LIMIT / ETC
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
        self.pipeline.append({"$sort": {field: direction for field, direction in ordering}})

    def _skip_stage(self):
        if self.queryset._skip:
            self.pipeline.append({"$skip": self.queryset._skip})

    def _limit_stage(self):
        if self.queryset._limit is not None:
            self.pipeline.append({"$limit": self.queryset._limit})

    # REGEX conversion
    @staticmethod
    def _convert_regex(value):
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

    @staticmethod
    def _walk_and_convert_regex(obj):
        if isinstance(obj, dict):
            return {k: PipelineBuilder._walk_and_convert_regex(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [PipelineBuilder._walk_and_convert_regex(v) for v in obj]
        return PipelineBuilder._convert_regex(obj)


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
