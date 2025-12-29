from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict

from .schema import Schema


class MatchPlanner:
    """
    Buckets match fragments by the required lookup prefix (db_field dotted path).

    NOTE (filter-only policy):
      We intentionally DO NOT rewrite matches into $expr forms for:
        - nested lists of ReferenceField
        - MapField(ReferenceField)
        - DictField(GenericReferenceField)
      because those rewrites assume hydrated subdocuments (e.g. $$it.age),
      which is false when we keep refs as ObjectId/DBRef unless select_related.
    """

    def bucket(self, doc_cls, query: Dict[str, Any]) -> Dict[str, Any]:
        return self._bucket_query_by_lookup_prefix(doc_cls, query)

    @staticmethod
    def _bucket_query_by_lookup_prefix(doc_cls, query: dict) -> dict:
        buckets: Dict[str, Any] = {}

        def merge(prefix: str, frag: dict):
            if not frag:
                return
            if prefix not in buckets:
                buckets[prefix] = frag
            else:
                existing = buckets[prefix]
                if existing != frag:
                    buckets[prefix] = {"$and": [existing, frag]}

        def dotted(k: str) -> str:
            # Convert mongoengine-style "__" to dotted path if it isn't already dotted.
            return k.replace("__", ".") if ("__" in k and "." not in k) else k

        def get_field_by_db_part(cur, part):
            fld = cur._fields.get(part)
            if fld:
                return fld
            for _name, f in cur._fields.items():
                if getattr(f, "db_field", None) == part:
                    return f
            return None

        def walk(q, cur_doc=doc_cls):
            if not isinstance(q, dict):
                merge("", q)
                return

            # logical operators
            for op in ("$and", "$or", "$nor"):
                if op in q:
                    clauses = q.get(op) or []
                    per_prefix = defaultdict(list)
                    for clause in clauses:
                        sub = MatchPlanner._bucket_query_by_lookup_prefix(cur_doc, clause)
                        for pfx, frag in sub.items():
                            per_prefix[pfx].append(frag)
                    for pfx, frags in per_prefix.items():
                        merge(pfx, frags[0] if len(frags) == 1 else {op: frags})

            for k, v in q.items():
                if isinstance(k, str) and k.startswith("$"):
                    # already handled logical ops above; keep other top-level operators at root
                    if k not in ("$and", "$or", "$nor"):
                        merge("", {k: v})
                    continue

                fk = dotted(k)
                parts = fk.split(".")
                if not parts:
                    continue

                first = parts[0]
                fld0 = get_field_by_db_part(cur_doc, first)

                # IMPORTANT:
                # We do not do any $expr rewrites here (map/dict/nested list), because those rely on hydration.
                # We only compute the required lookup prefix and bucket the plain predicate.
                prefix = MatchPlanner.required_lookup_prefix_for_field(cur_doc, fk)
                merge(prefix, {fk: v})

        walk(query)
        return buckets

    @staticmethod
    def required_lookup_prefix_for_field(doc_cls, field_key: str) -> str:
        """
        Return the deepest deref prefix required for a dotted path.
        Handles ReferenceField, ListField(ReferenceField), MapField(ReferenceField),
        DictField(ReferenceField), DictField(GenericReferenceField),
        and safe GenericReferenceField -> common ReferenceField traversal.
        """
        from mongoengine.fields import (
            ListField,
            ReferenceField,
            GenericReferenceField,
            EmbeddedDocumentField,
            EmbeddedDocumentListField,
            MapField,
            DictField,
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

            is_terminal = (i == len(parts) - 1)

            # ---- unwrap list leaf for type checks
            leaf = fld
            while isinstance(leaf, ListField):
                leaf = leaf.field

            # ---- MapField(...) / DictField(...)
            # If user queries "by_key.age" or "d.age", we must deref at that field
            # (can't be root match). So require lookup prefix at this db_path.
            if isinstance(fld, MapField):
                inner = getattr(fld, "field", None)
                inner_leaf = inner
                while isinstance(inner_leaf, ListField):
                    inner_leaf = inner_leaf.field
                if isinstance(inner_leaf, (ReferenceField, GenericReferenceField)) and not is_terminal:
                    last_deref_prefix = ".".join(db_path)
                    return last_deref_prefix

            if isinstance(fld, DictField):
                inner = getattr(fld, "field", None)
                inner_leaf = inner
                while isinstance(inner_leaf, ListField):
                    inner_leaf = inner_leaf.field
                if isinstance(inner_leaf, (ReferenceField, GenericReferenceField)) and not is_terminal:
                    last_deref_prefix = ".".join(db_path)
                    return last_deref_prefix

            # ---- ReferenceField
            if isinstance(leaf, ReferenceField):
                if not is_terminal:
                    last_deref_prefix = ".".join(db_path)
                    cur = getattr(leaf, "document_type_obj", None) or getattr(leaf, "document_type", None)
                    continue
                return last_deref_prefix

            # ---- GenericReferenceField
            if isinstance(leaf, GenericReferenceField):
                if not is_terminal:
                    next_part = parts[i + 1]
                    common_ref_field, _common_target = MatchPlanner.generic_common_ref(leaf, next_part)

                    if common_ref_field is not None:
                        last_deref_prefix = ".".join(db_path)
                        from mongoengine.document import _DocumentRegistry
                        ch0 = (leaf.choices or ())[0]
                        cur = _DocumentRegistry.get(ch0 if isinstance(ch0, str) else ch0.__name__)
                        continue

                    last_deref_prefix = ".".join(db_path)
                    return last_deref_prefix
                return last_deref_prefix

            # ---- embedded doc descend
            if isinstance(fld, (EmbeddedDocumentField, EmbeddedDocumentListField)) or getattr(leaf, "document_type",
                                                                                              None):
                cur = getattr(leaf, "document_type", None) or getattr(leaf, "document_type_obj", None)
                continue

            cur = None

        return last_deref_prefix

    @staticmethod
    def generic_common_ref(generic_field, next_part: str):
        """
        If all GenericReferenceField choices share `next_part` as a ReferenceField to the same doc type.
        """
        from mongoengine.fields import ReferenceField, ListField

        doc_classes = Schema.resolve_generic_choices(generic_field)
        if not doc_classes:
            return None, None

        targets = []
        representative_field = None

        for cls in doc_classes:
            fld = cls._fields.get(next_part)
            if fld is None:
                for _n, f in cls._fields.items():
                    if getattr(f, "db_field", None) == next_part:
                        fld = f
                        break
            if fld is None:
                return None, None

            representative_field = representative_field or fld

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

        return representative_field, targets[0]

    @staticmethod
    def _safe_resolve_generic_choices(generic_field):
        from .schema import Schema
        try:
            return Schema.resolve_generic_choices(generic_field) or []
        except Exception:
            return []
