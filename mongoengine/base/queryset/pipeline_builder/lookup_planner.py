from __future__ import annotations

from typing import Any, Dict, Iterable

from .match_planner import MatchPlanner
from .schema import Schema


class LookupPlanner:
    """
    Builds a lookup tree keyed by *field names* (not db_field), suitable for StageBuilder._walk_lookups.

    Inputs:
      - select_related: mongoengine select_related spec
      - bucket_prefixes: iterable of db_field dotted prefixes produced by MatchPlanner.bucket()

    Output:
      - tree: dict like {"items": {"parent": {}}, "parent": {"gp": {}}}
    """

    def plan_from_select_related(self, select_related) -> dict:
        return self._tree_from_select_related(select_related)

    def plan(self, doc_cls, select_related, bucket_prefixes: Iterable[str]) -> dict:
        tree: dict[str, Any] = {}

        # 1) bucket-prefix-derived tree FIRST (filter stages happen earlier)
        for prefix in bucket_prefixes or ():
            if not prefix:
                continue
            p_tree = self._tree_from_db_prefix(doc_cls, prefix)
            self._merge_tree(tree, p_tree)

        # 2) select_related tree AFTER (hydrate after filtering)
        if select_related:
            sr_tree = self.plan_from_select_related(select_related)
            self._merge_tree(tree, sr_tree)

        return tree

    # ---------------- internals ----------------

    def _tree_from_db_prefix(self, doc_cls, db_prefix: str) -> dict:
        """
        Convert db_field dotted path like "target.gp" into a field-name tree like {"target": {"gp": {}}}.

        Key behavior:
          - ReferenceField: if there are more segments, traverse into referenced document
          - GenericReferenceField: if there are more segments and next segment is a COMMON ReferenceField
            across choices, traverse into representative choice document so later segments can be planned.
        """
        from mongoengine.fields import (
            EmbeddedDocumentField,
            EmbeddedDocumentListField,
            ListField,
            ReferenceField,
            GenericReferenceField,
            MapField,
            DictField,
        )

        parts = [p for p in db_prefix.split(".") if p]
        if not parts:
            return {}

        cur_doc = doc_cls
        root: dict[str, Any] = {}
        node = root

        i = 0
        while i < len(parts):
            if cur_doc is None:
                break

            db_part = parts[i]
            fld = self._get_field_by_db_part(cur_doc, db_part)
            if fld is None:
                break

            field_name = fld.name
            node = node.setdefault(field_name, {})

            is_last = (i == len(parts) - 1)

            # unwrap list wrapper for leaf checks
            leaf = fld
            while isinstance(leaf, ListField):
                leaf = leaf.field

            # ---- embedded boundary: descend schema
            if isinstance(fld, EmbeddedDocumentField):
                cur_doc = fld.document_type
                i += 1
                continue

            if isinstance(fld, EmbeddedDocumentListField) or (
                    isinstance(fld, ListField) and isinstance(getattr(fld, "field", None), EmbeddedDocumentField)
            ):
                embedded_dt = getattr(fld, "document_type", None)
                if embedded_dt is None and isinstance(getattr(fld, "field", None), EmbeddedDocumentField):
                    embedded_dt = fld.field.document_type
                cur_doc = embedded_dt
                i += 1
                continue

            # ---- MapField / DictField: lookup happens at this node; deeper handled by MatchPlanner $expr rewrites
            if isinstance(fld, (MapField, DictField)):
                break

            # ---- ReferenceField: keep traversing if more segments remain
            if isinstance(leaf, ReferenceField):
                if is_last:
                    break
                cur_doc = getattr(leaf, "document_type_obj", None) or getattr(leaf, "document_type", None)
                i += 1
                continue

            # ---- GenericReferenceField:
            # If next segment is a COMMON ReferenceField across choices, traverse into representative choice doc
            if isinstance(leaf, GenericReferenceField):
                if is_last:
                    break

                next_part = parts[i + 1]
                common_ref_field, _common_target = MatchPlanner.generic_common_ref(leaf, next_part)
                if common_ref_field is None:
                    # cannot safely traverse beyond generic
                    break

                # Ensure the tree includes the common-ref child
                # (StageBuilder will use this to emit lookup on target.<common_ref>)
                node = node.setdefault(common_ref_field.name, {})

                # Traverse schema as if we're in a representative choice class
                # so we can plan deeper segments (like ...gp.age... -> prefix target.gp)
                doc_classes = MatchPlanner._safe_resolve_generic_choices(leaf)
                cur_doc = doc_classes[0] if doc_classes else None

                # We consumed "next_part" by inserting common_ref_field.name
                i += 2
                continue

            # ---- scalar: can't traverse further
            break

        return root

    @staticmethod
    def _merge_tree(dst: dict, src: dict) -> None:
        for k, v in (src or {}).items():
            if k not in dst:
                dst[k] = v if isinstance(v, dict) else {}
            else:
                if isinstance(dst[k], dict) and isinstance(v, dict):
                    LookupPlanner._merge_tree(dst[k], v)

    @staticmethod
    def _get_field_by_db_part(doc_cls, db_part: str):
        if doc_cls is None:
            return None
        _, field = Schema.resolve_field_name(doc_cls, db_part)
        return field

    # ---- select_related converter (keep / adapt to your queryset format)
    def _tree_from_select_related(self, select_related) -> dict:
        if not select_related:
            return {}

        if isinstance(select_related, (list, tuple, set)):
            paths = []
            for p in select_related:
                if isinstance(p, str) and p:
                    paths.append(p.replace("__", "."))
            return self._tree_from_paths(paths)

        return {}

    @staticmethod
    def _tree_from_paths(paths: Iterable[str]) -> dict:
        root: dict[str, Any] = {}
        for p in paths:
            if not p:
                continue
            parts = [x for x in p.split(".") if x]
            node = root
            for part in parts:
                node = node.setdefault(part, {})
        return root
