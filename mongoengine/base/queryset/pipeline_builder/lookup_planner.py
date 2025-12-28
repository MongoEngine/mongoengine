from __future__ import annotations

from typing import Dict, Iterable

from .schema import Schema
from .match_planner import MatchPlanner


class LookupPlanner:
    """
    Pure planning: produces a lookup tree (python field names).
    Does NOT emit Mongo stages.
    """

    def plan_from_select_related(self, select_related) -> dict:
        return self.build_related_tree(select_related)

    def plan(self, doc_cls, select_related, bucket_prefixes: Iterable[str]) -> dict:
        tree = {}
        if select_related:
            tree = self.merge_trees(tree, self.build_related_tree(select_related))
        tree = self.merge_trees(tree, self.auto_tree_from_bucket_prefixes(doc_cls, bucket_prefixes))
        return tree

    @staticmethod
    def build_related_tree(fields) -> dict:
        tree = {}
        for f in fields or []:
            parts = f.split("__")
            node = tree
            for p in parts:
                node = node.setdefault(p, {})
            node[""] = True
        return tree

    @staticmethod
    def merge_trees(a: dict, b: dict) -> dict:
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
                    out[k] = LookupPlanner.merge_trees(out[k], v)
        return out

    def auto_tree_from_bucket_prefixes(self, root_doc_cls, bucket_prefixes: Iterable[str]) -> dict:
        """
        Bucket prefixes are db_field dotted (e.g. "target.gp").
        We build a tree using python field names, with safe GenericRef traversal.
        """
        tree: Dict[str, dict] = {}

        for dotted_prefix in bucket_prefixes:
            if not dotted_prefix:
                continue

            parts = dotted_prefix.split(".")
            node = tree
            cur = root_doc_cls

            for idx, db_part in enumerate(parts):
                if cur is None:
                    break

                field_name, fld = Schema.resolve_field_name(cur, db_part)
                if not fld:
                    break

                node = node.setdefault(field_name, {})

                from mongoengine.fields import ReferenceField, GenericReferenceField, EmbeddedDocumentField, \
                    EmbeddedDocumentListField

                leaf = Schema.unwrap_list_leaf(fld)

                if isinstance(leaf, ReferenceField):
                    cur = getattr(leaf, "document_type_obj", None) or getattr(leaf, "document_type", None)
                    continue

                if isinstance(leaf, GenericReferenceField):
                    if idx < len(parts) - 1:
                        next_part = parts[idx + 1]
                        common_ref_field, _common_target = MatchPlanner.generic_common_ref(leaf, next_part)
                        if common_ref_field is None:
                            cur = None
                            break

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
