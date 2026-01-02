"""
MongoDB Aggregation Pipeline Builder for MongoEngine QuerySets.

This module provides the PipelineBuilder class that converts MongoEngine QuerySets
into MongoDB aggregation pipelines with automatic dereferencing support for various
field types, including ReferenceFields, GenericReferenceFields, and nested structures.
"""

from __future__ import annotations

from typing import Any, Dict, List

from .normalizer import QueryNormalizer
from .match_planner import MatchPlanner
from .lookup_planner import LookupPlanner
from .stage_builder import StageBuilder
from .tail_builder import TailBuilder

__all__ = ("PipelineBuilder", "needs_aggregation",)


class PipelineBuilder:
    """
    Orchestrator only. No heavy logic lives here.
    """

    def __init__(self, queryset):
        self.qs = queryset
        self.doc = queryset._document

        self.normalizer = QueryNormalizer()
        self.match_planner = MatchPlanner()
        self.lookup_planner = LookupPlanner()
        self.stage_builder = StageBuilder()
        self.tail_builder = TailBuilder()

    def build(self) -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = []
        mongo_query: dict[str, Any] = self.qs._query or {}

        hydrate_tree = self.lookup_planner.plan_from_select_related(self.qs._select_related)

        if not mongo_query:
            if self.qs._select_related:
                pipeline.extend(
                    self.stage_builder.emit(
                        doc_cls=self.doc,
                        prefix="",
                        tree=hydrate_tree,
                        buckets=None,
                        interleave=False,
                        embedded_list_path=None,
                        hydrate_tree=hydrate_tree,
                    )
                )
            pipeline.extend(self.tail_builder.build(self.qs))
            return pipeline

        cleaned, function_expr = self.normalizer.normalize(mongo_query)
        buckets = self.match_planner.bucket(self.doc, cleaned)

        root_match = buckets.pop("", None)
        if root_match:
            pipeline.append({"$match": root_match})

        tree = self.lookup_planner.plan(
            doc_cls=self.doc,
            select_related=self.qs._select_related,
            bucket_prefixes=list(buckets.keys()),
        )

        if tree:
            pipeline.extend(
                self.stage_builder.emit(
                    doc_cls=self.doc,
                    prefix="",
                    tree=tree,
                    buckets=buckets,
                    interleave=True,
                    embedded_list_path=None,
                    hydrate_tree=hydrate_tree,
                )
            )

        if buckets:
            leftovers = [q for q in buckets.values() if q]
            if leftovers:
                pipeline.append({"$match": leftovers[0] if len(leftovers) == 1 else {"$and": leftovers}})

        if function_expr:
            pipeline.append({"$match": function_expr})

        pipeline.extend(self.tail_builder.build(self.qs))
        return pipeline


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
