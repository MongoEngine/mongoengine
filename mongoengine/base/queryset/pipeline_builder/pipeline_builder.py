from __future__ import annotations

from typing import Any

from .normalizer import QueryNormalizer
from .match_planner import MatchPlanner
from .lookup_planner import LookupPlanner
from .stage_builder import StageBuilder
from .tail_builder import TailBuilder

__all__ = ("PipelineBuilder",)


class PipelineBuilder:
    """
    Orchestrator only. No heavy logic lives here.
    """

    def __init__(self, queryset, mongo_version=None):
        self.qs = queryset
        self.doc = queryset._document

        self.normalizer = QueryNormalizer()
        self.match_planner = MatchPlanner()
        self.lookup_planner = LookupPlanner()
        self.stage_builder = StageBuilder(mongo_version=mongo_version)
        self.tail_builder = TailBuilder()

    def build(self) -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = []
        mongo_query: dict[str, Any] = self.qs._query or {}

        hydrate_tree = self.lookup_planner.plan_from_select_related(
            self.qs._select_related
        )

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
                pipeline.append(
                    {
                        "$match": leftovers[0]
                        if len(leftovers) == 1
                        else {"$and": leftovers}
                    }
                )

        if function_expr:
            pipeline.append({"$match": function_expr})

        pipeline.extend(self.tail_builder.build(self.qs))
        return pipeline
