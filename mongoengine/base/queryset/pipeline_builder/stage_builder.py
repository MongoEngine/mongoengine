from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, List

from .schema import Schema
from .match_planner import MatchPlanner


@dataclass(frozen=True)
class WalkCtx:
    """
    Immutable traversal context.

    Attributes:
        doc_cls: The current MongoEngine Document/EmbeddedDocument class whose fields we are walking.
        prefix: Field prefix (dot path) to reach the current doc from the root aggregation document.
        tree: Lookup subtree for the current doc_cls.
        buckets: Optional dict of pre-planned $match buckets for interleaving.
        interleave: If True, pop/apply buckets as lookups are emitted.
        embedded_list_path: If not None, we're walking inside a list of embedded documents at this path.
        hydrate_tree: Tree indicating which paths are select_related requested (hydrate allowed).
    """
    doc_cls: Any
    prefix: str
    tree: Dict[str, Any]
    buckets: Optional[Dict[str, Any]]
    interleave: bool
    embedded_list_path: Optional[str]
    hydrate_tree: Dict[str, Any]


@dataclass(frozen=True)
class WalkNode:
    """
    Resolved info about a single field visit.

    Computed flags are derived from subtree presence and hydrate_tree request.
    """
    field_name: str
    field: Any
    subtree: Dict[str, Any]
    full_path: str

    requested_hydrate: bool
    subtree_hydrate_tree: Dict[str, Any]

    needs_traversal: bool
    hydrate_effective: bool

    preserve_orig: bool
    orig_alias: Optional[str]


class StageBuilder:
    """
    Emits MongoDB aggregation stages from a lookup tree.

    Policy:
      - Only hydrate ($addFields overwrite reference) when select_related asked for it,
        OR when required temporarily for deeper traversal.
      - Otherwise: lookup is filter-only (keeps ObjectId/DBRef unchanged) BUT still filters via join results.
      - If deeper traversal is required to evaluate lookups, we may hydrate temporarily and restore the original.

    Design:
      - _walk() traverses the lookup tree and dispatches to small handlers.
      - All actual stage composition happens in _add_* methods.
      - Buckets interleaving:
          - If possible, we convert a bucket on "prefix.somefield" into a foreign-doc match and apply it
            as a local-root filter against joined docs (avoids filtering the lookup result array and keeps
            hydration correct).
    """

    def __init__(self) -> None:
        self._pipeline: List[dict] = []

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------

    def emit(
            self,
            doc_cls,
            prefix: str,
            tree: dict,
            buckets: Optional[dict],
            interleave: bool,
            embedded_list_path=None,
            hydrate_tree: Optional[dict] = None,
    ) -> List[dict]:
        """
        Build pipeline stages for the given lookup tree.

        Args:
            doc_cls: Root Document class.
            prefix: Field prefix for this walk (usually "").
            tree: Lookup tree dict, e.g. {"author": {"books": {...}}}
            buckets: Optional match bucket dict emitted by planner.
            interleave: Whether to apply buckets interleaved with lookups.
            embedded_list_path: Internal: indicates we're walking embedded-doc list items.
            hydrate_tree: Tree of select_related requested paths.

        Returns:
            List of aggregation pipeline stages.
        """
        ctx = WalkCtx(
            doc_cls=doc_cls,
            prefix=prefix,
            tree=tree or {},
            buckets=buckets,
            interleave=interleave,
            embedded_list_path=embedded_list_path,
            hydrate_tree=hydrate_tree or {},
        )
        self._walk(ctx)
        return self._pipeline

    # ---------------------------------------------------------------------
    # Traversal / Dispatch
    # ---------------------------------------------------------------------

    def _walk(self, ctx: WalkCtx) -> None:
        """Traverse ctx.tree and append stages to self._pipeline."""
        from mongoengine.fields import FileField

        for field_name, subtree in (ctx.tree or {}).items():
            if not field_name or field_name == "":
                continue

            field = ctx.doc_cls._fields.get(field_name)
            if not field:
                continue

            if isinstance(field, FileField):
                continue

            node = self._resolve_node(ctx, field_name, field, subtree or {})

            # Dispatch in priority order:
            if self._handle_reference_field(ctx, node):
                continue
            if self._handle_list_field(ctx, node):
                continue
            if self._handle_embedded_document_field(ctx, node):
                continue
            if self._handle_map_ref_field(ctx, node):
                continue
            if self._handle_dict_field(ctx, node):
                continue
            if self._handle_generic_reference_field(ctx, node):
                continue

            # Non-relational field: nothing to do.

    @staticmethod
    def _resolve_node(ctx: WalkCtx, field_name: str, field: Any, subtree: Dict[str, Any]) -> WalkNode:
        """Compute derived properties for one field visit."""
        full_path = f"{ctx.prefix}{field.db_field}" if ctx.prefix else field.db_field

        requested_hydrate = field_name in ctx.hydrate_tree
        subtree_hydrate_tree = ctx.hydrate_tree.get(field_name, {}) if requested_hydrate else {}

        # Traversal requires hydration outside embedded-list mode.
        needs_traversal = bool(subtree) and not ctx.embedded_list_path
        hydrate_effective = requested_hydrate or needs_traversal

        preserve_orig = needs_traversal and not requested_hydrate
        orig_alias = f"__orig__{full_path.replace('.', '_')}" if preserve_orig else None

        return WalkNode(
            field_name=field_name,
            field=field,
            subtree=subtree,
            full_path=full_path,
            requested_hydrate=requested_hydrate,
            subtree_hydrate_tree=subtree_hydrate_tree,
            needs_traversal=needs_traversal,
            hydrate_effective=hydrate_effective,
            preserve_orig=preserve_orig,
            orig_alias=orig_alias,
        )

    # ---------------------------------------------------------------------
    # Bucket / preserve helpers
    # ---------------------------------------------------------------------

    def _apply_bucket_if_any(self, ctx: WalkCtx, full_path: str) -> None:
        """Apply an interleaved root bucket $match for a given full_path (if any)."""
        if not ctx.interleave or ctx.buckets is None:
            return
        bucket = ctx.buckets.pop(full_path, None)
        if bucket:
            self._pipeline.append({"$match": bucket})

    def _maybe_pop_foreign_match(self, ctx: WalkCtx, prefix: str) -> Optional[dict]:
        """
        If interleaving is enabled, attempt to pop a bucket for prefix and convert it
        to a foreign-doc match (keys relative to foreign doc).
        """
        if not ctx.interleave or ctx.buckets is None:
            return None
        return self._pop_foreign_match_for_prefix(ctx.buckets, prefix)

    def _maybe_preserve(self, full_path: str, preserve: bool, alias: Optional[str]) -> Optional[str]:
        """Preserve the original field value into alias (for temporary hydration traversal)."""
        if not preserve or not alias:
            return None
        self._pipeline.append({"$addFields": {alias: f"${full_path}"}})
        return alias

    def _maybe_restore(self, full_path: str, alias: Optional[str]) -> None:
        """Restore field value from alias and remove alias from projection."""
        if not alias:
            return
        self._pipeline.append({"$addFields": {full_path: f"${alias}"}})
        self._pipeline.append(self._project_remove(alias))

    # ---------------------------------------------------------------------
    # Field handlers
    # ---------------------------------------------------------------------

    def _handle_reference_field(self, ctx: WalkCtx, node: WalkNode) -> bool:
        from mongoengine.fields import ReferenceField

        if not isinstance(node.field, ReferenceField):
            return False

        target = node.field.document_type_obj
        orig_alias = self._maybe_preserve(node.full_path, node.preserve_orig, node.orig_alias)
        foreign_match = self._maybe_pop_foreign_match(ctx, node.full_path)

        if ctx.embedded_list_path:
            self._add_embedded_list_structured_ref_lookup(
                target_cls=target,
                field_shape=node.field,
                list_path=ctx.embedded_list_path,
                embedded_key=node.field.db_field,
                foreign_match=foreign_match,
                hydrate=node.hydrate_effective,
            )
            if foreign_match is None:
                self._apply_bucket_if_any(ctx, node.full_path)
        else:
            if target and target._meta.get("abstract", False):
                self._add_abstract_dbref_lookup(target, node.full_path)
                if foreign_match is not None:
                    self._pipeline.append({"$match": foreign_match})
            else:
                self._add_structured_ref_lookup(
                    target_cls=target,
                    field_shape=node.field,
                    local_field=node.full_path,
                    foreign_match=foreign_match,
                    hydrate=node.hydrate_effective,
                )
                if foreign_match is None:
                    self._apply_bucket_if_any(ctx, node.full_path)

        # descend
        if node.subtree and not ctx.embedded_list_path:
            self._walk(
                WalkCtx(
                    doc_cls=target,
                    prefix=f"{node.full_path}.",
                    tree=node.subtree,
                    buckets=ctx.buckets,
                    interleave=ctx.interleave,
                    embedded_list_path=None,
                    hydrate_tree=node.subtree_hydrate_tree,
                )
            )

        self._maybe_restore(node.full_path, orig_alias)
        return True

    def _handle_list_field(self, ctx: WalkCtx, node: WalkNode) -> bool:
        from mongoengine.fields import ListField, ReferenceField, GenericReferenceField

        if not isinstance(node.field, ListField):
            return False

        # list of embedded docs
        if self._is_list_of_embedded(node.field):
            embedded_doc = self._embedded_doc_type(node.field)
            if node.subtree and embedded_doc:
                self._walk(
                    WalkCtx(
                        doc_cls=embedded_doc,
                        prefix=f"{node.full_path}.",
                        tree=node.subtree,
                        buckets=ctx.buckets,
                        interleave=ctx.interleave,
                        embedded_list_path=node.full_path,
                        hydrate_tree=node.subtree_hydrate_tree,
                    )
                )
            return True

        leaf, _depth = Schema.unwrap_list_field(node.field)

        # List[ReferenceField]
        if leaf is not None and isinstance(leaf, ReferenceField):
            target = leaf.document_type
            orig_alias = self._maybe_preserve(node.full_path, node.preserve_orig, node.orig_alias)
            foreign_match = self._maybe_pop_foreign_match(ctx, node.full_path)

            if ctx.embedded_list_path:
                self._add_embedded_list_structured_ref_lookup(
                    target_cls=target,
                    field_shape=node.field,
                    list_path=ctx.embedded_list_path,
                    embedded_key=node.field.db_field,
                    foreign_match=foreign_match,
                    hydrate=node.hydrate_effective,
                )
                if foreign_match is None:
                    self._apply_bucket_if_any(ctx, node.full_path)
            else:
                self._add_structured_ref_lookup(
                    target_cls=target,
                    field_shape=node.field,
                    local_field=node.full_path,
                    foreign_match=foreign_match,
                    hydrate=node.hydrate_effective,
                )
                if foreign_match is None:
                    self._apply_bucket_if_any(ctx, node.full_path)

            if node.subtree and not ctx.embedded_list_path:
                self._walk(
                    WalkCtx(
                        doc_cls=target,
                        prefix=f"{node.full_path}.",
                        tree=node.subtree,
                        buckets=ctx.buckets,
                        interleave=ctx.interleave,
                        embedded_list_path=None,
                        hydrate_tree=node.subtree_hydrate_tree,
                    )
                )

            self._maybe_restore(node.full_path, orig_alias)
            return True

        # List[GenericReferenceField]
        if leaf is not None and isinstance(leaf, GenericReferenceField) and leaf.choices:
            if ctx.embedded_list_path:
                foreign_match = self._maybe_pop_foreign_match(ctx, node.full_path)
                self._add_embedded_list_generic_lookup(
                    generic_field=leaf,
                    list_path=ctx.embedded_list_path,
                    embedded_key=node.field.db_field,
                    foreign_match=foreign_match,
                    hydrate=node.requested_hydrate,  # select_related only
                )
                if foreign_match is None:
                    self._apply_bucket_if_any(ctx, node.full_path)
            else:
                # Keep existing behavior for scalar list-of-generic (hydrates)
                self._add_generic_lookup(leaf, node.full_path, is_list=True)
                self._apply_bucket_if_any(ctx, node.full_path)
            return True

        return True

    def _handle_embedded_document_field(self, ctx: WalkCtx, node: WalkNode) -> bool:
        from mongoengine.fields import EmbeddedDocumentField

        if not isinstance(node.field, EmbeddedDocumentField):
            return False

        if node.subtree:
            self._walk(
                WalkCtx(
                    doc_cls=node.field.document_type,
                    prefix=f"{node.full_path}.",
                    tree=node.subtree,
                    buckets=ctx.buckets,
                    interleave=ctx.interleave,
                    embedded_list_path=ctx.embedded_list_path,
                    hydrate_tree=node.subtree_hydrate_tree,
                )
            )
        return True

    def _handle_map_ref_field(self, ctx: WalkCtx, node: WalkNode) -> bool:
        from mongoengine.fields import MapField, ReferenceField

        if not (isinstance(node.field, MapField) and isinstance(node.field.field, ReferenceField)):
            return False

        if ctx.embedded_list_path:
            self._apply_bucket_if_any(ctx, node.full_path)
            return True

        foreign_match = self._maybe_pop_foreign_match(ctx, node.full_path)
        target = node.field.field.document_type_obj or node.field.field.document_type

        self._add_structured_ref_lookup(
            target_cls=target,
            field_shape=node.field,
            local_field=node.full_path,
            foreign_match=foreign_match,
            hydrate=node.requested_hydrate,  # IMPORTANT: hydrate only when select_related asked
        )

        if foreign_match is None:
            self._apply_bucket_if_any(ctx, node.full_path)

        return True

    def _handle_dict_field(self, ctx: WalkCtx, node: WalkNode) -> bool:
        from mongoengine.fields import DictField, GenericReferenceField

        if not isinstance(node.field, DictField):
            return False

        if ctx.embedded_list_path:
            self._apply_bucket_if_any(ctx, node.full_path)
            return True

        # DictField with ReferenceField leaves all pointing to same target
        target = self._resolve_single_ref_target(node.field)
        if target is not None:
            foreign_match = self._maybe_pop_foreign_match(ctx, node.full_path)
            self._add_structured_ref_lookup(
                target_cls=target,
                field_shape=node.field,
                local_field=node.full_path,
                foreign_match=foreign_match,
                hydrate=node.requested_hydrate,  # IMPORTANT
            )
            if foreign_match is None:
                self._apply_bucket_if_any(ctx, node.full_path)
            return True

        # DictField(GenericReferenceField)
        if isinstance(node.field.field, GenericReferenceField) and getattr(node.field.field, "choices", None):
            foreign_match = self._maybe_pop_foreign_match(ctx, node.full_path)
            self._add_object_generic_lookup(
                generic_field=node.field.field,
                local_field=node.full_path,
                foreign_match=foreign_match,
                hydrate=node.requested_hydrate,  # IMPORTANT
            )
            if foreign_match is None:
                self._apply_bucket_if_any(ctx, node.full_path)
            return True

        return True

    def _handle_generic_reference_field(self, ctx: WalkCtx, node: WalkNode) -> bool:
        from mongoengine.fields import GenericReferenceField

        if not (isinstance(node.field, GenericReferenceField) and node.field.choices):
            return False

        if ctx.embedded_list_path:
            foreign_match = self._maybe_pop_foreign_match(ctx, node.full_path)
            self._add_embedded_list_generic_lookup(
                generic_field=node.field,
                list_path=ctx.embedded_list_path,
                embedded_key=node.field.db_field,
                foreign_match=foreign_match,
                hydrate=node.requested_hydrate,  # select_related only
            )
            if foreign_match is None:
                self._apply_bucket_if_any(ctx, node.full_path)
            return True

        # Scalar generic lookup (existing behavior: hydrates always)
        orig_alias = self._maybe_preserve(node.full_path, node.preserve_orig, node.orig_alias)
        self._add_generic_lookup(node.field, node.full_path)
        self._apply_bucket_if_any(ctx, node.full_path)

        # Safe traversal under generic
        if node.subtree:
            self._walk_under_generic(ctx, node)

        self._maybe_restore(node.full_path, orig_alias)
        return True

    def _walk_under_generic(self, ctx: WalkCtx, node: WalkNode) -> None:
        """
        Traverse deeper under a scalar GenericReferenceField by finding common ref fields
        across choices for a given sub-path.
        """
        for sub_name, sub_tree in node.subtree.items():
            if not sub_name or sub_name == "":
                continue

            common_ref_field, common_target = MatchPlanner.generic_common_ref(node.field, sub_name)
            if common_ref_field is None or common_target is None:
                continue

            gp_path = f"{node.full_path}.{common_ref_field.db_field}"

            foreign_match = self._maybe_pop_foreign_match(ctx, gp_path)

            hydrate_gp = bool(node.subtree_hydrate_tree.get(sub_name))
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
                self._apply_bucket_if_any(ctx, gp_path)

            if sub_tree:
                self._walk(
                    WalkCtx(
                        doc_cls=common_target,
                        prefix=f"{gp_path}.",
                        tree=sub_tree,
                        buckets=ctx.buckets,
                        interleave=ctx.interleave,
                        embedded_list_path=None,
                        hydrate_tree=node.subtree_hydrate_tree.get(sub_name, {}),
                    )
                )

            if orig_gp_alias:
                self._pipeline.append({"$addFields": {gp_path: f"${orig_gp_alias}"}})
                self._pipeline.append(self._project_remove(orig_gp_alias))

    # ---------------------------------------------------------------------
    # Optimization helpers (buckets -> foreign match)
    # ---------------------------------------------------------------------

    def _pop_foreign_match_for_prefix(self, buckets: dict, prefix: str) -> Optional[dict]:
        """
        Pop a bucket for prefix and convert it into a foreign-doc match dict if possible.

        Example:
            bucket: {"author.age": {"$gt": 10}}
            prefix: "author"
            => foreign: {"age": {"$gt": 10}}
        """
        if prefix not in buckets:
            return None
        candidate = buckets[prefix]
        foreign = self._to_foreign_match(candidate, prefix)
        if foreign is None:
            return None
        buckets.pop(prefix, None)
        return foreign

    def _to_foreign_match(self, match: Any, prefix: str) -> Optional[dict]:
        """
        Convert a root match dict into a match relative to the foreign doc.
        Only accepts keys that start with "<prefix>." and only safe operators.
        """
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

    # ---------------------------------------------------------------------
    # Small field-shape helpers
    # ---------------------------------------------------------------------

    @staticmethod
    def _project_remove(*paths: str) -> dict:
        """Return a $project stage that removes listed fields."""
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
        (Kept for future optimization; not currently required by the pipeline.)
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

    # ---------------------------------------------------------------------
    # Reference extraction & hydration expressions
    # ---------------------------------------------------------------------

    @staticmethod
    def _build_ref_ids_expr(field, source_expr):
        """
        Produce an expression returning an array of referenced _ids from a field that may be:
          - scalar ReferenceField (ObjectId or DBRef)
          - list nested structures (ListField / DictField / MapField)
        """
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
        """
        MongoDB 4.2-safe hydration expression for ReferenceField / ListField / DictField shapes.

        IMPORTANT:
          - If reference is missing, we return an explicit marker:
                {"_missing_reference": True, "_ref": <ObjectId>}
            This ensures MongoEngine dereferencing can raise DoesNotExist when accessed.

          - For ListField/DictField, we apply recursively.
        """
        from mongoengine.fields import ReferenceField, ListField, DictField, GenericReferenceField

        # ---- ReferenceField ----
        if isinstance(field, ReferenceField):
            id_expr = f"{source_expr}.$id" if field.dbref else source_expr

            docs_arr = {"$cond": [{"$isArray": docs_expr}, docs_expr, []]}
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
                                    {"$arrayElemAt": ["$$docs", "$$idx"]},
                                    {"_missing_reference": True, "_ref": "$$refId"},
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
                    {"$map": {"input": source_expr, "as": "item",
                              "in": StageBuilder._build_value_expr(field.field, "$$item", docs_expr)}},
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
                        "in": {"k": "$$kv.k", "v": StageBuilder._build_value_expr(field.field, "$$kv.v", docs_expr)},
                    }
                }
            }

        return source_expr

    # ---------------------------------------------------------------------
    # Foreign-match translation to $expr for local filtering
    # ---------------------------------------------------------------------

    @staticmethod
    def _foreign_match_to_expr(match: Any, var: str = "$$d") -> Optional[dict]:
        """
        Convert a foreign-doc match dict (keys relative to the foreign doc) into an $expr condition
        usable inside $filter.cond.

        Supported:
          - scalar equality
          - ops: $eq,$ne,$gt,$gte,$lt,$lte,$in,$nin
          - $and/$or/$nor recursively
          - $regex (+ $options) via $regexMatch

        Rejected (returns None):
          - $expr/$where/$function anywhere
          - unknown operators
          - $exists
        """
        if not isinstance(match, dict):
            return None

        for bad in ("$expr", "$where", "$function"):
            if bad in match:
                return None

        def field_expr(field_path: str, predicate: Any) -> Optional[dict]:
            path = f"{var}.{field_path}" if field_path else var

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

    # ---------------------------------------------------------------------
    # Structured Reference lookups
    # ---------------------------------------------------------------------

    def _add_structured_ref_lookup(
            self,
            target_cls,
            field_shape,
            local_field: str,
            foreign_match: Optional[dict] = None,
            hydrate: bool = False,
    ):
        """
        Lookup referenced docs for a field shape that contains ReferenceField leaves.

        Behavior:
          - Always does ONE unfiltered lookup by refIds (for correct hydration).
          - If foreign_match is provided:
              - Prefer local root filtering using $filter/$expr (keeps docs array unfiltered)
              - If cannot translate safely, fallback to filtered lookup and match non-empty.
          - If hydrate=True: rewrite local_field using _build_value_expr (missing refs -> marker).
          - Always removes temporary docs array.
        """
        if not target_cls:
            return

        safe = local_field.replace(".", "_")
        docs_alias = f"{safe}__docs"

        ref_ids_expr = self._build_ref_ids_expr(field_shape, f"${local_field}")
        base_pipeline = [{"$match": {"$expr": {"$in": ["$_id", "$$refIds"]}}}]

        # 1) Always unfiltered lookup
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

        # 2) Local root filtering (preferred)
        if foreign_match:
            cond = self._foreign_match_to_expr(foreign_match, var="$$d")
            if cond is not None:
                self._pipeline.append(
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$size": {"$filter": {"input": f"${docs_alias}", "as": "d", "cond": cond}}},
                                    0,
                                ]
                            }
                        }
                    }
                )
            else:
                # Fallback: push down match to lookup (may shrink docs and affect hydration)
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

        # 3) Hydrate
        if hydrate:
            transformed_expr = self._build_value_expr(field_shape, f"${local_field}", f"${docs_alias}")
            self._pipeline.append({"$addFields": {local_field: transformed_expr}})

        # cleanup
        self._pipeline.append({"$project": {docs_alias: 0}})

    def _add_embedded_list_structured_ref_lookup(
            self,
            target_cls,
            field_shape,
            list_path: str,
            embedded_key: str,
            foreign_match: Optional[dict] = None,
            hydrate: bool = True,
    ):
        """
        Like _add_structured_ref_lookup, but the reference lives inside a list of embedded docs.

        Key behavior:
          - Builds refIds by reducing across list items.
          - Performs ONE unfiltered lookup (for correct hydration).
          - If foreign_match exists, filters root docs by joined docs satisfying the condition.
          - If hydrate=True, rewrites embedded_key values per item:
              - missing refs => marker dict (includes _cls for better debugging).
        """
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

        # 1) Unfiltered lookup
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

        # 2) Local root filtering
        if foreign_match:
            cond = self._foreign_match_to_expr(foreign_match, var="$$d")
            if cond is not None:
                self._pipeline.append(
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$size": {"$filter": {"input": f"${docs_alias}", "as": "d", "cond": cond}}},
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

        # 3) Hydrate embedded items
        if hydrate:
            docs_arr = {"$cond": [{"$isArray": f"${docs_alias}"}, f"${docs_alias}", []]}
            ids_arr = {"$map": {"input": docs_arr, "as": "d", "in": "$$d._id"}}

            self._pipeline.append(
                {
                    "$addFields": {
                        list_path: {
                            "$cond": [
                                {"$isArray": f"${list_path}"},
                                {
                                    "$let": {
                                        "vars": {"docs": docs_arr, "ids": ids_arr},
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
                                                                                                            "Unknown"),
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

    # ---------------------------------------------------------------------
    # MapField(ReferenceField) (kept for compatibility; not used by handlers now)
    # ---------------------------------------------------------------------

    def _add_map_ref_lookup(self, target_cls, map_field, local_field: str, foreign_match: Optional[dict] = None):
        """
        Older map lookup helper; retained for compatibility.
        Newer code routes MapField(ReferenceField) through _add_structured_ref_lookup directly.
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

    # ---------------------------------------------------------------------
    # DictField(GenericReferenceField) support
    # ---------------------------------------------------------------------

    def _add_object_generic_lookup(
            self,
            generic_field,
            local_field: str,
            foreign_match: Optional[dict] = None,
            hydrate: bool = False,
    ):
        """
        DictField(GenericReferenceField) support.

        - Always does lookups into each choice collection based on ids in dict values.
        - If foreign_match: filters root docs (keeps old behavior).
        - If hydrate: rewrites dict values into hydrated document dicts (merge doc + _ref/_cls).
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
                {"$lookup": {"from": cls._get_collection_name(), "let": {"refIds": ref_ids_expr}, "pipeline": pipeline,
                             "as": alias}}
            )

        if foreign_match:
            self._pipeline.append({"$match": {"$or": [{a: {"$ne": []}} for a in aliases]}})

        if hydrate:
            value_expr = self._generic_value_transform_expr(doc_classes, alias_for_cls=alias_for, val_var="$$kv.v")

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

    # ---------------------------------------------------------------------
    # GenericReferenceField helpers
    # ---------------------------------------------------------------------

    @staticmethod
    def _missing_generic_expr(ref_expr, cls_expr):
        return {"_missing_reference": True, "_ref": ref_expr, "_cls": cls_expr}

    @staticmethod
    def _generic_value_transform_expr(doc_classes, alias_for_cls, val_var="$$val"):
        """
        Build nested $cond expression that hydrates a GenericReferenceField value
        based on its _cls discriminator.
        """
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
                            {"$mergeObjects": [{"$first": "$$matches"},
                                               {"_ref": f"{val_var}._ref", "_cls": f"{val_var}._cls"}]},
                            StageBuilder._missing_generic_expr(f"{val_var}._ref", f"{val_var}._cls"),
                        ]
                    },
                }
            }

            expr = {"$cond": [class_test, branch, expr]}
        return expr

    # ---------------------------------------------------------------------
    # Embedded list GenericReferenceField support
    # ---------------------------------------------------------------------

    def _add_embedded_list_generic_lookup(
            self,
            generic_field,
            list_path: str,
            embedded_key: str,
            foreign_match: Optional[dict] = None,
            hydrate: bool = True,
    ):
        """
        GenericReferenceField inside a list of embedded documents.

        - Performs unfiltered lookups per choice collection (for correct hydration).
        - If foreign_match:
            - prefer local root filtering using $filter cond conversion
            - fallback to filtered match lookups.
        - If hydrate:
            - rewrite each embedded element's embedded_key value(s) into hydrated docs + _ref/_cls,
              missing => {"_missing_reference": True, "_ref": ..., "_cls": ...}
        """
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
                                                    "input": {"$filter": {"input": "$$this", "as": "m",
                                                                          "cond": class_test_m}},
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

        # 1) Unfiltered lookups
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

        # 2) Root filtering
        match_aliases = []
        if foreign_match:
            cond = self._foreign_match_to_expr(foreign_match, var="$$d")
            if cond is not None:
                self._pipeline.append(
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$gt": [{"$size": {
                                        "$filter": {"input": f"${alias_docs(cls)}", "as": "d", "cond": cond}}}, 0]}
                                    for cls in doc_classes
                                ]
                            }
                        }
                    }
                )
            else:
                # fallback to filtered lookups
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

        # 3) Hydrate
        if hydrate:
            def vbase(cls):
                n = cls.__name__
                return n[:1].lower() + n[1:]  # must start with the lowercase

            docs_vars = {}
            for cls in doc_classes:
                vb = vbase(cls)
                docs_vars[f"{vb}Docs"] = {"$cond": [{"$isArray": f"${alias_docs(cls)}"}, f"${alias_docs(cls)}", []]}

            ids_vars = {}
            for cls in doc_classes:
                vb = vbase(cls)
                ids_vars[f"{vb}Ids"] = {"$map": {"input": f"$${vb}Docs", "as": "d", "in": "$$d._id"}}

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
                                "idx": {"$indexOfArray": [ids_var, f"{val_expr}._ref.$id"]},
                            },
                            "in": {
                                "$cond": [
                                    {"$gte": ["$$idx", 0]},
                                    {"$mergeObjects": [{"$arrayElemAt": [docs_var, "$$idx"]},
                                                       {"_ref": f"{val_expr}._ref", "_cls": f"{val_expr}._cls"}]},
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
                                                                            {"$map": {"input": f"$$it.{embedded_key}",
                                                                                      "as": "val",
                                                                                      "in": hydrate_one_value(
                                                                                          "$$val")}},
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

        self._pipeline.append(self._project_remove(*(docs_aliases + match_aliases)))

    # ---------------------------------------------------------------------
    # Existing scalar generic lookup (kept)
    # ---------------------------------------------------------------------

    def _add_generic_lookup(self, field, local_field, is_list=False):
        """
        Existing GenericReferenceField hydration logic (kept as-is).

        Note:
          - For scalar generic fields, this hydrates always (historical behavior).
          - For list generic fields, it hydrates each item.
        """
        doc_classes = Schema.resolve_generic_choices(field)
        if not doc_classes:
            return

        def alias_for(cls):
            return f"{local_field}__{cls.__name__}"

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

            transformed = self._generic_value_transform_expr(doc_classes, alias_for_cls=alias_for,
                                                             val_var=f"${local_field}")
            self._pipeline.append({"$addFields": {local_field: transformed}})
            self._pipeline.append(self._project_remove(*[alias_for(cls) for cls in doc_classes]))
            return

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

        item_expr = self._generic_value_transform_expr(doc_classes, alias_for_cls=alias_for, val_var="$$item")
        self._pipeline.append(
            {"$addFields": {local_field: {"$map": {"input": f"${local_field}", "as": "item", "in": item_expr}}}})
        self._pipeline.append(self._project_remove(*[alias_for(cls) for cls in doc_classes]))

    # ---------------------------------------------------------------------
    # Abstract DBRef lookup
    # ---------------------------------------------------------------------

    @staticmethod
    def _concrete_subclasses(doc_cls):
        """Return all non-abstract subclasses (recursive) of an abstract base Document."""
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
                                                                        "input":
                                                                            {"$mergeObjects": ["$$doc",
                                                                                               {"id": "$$doc._id",
                                                                                                "_cls": cls_name}]},
                                                                        "value": "$$REMOVE",
                                                                    }
                                                                },
                                                                "$$v",
                                                            ]
                                                        },
                                                        # ObjectId storage
                                                        {
                                                            "$setField": {
                                                                "field": "_id",
                                                                "input":
                                                                    {"$mergeObjects": ["$$doc", {"id": "$$doc._id",
                                                                                                 "_cls": cls_name}]},
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
