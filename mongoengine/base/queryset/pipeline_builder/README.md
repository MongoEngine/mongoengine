# MongoEngine Aggregation Pipeline Builder

## Overview

This package translates a MongoEngine `QuerySet` into a MongoDB aggregation pipeline. It is the engine behind
`select_related` and join-based filtering on `ReferenceField`, `GenericReferenceField`, `ListField(ReferenceField)`,
`MapField(ReferenceField)`, `DictField(ReferenceField)`, and nested combinations thereof.

The public entry point is `needs_aggregation(queryset)` in `utils.py`. Callers check it first; only when it returns
`True` is `PipelineBuilder(queryset).build()` invoked.

## File Map

```
pipeline_builder/
├── utils.py            # needs_aggregation() — gate: should we use aggregation at all?
├── normalizer.py       # QueryNormalizer — sanitize raw query before planning
├── match_planner.py    # MatchPlanner — bucket filter fragments by required lookup prefix
├── lookup_planner.py   # LookupPlanner — build the lookup tree from buckets + select_related
├── stage_builder.py    # StageBuilder — emit $lookup / $addFields / $match stages
├── tail_builder.py     # TailBuilder — emit terminal stages ($project/$sort/$skip/$limit)
├── schema.py           # Schema — shared field-introspection helpers (no I/O)
└── pipeline_builder.py # PipelineBuilder — thin orchestrator wiring the above together
```

## Data Flow

```
QuerySet
  │
  ├─ needs_aggregation()          # utils.py — bail out early if no joins needed
  │
  └─ PipelineBuilder.build()
       │
       ├─ 1. QueryNormalizer.normalize()
       │       regex objects → {$regex}
       │       $where JS → $function expression (extracted separately)
       │
       ├─ 2. MatchPlanner.bucket()
       │       splits the query into {prefix → fragment} buckets
       │       prefix "" = safe to run before any $lookup
       │       prefix "field.nested" = must run after that $lookup
       │
       ├─ 3. root $match (bucket[""])
       │
       ├─ 4. LookupPlanner.plan()
       │       merges bucket prefixes + select_related into a field-name tree
       │       e.g. {"items": {"parent": {}}, "parent": {"gp": {}}}
       │
       ├─ 5. StageBuilder.emit()   (interleave=True when filters exist)
       │       walks the lookup tree, emitting $lookup stages
       │       interleaves $match stages for each bucket immediately after
       │       the $lookup that makes the filter safe to apply
       │
       ├─ 6. leftover bucket fragments → trailing $match
       │
       ├─ 7. $where function_expr → trailing $match (must be last)
       │
       └─ 8. TailBuilder.build()
               $project (only/exclude) → $sort → $skip → $limit
```

### Two build paths in `PipelineBuilder.build()`

**Pure `select_related` (no filter query):** `StageBuilder.emit()` is called with `interleave=False` and `buckets=None`.
No `$match` stages are interleaved; the pipeline is purely `$lookup` + `$addFields` + tail.

**Filtered query (with or without `select_related`):** `StageBuilder.emit()` is called with `interleave=True` and the
bucket dict. `$match` stages are inserted immediately after each `$lookup` that satisfies the predicate's required
prefix.

## Component Details

### `utils.py` — `needs_aggregation(queryset)`

Inspects the queryset's `_query`, `_ordering`, and projected fields to decide whether any of them touch a
`ReferenceField` (or related) deep enough to require a join. Returns `True` if the queryset must use an aggregation
pipeline instead of a plain `find()`.

Triggers aggregation if:

- A filter key path crosses a `ReferenceField` or `ListField(ReferenceField)`
- An ordering key crosses a reference boundary
- Any projected field is (or contains) a `ReferenceField`, `GenericReferenceField` (with choices),
  `MapField(ReferenceField)`, or `DictField(ReferenceField)`
- An `EmbeddedDocumentField` contains any of the above (checked recursively, cycle-safe)

### `normalizer.py` — `QueryNormalizer`

Runs before any planning. Two transforms:

- **Regex conversion**: Python `re.Pattern` objects → `{"$regex": ..., "$options": ...}`. Aggregation pipelines don't
  accept raw Python regex objects.
- **`$where` extraction**: Converts `$where: "function() { ... }"` into a `$function` expression against `$$ROOT`, with
  `this` rewritten to `doc`. The cleaned query (without `$where`) is returned separately from the function expression;
  the function expression is appended as the very last `$match` because `$function` can only run after all lookups.

### `match_planner.py` — `MatchPlanner`

Buckets filter fragments by the db-field dotted path of the shallowest required `$lookup`.

- `bucket(doc_cls, query)` → `dict[prefix, fragment]`
    - `""` prefix = filter is safe before any `$lookup` (pure scalar predicates)
    - `"field"` prefix = filter requires the `$lookup` on that field to have run first
    - `"field.nested"` prefix = requires nested lookup
- Handles logical operators (`$and`, `$or`, `$nor`) by recursing per-clause and merging.
- **Filter-only policy**: `MatchPlanner` never rewrites predicates into `$expr` forms. Nested list / `MapField` /
  `DictField` predicates stay as plain filters — rewriting them would require hydrated subdocuments, which aren't
  guaranteed unless `select_related` was used.

### `lookup_planner.py` — `LookupPlanner`

Builds a field-name tree describing which lookups must be performed.

- `plan(doc_cls, select_related, bucket_prefixes)` → tree dict
    - Bucket-prefix-derived nodes come **first** (filter lookups precede hydration lookups)
    - `select_related` nodes are merged in **after** (hydration runs after filtering)
- `_tree_from_db_prefix`: converts a db-field dotted prefix back to Python field names by walking `doc_cls._fields`,
  following `ReferenceField` into the referenced class and `GenericReferenceField` into its representative choice class.
- `_merge_tree`: deep-merges two trees (union of all paths).

### `schema.py` — `Schema`

Shared, stateless field-introspection helpers used by `MatchPlanner`, `LookupPlanner`, and `StageBuilder`. Keeps
schema-walking logic in one place.

| Method                                   | Purpose                                                                            |
|------------------------------------------|------------------------------------------------------------------------------------|
| `resolve_field_name(doc_cls, db_part)`   | Resolve a db_field segment to `(python_name, field_obj)`                           |
| `unwrap_list_leaf(field)`                | Strip `ListField` wrappers to reach the inner leaf field                           |
| `unwrap_list_field(field)`               | Same, also returns nesting depth                                                   |
| `resolve_generic_choices(generic_field)` | Resolve `GenericReferenceField.choices` strings/classes → live document classes    |
| `cls_regex(cls)`                         | Build `^ClassName(\.\|$)` regex string for `_cls` matching                         |
| `regex_match(input_expr, cls)`           | Build a `{$regexMatch: ...}` expression for class filtering                        |
| `is_list_of_embedded(field)`             | True if field is `EmbeddedDocumentListField` or `ListField(EmbeddedDocumentField)` |
| `embedded_doc_type(field)`               | Extract the embedded document class from an embedded field or list of embedded     |

### `stage_builder.py` — `StageBuilder`

Emits the actual MongoDB aggregation stages by walking the lookup tree recursively.

**Key behaviors:**

- **`ReferenceField`**: Uses a `$lookup` with `let`/`pipeline` (joining on `_id`). If the field stores a DBRef, extracts
  `.$id` first.
- **`ListField(ReferenceField)`**: Collects all IDs from the array, performs one batch `$lookup`, then optionally
  hydrates via `$map` + `$indexOfArray`.
- **`GenericReferenceField`** (scalar/list): Emits one `$lookup` per `choices` class, filtered by `_cls` regex match.
  Only supported when `choices` is set.
- **`MapField(ReferenceField)` / `DictField(ReferenceField)`**: Collects values via `$objectToArray`, batches IDs,
  performs one `$lookup`, optionally hydrates back via `$arrayToObject`.
- **`EmbeddedDocumentField`**: Descends schema without emitting a `$lookup` (embedded docs live in the parent document).
- **`EmbeddedDocumentListField`** / **`ListField(EmbeddedDocumentField)`**: Descends with `embedded_list_path` set,
  which changes how ref IDs are collected and how hydration is written back (via `$map` + `$mergeObjects`).
- **Abstract document classes**: When a `ReferenceField` points to an abstract class, fans out to a `$lookup` per
  concrete subclass.

**`interleave` + `foreign_match` pattern:**

When `interleave=True`, after emitting a `$lookup` the builder checks whether the next bucket matches that prefix. If
so, it tries to push the filter _inside_ the `$lookup` pipeline as a `$filter` expression (translating MQL predicates to
`$expr` equivalents). This avoids materializing documents that will be discarded by the filter. If the filter can't be
safely translated (e.g. contains `$exists`, `$expr`), a fallback second `$lookup` with the `$match` appended to its
pipeline is used instead.

**`preserve_orig` pattern:**

When `StageBuilder` needs to traverse _through_ a reference field to reach deeper refs (e.g. `parent.grandparent`) but
must **not** hydrate the intermediate field itself, it:

1. Stashes the original value in a temp alias (`__orig__<path>`)
2. Emits the intermediate `$lookup` (used only for filtering/traversal)
3. Restores the original value from the stash after the nested lookups are done
4. Removes the temp alias with `$project: {alias: 0}`

### `tail_builder.py` — `TailBuilder`

Appends terminal stages in fixed order — always last, never interleaved:

1. `$project` — from `only()`/`exclude()` loaded-fields spec (always includes `_id`)
2. `$sort`
3. `$skip`
4. `$limit`

### `pipeline_builder.py` — `PipelineBuilder`

Thin orchestrator. Instantiates one of each component and calls them in order (see Data Flow above). Contains no
field-walking logic itself.

## Design Invariants

**Missing reference marker**

When a referenced document cannot be found in the joined collection, `StageBuilder` writes a sentinel dict instead of
`null`:

```python
{"_missing_reference": True, "_ref": < ObjectId >}
```

For `ReferenceField` this marker must **not** include `"_cls"`, otherwise `MapField(ReferenceField).__get__` may
misidentify it as a `GenericReferenceField` wrapper. For `GenericReferenceField` the marker _does_ include `"_cls"`.

**`GenericReferenceField` requires `choices`**

`GenericReferenceField` without `choices` set is skipped entirely — there is no way to know which collections to join
without knowing the possible document types.

**Aggregation pipeline ordering is critical**

MongoDB aggregation is strictly ordered. The five-phase split (normalizer → match planner → lookup planner → stage
builder → tail builder) makes ordering explicit and safe by construction. `$sort`/`$skip`/`$limit` must never appear
before `$lookup` stages that filter documents.

**Base layer rule**

This package must stay free of all sync/async I/O. It builds queries and emits stage dicts. All cursor/collection I/O
lives in `synchronous/queryset/` or `asynchronous/queryset/`.
