# MongoEngine Aggregation Pipeline Architecture

## Architecture Overview

```
pipeline/
├── normalizer.py       # Normalize user query (regex, $where, etc.)
├── match_planner.py    # Decide WHERE each match can safely run
├── lookup_planner.py   # Decide WHICH lookups are required
├── stage_builder.py    # Emit $lookup / $addFields / $match stages
├── tail_builder.py     # Emit terminal stages ($sort/$skip/$limit/$project)
├── pipeline_builder.py # Orchestrator (very small)
```

### Mental Model

1. **Normalizer**
    - Input: raw queryset query
    - Output: MongoDB-safe query

2. **MatchPlanner**
    - Buckets filters by dereference depth
    - Ensures missing references never match

3. **LookupPlanner**
    - Determines lookup tree from queries + select_related

4. **StageBuilder**
    - Emits actual MongoDB aggregation stages
    - Interleaves lookups with safe `$match`

5. **TailBuilder**
    - Applies final shaping stages
    - Always runs last

## Why This Matters

MongoDB aggregation pipelines are **order-sensitive**.
This design makes ordering explicit and safe by construction.

If a file grows too large, it means responsibility is leaking.
