# Async Aggregate Fix Documentation

## Problem
`async_aggregate()` was returning a coroutine instead of an async iterator, causing `TypeError: 'async for' requires an object with __aiter__ method, got coroutine` when trying to use it directly with `async for`.

## Solution
Implemented `AsyncAggregationIterator` wrapper class that provides the async iterator interface, allowing direct use with `async for`.

### Implementation Details

1. **New Class**: `AsyncAggregationIterator` in `mongoengine/async_iterators.py`
   - Implements `__aiter__` and `__anext__` methods
   - Lazy execution - aggregation pipeline runs on first iteration
   - Handles all QuerySet options (filters, skip, limit, ordering)

2. **Modified Method**: `async_aggregate()` in `BaseQuerySet`
   - Changed from async function to regular function
   - Returns `AsyncAggregationIterator` instance instead of coroutine
   - No breaking changes for the API

### Usage

**Before (would cause error):**
```python
# This would fail with TypeError
async for poi in Model.objects.async_aggregate(pipeline):
    process(poi)
```

**After (works correctly):**
```python
# Now works directly with async for
async for poi in Model.objects.async_aggregate(pipeline):
    process(poi)
```

### Examples

```python
# Basic aggregation
pipeline = [
    {"$match": {"status": "active"}},
    {"$group": {"_id": "$category", "count": {"$sum": 1}}}
]

async for result in TravelPoiBounds.objects.async_aggregate(pipeline):
    print(f"Category: {result['_id']}, Count: {result['count']}")

# With QuerySet operations
async for result in (
    Model.objects
    .filter(active=True)
    .order_by("-created")
    .limit(10)
    .async_aggregate(pipeline)
):
    process(result)

# Complex pipeline with $lookup
pipeline = [
    {
        "$lookup": {
            "from": "authors",
            "localField": "author_id",
            "foreignField": "_id",
            "as": "author_details"
        }
    },
    {"$unwind": "$author_details"},
    {"$project": {
        "title": 1,
        "author_name": "$author_details.name"
    }}
]

async for book in Book.objects.async_aggregate(pipeline):
    print(f"{book['title']} by {book['author_name']}")
```

### Benefits

1. **Intuitive API**: Works like other async iterables in Python
2. **Lazy Execution**: Pipeline only executes when iteration starts
3. **Memory Efficient**: Results are streamed, not loaded all at once
4. **Backward Compatible**: Existing code structure maintained

### Migration

No migration needed - the new implementation works with the natural async for pattern that users expect.
