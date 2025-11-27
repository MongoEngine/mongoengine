# Async Select Related Fix Documentation

## Problem
When using `select_related()` with async QuerySets in MongoEngine, a `TypeError: 'AsyncCursor' object is not an iterator` error occurs because the synchronous `select_related()` method tries to iterate over an `AsyncCursor` using synchronous iteration.

## Solution
We've implemented a new `async_select_related()` method that properly handles async cursors and dereferences.

### Implementation Details

1. **New Method**: `async_select_related(max_depth=1)` in `BaseQuerySet`
2. **New Class**: `AsyncDeReference` in `mongoengine/dereference.py` that handles async operations
3. **New Method**: `async_in_bulk(object_ids)` for efficient bulk fetching in async context

### Usage

Instead of:
```python
# This will fail with TypeError
items = await self.skip(skip).limit(limit + 1).select_related().async_to_list()
```

Use:
```python
# This works correctly
items = await self.skip(skip).limit(limit + 1).async_select_related()
```

### Example

```python
from mongoengine import Document, ReferenceField, StringField, connect_async

class Author(Document):
    name = StringField(required=True)

class Post(Document):
    title = StringField(required=True)
    author = ReferenceField(Author)

class Comment(Document):
    content = StringField(required=True)
    post = ReferenceField(Post)
    author = ReferenceField(Author)

# Connect to async MongoDB
await connect_async('mydb')

# Create data
author = Author(name="John Doe")
await author.async_save()

post = Post(title="Hello World", author=author)
await post.async_save()

# Query with select_related
posts = await Post.objects.async_select_related()
# posts[0].author is now the actual Author document, not a reference

# With nested references (max_depth=2)
comments = await Comment.objects.async_select_related(max_depth=2)
# comments[0].post and comments[0].author are dereferenced
# Note: comments[0].post.author remains an AsyncReferenceProxy
```

### Limitations

1. **Nested References**: With `max_depth > 1`, deeply nested references may still be `AsyncReferenceProxy` objects that require explicit fetching:
   ```python
   # For deeply nested references
   author = await comment.post.author.fetch()
   ```

2. **Performance**: Like the sync version, `async_select_related()` performs additional queries to fetch all referenced documents. Use it when you know you'll need the referenced data.

### Migration from Sync to Async

If you're migrating from synchronous to asynchronous MongoEngine:

1. Replace `select_related()` with `async_select_related()`
2. Replace `in_bulk()` with `async_in_bulk()` if used directly
3. Be aware that async operations return `AsyncReferenceProxy` for references by default

### Testing

The implementation includes comprehensive tests in `tests/test_async_select_related.py` covering:
- Single reference dereferencing
- Multiple reference fields
- Filtering with select_related
- Skip/limit with select_related
- Empty querysets
- Max depth handling
