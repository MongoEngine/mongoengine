# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Testing
```bash
# Run all tests (requires MongoDB running locally)
pytest tests/

# Run specific test file or directory
pytest tests/test_connection.py
pytest tests/document/

# Run async tests specifically
pytest tests/test_async_*.py
pytest tests/test_async_document.py -v  # Verbose for specific async test

# Run with coverage
pytest tests/ --cov=mongoengine

# Test multiple Python/PyMongo versions
tox
tox -e py39-mg413  # Specific environment (PyMongo 4.13+ required for async)
```

### Code Quality
```bash
# Run all pre-commit checks (formatting, linting)
.venv/bin/pre-commit run -a

# Individual tools
.venv/bin/black .              # Auto-format
.venv/bin/isort .              # Sort imports
.venv/bin/flake8               # Lint
```

### Documentation
```bash
# Build docs locally
cd docs && make html
# View at docs/_build/html/index.html
```

## Architecture Overview

MongoEngine is an Object-Document Mapper (ODM) for MongoDB, providing a Django-like API for defining and querying documents.

### Core Components

1. **Document Classes** (mongoengine/document.py)
   - `Document`: Base class for collection-backed documents
   - `EmbeddedDocument`: For nested documents without own collection
   - `DynamicDocument`: Flexible schema documents

2. **Fields System** (mongoengine/fields.py)
   - Field types define document schema
   - Complex fields: `ListField`, `DictField`, `ReferenceField`
   - File storage: `FileField`, `ImageField` using GridFS

3. **QuerySet** (mongoengine/queryset/)
   - Chainable query interface similar to Django ORM
   - Lazy evaluation with caching
   - Supports filtering, ordering, aggregation

4. **Connection Management** (mongoengine/connection.py)
   - `connect()`: Synchronous connections
   - `connect_async()`: Async connections (new, requires PyMongo 4.13+)
   - Multi-database support via aliases

5. **Async Support** (NEW - Fork Feature)
   - All major operations have `async_` prefixed methods
   - Separate async connection required via `connect_async()`
   - Core files: `async_utils.py`, `async_context_managers.py`
   - Requires PyMongo 4.13+ for async functionality

### Key Design Patterns

- **Field Descriptors**: Fields use Python descriptors for attribute access
- **Lazy Loading**: QuerySets evaluate only when needed
- **Signal System**: Pre/post save/delete hooks via blinker
- **Inheritance**: Document classes support single-table and multi-collection inheritance

### Testing Approach

- Tests require local MongoDB instance (default port 27017)
- Use pytest-asyncio for async tests
- Test files mirror source structure in tests/
- CI tests against MongoDB 3.6-8.0 and PyMongo 3.12-4.13

### Development Notes

- Python 3.9+ required
- Maintain backward compatibility for all changes
- Async methods must have `async_` prefix
- Pre-commit hooks enforce code style (black, isort, flake8)
- All new features need tests and documentation
- Common pre-commit issues:
  - E722: Don't use bare except, use `except Exception:` instead
  - F401: Remove unused imports
  - Run `.venv/bin/pre-commit run -a` before committing

## Async Feature Details (Fork-specific)

### Async Connection Setup
```python
# Basic async connection
await connect_async('mydb', alias='async_db')

# With options
await connect_async(
    db='mydb',
    host='mongodb://localhost:27017',
    alias='async_db',
    maxPoolSize=50
)

# Check connection type
from mongoengine import is_async_connection
is_async = is_async_connection('async_db')  # True

# Disconnect single connection
await disconnect_async('async_db')

# Disconnect all async connections
await disconnect_all_async()
```

### Async Document Operations
```python
# All document operations have async_ versions
class User(Document):
    name = StringField()
    email = EmailField()
    meta = {'db_alias': 'async_db'}  # Use async connection

# Create and save
user = User(name="John", email="john@example.com")
await user.async_save()

# Query operations
user = await User.objects.async_get(name="John")
users = await User.objects.filter(active=True).async_to_list()
count = await User.objects.async_count()
exists = await User.objects.filter(name="John").async_exists()

# Update
await user.async_update(inc__login_count=1)
await User.objects.filter(active=False).async_update(set__active=True)

# Delete
await user.async_delete()
await User.objects.filter(created__lt=cutoff_date).async_delete()

# Reload from database
await user.async_reload()
```

### Async QuerySet Iteration
```python
# Async iteration support
async for user in User.objects.filter(active=True):
    await process_user(user)

# Convert to list
users = await User.objects.filter(active=True).async_to_list()
```

### Async Reference Handling
```python
class Post(Document):
    author = ReferenceField(User)
    meta = {'db_alias': 'async_db'}

# References return AsyncReferenceProxy
post = await Post.objects.async_first()
# Must explicitly fetch reference
author = await post.author.async_fetch()

# LazyReferenceField works similarly
class Comment(Document):
    post = LazyReferenceField(Post)
    meta = {'db_alias': 'async_db'}

comment = await Comment.objects.async_first()
post = await comment.post.async_fetch()
```

### Async Context Managers
```python
# Transaction support
from mongoengine.async_context_managers import async_run_in_transaction

async with async_run_in_transaction():
    await user1.async_save()
    await user2.async_save()
    # Commits on success, rolls back on error

# Switch database temporarily
from mongoengine.async_context_managers import async_switch_db

async with async_switch_db(User, 'other_db'):
    users = await User.objects.async_to_list()  # Uses 'other_db'

# Disable dereferencing
from mongoengine.async_context_managers import async_no_dereference

async with async_no_dereference(User):
    user = await User.objects.async_first()
    # user.referenced_field remains as DBRef
```

### Async GridFS Operations
```python
class Photo(Document):
    image = FileField()
    meta = {'db_alias': 'async_db'}

# Save file
photo = Photo()
with open('image.jpg', 'rb') as f:
    await photo.image.async_put(f, filename='image.jpg')
await photo.async_save()

# Read file
photo = await Photo.objects.async_first()
content = await photo.image.async_read()

# Delete file
await photo.image.async_delete()
```

### Important Async Patterns

1. **Connection Isolation**: Never mix sync/async operations
   ```python
   # WRONG - will raise RuntimeError
   await connect_async('mydb', alias='async_db')
   User.objects.get(name="John")  # Sync operation on async connection

   # CORRECT
   await connect_async('mydb', alias='async_db')
   await User.objects.async_get(name="John")
   ```

2. **Explicit Dereferencing**: References must be fetched explicitly
   ```python
   # Sync automatically dereferences
   post = Post.objects.first()
   print(post.author.name)  # Works

   # Async requires explicit fetch
   post = await Post.objects.async_first()
   author = await post.author.async_fetch()  # Required
   print(author.name)
   ```

3. **Concurrent Operations**: Leverage asyncio for parallel queries
   ```python
   # Concurrent queries
   import asyncio

   users, posts, comments = await asyncio.gather(
       User.objects.async_to_list(),
       Post.objects.filter(published=True).async_to_list(),
       Comment.objects.async_count()
   )
   ```

### Testing Async Code
```python
# Use pytest-asyncio
import pytest
import pytest_asyncio
from mongoengine import connect_async, disconnect_async

@pytest_asyncio.fixture
async def async_db():
    await connect_async('test_db', alias='test')
    yield
    await disconnect_async('test')

@pytest.mark.asyncio
async def test_async_save(async_db):
    user = User(name="Test")
    await user.async_save()

    fetched = await User.objects.async_get(name="Test")
    assert fetched.name == "Test"
```

### Async Select Related (NEW)
```python
# Dereference references in async context
posts = await Post.objects.async_select_related()
# Now post.author is the actual Author document, not AsyncReferenceProxy

# With max_depth for nested references
comments = await Comment.objects.async_select_related(max_depth=2)
# comment.post and comment.author are dereferenced
# Note: Deeply nested refs (comment.post.author) may still be AsyncReferenceProxy
```

### Async Aggregation (NEW)
```python
# Direct async for usage with aggregation
pipeline = [
    {"$match": {"status": "active"}},
    {"$group": {"_id": "$category", "count": {"$sum": 1}}}
]

# Works directly with async for
async for result in Model.objects.async_aggregate(pipeline):
    print(f"Category: {result['_id']}, Count: {result['count']}")

# With QuerySet operations
async for doc in Model.objects.filter(active=True).order_by("-created").async_aggregate(pipeline):
    process(doc)
```

### Async Limitations

1. **No bulk insert**: Use loop with `async_save()`
2. **JavaScript execution deprecated**: `async_exec_js()` may not work with newer MongoDB
3. **Cursor caching**: Async cursors can't be cached like sync ones

### Migration Guidelines

1. **Gradual adoption**: Keep sync code, add async where needed
2. **Separate connections**: Use different aliases for sync/async
3. **Test thoroughly**: Async behavior may differ subtly
4. **Monitor performance**: Async shines with concurrent I/O, not CPU-bound tasks
