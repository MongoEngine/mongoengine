# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MongoEngine is a Python Object-Document Mapper (ODM) for MongoDB. It provides a declarative API similar to Django's ORM but designed for document databases.

## Common Development Commands

### Testing
```bash
# Run all tests (requires MongoDB running locally on default port 27017)
pytest tests/

# Run specific test file or directory
pytest tests/fields/test_string_field.py
pytest tests/document/

# Run tests with coverage
pytest tests/ --cov=mongoengine

# Run a single test
pytest tests/fields/test_string_field.py::TestStringField::test_string_field

# Run tests for all Python/PyMongo versions
tox
```

### Code Quality
```bash
# Run all pre-commit checks
pre-commit run -a

# Auto-format code
black .

# Sort imports
isort .

# Run linter
flake8
```

### Development Setup
```bash
# Install development dependencies
pip install -r requirements-dev.txt
pip install -e .[test]

# Install pre-commit hooks
pre-commit install
```

## Architecture Overview

### Core Components

1. **Document Classes** (mongoengine/document.py):
   - `Document` - Top-level documents stored in MongoDB collections
   - `EmbeddedDocument` - Documents embedded within other documents
   - `DynamicDocument` - Documents with flexible schema
   - Uses metaclasses (`DocumentMetaclass`, `TopLevelDocumentMetaclass`) for class creation

2. **Field System** (mongoengine/fields.py):
   - Fields are implemented as descriptors
   - Common fields: StringField, IntField, ListField, ReferenceField, EmbeddedDocumentField
   - Custom validation and conversion logic per field type

3. **QuerySet API** (mongoengine/queryset/):
   - Chainable query interface similar to Django ORM
   - Lazy evaluation of queries
   - Support for aggregation pipelines
   - Query optimization and caching

4. **Connection Management** (mongoengine/connection.py):
   - Multi-database support with aliasing
   - Connection pooling handled by PyMongo
   - MongoDB URI and authentication support

### Key Design Patterns

- **Metaclass-based document definition**: Document structure is defined at class creation time
- **Descriptor pattern for fields**: Enables validation and type conversion on attribute access
- **Lazy loading**: ReferenceFields can be dereferenced on demand
- **Signal system**: Pre/post hooks for save, delete, and bulk operations
- **Query builder pattern**: Fluent interface for constructing MongoDB queries

### Testing Approach

- Tests mirror the package structure (e.g., `tests/fields/` for field tests)
- Heavy use of fixtures defined in `tests/fixtures.py`
- Tests require a running MongoDB instance
- Matrix testing across Python versions (3.9-3.13) and PyMongo versions

### Code Style

- Black formatting (88 character line length)
- isort for import sorting
- flake8 for linting (max complexity: 47)
- All enforced via pre-commit hooks

## Important Notes

- Always ensure MongoDB is running before running tests
- When modifying fields or documents, check impact on both regular and dynamic document types
- QuerySet modifications often require updates to both `queryset/queryset.py` and `queryset/manager.py`
- New features should include tests and documentation updates
- Backward compatibility is important - avoid breaking changes when possible

## Async Support Development Workflow

When working on async support implementation, follow this workflow:

1. **Branch Strategy**: Create a separate branch for each phase (e.g., `async-phase1-foundation`)
2. **Planning**: Create `PROGRESS_{PHASE_NAME}.md` (e.g., `PROGRESS_FOUNDATION.md`) detailing the work for that phase
3. **PR Creation**: Create a GitHub PR after initial planning commit
4. **Communication**: Use PR comments for questions, blockers, and discussions
5. **Completion Process**:
   - Delete the phase-specific PROGRESS file
   - Update main `PROGRESS.md` with completed work
   - Update `CLAUDE.md` with learnings for future reference
   - Finalize PR for review

### Current Async Implementation Strategy

- **Integrated Approach**: Adding async methods directly to existing Document classes
- **Naming Convention**: Use `async_` prefix for all async methods (e.g., `async_save()`)
- **Connection Detection**: Methods check connection type and raise errors if mismatched
- **Backward Compatibility**: Existing sync code remains unchanged

### Key Design Decisions

1. **No Separate AsyncDocument**: Async methods are added to existing Document class
2. **Explicit Async Methods**: Rather than automatic async/sync switching, use explicit method names
3. **Connection Type Enforcement**: Runtime errors when using wrong method type with connection
4. **Gradual Migration Path**: Projects can migrate incrementally by connection type

### Phase 1 Implementation Learnings

#### Testing Async Code
- Use `pytest-asyncio` for async test support
- Always use `@pytest_asyncio.fixture` instead of `@pytest.fixture` for async fixtures
- Test setup/teardown must be done via fixtures with `yield`, not `setup_method`/`teardown_method`
- Example pattern:
  ```python
  @pytest_asyncio.fixture(autouse=True)
  async def setup_and_teardown(self):
      await connect_async(db="test_db", alias="test_alias")
      Document._meta["db_alias"] = "test_alias"
      yield
      await Document.async_drop_collection()
      await disconnect_async("test_alias")
  ```

#### Connection Management
- `ConnectionType` enum tracks whether connection is SYNC or ASYNC
- Store in `_connection_types` dict alongside `_connections`
- Always check connection type before operations:
  ```python
  def ensure_async_connection(alias):
      if not is_async_connection(alias):
          raise RuntimeError("Connection is not async")
  ```

#### Error Handling Patterns
- Be flexible in error message assertions - messages may vary
- Example: `assert "different connection" in str(exc) or "synchronous connection" in str(exc)`
- Always provide clear error messages guiding users to correct method

#### Implementation Patterns
- Add `_get_db_alias()` classmethod to Document for proper alias resolution
- Use `contextvars` for async context management instead of thread-local storage
- Export new functions in `__all__` to avoid import errors
- For cascade operations with unsaved references, implement step-by-step

#### Common Pitfalls
- Forgetting to add new functions to `__all__` causes ImportError
- Index definitions in meta must use proper syntax: `("-field_name",)` not `("field_name", "-1")`
- AsyncMongoClient.close() must be awaited - handle in disconnect_async properly
- Virtual environment: use `.venv/bin/python -m` directly instead of repeated activation

### Phase 2 Implementation Learnings

#### QuerySet Async Design
- Extended `BaseQuerySet` directly - all subclasses (QuerySet, QuerySetNoCache) inherit async methods automatically
- Async methods follow same pattern as Document: `async_` prefix for all async operations
- Connection type checking at method entry ensures proper usage

#### Async Cursor Management
- AsyncIOMotor cursors require special handling:
  ```python
  # Check if close() is a coroutine before calling
  if asyncio.iscoroutinefunction(cursor.close):
      await cursor.close()
  else:
      cursor.close()
  ```
- Cannot cache async cursors like sync cursors - must create fresh in async context
- Always close cursors in finally blocks to prevent resource leaks

#### Document Creation from MongoDB Data
- `_from_son()` method doesn't accept `only_fields` parameter
- Use `_auto_dereference` parameter instead:
  ```python
  self._document._from_son(
      doc,
      _auto_dereference=self.__auto_dereference,
      created=True
  )
  ```

#### MongoDB Operations
- `count_documents()` doesn't accept None values - filter them out:
  ```python
  if self._skip is not None and self._skip > 0:
      kwargs["skip"] = self._skip
  ```
- Update operations need proper operator handling:
  - Direct field updates should be wrapped in `$set`
  - Support MongoEngine style operators: `inc__field` → `{"$inc": {"field": value}}`
  - Handle nested operators correctly

#### Testing Patterns
- Create comprehensive test documents with references for integration testing
- Test query chaining to ensure all methods work together
- Always test error cases (DoesNotExist, MultipleObjectsReturned)
- Verify `as_pymongo()` mode returns dictionaries not Document instances

#### Performance Considerations
- Async iteration (`__aiter__`) enables efficient streaming of large result sets
- Bulk operations (update/delete) can leverage async for better throughput
- Connection pooling handled automatically by AsyncIOMotorClient

#### Migration Strategy
- Projects can use both sync and async QuerySets in same codebase
- Connection type determines which methods are available
- Clear error messages guide users to correct method usage

### Phase 3 Implementation Learnings

#### ReferenceField Async Design
- **AsyncReferenceProxy Pattern**: In async context, ReferenceField returns a proxy object requiring explicit `await proxy.fetch()`
- This prevents accidental sync operations in async code and makes async dereferencing explicit
- The proxy caches fetched values to avoid redundant database calls

#### Field-Level Async Methods
- Async methods should be on the field class, not on proxy instances:
  ```python
  # Correct - call on field class
  await AsyncFileDoc.file.async_put(file_obj, instance=doc)
  
  # Incorrect - don't call on proxy instance
  await doc.file.async_put(file_obj)
  ```
- This pattern maintains consistency and avoids confusion with instance methods

#### GridFS Async Implementation
- Use PyMongo's native `gridfs.asynchronous` module instead of Motor
- Key imports: `from gridfs.asynchronous import AsyncGridFSBucket`
- AsyncGridFSProxy handles async file operations (read, delete, replace)
- File operations return sync GridFSProxy for storage in document to maintain compatibility

#### LazyReferenceField Enhancement
- Added `async_fetch()` method directly to LazyReference class
- Maintains same caching behavior as sync version
- Works seamlessly with existing passthrough mode

#### Error Handling Patterns
- GridFS operations need careful error handling for missing files
- Stream position management critical for file reads (always seek(0) after write)
- Grid ID extraction from different proxy types requires type checking

#### Testing Async Fields
- Use separate test classes for each field type for clarity
- Test both positive cases and error conditions
- Always clean up GridFS collections in teardown to avoid test pollution
- Verify proxy behavior separately from actual async operations

#### Known Limitations
- **ListField with ReferenceField**: Currently doesn't auto-convert to AsyncReferenceProxy
  - This is a complex case requiring deeper changes to ListField
  - Documented as limitation - users need manual async dereferencing for now
  - Could be addressed in future enhancement

#### Design Decisions
- **Explicit over Implicit**: Async dereferencing must be explicit via `fetch()` method
- **Proxy Pattern**: Provides clear indication when async operation needed
- **Field-Level Methods**: Consistency with sync API while maintaining async safety

### Phase 4 Advanced Features Implementation Learnings

#### Cascade Operations Async Implementation
- **Cursor Handling**: AsyncIOMotor cursors need special iteration - collect documents first, then process
- **Bulk Operations**: Convert Document objects to IDs for PULL operations to avoid InvalidDocument errors
- **Operator Support**: Add new operators like `pull_all` to the QuerySet operator mapping
- **Error Handling**: Be flexible with error message assertions - MongoDB messages may vary between versions

#### Async Transaction Implementation  
- **PyMongo API**: `session.start_transaction()` is a coroutine that must be awaited, not a context manager
- **Session Management**: Use proper async session handling with retry logic for commit operations
- **Error Recovery**: Implement automatic abort on exceptions with proper cleanup in finally blocks
- **Connection Requirements**: MongoDB transactions require replica set or sharded cluster setup

#### Async Context Managers
- **Collection Caching**: Handle both `_collection` (sync) and `_async_collection` (async) attributes separately
- **Exception Safety**: Always restore original state in `__aexit__` even when exceptions occur
- **Method Binding**: Use `@classmethod` wrapper pattern for dynamic method replacement

#### Async Aggregation Framework
- **Pipeline Execution**: `collection.aggregate()` returns a coroutine that must be awaited to get AsyncCommandCursor
- **Cursor Iteration**: Use `async for` with the awaited cursor result
- **Session Integration**: Pass async session to aggregation operations for transaction support
- **Query Integration**: Properly merge queryset filters with aggregation pipeline stages

#### Testing Async MongoDB Operations
- **Fixture Setup**: Always use `@pytest_asyncio.fixture` for async fixtures, not `@pytest.fixture`
- **Connection Testing**: Skip tests gracefully when MongoDB doesn't support required features (transactions, replica sets)
- **Error Message Flexibility**: Use partial string matching for error assertions across MongoDB versions
- **Resource Cleanup**: Ensure all collections are properly dropped in test teardown

#### Regression Prevention
- **EmbeddedDocument Compatibility**: Always check `hasattr(instance, '_get_db_alias')` before calling connection methods
- **Field Descriptor Safety**: Handle cases where descriptors are accessed on non-Document instances
- **Backward Compatibility**: Ensure all existing sync functionality remains unchanged

#### Implementation Quality Guidelines
- **Professional Standards**: All code should be ready for upstream contribution
- **Comprehensive Testing**: Each feature needs multiple test scenarios including edge cases
- **Documentation**: Every public method needs clear docstrings with usage examples
- **Error Messages**: Provide clear guidance to users on proper async/sync method usage
- **Native PyMongo**: Leverage PyMongo's built-in async support rather than external libraries