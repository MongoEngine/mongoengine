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