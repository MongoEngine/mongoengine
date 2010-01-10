=========
Changelog
=========

Changes in v0.2
===============
- Added Q class for building advanced queries
- Added QuerySet methods for atomic updates to documents
- Fields may now specify ``unique=True`` to enforce uniqueness across a collection
- Added option for default document ordering
- Fixed bug in index definitions

Changes in v0.1.3
=================
- Added Django authentication backend
- Added Document.meta support for indexes, which are ensured just before 
  querying takes place
- A few minor bugfixes


Changes in v0.1.2
=================
- Query values may be processed before before being used in queries
- Made connections lazy
- Fixed bug in Document dictionary-style access
- Added BooleanField
- Added Document.reload method


Changes in v0.1.1
=================
- Documents may now use capped collections
