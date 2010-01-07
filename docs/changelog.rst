=========
Changelog
=========

Changes is v0.1.3
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
