=========
Changelog
=========

Changes in v0.4
===============
- Added ``GridFSStorage`` Django storage backend
- Added ``FileField`` for GridFS support
- New Q-object implementation, which is no longer based on Javascript
- Added ``SortedListField``
- Added ``EmailField``
- Added ``GeoPointField``
- Added ``exact`` and ``iexact`` match operators to ``QuerySet``
- Added ``get_document_or_404`` and ``get_list_or_404`` Django shortcuts
- Added new query operators for Geo queries
- Added ``not`` query operator
- Added new update operators: ``pop`` and ``add_to_set``
- Added ``__raw__`` query parameter
- Added support for custom querysets
- Fixed document inheritance primary key issue
- Added support for querying by array element position
- Base class can now be defined for ``DictField``
- Fixed MRO error that occured on document inheritance
- Added ``QuerySet.distinct``, ``QuerySet.create``, ``QuerySet.snapshot``,
  ``QuerySet.timeout`` and ``QuerySet.all``
- Subsequent calls to ``connect()`` now work
- Introduced ``min_length`` for ``StringField``
- Fixed multi-process connection issue
- Other minor fixes

Changes in v0.3
===============
- Added MapReduce support
- Added ``contains``, ``startswith`` and ``endswith`` query operators (and
  case-insensitive versions that are prefixed with 'i') 
- Deprecated fields' ``name`` parameter, replaced with ``db_field``
- Added ``QuerySet.only`` for only retrieving specific fields
- Added ``QuerySet.in_bulk()`` for bulk querying using ids
- ``QuerySet``\ s now have a ``rewind()`` method, which is called automatically
  when the iterator is exhausted, allowing ``QuerySet``\ s to be reused
- Added ``DictField``
- Added ``URLField``
- Added ``DecimalField``
- Added ``BinaryField``
- Added ``GenericReferenceField``
- Added ``get()`` and ``get_or_create()`` methods to ``QuerySet``
- ``ReferenceField``\ s may now reference the document they are defined on
  (recursive references) and documents that have not yet been defined
- ``Document`` objects may now be compared for equality (equal if _ids are
  equal and documents are of same type)
- ``QuerySet`` update methods now have an ``upsert`` parameter
- Added field name substitution for Javascript code (allows the user to use the
  Python names for fields in JS, which are later substituted for the real field
  names)
- ``Q`` objects now support regex querying
- Fixed bug where referenced documents within lists weren't properly
  dereferenced
- ``ReferenceField``\ s may now be queried using their _id
- Fixed bug where ``EmbeddedDocuments`` couldn't be non-polymorphic
- ``queryset_manager`` functions now accept two arguments -- the document class
  as the first and the queryset as the second
- Fixed bug where ``QuerySet.exec_js`` ignored ``Q`` objects
- Other minor fixes

Changes in v0.2.2
=================
- Fixed bug that prevented indexes from being used on ``ListField``\ s
- ``Document.filter()`` added as an alias to ``Document.__call__()``
- ``validate()`` may now be used on ``EmbeddedDocument``\ s

Changes in v0.2.1
=================
- Added a MongoEngine backend for Django sessions
- Added ``force_insert`` to ``Document.save()``
- Improved querying syntax for ``ListField`` and ``EmbeddedDocumentField``
- Added support for user-defined primary keys (``_id`` in MongoDB)

Changes in v0.2
===============
- Added ``Q`` class for building advanced queries
- Added ``QuerySet`` methods for atomic updates to documents
- Fields may now specify ``unique=True`` to enforce uniqueness across a 
  collection
- Added option for default document ordering
- Fixed bug in index definitions

Changes in v0.1.3
=================
- Added Django authentication backend
- Added ``Document.meta`` support for indexes, which are ensured just before 
  querying takes place
- A few minor bugfixes


Changes in v0.1.2
=================
- Query values may be processed before before being used in queries
- Made connections lazy
- Fixed bug in Document dictionary-style access
- Added ``BooleanField``
- Added ``Document.reload()`` method


Changes in v0.1.1
=================
- Documents may now use capped collections
