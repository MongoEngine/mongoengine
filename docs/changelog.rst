=========
Changelog
=========

Changes in 0.7.7
================
- Fix handling for old style _types

Changes in 0.7.6
================
- Unicode fix for repr (MongoEngine/mongoengine#133)
- Allow updates with match operators (MongoEngine/mongoengine#144)
- Updated URLField - now can have a override the regex (MongoEngine/mongoengine#136)
- Allow Django AuthenticationBackends to work with Django user (hmarr/mongoengine#573)
- Fixed reload issue with ReferenceField where dbref=False (MongoEngine/mongoengine#138)

Changes in 0.7.5
================
- ReferenceFields with dbref=False use ObjectId instead of strings (MongoEngine/mongoengine#134)
  See ticket for upgrade notes (https://github.com/MongoEngine/mongoengine/issues/134)

Changes in 0.7.4
================
- Fixed index inheritance issues - firmed up testcases (MongoEngine/mongoengine#123) (MongoEngine/mongoengine#125)

Changes in 0.7.3
================
- Reverted EmbeddedDocuments meta handling - now can turn off inheritance (MongoEngine/mongoengine#119)

Changes in 0.7.2
================
- Update index spec generation so its not destructive (MongoEngine/mongoengine#113)

Changes in 0.7.1
=================
- Fixed index spec inheritance (MongoEngine/mongoengine#111)

Changes in 0.7.0
=================
- Updated queryset.delete so you can use with skip / limit (MongoEngine/mongoengine#107)
- Updated index creation allows kwargs to be passed through refs (MongoEngine/mongoengine#104)
- Fixed Q object merge edge case (MongoEngine/mongoengine#109)
- Fixed reloading on sharded documents (hmarr/mongoengine#569)
- Added NotUniqueError for duplicate keys (MongoEngine/mongoengine#62)
- Added custom collection / sequence naming for SequenceFields (MongoEngine/mongoengine#92)
- Fixed UnboundLocalError in composite index with pk field (MongoEngine/mongoengine#88)
- Updated ReferenceField's to optionally store ObjectId strings
  this will become the default in 0.8 (MongoEngine/mongoengine#89)
- Added FutureWarning - save will default to `cascade=False` in 0.8
- Added example of indexing embedded document fields (MongoEngine/mongoengine#75)
- Fixed ImageField resizing when forcing size (MongoEngine/mongoengine#80)
- Add flexibility for fields handling bad data (MongoEngine/mongoengine#78)
- Embedded Documents no longer handle meta definitions
- Use weakref proxies in base lists / dicts (MongoEngine/mongoengine#74)
- Improved queryset filtering (hmarr/mongoengine#554)
- Fixed Dynamic Documents and Embedded Documents (hmarr/mongoengine#561)
- Fixed abstract classes and shard keys (MongoEngine/mongoengine#64)
- Fixed Python 2.5 support
- Added Python 3 support (thanks to Laine Heron)

Changes in 0.6.20
=================
- Added support for distinct and db_alias (MongoEngine/mongoengine#59)
- Improved support for chained querysets when constraining the same fields (hmarr/mongoengine#554)
- Fixed BinaryField lookup re (MongoEngine/mongoengine#48)

Changes in 0.6.19
=================

- Added Binary support to UUID (MongoEngine/mongoengine#47)
- Fixed MapField lookup for fields without declared lookups (MongoEngine/mongoengine#46)
- Fixed BinaryField python value issue (MongoEngine/mongoengine#48)
- Fixed SequenceField non numeric value lookup (MongoEngine/mongoengine#41)
- Fixed queryset manager issue (MongoEngine/mongoengine#52)
- Fixed FileField comparision (hmarr/mongoengine#547)

Changes in 0.6.18
=================
- Fixed recursion loading bug in _get_changed_fields

Changes in 0.6.17
=================
- Fixed issue with custom queryset manager expecting explict variable names

Changes in 0.6.16
=================
- Fixed issue where db_alias wasn't inherited

Changes in 0.6.15
=================
- Updated validation error messages
- Added support for null / zero / false values in item_frequencies
- Fixed cascade save edge case
- Fixed geo index creation through reference fields
- Added support for args / kwargs when using @queryset_manager
- Deref list custom id fix

Changes in 0.6.14
=================
- Fixed error dict with nested validation
- Fixed Int/Float fields and not equals None
- Exclude tests from installation
- Allow tuples for index meta
- Fixed use of str in instance checks
- Fixed unicode support in transform update
- Added support for add_to_set and each

Changes in 0.6.13
=================
- Fixed EmbeddedDocument db_field validation issue
- Fixed StringField unicode issue
- Fixes __repr__ modifying the cursor

Changes in 0.6.12
=================
- Fixes scalar lookups for primary_key
- Fixes error with _delta handling DBRefs

Changes in 0.6.11
==================
- Fixed inconsistency handling None values field attrs
- Fixed map_field embedded db_field issue
- Fixed .save() _delta issue with DbRefs
- Fixed Django TestCase
- Added cmp to Embedded Document
- Added PULL reverse_delete_rule
- Fixed CASCADE delete bug
- Fixed db_field data load error
- Fixed recursive save with FileField

Changes in 0.6.10
=================
- Fixed basedict / baselist to return super(..)
- Promoted BaseDynamicField to DynamicField

Changes in 0.6.9
================
- Fixed sparse indexes on inherited docs
- Removed FileField auto deletion, needs more work maybe 0.7

Changes in 0.6.8
================
- Fixed FileField losing reference when no default set
- Removed possible race condition from FileField (grid_file)
- Added assignment to save, can now do: `b = MyDoc(**kwargs).save()`
- Added support for pull operations on nested EmbeddedDocuments
- Added support for choices with GenericReferenceFields
- Added support for choices with GenericEmbeddedDocumentFields
- Fixed Django 1.4 sessions first save data loss
- FileField now automatically delete files on .delete()
- Fix for GenericReference to_mongo method
- Fixed connection regression
- Updated Django User document, now allows inheritance

Changes in 0.6.7
================
- Fixed indexing on '_id' or 'pk' or 'id'
- Invalid data from the DB now raises a InvalidDocumentError
- Cleaned up the Validation Error - docs and code
- Added meta `auto_create_index` so you can disable index creation
- Added write concern options to inserts
- Fixed typo in meta for index options
- Bug fix Read preference now passed correctly
- Added support for File like objects for GridFS
- Fix for #473 - Dereferencing abstracts

Changes in 0.6.6
================
- Django 1.4 fixed (finally)
- Added tests for Django

Changes in 0.6.5
================
- More Django updates

Changes in 0.6.4
================

- Refactored connection / fixed replicasetconnection
- Bug fix for unknown connection alias error message
- Sessions support Django 1.3 and Django 1.4
- Minor fix for ReferenceField

Changes in 0.6.3
================
- Updated sessions for Django 1.4
- Bug fix for updates where listfields contain embedded documents
- Bug fix for collection naming and mixins

Changes in 0.6.2
================
- Updated documentation for ReplicaSet connections
- Hack round _types issue with SERVER-5247 - querying other arrays may also cause problems.

Changes in 0.6.1
================
- Fix for replicaSet connections

Changes in 0.6
================

- Added FutureWarning to inherited classes not declaring 'allow_inheritance' as the default will change in 0.7
- Added support for covered indexes when inheritance is off
- No longer always upsert on save for items with a '_id'
- Error raised if update doesn't have an operation
- DeReferencing is now thread safe
- Errors raised if trying to perform a join in a query
- Updates can now take __raw__ queries
- Added custom 2D index declarations
- Added replicaSet connection support
- Updated deprecated imports from pymongo (safe for pymongo 2.2)
- Added uri support for connections
- Added scalar for efficiently returning partial data values (aliased to values_list)
- Fixed limit skip bug
- Improved Inheritance / Mixin
- Added sharding support
- Added pymongo 2.1 support
- Fixed Abstract documents can now declare indexes
- Added db_alias support to individual documents
- Fixed GridFS documents can now be pickled
- Added Now raises an InvalidDocumentError when declaring multiple fields with the same db_field
- Added InvalidQueryError when calling with_id with a filter
- Added support for DBRefs in distinct()
- Fixed issue saving False booleans
- Fixed issue with dynamic documents deltas
- Added Reverse Delete Rule support to ListFields - MapFields aren't supported
- Added customisable cascade kwarg options
- Fixed Handle None values for non-required fields
- Removed Document._get_subclasses() - no longer required
- Fixed bug requiring subclasses when not actually needed
- Fixed deletion of dynamic data
- Added support for the $elementMatch operator
- Added reverse option to SortedListFields
- Fixed dereferencing - multi directional list dereferencing
- Fixed issue creating indexes with recursive embedded documents
- Fixed recursive lookup in _unique_with_indexes
- Fixed passing ComplexField defaults to constructor for ReferenceFields
- Fixed validation of DictField Int keys
- Added optional cascade saving
- Fixed dereferencing - max_depth now taken into account
- Fixed document mutation saving issue
- Fixed positional operator when replacing embedded documents
- Added Non-Django Style choices back (you can have either)
- Fixed __repr__ of a sliced queryset
- Added recursive validation error of documents / complex fields
- Fixed breaking during queryset iteration
- Added pre and post bulk-insert signals
- Added ImageField - requires PIL
- Fixed Reference Fields can be None in get_or_create / queries
- Fixed accessing pk on an embedded document
- Fixed calling a queryset after drop_collection now recreates the collection
- Add field name to validation exception messages
- Added UUID field
- Improved efficiency of .get()
- Updated ComplexFields so if required they won't accept empty lists / dicts
- Added spec file for rpm-based distributions
- Fixed ListField so it doesnt accept strings
- Added DynamicDocument and EmbeddedDynamicDocument classes for expando schemas

Changes in v0.5.2
=================

- A Robust Circular reference bugfix


Changes in v0.5.1
=================

- Fixed simple circular reference bug

Changes in v0.5
===============

- Added InvalidDocumentError - so Document core methods can't be overwritten
- Added GenericEmbeddedDocument - so you can embed any type of embeddable document
- Added within_polygon support - for those with mongodb 1.9
- Updated sum / average to use map_reduce as db.eval doesn't work in sharded environments
- Added where() - filter to allowing users to specify query expressions as Javascript
- Added SequenceField - for creating sequential counters
- Added update() convenience method to a document
- Added cascading saves - so changes to Referenced documents are saved on .save()
- Added select_related() support
- Added support for the positional operator
- Updated geo index checking to be recursive and check in embedded documents
- Updated default collection naming convention
- Added Document Mixin support
- Fixed queryet __repr__ mid iteration
- Added hint() support, so cantell Mongo the proper index to use for the query
- Fixed issue with inconsitent setting of _cls breaking inherited referencing
- Added help_text and verbose_name to fields to help with some form libs
- Updated item_frequencies to handle embedded document lookups
- Added delta tracking now only sets / unsets explicitly changed fields
- Fixed saving so sets updated values rather than overwrites
- Added ComplexDateTimeField - Handles datetimes correctly with microseconds
- Added ComplexBaseField - for improved flexibility and performance
- Added get_FIELD_display() method for easy choice field displaying
- Added queryset.slave_okay(enabled) method
- Updated queryset.timeout(enabled) and queryset.snapshot(enabled) to be chainable
- Added insert method for bulk inserts
- Added blinker signal support
- Added query_counter context manager for tests
- Added map_reduce method item_frequencies and set as default (as db.eval doesn't work in sharded environments)
- Added inline_map_reduce option to map_reduce
- Updated connection exception so it provides more info on the cause.
- Added searching multiple levels deep in ``DictField``
- Added ``DictField`` entries containing strings to use matching operators
- Added ``MapField``, similar to ``DictField``
- Added Abstract Base Classes
- Added Custom Objects Managers
- Added sliced subfields updating
- Added ``NotRegistered`` exception if dereferencing ``Document`` not in the registry
- Added a write concern for ``save``, ``update``, ``update_one`` and ``get_or_create``
- Added slicing / subarray fetching controls
- Fixed various unique index and other index issues
- Fixed threaded connection issues
- Added spherical geospatial query operators
- Updated queryset to handle latest version of pymongo
  map_reduce now requires an output.
- Added ``Document`` __hash__, __ne__ for pickling
- Added ``FileField`` optional size arg for read method
- Fixed ``FileField`` seek and tell methods for reading files
- Added ``QuerySet.clone`` to support copying querysets
- Fixed item_frequencies when using name thats the same as a native js function
- Added reverse delete rules
- Fixed issue with unset operation
- Fixed Q-object bug
- Added ``QuerySet.all_fields`` resets previous .only() and .exclude()
- Added ``QuerySet.exclude``
- Added django style choices
- Fixed order and filter issue
- Added ``QuerySet.only`` subfield support
- Added creation_counter to ``BaseField`` allowing fields to be sorted in the
  way the user has specified them
- Fixed various errors
- Added many tests

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
