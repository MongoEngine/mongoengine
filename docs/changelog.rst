=========
Changelog
=========

Changes in 0.8.6
================
- Fix django auth import (#531)

Changes in 0.8.5
================
- Fix multi level nested fields getting marked as changed (#523)
- Django 1.6 login fix (#522) (#527)
- Django 1.6 session fix (#509)
- EmbeddedDocument._instance is now set when settng the attribute (#506)
- Fixed EmbeddedDocument with ReferenceField equality issue (#502)
- Fixed GenericReferenceField serialization order (#499)
- Fixed count and none bug (#498)
- Fixed bug with .only() and DictField with digit keys (#496)
- Added user_permissions to Django User object (#491, #492)
- Fix updating Geo Location fields (#488)
- Fix handling invalid dict field value (#485)
- Added app_label to MongoUser (#484)
- Use defaults when host and port are passed as None (#483)
- Fixed distinct casting issue with ListField of EmbeddedDocuments (#470)
- Fixed Django 1.6 sessions (#454, #480)

Changes in 0.8.4
================
- Remove database name necessity in uri connection schema (#452)
- Fixed "$pull" semantics for nested ListFields (#447)
- Allow fields to be named the same as query operators (#445)
- Updated field filter logic - can now exclude subclass fields (#443)
- Fixed dereference issue with embedded listfield referencefields (#439)
- Fixed slice when using inheritance causing fields to be excluded (#437)
- Fixed ._get_db() attribute after a Document.switch_db() (#441)
- Dynamic Fields store and recompose Embedded Documents / Documents correctly (#449)
- Handle dynamic fieldnames that look like digits (#434)
- Added get_user_document and improve mongo_auth module (#423)
- Added str representation of GridFSProxy (#424)
- Update transform to handle docs erroneously passed to unset (#416)
- Fixed indexing - turn off _cls (#414)
- Fixed dereference threading issue in ComplexField.__get__ (#412)
- Fixed QuerySetNoCache.count() caching (#410)
- Don't follow references in _get_changed_fields (#422, #417)
- Allow args and kwargs to be passed through to_json (#420)

Changes in 0.8.3
================
- Fixed EmbeddedDocuments with `id` also storing `_id` (#402)
- Added get_proxy_object helper to filefields (#391)
- Added QuerySetNoCache and QuerySet.no_cache() for lower memory consumption (#365)
- Fixed sum and average mapreduce dot notation support (#375, #376, #393)
- Fixed as_pymongo to return the id (#386)
- Document.select_related() now respects `db_alias` (#377)
- Reload uses shard_key if applicable (#384)
- Dynamic fields are ordered based on creation and stored in _fields_ordered (#396)

  **Potential breaking change:** http://docs.mongoengine.org/en/latest/upgrade.html#to-0-8-3

- Fixed pickling dynamic documents `_dynamic_fields` (#387)
- Fixed ListField setslice and delslice dirty tracking (#390)
- Added Django 1.5 PY3 support (#392)
- Added match ($elemMatch) support for EmbeddedDocuments (#379)
- Fixed weakref being valid after reload (#374)
- Fixed queryset.get() respecting no_dereference (#373)
- Added full_result kwarg to update (#380)



Changes in 0.8.2
================
- Added compare_indexes helper (#361)
- Fixed cascading saves which weren't turned off as planned (#291)
- Fixed Datastructures so instances are a Document or EmbeddedDocument (#363)
- Improved cascading saves write performance (#361)
- Fixed ambiguity and differing behaviour regarding field defaults (#349)
- ImageFields now include PIL error messages if invalid error (#353)
- Added lock when calling doc.Delete() for when signals have no sender (#350)
- Reload forces read preference to be PRIMARY (#355)
- Querysets are now lest restrictive when querying duplicate fields (#332, #333)
- FileField now honouring db_alias (#341)
- Removed customised __set__ change tracking in ComplexBaseField (#344)
- Removed unused var in _get_changed_fields (#347)
- Added pre_save_post_validation signal (#345)
- DateTimeField now auto converts valid datetime isostrings into dates (#343)
- DateTimeField now uses dateutil for parsing if available (#343)
- Fixed Doc.objects(read_preference=X) not setting read preference (#352)
- Django session ttl index expiry fixed (#329)
- Fixed pickle.loads (#342)
- Documentation fixes

Changes in 0.8.1
================
- Fixed Python 2.6 django auth importlib issue (#326)
- Fixed pickle unsaved document regression (#327)

Changes in 0.8.0
================
- Fixed querying ReferenceField custom_id (#317)
- Fixed pickle issues with collections (#316)
- Added `get_next_value` preview for SequenceFields (#319)
- Added no_sub_classes context manager and queryset helper (#312)
- Querysets now utilises a local cache
- Changed __len__ behavour in the queryset (#247, #311)
- Fixed querying string versions of ObjectIds issue with ReferenceField (#307)
- Added $setOnInsert support for upserts (#308)
- Upserts now possible with just query parameters (#309)
- Upserting is the only way to ensure docs are saved correctly (#306)
- Fixed register_delete_rule inheritance issue
- Fix cloning of sliced querysets (#303)
- Fixed update_one write concern (#302)
- Updated minimum requirement for pymongo to 2.5
- Add support for new geojson fields, indexes and queries (#299)
- If values cant be compared mark as changed (#287)
- Ensure as_pymongo() and to_json honour only() and exclude() (#293)
- Document serialization uses field order to ensure a strict order is set (#296)
- DecimalField now stores as float not string (#289)
- UUIDField now stores as a binary by default (#292)
- Added Custom User Model for Django 1.5 (#285)
- Cascading saves now default to off (#291)
- ReferenceField now store ObjectId's by default rather than DBRef (#290)
- Added ImageField support for inline replacements (#86)
- Added SequenceField.set_next_value(value) helper (#159)
- Updated .only() behaviour - now like exclude it is chainable (#202)
- Added with_limit_and_skip support to count() (#235)
- Objects queryset manager now inherited (#256)
- Updated connection to use MongoClient (#262, #274)
- Fixed db_alias and inherited Documents (#143)
- Documentation update for document errors (#124)
- Deprecated `get_or_create` (#35)
- Updated inheritable objects created by upsert now contain _cls (#118)
- Added support for creating documents with embedded documents in a single operation (#6)
- Added to_json and from_json to Document (#1)
- Added to_json and from_json to QuerySet (#131)
- Updated index creation now tied to Document class (#102)
- Added none() to queryset (#127)
- Updated SequenceFields to allow post processing of the calculated counter value (#141)
- Added clean method to documents for pre validation data cleaning (#60)
- Added support setting for read prefrence at a query level (#157)
- Added _instance to EmbeddedDocuments pointing to the parent (#139)
- Inheritance is off by default (#122)
- Remove _types and just use _cls for inheritance (#148)
- Only allow QNode instances to be passed as query objects (#199)
- Dynamic fields are now validated on save (#153) (#154)
- Added support for multiple slices and made slicing chainable. (#170) (#190) (#191)
- Fixed GridFSProxy __getattr__ behaviour (#196)
- Fix Django timezone support (#151)
- Simplified Q objects, removed QueryTreeTransformerVisitor (#98) (#171)
- FileFields now copyable (#198)
- Querysets now return clones and are no longer edit in place (#56)
- Added support for $maxDistance (#179)
- Uses getlasterror to test created on updated saves (#163)
- Fixed inheritance and unique index creation (#140)
- Fixed reverse delete rule with inheritance (#197)
- Fixed validation for GenericReferences which havent been dereferenced
- Added switch_db context manager (#106)
- Added switch_db method to document instances (#106)
- Added no_dereference context manager (#82) (#61)
- Added switch_collection context manager (#220)
- Added switch_collection method to document instances (#220)
- Added support for compound primary keys (#149) (#121)
- Fixed overriding objects with custom manager (#58)
- Added no_dereference method for querysets (#82) (#61)
- Undefined data should not override instance methods (#49)
- Added Django Group and Permission (#142)
- Added Doc class and pk to Validation messages (#69)
- Fixed Documents deleted via a queryset don't call any signals (#105)
- Added the "get_decoded" method to the MongoSession class (#216)
- Fixed invalid choices error bubbling (#214)
- Updated Save so it calls $set and $unset in a single operation (#211)
- Fixed inner queryset looping (#204)

Changes in 0.7.10
=================
- Fix UnicodeEncodeError for dbref (#278)
- Allow construction using positional parameters (#268)
- Updated EmailField length to support long domains (#243)
- Added 64-bit integer support (#251)
- Added Django sessions TTL support (#224)
- Fixed issue with numerical keys in MapField(EmbeddedDocumentField()) (#240)
- Fixed clearing _changed_fields for complex nested embedded documents (#237, #239, #242)
- Added "id" back to _data dictionary (#255)
- Only mark a field as changed if the value has changed (#258)
- Explicitly check for Document instances when dereferencing (#261)
- Fixed order_by chaining issue (#265)
- Added dereference support for tuples (#250)
- Resolve field name to db field name when using distinct(#260, #264, #269)
- Added kwargs to doc.save to help interop with django (#223, #270)
- Fixed cloning querysets in PY3
- Int fields no longer unset in save when changed to 0 (#272)
- Fixed ReferenceField query chaining bug fixed (#254)

Changes in 0.7.9
================
- Better fix handling for old style _types
- Embedded SequenceFields follow collection naming convention

Changes in 0.7.8
================
- Fix sequence fields in embedded documents (#166)
- Fix query chaining with .order_by() (#176)
- Added optional encoding and collection config for Django sessions (#180, #181, #183)
- Fixed EmailField so can add extra validation (#173, #174, #187)
- Fixed bulk inserts can now handle custom pk's (#192)
- Added as_pymongo method to return raw or cast results from pymongo (#193)

Changes in 0.7.7
================
- Fix handling for old style _types

Changes in 0.7.6
================
- Unicode fix for repr (#133)
- Allow updates with match operators (#144)
- Updated URLField - now can have a override the regex (#136)
- Allow Django AuthenticationBackends to work with Django user (hmarr/mongoengine#573)
- Fixed reload issue with ReferenceField where dbref=False (#138)

Changes in 0.7.5
================
- ReferenceFields with dbref=False use ObjectId instead of strings (#134)
  See ticket for upgrade notes (#134)

Changes in 0.7.4
================
- Fixed index inheritance issues - firmed up testcases (#123) (#125)

Changes in 0.7.3
================
- Reverted EmbeddedDocuments meta handling - now can turn off inheritance (#119)

Changes in 0.7.2
================
- Update index spec generation so its not destructive (#113)

Changes in 0.7.1
=================
- Fixed index spec inheritance (#111)

Changes in 0.7.0
=================
- Updated queryset.delete so you can use with skip / limit (#107)
- Updated index creation allows kwargs to be passed through refs (#104)
- Fixed Q object merge edge case (#109)
- Fixed reloading on sharded documents (hmarr/mongoengine#569)
- Added NotUniqueError for duplicate keys (#62)
- Added custom collection / sequence naming for SequenceFields (#92)
- Fixed UnboundLocalError in composite index with pk field (#88)
- Updated ReferenceField's to optionally store ObjectId strings
  this will become the default in 0.8 (#89)
- Added FutureWarning - save will default to `cascade=False` in 0.8
- Added example of indexing embedded document fields (#75)
- Fixed ImageField resizing when forcing size (#80)
- Add flexibility for fields handling bad data (#78)
- Embedded Documents no longer handle meta definitions
- Use weakref proxies in base lists / dicts (#74)
- Improved queryset filtering (hmarr/mongoengine#554)
- Fixed Dynamic Documents and Embedded Documents (hmarr/mongoengine#561)
- Fixed abstract classes and shard keys (#64)
- Fixed Python 2.5 support
- Added Python 3 support (thanks to Laine Heron)

Changes in 0.6.20
=================
- Added support for distinct and db_alias (#59)
- Improved support for chained querysets when constraining the same fields (hmarr/mongoengine#554)
- Fixed BinaryField lookup re (#48)

Changes in 0.6.19
=================

- Added Binary support to UUID (#47)
- Fixed MapField lookup for fields without declared lookups (#46)
- Fixed BinaryField python value issue (#48)
- Fixed SequenceField non numeric value lookup (#41)
- Fixed queryset manager issue (#52)
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
