Differences between Mongomallard and Mongoengine
-----

* All document fields are lazily evaluated, resulting in much faster object initialization time.
* `_data` is removed due to lazy evaluation. `to_dict()` can be used to convert a document to a dictionary, and `_internal_data` contains previously evaluated data.
* Field methods `to_python`, `from_python`, `to_mongo`, `value_for_instance`:
    * `to_python` is called when converting from a MongoDB type to a document Python type only.
    * `from_python` is called when converting an assignment in Python to the document Python type.
    * `to_mongo` is called when converting from a document Python type to a MongoDB type.
    * `value_for_instance` is called just before returning a value in Python allowing for instance-specific transformations.
* `pre_init`, `post_init`, `pre_save_post_validation` signals are removed to ensure fast object initialization.
* `DecimalField` is removed since there is no corresponding MongoDB type
* `LongField` is removed since it is equivalent with `IntField`
* Adding `SafeReferenceField` which returns None if the reference does not exist.
* Adding `SafeReferenceListField` which doesn't return references that don't exist.
* Accessing a `ListField(ReferenceField)` doesn't automatically dereference all objects since they are lazily evaluated. A `SafeReferenceListField` may be used instead.
* Accessing a related object's id doesn't fetch the object from the database, e.g. `book.author.id` where author is a `ReferenceField` will not make a database lookup except when using a `SafeReferenceField`. When inheritance is allowed, a proxy object will be returned, otherwise a lazy object from the referenced document class will be returned.
* The primary key is only stored as `_id` in the database and is referenced in Python as `pk` or as the name of the primary key field.
* Saves are not cascaded by default.
* `Document.save()` supports `full=True` keyword argument to force saving all model fields.
* `_get_changed_fields()` / `_changed_fields` returns a set of field names (not db field names)
* Simplified `EmailField` email regex to be more compatible
* Assigning invalid types (e.g. an invalid string to `IntField`) raises immediately a `ValueError`
* `order_by()` without an argument resets the ordering (no ordering will be applied)

Untested / not implemented yet:
-----

* Dynamic documents / `DynamicField`, dynamic addition/deletion of fields
* Field display name methods
* `SequenceField`
* Pickling documents
* `FileField`
* All Geo fields
* `no_dereference()`
* using `SafeReferenceListField` with `GenericReferenceField`
* `max_depth` argument for `doc.reload()`
