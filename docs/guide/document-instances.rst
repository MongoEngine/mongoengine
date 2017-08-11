===================
Documents instances
===================
To create a new document object, create an instance of the relevant document
class, providing values for its fields as constructor keyword arguments.
You may provide values for any of the fields on the document::

    >>> page = Page(title="Test Page")
    >>> page.title
    'Test Page'

You may also assign values to the document's fields using standard object
attribute syntax::

    >>> page.title = "Example Page"
    >>> page.title
    'Example Page'

Saving and deleting documents
=============================
MongoEngine tracks changes to documents to provide efficient saving.  To save
the document to the database, call the :meth:`~mongoengine.Document.save` method.
If the document does not exist in the database, it will be created. If it does
already exist, then any changes will be updated atomically.  For example::

    >>> page = Page(title="Test Page")
    >>> page.save()  # Performs an insert
    >>> page.title = "My Page"
    >>> page.save()  # Performs an atomic set on the title field.

.. note::

    Changes to documents are tracked and on the whole perform ``set`` operations.

    * ``list_field.push(0)`` --- *sets* the resulting list
    * ``del(list_field)``   --- *unsets* whole list

    With lists its preferable to use ``Doc.update(push__list_field=0)`` as
    this stops the whole list being updated --- stopping any race conditions.

.. seealso::
    :ref:`guide-atomic-updates`

Pre save data validation and cleaning
-------------------------------------
MongoEngine allows you to create custom cleaning rules for your documents when
calling :meth:`~mongoengine.Document.save`.  By providing a custom
:meth:`~mongoengine.Document.clean` method you can do any pre validation / data
cleaning.

This might be useful if you want to ensure a default value based on other
document values for example::

    class Essay(Document):
        status = StringField(choices=('Published', 'Draft'), required=True)
        pub_date = DateTimeField()

        def clean(self):
            """Ensures that only published essays have a `pub_date` and
            automatically sets `pub_date` if essay is published and `pub_date`
            is not set"""
            if self.status == 'Draft' and self.pub_date is not None:
                msg = 'Draft entries should not have a publication date.'
                raise ValidationError(msg)
            # Set the pub_date for published items if not set.
            if self.status == 'Published' and self.pub_date is None:
                self.pub_date = datetime.now()

.. note::
    Cleaning is only called if validation is turned on and when calling
    :meth:`~mongoengine.Document.save`.

Cascading Saves
---------------
If your document contains :class:`~mongoengine.fields.ReferenceField` or
:class:`~mongoengine.fields.GenericReferenceField` objects, then by default the
:meth:`~mongoengine.Document.save` method will not save any changes to
those objects.  If you want all references to be saved also, noting each
save is a separate query, then passing :attr:`cascade` as True
to the save method will cascade any saves.

Deleting documents
------------------
To delete a document, call the :meth:`~mongoengine.Document.delete` method.
Note that this will only work if the document exists in the database and has a
valid :attr:`id`.

Document IDs
============
Each document in the database has a unique id. This may be accessed through the
:attr:`id` attribute on :class:`~mongoengine.Document` objects. Usually, the id
will be generated automatically by the database server when the object is save,
meaning that you may only access the :attr:`id` field once a document has been
saved::

    >>> page = Page(title="Test Page")
    >>> page.id
    >>> page.save()
    >>> page.id
    ObjectId('123456789abcdef000000000')

Alternatively, you may define one of your own fields to be the document's
"primary key" by providing ``primary_key=True`` as a keyword argument to a
field's constructor. Under the hood, MongoEngine will use this field as the
:attr:`id`; in fact :attr:`id` is actually aliased to your primary key field so
you may still use :attr:`id` to access the primary key if you want::

    >>> class User(Document):
    ...     email = StringField(primary_key=True)
    ...     name = StringField()
    ...
    >>> bob = User(email='bob@example.com', name='Bob')
    >>> bob.save()
    >>> bob.id == bob.email == 'bob@example.com'
    True

You can also access the document's "primary key" using the :attr:`pk` field,
it's an alias to :attr:`id`::

    >>> page = Page(title="Another Test Page")
    >>> page.save()
    >>> page.id == page.pk
    True

.. note::

   If you define your own primary key field, the field implicitly becomes
   required, so a :class:`~mongoengine.ValidationError` will be thrown if
   you don't provide it.
