===================
Documents instances
===================
To create a new document object, create an instance of the relevant document
class, providing values for its fields as its constructor keyword arguments.
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

    Changes to documents are tracked and on the whole perform `set` operations.

    * ``list_field.pop(0)`` - *sets* the resulting list
    * ``del(list_field)``   - *unsets* whole list

.. seealso::
    :ref:`guide-atomic-updates`

Cascading Saves
---------------
If your document contains :class:`~mongoengine.ReferenceField` or
:class:`~mongoengine.GenericReferenceField` objects, then by default the
:meth:`~mongoengine.Document.save` method will automatically save any changes to
those objects as well.  If this is not desired passing :attr:`cascade` as False
to the save method turns this feature off.

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

You can also access the document's "primary key" using the :attr:`pk` field; in
is an alias to :attr:`id`::

    >>> page = Page(title="Another Test Page")
    >>> page.save()
    >>> page.id == page.pk

.. note::

   If you define your own primary key field, the field implicitly becomes
   required, so a :class:`ValidationError` will be thrown if you don't provide
   it.
