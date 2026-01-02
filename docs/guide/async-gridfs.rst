============
Async GridFS
============

MongoEngine provides asynchronous support for GridFS through the
:class:`~mongoengine.fields.FileField` when used in an asynchronous context.
The asynchronous methods are prefixed with ``a`` (e.g., ``aput``, ``aread``, ``awrite``).

Writing
-------

In the following example, a document is created to store details about animals,
including a photo using the asynchronous :meth:`~mongoengine.fields.GridFSProxy.aput` method::

    class Animal(Document):
        genus = StringField()
        family = StringField()
        photo = FileField()

    marmot = Animal(genus='Marmota', family='Sciuridae')

    with open('marmot.jpg', 'rb') as fd:
        await marmot.photo.aput(fd, content_type='image/jpeg')
    await marmot.asave()

Retrieval
---------

Retrieving files asynchronously is done using the :meth:`~mongoengine.fields.GridFSProxy.aread` method::

    marmot = await Animal.aobjects(genus='Marmota').first()
    photo = await marmot.photo.aread()
    content_type = marmot.photo.content_type

.. note:: If you need to :meth:`aread` the content of a file multiple times, you'll need to "rewind"
    the file-like object using :meth:`seek`::

        marmot = await Animal.aobjects(genus='Marmota').first()
        content1 = await marmot.photo.aread()
        assert content1 != ""

        content2 = await marmot.photo.aread()    # will be empty
        assert content2 == ""

        marmot.photo.seek(0)              # rewind the file
        content3 = await marmot.photo.aread()
        assert content3 == content1

Streaming
---------

Streaming data into a :class:`~mongoengine.fields.FileField` asynchronously is
achieved using :meth:`~mongoengine.fields.GridFSProxy.anew_file`,
:meth:`~mongoengine.fields.GridFSProxy.awrite`, and :meth:`~mongoengine.fields.GridFSProxy.aclose`::

    await marmot.photo.anew_file()
    await marmot.photo.awrite('some_image_data')
    await marmot.photo.awrite('some_more_image_data')
    await marmot.photo.aclose()

    await marmot.asave()

Deletion
--------

Deleting stored files asynchronously is achieved with the :meth:`~mongoengine.fields.GridFSProxy.adelete` method::

    await marmot.photo.adelete()    # Deletes the GridFS document
    await marmot.asave()            # Saves the GridFS reference (being None) contained in the marmot instance

.. warning::

    The FileField in a Document actually only stores the ID of a file in a
    separate GridFS collection. This means that deleting a document
    with a defined FileField does not actually delete the file. You must be
    careful to delete any files in a Document as above before deleting the
    Document itself.

Replacing files
---------------

Files can be replaced asynchronously with the :meth:`~mongoengine.fields.GridFSProxy.areplace` method::

    another_marmot = open('another_marmot.png', 'rb')
    await marmot.photo.areplace(another_marmot, content_type='image/png')  # Replaces the GridFS document
    await marmot.asave()                                                   # Replaces the GridFS reference contained in marmot instance
