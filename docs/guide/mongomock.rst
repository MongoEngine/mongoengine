==============================
Use mongomock for testing
==============================

`mongomock <https://github.com/vmalloc/mongomock/>`_ is a package to do just 
what the name implies, mocking a mongo database.

To use with mongoengine, simply specify mongomock when connecting with 
mongoengine:

.. code-block:: python

    connect('mongoenginetest', host='mongomock://localhost')
    conn = get_connection()

or with an alias:

.. code-block:: python

    connect('mongoenginetest', host='mongomock://localhost', alias='testdb')
    conn = get_connection('testdb')
