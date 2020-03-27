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

Example of test file:
---------------------
.. code-block:: python

    import unittest
    from mongoengine import connect, disconnect

    class Person(Document):
        name = StringField()

    class TestPerson(unittest.TestCase):

        @classmethod
        def setUpClass(cls):
            connect('mongoenginetest', host='mongomock://localhost')

        @classmethod
        def tearDownClass(cls):
           disconnect()

        def test_thing(self):
            pers = Person(name='John')
            pers.save()

            fresh_pers = Person.objects().first()
            assert fresh_pers.name ==  'John'
