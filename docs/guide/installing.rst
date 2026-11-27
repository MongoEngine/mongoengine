======================
Installing MongoEngine
======================

To use MongoEngine, you will need to download `MongoDB <https://www.mongodb.com/>`_
and ensure it is running in an accessible location. You will also need
`PyMongo <https://pymongo.readthedocs.io/>`_ >=4.2, <5.0 to use MongoEngine.
When you install MongoEngine with pip, its dependencies are installed for you.

MongoEngine is available on PyPI, so you can use :program:`pip`:

.. code-block:: console

    $ python -m pip install mongoengine

Alternatively, if you don't have setuptools installed, `download it from PyPi
<http://pypi.python.org/pypi/mongoengine/>`_ and run

.. code-block:: console

    $ python setup.py install

To use the bleeding-edge version of MongoEngine, you can get the source from
`GitHub <https://github.com/MongoEngine/mongoengine/>`_ and install it as above:

.. code-block:: console

    $ git clone git://github.com/mongoengine/mongoengine
    $ cd mongoengine
    $ python setup.py install
