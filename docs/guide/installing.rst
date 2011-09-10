======================
Installing MongoEngine
======================

To use MongoEngine, you will need to download `MongoDB <http://mongodb.org/>`_
and ensure it is running in an accessible location. You will also need
`PyMongo <http://api.mongodb.org/python>`_ to use MongoEngine, but if you
install MongoEngine using setuptools, then the dependencies will be handled for
you.

MongoEngine is available on PyPI, so to use it you can use :program:`pip`:

.. code-block:: console

    $ pip install mongoengine

Alternatively, if you don't have setuptools installed, `download it from PyPi
<http://pypi.python.org/pypi/mongoengine/>`_ and run

.. code-block:: console

    $ python setup.py install

To use the bleeding-edge version of MongoEngine, you can get the source from
`GitHub <http://github.com/hmarr/mongoengine/>`_ and install it as above:

.. code-block:: console

    $ git clone git://github.com/hmarr/mongoengine
    $ cd mongoengine
    $ python setup.py install
