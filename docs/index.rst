==============================
MongoEngine User Documentation
==============================

**MongoEngine** is an Object-Document Mapper, written in Python for working with
MongoDB. To install it, simply run

.. code-block:: console

    $ python -m pip install -U mongoengine

:doc:`tutorial`
  A quick tutorial building a tumblelog to get you up and running with
  MongoEngine.

:doc:`guide/index`
  The Full guide to MongoEngine --- from modeling documents to storing files,
  from querying for data to firing signals and *everything* between.

:doc:`apireference`
  The complete API documentation --- the innards of documents, querysets and fields.

:doc:`faq`
  Frequently Asked Questions

:doc:`django`
  Using MongoEngine and Django

MongoDB and driver support
--------------------------

MongoEngine is based on the PyMongo driver and tested against multiple versions of MongoDB.
For further details, please refer to the `readme <https://github.com/MongoEngine/mongoengine>`_.

Community
---------

To get help with using MongoEngine, use the `MongoEngine Users mailing list
<http://groups.google.com/group/mongoengine-users>`_ or the ever popular
`stackoverflow <http://www.stackoverflow.com>`_.

Contributing
------------

**Yes please!**  We are always looking for contributions, additions and improvements.

The source is available on `GitHub <https://github.com/MongoEngine/mongoengine>`_
and contributions are always encouraged. Contributions can be as simple as
minor tweaks to this documentation, the website or the core.

To contribute, fork the project on
`GitHub <https://github.com/MongoEngine/mongoengine>`_ and send a
pull request.

Changes
-------

See the :doc:`changelog` for a full list of changes to MongoEngine.

Offline Reading
---------------

Download the docs in `pdf <https://media.readthedocs.org/pdf/mongoengine-odm/latest/mongoengine-odm.pdf>`_
or `epub <https://media.readthedocs.org/epub/mongoengine-odm/latest/mongoengine-odm.epub>`_
formats for offline reading.


.. toctree::
    :maxdepth: 1
    :numbered:
    :hidden:

    tutorial
    guide/index
    apireference
    changelog
    faq
    django

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
