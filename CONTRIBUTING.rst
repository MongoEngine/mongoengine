Contributing to MongoEngine
===========================

MongoEngine has a large `community
<https://raw.github.com/MongoEngine/mongoengine/master/AUTHORS>`_ and
contributions are always encouraged. Contributions can be as simple as
minor tweaks to the documentation. Please read these guidelines before
sending a pull request.

Bugfixes and New Features
-------------------------

Before starting to write code, look for existing `tickets
<https://github.com/MongoEngine/mongoengine/issues?state=open>`_ or `create one
<https://github.com/MongoEngine/mongoengine/issues>`_ for your specific
issue or feature request. That way you avoid working on something
that might not be of interest or that has already been addressed.  If in doubt
post to the `user group <http://groups.google.com/group/mongoengine-users>`

Supported Interpreters
----------------------

PyMongo supports CPython 2.5 and newer. Language
features not supported by all interpreters can not be used.
Please also ensure that your code is properly converted by
`2to3 <http://docs.python.org/library/2to3.html>`_ for Python 3 support.

Style Guide
-----------

MongoEngine aims to follow `PEP8 <http://www.python.org/dev/peps/pep-0008/>`_
including 4 space indents and 79 character line limits.

Testing
-------

All tests are run on `Travis <http://travis-ci.org/MongoEngine/mongoengine>`_
and any pull requests are automatically tested by Travis. Any pull requests
without tests will take longer to be integrated and might be refused.

General Guidelines
------------------

- Avoid backward breaking changes if at all possible.
- Write inline documentation for new classes and methods.
- Write tests and make sure they pass (make sure you have a mongod
  running on the default port, then execute ``python setup.py test``
  from the cmd line to run the test suite).
- Add yourself to AUTHORS.rst :)

Documentation
-------------

To contribute to the `API documentation
<http://docs.mongoengine.org/en/latest/apireference.html>`_
just make your changes to the inline documentation of the appropriate
`source code <https://github.com/MongoEngine/mongoengine>`_ or `rst file
<https://github.com/MongoEngine/mongoengine/tree/master/docs>`_ in a
branch and submit a `pull request <https://help.github.com/articles/using-pull-requests>`_.
You might also use the github `Edit <https://github.com/blog/844-forking-with-the-edit-button>`_
button.
