==================
Logging/Monitoring
==================

It is possible to use `pymongo.monitoring <https://api.mongodb.com/python/current/api/pymongo/monitoring.html>`_ to monitor
the driver events (e.g: queries, connections, etc). This can be handy if you want to monitor the queries issued by
MongoEngine to the driver.

To use `pymongo.monitoring` with MongoEngine, you need to make sure that you are registering the listeners
**before** establishing the database connection (i.e calling `connect`):

The following snippet provides a basic logging of all command events:

.. code-block:: python

    import logging
    from pymongo import monitoring
    from mongoengine import *

    log = logging.getLogger()
    log.setLevel(logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG)


    class CommandLogger(monitoring.CommandListener):

        def started(self, event):
            log.debug("Command {0.command_name} with request id "
                     "{0.request_id} started on server "
                     "{0.connection_id}".format(event))

        def succeeded(self, event):
            log.debug("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "succeeded in {0.duration_micros} "
                     "microseconds".format(event))

        def failed(self, event):
            log.debug("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "failed in {0.duration_micros} "
                     "microseconds".format(event))

    monitoring.register(CommandLogger())


    class Jedi(Document):
        name = StringField()


    connect()


    log.info('GO!')

    log.info('Saving an item through MongoEngine...')
    Jedi(name='Obi-Wan Kenobii').save()

    log.info('Querying through MongoEngine...')
    obiwan = Jedi.objects.first()

    log.info('Updating through MongoEngine...')
    obiwan.name = 'Obi-Wan Kenobi'
    obiwan.save()


Executing this prints the following output::

    INFO:root:GO!
    INFO:root:Saving an item through MongoEngine...
    DEBUG:root:Command insert with request id 1681692777 started on server ('localhost', 27017)
    DEBUG:root:Command insert with request id 1681692777 on server ('localhost', 27017) succeeded in 562 microseconds
    INFO:root:Querying through MongoEngine...
    DEBUG:root:Command find with request id 1714636915 started on server ('localhost', 27017)
    DEBUG:root:Command find with request id 1714636915 on server ('localhost', 27017) succeeded in 341 microseconds
    INFO:root:Updating through MongoEngine...
    DEBUG:root:Command update with request id 1957747793 started on server ('localhost', 27017)
    DEBUG:root:Command update with request id 1957747793 on server ('localhost', 27017) succeeded in 455 microseconds

More details can of course be obtained by checking the `event` argument from the `CommandListener`.
