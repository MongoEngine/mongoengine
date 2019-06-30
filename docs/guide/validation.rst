====================
Document Validation
====================

By design, MongoEngine strictly validates the documents right before they are inserted in MongoDB
and make sure they are consistent with the fields defined in your models.

Mongoengine will not validate a document when an object is loaded from the DB into an instance
of your model but this operation will fail under some circumstances (e.g. if there is a field in
the document fetched from the database that is not defined in your model).


Built-in validation
=================

Mongoengine provides different fields that encapsulate the corresponding validation
out of the box. Validation runs when calling `.validate()` or `.save()`

.. code-block:: python

    from mongoengine import Document, EmailField

    class User(Document):
        email = EmailField()
        age = IntField(min_value=0, max_value=99)

    user = User(email='invalid@', age=24)
    user.validate()     # raises ValidationError (Invalid email address: ['email'])
    user.save()         # raises ValidationError (Invalid email address: ['email'])

    user2 = User(email='john.doe@garbage.com', age=1000)
    user2.save()        # raises ValidationError (Integer value is too large: ['age'])

Custom validation
=================

The following feature can be used to customize the validation:

* Field `validation` parameter

.. code-block:: python

    def not_john_doe(name):
        if name == 'John Doe':
            raise ValidationError("John Doe is not a valid name")

    class Person(Document):
        full_name = StringField(validation=not_john_doe)

    Person(full_name='Billy Doe').save()
    Person(full_name='John Doe').save()  # raises ValidationError (John Doe is not a valid name)


* Document `clean` method

Although not its primary use case, `clean` may be use to do validation that involves multiple fields.
Note that `clean` runs before the validation when you save a Document.

.. code-block:: python

    class Person(Document):
        first_name = StringField()
        last_name = StringField()

        def clean(self):
            if self.first_name == 'John' and self.last_name == 'Doe':
                raise ValidationError('John Doe is not a valid name')

    Person(first_name='Billy', last_name='Doe').save()
    Person(first_name='John', last_name='Doe').save()      # raises ValidationError (John Doe is not a valid name)



* Adding custom Field classes

We recommend as much as possible to use fields provided by MongoEngine. However, it is also possible
to subclass a Field and encapsulate some validation by overriding the `validate` method

.. code-block:: python

    class AgeField(IntField):

        def validate(self, value):
            super(AgeField, self).validate(value)     # let IntField.validate run first
            if value == 60:
                self.error('60 is not allowed')

    class Person(Document):
        age = AgeField(min_value=0, max_value=99)

    Person(age=20).save()   # passes
    Person(age=1000).save() # raises ValidationError (Integer value is too large: ['age'])
    Person(age=60).save()   # raises ValidationError (Person:None) (60 is not allowed: ['age'])


.. note::

   When overriding `validate`, use `self.error("your-custom-error")` instead of raising ValidationError explicitly,
   it will provide a better context with the error message

Disabling validation
====================

We do not recommend to do this but if for some reason you need to disable the validation of a document
when you call `.save()`, you can use `.save(validate=False)`.

.. code-block:: python

    class Person(Document):
        age = IntField()

    Person(age='garbage').save()    # raises ValidationError (garbage could not be converted to int: ['age'])

    Person(age='garbage').save(validate=False)
    person = Person.objects.first()
    assert person.age == 'garbage'
