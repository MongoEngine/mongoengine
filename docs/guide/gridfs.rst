======
GridFS
======
GridFS support comes in the form of the :class:`~mongoengine.FileField` field
object. This field acts as a file-like object and provides a couple of
different ways of inserting and retrieving data. Metadata such as content-type
can also be stored alongside the stored files. In the following example, an
document is created to store details about animals, including a photo:

    class Animal(Document):
        genus = StringField()
        family = StringField()
        photo = FileField()

    marmot = Animal('Marmota', 'Sciuridae')

    marmot_photo = open('marmot.jpg')   # Retrieve a photo from disk
    marmot.photo = marmot_photo         # Store the photo in the document

    marmot.save()

So adding file data to a document is as easy as adding data to any other 

.. versionadded:: 0.4


