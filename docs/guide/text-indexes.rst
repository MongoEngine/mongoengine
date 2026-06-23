===========
Text Search
===========

After MongoDB 2.4 version, supports search documents by text indexes.


Defining a Document with text index
===================================
Use the *$* prefix to set a text index, Look the declaration::

  class News(Document):
      title = StringField()
      content = StringField()
      is_active = BooleanField()

      meta = {'indexes': [
          {'fields': ['$title', "$content"],
           'default_language': 'english',
           'weights': {'title': 10, 'content': 2}
          }
      ]}



Querying
========

Saving a document::

  News(title="Using mongodb text search",
       content="Testing text search").save()

  News(title="MongoEngine 0.9 released",
       content="Various improvements").save()

  # The asynchronous alternative is as follows:

  await News(title="Using mongodb text search",
             content="Testing text search").asave()

  await News(title="MongoEngine 0.9 released",
             content="Various improvements").asave()

Next, start a text search using :attr:`QuerySet.search_text` method::

  document = News.objects.search_text('testing').first()
  document.title # may be: "Using mongodb text search"

  document = News.objects.search_text('released').first()
  document.title # may be: "MongoEngine 0.9 released"

  # The asynchronous alternative is as follows:

  document = await News.aobjects.search_text('testing').first()
  document.title # may be: "Using mongodb text search"

  document = await News.aobjects.search_text('released').first()
  document.title # may be: "MongoEngine 0.9 released"


Ordering by text score
======================

::

  objects = News.objects.search_text('mongo').order_by('$text_score')

  # The asynchronous alternative is as follows:

  async for news in News.aobjects.search_text('mongo').order_by('$text_score'):
      print(news.title)
