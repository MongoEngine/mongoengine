import unittest
import pymongo
from datetime import datetime

from mongoengine.queryset import (QuerySet, MultipleObjectsReturned, 
                                  DoesNotExist)
from mongoengine import *


class QuerySetTest(unittest.TestCase):
    
    def setUp(self):
        connect(db='mongoenginetest')

        class Person(Document):
            name = StringField()
            age = IntField()
        self.Person = Person
        
    def test_initialisation(self):
        """Ensure that a QuerySet is correctly initialised by QuerySetManager.
        """
        self.assertTrue(isinstance(self.Person.objects, QuerySet))
        self.assertEqual(self.Person.objects._collection.name, 
                         self.Person._meta['collection'])
        self.assertTrue(isinstance(self.Person.objects._collection,
                                   pymongo.collection.Collection))

    def test_transform_query(self):
        """Ensure that the _transform_query function operates correctly.
        """
        self.assertEqual(QuerySet._transform_query(name='test', age=30),
                         {'name': 'test', 'age': 30})
        self.assertEqual(QuerySet._transform_query(age__lt=30), 
                         {'age': {'$lt': 30}})
        self.assertEqual(QuerySet._transform_query(age__gt=20, age__lt=50),
                         {'age': {'$gt': 20, '$lt': 50}})
        self.assertEqual(QuerySet._transform_query(age=20, age__gt=50),
                         {'age': 20})
        self.assertEqual(QuerySet._transform_query(friend__age__gte=30), 
                         {'friend.age': {'$gte': 30}})
        self.assertEqual(QuerySet._transform_query(name__exists=True), 
                         {'name': {'$exists': True}})

    def test_find(self):
        """Ensure that a query returns a valid set of results.
        """
        person1 = self.Person(name="User A", age=20)
        person1.save()
        person2 = self.Person(name="User B", age=30)
        person2.save()

        q1 = Q(name='test')
        q2 = Q(age__gte=18)

        # Find all people in the collection
        people = self.Person.objects
        self.assertEqual(len(people), 2)
        results = list(people)
        self.assertTrue(isinstance(results[0], self.Person))
        self.assertTrue(isinstance(results[0].id, (pymongo.objectid.ObjectId,
                                                    str, unicode)))
        self.assertEqual(results[0].name, "User A")
        self.assertEqual(results[0].age, 20)
        self.assertEqual(results[1].name, "User B")
        self.assertEqual(results[1].age, 30)

        # Use a query to filter the people found to just person1
        people = self.Person.objects(age=20)
        self.assertEqual(len(people), 1)
        person = people.next()
        self.assertEqual(person.name, "User A")
        self.assertEqual(person.age, 20)

        # Test limit
        people = list(self.Person.objects.limit(1))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0].name, 'User A')

        # Test skip
        people = list(self.Person.objects.skip(1))
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0].name, 'User B')

        person3 = self.Person(name="User C", age=40)
        person3.save()

        # Test slice limit
        people = list(self.Person.objects[:2])
        self.assertEqual(len(people), 2)
        self.assertEqual(people[0].name, 'User A')
        self.assertEqual(people[1].name, 'User B')

        # Test slice skip
        people = list(self.Person.objects[1:])
        self.assertEqual(len(people), 2)
        self.assertEqual(people[0].name, 'User B')
        self.assertEqual(people[1].name, 'User C')

        # Test slice limit and skip
        people = list(self.Person.objects[1:2])
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0].name, 'User B')

        people = list(self.Person.objects[1:1])
        self.assertEqual(len(people), 0)

    def test_find_one(self):
        """Ensure that a query using find_one returns a valid result.
        """
        person1 = self.Person(name="User A", age=20)
        person1.save()
        person2 = self.Person(name="User B", age=30)
        person2.save()

        # Retrieve the first person from the database
        person = self.Person.objects.first()
        self.assertTrue(isinstance(person, self.Person))
        self.assertEqual(person.name, "User A")
        self.assertEqual(person.age, 20)

        # Use a query to filter the people found to just person2
        person = self.Person.objects(age=30).first()
        self.assertEqual(person.name, "User B")

        person = self.Person.objects(age__lt=30).first()
        self.assertEqual(person.name, "User A")

        # Use array syntax
        person = self.Person.objects[0]
        self.assertEqual(person.name, "User A")

        person = self.Person.objects[1]
        self.assertEqual(person.name, "User B")

        self.assertRaises(IndexError, self.Person.objects.__getitem__, 2)
        
        # Find a document using just the object id
        person = self.Person.objects.with_id(person1.id)
        self.assertEqual(person.name, "User A")

    def test_find_only_one(self):
        """Ensure that a query using ``get`` returns at most one result.
        """
        # Try retrieving when no objects exists
        self.assertRaises(DoesNotExist, self.Person.objects.get)

        person1 = self.Person(name="User A", age=20)
        person1.save()
        person2 = self.Person(name="User B", age=30)
        person2.save()

        # Retrieve the first person from the database
        self.assertRaises(MultipleObjectsReturned, self.Person.objects.get)

        # Use a query to filter the people found to just person2
        person = self.Person.objects.get(age=30)
        self.assertEqual(person.name, "User B")

        person = self.Person.objects.get(age__lt=30)
        self.assertEqual(person.name, "User A")

    def test_get_or_create(self):
        """Ensure that ``get_or_create`` returns one result or creates a new
        document.
        """
        person1 = self.Person(name="User A", age=20)
        person1.save()
        person2 = self.Person(name="User B", age=30)
        person2.save()

        # Retrieve the first person from the database
        self.assertRaises(MultipleObjectsReturned, 
                          self.Person.objects.get_or_create)

        # Use a query to filter the people found to just person2
        person = self.Person.objects.get_or_create(age=30)
        self.assertEqual(person.name, "User B")

        person = self.Person.objects.get_or_create(age__lt=30)
        self.assertEqual(person.name, "User A")

        # Try retrieving when no objects exists - new doc should be created
        self.Person.objects.get_or_create(age=50, defaults={'name': 'User C'})

        person = self.Person.objects.get(age=50)
        self.assertEqual(person.name, "User C")

    def test_repeated_iteration(self):
        """Ensure that QuerySet rewinds itself one iteration finishes.
        """
        self.Person(name='Person 1').save()
        self.Person(name='Person 2').save()

        queryset = self.Person.objects
        people1 = [person for person in queryset]
        people2 = [person for person in queryset]

        self.assertEqual(people1, people2)

    def test_regex_query_shortcuts(self):
        """Ensure that contains, startswith, endswith, etc work.
        """
        person = self.Person(name='Guido van Rossum')
        person.save()

        # Test contains
        obj = self.Person.objects(name__contains='van').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(name__contains='Van').first()
        self.assertEqual(obj, None)
        obj = self.Person.objects(Q(name__contains='van')).first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__contains='Van')).first()
        self.assertEqual(obj, None)

        # Test icontains
        obj = self.Person.objects(name__icontains='Van').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__icontains='Van')).first()
        self.assertEqual(obj, person)

        # Test startswith
        obj = self.Person.objects(name__startswith='Guido').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(name__startswith='guido').first()
        self.assertEqual(obj, None)
        obj = self.Person.objects(Q(name__startswith='Guido')).first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__startswith='guido')).first()
        self.assertEqual(obj, None)

        # Test istartswith
        obj = self.Person.objects(name__istartswith='guido').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__istartswith='guido')).first()
        self.assertEqual(obj, person)

        # Test endswith
        obj = self.Person.objects(name__endswith='Rossum').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(name__endswith='rossuM').first()
        self.assertEqual(obj, None)
        obj = self.Person.objects(Q(name__endswith='Rossum')).first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__endswith='rossuM')).first()
        self.assertEqual(obj, None)

        # Test iendswith
        obj = self.Person.objects(name__iendswith='rossuM').first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__iendswith='rossuM')).first()
        self.assertEqual(obj, person)

    def test_filter_chaining(self):
        """Ensure filters can be chained together.
        """
        from datetime import datetime
        
        class BlogPost(Document):
            title = StringField()
            is_published = BooleanField()
            published_date = DateTimeField()
            
            @queryset_manager
            def published(doc_cls, queryset):
                return queryset(is_published=True)
                
        blog_post_1 = BlogPost(title="Blog Post #1", 
                               is_published = True,
                               published_date=datetime(2010, 1, 5, 0, 0 ,0))
        blog_post_2 = BlogPost(title="Blog Post #2", 
                               is_published = True,
                               published_date=datetime(2010, 1, 6, 0, 0 ,0))
        blog_post_3 = BlogPost(title="Blog Post #3", 
                               is_published = True,
                               published_date=datetime(2010, 1, 7, 0, 0 ,0))

        blog_post_1.save()
        blog_post_2.save()
        blog_post_3.save()
        
        # find all published blog posts before 2010-01-07
        published_posts = BlogPost.published()
        published_posts = published_posts.filter(
            published_date__lt=datetime(2010, 1, 7, 0, 0 ,0))
        self.assertEqual(published_posts.count(), 2)
        
        BlogPost.drop_collection()

    def test_ordering(self):
        """Ensure default ordering is applied and can be overridden.
        """
        class BlogPost(Document):
            title = StringField()
            published_date = DateTimeField()

            meta = {
                'ordering': ['-published_date']
            }

        BlogPost.drop_collection()

        blog_post_1 = BlogPost(title="Blog Post #1", 
                               published_date=datetime(2010, 1, 5, 0, 0 ,0))
        blog_post_2 = BlogPost(title="Blog Post #2", 
                               published_date=datetime(2010, 1, 6, 0, 0 ,0))
        blog_post_3 = BlogPost(title="Blog Post #3", 
                               published_date=datetime(2010, 1, 7, 0, 0 ,0))

        blog_post_1.save()
        blog_post_2.save()
        blog_post_3.save()
        
        # get the "first" BlogPost using default ordering
        # from BlogPost.meta.ordering
        latest_post = BlogPost.objects.first() 
        self.assertEqual(latest_post.title, "Blog Post #3")
        
        # override default ordering, order BlogPosts by "published_date"
        first_post = BlogPost.objects.order_by("+published_date").first()
        self.assertEqual(first_post.title, "Blog Post #1")

        BlogPost.drop_collection()

    def test_only(self):
        """Ensure that QuerySet.only only returns the requested fields.
        """
        person = self.Person(name='test', age=25)
        person.save()

        obj = self.Person.objects.only('name').get()
        self.assertEqual(obj.name, person.name)
        self.assertEqual(obj.age, None)

        obj = self.Person.objects.only('age').get()
        self.assertEqual(obj.name, None)
        self.assertEqual(obj.age, person.age)

        obj = self.Person.objects.only('name', 'age').get()
        self.assertEqual(obj.name, person.name)
        self.assertEqual(obj.age, person.age)

        # Check polymorphism still works
        class Employee(self.Person):
            salary = IntField(name='wage')

        employee = Employee(name='test employee', age=40, salary=30000)
        employee.save()

        obj = self.Person.objects(id=employee.id).only('age').get()
        self.assertTrue(isinstance(obj, Employee))

        # Check field names are looked up properly
        obj = Employee.objects(id=employee.id).only('salary').get()
        self.assertEqual(obj.salary, employee.salary)
        self.assertEqual(obj.name, None)

    def test_find_embedded(self):
        """Ensure that an embedded document is properly returned from a query.
        """
        class User(EmbeddedDocument):
            name = StringField()

        class BlogPost(Document):
            content = StringField()
            author = EmbeddedDocumentField(User)

        BlogPost.drop_collection()

        post = BlogPost(content='Had a good coffee today...')
        post.author = User(name='Test User')
        post.save()

        result = BlogPost.objects.first()
        self.assertTrue(isinstance(result.author, User))
        self.assertEqual(result.author.name, 'Test User')
        
        BlogPost.drop_collection()

    def test_find_dict_item(self):
        """Ensure that DictField items may be found.
        """
        class BlogPost(Document):
            info = DictField()

        BlogPost.drop_collection()

        post = BlogPost(info={'title': 'test'})
        post.save()

        post_obj = BlogPost.objects(info__title='test').first()
        self.assertEqual(post_obj.id, post.id)

        BlogPost.drop_collection()

    def test_q(self):
        """Ensure that Q objects may be used to query for documents.
        """
        class BlogPost(Document):
            publish_date = DateTimeField()
            published = BooleanField()

        BlogPost.drop_collection()

        post1 = BlogPost(publish_date=datetime(2010, 1, 8), published=False)
        post1.save()

        post2 = BlogPost(publish_date=datetime(2010, 1, 15), published=True)
        post2.save()

        post3 = BlogPost(published=True)
        post3.save()

        post4 = BlogPost(publish_date=datetime(2010, 1, 8))
        post4.save()

        post5 = BlogPost(publish_date=datetime(2010, 1, 15))
        post5.save()

        post6 = BlogPost(published=False)
        post6.save()

        # Check ObjectId lookup works
        obj = BlogPost.objects(id=post1.id).first()
        self.assertEqual(obj, post1)

        # Check Q object combination
        date = datetime(2010, 1, 10)
        q = BlogPost.objects(Q(publish_date__lte=date) | Q(published=True))
        posts = [post.id for post in q]

        published_posts = (post1, post2, post3, post4)
        self.assertTrue(all(obj.id in posts for obj in published_posts))

        self.assertFalse(any(obj.id in posts for obj in [post5, post6]))

        BlogPost.drop_collection()

        # Check the 'in' operator
        self.Person(name='user1', age=20).save()
        self.Person(name='user2', age=20).save()
        self.Person(name='user3', age=30).save()
        self.Person(name='user4', age=40).save()
        
        self.assertEqual(len(self.Person.objects(Q(age__in=[20]))), 2)
        self.assertEqual(len(self.Person.objects(Q(age__in=[20, 30]))), 3)

    def test_q_regex(self):
        """Ensure that Q objects can be queried using regexes.
        """
        person = self.Person(name='Guido van Rossum')
        person.save()

        import re
        obj = self.Person.objects(Q(name=re.compile('^Gui'))).first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name=re.compile('^gui'))).first()
        self.assertEqual(obj, None)

        obj = self.Person.objects(Q(name=re.compile('^gui', re.I))).first()
        self.assertEqual(obj, person)

        obj = self.Person.objects(Q(name__ne=re.compile('^bob'))).first()
        self.assertEqual(obj, person)
        obj = self.Person.objects(Q(name__ne=re.compile('^Gui'))).first()
        self.assertEqual(obj, None)

    def test_exec_js_query(self):
        """Ensure that queries are properly formed for use in exec_js.
        """
        class BlogPost(Document):
            hits = IntField()
            published = BooleanField()

        BlogPost.drop_collection()

        post1 = BlogPost(hits=1, published=False)
        post1.save()

        post2 = BlogPost(hits=1, published=True)
        post2.save()

        post3 = BlogPost(hits=1, published=True)
        post3.save()

        js_func = """
            function(hitsField) {
                var count = 0;
                db[collection].find(query).forEach(function(doc) {
                    count += doc[hitsField];
                });
                return count;
            }
        """

        # Ensure that normal queries work
        c = BlogPost.objects(published=True).exec_js(js_func, 'hits')
        self.assertEqual(c, 2)

        c = BlogPost.objects(published=False).exec_js(js_func, 'hits')
        self.assertEqual(c, 1)

        # Ensure that Q object queries work
        c = BlogPost.objects(Q(published=True)).exec_js(js_func, 'hits')
        self.assertEqual(c, 2)

        c = BlogPost.objects(Q(published=False)).exec_js(js_func, 'hits')
        self.assertEqual(c, 1)

        BlogPost.drop_collection()

    def test_exec_js_field_sub(self):
        """Ensure that field substitutions occur properly in exec_js functions.
        """
        class Comment(EmbeddedDocument):
            content = StringField(name='body')

        class BlogPost(Document):
            name = StringField(name='doc-name')
            comments = ListField(EmbeddedDocumentField(Comment), name='cmnts')

        BlogPost.drop_collection()

        comments1 = [Comment(content='cool'), Comment(content='yay')]
        post1 = BlogPost(name='post1', comments=comments1)
        post1.save()

        comments2 = [Comment(content='nice stuff')]
        post2 = BlogPost(name='post2', comments=comments2)
        post2.save()

        code = """
        function getComments() {
            var comments = [];
            db[collection].find(query).forEach(function(doc) {
                var docComments = doc[~comments];
                for (var i = 0; i < docComments.length; i++) {
                    comments.push({
                        'document': doc[~name],
                        'comment': doc[~comments][i][~comments.content]
                    });
                }
            });
            return comments;
        }
        """
        
        sub_code = BlogPost.objects._sub_js_fields(code)
        code_chunks = ['doc["cmnts"];', 'doc["doc-name"],', 
                       'doc["cmnts"][i]["body"]']
        for chunk in code_chunks:
            self.assertTrue(chunk in sub_code)

        results = BlogPost.objects.exec_js(code)
        expected_results = [
            {u'comment': u'cool', u'document': u'post1'}, 
            {u'comment': u'yay', u'document': u'post1'}, 
            {u'comment': u'nice stuff', u'document': u'post2'},
        ]
        self.assertEqual(results, expected_results)

        BlogPost.drop_collection()

    def test_delete(self):
        """Ensure that documents are properly deleted from the database.
        """
        self.Person(name="User A", age=20).save()
        self.Person(name="User B", age=30).save()
        self.Person(name="User C", age=40).save()

        self.assertEqual(len(self.Person.objects), 3)

        self.Person.objects(age__lt=30).delete()
        self.assertEqual(len(self.Person.objects), 2)

        self.Person.objects.delete()
        self.assertEqual(len(self.Person.objects), 0)

    def test_update(self):
        """Ensure that atomic updates work properly.
        """
        class BlogPost(Document):
            title = StringField()
            hits = IntField()
            tags = ListField(StringField())

        BlogPost.drop_collection()

        post = BlogPost(name="Test Post", hits=5, tags=['test'])
        post.save()

        BlogPost.objects.update(set__hits=10)
        post.reload()
        self.assertEqual(post.hits, 10)

        BlogPost.objects.update_one(inc__hits=1)
        post.reload()
        self.assertEqual(post.hits, 11)

        BlogPost.objects.update_one(dec__hits=1)
        post.reload()
        self.assertEqual(post.hits, 10)

        BlogPost.objects.update(push__tags='mongo')
        post.reload()
        self.assertTrue('mongo' in post.tags)

        BlogPost.objects.update_one(push_all__tags=['db', 'nosql'])
        post.reload()
        self.assertTrue('db' in post.tags and 'nosql' in post.tags)

        BlogPost.drop_collection()

    def test_order_by(self):
        """Ensure that QuerySets may be ordered.
        """
        self.Person(name="User A", age=20).save()
        self.Person(name="User B", age=40).save()
        self.Person(name="User C", age=30).save()

        names = [p.name for p in self.Person.objects.order_by('-age')]
        self.assertEqual(names, ['User B', 'User C', 'User A'])

        names = [p.name for p in self.Person.objects.order_by('+age')]
        self.assertEqual(names, ['User A', 'User C', 'User B'])

        names = [p.name for p in self.Person.objects.order_by('age')]
        self.assertEqual(names, ['User A', 'User C', 'User B'])
        
        ages = [p.age for p in self.Person.objects.order_by('-name')]
        self.assertEqual(ages, [30, 40, 20])

    def test_item_frequencies(self):
        """Ensure that item frequencies are properly generated from lists.
        """
        class BlogPost(Document):
            hits = IntField()
            tags = ListField(StringField(), name='blogTags')

        BlogPost.drop_collection()

        BlogPost(hits=1, tags=['music', 'film', 'actors']).save()
        BlogPost(hits=2, tags=['music']).save()
        BlogPost(hits=3, tags=['music', 'actors']).save()

        f = BlogPost.objects.item_frequencies('tags')
        f = dict((key, int(val)) for key, val in f.items())
        self.assertEqual(set(['music', 'film', 'actors']), set(f.keys()))
        self.assertEqual(f['music'], 3)
        self.assertEqual(f['actors'], 2)
        self.assertEqual(f['film'], 1)

        # Ensure query is taken into account
        f = BlogPost.objects(hits__gt=1).item_frequencies('tags')
        f = dict((key, int(val)) for key, val in f.items())
        self.assertEqual(set(['music', 'actors']), set(f.keys()))
        self.assertEqual(f['music'], 2)
        self.assertEqual(f['actors'], 1)

        # Check that normalization works
        f = BlogPost.objects.item_frequencies('tags', normalize=True)
        self.assertAlmostEqual(f['music'], 3.0/6.0)
        self.assertAlmostEqual(f['actors'], 2.0/6.0)
        self.assertAlmostEqual(f['film'], 1.0/6.0)

        BlogPost.drop_collection()

    def test_average(self):
        """Ensure that field can be averaged correctly.
        """
        ages = [23, 54, 12, 94, 27]
        for i, age in enumerate(ages):
            self.Person(name='test%s' % i, age=age).save()

        avg = float(sum(ages)) / len(ages)
        self.assertAlmostEqual(int(self.Person.objects.average('age')), avg)

        self.Person(name='ageless person').save()
        self.assertEqual(int(self.Person.objects.average('age')), avg)

    def test_sum(self):
        """Ensure that field can be summed over correctly.
        """
        ages = [23, 54, 12, 94, 27]
        for i, age in enumerate(ages):
            self.Person(name='test%s' % i, age=age).save()

        self.assertEqual(int(self.Person.objects.sum('age')), sum(ages))

        self.Person(name='ageless person').save()
        self.assertEqual(int(self.Person.objects.sum('age')), sum(ages))

    def test_custom_manager(self):
        """Ensure that custom QuerySetManager instances work as expected.
        """
        class BlogPost(Document):
            tags = ListField(StringField())

            @queryset_manager
            def music_posts(doc_cls, queryset):
                return queryset(tags='music')

        BlogPost.drop_collection()

        post1 = BlogPost(tags=['music', 'film'])
        post1.save()
        post2 = BlogPost(tags=['music'])
        post2.save()
        post3 = BlogPost(tags=['film', 'actors'])
        post3.save()

        self.assertEqual([p.id for p in BlogPost.objects],
                         [post1.id, post2.id, post3.id])
        self.assertEqual([p.id for p in BlogPost.music_posts],
                         [post1.id, post2.id])

        BlogPost.drop_collection()

    def test_query_field_name(self):
        """Ensure that the correct field name is used when querying.
        """
        class Comment(EmbeddedDocument):
            content = StringField(name='commentContent')

        class BlogPost(Document):
            title = StringField(name='postTitle')
            comments = ListField(EmbeddedDocumentField(Comment),
                                 name='postComments')
                                 

        BlogPost.drop_collection()

        data = {'title': 'Post 1', 'comments': [Comment(content='test')]}
        BlogPost(**data).save()

        self.assertTrue('postTitle' in 
                        BlogPost.objects(title=data['title'])._query)
        self.assertFalse('title' in 
                         BlogPost.objects(title=data['title'])._query)
        self.assertEqual(len(BlogPost.objects(title=data['title'])), 1)

        self.assertTrue('postComments.commentContent' in 
                        BlogPost.objects(comments__content='test')._query)
        self.assertEqual(len(BlogPost.objects(comments__content='test')), 1)

        BlogPost.drop_collection()

    def test_query_value_conversion(self):
        """Ensure that query values are properly converted when necessary.
        """
        class BlogPost(Document):
            author = ReferenceField(self.Person)

        BlogPost.drop_collection()

        person = self.Person(name='test', age=30)
        person.save()

        post = BlogPost(author=person)
        post.save()

        # Test that query may be performed by providing a document as a value
        # while using a ReferenceField's name - the document should be 
        # converted to an DBRef, which is legal, unlike a Document object
        post_obj = BlogPost.objects(author=person).first()
        self.assertEqual(post.id, post_obj.id)

        # Test that lists of values work when using the 'in', 'nin' and 'all'
        post_obj = BlogPost.objects(author__in=[person]).first()
        self.assertEqual(post.id, post_obj.id)

        BlogPost.drop_collection()

    def test_update_value_conversion(self):
        """Ensure that values used in updates are converted before use.
        """
        class Group(Document):
            members = ListField(ReferenceField(self.Person))

        Group.drop_collection()

        user1 = self.Person(name='user1')
        user1.save()
        user2 = self.Person(name='user2')
        user2.save()

        group = Group()
        group.save()

        Group.objects(id=group.id).update(set__members=[user1, user2])
        group.reload()

        self.assertTrue(len(group.members) == 2)
        self.assertEqual(group.members[0].name, user1.name)
        self.assertEqual(group.members[1].name, user2.name)

        Group.drop_collection()

    def test_types_index(self):
        """Ensure that and index is used when '_types' is being used in a
        query.
        """
        class BlogPost(Document):
            date = DateTimeField()
            meta = {'indexes': ['-date']}

        # Indexes are lazy so use list() to perform query
        list(BlogPost.objects)
        info = BlogPost.objects._collection.index_information()
        self.assertTrue([('_types', 1)] in info.values())
        self.assertTrue([('_types', 1), ('date', -1)] in info.values())

        BlogPost.drop_collection()

        class BlogPost(Document):
            title = StringField()
            meta = {'allow_inheritance': False}

        # _types is not used on objects where allow_inheritance is False
        list(BlogPost.objects)
        info = BlogPost.objects._collection.index_information()
        self.assertFalse([('_types', 1)] in info.values())

        BlogPost.drop_collection()
        
    def test_bulk(self):
        """Ensure bulk querying by object id returns a proper dict.
        """
        class BlogPost(Document):
            title = StringField()
            
        BlogPost.drop_collection()

        post_1 = BlogPost(title="Post #1")
        post_2 = BlogPost(title="Post #2")
        post_3 = BlogPost(title="Post #3")
        post_4 = BlogPost(title="Post #4")
        post_5 = BlogPost(title="Post #5")

        post_1.save()
        post_2.save()
        post_3.save()
        post_4.save()
        post_5.save()
        
        ids = [post_1.id, post_2.id, post_5.id]
        objects = BlogPost.objects.in_bulk(ids)
        
        self.assertEqual(len(objects), 3)

        self.assertTrue(post_1.id in objects)
        self.assertTrue(post_2.id in objects)
        self.assertTrue(post_5.id in objects)
        
        self.assertTrue(objects[post_1.id].title == post_1.title)
        self.assertTrue(objects[post_2.id].title == post_2.title)
        self.assertTrue(objects[post_5.id].title == post_5.title)        
        
        BlogPost.drop_collection()

    def tearDown(self):
        self.Person.drop_collection()


class QTest(unittest.TestCase):
    
    def test_or_and(self):
        """Ensure that Q objects may be combined correctly.
        """
        q1 = Q(name='test')
        q2 = Q(age__gte=18)

        query = ['(', {'name': 'test'}, '||', {'age__gte': 18}, ')']
        self.assertEqual((q1 | q2).query, query)

        query = ['(', {'name': 'test'}, '&&', {'age__gte': 18}, ')']
        self.assertEqual((q1 & q2).query, query)

        query = ['(', '(', {'name': 'test'}, '&&', {'age__gte': 18}, ')', '||',
                 {'name': 'example'}, ')']
        self.assertEqual((q1 & q2 | Q(name='example')).query, query)

    def test_item_query_as_js(self):
        """Ensure that the _item_query_as_js utilitiy method works properly.
        """
        q = Q()
        examples = [
            ({'name': 'test'}, 'this.name == i0f0', {'i0f0': 'test'}),
            ({'age': {'$gt': 18}}, 'this.age > i0f0o0', {'i0f0o0': 18}),
            ({'name': 'test', 'age': {'$gt': 18, '$lte': 65}}, 
             'this.age <= i0f0o0 && this.age > i0f0o1 && this.name == i0f1', 
             {'i0f0o0': 65, 'i0f0o1': 18, 'i0f1': 'test'}),
        ]
        for item, js, scope in examples:
            test_scope = {}
            self.assertEqual(q._item_query_as_js(item, test_scope, 0), js)
            self.assertEqual(scope, test_scope)

if __name__ == '__main__':
    unittest.main()
