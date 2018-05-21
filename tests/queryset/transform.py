import unittest

from bson.son import SON

from mongoengine import *
from mongoengine.queryset import Q, transform

__all__ = ("TransformTest",)


class TransformTest(unittest.TestCase):

    def setUp(self):
        connect(db='mongoenginetest')

    def test_transform_query(self):
        """Ensure that the _transform_query function operates correctly.
        """
        self.assertEqual(transform.query(name='test', age=30),
                         {'name': 'test', 'age': 30})
        self.assertEqual(transform.query(age__lt=30),
                         {'age': {'$lt': 30}})
        self.assertEqual(transform.query(age__gt=20, age__lt=50),
                         {'age': {'$gt': 20, '$lt': 50}})
        self.assertEqual(transform.query(age=20, age__gt=50),
                         {'$and': [{'age': {'$gt': 50}}, {'age': 20}]})
        self.assertEqual(transform.query(friend__age__gte=30),
                         {'friend.age': {'$gte': 30}})
        self.assertEqual(transform.query(name__exists=True),
                         {'name': {'$exists': True}})

    def test_transform_update(self):
        class LisDoc(Document):
            foo = ListField(StringField())

        class DicDoc(Document):
            dictField = DictField()

        class Doc(Document):
            pass

        LisDoc.drop_collection()
        DicDoc.drop_collection()
        Doc.drop_collection()

        DicDoc().save()
        doc = Doc().save()

        for k, v in (("set", "$set"), ("set_on_insert", "$setOnInsert"), ("push", "$push")):
            update = transform.update(DicDoc, **{"%s__dictField__test" % k: doc})
            self.assertTrue(isinstance(update[v]["dictField.test"], dict))

        # Update special cases
        update = transform.update(DicDoc, unset__dictField__test=doc)
        self.assertEqual(update["$unset"]["dictField.test"], 1)

        update = transform.update(DicDoc, pull__dictField__test=doc)
        self.assertTrue(isinstance(update["$pull"]["dictField"]["test"], dict))
        
        update = transform.update(LisDoc, pull__foo__in=['a'])
        self.assertEqual(update, {'$pull': {'foo': {'$in': ['a']}}})

    def test_transform_update_push(self):
        """Ensure the differences in behvaior between 'push' and 'push_all'"""
        class BlogPost(Document):
            tags = ListField(StringField())

        update = transform.update(BlogPost, push__tags=['mongo', 'db'])
        self.assertEqual(update, {'$push': {'tags': ['mongo', 'db']}})

        update = transform.update(BlogPost, push_all__tags=['mongo', 'db'])
        self.assertEqual(update, {'$push': {'tags': {'$each': ['mongo', 'db']}}})

    def test_query_field_name(self):
        """Ensure that the correct field name is used when querying.
        """
        class Comment(EmbeddedDocument):
            content = StringField(db_field='commentContent')

        class BlogPost(Document):
            title = StringField(db_field='postTitle')
            comments = ListField(EmbeddedDocumentField(Comment),
                                 db_field='postComments')

        BlogPost.drop_collection()

        data = {'title': 'Post 1', 'comments': [Comment(content='test')]}
        post = BlogPost(**data)
        post.save()

        self.assertTrue('postTitle' in
                        BlogPost.objects(title=data['title'])._query)
        self.assertFalse('title' in
                         BlogPost.objects(title=data['title'])._query)
        self.assertEqual(BlogPost.objects(title=data['title']).count(), 1)

        self.assertTrue('_id' in BlogPost.objects(pk=post.id)._query)
        self.assertEqual(BlogPost.objects(pk=post.id).count(), 1)

        self.assertTrue('postComments.commentContent' in
                        BlogPost.objects(comments__content='test')._query)
        self.assertEqual(BlogPost.objects(comments__content='test').count(), 1)

        BlogPost.drop_collection()

    def test_query_pk_field_name(self):
        """Ensure that the correct "primary key" field name is used when
        querying
        """
        class BlogPost(Document):
            title = StringField(primary_key=True, db_field='postTitle')

        BlogPost.drop_collection()

        data = {'title': 'Post 1'}
        post = BlogPost(**data)
        post.save()

        self.assertTrue('_id' in BlogPost.objects(pk=data['title'])._query)
        self.assertTrue('_id' in BlogPost.objects(title=data['title'])._query)
        self.assertEqual(BlogPost.objects(pk=data['title']).count(), 1)

        BlogPost.drop_collection()

    def test_chaining(self):
        class A(Document):
            pass

        class B(Document):
            a = ReferenceField(A)

        A.drop_collection()
        B.drop_collection()

        a1 = A().save()
        a2 = A().save()

        B(a=a1).save()

        # Works
        q1 = B.objects.filter(a__in=[a1, a2], a=a1)._query

        # Doesn't work
        q2 = B.objects.filter(a__in=[a1, a2])
        q2 = q2.filter(a=a1)._query

        self.assertEqual(q1, q2)

    def test_raw_query_and_Q_objects(self):
        """
        Test raw plays nicely
        """
        class Foo(Document):
            name = StringField()
            a = StringField()
            b = StringField()
            c = StringField()

            meta = {
                'allow_inheritance': False
            }

        query = Foo.objects(__raw__={'$nor': [{'name': 'bar'}]})._query
        self.assertEqual(query, {'$nor': [{'name': 'bar'}]})

        q1 = {'$or': [{'a': 1}, {'b': 1}]}
        query = Foo.objects(Q(__raw__=q1) & Q(c=1))._query
        self.assertEqual(query, {'$or': [{'a': 1}, {'b': 1}], 'c': 1})

    def test_raw_and_merging(self):
        class Doc(Document):
            meta = {'allow_inheritance': False}

        raw_query = Doc.objects(__raw__={
            'deleted': False,
            'scraped': 'yes',
            '$nor': [
                {'views.extracted': 'no'},
                {'attachments.views.extracted': 'no'}
            ]
        })._query

        self.assertEqual(raw_query, {
            'deleted': False,
            'scraped': 'yes',
            '$nor': [
                {'views.extracted': 'no'},
                {'attachments.views.extracted': 'no'}
            ]
        })

    def test_geojson_PointField(self):
        class Location(Document):
            loc = PointField()

        update = transform.update(Location, set__loc=[1, 2])
        self.assertEqual(update, {'$set': {'loc': {"type": "Point", "coordinates": [1, 2]}}})

        update = transform.update(Location, set__loc={"type": "Point", "coordinates": [1, 2]})
        self.assertEqual(update, {'$set': {'loc': {"type": "Point", "coordinates": [1, 2]}}})

    def test_geojson_LineStringField(self):
        class Location(Document):
            line = LineStringField()

        update = transform.update(Location, set__line=[[1, 2], [2, 2]])
        self.assertEqual(update, {'$set': {'line': {"type": "LineString", "coordinates": [[1, 2], [2, 2]]}}})

        update = transform.update(Location, set__line={"type": "LineString", "coordinates": [[1, 2], [2, 2]]})
        self.assertEqual(update, {'$set': {'line': {"type": "LineString", "coordinates": [[1, 2], [2, 2]]}}})

    def test_geojson_PolygonField(self):
        class Location(Document):
            poly = PolygonField()

        update = transform.update(Location, set__poly=[[[40, 5], [40, 6], [41, 6], [40, 5]]])
        self.assertEqual(update, {'$set': {'poly': {"type": "Polygon", "coordinates": [[[40, 5], [40, 6], [41, 6], [40, 5]]]}}})

        update = transform.update(Location, set__poly={"type": "Polygon", "coordinates": [[[40, 5], [40, 6], [41, 6], [40, 5]]]})
        self.assertEqual(update, {'$set': {'poly': {"type": "Polygon", "coordinates": [[[40, 5], [40, 6], [41, 6], [40, 5]]]}}})

    def test_type(self):
        class Doc(Document):
            df = DynamicField()
        Doc(df=True).save()
        Doc(df=7).save()
        Doc(df="df").save()
        self.assertEqual(Doc.objects(df__type=1).count(), 0)  # double
        self.assertEqual(Doc.objects(df__type=8).count(), 1)  # bool
        self.assertEqual(Doc.objects(df__type=2).count(), 1)  # str
        self.assertEqual(Doc.objects(df__type=16).count(), 1)  # int

    def test_last_field_name_like_operator(self):
        class EmbeddedItem(EmbeddedDocument):
            type = StringField()
            name = StringField()

        class Doc(Document):
            item = EmbeddedDocumentField(EmbeddedItem)

        Doc.drop_collection()

        doc = Doc(item=EmbeddedItem(type="axe", name="Heroic axe"))
        doc.save()

        self.assertEqual(1, Doc.objects(item__type__="axe").count())
        self.assertEqual(1, Doc.objects(item__name__="Heroic axe").count())

        Doc.objects(id=doc.id).update(set__item__type__='sword')
        self.assertEqual(1, Doc.objects(item__type__="sword").count())
        self.assertEqual(0, Doc.objects(item__type__="axe").count())

    def test_understandable_error_raised(self):
        class Event(Document):
            title = StringField()
            location = GeoPointField()

        box = [(35.0, -125.0), (40.0, -100.0)]
        # I *meant* to execute location__within_box=box
        events = Event.objects(location__within=box)
        with self.assertRaises(InvalidQueryError):
            events.count()
    
    def test_update_pull_for_list_fields(self):
        """ 
        Test added to check pull operation in update for 
        EmbeddedDocumentListField which is inside a EmbeddedDocumentField
        """
        class Word(EmbeddedDocument):
            word = StringField()
            index = IntField()
        
        class SubDoc(EmbeddedDocument):
            heading = ListField(StringField())
            text = EmbeddedDocumentListField(Word)
        
        class MainDoc(Document):
            title = StringField()
            content = EmbeddedDocumentField(SubDoc)
        
        word = Word(word='abc', index=1)
        update = transform.update(MainDoc, pull__content__text=word)
        self.assertEqual(update, {'$pull': {'content.text': SON([('word', u'abc'), ('index', 1)])}})

        update = transform.update(MainDoc, pull__content__heading='xyz')
        self.assertEqual(update, {'$pull': {'content.heading': 'xyz'}})
        
        
if __name__ == '__main__':
    unittest.main()
