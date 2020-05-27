# -*- coding: utf-8 -*-
from bson import InvalidDocument
import pytest

from mongoengine import *
from mongoengine.base import BaseSet
from mongoengine.mongodb_support import MONGODB_36, get_mongodb_version

from tests.utils import MongoDBTestCase, get_as_pymongo


def get_from_db(doc):
    """Fetch the Document from the database"""
    return doc.__class__.objects.get(id=doc.id)


class TestSetField(MongoDBTestCase):
    def test_storage(self):
        class BlogPost(Document):
            info = SetField()

        BlogPost.drop_collection()
        info = {"testvalue1", "testvalue2"}
        post = BlogPost(info=info)
        assert isinstance(post.info, BaseSet)

        post.save()
        assert isinstance(post.info, BaseSet)

        post = get_from_db(post)
        assert isinstance(post.info, BaseSet)

        assert get_as_pymongo(post) == {"_id": post.id, "info": sorted(list(info))}

    def test_validate_invalid_type(self):
        class BlogPost(Document):
            info = SetField()

        BlogPost.drop_collection()

        invalid_infos = ["my post", {1: "test"}]
        for invalid_info in invalid_infos:
            with pytest.raises(ValidationError):
                BlogPost(info=invalid_info).validate()

    def test_general_things(self):
        """Ensure that set types work as expected."""

        class BlogPost(Document):
            info = SetField()

        BlogPost.drop_collection()

        post = BlogPost(info=["test1", "test2"])
        post.save()

        post = BlogPost()
        post.info = {"test3"}
        post.save()

        post = BlogPost()
        post.info = ["test3", "test3", "test4"]
        post.save()

        post = BlogPost()
        post.info = {"test2", "test3", "test4"}
        post.save()

        assert BlogPost.objects.count() == 4
        assert BlogPost.objects.filter(info="test3").count() == 3
        assert BlogPost.objects.filter(info__0="test1").count() == 1
        assert BlogPost.objects.filter(info__0="test2").count() == 1
        assert BlogPost.objects.filter(info__in=["test2", "test4"]).count() == 3

        post = BlogPost.objects.create(info={"test5", "test6"})
        post.info.update({"updated"})
        post.save()
        post.reload()
        assert "updated" in post.info

    def test_list_and_tuples(self):
        """Ensure that sets can be created from lists and tuples."""

        class BlogPost(Document):
            info = SetField()

        BlogPost.drop_collection()

        post = BlogPost(info=[1, 2, 2])
        assert post.info == {1, 2}
        post.save()
        assert post.info == {1, 2}
        post.reload()
        assert post.info == {1, 2}
        post = get_from_db(post)
        assert post.info == {1, 2}

        post = BlogPost()
        post.info = [1, 2, 2]
        assert post.info == {1, 2}
        post.save()
        assert post.info == {1, 2}
        post.reload()
        assert post.info == {1, 2}
        post = get_from_db(post)
        assert post.info == {1, 2}

        post = BlogPost(info=(1, 2, 2))
        assert post.info == {1, 2}
        post.save()
        assert post.info == {1, 2}
        post.reload()
        assert post.info == {1, 2}
        post = get_from_db(post)
        assert post.info == {1, 2}

        post = BlogPost()
        post.info = (1, 2, 2)
        assert post.info == {1, 2}
        post.save()
        assert post.info == {1, 2}
        post.reload()
        assert post.info == {1, 2}
        post = get_from_db(post)
        assert post.info == {1, 2}

    def test_set_field_field(self):
        """Ensure subfields are validated."""

        class BlogPost(Document):
            info = SetField(BooleanField())

        BlogPost.drop_collection()

        with pytest.raises(ValidationError):
            post = BlogPost()
            post.info = {"a", "b"}
            post.validate()
