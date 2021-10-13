import pytest

from mongoengine import *
from tests.utils import MongoDBTestCase


class TestURLField(MongoDBTestCase):
    def test_validation(self):
        """Ensure that URLFields validate urls properly."""

        class Link(Document):
            url = URLField()

        link = Link()
        link.url = "google"
        with pytest.raises(ValidationError):
            link.validate()

        link.url = "http://www.google.com:8080"
        link.validate()

    def test_unicode_url_validation(self):
        """Ensure unicode URLs are validated properly."""

        class Link(Document):
            url = URLField()

        link = Link()
        link.url = "http://привет.com"

        # TODO fix URL validation - this *IS* a valid URL
        # For now we just want to make sure that the error message is correct
        with pytest.raises(ValidationError) as exc_info:
            link.validate()
        assert (
            str(exc_info.value)
            == "ValidationError (Link:None) (Invalid URL: http://\u043f\u0440\u0438\u0432\u0435\u0442.com: ['url'])"
        )

    def test_url_scheme_validation(self):
        """Ensure that URLFields validate urls with specific schemes properly."""

        class Link(Document):
            url = URLField()

        class SchemeLink(Document):
            url = URLField(schemes=["ws", "irc"])

        link = Link()
        link.url = "ws://google.com"
        with pytest.raises(ValidationError):
            link.validate()

        scheme_link = SchemeLink()
        scheme_link.url = "ws://google.com"
        scheme_link.validate()

    def test_underscore_allowed_in_domains_names(self):
        class Link(Document):
            url = URLField()

        link = Link()
        link.url = "https://san_leandro-ca.geebo.com"
        link.validate()
