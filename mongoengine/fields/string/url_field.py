import re

from mongoengine.base.utils import LazyRegexCompiler

from .string_field import StringField


class URLField(StringField):
    """A field that validates input as an URL."""

    _URL_REGEX = LazyRegexCompiler(
        r"^(?:[a-z0-9\.\-]*)://"  # scheme is validated separately
        r"(?:(?:[A-Z0-9](?:[A-Z0-9-_]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}(?<!-)\.?)|"  # domain...
        r"localhost|"  # localhost...
        r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|"  # ...or ipv4
        r"\[?[A-F0-9]*:[A-F0-9:]+\]?)"  # ...or ipv6
        r"(?::\d+)?"  # optional port
        r"(?:/?|[/?]\S+)$",
        re.IGNORECASE,
    )
    _URL_SCHEMES = ["http", "https", "ftp", "ftps"]

    def __init__(self, url_regex=None, schemes=None, **kwargs):
        """
        :param url_regex: (optional) Overwrite the default regex used for validation
        :param schemes: (optional) Overwrite the default URL schemes that are allowed
        :param kwargs: Keyword arguments passed into the parent :class:`~mongoengine.StringField`
        """
        self.url_regex = url_regex or self._URL_REGEX
        self.schemes = schemes or self._URL_SCHEMES
        super().__init__(**kwargs)

    def validate(self, value, clean=True):
        # Check first if the scheme is valid
        scheme = value.split("://")[0].lower()
        if scheme not in self.schemes:
            self.error(f"Invalid scheme {scheme} in URL: {value}")

        # Then check full URL
        if not self.url_regex.match(value):
            self.error(f"Invalid URL: {value}")


__all__ = ("URLField",)
