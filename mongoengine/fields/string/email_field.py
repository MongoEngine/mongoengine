import re
import socket

from mongoengine.base.utils import LazyRegexCompiler

from .string_field import StringField


class EmailField(StringField):
    """A field that validates input as an email address."""

    USER_REGEX = LazyRegexCompiler(
        # `dot-atom` defined in RFC 5322 Section 3.2.3.
        r"(^[-!#$%&'*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z]+)*\Z"
        # `quoted-string` defined in RFC 5322 Section 3.2.4.
        r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-\011\013\014\016-\177])*"\Z)',
        re.IGNORECASE,
    )

    UTF8_USER_REGEX = LazyRegexCompiler(
        (
            # RFC 6531 Section 3.3 extends `atext` (used by dot-atom) to
            # include `UTF8-non-ascii`.
            r"(^[-!#$%&'*+/=?^_`{}|~0-9A-Z-\U0010FFFF]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z-\U0010FFFF]+)*\Z"
            # `quoted-string`
            r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-\011\013\014\016-\177])*"\Z)'
        ),
        re.IGNORECASE | re.UNICODE,
    )

    DOMAIN_REGEX = LazyRegexCompiler(
        r"((?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+)(?:[A-Z0-9-]{2,63}(?<!-))\Z",
        re.IGNORECASE,
    )

    error_msg = "Invalid email address: %s"

    def __init__(
        self,
        domain_whitelist=None,
        allow_utf8_user=False,
        allow_ip_domain=False,
        *args,
        **kwargs,
    ):
        """
        :param domain_whitelist: (optional) list of valid domain names applied during validation
        :param allow_utf8_user: Allow user part of the email to contain utf8 char
        :param allow_ip_domain: Allow domain part of the email to be an IPv4 or IPv6 address
        :param kwargs: Keyword arguments passed into the parent :class:`~mongoengine.StringField`
        """
        self.domain_whitelist = domain_whitelist or []
        self.allow_utf8_user = allow_utf8_user
        self.allow_ip_domain = allow_ip_domain
        super().__init__(*args, **kwargs)

    def validate_user_part(self, user_part):
        """Validate the user part of the email address. Return True if
        valid and False otherwise.
        """
        if self.allow_utf8_user:
            return self.UTF8_USER_REGEX.match(user_part)
        return self.USER_REGEX.match(user_part)

    def validate_domain_part(self, domain_part):
        """Validate the domain part of the email address. Return True if
        valid and False otherwise.
        """
        # Skip domain validation if it's in the whitelist.
        if domain_part in self.domain_whitelist:
            return True

        if self.DOMAIN_REGEX.match(domain_part):
            return True

        # Validate IPv4/IPv6, e.g. user@[192.168.0.1]
        if self.allow_ip_domain and domain_part[0] == "[" and domain_part[-1] == "]":
            for addr_family in (socket.AF_INET, socket.AF_INET6):
                try:
                    socket.inet_pton(addr_family, domain_part[1:-1])
                    return True
                except (OSError, UnicodeEncodeError):
                    pass

        return False

    def validate(self, value, clean=True):
        super().validate(value)

        if "@" not in value:
            self.error(self.error_msg % value)

        user_part, domain_part = value.rsplit("@", 1)

        # Validate the user part.
        if not self.validate_user_part(user_part):
            self.error(self.error_msg % value)

        # Validate the domain and, if invalid, see if it's IDN-encoded.
        if not self.validate_domain_part(domain_part):
            try:
                domain_part = domain_part.encode("idna").decode("ascii")
            except UnicodeError:
                self.error(
                    "{} {}".format(
                        self.error_msg % value, "(domain failed IDN encoding)"
                    )
                )
            else:
                if not self.validate_domain_part(domain_part):
                    self.error(
                        "{} {}".format(
                            self.error_msg % value, "(domain validation failed)"
                        )
                    )


__all__ = ("EmailField",)
