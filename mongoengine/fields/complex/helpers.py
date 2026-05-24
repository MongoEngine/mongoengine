"""Helper functions for container fields."""


def key_not_string(d):
    """Helper function to recursively determine if any key in a
    dictionary is not a string.
    """
    for k, v in d.items():
        if not isinstance(k, str) or (isinstance(v, dict) and key_not_string(v)):
            return True


def key_starts_with_dollar(d):
    """Helper function to recursively determine if any key in a
    dictionary starts with a dollar
    """
    for k, v in d.items():
        if (k.startswith("$")) or (isinstance(v, dict) and key_starts_with_dollar(v)):
            return True


__all__ = ("key_not_string", "key_starts_with_dollar")
