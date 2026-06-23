"""GeoJSON PointField."""

from mongoengine.base import GeoJsonBaseField


class PointField(GeoJsonBaseField):
    """A GeoJSON field storing a longitude and latitude coordinate.

    The data is represented as:

    .. code-block:: js

        {'type' : 'Point' ,
         'coordinates' : [x, y]}

    You can either pass a dict with the full information or a list
    to set the value.

    Requires mongodb >= 2.4
    """

    _type = "Point"


__all__ = ("PointField",)
