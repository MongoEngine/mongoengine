"""GeoJSON MultiPointField."""

from mongoengine.base import GeoJsonBaseField


class MultiPointField(GeoJsonBaseField):
    """A GeoJSON field storing a list of Points.

    The data is represented as:

    .. code-block:: js

        {'type' : 'MultiPoint' ,
         'coordinates' : [[x1, y1], [x2, y2]]}

    You can either pass a dict with the full information or a list
    to set the value.

    Requires mongodb >= 2.6
    """

    _type = "MultiPoint"


__all__ = ("MultiPointField",)
