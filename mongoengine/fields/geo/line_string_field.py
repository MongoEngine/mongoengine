"""GeoJSON LineStringField."""

from mongoengine.base import GeoJsonBaseField


class LineStringField(GeoJsonBaseField):
    """A GeoJSON field storing a line of longitude and latitude coordinates.

    The data is represented as:

    .. code-block:: js

        {'type' : 'LineString' ,
         'coordinates' : [[x1, y1], [x2, y2] ... [xn, yn]]}

    You can either pass a dict with the full information or a list of points.

    Requires mongodb >= 2.4
    """

    _type = "LineString"


__all__ = ("LineStringField",)
