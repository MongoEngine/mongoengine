"""GeoJSON PolygonField."""

from mongoengine.base import GeoJsonBaseField


class PolygonField(GeoJsonBaseField):
    """A GeoJSON field storing a polygon of longitude and latitude coordinates.

    The data is represented as:

    .. code-block:: js

        {'type' : 'Polygon' ,
         'coordinates' : [[[x1, y1], [x1, y1] ... [xn, yn]],
                          [[x1, y1], [x1, y1] ... [xn, yn]]}

    You can either pass a dict with the full information or a list
    of LineStrings. The first LineString being the outside and the rest being
    holes.

    Requires mongodb >= 2.4
    """

    _type = "Polygon"


__all__ = ("PolygonField",)
