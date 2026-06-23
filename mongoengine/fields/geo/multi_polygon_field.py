"""GeoJSON MultiPolygonField."""

from mongoengine.base import GeoJsonBaseField


class MultiPolygonField(GeoJsonBaseField):
    """A GeoJSON field storing  list of Polygons.

    The data is represented as:

    .. code-block:: js

        {'type' : 'MultiPolygon' ,
         'coordinates' : [[
               [[x1, y1], [x1, y1] ... [xn, yn]],
               [[x1, y1], [x1, y1] ... [xn, yn]]
           ], [
               [[x1, y1], [x1, y1] ... [xn, yn]],
               [[x1, y1], [x1, y1] ... [xn, yn]]
           ]
        }

    You can either pass a dict with the full information or a list
    of Polygons.

    Requires mongodb >= 2.6
    """

    _type = "MultiPolygon"


__all__ = ("MultiPolygonField",)
