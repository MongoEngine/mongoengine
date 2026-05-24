import datetime
import unittest

from mongoengine import *
from mongoengine.pymongo_support import PYMONGO_VERSION
from tests.asynchronous.utils import MongoDBAsyncTestCase


class TestGeoQueries(MongoDBAsyncTestCase):
    async def _create_event_data(self, point_field_class=GeoPointField):
        """Create some sample data re-used in many of the tests below."""

        class Event(Document):
            title = StringField()
            date = DateTimeField()
            location = point_field_class()

            def __unicode__(self):
                return self.title

        self.Event = Event

        await Event.adrop_collection()

        event1 = await Event.aobjects.create(
            title="Coltrane Motion @ Double Door",
            date=datetime.datetime.now() - datetime.timedelta(days=1),
            location=[-87.677137, 41.909889],
        )
        event2 = await Event.aobjects.create(
            title="Coltrane Motion @ Bottom of the Hill",
            date=datetime.datetime.now() - datetime.timedelta(days=10),
            location=[-122.4194155, 37.7749295],
        )
        event3 = await Event.aobjects.create(
            title="Coltrane Motion @ Empty Bottle",
            date=datetime.datetime.now(),
            location=[-87.686638, 41.900474],
        )

        return event1, event2, event3

    async def test_near(self):
        """Make sure the "near" operator works."""
        event1, event2, event3 = await self._create_event_data()

        # find all events "near" pitchfork office, chicago.
        # note that "near" will show the san francisco event, too,
        # although it sorts to last.
        events = self.Event.aobjects(location__near=[-87.67892, 41.9120459])
        if PYMONGO_VERSION < (4,):
            assert events.count() == 3
        assert await events.to_list() == [event1, event3, event2]

        # ensure ordering is respected by "near"
        events = self.Event.aobjects(location__near=[-87.67892, 41.9120459])
        events = events.order_by("-date")
        assert await events.to_list() == [event3, event1, event2]

    async def test_near_and_max_distance(self):
        """Ensure the "max_distance" operator works alongside the "near"
        operator.
        """
        event1, event2, event3 = await self._create_event_data()

        # find events within 10 degrees of San Francisco
        point = [-122.415579, 37.7566023]
        events = self.Event.aobjects(location__near=point, location__max_distance=10)
        assert await events.to_list() == [event2]

    async def test_near_and_min_distance(self):
        """Ensure the "min_distance" operator works alongside the "near"
        operator.
        """
        event1, event2, event3 = await self._create_event_data()

        # find events at least 10 degrees away of San Francisco
        point = [-122.415579, 37.7566023]
        events = self.Event.aobjects(location__near=point, location__min_distance=10)
        assert await events.to_list() == [event3, event1]

    async def test_within_distance(self):
        """Make sure the "within_distance" operator works."""
        event1, event2, event3 = await self._create_event_data()

        # find events within 5 degrees of pitchfork office, Chicago
        point_and_distance = [[-87.67892, 41.9120459], 5]
        events = self.Event.aobjects(location__within_distance=point_and_distance)
        assert await events.count() == 2
        events = await events.to_list()
        assert event2 not in events
        assert event1 in events
        assert event3 in events

        # find events within 10 degrees of San Francisco
        point_and_distance = [[-122.415579, 37.7566023], 10]
        events = self.Event.aobjects(location__within_distance=point_and_distance)
        assert await events.count() == 1
        assert (await events.to_list())[0] == event2

        # find events within 1 degree of greenpoint, broolyn, nyc, ny
        point_and_distance = [[-73.9509714, 40.7237134], 1]
        events = self.Event.aobjects(location__within_distance=point_and_distance)
        assert await events.count() == 0

        # ensure ordering is respected by "within_distance"
        point_and_distance = [[-87.67892, 41.9120459], 10]
        events = self.Event.aobjects(location__within_distance=point_and_distance)
        events = events.order_by("-date")
        assert await events.count() == 2
        assert (await events.to_list())[0] == event3

    async def test_within_box(self):
        """Ensure the "within_box" operator works."""
        event1, event2, event3 = await self._create_event_data()

        # check that within_box works
        box = [(-125.0, 35.0), (-100.0, 40.0)]
        events = self.Event.aobjects(location__within_box=box)
        assert await events.count() == 1
        assert (await events.to_list())[0].id == event2.id

    async def test_within_polygon(self):
        """Ensure the "within_polygon" operator works."""
        event1, event2, event3 = await self._create_event_data()

        polygon = [
            (-87.694445, 41.912114),
            (-87.69084, 41.919395),
            (-87.681742, 41.927186),
            (-87.654276, 41.911731),
            (-87.656164, 41.898061),
        ]
        events = self.Event.aobjects(location__within_polygon=polygon)
        assert await events.count() == 1
        assert (await events.to_list())[0].id == event1.id

        polygon2 = [
            (-1.742249, 54.033586),
            (-1.225891, 52.792797),
            (-4.40094, 53.389881),
        ]
        events = self.Event.aobjects(location__within_polygon=polygon2)
        assert await events.count() == 0

    async def test_2dsphere_near(self):
        """Make sure the "near" operator works with a PointField, which
        corresponds to a 2dsphere index.
        """
        event1, event2, event3 = await self._create_event_data(
            point_field_class=PointField
        )

        # find all events "near" pitchfork office, chicago.
        # note that "near" will show the san francisco event, too,
        # although it sorts to last.
        events = self.Event.aobjects(location__near=[-87.67892, 41.9120459])
        assert await events.to_list() == [event1, event3, event2]

        # ensure ordering is respected by "near"
        events = self.Event.aobjects(location__near=[-87.67892, 41.9120459])
        events = events.order_by("-date")
        assert await events.to_list() == [event3, event1, event2]

    async def test_2dsphere_near_and_max_distance(self):
        """Ensure the "max_distance" operator works alongside the "near"
        operator with a 2dsphere index.
        """
        event1, event2, event3 = await self._create_event_data(
            point_field_class=PointField
        )

        # find events within 10km of san francisco
        point = [-122.415579, 37.7566023]
        events = self.Event.aobjects(location__near=point, location__max_distance=10000)
        assert await events.to_list() == [event2]

        # find events within 1km of greenpoint, broolyn, nyc, ny
        events = self.Event.aobjects(
            location__near=[-73.9509714, 40.7237134], location__max_distance=1000
        )
        assert await events.to_list() == []

        # ensure ordering is respected by "near"
        events = self.Event.aobjects(
            location__near=[-87.67892, 41.9120459], location__max_distance=10000
        ).order_by("-date")
        assert await events.to_list() == [event3, event1]

    async def test_2dsphere_geo_within_box(self):
        """Ensure the "geo_within_box" operator works with a 2dsphere
        index.
        """
        event1, event2, event3 = await self._create_event_data(
            point_field_class=PointField
        )

        # check that within_box works
        box = [(-125.0, 35.0), (-100.0, 40.0)]
        events = await self.Event.aobjects(location__geo_within_box=box).to_list()
        assert len(events) == 1
        assert events[0].id == event2.id

    async def test_2dsphere_geo_within_polygon(self):
        """Ensure the "geo_within_polygon" operator works with a
        2dsphere index.
        """
        event1, event2, event3 = await self._create_event_data(
            point_field_class=PointField
        )

        polygon = [
            (-87.694445, 41.912114),
            (-87.69084, 41.919395),
            (-87.681742, 41.927186),
            (-87.654276, 41.911731),
            (-87.656164, 41.898061),
        ]
        events = await self.Event.aobjects(
            location__geo_within_polygon=polygon
        ).to_list()
        assert len(events) == 1
        assert events[0].id == event1.id

        polygon2 = [
            (-1.742249, 54.033586),
            (-1.225891, 52.792797),
            (-4.40094, 53.389881),
        ]
        events = self.Event.aobjects(location__geo_within_polygon=polygon2)
        assert await events.count() == 0

    async def test_2dsphere_near_and_min_max_distance(self):
        """Ensure "min_distance" and "max_distance" operators work well
        together with the "near" operator in a 2dsphere index.
        """
        event1, event2, event3 = await self._create_event_data(
            point_field_class=PointField
        )

        # ensure min_distance and max_distance combine well
        events = self.Event.aobjects(
            location__near=[-87.67892, 41.9120459],
            location__min_distance=1000,
            location__max_distance=10000,
        ).order_by("-date")
        assert await events.to_list() == [event3]

        # ensure ordering is respected by "near" with "min_distance"
        events = self.Event.aobjects(
            location__near=[-87.67892, 41.9120459], location__min_distance=10000
        ).order_by("-date")
        if PYMONGO_VERSION < (4,):
            assert events.count() == 1
        assert await events.to_list() == [event2]

    async def test_2dsphere_geo_within_center(self):
        """Make sure the "geo_within_center" operator works with a
        2dsphere index.
        """
        event1, event2, event3 = await self._create_event_data(
            point_field_class=PointField
        )

        # find events within 5 degrees of pitchfork office, chicago
        point_and_distance = [[-87.67892, 41.9120459], 2]
        events = self.Event.aobjects(location__geo_within_center=point_and_distance)
        assert await events.count() == 2
        events = await events.to_list()
        assert event2 not in events
        assert event1 in events
        assert event3 in events

    async def _test_embedded(self, point_field_class):
        """Helper test method ensuring given point field class works
        well in an embedded document.
        """

        class Venue(EmbeddedDocument):
            location = point_field_class()
            name = StringField()

        class Event(Document):
            title = StringField()
            venue = EmbeddedDocumentField(Venue)

        await Event.adrop_collection()

        venue1 = Venue(name="The Rock", location=[-87.677137, 41.909889])
        venue2 = Venue(name="The Bridge", location=[-122.4194155, 37.7749295])

        event1 = await Event(
            title="Coltrane Motion @ Double Door", venue=venue1
        ).asave()
        event2 = await Event(
            title="Coltrane Motion @ Bottom of the Hill", venue=venue2
        ).asave()
        event3 = await Event(
            title="Coltrane Motion @ Empty Bottle", venue=venue1
        ).asave()

        # find all events "near" pitchfork office, Chicago.
        # note that "near" will show the San Francisco event, too,
        # although it sorts to last.
        events = await Event.aobjects(
            venue__location__near=[-87.67892, 41.9120459]
        ).to_list()
        assert events == [event1, event3, event2]

    async def test_geo_spatial_embedded(self):
        """Make sure GeoPointField works properly in an embedded document."""
        await self._test_embedded(point_field_class=GeoPointField)

    async def test_2dsphere_point_embedded(self):
        """Make sure PointField works properly in an embedded document."""
        await self._test_embedded(point_field_class=PointField)

    async def test_spherical_geospatial_operators(self):
        """Ensure that spherical geospatial queries are working."""

        class Point(Document):
            location = GeoPointField()

        await Point.adrop_collection()

        # These points are one degree apart, which (according to Google Maps)
        # is about 110 km apart at this place on the Earth.
        north_point = await Point(location=[-122, 38]).asave()  # Near Concord, CA
        south_point = await Point(location=[-122, 37]).asave()  # Near Santa Cruz, CA

        earth_radius = 6378.009  # in km (needs to be a float for dividing by)

        # Finds both points because they are within 60 km of the reference
        # point equidistant between them.
        points = Point.aobjects(location__near_sphere=[-122, 37.5])
        assert await points.to_list() == [north_point, south_point]

        # Same behavior for _within_spherical_distance
        points = Point.aobjects(
            location__within_spherical_distance=[[-122, 37.5], 60 / earth_radius]
        )
        assert await points.count() == 2

        points = Point.aobjects(
            location__near_sphere=[-122, 37.5], location__max_distance=60 / earth_radius
        )
        assert await points.to_list() == [north_point, south_point]

        # Test query works with max_distance, being farer from one point
        points = Point.aobjects(
            location__near_sphere=[-122, 37.8], location__max_distance=60 / earth_radius
        )
        close_point = await points.first()
        assert await points.to_list() == [north_point]

        # Test query works with min_distance, being farer from one point
        points = Point.aobjects(
            location__near_sphere=[-122, 37.8], location__min_distance=60 / earth_radius
        )
        far_point = await points.first()
        assert await points.to_list() == [south_point]
        assert close_point != far_point

        # Finds both points, but orders the north point first because it's
        # closer to the reference point to the north.
        points = Point.aobjects(location__near_sphere=[-122, 38.5])
        assert await points.to_list() == [north_point, south_point]

        # Finds both points, but orders the south point first because it's
        # closer to the reference point to the south.
        points = Point.aobjects(location__near_sphere=[-122, 36.5])
        assert await points.to_list() == [south_point, north_point]

        # Finds only one point because only the first point is within 60km of
        # the reference point to the south.
        points = Point.aobjects(
            location__within_spherical_distance=[[-122, 36.5], 60 / earth_radius]
        )
        assert await points.count() == 1
        assert (await points.to_list())[0].id == south_point.id

    async def test_linestring(self):
        class Road(Document):
            name = StringField()
            line = LineStringField()

        Road.adrop_collection()

        road = Road(name="66", line=[[40, 5], [41, 6]])
        await road.asave()

        # near
        point = {"type": "Point", "coordinates": [40, 5]}
        roads = Road.aobjects.filter(line__near=point["coordinates"])
        if PYMONGO_VERSION < (4,):
            assert roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(line__near=point)
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(line__near={"$geometry": point})
        assert await roads.to_list() == [road]

        # Within
        polygon = {
            "type": "Polygon",
            "coordinates": [[[40, 5], [40, 6], [41, 6], [41, 5], [40, 5]]],
        }
        roads = Road.aobjects.filter(line__geo_within=polygon["coordinates"])
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(line__geo_within=polygon)
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(line__geo_within={"$geometry": polygon})
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        # Intersects
        line = {"type": "LineString", "coordinates": [[40, 5], [40, 6]]}
        roads = Road.aobjects.filter(line__geo_intersects=line["coordinates"])
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(line__geo_intersects=line)
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(line__geo_intersects={"$geometry": line})
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        polygon = {
            "type": "Polygon",
            "coordinates": [[[40, 5], [40, 6], [41, 6], [41, 5], [40, 5]]],
        }
        roads = Road.aobjects.filter(line__geo_intersects=polygon["coordinates"])
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(line__geo_intersects=polygon)
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(line__geo_intersects={"$geometry": polygon})
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

    async def test_polygon(self):
        class Road(Document):
            name = StringField()
            poly = PolygonField()

        await Road.adrop_collection()

        road = Road(name="66", poly=[[[40, 5], [40, 6], [41, 6], [40, 5]]])
        await road.asave()

        # near
        point = {"type": "Point", "coordinates": [40, 5]}
        roads = Road.aobjects.filter(poly__near=point["coordinates"])
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(poly__near=point)
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(poly__near={"$geometry": point})
        assert await roads.to_list() == [road]

        # Within
        polygon = {
            "type": "Polygon",
            "coordinates": [[[40, 5], [40, 6], [41, 6], [41, 5], [40, 5]]],
        }
        roads = Road.aobjects.filter(poly__geo_within=polygon["coordinates"])
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(poly__geo_within=polygon)
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(poly__geo_within={"$geometry": polygon})
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        # Intersects
        line = {"type": "LineString", "coordinates": [[40, 5], [41, 6]]}
        roads = Road.aobjects.filter(poly__geo_intersects=line["coordinates"])
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(poly__geo_intersects=line)
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(poly__geo_intersects={"$geometry": line})
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        polygon = {
            "type": "Polygon",
            "coordinates": [[[40, 5], [40, 6], [41, 6], [41, 5], [40, 5]]],
        }
        roads = Road.aobjects.filter(poly__geo_intersects=polygon["coordinates"])
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(poly__geo_intersects=polygon)
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

        roads = Road.aobjects.filter(poly__geo_intersects={"$geometry": polygon})
        assert await roads.count() == 1
        assert await roads.to_list() == [road]

    async def test_aspymongo_with_only(self):
        """Ensure as_pymongo works with only"""

        class Place(Document):
            location = PointField()

        await Place.adrop_collection()
        p = Place(location=[24.946861267089844, 60.16311983618494])
        await p.asave()
        qs = Place.aobjects().only("location")
        assert (await qs.as_pymongo().to_list())[0]["location"] == {
            "type": "Point",
            "coordinates": [24.946861267089844, 60.16311983618494],
        }

    async def test_2dsphere_point_sets_correctly(self):
        class Location(Document):
            loc = PointField()

        await Location.adrop_collection()

        await Location(loc=[1, 2]).asave()
        loc = (await Location.aobjects.as_pymongo().to_list())[0]
        assert loc["loc"] == {"type": "Point", "coordinates": [1, 2]}

        await Location.aobjects.update(set__loc=[2, 1])
        loc = (await Location.aobjects.as_pymongo().to_list())[0]
        assert loc["loc"] == {"type": "Point", "coordinates": [2, 1]}

    async def test_2dsphere_linestring_sets_correctly(self):
        class Location(Document):
            line = LineStringField()

        await Location.adrop_collection()

        await Location(line=[[1, 2], [2, 2]]).asave()
        loc = (await Location.aobjects.as_pymongo().to_list())[0]
        assert loc["line"] == {"type": "LineString", "coordinates": [[1, 2], [2, 2]]}

        await Location.aobjects.update(set__line=[[2, 1], [1, 2]])
        loc = (await Location.aobjects.as_pymongo().to_list())[0]
        assert loc["line"] == {"type": "LineString", "coordinates": [[2, 1], [1, 2]]}

    async def test_geojson_PolygonField(self):
        class Location(Document):
            poly = PolygonField()

        await Location.adrop_collection()

        await Location(poly=[[[40, 5], [40, 6], [41, 6], [40, 5]]]).asave()
        loc = (await Location.aobjects.as_pymongo().to_list())[0]
        assert loc["poly"] == {
            "type": "Polygon",
            "coordinates": [[[40, 5], [40, 6], [41, 6], [40, 5]]],
        }

        await Location.aobjects.update(set__poly=[[[40, 4], [40, 6], [41, 6], [40, 4]]])
        loc = (await Location.aobjects.as_pymongo().to_list())[0]
        assert loc["poly"] == {
            "type": "Polygon",
            "coordinates": [[[40, 4], [40, 6], [41, 6], [40, 4]]],
        }


if __name__ == "__main__":
    unittest.main()
