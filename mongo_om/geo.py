from typing import Literal

from geojson_pydantic import base, features, geometries
from geojson_pydantic.types import *


#  Patching GeoJsonBase clean_model method (monkey patch !!)
def __clean_model__(self, serializer, *_):
    data = serializer(self)
    # remove unnecesary fields.
    for field in getattr(self, "__geojson_exclude_if_none__"):
        if field in data and data[field] is None:
            del data[field]
    return data


base._GeoJsonBase.clean_model = __clean_model__  # type: ignore


class Point(geometries.Point):
    type: Literal["Point"] = "Point"


class MultiPoint(geometries.MultiPoint):
    type: Literal["MultiPoint"] = "MultiPoint"


class LineString(geometries.LineString):
    type: Literal["LineString"] = "LineString"


class MultiLineString(geometries.MultiLineString):
    type: Literal["MultiLineString"] = "MultiLineString"


class Polygon(geometries.Polygon):
    type: Literal["Polygon"] = "Polygon"


class MultiPolygon(geometries.MultiPolygon):
    type: Literal["MultiPolygon"] = "MultiPolygon"


class GeometryCollection(geometries.GeometryCollection):
    type: Literal["GeometryCollection"] = "GeometryCollection"


class Feature(features.Feature):
    type: Literal["Feature"] = "Feature"


class FeatureCollection(features.FeatureCollection):
    type: Literal["FeatureCollection"] = "FeatureCollection"
