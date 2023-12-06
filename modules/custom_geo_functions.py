import sys
from pandas import Series
from geopandas import GeoSeries
from shapely.geometry import Point


def getDistance(start: list[int], end: list[int]) -> float:
    """
    Return distance between two points

    Parameters
    ----------
    start: list of integers making up longitude and latitude
    end: list of integers making up longitude and latitude
    """
    try:
        start_geom = Point(*start)
        end_geom = Point(*end)
        start_sr = GeoSeries([start_geom])
        distance: Series = start_sr.distance(end_geom)
        return distance.iloc[0]
    except Exception as e:
        print(e)
        sys.exit()
