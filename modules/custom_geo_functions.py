import sys
from pandas import Series
from geopandas import GeoSeries
from pandas import DataFrame, Series, concat
from geopandas import GeoDataFrame
from copy import deepcopy
from time import sleep
from shapely.geometry import Point
from .custom_functions import changeCountriesByExpression, getProgressIndicator

# Mapbox
from mapbox import Geocoder

default_missing_value = 999999


def getCoordinate(value: str, side: str, valueDict: dict[str, tuple[int, int]], expressionDict: dict[str, str]):
    try:
        country = changeCountriesByExpression(value, expressionDict)
        return valueDict[country][side]
    except Exception as e:
        return default_missing_value


def processFieldCoordinates(df: DataFrame, columnDict: dict[str, dict[str, str]], valueDict: dict[str, tuple[int, int]], expressionDict: dict[str, str]):
    local_df = deepcopy(df)
    for column in columnDict.keys():
        local_df[columnDict[column]["x"]] = local_df[column].str.lower().apply(
            lambda x: getCoordinate(x, "x", valueDict, expressionDict))
        local_df[columnDict[column]["y"]] = local_df[column].str.lower().apply(
            lambda x: getCoordinate(x, "y", valueDict, expressionDict))

    return local_df


def processGeocodeData(data):
    features = data['features']
    output = {}
    for feature in features:
        _id: str = feature['id']
        id = _id.split(".")[0]
        output[f"{id}_text"] = feature["text"]
        output[f"{id}_name"] = feature["place_name"]
        if (id == "country"):
            output["country_code"] = feature["properties"]["short_code"]
    return output


def getMapboxGeocoder(token: str):
    if (token):
        return Geocoder(access_token=token)
    else:
        raise Exception("Invalid Token")


def reverseGeocode(longitude: int, latitude: int, token: str):
    mb_geocoder = getMapboxGeocoder(token)
    response = mb_geocoder.reverse(lat=latitude, lon=longitude)
    if (response.status_code == 200):
        data = response.json()
        return data
    else:
        return None


def processReverseGeoding(data: list[tuple[int, int]], token: str, name: str):
    _output = []
    pbar = getProgressIndicator(
        data=data, desc=f"Processing Reverse Geocoding {name}", size=7, unit="coords")
    for lon, lat in data:
        try:
            result = reverseGeocode(lon, lat, token)
            _decoded = processGeocodeData(result)
            _output.append(_decoded)
            pbar.update(1)
            sleep(0.1)

        except Exception as e:
            print(e)
            _output.append([])

    return _output


def addReverseGeocodedToDataFrame(df: DataFrame, lon_column: str, lat_column: str, token: str, name: str):
    """
    Takes in a DataFrame with longitude and latitude to produce new fields containing geo-administrative attributes

    """
    local_df = deepcopy(df)
    coordinates = list(zip(list(local_df[lon_column].astype(float).to_list()), list(
        local_df[lat_column].astype(float).to_list())))
    reversed_geocoded = processReverseGeoding(coordinates, token, name)
    reversed_geocoded_df = DataFrame(reversed_geocoded)
    output = concat([local_df, reversed_geocoded_df],
                    axis=1).fillna(default_missing_value)
    return output


def dataFrameToGeoDataFrame(df: DataFrame, geometry_column_name: str, lat_column: str, long_column: str):
    """
    Returns GeoDataFrame from DataFrame with location fields
    """
    local_df = deepcopy(df)
    local_df[geometry_column_name] = Point(
        local_df[lat_column], local_df[long_column])
    local_geo_df = GeoDataFrame(local_df)
    return local_geo_df


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
