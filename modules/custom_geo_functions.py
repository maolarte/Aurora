import sys
from pandas import Series
from geopandas import GeoSeries, GeoDataFrame
from pandas import DataFrame, Series, concat
from geopandas import GeoDataFrame
from copy import deepcopy
from shapely.geometry import Point
from .custom_functions import changeCountriesByExpression, getProgressIndicator

# Mapbox
from mapbox import Geocoder

default_missing_value = 999999


def getCountriesWithCoordinates(countries: list[str], geo_countries: DataFrame):
    output = {}
    for country in countries:
        try:
            filtered_country = geo_countries[geo_countries["name"].str.lower(
            ) == country].reindex()
            centroidValue = filtered_country.iloc[0]
            output[country] = {"x": centroidValue.x, "y": centroidValue.y}
        except Exception as e:
            print(e, "not associated with", country)
            output[country] = {"x": default_missing_value,
                               "y": default_missing_value}

    return output


def getCoordinate(value: str, side: str, valueDict: dict[str, tuple[int, int]], expressionDict: dict[str, str]):
    """
    return coordinate from dictionary as related to input value.
    """
    try:
        country = changeCountriesByExpression(value, expressionDict)
        return valueDict[country][side]
    except Exception as e:
        return default_missing_value


def processFieldCoordinates(df: DataFrame, columnDict: dict[str, dict[str, str]], valueDict: dict[str, tuple[int, int]], expressionDict: dict[str, str]):
    """
    return a dataframe with coordinates fields retrieved from a coordinates dictionary.
    """
    local_df = deepcopy(df)
    for column in columnDict.keys():
        local_df[columnDict[column]["x"]] = local_df[column].str.lower().apply(
            lambda x: getCoordinate(x, "x", valueDict, expressionDict))
        local_df[columnDict[column]["y"]] = local_df[column].str.lower().apply(
            lambda x: getCoordinate(x, "y", valueDict, expressionDict))

    return local_df


def processGeocodeData(data):
    """
    return extracted properties from results of reverse geocoding
    """
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
    """
    return a mapbox geocoder object
    """
    if (token):
        return Geocoder(access_token=token)
    else:
        raise Exception("Invalid Token")


def reverseGeocode(longitude: int, latitude: int, token: str):
    """
    return reverse geocoded data from coordinates using mapbox api
    """
    mb_geocoder = getMapboxGeocoder(token)
    response = mb_geocoder.reverse(lat=latitude, lon=longitude)
    if (response.status_code == 200):
        data = response.json()
        return data
    else:
        return None


def processReverseGeoding(data: list[tuple[int, int, int]], token: str, name: str):
    """
    return list of objects with geo-administrative properties
    """
    _output = []
    pbar = getProgressIndicator(
        data=data, desc=f"Processing Reverse Geocoding {name}", size=7, unit="coords")
    for id, lon, lat in data:
        try:
            result = reverseGeocode(lon, lat, token)
            _decoded = processGeocodeData(result)
            _decoded['id'] = id
            _output.append(_decoded)
            pbar.update(1)
            # sleep(0.1)

        except Exception as e:
            print(e)
            _output.append({"id": id})

    return _output


def addReverseGeocodedToDataFrame(df: DataFrame, lon_column: str, lat_column: str, token: str, name: str, id="objectid"):
    """
    return dataframe with geo-administrative attributes fields as related longitude and latitude of each row.

    """
    local_df = deepcopy(df)
    coordinates = list(zip(list(local_df[id].to_list()), list(local_df[lon_column].astype(float).to_list()), list(
        local_df[lat_column].astype(float).to_list())))
    reversed_geocoded = processReverseGeoding(coordinates, token, name)
    reversed_geocoded_df = DataFrame(reversed_geocoded)
    output = concat([local_df.set_index(id), reversed_geocoded_df.set_index("id")],
                    axis=1).reset_index(names=[id])
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
