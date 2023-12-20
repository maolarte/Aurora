from pandas import DataFrame, Series, concat
from geopandas import GeoDataFrame
from copy import deepcopy
from datetime import datetime, timezone
import json
import re
import geopandas as gpd
from time import sleep
from tqdm import tqdm
from shapely.geometry import Point

# Mapbox
from mapbox import Geocoder

default_value = 999999


def getProgressIndicator(data: iter, desc: str, size: int, unit: str):
    """
    Shows progressing of a looping function.
    """
    return tqdm(total=len(data), desc=desc)


def loadLocalJsonDoc(filepath, dataProp=''):
    """
    return deserialised json in dictionary

    Parameters
    ----------
    filepath: file location or buffer.
    dataProp: (optional) specified property to access required data
    """
    output = {}
    with open(file=filepath, mode='r', encoding='utf-8') as f:
        json_load = json.load(f)
        if (dataProp):
            output = json_load[dataProp]
        else:
            output = json_load
    return output


def toUnixTimestamp(time, format: str = "%d/%m/%Y"):
    """
    Returns timestring to unixtimestamp
    """
    start = datetime(1970, 1, 1)
    target = datetime.strptime(time, format)
    in_seconds = (target - start).total_seconds()
    in_milliseconds = int(in_seconds) * 1000
    return in_milliseconds


def datetimetoUnixTimestamp(value: datetime):
    """
    Returns  unixtimestamp from datetime value
    """
    start = datetime(1970, 1, 1).date()
    target = value.date()
    time_diff = target - start
    in_seconds = time_diff.total_seconds()
    in_milliseconds = int(in_seconds) * 1000
    return in_milliseconds


def toUnixTimestampMultiFormatted(time: str, formats: list[str]):
    """
    Returns  unixtimestamp while taking in multiple datetime string formats
    """

    for format_str in formats:
        try:
            # Parse the date with the specified format
            target = datetime.strptime(time, format_str)

            # Convert the datetime to UTC if it doesn't have a timezone
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)

            # Calculate the Unix timestamp in milliseconds
            in_seconds = (target - datetime(1970, 1, 1,
                                            tzinfo=timezone.utc)).total_seconds()
            in_milliseconds = int(in_seconds) * 1000

            return in_milliseconds
        except ValueError:
            continue


def codifyServices(value: str, values_dict: dict[str, int], otherValue: str):
    """
    Return codified value based on value dictonary
    """
    if (type(value) == float or type(value) == int):
        return otherValue
    raw_values = value.split(" ")
    output = []
    for value in raw_values:
        try:
            codedValue = values_dict[value]
            output.append(str(codedValue))
        except Exception as e:
            output.append(otherValue)

    return "|".join(output)


def processColumn(dfColumn: Series, values_dict: dict[int, str], other_value: str):
    reversed_values_dict = dict([(x[1], x[0]) for x in values_dict.items()])
    return dfColumn.apply(lambda x: codifyServices(x, reversed_values_dict, other_value))


def processMultValueColumns(df: DataFrame, columnObjectsList: list[dict]):
    """
    df: DataFrame object
    columnsObjectsList: list of column object
    columnObject: dictionary {"target_column": str, "output_column": str, values_dict: dict, other_value: str}

    return DataFrame Object
    """
    for columnObject in columnObjectsList:
        try:
            target_column = columnObject["target_column"]
            output_column = columnObject["output_column"]
            values_dict = columnObject["values_dict"]
            other_value = str(columnObject["other_value"])
            df[output_column] = processColumn(
                df[target_column], values_dict, other_value)
        except Exception as e:
            print(e)
            continue
    return df


def exportToFile(df: DataFrame, fileType: str, exportName: str):
    """ 
    EXport DataFrame to csv or json.

    Parameters
    -----------
    df: Pandas DataFrame object
    fileType: Either "csv" or "json"
    exportName: File location
    """
    if (fileType == "csv"):
        name = f"{exportName}.csv"
        df.to_csv(name, index=False)
        print(f"data export to {name}")
    else:
        name = f"{exportName}.json"
        df.to_json(name, orient="records", index=False)
        print(f"data export to {name}")


def valueLabelChange(value: str, values_dict: dict[str, str], otherValue: str):
    if (type(value) == float):
        return otherValue

    if (len(value) == 0):
        return otherValue

    try:
        return values_dict[value.strip()]
    except Exception as e:
        return value


def processValueReplacement(df: DataFrame, replace_dict: dict[str, dict[str, str]]):
    local_df = deepcopy(df)
    columns = list(replace_dict.keys())

    for col in columns:
        local_df[col] = local_df[col].apply(
            lambda x: valueLabelChange(x, replace_dict[col], "Otro"))

    return local_df


def capitaliseColumns(df: DataFrame, columns: list[str]):
    local_df = deepcopy(df)
    for col in columns:
        try:
            local_df[col] = local_df[col].str.title()
        except Exception as e:
            continue
    return local_df


def replaceOrganisation(df: DataFrame, value: str, col1: str, col2: str):
    local_df = deepcopy(df)
    output_values = []
    _col1 = list(local_df[col1].to_list())
    _col2 = list(local_df[col2].to_list())
    _merge = zip(_col1, _col2)
    for (target, replacement) in _merge:
        if (type(target) == float):
            output_values.append("Otros")
        elif (target == value):
            if (type(replacement) == str):
                output_values.append(replacement)
            else:
                output_values.append("Otros")
        else:
            output_values.append(target)

    local_df[col1] = Series(output_values)
    return local_df


def changeCountriesByExpression(country, valueDict: dict[str, str]):
    output = ""
    for key, value in valueDict.items():
        match = re.match(r"^"+key+r".$", country)
        if (match):
            return value

    return output if len(output) else country


def processCountries(countries: list[str], valueDict: dict[str, str]):
    output = []
    for country in countries:
        try:
            new_country = changeCountriesByExpression(
                country=country, valueDict=valueDict)
            output.append(new_country)
        except Exception as e:
            output.append(default_value)
    return output


def getCountriesWithCoordinates(countries: list[str], geo_countries: gpd.GeoDataFrame):
    output = {}
    for country in countries:
        try:
            filtered_country = geo_countries[geo_countries["NAME"].str.lower(
            ) == country].reindex()
            centroidValue = (filtered_country.centroid).iloc[0]
            output[country] = {"x": centroidValue.x, "y": centroidValue.y}
        except Exception as e:
            print(e)
            output[country] = {"x": default_value, "y": default_value}

    return output


def toUnixTimestamp(time, format: str = "%d/%m/%Y"):
    start = datetime(1970, 1, 1)
    target = datetime.strptime(time, format)
    in_seconds = (target - start).total_seconds()
    in_milliseconds = int(in_seconds) * 1000
    return in_milliseconds


def getCoordinate(value: str, side: str, valueDict: dict[str, tuple[int, int]], expressionDict: dict[str, str]):
    try:
        country = changeCountriesByExpression(value, expressionDict)
        return valueDict[country][side]
    except Exception as e:
        return default_value


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
        output[f"{id}_place_name"] = feature["place_name"]
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
                    axis=1).fillna(default_value)
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
