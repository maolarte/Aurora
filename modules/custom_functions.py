from pandas import DataFrame, Series
from copy import deepcopy
from datetime import datetime, timezone
import json
import re
from tqdm import tqdm

default_missing_value = 999999


def getProgressIndicator(data: iter, desc: str, size: int, unit: str):
    """
    Shows progressing of a looping function.
    """
    return tqdm(total=len(data), desc=desc, size=size, unit=unit)


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
    """
    return a column that has processed codified values
    """
    reversed_values_dict = dict([(x[1], x[0]) for x in values_dict.items()])
    return dfColumn.apply(lambda x: codifyServices(x, reversed_values_dict, other_value))


def processMultValueColumns(df: DataFrame, columnObjectsList: dict[dict]):
    """
    df: DataFrame object
    columnsObjectsList: dictionary of column object
    columnObject: dictionary {"target_column": str, "output_column": str, values_dict: dict, other_value: str}

    return DataFrame Object
    """
    for columnObject in columnObjectsList:
        try:
            current = columnObjectsList[columnObject]
            target_column = current["target_column"]
            output_column = current["output_column"]
            values_dict = current["values_dict"]
            other_value = str(current["other_value"])
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
    """
    return value replaced with a value in a dictonary.
    """
    if (type(value) == float):
        return otherValue

    if (len(value) == 0):
        return otherValue

    try:
        return values_dict[value.strip()]
    except Exception as e:
        return value


def processValueReplacement(df: DataFrame, replace_dict: dict[str, dict[str, str]]):
    """
    return DataFrame with some values replaced using a dictonary
    """
    local_df = deepcopy(df)
    columns = list(replace_dict.keys())

    for col in columns:
        local_df[col] = local_df[col].apply(
            lambda x: valueLabelChange(x, replace_dict[col], "Otro"))

    return local_df


def capitaliseColumns(df: DataFrame, columns: list[str]):
    """
    return DataFrame with column values that have been capitalised.
    """
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


def changeCountriesByExpression(country: str, valueDict: dict[str, str]):
    """
    return a country name value after being testing through an expresion
    """
    for key, value in valueDict.items():
        match = re.match(r"^"+key+r".$", country)
        if (match):
            return value

    return country


def processCountries(countries: list[str], valueDict: dict[str, str]):
    """
    return list of country name that some of which will be replaced by using a dictionary
    """
    output = []
    for country in countries:
        try:
            new_country = changeCountriesByExpression(
                country=country, valueDict=valueDict)
            output.append(new_country)
        except Exception as e:
            output.append(default_missing_value)
    return output


def toUnixTimestamp(time, format: str = "%d/%m/%Y"):
    """
    return number in unixtime after parsing a date string
    """
    start = datetime(1970, 1, 1)
    target = datetime.strptime(time, format)
    in_seconds = (target - start).total_seconds()
    in_milliseconds = int(in_seconds) * 1000
    return in_milliseconds
