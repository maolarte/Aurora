import pandas as pd
import numpy as np
from pandas import DataFrame
from pandas import Series
from copy import deepcopy
from datetime import datetime
import json


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
    start = datetime(1970, 1, 1)
    target = datetime.strptime(time, format)
    in_seconds = (target - start).total_seconds()
    in_milliseconds = int(in_seconds) * 1000
    return in_milliseconds


def codifyServices(value: str, values_dict: dict[str, int], otherValue: str):
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
    df -> Pandas DataFrame object
    fileType -> Either "csv" or "json"
    exportName -> File location
    """
    if (fileType == "csv"):
        name = f"{exportName}.csv"
        df.to_csv(name)
        print(f"data export to {name}")
    else:
        name = f"{exportName}.json"
        df.to_json(name, orient="records")
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
        local_df[col] = local_df[col].str.title()
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

    local_df[col1] = pd.Series(output_values)
    return local_df


defaultMissingValue = 999999

# Import dataset
raw_data = input("raw_data_path: ")
output_path = input("output_path: ")

services = pd.read_csv(raw_data, sep=';', index_col=False)

# Replace "other in organizations"
services = replaceOrganisation(
    services, 'otra', 'organizacionimplementadora', 'cualotraorganizacionimp')

services = replaceOrganisation(
    services, 'otra', 'organizacionprincipal', 'cualotraorganizacionprin')

services = replaceOrganisation(
    services, 'otra', 'organizacionpertence', 'cualotraorganizacionprin')


# Fill  missing values of variables of attended separated children
common = services['nnanoacompanados'].isnull()
common1 = services['nnaseparados'].isnull()
condition = [(services['numeronna'] == 0) & (common)]
condition1 = [(services['numeronna'] == 0) & (common)]
fill_with = ['no']

services['nnanoacompanados'] = np.select(
    condition, fill_with, default=services['nnanoacompanados'])
services['nnaseparados'] = np.select(
    condition1, fill_with, default=services['nnaseparados'])

# Fill  missing values
services = services.fillna(defaultMissingValue)

# rename variables
newColumns = loadLocalJsonDoc("defaults/rename_columns.json")
services_carto = services.rename(columns=newColumns)

# serv_tipo (separating by pipe symbol and categorized with number)
codify_dict = loadLocalJsonDoc("defaults/codification_dict.json")
services_dict = codify_dict["services_dict"]

# re structure variable cuenta_con
cuenta_con_dict = codify_dict["cuenta_con_dict"]

# re structure variable children services (cual_serv1)
cual_serv1_dict = codify_dict["cual_serv1_dict"]

# re structure variable women services (cual_ser_2)
cual_ser_2_dict = codify_dict["cual_ser_2_dict"]

# re structure variable data storage (almacenamientoregistros)
registro_dict = codify_dict["registro_dict"]

# variable funding
financ_dict = codify_dict["financ_dict"]

#  variable challenges
reto_dict = codify_dict["reto_dict"]

# variable lenguages
idio_dict = codify_dict["idio_dict"]

# variable medios
medio_dict = codify_dict["medio_dict"]

values = [
    {
        "target_column": "serv_tipo",
        "output_column": "serv_tipo1",
        "values_dict": services_dict,
        "other_value": defaultMissingValue
    },
    {
        "target_column": "cuenta_con",
        "output_column": "cuenta_c_1",
        "values_dict": cuenta_con_dict,
        "other_value": defaultMissingValue
    },
    {
        "target_column": "cual_serv1",
        "output_column": "cual_ser_1",
        "values_dict": cual_serv1_dict,
        "other_value": defaultMissingValue
    },
    {
        "target_column": "cual_ser_2",
        "output_column": "cual_ser_3",
        "values_dict": cual_ser_2_dict,
        "other_value": defaultMissingValue
    },
    {
        "target_column": "almacenamientoregistros",
        "output_column": "almacenamientoregistros_",
        "values_dict": cual_ser_2_dict,
        "other_value": defaultMissingValue
    },
    {
        "target_column": "financiamiento",
        "output_column": "financb",
        "values_dict": financ_dict,
        "other_value": defaultMissingValue
    },
    {
        "target_column": "princ_reto",
        "output_column": "princ_re_1",
        "values_dict": reto_dict,
        "other_value": defaultMissingValue
    },
    {
        "target_column": "idioma_ent",
        "output_column": "idioma_e_1",
        "values_dict": idio_dict,
        "other_value": defaultMissingValue
    },
    {
        "target_column": "medios_bri",
        "output_column": "medios_b_1",
        "values_dict": medio_dict,
        "other_value": defaultMissingValue
    },
]

replace_lib = loadLocalJsonDoc("defaults/value_replacements.json")
services_carto = processValueReplacement(services_carto, replace_lib)

_capitalise_columns = loadLocalJsonDoc(
    filepath="defaults/column_capitalisation.json")

services_carto = capitaliseColumns(services_carto, _capitalise_columns)

# parsing date field into unix timestamp
services_carto["timeunix"] = services_carto["fecha"].apply(
    lambda x: toUnixTimestamp(time=x, format="%Y-%m-%d"))

output_df = processMultValueColumns(services_carto, values)
exportToFile(output_df, "csv", output_path)
