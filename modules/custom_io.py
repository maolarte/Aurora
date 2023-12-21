import json
from pandas import DataFrame
import carto_auth
from carto_auth import CartoAuth
from google.cloud.bigquery import Client


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


def exportDataFrameToFile(df: DataFrame, fileType: str, exportName: str):
    """ 
    df -> Pandas DataFrame object
    fileType -> Either "csv" or "json"
    exportName -> File location
    """
    if (fileType == "csv"):
        name = f"{exportName}.csv"
        df.to_csv(name, index=False)
        print(f"data export to {name}")
    else:
        name = f"{exportName}.json"
        df.to_json(name, orient="records", index=False)
        print(f"data export to {name}")


def useCartoM2M():
    file_path = input("File path: ")
    carto_auth = CartoAuth.from_m2m(filepath=file_path, use_cache=False)
    return carto_auth


def useCartoAuth():
    carto_auth = CartoAuth.from_oauth()
    return carto_auth


def getCartoClient(auth: CartoAuth):
    return auth.get_carto_dw_client()


def uploadDataFrameToCarto(cartDW: Client, df: DataFrame, destination: str,  config):
    job = cartDW.load_table_from_dataframe(
        dataframe=df, destination=destination, job_config=config)
    print("uploaded to "+destination)
    return job


def getCartoToken(auth: CartoAuth):
    return auth.get_access_token()


def uploadToCarto():
    return
