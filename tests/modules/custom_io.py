import json
from pandas import DataFrame


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
