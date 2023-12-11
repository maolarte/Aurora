from modules.custom_io import useCartoM2M, useCartoAuth, getCartoClient, uploadDataFrameToCarto
from pandas import DataFrame
from geopandas import GeoDataFrame
from shapely.geometry import Point
from google.cloud import bigquery

data = [
    {
        "lat": - 15.4,
        "long": 36.5,
        'index': 1,
    },
    {
        "lat": - 15.6,
        "long": 36.3,
        'index': 2
    },
    {
        "lat": - 15.1,
        "long": 36.9,
        'index': 3
    },
]

dataWithCoords = []

for item in data:
    item["geom"] = Point(item['long'], item['lat'])
    dataWithCoords.append(item)

df = GeoDataFrame(dataWithCoords)

m2m_file = r"C:\Users\DELL LATITUDE 7490\Desktop\code\python\immap\Aurora\secrets\carto_m2m.json"

carto_auth = useCartoAuth()

carto_client = getCartoClient(carto_auth)

config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE",)

uploadDataFrameToCarto(cartDW=carto_client, df=df,
                       destination="carto-dw-ac-4v8fnfsh.shared.pyloadTest", config=config)
