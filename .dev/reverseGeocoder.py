import os
from pandas import DataFrame as DF
from modules.custom_functions import addReverseGeocodedToDataFrame


MAPBOX_TOKEN = os.environ.get("MAPBOX_TOKEN")
token = MAPBOX_TOKEN


data = [
    {
        "id": 1,
        "lat": -19.2752460,
        "long": -68.621534
    },
    {
        "id": 2,
        "lat": -19.275197,
        "long": -68.62144
    },
    {
        "id": 3,
        "lat": -18.48197,
        "long": -70.303048
    },
]

df = DF(data)

new_df = addReverseGeocodedToDataFrame(
    df=df, lat_column="lat", lon_column="long", token=token, name="Test Data")

print(new_df)

new_df.to_csv("tests/test.csv")
