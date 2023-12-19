from custom_functions import exportToFile, addReverseGeocodedToDataFrame
from pandas import read_csv
import os


def main():
    file_path = input("file path: ")
    lat = input("lat column: ")
    lon = input("lon column: ")
    output_path = input("output file: ")

    MAPBOX_TOKEN = "MAPBOX_TOKEN"

    token = os.environ.get(MAPBOX_TOKEN)

    df = read_csv(filepath_or_buffer=file_path)

    output_df = addReverseGeocodedToDataFrame(
        df=df, lon_column=lon, lat_column=lat, token=token, name=file_path)

    exportToFile(output_df, "csv", output_path)


if (__name__ == "__main__"):
    main()
