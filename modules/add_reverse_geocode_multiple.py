from modules.custom_functions import addReverseGeocodedToDataFrame, exportToFile
import os
from pandas import read_csv


def main():
    MAPBOX_TOKEN = "MAPBOX_TOKEN"
    token = os.environ.get(MAPBOX_TOKEN)
    files_path = input("files_path: ")
    files = os.listdir(files_path)
    for file in files:
        try:

            file_path = os.path.join(files_path, file)
            output_name = file.split(".")[0]
            output_path = os.path.join(f"output/carto-data/{output_name}")
            print(output_path)
            df = read_csv(file_path)
            output_df = addReverseGeocodedToDataFrame(
                df=df, token=token, lat_column="latitude", lon_column="longitude", name=output_name)
            exportToFile(output_df, "csv", output_path)
        except Exception as e:
            print(e)


if (__name__ == "__main__"):
    main()
