import sys
from pandas import merge, read_csv, read_json
from modules.custom_functions import loadLocalJsonDoc, processCountries,  toUnixTimestamp
from modules.custom_geo_functions import addReverseGeocodedToDataFrame, dataFrameToGeoDataFrame, processFieldCoordinates, getCountriesWithCoordinates
from modules.custom_io import uploadDataFrameToCarto, getCartoClient, useCartoAuth, exportDataFrameToFile
import os
from google.cloud import bigquery
from argparse import ArgumentParser

defaultMissingValue = 999999


def main(cara_path: str, feedback_path: str, destination: str = "", output_path: str = "", output_format: str = "csv"):

    working_dir = os.getcwd()

    # Import dataset
    aurora_cara = read_csv(
        filepath_or_buffer=cara_path)
    aurora_feedback = read_csv(
        filepath_or_buffer=feedback_path)

    # merging first connection files (caracterization and feedback)
    aurora = merge(aurora_cara, aurora_feedback)

    # Drop observations of Aurora team phones, test registers and geographical atypical rows
    user_ids_to_remove = loadLocalJsonDoc(
        os.path.join(working_dir, "defaults", "test_user_ids.json"))

    for user_id in user_ids_to_remove:
        aurora = aurora.drop(aurora[aurora.UserId == user_id].index)

    # Filtering by geographical errors and informed consent
    aurora = aurora[aurora['Consentimiento'] != 'NO']
    aurora = aurora[aurora['Latitud'] != "None"]
    # Rename variables to be consistent with the first round exercise.
    newColumns = loadLocalJsonDoc(os.path.join(
        working_dir, "defaults", "aurora_column_name.json"))

    aurora_carto = aurora.rename(columns=newColumns)
    # Adding coordinates of variables (país de nacimiento, país donde inicio el viaje and país donde vivía hace un año)

    available_countries = [x.lower() for x in list(set(list(aurora_carto["e08_pais_"].unique(
    )) + list(aurora_carto["e10_pais_"].unique()) + list(aurora_carto["e12_pais_"].unique()))) if type(x) == str]

    countries_dict = loadLocalJsonDoc(os.path.join(
        working_dir, "defaults", "countries_dict.json"))

    available_countries = processCountries(available_countries, countries_dict)

    country_df = read_json(
        path_or_buf="defaults/countries.json", orient='records')

    countriesWithCoordinates = getCountriesWithCoordinates(
        available_countries, country_df)
    country_column_dict = loadLocalJsonDoc(os.path.join(
        working_dir, "defaults", "country_column_dict.json"))
    aurora_carto = processFieldCoordinates(
        aurora_carto, country_column_dict, countriesWithCoordinates, countries_dict)

    aurora_carto['lon_eng'] = aurora_carto['lon']
    aurora_carto['lat_eng'] = aurora_carto['lat']
    aurora_carto['longitude'] = aurora_carto['lon']
    aurora_carto['latitude'] = aurora_carto['lat']
    # Create the variable time
    # the format was missing other elements thus the timestring was not parsing
    aurora_carto["timeunix"] = aurora_carto["Inicio interacción"].apply(
        lambda x: toUnixTimestamp(x, '%Y-%m-%d %H:%M:%S.%f+00:00'))
    # Adding coordinates of variables (país de nacimiento, país donde inicio el viaje and país donde vivía hace un año)
    MAPBOX_TOKEN = os.environ.get("MAPBOX_TOKEN")
    # This is heavy process that takes a while to finish
    # should be used sparingly and closer to end processes.
    aurora_carto = addReverseGeocodedToDataFrame(
        df=aurora_carto, token=MAPBOX_TOKEN, lat_column="latitude", lon_column="longitude", name="Auora")

    # After checking some coordinates, especially in country borders where info was collected, and using some extra info as the name collector, there are some country names that were changed
    # This was did for Chile and Colombia
    # Colombia
    user_ids_col = loadLocalJsonDoc(
        os.path.join(working_dir, "defaults", "user_ids_col.json"))
    condition_eng = '¿Cómo interactúa con el sistema?'
    condition_value = 'Enganche'
    new_country_value = 'Colombia'
    condition_col = (aurora_carto['objectid'].isin(user_ids_col)) & (
        aurora_carto[condition_eng] == condition_value)
    # Use the loc method to update the 'country_name' column
    aurora_carto.loc[condition_col, 'country_name'] = new_country_value

   # Chile
    user_ids_ch = loadLocalJsonDoc(
        os.path.join(working_dir, "defaults", "user_ids_ch.json"))
    new_country_value1 = 'Chile'
    condition1 = (aurora_carto['objectid'].isin(user_ids_ch)) & (
        aurora_carto[condition_eng] == condition_value)
    # Use the loc method to update the 'country_name' column
    aurora_carto.loc[condition1, 'country_name'] = new_country_value1

    # Fix the coordinates of the rows changed before (lines 78-98) that did not has coordinates (1 observation Chile, 2 Colombia)
    aurora_carto.loc[aurora_carto['objectid'] == 313172106, 'lat'] = -18.475525
    aurora_carto.loc[aurora_carto['objectid']
                     == 313172106, 'lon'] = -70.3137029

    aurora_carto.loc[aurora_carto['objectid'] == 320582739, 'lat'] = 8.42152826
    aurora_carto.loc[aurora_carto['objectid']
                     == 320582739, 'lon'] = -76.78180133

    aurora_carto.loc[aurora_carto['objectid'] == 325857664, 'lat'] = 8.42152826
    aurora_carto.loc[aurora_carto['objectid']
                     == 325857664, 'lon'] = -76.78180133

    aurora_carto.loc[aurora_carto['objectid'] ==
                     317308056, 'country_name'] = "Colombia"
    aurora_carto.loc[aurora_carto['objectid']
                     == 319708059, 'country_name'] = "Chile"

    # filling missing values
    # should be done at the very end
    aurora_carto = aurora_carto.fillna(defaultMissingValue)

    if (len(output_path) > 0):
        exportDataFrameToFile(
            df=aurora_carto, fileType=output_format, exportName=output_path)
        return

    # database for Carto
    if (bool(destination)):
        output_df = dataFrameToGeoDataFrame(
            df=aurora_carto, geometry_column_name="geom", lat_column="latitude", long_column="longitude")

        carto_auth = useCartoAuth()

        carto_client = getCartoClient(carto_auth)

        config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE",)

        uploadDataFrameToCarto(cartDW=carto_client, df=output_df,
                               destination=destination, config=config)
        return

    print("No file was exported")


parser = ArgumentParser()
if __name__ == "__main__":
    parser.add_argument('--cara_path', type=str, required=True,
                        help="File location path for Aurora Characterization data")
    parser.add_argument('--ayuda_path', type=str, required=True,
                        help="File location path for Aurora Feedback data")
    parser.add_argument('--destination', type=str,
                        help='Carto data warehouse endpoint')
    parser.add_argument('--output', type=str,
                        help='Output name or output path')

    parser.add_argument('--format', type=str, choices=[
                        'csv', 'json'], default='csv', help='Output format if output path is given')

    args = parser.parse_args()

    cara_path = args.cara_path
    ayuda_path = args.ayuda_path
    destination = args.destination
    output_path = args.output
    output_format = args.format

    if (not bool(cara_path)):
        print("Please add both Characterization data path")
        sys.exit()

    if (not bool(ayuda_path)):
        print("Please add both Feedback data path")
        sys.exit()

    if ((not bool(output_path)) & (not bool(destination))):
        print("Print add at least one output method")
        sys.exit()

    main(cara_path=cara_path, feedback_path=ayuda_path,
         output_path=output_path, output_format=output_format, destination=destination)
