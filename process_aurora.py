from pandas import merge, read_csv
from geopandas import read_file as read_geo_file
from modules.custom_functions import loadLocalJsonDoc, processCountries, getCountriesWithCoordinates, addReverseGeocodedToDataFrame, exportToFile, processFieldCoordinates, toUnixTimestamp
import os
import fsspec

defaultMissingValue = 999999


def main():
    aurora_cara_path = input("Aurora Cara Path: ")
    aurora_feedback_path = input("Aurora Feedback Path: ")
    aurora_monitoreos_path = input("Aurora Feedback Path: ")
    output_path = input("Output Path: ")

    # Import dataset
    aurora_cara = read_csv(
        filepath_or_buffer=aurora_cara_path, date_format="", parse_dates="")
    aurora_feedback = read_csv(
        filepath_or_buffer=aurora_feedback_path, date_format="", parse_dates="")
    aurora_monitoreos = read_csv(
        filepath_or_buffer=aurora_monitoreos_path, date_format="", parse_dates="")

    # merging first connection files (caracterization and feedback)
    aurora = merge(aurora_cara, aurora_feedback, aurora_monitoreos)

    # Drop observations of Aurora team phones, test registers and geographical atypical rows
    user_ids_to_remove = [311571598, 311398466, 311396734, 311361421, 311361350, 311361257, 311337494, 311325070,
                          311325038, 311272934, 310820267, 310543580, 310357249, 310191611, 308421831, 306028996,
                          310191611, 308421831, 306028996, 311725039, 311719001, 311718121, 311699383, 311696700,
                          312179120, 311965863, 311965863, 316773170, 311440316, 313260546, 316563135, 316734459,
                          317064115]

    for user_id in user_ids_to_remove:
        aurora = aurora.drop(aurora[aurora.UserId == user_id].index)

    # Filtering by geographical errors, informed consent and first connections by QR
    aurora = aurora[aurora['Consentimiento'] != 'NO']
    # has to be analized if we include these observations or not
    aurora = aurora[aurora['¿Cómo interactúa con el sistema?']
                    != 'QR-Enganche']
    aurora = aurora[aurora['Latitud'] != "None"]
    # Rename variables to be consistent with the fist round exercise.
    newColumns = loadLocalJsonDoc("/defaults/aurora_column_name.json")

    aurora_carto = aurora.rename(columns=newColumns)
    # Adding coordinates of variables (país de nacimiento, país donde inicio el viaje and país donde vivía hace un año)

    available_countries = [x.lower() for x in list(set(list(aurora_carto["e08_pais_"].unique(
    )) + list(aurora_carto["e10_pais_"].unique()) + list(aurora_carto["e12_pais_"].unique()))) if type(x) == str]

    countries_dict = loadLocalJsonDoc("defaults/countries_dict.json")

    available_countries = processCountries(available_countries, countries_dict)

    # adding coordinates value
    country_data_path = "simplecache::https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/110m/cultural/ne_110m_admin_0_countries.zip"

    country_df = ""

    with fsspec.open(country_data_path) as file:
        country_df = read_geo_file(file)

    countriesWithCoordinates = getCountriesWithCoordinates(
        available_countries, country_df)
    country_column_dict = loadLocalJsonDoc("defaults/country_column_dict.json")
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
    aurora_carto = addReverseGeocodedToDataFrame(aurora_carto, MAPBOX_TOKEN)
    # filling missing values
    # should be done at the very end
    aurora_carto = aurora_carto.fillna(defaultMissingValue)
    # database for Carto
    exportToFile(aurora_carto, "csv", output_path)


if __name__ == "__main__":
    main()
