from pandas import merge, read_csv, to_datetime, DataFrame, concat
from geopandas import read_file as read_geo_file
from modules.custom_functions import addReverseGeocodedToDataFrame, toUnixTimestampMultiFormatted, dataFrameToGeoDataFrame
from modules.custom_io import useCartoAuth, uploadDataFrameToCarto, getCartoClient, exportDataFrameToFile
from google.cloud import bigquery
import os
import fsspec
from argparse import ArgumentParser


def main(cara_path: str, feedback_path: str, monitoreo_path: str, destination: str = "", output_path: str = "", output_format: str = "csv"):

    working_dir = os.getcwd()

    # Import dataset
    aurora_cara = read_csv(
        filepath_or_buffer=cara_path)
    aurora_feedback = read_csv(
        filepath_or_buffer=feedback_path)
    aurora_monitoreos = read_csv(
        filepath_or_buffer=monitoreo_path)
    # Merge tables of first connection
    aurora = merge(aurora_cara, aurora_feedback)

    # Adding the monitorings tables
    aurora_comple = aurora.append(aurora_monitoreos)
    # Drop observations of Aurora team phones, test registers and geographical atypical rows

    user_ids_to_remove = [311571598, 311398466, 311396734, 311361421, 311361350, 311361257, 311337494, 311325070,
                          311325038, 311272934, 310820267, 310543580, 310357249, 310191611, 308421831, 306028996,
                          310191611, 308421831, 306028996, 311725039, 311719001, 311718121, 311699383, 311696700,
                          312179120, 311965863, 311965863, 316773170, 311440316, 313260546, 316563135, 316734459,
                          317064115]

    for user_id in user_ids_to_remove:
        aurora_comple = aurora_comple.drop(
            aurora_comple[aurora_comple.UserId == user_id].index)

    # Variable of date in date format (for generating panel data)
    aurora_comple["fecha"] = to_datetime(
        aurora_comple["Inicio interacción"], format='%Y-%m-%d %H:%M:%S', errors='coerce', utc=True).dt.strftime('%Y-%m-%d')

    # Filling all observations of the same ID with the variable consent and actual country (variables of first connection)
    aurora_comple['País actual'] = aurora_comple.groupby(
        'UserId')['País actual'].transform('first')
    aurora_comple['Consentimiento'] = aurora_comple.groupby(
        'UserId')['Consentimiento'].transform('first')
    aurora_comple['País destino'] = aurora_comple.groupby(
        'UserId')['País destino'].transform('first')
    aurora_comple['Zona país'] = aurora_comple.groupby(
        'UserId')['Zona país'].transform('first')

    # filering by consent, cleaning geographical variables that are equal to "None" and drop the ones that can't identify
    # the zone of first connection
    aurora_comple = aurora_comple[aurora_comple['Consentimiento'] != 'NO']
    aurora_comple = aurora_comple[aurora_comple['Latitud'] != "None"]
    # Dropping duplicates of monitorings (same answers differents days)
    columns_to_consider = [col for col in aurora_comple.columns if col not in [
        'Inicio interacción', 'InteraID']]

    # Use drop_duplicates with the specified columns to consider
    aurora_comple = aurora_comple.drop_duplicates(subset=columns_to_consider)

    aurora_comple = aurora_comple.sort_values(
        by=['UserId', 'Inicio interacción'])

    # Reset the index for proper grouping
    aurora_comple = aurora_comple.reset_index(drop=True)

    # Create a new variable "Interaction_Sequence" based on the grouping
    aurora_comple['Interaction_Sequence'] = aurora_comple.groupby(
        'UserId').cumcount() + 1

    default_value = 999999
    # Rename variables
    newColumns = {
        "¿En qué país naciste?": 'e08_pais_',
        'Otro país de nacimiento': 'e09_otro_p',
        '¿En qué país iniciaste tu viaje actual?': 'e10_pais_',
        'Otro país de inicio': 'e11_otro_p',
        '¿En qué país vivías hace un año?': 'e12_pais_',
        'Otro país': 'e13_otro_p',
    }

    aurora_comple = aurora_comple.rename(columns=newColumns)

    # Creating the dataset for interactions across the route

    df = DataFrame(aurora_comple, columns=[
        'UserId', 'Latitud', 'Longitud', 'Inicio interacción', 'Interaction_Sequence'])

    possible_formats = ["%Y-%m-%d %H:%M:%S.%f+00:00",
                        "%d/%m/%Y %H:%M:%S.%f+00:00"]

    df["timeunix"] = df["Inicio interacción"].apply(
        lambda x: toUnixTimestampMultiFormatted(time=x, formats=possible_formats))
    # renaming variables
    newColumns = {
        'UserId': 'id',
        'Latitud': 'lat',
        'Longitud': 'lon',
        'Inicio interacción': 'date',

    }

    df = df.rename(columns=newColumns)
    df["date"] = to_datetime(
        df["date"], format='%Y-%m-%d %H:%M:%S', errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    # sortig by time
    df = df.sort_values(['id', 'timeunix'], ascending=[True, True])
    df['idx'] = df.groupby('id').cumcount()
    max_sequence_df = df.groupby(
        'id')['Interaction_Sequence'].max().reset_index()
    # Merge the result back to the original DataFrame based on 'id'
    df = merge(df, max_sequence_df, on='id',
               how='left', suffixes=('', '_max'))

    # Rename the new column to 'max_seq'
    df = df.rename(columns={'Interaction_Sequence_max': 'max_seq'})
    # adding coordinates value
    country_data_path = "simplecache::https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/110m/cultural/ne_110m_admin_0_countries.zip"

    country_df = ""

    with fsspec.open(country_data_path) as file:
        country_df = read_geo_file(file)

    df['lon_eng'] = df['lon']
    df['lat_eng'] = df['lat']
    df['longitude'] = df['lon']
    df['latitude'] = df['lat']

    MAPBOX_TOKEN = os.environ.get("MAPBOX_TOKEN")
    # This is heavy process that takes a while to finish
    # should be used sparingly and closer to end processes.
    df = addReverseGeocodedToDataFrame(df, MAPBOX_TOKEN)
    # creating the structure of the variables pivoted

    df['lat_idx'] = 'lat_mon' + df['idx'].astype(str)
    df['lon_idx'] = 'lon_mon' + df['idx'].astype(str)

    lat = df.pivot(index='id', columns='lat_idx', values='lat')
    lon = df.pivot(index='id', columns='lon_idx', values='lon')

    # Merge the pivoted DataFrames
    reshape = concat([lat, lon], axis=1).reset_index()

    # Add the other columns
    additional_columns = ['timeunix',
                          'country_name_eng', 'max_seq', 'country_code']
    reshape = merge(reshape, df.groupby(
        'id')[additional_columns].first().reset_index(), on='id')

    print(reshape)
    # Create additional variables that were calculate in first round
    reshape['pais_fin'] = 999999
    reshape['dias'] = 999999
    # renaming variables according to first round names
    newColumns = {
        'lat_mon0': 'lat_mon',
        'lon_mon0': 'lon_mon',
        'country_place_name': 'country_name',
        'max_seq': 'max_interaction'
    }

    reshape = reshape.rename(columns=newColumns)
    reshape.fillna(value=999999, inplace=True)

    if (len(output_path) > 0):
        exportDataFrameToFile(df=reshape, fileType=output_format,
                              exportName=output_path)
        return

    # database for Carto
    if (bool(destination)):
        output_df = dataFrameToGeoDataFrame(
            df=reshape, geometry_column_name="geom", lat_column="latitude", long_column="longitude")

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
    parser.add_argument('--feedback_path', type=str, required=True,
                        help="File location path for Aurora Feedback data")
    parser.add_argument('--monitoreo_path', type=str, required=True,
                        help="File location path for Aurora Monitoreo data")
    parser.add_argument('--destination', type=str,
                        help='Carto data warehouse endpoint')
    parser.add_argument('--output', type=str,
                        help='Output name or output path')

    parser.add_argument('--format', type=str, choices=[
                        'csv', 'json'], default='csv', help='Output format if output path is given')

    args = parser.parse_args()

    cara_path = args.cara_path
    feedback_path = args.feedback_path
    monitoreo_path = args.monitoreo_path
    destination = args.destination
    output_path = args.output
    output_format = args.format

    if (bool(cara_path) & bool(feedback_path) & bool(monitoreo_path)):
        main(cara_path=cara_path, feedback_path=feedback_path, monitoreo_path=monitoreo_path, destination=destination,
             output_path=output_path, output_format=output_format)
    else:
        print("Please make sure you have added all data paths")
