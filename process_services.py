import sys
from argparse import ArgumentParser
from pandas import read_csv
import numpy as np
import os
from google.cloud import bigquery

from modules.custom_functions import replaceOrganisation, loadLocalJsonDoc, toUnixTimestamp, processMultValueColumns, processValueReplacement, capitaliseColumns
from modules.custom_geo_functions import addReverseGeocodedToDataFrame, dataFrameToGeoDataFrame

from modules.custom_io import uploadDataFrameToCarto, getCartoClient, useCartoAuth, exportDataFrameToFile


defaultMissingValue = 999999


def main(raw_data: str, destination: str = "", output_path: str = "", output_format: str = "csv"):

    working_dir = os.getcwd()

    services = read_csv(raw_data, sep=';', index_col=False)

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
    condition1 = [(services['numeronna'] == 0) & (common1)]
    fill_with = ['no']

    services['nnanoacompanados'] = np.select(
        condition, fill_with, default=services['nnanoacompanados'])
    services['nnaseparados'] = np.select(
        condition1, fill_with, default=services['nnaseparados'])

    # keep the observations that were agreed with the informed consent
    services = services[services['consentimiento'] != 'no']

    # Renaming variables

    # organizations name
    organisation = loadLocalJsonDoc(os.path.join(
        working_dir, "defaults/organisation_names.json"))

    # Implementor organisation
    implemen_dict = organisation['implementors']
    services['organizacionimplementadora'] = services['organizacionimplementadora'].replace(
        implemen_dict)

    # Principals organisation
    principal_dict = organisation['principals']
    services['organizacionprincipal'] = services['organizacionprincipal'].replace(
        principal_dict)

    # Places
    places = loadLocalJsonDoc(os.path.join(
        working_dir, "defaults/places.json"))
    # locals
    places_dict = places['locals']
    services['punto_reporte'] = services['punto_reporte'].replace(places_dict)

    # countries
    country_dict = places['countries']
    services['pais'] = services['pais'].replace(country_dict)

    # Columns
    newColumns = loadLocalJsonDoc(os.path.join(
        working_dir, "defaults/rename_columns.json"))
    services_carto = services.rename(columns=newColumns)

    # Dropping two text variables related to final observations and scarce services
    # (they are not needed in Carto, they are important on other analysis)
    services_carto = services_carto.drop("observacion", axis='columns')
    services_carto = services_carto.drop("serviciosescasos", axis='columns')

    # serv_tipo (separating by pipe symbol and categorized with number)
    codify_dict: dict = loadLocalJsonDoc(os.path.join(
        working_dir, "defaults/codification_dict.json"))
    codify_list = codify_dict.items()

    replace_lib = loadLocalJsonDoc(os.path.join(
        working_dir, "defaults/value_replacements.json"))
    services_carto = processValueReplacement(services_carto, replace_lib)

    _capitalise_columns = loadLocalJsonDoc(os.path.join(
        working_dir, "defaults/column_capitalisation.json"))

    services_carto = capitaliseColumns(services_carto, _capitalise_columns)

    # parsing date field into unix timestamp
    services_carto["timeunix"] = services_carto["fecha"].apply(
        lambda x: toUnixTimestamp(time=x, format="%Y-%m-%d"))

    output_df = processMultValueColumns(services_carto, codify_list)

    token = os.environ.get("MAPBOX_TOKEN")

    # add geo-administrative attributes
    output_df = addReverseGeocodedToDataFrame(
        output_df, lon_column="longitude", lat_column="latitude", token=token, name="Service data")

    # Fill  missing values
    output_df = output_df.fillna(defaultMissingValue)

    if (len(output_path) > 0):
        exportDataFrameToFile(df=output_df, fileType=output_format,
                              exportName=output_path)
        return

    if (bool(destination)):
        output_df = dataFrameToGeoDataFrame(
            df=output_df, geometry_column_name="geom", lat_column="latitude", long_column="longitude")

        carto_auth = useCartoAuth()

        carto_client = getCartoClient(carto_auth)

        config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE",)

        uploadDataFrameToCarto(cartDW=carto_client, df=output_df,
                               destination=destination, config=config)

    print("No file was exported")


parser = ArgumentParser()

if __name__ == "__main__":
    parser.add_argument('file_path', type=str,
                        help='File path, the location of which the service data file is located. Also accepts https file endpoints.')
    parser.add_argument('--destination', type=str,
                        help='Carto data warehouse endpoint')

    parser.add_argument('--output', type=str,
                        help='Output path location')

    parser.add_argument('--format', type=str, choices=[
                        'csv', 'json'], default='csv', help='Output format if output path is given')

    args = parser.parse_args()
    raw_data = args.file_path
    destination = args.destination
    output_path = args.output
    output_format = args.format

    if (not bool(raw_data)):
        print("Please add both raw data path")
        sys.exit()

    if ((not bool(output_path)) & (not bool(destination))):
        print("Print add at least one output method")
        sys.exit()

    main(raw_data=raw_data, destination=destination,
         output_format=output_format, output_path=output_path)
