import sys
import pandas as pd
from pandas import merge, read_csv, read_json
from modules.custom_geo_functions import addReverseGeocodedToDataFrame, dataFrameToGeoDataFrame, processFieldCoordinates, getCountriesWithCoordinates
from modules.custom_io import useCartoAuth, uploadDataFrameToCarto, getCartoClient, exportDataFrameToFile, loadLocalJsonDoc
from modules.custom_functions import toUnixTimestamp, processCountries
from google.cloud import bigquery
import os
from argparse import ArgumentParser


def main(cara_path: str, feedback_path: str, monitoreo_path: str, info_path: str, destinations: str = "", output_paths: str = "", output_format: str = "csv"):

    working_dir = os.getcwd()

    # Import dataset
    aurora_cara = read_csv(
        filepath_or_buffer=cara_path)
    aurora_feedback = read_csv(
        filepath_or_buffer=feedback_path)
    aurora_monitoreos = read_csv(
        filepath_or_buffer=monitoreo_path)
    aurora_info = read_csv(
        filepath_or_buffer=info_path)

    # Merge tables of first connection
    aurora1 = merge(aurora_cara, aurora_feedback)
    aurora = pd.merge(aurora1, aurora_info, how='left')

    # Adding the monitorings tables
    aurora_comple = pd.concat([aurora, aurora_monitoreos], ignore_index=True)

    # Drop observations of Aurora team phones, test registers and geographical atypical rows

    user_ids_to_remove = loadLocalJsonDoc(
        os.path.join(working_dir, "defaults", "test_user_ids.json"))

    for user_id in user_ids_to_remove:
        aurora_comple = aurora_comple.drop(
            aurora_comple[aurora_comple.UserId == user_id].index)

   # Variable of date in date format (for generating panel data)
    aurora_comple["fecha"] = pd.to_datetime(
        aurora_comple["Inicio interacción"],  errors='coerce', utc=True, infer_datetime_format=True).dt.strftime('%Y-%m-%d')
    # fixing the format of nov 5th 2023 (monitorings) is diferrent fron the rest of days
    aurora_comple.loc[aurora_comple['fecha'] ==
                      '2023-05-11', 'fecha'] = "2023-11-05"

    # Filling all observations of the same ID with the variable consent and actual country (variables of first connection)
    aurora_comple['Consentimiento'] = aurora_comple.groupby(
        'UserId')['Consentimiento'].transform('first')
    aurora_comple['País destino'] = aurora_comple.groupby(
        'UserId')['País destino'].transform('first')
    aurora_comple['Zona país'] = aurora_comple.groupby(
        'UserId')['Zona país'].transform('first')
    aurora_comple['Edad'] = aurora_comple.groupby(
        'UserId')['Edad'].transform('first')
    aurora_comple['Género'] = aurora_comple.groupby(
        'UserId')['Género'].transform('first')
    aurora_comple['Hay niños, niñas o adolescentes'] = aurora_comple.groupby(
        'UserId')['Hay niños, niñas o adolescentes'].transform('first')
    # When you join 'Enganche' field is missing
    # aurora_comple['Enganche'] = aurora_comple.groupby(
    #     'UserId')['Enganche'].transform('first')

    # filering by consent, cleaning geographical variables that are equal to "None" and drop the ones that can't identify
    # the zone of first connection
    aurora_comple = aurora_comple[aurora_comple['Consentimiento'] != 'NO']

    # Filtered out entries with out coordinates
    latitude_test = aurora_comple['Latitud'].fillna(999999).astype(str).str.replace(
        "-", "").str.replace(".", "").str.isnumeric()
    longitude_test = aurora_comple['Longitud'].fillna(999999).astype(str).str.replace(
        "-", "").str.replace(".", "").str.isnumeric()
    aurora_comple = aurora_comple[latitude_test & longitude_test]
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

# Adding coordinates of variables (país de nacimiento, país donde inicio el viaje and país donde vivía hace un año)

    available_countries = [x.lower() for x in list(set(list(aurora_comple["e08_pais_"].unique(
    )) + list(aurora_comple["e10_pais_"].unique()) + list(aurora_comple["e12_pais_"].unique()))) if type(x) == str]

    countries_dict = loadLocalJsonDoc(os.path.join(
        working_dir, "defaults", "countries_dict.json"))

    available_countries = processCountries(available_countries, countries_dict)

    country_df = read_json(
        path_or_buf="defaults/countries.json", orient='records')

    countriesWithCoordinates = getCountriesWithCoordinates(
        available_countries, country_df)
    country_column_dict = loadLocalJsonDoc(os.path.join(
        working_dir, "defaults", "country_column_dict.json"))
    aurora_comple = processFieldCoordinates(
        aurora_comple, country_column_dict, countriesWithCoordinates, countries_dict)

    aurora_comple['lon_eng'] = aurora_comple['Longitud']
    aurora_comple['lat_eng'] = aurora_comple['Latitud']
    aurora_comple['longitude'] = aurora_comple['Longitud']
    aurora_comple['latitude'] = aurora_comple['Latitud']
    # Create the variable time
    # the format was missing other elements thus the timestring was not parsing
    aurora_comple["timeunix"] = aurora_comple["Inicio interacción"].apply(
        lambda x: toUnixTimestamp(x, '%Y-%m-%d %H:%M:%S.%f+00:00'))
    # Adding coordinates of variables (país de nacimiento, país donde inicio el viaje and país donde vivía hace un año)
    MAPBOX_TOKEN = os.environ.get("MAPBOX_TOKEN")
    # This is heavy process that takes a while to finish
    # should be used sparingly and closer to end processes.
    aurora_comple = addReverseGeocodedToDataFrame(
        df=aurora_comple, token=MAPBOX_TOKEN, lat_column="latitude", lon_column="longitude", name="Auora")

    # After checking some coordinates, especially in country borders where info was collected, and using some extra info as the name collector, there are some country names that were changed
    # This was did for Chile and Colombia
    # Colombia
    user_ids_col = loadLocalJsonDoc(
        os.path.join(working_dir, "defaults", "user_ids_col.json"))
    condition_eng = '¿Cómo interactúa con el sistema?'
    condition_value = 'Enganche'
    new_country_value = 'Colombia'
    condition_col = (aurora_comple['UserId'].isin(user_ids_col)) & (
        aurora_comple[condition_eng] == condition_value)
    # Use the loc method to update the 'country_name' column
    aurora_comple.loc[condition_col, 'country_name'] = new_country_value

   # Chile
    user_ids_ch = loadLocalJsonDoc(
        os.path.join(working_dir, "defaults", "user_ids_ch.json"))
    new_country_value1 = 'Chile'
    condition1 = (aurora_comple['UserId'].isin(user_ids_ch)) & (
        aurora_comple[condition_eng] == condition_value)
    # Use the loc method to update the 'country_name' column
    aurora_comple.loc[condition1, 'country_name'] = new_country_value1

    # Fix the coordinates of the rows changed before (lines 78-98) that did not has coordinates (1 observation Chile, 2 Colombia)
    aurora_comple.loc[aurora_comple['UserId'] == 313172106, 'lat'] = -18.475525
    aurora_comple.loc[aurora_comple['UserId']
                      == 313172106, 'lon'] = -70.3137029

    aurora_comple.loc[aurora_comple['UserId'] == 320582739, 'lat'] = 8.42152826
    aurora_comple.loc[aurora_comple['UserId']
                      == 320582739, 'lon'] = -76.78180133

    aurora_comple.loc[aurora_comple['UserId'] == 325857664, 'lat'] = 8.42152826
    aurora_comple.loc[aurora_comple['UserId']
                      == 325857664, 'lon'] = -76.78180133

    aurora_comple.loc[aurora_comple['UserId'] ==
                      317308056, 'country_name'] = "Colombia"
    aurora_comple.loc[aurora_comple['UserId']
                      == 319708059, 'country_name'] = "Chile"

    excel_file = 'completa.xlsx'  # integrate
    aurora_comple.to_excel(excel_file, index=False)

# Dataset of general feedback

    feedback = aurora_comple[['UserId', 'Latitud', 'Longitud', 'Cual ayuda humanitaria', 'Qué tan fácil fue acceder a la ayuda', 'Interaction_Sequence', 'País actual', 'Edad', 'Género',
                              'Qué tan satisfecho te sientes respecto a la ayuda', 'Recomendarías la ayuda ', '¿Recibiste ayuda humanitaria en el lugar actual?',
                              'País destino', 'Lugar interacción', 'Hay niños, niñas o adolescentes', 'fecha', 'country_name', 'country_code', 'Tipo de monitoreo', '¿Cómo interactúa con el sistema?', 'timeunix']].reset_index()

    feedback['tipo'] = feedback['Tipo de monitoreo']
    feedback['tipo'].fillna('Enganche', inplace=True)
    feedback = feedback[feedback['tipo'].isin(['Enganche', 'Feedback'])]

    # Rename variables to be easy to work with them

    newColumns = {
        'UserId': 'id',
        'Latitud': 'latitud',
        'Longitud': 'longitud',
        'Cual ayuda humanitaria': 'Ayuda',
        'Qué tan fácil fue acceder a la ayuda': 'Acceso',
        'Qué tan satisfecho te sientes respecto a la ayuda': 'Satisfaccion',
        'Recomendarías la ayuda ': 'Recomendacion',
        '¿Recibiste ayuda humanitaria en el lugar actual?': 'Ayuda_lugaractual',
        'Hay niños, niñas o adolescentes': 'NNA',
        'fecha': 'Inicio interacción',
        'Interaction_Sequence': 'push'
    }

    feedback = feedback.rename(columns=newColumns)

    feedback['Ayuda'] = feedback['Ayuda'].str.split('|')
    feedback['Acceso'] = feedback['Acceso'].str.split('|')
    feedback['Satisfaccion'] = feedback['Satisfaccion'].str.split('|')
    feedback['Recomendacion'] = feedback['Recomendacion'].str.split('|')

    df = pd.DataFrame(feedback)

    df = df[df.apply(lambda row: all(row[['Ayuda', 'Acceso', 'Satisfaccion', 'Recomendacion']].str.len(
    ) == row[['Ayuda', 'Acceso', 'Satisfaccion', 'Recomendacion']].str.len().iloc[0]), axis=1)]

    df1 = df.explode(['Ayuda', 'Acceso', 'Satisfaccion', 'Recomendacion'])

    df1['m12'] = df1['Ayuda']
    df1['m14'] = df1['Acceso']
    df1['m15'] = df1['Satisfaccion']

    # Change the variable of recomendation to be numeric, as the others
    def change_variable_value(value):
        if value == "SI":
            return 1
        elif value == "NO":
            return 2
        else:
            return value

    df1['m16'] = df1['Recomendacion'].apply(change_variable_value)

    # use dictionary for aid

    nec_dict = {
        1: "Agua",
        2: "Alimentación o kit de alimentación",
        3: "Alojamiento temporal",
        4: "Asistencia legal",
        5: "Ayuda psicológica",
        6: "Dinero en efectivo",
        7: "Duchas o baños",
        8: "Educación o espacios educativos",
        9: "Espacios seguros para adultos",
        10: "Kit de aseo o elementos de higiene",
        11: "Salud, primeros auxilios o atención médica",
        12: "Transporte humanitario",
        13: "Otra"
    }

    def necesidadMap(value):
        try:

            if (type(value) == int):
                return nec_dict[value]
            if (type(value) == str):
                return nec_dict[int(value)]
        except Exception as e:
            return value

    df1['Ayuda'] = df1['Ayuda'].apply(lambda x: necesidadMap(x))

    # use dictionary for access, satisfaction and recomendation

    acc_dict = {
        1: "Fácil",
        2: "Regular",
        3: "Difícil",
    }

    sat_dict = {
        1: "Mucho",
        2: "Poco",
        3: "Nada",
    }

    def accMap(value):
        try:

            if (type(value) == int):
                return acc_dict[value]
            if (type(value) == str):
                return acc_dict[int(value)]
        except Exception as e:
            return value

    def satMap(value):
        try:

            if (type(value) == int):
                return sat_dict[value]
            if (type(value) == str):
                return sat_dict[int(value)]
        except Exception as e:
            return value

    df1['Acceso'] = df1['Acceso'].apply(lambda x: accMap(x))
    df1['Satisfaccion'] = df1['Satisfaccion'].apply(lambda x: satMap(x))

    feedback_output_path = output_paths.split(",")[0]

    if (feedback_output_path):
        exportDataFrameToFile(df=df1, fileType=output_format,
                              exportName=feedback_output_path)

    # df1.to_csv('feedback.csv', index=False)  # integrate

    # Children's services feedback

    feedback_NNA = aurora_comple[['UserId', 'Latitud', 'Longitud', 'Cual ayuda humanitaria NNA', 'NNA: Qué tan fácil fue acceder a la ayuda', 'Interaction_Sequence', 'País actual', 'Edad', 'Género',
                                  'NNA: Qué tan satisfecho te sientes respecto a la ayuda', 'NNA: Recomendarías la ayuda ', '¿Recibiste ayuda humanitaria en el lugar actual?',
                                  'País destino', 'Lugar interacción', 'Hay niños, niñas o adolescentes', 'country_name', 'country_code', 'Tipo de monitoreo', '¿Cómo interactúa con el sistema?']].reset_index()

    feedback_NNA['tipo'] = feedback_NNA['Tipo de monitoreo']
    feedback_NNA['tipo'].fillna('Enganche', inplace=True)
    feedback_NNA = feedback_NNA[feedback_NNA['tipo'].isin(
        ['Enganche', 'Feedback'])]

    # Rename variables

    newColumns = {
        'UserId': 'id',
        'Latitud': 'latitud',
        'Longitud': 'longitud',
        'Cual ayuda humanitaria NNA': 'Ayuda_NNA',
        'NNA: Qué tan fácil fue acceder a la ayuda': 'Acceso_NNA',
        'NNA: Qué tan satisfecho te sientes respecto a la ayuda': 'Satisfaccion_NNA',
        'NNA: Recomendarías la ayuda ': 'Recomendacion_NNA',
        '¿Recibiste ayuda humanitaria en el lugar actual?': 'Ayuda_lugaractual',
        'Hay niños, niñas o adolescentes': 'NNA',
        'Interaction_Sequence': 'push'

    }

    feedback_NNA = feedback_NNA.rename(columns=newColumns)

    feedback_NNA['Ayuda_NNA'] = feedback_NNA['Ayuda_NNA'].str.split('|')
    feedback_NNA['Acceso_NNA'] = feedback_NNA['Acceso_NNA'].str.split('|')
    feedback_NNA['Satisfaccion_NNA'] = feedback_NNA['Satisfaccion_NNA'].str.split(
        '|')
    feedback_NNA['Recomendacion_NNA'] = feedback_NNA['Recomendacion_NNA'].str.split(
        '|')

    df = pd.DataFrame(feedback_NNA)

    df = df[df.apply(lambda row: all(row[['Ayuda_NNA', 'Acceso_NNA', 'Satisfaccion_NNA', 'Recomendacion_NNA']].str.len(
    ) == row[['Ayuda_NNA', 'Acceso_NNA', 'Satisfaccion_NNA', 'Recomendacion_NNA']].str.len().iloc[0]), axis=1)]

    df2 = df.explode(['Ayuda_NNA', 'Acceso_NNA',
                     'Satisfaccion_NNA', 'Recomendacion_NNA'])

    df2['m18_1'] = df2['Ayuda_NNA']
    df2['m19'] = df2['Acceso_NNA']
    df2['m20'] = df2['Satisfaccion_NNA']

    # Change the variable of recomendation to be numeric, as the others

    df2['m21'] = df2['Recomendacion_NNA'].apply(change_variable_value)

    # use dictionary for children aid
    aid_NNA_dict = {
        1: "Agua",
        2: "Alimentación o kit de alimentación",
        3: "Alojamiento temporal",
        4: "Ayuda psicológica",
        5: "Duchas o baños",
        6: "Educación o Espacios educativos y de cuidado para niños y niñas",
        7: "Kit de aseo o elementos de higiene",
        8: "Salud, primeros auxilios o atención médica",
        9: "Otra"
    }

    def necesidadMap(value):
        try:

            if (type(value) == int):
                return aid_NNA_dict[value]
            if (type(value) == str):
                return aid_NNA_dict[int(value)]
        except Exception as e:
            return value

    df2['Ayuda_NNA'] = df2['Ayuda_NNA'].apply(lambda x: necesidadMap(x))

    df2['Acceso_NNA'] = df2['Acceso_NNA'].apply(lambda x: accMap(x))
    df2['Satisfaccion_NNA'] = df2['Satisfaccion_NNA'].apply(
        lambda x: satMap(x))

    feedback_nna_output_path = output_paths.split(",")[-1]
    # df2.to_csv(output_paths[-1], index=False)  # integrate

    if (feedback_nna_output_path):
        exportDataFrameToFile(df=df2, fileType=output_format,
                              exportName=feedback_nna_output_path)
        return

    # database for Carto
    # if (bool(destination)):
    #     output_df = dataFrameToGeoDataFrame(
    #         df=reshape, geometry_column_name="geom", lat_column="latitude", long_column="longitude")

    #     carto_auth = useCartoAuth()

    #     carto_client = getCartoClient(carto_auth)

    #     config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE",)

    #     uploadDataFrameToCarto(cartDW=carto_client, df=output_df,
    #                            destination=destination, config=config)
    #     return

    # print("No file was exported")


parser = ArgumentParser()
if __name__ == "__main__":
    parser.add_argument('--cara_path', type=str, required=True,
                        help="File location path for Aurora Characterization data")
    parser.add_argument('--ayuda_path', type=str, required=True,
                        help="File location path for Aurora Feedback data")
    parser.add_argument('--mon_path', type=str, required=True,
                        help="File location path for Aurora Monitoreo data")
    parser.add_argument('--info_path', type=str, required=True,
                        help="File location path for Aurora Info data")
    parser.add_argument('--destination', type=str,
                        help='A listCarto data warehouse endpoint')
    parser.add_argument('--output', type=str,
                        help='A list of output names or output paths seperated by comma')

    parser.add_argument('--format', type=str, choices=[
                        'csv', 'json'], default='csv', help='Output format if output path is given')

    args = parser.parse_args()

    cara_path = args.cara_path
    ayuda_path = args.ayuda_path
    mon_path = args.mon_path
    info_path = args.info_path
    destinations = args.destination
    output_paths = args.output
    output_format = args.format

    if (not bool(cara_path)):
        print("Please add both Characterization data path")
        sys.exit()

    if (not bool(ayuda_path)):
        print("Please add both Feedback data path")
        sys.exit()

    if (not bool(mon_path)):
        print("Please add both Monitoring data path")
        sys.exit()

    if (not bool(info_path)):
        print("Please add both Information data path")
        sys.exit()

    if ((not bool(output_paths)) & (not bool(destinations))):
        print("Print add at least one output method")
        sys.exit()

    main(cara_path=cara_path, feedback_path=ayuda_path, monitoreo_path=mon_path, info_path=info_path, destinations=destinations,
         output_paths=output_paths, output_format=output_format)
