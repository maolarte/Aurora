import pandas as pd
import geopandas as gpd
import os
import re
import json
from datetime import datetime
import math
import numpy as np
from shapely.geometry import Point
import itertools


MEDIA_SOURCES = [
    'facebook',
    'twitter',
    'news',
    'reddit',
    'youtube',
    'forums',
    'tiktok',
    'social_blogs',
    'all',
]

UNWANTED_SOURCES = [
    'social_comments',
    'social_message_boards',
    'social_reviews',
]

analysis_criteria = {
    "term": [
        "migrante",
        "migrante irregulare",
        "refugiado",
        "migración",
        "migración forzada",
        "Personas desplazada",
        "Ruta migratoria",
        "Flujo migratorio",
    ],
    "place": [
        "selva del darién",
        "darién",
        "frontera Colombia Panamá",
        "Panamá",
        "Necoclí",
        "Tapón",
        "cruce del darién"
    ],
    "subgroup": [
        "niña",
        "niño",
        "adolescente",
        "mujeres embarazada",
        "mujere embarazada",
        "venezolano",
        "haitiano",
        "ecuatariano",
        "chino",
        "colombiano",
        "peruano",
        "cubano"
    ],
    "context": [
        "tráfico",
        "abuso",
        "sexual"
    ],
    "temporality": [
        "2023",
        "marzo"
    ]
}

working_dir = os.getcwd()

# I/O Functions


def loadLocalJsonDoc(filepath, dataProp=''):
    output = {}
    with open(file=filepath, mode='r', encoding='utf-8') as f:
        json_load = json.load(f)
        if (dataProp):
            output = json_load[dataProp]
        else:
            output = json_load
    return output


def saveJsonTofile(file, filename):
    with open(os.path.join(working_dir, f'{filename}.json'), 'w') as f:
        stringfy_json = json.dump(file, f)

# Dataframe/Analysis Functions


def loadJsonToDataframe(loadedJson):
    return pd.DataFrame(loadedJson)


def shrinkTable(data, columns):
    return data[columns]


def getUniqueValueByColumn(data, column):
    return data[column].unique().tolist()


def getValueCount(data):
    return len(data)


def dictToArray(df, valueCount='count'):
    data = df.items()
    output = []
    for (key, value) in list(data):
        count = value[valueCount]
        output.append([key, count])
    return output


def getSentimentAnalysis(data, source=''):
    try:
        target_column = 'document_sentiment'
        secondary_column = 'count'
        columns = [target_column, secondary_column]
        df = shrinkTable(data, columns)
        df_grouped = df.groupby([target_column]).sum()
        return dictToArray(df_grouped.to_dict('index'))
    except:
        return {}


def getPercentile(df, percent, valueColumn='count'):
    df_size = len(df)
    quarter_percentile = round(df_size * percent)
    sorted_df = df.sort_values(by=[valueColumn])
    percentile_df = sorted_df.tail(quarter_percentile)
    return percentile_df


def getTopPhrases(data):
    try:
        target_column = 'document_key_phrases'
        phrases = []
        for groups in data[target_column].tolist():
            if (len(groups) > 0):
                phrases.extend(groups)

        data2 = pd.DataFrame({
            'phrases': phrases,
            'count': np.repeat(1, len(phrases))
        })
        target_column2 = 'phrases'
        secondary_column = 'count'
        columns = [target_column2, secondary_column]
        df = shrinkTable(data2, columns)
        df_grouped = df.groupby([target_column2]).sum()
        df_percentile = getPercentile(df_grouped, 0.1)
        return dictToArray(df_percentile.to_dict('index'))
    except:
        return {}


def toUnixTimestamp(time):
    start = datetime(1970, 1, 1)
    target = datetime.strptime(time, "%Y-%m-%d")
    in_seconds = (start - target).total_seconds()
    in_milliseconds = int(in_seconds) * -1000
    return in_milliseconds


def timestampToString():
    return lambda x: toUnixTimestamp(datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%Y-%m-%d'))


def applyFormatToDataframeColumn(df, column):
    return df[column].apply(timestampToString())


def getUniqueTimeSeries(data):
    dates = applyFormatToDataframeColumn(
        data, 'document_publish_date').unique().tolist()
    return dates


def getCountriesByCount(data):
    try:
        target_column = 'source_country_code'
        secondary_column = 'count'
        columns = [target_column, secondary_column]
        df = shrinkTable(data, columns)
        df_grouped = df.groupby([target_column]).sum()
        return dictToArray(df_grouped.to_dict('index'))
    except:
        return {}


def getViews(data):
    values = [x for x in getUniqueValueByColumn(
        data, 'document_views') if math.isnan(x) == False]
    total = sum(values)
    if (total):
        return total
    return 0


def getLanguagesByCount(data):
    try:
        target_column = 'document_language_code'
        secondary_column = 'count'
        columns = [target_column, secondary_column]
        df = shrinkTable(data, columns)
        df_grouped = df.groupby([target_column]).sum()
        return dictToArray(df_grouped.to_dict('index'))
    except:
        return {}


def getTopPosts(data, source):
    column = ''
    sec_column = 'document_views'
    if (source == MEDIA_SOURCES[1]):
        column = 'document_external_id'
    else:
        column = 'document_url'
    _data = data[data[sec_column] > 0]
    df = shrinkTable(_data, [column, sec_column])
    df_grouped = df.groupby([column]).sum()
    df_percentile = getPercentile(df_grouped, 1, sec_column)
    return dictToArray(df_percentile.to_dict('index'), sec_column)


def getAnalyticsCriteria(data, criteria):
    columns = ['document_hit_sentence',
               "document_opening_text", "document_key_phrases"]
    output = []
    for column in columns:
        _data = data[column].tolist()
        if (column == "document_key_phrases"):
            _data = itertools.chain.from_iterable(_data)
        value = ",".join(_data)
        for index, criterium in enumerate(criteria):
            key = str(index+1)
            pattern = "^(.*){i}(.*)$".format(i=criterium).lower()
            string = value.lower()
            match = re.search(pattern, string)
            if (match):
                if (key not in output):
                    output.append(key)
    if (len(output) > 0):
        return ",".join(output)
    else:
        return "0"


def getDataByValue(df, column, value):
    return df[df[column] == value]


def getSourceStatistics(df, source=''):
    output = {}
    output['volume'] = getValueCount(df)
    output['sentiment'] = getSentimentAnalysis(df)
    output['topPhrases'] = getTopPhrases(df)
    output['languages'] = getLanguagesByCount(df)
    output['views'] = getViews(df)
    output['topPosts'] = getTopPosts(df, source)
    output["terms"] = getAnalyticsCriteria(df, analysis_criteria['term'])
    output["places"] = getAnalyticsCriteria(df, analysis_criteria['place'])
    output["subgroups"] = getAnalyticsCriteria(
        df, analysis_criteria['subgroup'])
    output["context"] = getAnalyticsCriteria(df, analysis_criteria['context'])
    output["temporality"] = getAnalyticsCriteria(
        df, analysis_criteria['temporality'])
    return output


def getTabularSummarisation(df, required):
    time_column = 'document_publish_date'
    source_column = 'source_name'
    country_column = 'source_country_code'
    data = df
    dates = getUniqueValueByColumn(data, time_column)
    output = []
    for date in dates:
        dataByDate = getDataByValue(data, time_column, date)
        sources = getUniqueValueByColumn(dataByDate, source_column)
        for source in sources:
            if (source in required):
                dataBySource = getDataByValue(
                    dataByDate, source_column, source)
                countries = getUniqueValueByColumn(
                    dataBySource, country_column)
                for country in countries:
                    dataByCountry = getDataByValue(
                        dataBySource, country_column, country)
                    subOutput = dict()
                    subOutput['date'] = date
                    subOutput['source'] = source
                    subOutput['country'] = country
                    subOutput.update(
                        getSourceStatistics(dataByCountry, source))
                    output.append(subOutput)
    return output


def getGeneralSummary(df, sources):
    output = {}
    output['volume'] = getValueCount(df)
    output['countries'] = getUniqueValueByColumn(df, 'source_country_code')
    output['sources'] = sources
    return output


def executeMediaSummarization(df, sources):
    output = {}
    output['summary'] = getGeneralSummary(df, sources)
    output['sources'] = getTabularSummarisation(df, sources)
    return output


# 1. Loaded Data From Local File
raw1 = loadJsonToDataframe(loadLocalJsonDoc(
    "./meltwater_raw/meltwater_01012023_08092023.json", 'data'))
raw2 = loadJsonToDataframe(loadLocalJsonDoc(
    "./meltwater_raw/meltwater_01012023_08092023.json", 'data'))
raw3 = loadJsonToDataframe(loadLocalJsonDoc(
    "./meltwater_raw/meltwater_08092023_18092023.json", 'data'))
raw4 = loadJsonToDataframe(loadLocalJsonDoc(
    "./meltwater_raw/meltwater_18092023_25092023.json", 'data'))
raw5 = loadJsonToDataframe(loadLocalJsonDoc(
    "./meltwater_raw/meltwater_25102023_02102023.json", 'data'))
raw6 = loadJsonToDataframe(loadLocalJsonDoc(
    "./meltwater_raw/meltwater_02102023_09102023.json", 'data'))
raw7 = loadJsonToDataframe(loadLocalJsonDoc(
    "./meltwater_raw/meltwater_16102023_23102023.json", 'data'))
raw8 = loadJsonToDataframe(loadLocalJsonDoc(
    "./meltwater_raw/meltwater_23102023_29102023.json", 'data'))

df = pd.concat([raw1, raw2, raw3, raw4, raw5, raw6, raw7, raw8],
               ignore_index=True, sort=False)
df["count"] = 1
df['source_name'] = df['source_name'].apply(
    lambda x: x if x in MEDIA_SOURCES and x not in UNWANTED_SOURCES else 'news')

output = executeMediaSummarization(df, sources=MEDIA_SOURCES)

output_df = pd.DataFrame(output['sources'])
output_df["country"] = output_df["country"].str.upper()

saveJsonTofile(output, 'summarised_meltwater_data_v14')

output_df = pd.DataFrame(loadJsonToDataframe(
    loadLocalJsonDoc("summarised_meltwater_data_v12.json", "sources")))

# Save in GeoJSON

# import countries data
countries_df = gpd.read_file(
    "ne_110m_land/ne_110m_admin_0_countries_lakes.shp")

countries_df["geometry"] = countries_df.centroid

WANTED_COUNTRIES_COLUMNS = ['ADMIN', 'ISO_A2',
                            'ISO_A2_EH', 'CONTINENT', 'REGION_UN', 'geometry']

UNWANTED_COUNTRIES_COLUMNS = [x for x in list(
    countries_df.columns) if x not in WANTED_COUNTRIES_COLUMNS]

clean_countries_df = countries_df.drop(
    labels=UNWANTED_COUNTRIES_COLUMNS, axis=1)

clean_countries_df["ISO_A2"] = clean_countries_df["ISO_A2"]

clean_countries_df["ISO_A2_EH"] = clean_countries_df["ISO_A2_EH"]

output_with_coords = gpd.GeoDataFrame(pd.merge(
    left=output_df, right=clean_countries_df, how="left", left_on="country", right_on="ISO_A2"))

centre_point = Point(30.000062, 31.000062)

output_with_coords["geometry"] = output_with_coords["geometry"].fillna(
    centre_point)

json_df = output_with_coords.to_json()

with open("output/processed_meltwater_data_v3_with_location.geojson", "w") as f:
    f.write(json_df)
