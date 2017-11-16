import datetime
import pandas
import re
import requests
import sys
import urllib
from influxdb import DataFrameClient
from influxdb.exceptions import InfluxDBClientError

from bs4 import BeautifulSoup, NavigableString, Tag
from influxdb import DataFrameClient, InfluxDBClient
from SqlSnippets import *

from Common import APP_SETTINGS, Credentials
from Tools.WaterMLParser import *
from Tools.InfluxHelper import *
from Tools.QueryDriver import iUtahDriver, WebSDLDriver, QueryDriver
from tsa_catalog_update import *


# def UpdateAllCatalogs():
#     iutah_catalog_purged = False
#
#     for credential in APP_SETTINGS.credentials.values():  # type: Credentials
#         sql_snippets = SqlSnippets.GetSqlSnippets(credential.name)
#         source_catalog_str = build_connection_string(**credential.tsa_catalog_source)
#         catalog_table = fetch_catalog(source_catalog_str, sql_snippets)
#         print 'Database {} returned {} results for catalog'.format(credential.name, len(catalog_table))
#
#         destination_catalog_str = build_connection_string(**credential.tsa_catalog_destination)
#
#         if 'iutah' in credential.name.lower():
#             if not iutah_catalog_purged:
#                 iutah_catalog_purged = True
#                 print 'Purging iUtah DB ' + credential.name
#                 purge_catalog(destination_catalog_str, sql_snippets)
#             else:
#                 print 'Already purged - won\'t purge other'
#         else:
#             print 'Skipping {}'.format(credential.name)
#             continue
#
#         print 'Inserting into '
#         insert_into_catalog(destination_catalog_str, catalog_table)

# def UpdateInflux():
#     print 'Updating influx DB'
#     for credential in APP_SETTINGS.credentials.values():  # type: Credentials
#         sql_snippets = SqlSnippets.GetSqlSnippets(credential.name)
#         source_catalog_str = build_connection_string(**credential.tsa_catalog_source)
#         catalog_table = fetch_catalog(source_catalog_str, sql_snippets)
#         print 'Database {} returned {} results for catalog'.format(credential.name, len(catalog_table))
#
#         destination_catalog_str = build_connection_string(**credential.tsa_catalog_destination)
#
#         if 'iutah' in credential.name.lower():
#             if not iutah_catalog_purged:
#                 iutah_catalog_purged = True
#                 print 'Purging iUtah DB ' + credential.name
#                 purge_catalog(destination_catalog_str, sql_snippets)
#             else:
#                 print 'Already purged - won\'t purge other'
#         else:
#             print 'Skipping {}'.format(credential.name)
#             continue
#
#         print 'Inserting into '
#         insert_into_catalog(destination_catalog_str, catalog_table)


def GetDataseries(sql_snippets, connection_string):
    # type: () -> pd.DataFrame
    source_connection = sqlalchemy.create_engine(connection_string)
    values = pd.read_sql(sqlalchemy.text(sql_snippets.get_catalog_sites), source_connection)
    return values

def update_catalog(credential, purge_existing):
    source_db_str = build_connection_string(**credential.tsa_catalog_source)
    catalog_table = fetch_catalog(source_db_str, sql_snippets)

    catalog_db_str = build_connection_string(**credential.tsa_catalog_destination)
    if purge_existing:
        print 'Purging catalog for ' + credential.name
        purge_catalog(catalog_db_str, sql_snippets)
    else:
        print 'Skipping catalog purge for {}'.format(credential.name)

    print 'Inserting {} records from {} into catalog'.format(len(catalog_table), credential.name)
    insert_into_catalog(catalog_db_str, catalog_table)
    return catalog_table

def process_dataseries(connection_string, sql_string):
    source_connection = sqlalchemy.create_engine(connection_string)
    values = pd.read_sql(sqlalchemy.text(sql_string), source_connection, coerce_float=True)
    values['DateTime'] = pandas.to_datetime(values['DateTime'])
    values.set_index(['DateTime'], inplace=True)
    values['DataValue'] = pandas.to_numeric(values['DataValue'], errors='coerce')
    values['UTCOffset'] = pandas.to_numeric(values['UTCOffset'], errors='coerce')
    values.dropna(how='any', inplace=True)
    return values

if __name__ == '__main__':
    print 'Starting WebTSA runner'
    iutah_catalog_purged = False

    for credential in APP_SETTINGS.credentials.values():  # type: Credentials
        sql_snippets = SqlSnippets.GetSqlSnippets(credential.name)
        if APP_SETTINGS.update_catalogs:
            print 'Updating the TSA catalog'
            do_purge = False if (iutah_catalog_purged and 'iutah' in credential.name.lower()) else True
            if 'iutah' in credential.name.lower() and do_purge:
                iutah_catalog_purged = True
                print 'Marking iutah as purged'
            catalog_table = update_catalog(credential, do_purge)
            del catalog_table

        if APP_SETTINGS.update_influx:
            print 'Updating influx DB'
            influx_client = InfluxClient(**credential.influx_credentials)
            catalog_db_str = build_connection_string(**credential.tsa_catalog_source)
            series_dataframe = GetDataseries(sql_snippets, catalog_db_str)
            for i in range(0, len(series_dataframe)):
                identifier = series_dataframe.get_value(i, 'InfluxIdentifier')
                last_entry = influx_client.GetTimeSeriesEndTime(identifier)
                query_args = sql_snippets.extract_identifying_args(series_dataframe, i)
                datavalue_query = sql_snippets.get_datavalues_query(query_args, last_entry)
                print datavalue_query
                datavalues = process_dataseries(catalog_db_str, datavalue_query)
                influx_client.AddDataFrameToDatabase(datavalues, identifier)
