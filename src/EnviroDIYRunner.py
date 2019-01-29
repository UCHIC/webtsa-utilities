import pandas
import sqlalchemy
import time
import numpy as np

from Common import Credentials, APP_SETTINGS
from InfluxHelper import *
from SqlSnippets import *


def process_dataseries(connection_string, sql_string):
    source_connection = sqlalchemy.create_engine(connection_string)
    values = pandas.read_sql(sqlalchemy.text(sql_string), source_connection, coerce_float=True)
    values['DateTime'] = pandas.to_datetime(values['DateTime'])
    values.set_index(['DateTime'], inplace=True)
    values['DataValue'] = pandas.to_numeric(values['DataValue'], errors='coerce').astype(np.float64)
    values['UTCOffset'] = pandas.to_numeric(values['UTCOffset'], errors='coerce').astype(np.float64)
    values.dropna(how='any', inplace=True)
    return values


if __name__ == '__main__':
    print 'Starting WebTSA runner for EnviroDIY'
    bad_identifiers = []

    for credential in APP_SETTINGS.credentials.values():  # type: Credentials
        if 'envirodiy' not in credential.name.lower():
            print 'Skipping {}'.format(credential.name)
            continue
        sql_snippets = SqlSnippets.GetSqlSnippets(credential)
        influx_client = InfluxClient(**credential.influx_credentials)
        source_catalog_str = build_connection_string(**credential.tsa_catalog_source)
        destination_catalog_str = build_connection_string(**credential.tsa_catalog_destination)
        catalog_connection = sqlalchemy.create_engine(destination_catalog_str)
        series_dataframe = pandas.read_sql(sqlalchemy.text(sql_snippets.get_sites_from_catalog), catalog_connection)
        for i in range(0, len(series_dataframe)):
            identifier = series_dataframe.get_value(i, 'InfluxIdentifier')
            last_entry = influx_client.GetTimeSeriesEndTime(identifier)
            query_args = sql_snippets.extract_identifying_args(series_dataframe, i)
            datavalue_query = sql_snippets.get_datavalues_query(query_args, last_entry)
            datavalues = process_dataseries(source_catalog_str, datavalue_query)
            result = influx_client.AddDataFrameToDatabase(datavalues, identifier)
            if result is None or (result == 0 and len(datavalues) > 0):
                bad_identifiers.append(query_args)
            if datavalues is not None:
                del datavalues

    for identifier in bad_identifiers:
        print 'Bad: {}'.format(identifier)
