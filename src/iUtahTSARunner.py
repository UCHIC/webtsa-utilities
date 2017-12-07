import pandas
import sqlalchemy

from Common import Credentials, APP_SETTINGS
from InfluxHelper import *
from SqlSnippets import *
from USGS_CatalogUpdater import purge_catalog_and_insert_usgs_sites, update_usgs_influx_timeseries



def GetDataseries(sql_snippets, connection_string):
    # type: () -> pd.DataFrame
    source_connection = sqlalchemy.create_engine(connection_string)
    values = pandas.read_sql(sqlalchemy.text(sql_snippets.get_catalog_sites), source_connection)
    return values


def process_dataseries(connection_string, sql_string):
    source_connection = sqlalchemy.create_engine(connection_string)
    values = pandas.read_sql(sqlalchemy.text(sql_string), source_connection, coerce_float=True)
    values['DateTime'] = pandas.to_datetime(values['DateTime'])
    values.set_index(['DateTime'], inplace=True)
    values['DataValue'] = pandas.to_numeric(values['DataValue'], errors='coerce')
    values['UTCOffset'] = pandas.to_numeric(values['UTCOffset'], errors='coerce')
    values.dropna(how='any', inplace=True)
    return values


def sanitize_iutah_identifiers(catalog_table):
    """
    :type catalog_table: pandas.DataFrame
    """
    for i in range(0, len(catalog_table)):
        site_code = catalog_table.get_value(i, 'SiteCode')
        variable_code = catalog_table.get_value(i, 'VariableCode')
        qc_id = catalog_table.get_value(i, 'QualityControlLevelID')
        source_id = catalog_table.get_value(i, 'SourceID')
        method_id = catalog_table.get_value(i, 'MethodID')
        sanitized_id = InfluxClient.GetIdentifier(site_code, variable_code, qc_id, source_id, method_id)
        sanitized_url = InfluxClient.GetiUtahUrlQueryString(sanitized_id)
        catalog_table.set_value(i, 'InfluxIdentifier', sanitized_id)
        catalog_table.set_value(i, 'GetDataInflux', sanitized_url)
    return catalog_table


if __name__ == '__main__':
    print 'Starting WebTSA runner'
    iutah_catalog_purged = False
    bad_identifiers = []

    if 'USGS_Datavalues' in APP_SETTINGS.credentials.keys():
        print 'Updating USGS data values'
        usgs_credentials = APP_SETTINGS.credentials['USGS_Datavalues']
        purge_catalog_and_insert_usgs_sites(usgs_credentials)
        iutah_catalog_purged = True
    else:
        print 'Unable to update USGS data values'

    for credential in APP_SETTINGS.credentials.values():  # type: Credentials
        if 'USGS_Datavalues' == credential.name:
            continue
        elif 'iutah' not in credential.name.lower():
            print 'Skipping {}'.format(credential.name)
            continue
        sql_snippets = SqlSnippets.GetSqlSnippets(credential)
        do_purge = False if (iutah_catalog_purged and 'iutah' in credential.name.lower()) else True
        source_catalog_str = build_connection_string(**credential.tsa_catalog_source)
        catalog_table = pandas.read_sql(sqlalchemy.text(sql_snippets.compile_dataseries), source_catalog_str)
        destination_catalog_str = build_connection_string(**credential.tsa_catalog_destination)
        if do_purge:
            print 'Purging catalog for ' + credential.name
            purge_catalog(destination_catalog_str, sql_snippets)

        iutah_catalog_purged = True
        catalog_table = sanitize_iutah_identifiers(catalog_table)
        print 'Inserting {} records from {} into catalog'.format(len(catalog_table), credential.name)
        insert_into_catalog(destination_catalog_str, catalog_table)
        del catalog_table

    for credential in APP_SETTINGS.credentials.values():  # type: Credentials
        if 'USGS_Datavalues' == credential.name:
            update_usgs_influx_timeseries(credential)
            continue
        elif 'iutah' not in credential.name.lower():
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
        if series_dataframe is not None:
            del series_dataframe

    for identifier in bad_identifiers:
        print 'Bad: {}'.format(identifier)
