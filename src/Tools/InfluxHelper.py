import datetime
import urllib

import pandas
import re

from influxdb import DataFrameClient
from influxdb.exceptions import InfluxDBClientError


class InfluxClient(object):
    def __init__(self, host, port, username, password, database):
        self.client = DataFrameClient(host=host, port=port, username=username, password=password, database=database)
        self.database = database
        if database is not None and database not in self.GetDatabases():
            self.CreateDatabase(database)
        self.query_errors = {}

    def GetDatabases(self):
        return [db['name'] for db in self.client.get_list_database()]

    def CreateDatabase(self, database_name):
        self.client.create_database(database_name)

    @staticmethod
    def GetIdentifier(site_code, var_code, qc_id, source_id, method_id):
        """
        InfluxDB Identifiers:
        For the following time series:
            Turbidity; Campbell_OBS-3+_Turb, 1, 67, 2
        Format as 'wof_{site_code}_{var_code}_{qc_id}_{source_id}_{method_id}'
            wof_PUPP2S_Campbell_OBS_3+_Turb_Raw_1_67_2
        Encode as a URI (to remove invalid characters while keeping uniqueness)
            wof_PUPP2S_Campbell_OBS_3%2B_Turb_1_67_2
        Replace all non-word characters with an underscore
            wof_PUPP2S_Campbell_OBS_3_2B_Turb_1_67_2

        Example python code:
        def GetIdentifier(site_code, var_code, qc_id, source_id, method_id):
            pre_identifier = 'wof_{}_{}_{}_{}_{}'.format(site_code, var_code, qc_id, source_id, method_id)
            return re.sub('[\W]', '_', urllib.quote(pre_identifier, safe=''))
        """
        pre_identifier = 'wof_{}_{}_{}_{}_{}'.format(site_code, var_code, qc_id, source_id, method_id)
        return re.sub('[\W]', '_', urllib.quote(pre_identifier, safe=''))

    @staticmethod
    def GetEnviroDiyIdentifier(result_uuid):
        return 'uuid_{}'.format(result_uuid.replace('-', '_'))

    @staticmethod
    def GetIdentifierBySeriesDetails(series):
        if series is None:
            return None
        return InfluxClient.GetIdentifier(series.site_code, series.variable_code, series.qc_id, series.source_id,
                                          series.method_id)

    def RunQuery(self, query_string, identifier):
        try:
            return self.client.query(query_string, database=self.database)
        except InfluxDBClientError as e:
            print 'Query Error for {}: {}'.format(identifier, e.message)
            if identifier not in self.query_errors.keys():
                self.query_errors[identifier] = []
            self.query_errors[identifier].append(e.message)
        return None

    def AddSeriesToDatabase(self, series):
        if series is None:
            return None
        identifier = self.GetIdentifierBySeriesDetails(series)
        print 'Writing data points for ' + identifier
        write_success = self.client.write_points(series.datavalues, identifier, protocol='json', batch_size=20000)
        if not write_success:
            print 'Write failed for series with identifier {}'.format(identifier)
        else:
            print '{} Data points written for time series with identifier {}'.format(len(series.datavalues), identifier)

    def AddDataFrameToDatabase(self, datavalues, identifier):
        print 'Writing data points for ' + identifier
        write_success = self.client.write_points(datavalues, identifier, protocol='json', batch_size=20000)
        if not write_success:
            print 'Write failed for series with identifier {}'.format(identifier)
        else:
            print '{} Data points written for time series with identifier {}'.format(len(datavalues), identifier)

    def GetTimeSeriesBySeriesDetails(self, series, start='', end=''):
        return self.GetTimeSeries(series.site_code, series.variable_code, series.qc_code, series.source_code,
                                  series.method_code, start, end)

    def GetTimeSeries(self, site_code, var_code, qc_code, source_code, method_code, start='', end=''):
        identifier = self.GetIdentifier(site_code, var_code, qc_code, source_code, method_code)
        print 'Getting time series for ' + identifier
        query_string = 'Select {select} from {series}'.format(select='*', series=identifier)
        if len(start) > 0:
            query_string += ' where time > \'{}\''.format(start)
        if len(end) > 0 and len(start) > 0:
            query_string += ' and time < \'{}\''.format(end)
        elif len(end) > 0:
            query_string += ' where time < \'{}\''.format(end)
            return self.RunQuery(query_string, identifier)
        return None

    def GetTimeSeriesStartTime(self, site_code, var_code, qc_code, source_code, method_code):
        identifier = self.GetIdentifier(site_code, var_code, qc_code, source_code, method_code)
        print 'Getting start time for ' + identifier
        query_string = 'Select first(DataValue), time from {identifier}'.format(identifier=identifier)
        result = self.RunQuery(query_string, identifier)
        if result is not None and len(result) == 1:
            dataframe = result[identifier]  # type: pandas.DataFrame
            return dataframe.first_valid_index().to_pydatetime()
        return None

    def GetTimeSeriesEndTime(self, identifier):
        print 'Getting end time for ' + identifier
        query_string = 'Select last(DataValue), time from {identifier}'.format(identifier=identifier)
        result = self.RunQuery(query_string, identifier)
        if result is not None and len(result) == 1:
            dataframe = result[identifier]  # type: pandas.DataFrame
            return dataframe.first_valid_index().to_pydatetime()
        return None

    # def GetTimeSeriesEndTime(self, site_code, var_code, qc_code, source_code, method_code):
    #     identifier = self.GetIdentifier(site_code, var_code, qc_code, source_code, method_code)
    #     print 'Getting end time for ' + identifier
    #     query_string = 'Select last(DataValue), time from {identifier}'.format(identifier=identifier)
    #     result = self.RunQuery(query_string, identifier)
    #     if result is not None and len(result) == 1:
    #         dataframe = result[identifier]  # type: pandas.DataFrame
    #         return dataframe.first_valid_index().to_pydatetime()
    #     return None
