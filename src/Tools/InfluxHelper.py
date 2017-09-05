import datetime
import pandas
import re

from influxdb import DataFrameClient, InfluxDBClient


class InfluxClient(object):
    def __init__(self, host, port, username, password, database):
        self.client = DataFrameClient(host=host, port=port, username=username, password=password, database=database)
        self.database = database
        if database is not None and database not in self.GetDatabases():
            self.CreateDatabase(database)

    def GetDatabases(self):
        return [db['name'] for db in self.client.get_list_database()]

    def CreateDatabase(self, database_name):
        self.client.create_database(database_name)

    @staticmethod
    def GetIdentifier(site_code, var_code, qc_code, source_code, method_code):
        pre_identifier = '{}_{}_{}_{}_{}'.format(site_code, var_code, qc_code, source_code, method_code)
        return re.sub('[\W]', '_', pre_identifier)

    @staticmethod
    def GetIdentifierBySeriesDetails(series):
        if series is None:
            return None
        return InfluxClient.GetIdentifier(series.site_code, series.variable_code, series.qc_code, series.source_code,
                                          series.method_code)

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
        return self.client.query(query_string, database=self.database)

    def GetTimeSeriesStartTime(self, site_code, var_code, qc_code, source_code, method_code):
        identifier = self.GetIdentifier(site_code, var_code, qc_code, source_code, method_code)
        print 'Getting start time for ' + identifier
        query_string = 'Select first(DataValue), time from {identifier}'.format(identifier=identifier)
        result = self.client.query(query_string, database=self.database)
        if result is not None and len(result) == 1:
            dataframe = result[identifier]  # type: pandas.DataFrame
            return dataframe.first_valid_index().to_pydatetime()
        return None

    def GetTimeSeriesEndTime(self, site_code, var_code, qc_code, source_code, method_code):
        identifier = self.GetIdentifier(site_code, var_code, qc_code, source_code, method_code)
        print 'Getting end time for ' + identifier
        query_string = 'Select last(DataValue), time from {identifier}'.format(identifier=identifier)
        result = self.client.query(query_string, database=self.database)
        if result is not None and len(result) == 1:
            dataframe = result[identifier]  # type: pandas.DataFrame
            return dataframe.first_valid_index().to_pydatetime()
        return None
