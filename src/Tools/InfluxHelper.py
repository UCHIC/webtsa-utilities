import pandas
import re

from influxdb import DataFrameClient, InfluxDBClient


class InfluxHelper(object):
    def __init__(self, host, port, username, password):
        self.client = DataFrameClient(host=host, port=port, username=username, password=password)

    def GetDatabases(self):
        return [db['name'] for db in self.client.get_list_database()]

    def CreateDatabase(self, database_name):
        self.client.create_database(database_name)

    @staticmethod
    def GetIdentifier(site_code, var_code, qc_code, source_code, method_code):
        pre_identifier = '{}_{}_{}_{}_{}'.format(site_code, var_code, qc_code, source_code, method_code)
        return re.sub('[\W]', '_', pre_identifier)

    def AddStuff(self, dataframe, database):
        identifier = self.GetIdentifier()
        write_success = self.client.write_points(dataframe, identifier, protocol='json')
        if not write_success:
            print 'Write failed for series with identifier identifier {}'.format(identifier)
        else:
            print 'Data points written'

        dataframe = pandas.DataFrame(self.values, columns=['DataValue', 'DateTime', 'UTCOffset'])
        dataframe['DateTime'] = pandas.to_datetime(dataframe['DateTime'])
        dataframe.set_index(['DateTime'], inplace=True)
        print dataframe
        print 'Starting to write datapoints to Influx'
        if not write_success:
            print 'Write failed for some reason with identifier {}'.format(identifier)
            raw_input('Press \'Enter\' to continue')
        else:
            print 'Data points written'
