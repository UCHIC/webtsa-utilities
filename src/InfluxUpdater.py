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

from Common import APP_SETTINGS
from Tools.WaterMLParser import *
from Tools.InfluxHelper import *
from Tools.QueryDriver import iUtahDriver, WebSDLDriver, QueryDriver


class InfluxUpdater:
    def __init__(self, wof_driver, influx_client):
        self.query_driver = wof_driver        # type: WebSDLDriver
        self.influx_client = influx_client    # type: InfluxClient
        if APP_SETTINGS.VERBOSE:
            print self.influx_client.GetDatabases()

    def GetSiteCodes(self):
        all_sites_xml = self.query_driver.GetAllSites()
        site_list = WaterMLParser.ExtractSiteCodes(all_sites_xml)
        return site_list

    def GetSiteInfo(self, site_code):
        redbutte_site_xml = self.query_driver.GetSiteInfo(site_code)
        if redbutte_site_xml is not None:
            site_details = WaterMLParser.ExtractSiteDetails(redbutte_site_xml)
            if APP_SETTINGS.VERBOSE:
                print site_details
            return site_details
        else:
            print 'Site query failed'
        return None

    def GetTimeSeries(self, network, site, variable):
        details = self.query_driver.GetTimeSeriesValues(network, site, variable.code, variable.method, variable.source,
                                                        variable.qc)
        return WaterMLParser.ExtractTimeSeries(details)

    def UpdateSites(self, site_codes):
        if site_codes is None:
            print 'ERROR: No site codes supplied, cannot be processed'
            return
        for site in site_codes:
            site_info = self.GetSiteInfo(site)
            if site_info is None:
                print 'Failed to get site info for Network: {}, Site: {}'.format(self.query_driver.wof, site)
                continue
            for var in site_info.variables:                 # type: Variable
                try:
                    last_entry = self.influx_client.GetTimeSeriesEndTime(site, var.code, var.qc, var.source, var.method)
                    if last_entry is None:
                        begin_time = ''
                        end_time = ''
                    else:
                        print 'Database entry found for this series, most recently updated at {}'.format(last_entry)
                        begin_time = (last_entry + datetime.timedelta(seconds=0)).strftime('%Y-%m-%dT%H:%M:%S')
                        end_time = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')

                    details = self.query_driver.GetTimeSeriesValues(site, var.code, var.method, var.source, var.qc,
                                                                    start=begin_time, end=end_time)
                    if details is None:
                        continue
                    time_series = WaterMLParser.ExtractTimeSeries(details)
                    if time_series is None or time_series.datavalues is None:
                        print 'No data for time series'
                        continue
                    self.influx_client.AddSeriesToDatabase(time_series)
                    del details
                    del time_series
                except Exception as e:
                    print 'Exception encountered while attempting to update {}:\n{}'.format(var, e)

    def RunQuery(self, query_string):
        # encoded_string = urllib.quote(query_string, safe='')
        if APP_SETTINGS.VERBOSE:
            print query_string
        query_response = requests.get(query_string)
        if '200' not in str(query_response.status_code):
            print 'Response was not successful: {}'.format(query_response.status_code)
            print 'URL: {}'.format(query_string)
            print 'Message: {}'.format(query_response.text)
            return None
        return query_response.text

    def Start(self):
        values = self.query_driver.GetSitesFromCatalog()  # type: pandas.DataFrame
        failed_identifiers = []
        successful_identifiers = []
        print 'Got dataframe!'
        influx_urls = []
        for i in range(0, len(values)):
            print '-------------'
            identifier = values.get_value(i, 'InfluxIdentifier')
            influx_url = values.get_value(i, 'GetDataInflux')
            influx_urls.append(influx_url)
            last_entry = influx_client.GetTimeSeriesEndTime(identifier)
            if last_entry is None:
                begin_time = ''
                end_time = ''
            else:
                print 'Database entry found for this series, most recently updated at {}'.format(last_entry)
                begin_time = (last_entry + datetime.timedelta(seconds=0)).strftime('%Y-%m-%dT%H:%M:%S')
                end_time = ''

            # time_series = self.query_driver.GetDataValues(values.get_value(i, 'ResultID'), begin_time, end_time)
            time_series = self.query_driver.GetDataValues(values.get_value(i, 'ResultUUID'), begin_time, end_time)
            print 'Influx: {}'.format(influx_url)
            print time_series

            if time_series is None:
                print 'No data for time series'
                continue
            if len(time_series) > 0:
                written = self.influx_client.AddDataFrameToDatabase(time_series, identifier)
                if written == 0:
                    failed_identifiers.append(identifier)
                else:
                    successful_identifiers.append(identifier)
                del time_series
            else:
                print 'No new values for {}'.format(identifier)

        if APP_SETTINGS.VERBOSE:
            for url in influx_urls:
                print url
            for identifier in successful_identifiers:
                print 'Succeeded: {}'.format(identifier)
            for identifier in failed_identifiers:
                print 'Failed: {}'.format(identifier)


if __name__ == '__main__':
    print 'Starting Influx Update tool'
    influx_client = InfluxClient(**APP_SETTINGS.influx_credentials)
    updater = InfluxUpdater(iUtahDriver(), influx_client)
    updater.Start()

    for identifier, message in updater.influx_client.query_errors.iteritems():
        print '{}: {}'.format(identifier, message)

