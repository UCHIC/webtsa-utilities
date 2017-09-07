import datetime
import pandas
import re
import requests
import sys

from bs4 import BeautifulSoup, NavigableString, Tag
from influxdb import DataFrameClient, InfluxDBClient

from Common import APP_SETTINGS
from Tools.WaterMLParser import *
from Tools.InfluxHelper import *
from Tools.QueryDriver import iUtahDriver, WebSDLDriver, QueryDriver


class InfluxUpdater:
    def __init__(self, wof_driver, influx_client):
        print "Test statement"
        self.query_driver = wof_driver
        self.influx_client = influx_client
        print self.influx_client.GetDatabases()

    def GetSiteCodes(self):
        all_sites_xml = self.query_driver.GetAllSites()
        site_list = WaterMLParser.ExtractSiteCodes(all_sites_xml)
        return site_list

    def GetSiteInfo(self, site_code):
        redbutte_site_xml = self.query_driver.GetSiteInfo(site_code)
        if redbutte_site_xml is not None:
            site_details = WaterMLParser.ExtractSiteDetails(redbutte_site_xml)
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
                        begin_time = (last_entry + datetime.timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S')
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

    def Start(self):
        print 'Updater starting'
        site_codes = self.GetSiteCodes()
        self.UpdateSites(site_codes)
        # print self.influx_client.GetTimeSeries('RB_KF_C', 'RH_HC2S3', 0, 1, 2)
        # result = self.influx_client.GetTimeSeriesStartTime('RB_KF_C', 'RH_HC2S3', 0, 1, 2)
        # print result
        # result = self.influx_client.GetTimeSeriesEndTime('RB_KF_C', 'RH_HC2S3', 0, 1, 2)
        # print result

if __name__ == '__main__':
    print 'Starting Influx Update tool'
    influx_client = InfluxClient(**APP_SETTINGS.influx_credentials)
    updater = InfluxUpdater(WebSDLDriver(), influx_client)
    updater.Start()

    for identifier, message in updater.influx_client.query_errors.iteritems():
        print '{}: {}'.format(identifier, message)

    # for driver in iUtahDriver.iUtahWOF.as_list():
    #     try:
    #         updater = InfluxUpdater(iUtahDriver(driver), influx_client)
    #         updater.Start()
    #         for identifier, message in updater.influx_client.query_errors.iteritems():
    #             print '{}: {}'.format(identifier, message)
    #     except Exception as e:
    #         print 'Exception encountered using driver {}: {}'.format(driver, e)

