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
from Tools.QueryDriver import iUtahDriver, WebSDLDriver


class InfluxUpdater:
    def __init__(self):
        print "Test statement"
        self.query_driver = iUtahDriver(iUtahDriver.iUtahWOF.REDBUTTE)
        self.influx_client = InfluxClient(**APP_SETTINGS.influx_credentials)
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
            for variable in site_details.variables:
                print variable
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
                details = self.query_driver.GetTimeSeriesValues(site, var.code, var.method, var.source, var.qc)
                if details is None:
                    continue
                time_series = WaterMLParser.ExtractTimeSeries(details)
                if time_series.datavalues is not None:
                    self.influx_client.AddSeriesToDatabase(time_series)

    def Start(self):
        print 'Updater starting'
        site_codes = self.GetSiteCodes()
        self.UpdateSites(site_codes)
        # print self.influx_client.GetTimeSeries('RB_KF_C', 'RH_HC2S3', 0, 1, 2)


if __name__ == '__main__':
    print 'Starting Influx Update tool'
    updater = InfluxUpdater()
    updater.Start()
