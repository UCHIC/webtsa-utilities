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
            # if site != 'RB_1300E_A':
            #     continue
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

        # print self.influx_client.GetTimeSeries('RB_KF_C', 'AirTemp_HC2S3_avg', 0, 1, 1, end='2017-01-01')

        # self.TestiUtahDriver()
        # self.TestWebSDLDriver()

    def TestiUtahDriver(self):
        driver = iUtahDriver()
        # print driver.GetLoganSites()
        # print driver.GetDataValues('RedButteCreek', 'RB_1300E_A', 'ODO_Local', 61, 1, 0)
        # print driver.GetSiteInfo('RB_1300E_A')
        print driver.GetVariableInfo('RedButteCreek', 'ODO_Local')

    def TestWebSDLDriver(self):
        driver = WebSDLDriver()
        # print driver.GetAllSites()
        # print driver.GetDataValues('KINNI_LOGGER6', 'EnviroDIY_Mayfly_Temp')
        # print driver.GetSiteInfo('KINNI_LOGGER6')
        # print driver.GetVariableInfo('EnviroDIY_Mayfly_Temp')


if __name__ == '__main__':
    print 'Starting Influx Update tool'
    updater = InfluxUpdater()
    updater.Start()
