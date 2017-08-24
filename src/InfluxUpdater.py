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
        self.query_driver = iUtahDriver()

    def GetSiteCodes(self, network):
        all_sites_xml = self.query_driver.GetRedButteSites()
        site_list = WaterMLParser.ExtractSiteCodes(all_sites_xml)
        return site_list

    def GetSiteInfo(self, network, site_code):
        redbutte_site_xml = self.query_driver.GetSiteInfo(network, site_code)
        if redbutte_site_xml is not None:
            site_details = WaterMLParser.ExtractSiteDetails(redbutte_site_xml)
            print site_details
            for series in site_details.series:
                print series
            return site_details
        else:
            print 'Site query failed'


    def Start(self):
        print 'Updater starting'
        print self.GetSiteCodes('RedButteCreek')

        site_info = self.GetSiteInfo('RedButteCreek', 'RB_1300E_A')
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
