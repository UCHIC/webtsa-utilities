import pandas
import re
from bs4 import BeautifulSoup, NavigableString, Tag


class WaterMLParser:
    def __init__(self):
        pass

    @staticmethod
    def ExtractSiteDetails(xml_string):
        if xml_string is None or len(xml_string) == 0:
            print 'The XML string provided is either empty or None'
            return None
        soup = BeautifulSoup(xml_string, 'lxml-xml')
        site_soup = soup.site
        if site_soup is None:
            print 'No site info found'

        site_info = site_soup.siteInfo
        series_catalog = site_soup.seriesCatalog

        if site_info is None or series_catalog is None:
            print 'No site or series info found'

        site = Site(site_info.siteName.contents[0], site_info.siteName.contents[0])
        for node in series_catalog.children:
            if type(node) != Tag or node.name != 'series':
                continue
            variable = Variable()
            variable.id = node.variable.variableCode.attrs['variableID']
            variable.name = node.variable.variableName.contents[0]
            variable.code = node.variable.variableCode.contents[0]
            variable.method = node.method.attrs['methodID']
            variable.source = node.source.attrs['sourceID']
            variable.qc = node.qualityControlLevel.attrs['qualityControlLevelID']
            site.variables.append(variable)
        return site

    @staticmethod
    def ExtractSiteCodes(xml_string):
        if xml_string is None or len(xml_string) == 0:
            print 'The XML string provided is either empty or None'
            return None
        soup = BeautifulSoup(xml_string, 'lxml-xml')
        site_soup = soup.sitesResponse
        if site_soup is None:
            print 'No sites contained in XML response'
            return []
        site_codes = [dv.siteInfo.siteCode.contents[0] for dv in site_soup.children
                      if type(dv) == Tag and dv.name == 'site']
        return site_codes

    @staticmethod
    def ExtractTimeSeries(xml_string):
        if xml_string is None or len(xml_string) == 0:
            print 'The XML string provided is either empty or None'
            return None
        soup = BeautifulSoup(xml_string, 'lxml-xml')
        series_node = soup.find('timeSeries')
        if series_node is None:
            print 'Could not find timeSeries'
            return None
        try:
            series = TimeSeries(series_node.sourceInfo.siteCode.contents[0],
                                series_node.variable.variableCode.contents[0],
                                series_node.method.methodCode.contents[0],
                                series_node.source.sourceCode.contents[0],
                                series_node.qualityControlLevel.qualityControlLevelCode.contents[0],
                                series_node.sourceInfo.siteCode.attrs['siteID'],
                                series_node.variable.variableCode.attrs['variableID'],
                                series_node.method.attrs['methodID'],
                                series_node.source.attrs['sourceID'],
                                series_node.qualityControlLevel.attrs['qualityControlLevelID'])

            values = [(dv.contents[0], re.sub('[T]', ' ', dv.attrs['dateTime']), dv.attrs['timeOffset']) for dv
                      in series_node.values.children if type(dv) == Tag and 'dateTime' in dv.attrs]
            series.datavalues = pandas.DataFrame(values, columns=['DataValue', 'DateTime', 'UTCOffset'])
            series.datavalues['DateTime'] = pandas.to_datetime(series.datavalues['DateTime'])
            series.datavalues.set_index(['DateTime'], inplace=True)
            return series
        except Exception as e:
            print 'No time series values were found'
        return None


class Variable:
    def __init__(self):
        self.code = ''
        self.name = ''
        self.id = ''
        self.method = ''
        self.source = ''
        self.qc = ''

    def __str__(self):
        return '{}: {}; {}, {}, {}, {}'.format(self.id, self.name, self.code, self.qc, self.method, self.source)


class Site:
    def __init__(self, network, name='', code=''):
        self.network = network
        self.name = name
        self.code = code
        self.variables = []


class SiteDetails(object):
    def __init__(self, name, code, site_id):
        self.name = name  # type: str
        self.code = code  # type: str
        self.site_id = site_id  # type: int
        self.series = []  # type: List[TimeSeries]

    @classmethod
    def CreateFromSiteInfoNode(site, node):
        return site(node.siteName.contents[0], node.siteCode.contents[0], node.siteCode.attrs['siteID'])


class TimeSeries(object):
    def __init__(self, site_code, variable_code, method_code, source_code, qc_code,
                 site_id, variable_id, method_id, source_id, qc_id):
        self.site_code = site_code  # type: str
        self.variable_code = variable_code  # type: str
        self.method_code = method_code  # type: str
        self.source_code = source_code  # type: str
        self.qc_code = qc_code  # type: str
        self.site_id = site_id  # type: str
        self.variable_id = variable_id  # type: str
        self.method_id = method_id  # type: str
        self.source_id = source_id  # type: str
        self.qc_id = qc_id  # type: str
        self.datavalues = None  # type: pandas.DataFrame

    @classmethod
    def CreateFromVariableNode(cls, site_code, node):
        series_details = cls(site_code, node.variable.variableCode.attrs['variableID'], node.method.attrs['methodID'],
                             node.source.attrs['sourceID'], node.qualityControlLevel.attrs['qualityControlLevelID'])
        return series_details
