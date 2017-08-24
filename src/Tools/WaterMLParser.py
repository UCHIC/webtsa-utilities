
from bs4 import BeautifulSoup, NavigableString, Tag


class WaterMLParser:
    def __init__(self):
        pass

    @staticmethod
    def ExtractSiteDetails(xml_string):
        soup = BeautifulSoup(xml_string, 'lxml-xml')
        site_soup = soup.site
        if site_soup is None:
            print 'No site info found'

        site_info = site_soup.siteInfo
        series_catalog = site_soup.seriesCatalog

        if site_info is None or series_catalog is None:
            print 'No site or series info found'

        print site_info.siteName.contents[0]
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
            site.series.append(variable)
        return site

    @staticmethod
    def ExtractSiteCodes(xml_string):
        soup = BeautifulSoup(xml_string, 'lxml-xml')
        site_soup = soup.sitesResponse
        if site_soup is None:
            print 'No sites contained in XML response'
            return []

        site_codes = [dv.siteInfo.siteCode.contents[0] for dv in site_soup.children if
                      type(dv) == Tag and dv.name == 'site']
        return site_codes

    def GetSeriesDetails(self):
        print 'Getting the details'
        series = self.soup.find('timeSeries')
        if series is None:
            print 'Could not find timeSeries'
            return
        self.site_code = series.sourceInfo.siteCode.contents[0]
        self.var_code = series.variable.variableCode.contents[0]
        self.qc_code = series.qualityControlLevel.qualityControlLevelCode.contents[0]
        self.method_code = series.method.methodCode.contents[0]
        self.source_code = series.source.sourceCode.contents[0]
        self.censor_code = series.censorCode.censorCode.contents[0]

        self.values = [(dv.contents[0], re.sub('[T]', ' ', dv.attrs['dateTime']), dv.attrs['timeOffset']) for dv
                       in series.values.children if type(dv) == Tag and 'dateTime' in dv.attrs]

        print len(self.values)

        for i in range(0, 15):
            print self.values[i]

    def GetIdentifier(self):
        return Get_Identifier(self.site_code, self.var_code, self.qc_code, self.source_code, self.method_code)

    def AddSeriesToInflux(self):
        identifier = self.GetIdentifier()
        print identifier
        dataframe = pandas.DataFrame(self.values, columns=['DataValue', 'DateTime', 'UTCOffset'])
        dataframe['DateTime'] = pandas.to_datetime(dataframe['DateTime'])
        dataframe.set_index(['DateTime'], inplace=True)
        print dataframe
        print 'Starting to write datapoints to Influx'
        write_success = influx.write_points(dataframe, identifier, protocol='json')
        if not write_success:
            print 'Write failed for some reason with identifier {}'.format(identifier)
            raw_input('Press \'Enter\' to continue')
        else:
            print 'Data points written'

    def GetSeriesFromInflux(self):
        identifier = self.GetIdentifier()
        query_string = 'Select {select} from {series};'.format(select='*', series=identifier)
        result = influx.query(query_string)
        print 'Result is: {}'.format(result)


class Variable:
    def __init__(self):
        self.code = ''
        self.name = ''
        self.id = ''
        self.method = ''
        self.source = ''
        self.qc = ''

    def __str__(self):
        return '{}: {}; {}, {}, {}, {}'.format(self.id, self.name, self.code, self.method, self.source, self.qc)

class Site:
    def __init__(self, name='', code=''):
        self.name = name
        self.code = code
        self.series = []

class SiteDetails(object):
    def __init__(self, name, code, site_id):
        self.name = name            # type: str
        self.code = code            # type: str
        self.site_id = site_id      # type: int
        self.series = []            # type: List[SeriesDetails]

    @classmethod
    def CreateFromSiteInfoNode(site, node):
        return site(node.siteName.contents[0], node.siteCode.contents[0], node.siteCode.attrs['siteID'])


class SeriesDetails(object):
    def __init__(self, site_id, variable_id, method_id, source_id, qc_id):
        self.site_id = site_id          # type: int
        self.variable_id = variable_id  # type: int
        self.method_id = method_id      # type: int
        self.source_id = source_id      # type: int
        self.qc_id = qc_id              # type: int

    @classmethod
    def CreateFromVariableNode(series, site_id, node):
        return series(site_id,
                             node.variable.variableCode.attrs['variableID'],
                             node.method.attrs['methodID'],
                             node.source.attrs['sourceID'],
                             node.qualityControlLevel.attrs['qualityControlLevelID'])
