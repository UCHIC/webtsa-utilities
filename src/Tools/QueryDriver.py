import abc
import requests


class QueryDriver(object):
    def __init__(self, name, base_url, loc_prefix, get_sites_query, variable_info_query, datavalues_query,
                 site_info_query):
        self.name = name
        self.base_url = base_url
        self.location_prefix = loc_prefix + ':'
        self.get_sites_query = get_sites_query
        self.variable_info_query = variable_info_query
        self.data_values_query = datavalues_query
        self.site_info_query = site_info_query

    def RunQuery(self, query_string):
        query_response = requests.get(query_string)
        if '200' not in str(query_response.status_code):
            print 'Response was not successful: {}'.format(query_response.status_code)
            print 'URL: {}'.format(query_string)
            print 'Message: {}'.format(query_response.text)
            return None
        return query_response.text

    def GetSitesForNetwork(self, network):
        query = self.base_url.format(network=network) + self.get_sites_query
        return self.RunQuery(query)

    def GetTimeSeriesValues(self, network, query_args):
        query = self.base_url.format(network=network) + self.data_values_query.format(**query_args)
        return self.RunQuery(query)

    @abc.abstractmethod
    def GetDataValues(self, network, args):
        raise NotImplementedError

    @abc.abstractmethod
    def GetSiteInfo(self, network, site_code):
        query = self.base_url.format(network=network) + self.site_info_query.format(prefix=self.location_prefix,
                                                                                    site=site_code)
        return self.RunQuery(query)

    @abc.abstractmethod
    def GetVariableInfo(self, network, query_args):
        query = self.base_url.format(network=network) + self.variable_info_query.format(**query_args)
        return self.RunQuery(query)

    def __str__(self):
        return 'Query Driver: '.format(self.name)


class iUtahDriver(QueryDriver):
    def __init__(self):
        datavalues_query = 'datavalues?location={prefix}{site}&variable={prefix}{var}/methodCode={method}/' \
                           'sourceCode={source}/qualityControlLevelCode={qc}&startDate={start}&endDate={end}'
        variable_query = 'variables?variable={prefix}{var}'
        site_info_query = 'siteinfo?location={prefix}{site}'
        self.networks = ['LoganRiver', 'RedButteCreek', 'ProvoRiver']
        super(iUtahDriver, self).__init__(name='iUtahDriver',
                                          base_url='http://data.iutahepscor.org/{network}WOF/REST/waterml_1_1.svc/',
                                          loc_prefix='iutah',
                                          get_sites_query='sites',
                                          variable_info_query=variable_query,
                                          datavalues_query=datavalues_query,
                                          site_info_query=site_info_query)

    def GetAllSites(self):
        result = {}
        for network in self.networks:
            response = self.GetSitesForNetwork(network)
            result[network] = response if response is not None else ''
        return result

    def GetLoganSites(self):
        return self.GetSitesForNetwork(self.networks[0])

    def GetProvoSites(self):
        return self.GetSitesForNetwork(self.networks[2])

    def GetRedButteSites(self):
        return self.GetSitesForNetwork(self.networks[1])

    def GetSiteInfo(self, network, site_code):
        return QueryDriver.GetSiteInfo(self, network, site_code)

    def GetVariableInfo(self, network, variable_code):
        return QueryDriver.GetVariableInfo(self, network, dict(prefix=self.location_prefix, var=variable_code))

    def GetDataValues(self, network, site_code, variable_code, method_code, source_code, qc_code, start='', end=''):
        return self.GetTimeSeriesValues(network, dict(site=site_code, var=variable_code, method=method_code,
                                                      source=source_code, qc=qc_code, start=start, end=end,
                                                      prefix=self.location_prefix))


class WebSDLDriver(QueryDriver):
    def __init__(self):
        datavalue_query = 'GetValues?location={network}:{site}&variable={network}:{var}&startDate={start}&endDate={end}'
        variable_query = 'GetVariableInfo?variable={network}:{var}'
        site_info_query = 'GetSiteInfo?site={prefix}{site}'
        self.network = 'wofpy'
        super(WebSDLDriver, self).__init__(name='WebSDLDriver',
                                           base_url='http://envirodiysandbox.usu.edu/{network}/rest/1_1/',
                                           loc_prefix='wofpy',
                                           get_sites_query='GetSites',
                                           variable_info_query=variable_query,
                                           datavalues_query=datavalue_query,
                                          site_info_query=site_info_query)

    def GetAllSites(self):
        return self.GetSitesForNetwork(self.network)

    def GetSiteInfo(self, site_code):
        return QueryDriver.GetSiteInfo(self, self.network, site_code)

    def GetVariableInfo(self, variable_code):
        return QueryDriver.GetVariableInfo(self, self.network, dict(var=variable_code, network=self.network))

    def GetDataValues(self, site_code, variable_code, start='', end=''):
        return self.GetTimeSeriesValues(self.network, dict(site=site_code, var=variable_code, start=start, end=end,
                                                           network=self.network))

