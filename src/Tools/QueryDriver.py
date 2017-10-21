import pandas
import requests
import urllib
import pandas as pd
import psycopg2
from Common import APP_SETTINGS
import sqlalchemy

class QueryDriver(object):
    def __init__(self, name, url, wof, network, query_site_info, query_variable, query_datavalues, query_all_sites):
        self.name = name
        self.wof = wof
        self._original_url = url
        self.base_url = url.format(wof=wof)
        self.network = network
        self.query_site_info = query_site_info
        self.query_variable = query_variable
        self.query_datavalues = query_datavalues
        self.query_all_sites = query_all_sites

    def RunQuery(self, query_string):
        # encoded_string = urllib.quote(query_string, safe='')
        print query_string
        query_response = requests.get(query_string)
        if '200' not in str(query_response.status_code):
            print 'Response was not successful: {}'.format(query_response.status_code)
            print 'URL: {}'.format(query_string)
            print 'Message: {}'.format(query_response.text)
            return None
        return query_response.text

    def SanitizeArgs(self, query_args):
        for key, value in query_args.iteritems():
            query_args[key] = urllib.quote(value, safe=':')
        return query_args

    def ChangeWOFSource(self, new_wof):
        self.wof = new_wof
        self.base_url = self._original_url.format(wof=new_wof)

    def GetAllSites(self):
        query = self.base_url + self.query_all_sites
        return self.RunQuery(query)

    def _base_GetTimeSeriesValues(self, query_args):
        query = self.base_url + self.query_datavalues.format(network=self.network, **self.SanitizeArgs(query_args))
        return self.RunQuery(query)

    def _base_GetSiteInfo(self, query_args):
        query = self.base_url + self.query_site_info.format(network=self.network, **self.SanitizeArgs(query_args))
        return self.RunQuery(query)

    def _base_GetVariableInfo(self, query_args):
        query = self.base_url + self.query_variable.format(network=self.network, **self.SanitizeArgs(query_args))
        return self.RunQuery(query)

    def __str__(self):
        return 'Query Driver: '.format(self.name)


class iUtahDriver(QueryDriver):
    class iUtahWOF:
        LOGAN = 'LoganRiverWOF'
        REDBUTTE = 'RedButteCreekWOF'
        PROVO = 'ProvoRiverWOF'

        @staticmethod
        def as_list():
            return [iUtahDriver.iUtahWOF.LOGAN, iUtahDriver.iUtahWOF.REDBUTTE, iUtahDriver.iUtahWOF.PROVO]

    def __init__(self, iutah_wof):
        datavalues_query = 'datavalues?location={network}:{site}&variable={network}:{var}/methodCode={method}/' \
                           'sourceCode={source}/qualityControlLevelCode={qc}&startDate={start}&endDate={end}'
        variable_query = 'variables?variable={network}:{var}'
        site_info_query = 'siteinfo?location={network}:{site}'
        super(iUtahDriver, self).__init__(name='iUtahDriver',
                                          url='http://data.iutahepscor.org/{wof}/REST/waterml_1_1.svc/',
                                          wof=iutah_wof,
                                          network='iutah',
                                          query_all_sites='sites',
                                          query_variable=variable_query,
                                          query_datavalues=datavalues_query,
                                          query_site_info=site_info_query)

    def GetTimeSeriesValues(self, site_code, variable_code, method_code, source_code, qc_code, start='', end=''):
        return self._base_GetTimeSeriesValues(dict(site=site_code, var=variable_code, method=method_code,
                                                   source=source_code, qc=qc_code, start=start, end=end))

    def GetSiteInfo(self, site_code):
        return self._base_GetSiteInfo(dict(site=site_code))

    def GetVariableInfo(self, variable_code):
        return self._base_GetVariableInfo(dict(var=variable_code))


class WebSDLDriver(QueryDriver):
    def __init__(self):
        datavalue_query = 'GetValues?location={network}:{site}&variable={network}:{var}&startDate={start}&endDate={end}'
        variable_query = 'GetVariableInfo?variable={network}:{var}'
        site_info_query = 'GetSiteInfo?site={network}:{site}'
        self.network = 'wofpy'
        super(WebSDLDriver, self).__init__(name='WebSDLDriver',
                                           url='http://envirodiysandbox.usu.edu/{wof}/rest/1_1/',
                                           wof='wofpy',
                                           network='wofpy',
                                           query_all_sites='GetSites',
                                           query_variable=variable_query,
                                           query_datavalues=datavalue_query,
                                           query_site_info=site_info_query)

    def GetSiteInfo(self, site_code):
        return self._base_GetSiteInfo(dict(site=site_code))

    def GetVariableInfo(self, variable_code):
        return self._base_GetVariableInfo(dict(var=variable_code, network=self.network))

    def GetTimeSeriesValues(self, site_code, variable_code, method_code='', source_code='', qc_code='',  start='', end=''):
        return self._base_GetTimeSeriesValues(dict(site=site_code, var=variable_code, start=start, end=end))

    def GetDataValues(self, result_id, start='', end=''):
        # type: () -> pd.DataFrame

        script = """   
            SELECT tsrv.datavalue AS "DataValue", tsrv.valuedatetimeutcoffset AS "UTCOffset", 
                   tsrv.valuedatetime AS "DateTime"
            FROM odm2.timeseriesresultvalues as tsrv
            JOIN odm2.results as r on r.resultid = tsrv.resultid
            WHERE r.resultuuid = \'{}\'::uuid""".format(result_id)
        if len(start) > 0:
            script += ' AND valuedatetime > \'{}\'::timestamp'.format(start)
        if len(end) > 0:
            script += ' AND valuedatetime < \'{}\'::timestamp'.format(end)
        script += ';'

        if APP_SETTINGS.VERBOSE:
            print script

        connection_string = 'postgresql://{username}:{password}@{host}:{port}/{database}'
        source_url = connection_string.format(**APP_SETTINGS.tsa_catalog_source)
        from_conn = sqlalchemy.create_engine(source_url)
        values = pd.read_sql(sqlalchemy.text(script), from_conn, coerce_float=True)
        values['DateTime'] = pandas.to_datetime(values['DateTime'])
        values.set_index(['DateTime'], inplace=True)
        values['DataValue'] = pandas.to_numeric(values['DataValue'], errors='coerce')
        values['UTCOffset'] = pandas.to_numeric(values['UTCOffset'], errors='coerce')
        values.dropna(how='any', inplace=True)
        return values

    def GetSitesFromCatalog(self):
        # type: () -> pd.DataFrame
        script = """   
            SELECT r.resultid AS "ResultID", r.resultuuid AS "ResultUUID",
                ('http://wpfdbs.uwrl.usu.edu:8086/query?u=web_client&p=password&db=envirodiy&q=SELECT%20%2A%20FROM%20'||
                '%22uuid_' || replace( cast(r.resultuuid AS TEXT), '-', '_' ) || '%22') AS "GetDataInflux", 
                'uuid_' || replace( cast(r.resultuuid AS TEXT), '-', '_' ) AS "InfluxIdentifier"
            FROM odm2.results AS r
            JOIN odm2.variables AS v 
            ON r.VariableID = v.VariableID
            JOIN odm2.ProcessingLevels AS pl
            ON r.ProcessingLevelID = pl.ProcessingLevelID
            JOIN odm2.FeatureActions AS fa
            ON r.FeatureActionID = fa.FeatureActionID
            JOIN odm2.SamplingFeatures AS sf
            ON fa.SamplingFeatureID = sf.SamplingFeatureID
            JOIN odm2.Sites AS s
            ON sf.SamplingFeatureID = s.SamplingFeatureID
            JOIN odm2.Actions AS ac
            ON fa.ActionID = ac.ActionID
            JOIN odm2.Methods AS me
            ON ac.MethodID = me.MethodID
            JOIN odm2.Units AS uv
            ON r.UnitsID = uv.UnitsID
            JOIN odm2.TimeSeriesResults AS tsr
            ON r.ResultID = tsr.ResultID
            JOIN odm2.ActionBy AS ab
            ON ab.ActionID = ac.ActionID
            JOIN odm2.Affiliations AS aff
            ON ab.AffiliationID = aff.AffiliationID
            JOIN odm2.Organizations AS o 
            ON aff.OrganizationID = o.OrganizationID
            LEFT JOIN odm2.Units AS ut
            ON tsr.IntendedTimeSpacingUnitsID = ut.UnitsID
            WHERE r.ValueCount > 0;
        """

        connection_string = 'postgresql://{username}:{password}@{host}:{port}/{database}'
        source_url = connection_string.format(**APP_SETTINGS.tsa_catalog_source)
        from_conn = sqlalchemy.create_engine(source_url)
        values = pd.read_sql(sqlalchemy.text(script), from_conn)
        return values
