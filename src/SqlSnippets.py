import datetime

catalog_purge = 'DELETE FROM public."DataSeries"'
sequence_reset = 'ALTER SEQUENCE "Catalog".series_increment RESTART WITH 1'

envirodiy_catalog_script = """   
SELECT  
    1 AS "SourceDataServiceID", 'EnviroDIY' AS "Network", sf.SamplingFeatureCode AS "SiteCode", 
    sf.SamplingFeatureName AS "SiteName", s.Latitude AS "Latitude", s.Longitude AS "Longitude", 
    NULL AS "State", 
    NULL AS "County", s.SiteTypeCV AS "SiteType", v.VariableCode AS "VariableCode", 
    v.VariableNameCV AS "VariableName", 'Common' AS "VariableLevel", me.MethodDescription AS 
    "MethodDescription", 
    uv.UnitsName AS "VariableUnitsName", uv.UnitsTypeCV AS "VariableUnitsType", 
    uv.UnitsAbbreviation AS "VariableUnitsAbbreviation",r.SampledMediumCV AS "SampleMedium", 
    'Field Observation' AS "ValueType", tsr.AggregationstatisticCV AS "DataType", 
    v.VariableTypeCV AS "GeneralCategory", tsr.IntendedTimeSpacing AS "TimeSupport", 
    ut.UnitsName AS "TimeSupportUnitsName", ut.UnitsTypeCV AS "TimeSupportUnitsType", 
    ut.UnitsAbbreviation AS "TimeSupportUnitsAbbreviation", pl.ProcessingLevelCode AS 
    "QualityControlLevelCode", 
    pl.Definition AS "QualityControlLevelDefinition", pl.Explanation AS "QualityControlLevelExplanation", 
    o.OrganizationName AS "SourceOrganization", o.OrganizationDescription AS "SourceDescription", 
    ac.BeginDateTime AS "BeginDateTime", ac.EndDateTime AS "EndDateTime", 
    ac.BeginDateTimeUTCOffset AS "UTCOffset", r.ValueCount AS "NumberObservations", 
    tsrvalues.max AS "DateLastUpdated", 1 AS "IsActive", r.resultuuid AS "ResultUUID",
    ('http://envirodiysandbox.usu.edu/wofpy/rest/1_1/GetValues?' || 
    'location=wofpy:' || sf.SamplingFeatureCode || '&variable=wofpy:' || v.VariableCode || '&methodCode=' || 
    CAST(me.MethodID AS VARCHAR(15)) || '&sourceCode=' || CAST(o.OrganizationID AS VARCHAR(15)) || 
    '&qualityControlLevelCode=' || pl.ProcessingLevelCode || '&startDate=&endDate=') AS "GetDataURL", 
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
JOIN (SELECT DISTINCT ON (resultid) resultid, MAX(valuedatetime) 
      FROM odm2.TimeSeriesResultValues GROUP BY resultid) AS tsrvalues
ON r.ResultID = tsrvalues.resultid
JOIN odm2.ActionBy AS ab
ON ab.ActionID = ac.ActionID
JOIN odm2.Affiliations AS aff
ON ab.AffiliationID = aff.AffiliationID
JOIN odm2.Organizations AS o 
ON aff.OrganizationID = o.OrganizationID
LEFT JOIN odm2.Units AS ut
ON tsr.IntendedTimeSpacingUnitsID = ut.UnitsID
WHERE r.ValueCount >0;
"""

get_sites_envirodiy = """   
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

get_sites_iutah = """
SELECT sc.SiteID, v.VariableID, qc.QualityControlLevelID, sc.SourceID, sc.MethodID, 
  ('wof_' + sc.SiteCode + '_' + v.VariableCode + '_' + qc.QualityControlLevelCode + '_' + 
CAST(sc.SourceID AS VARCHAR(15)) + '_' + CAST(sc.MethodID AS VARCHAR(15))) AS InfluxIdentifier
FROM [{DATABASE}].dbo.[SeriesCatalog] AS sc
JOIN [{DATABASE}].dbo.[Variables] AS v
ON sc.VariableID = v.VariableID
JOIN [{DATABASE}].dbo.[Sites] AS S
ON sc.SiteID = S.SiteID
JOIN [{DATABASE}].dbo.[Units] AS uv
ON sc.VariableUnitsID = uv.UnitsID
JOIN [{DATABASE}].dbo.[Units] AS ut
ON sc.TimeUnitsID = ut.UnitsID
JOIN [{DATABASE}].dbo.[QualityControlLevels] AS qc
ON sc.QualityControlLevelID = qc.QualityControlLevelID
WHERE sc.ValueCount > CAST(0 AS INT);
"""

iutah_catalog_script = """
SELECT 2 AS SourceDataServiceID, '{network}' AS Network, sc.SiteCode, sc.SiteName, s.Latitude, s.Longitude, s.State, 
s.County, s.SiteType,
v.VariableCode, v.VariableName, vl.VariableLevel, sc.MethodDescription,
uv.UnitsName AS VariableUnitsName, uv.UnitsType AS VariableUnitsType, uv.UnitsAbbreviation AS VariableUnitsAbbreviation,
v.SampleMedium, v.ValueType, v.DataType, v.GeneralCategory, v.TimeSupport,
ut.UnitsName AS TimeSupportUnitsName, ut.UnitsType AS TimeSupportUnitsType, ut.UnitsAbbreviation AS 
TimeSupportUnitsAbbreviation,
qc.QualityControlLevelCode, qc.Definition AS QualityControlLevelDefinition, qc.Explanation AS 
QualityControlLevelExplanation,
sc.Organization AS SourceOrganization, sc.SourceDescription,
sc.BeginDateTime, sc.EndDateTime, DATEDIFF(hh,sc.BeginDateTimeUTC,sc.BeginDateTime) AS UTCOffset,
sc.ValueCount AS NumberObservations, sc.EndDateTime AS DateLastUpdated, 1 AS IsActive,
('http://data.iutahepscor.org/{wof}/REST/waterml_1_1.svc/datavalues?' +
'location=iutah:' + sc.SiteCode + '&variable=iutah:' + sc.VariableCode + '/methodCode=' + CAST(sc.MethodID AS 
VARCHAR(15)) +
'/sourceCode=' + CAST(sc.SourceID AS VARCHAR(15)) + '/qualityControlLevelCode=' + CAST(sc.QualityControlLevelID AS 
VARCHAR(15)) +
'&startDate=&endDate=') AS GetDataURL, 
('http://iutahinflux.uwrl.usu.edu:8086/query?u=web_client&p=password&db=iutah&q=SELECT%20%2A%20FROM%20' 
+ '%22wof_' + sc.SiteCode + '_' + v.VariableCode + '_' + qc.QualityControlLevelCode + '_' + 
CAST(sc.SourceID AS VARCHAR(15)) + '_' + CAST(sc.MethodID AS VARCHAR(15)) + '%22') AS GetDataInflux,
  ('wof_' + sc.SiteCode + '_' + v.VariableCode + '_' + qc.QualityControlLevelCode + '_' + 
CAST(sc.SourceID AS VARCHAR(15)) + '_' + CAST(sc.MethodID AS VARCHAR(15))) AS InfluxIdentifier
FROM [{DATABASE}].dbo.[SeriesCatalog] AS sc
JOIN [{DATABASE}].dbo.[Variables] AS v
ON sc.VariableID = v.VariableID
JOIN [TSA_Catalog].dbo.[CommonVariableLookup] AS vl
ON v.VariableCode = vl.VariableCode
JOIN [{DATABASE}].dbo.[Sites] AS S
ON sc.SiteID = S.SiteID
JOIN [{DATABASE}].dbo.[Units] AS uv
ON sc.VariableUnitsID = uv.UnitsID
JOIN [{DATABASE}].dbo.[Units] AS ut
ON sc.TimeUnitsID = ut.UnitsID
JOIN [{DATABASE}].dbo.[QualityControlLevels] AS qc
ON sc.QualityControlLevelID = qc.QualityControlLevelID
WHERE sc.ValueCount > CAST(0 AS INT)
ORDER BY SiteCode, VariableCode
"""

iutah_get_datavalues = """
SELECT DataValue AS "DataValue", LocalDateTime AS "DateTime", UTCOffset AS "UTCOffset"
  FROM {database}.dbo.DataValues
  WHERE SiteID = {site}
    AND VariableID = {variable}
    AND MethodID = {method}
    AND SourceID = {source}
    AND QualityControlLevelID = {qc}
    {from_date}
"""

envirodiy_get_datavalues = """   
            SELECT tsrv.datavalue AS "DataValue", tsrv.valuedatetimeutcoffset AS "UTCOffset", 
                   tsrv.valuedatetime AS "DateTime"
            FROM odm2.timeseriesresultvalues AS tsrv
            JOIN odm2.results AS r ON r.resultid = tsrv.resultid
            WHERE r.resultuuid = \'{}\'::UUID{resultuuid}"""


class SqlSnippets(object):
    DB_SQL_MAP = {
        'envirodiy': lambda database: EnvirodiyDataSeriesSnippets(database),
        'iUTAH_Logan_OD': lambda database: iUtahSqlSnippets(database),
        'iUTAH_RedButte_OD': lambda database: iUtahSqlSnippets(database),
        'iUTAH_Provo_OD': lambda database: iUtahSqlSnippets(database)
    }

    def __init__(self, database, language, fetch_script, purge_catalog, reset_sequence, get_sites, get_datavalues):
        self.get_catalog_sites = get_sites
        self.get_datavalues = get_datavalues
        self.extract_identifying_args = None
        self.fetch_dataseries = fetch_script
        self.purge_catalog = purge_catalog
        self.reset_sequence = reset_sequence
        self.sql_language = language
        self.database = database

    def get_datavalues_query(self, args):
        print 'Can\'t get data values from the base function'

    def __str__(self):
        return '<SqlSnippets: {}, {}>'.format(self.database, self.sql_language)

    @staticmethod
    def GetSqlSnippets(database):
        """

        :rtype: SqlSnippets
        """
        return SqlSnippets.DB_SQL_MAP[database](database)


class EnvirodiyDataSeriesSnippets(SqlSnippets):
    def __init__(self, database):
        SqlSnippets.__init__(self, database, 'postgresql', envirodiy_catalog_script, catalog_purge, sequence_reset,
                             get_sites_envirodiy, envirodiy_get_datavalues)
        self.extract_identifying_args = lambda dataseries, index: {
            'resultuuid': dataseries.get_value(index, 'ResultUUID')
        }

    def get_datavalues_query(self, result_id, last_entry):
        script = """   
            SELECT tsrv.datavalue AS "DataValue", tsrv.valuedatetimeutcoffset AS "UTCOffset", 
                   tsrv.valuedatetime AS "DateTime"
            FROM odm2.timeseriesresultvalues as tsrv
            JOIN odm2.results as r on r.resultid = tsrv.resultid
            WHERE r.resultuuid = \'{}\'::uuid""".format(result_id)


        if last_entry is not None:
            end_time = (last_entry + datetime.timedelta(seconds=0)).strftime('%Y-%m-%dT%H:%M:%S')
            script += ' AND valuedatetime > \'{}\'::timestamp'.format(end_time)
        script += ';'

        print script

        return script


class iUTAH_MAPS:
    NETWORKS = {
        'iUTAH_Logan_OD': 'Logan River',
        'iUTAH_RedButte_OD': 'Red Butte Creek',
        'iUTAH_Provo_OD': 'Provo River'
    }

    WOF = {
        'iUTAH_Logan_OD': 'LoganRiverWOF',
        'iUTAH_RedButte_OD': 'RedButteCreekWOF',
        'iUTAH_Provo_OD': 'ProvoRiverWOF'
    }


class iUtahSqlSnippets(SqlSnippets):
    def __init__(self, database):
        print database
        fetch_script = iutah_catalog_script.format(DATABASE=database, network=iUTAH_MAPS.NETWORKS[database],
                                                   wof=iUTAH_MAPS.WOF[database])
        get_sites = get_sites_iutah.format(DATABASE=database)
        SqlSnippets.__init__(self, database, 'mssql', fetch_script, catalog_purge, sequence_reset, get_sites,
                             iutah_get_datavalues)
        self.extract_identifying_args = lambda dataseries, index: {
            'site': dataseries.get_value(index, 'SiteID'),
            'variable': dataseries.get_value(index, 'VariableID'),
            'qc': dataseries.get_value(index, 'QualityControlLevelID'),
            'source': dataseries.get_value(index, 'SourceID'),
            'method': dataseries.get_value(index, 'MethodID'),
            'database': self.database
        }

    def get_datavalues_query(self, series_dict, last_entry):
        print series_dict
        query_date = ''
        if last_entry is not None:
            end_time = (last_entry + datetime.timedelta(seconds=0)).strftime('%Y-%m-%dT%H:%M:%S')
            query_date = "AND LocalDateTime >= '{}'".format(end_time)
        # LocalDateTime >= '2016-09-27T12:00:00'

        return self.get_datavalues.format(from_date=query_date, **series_dict)
