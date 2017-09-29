import pandas as pd
import psycopg2
from Common import APP_SETTINGS
import sqlalchemy


def update_catalog():
    script = """   
        SELECT  
            1 AS "SourceDataServiceID", 'EnviroDIY' AS "Network", sf.SamplingFeatureCode AS "SiteCode", 
            sf.SamplingFeatureName AS "SiteName", s.Latitude AS "Latitude", s.Longitude AS "Longitude", NULL AS "State", 
            NULL AS "County", s.SiteTypeCV AS "SiteType", v.VariableCode AS "VariableCode", 
            v.VariableNameCV AS "VariableName", 'Common' AS "VariableLevel", me.MethodDescription AS "MethodDescription", 
            uv.UnitsName AS "VariableUnitsName", uv.UnitsTypeCV AS "VariableUnitsType", 
            uv.UnitsAbbreviation AS "VariableUnitsAbbreviation",r.SampledMediumCV AS "SampleMedium", 
            'Field Observation' AS "ValueType", tsr.AggregationstatisticCV AS "DataType", 
            v.VariableTypeCV AS "GeneralCategory", tsr.IntendedTimeSpacing AS "TimeSupport", 
            ut.UnitsName AS "TimeSupportUnitsName", ut.UnitsTypeCV AS "TimeSupportUnitsType", 
            ut.UnitsAbbreviation AS "TimeSupportUnitsAbbreviation", pl.ProcessingLevelCode AS "QualityControlLevelCode", 
            pl.Definition AS "QualityControlLevelDefinition", pl.Explanation AS "QualityControlLevelExplanation", 
            o.OrganizationName AS "SourceOrganization", o.OrganizationDescription AS "SourceDescription", 
            ac.BeginDateTime AS "BeginDateTime", ac.EndDateTime AS "EndDateTime", 
            ac.BeginDateTimeUTCOffset AS "UTCOffset", r.ValueCount AS "NumberObservations", 
            ac.EndDateTime AS "DateLastUpdated", 1 AS "IsActive", r.resultuuid AS "ResultUUID",
            ('http://envirodiysandbox.usu.edu/wofpy/rest/1_1/GetValues?' || 
            'location=wofpy:' || sf.SamplingFeatureCode || '&variable=wofpy:' || v.VariableCode || '&methodCode=' || 
            CAST(me.MethodID AS varchar(15)) || '&sourceCode=' || CAST(o.OrganizationID AS varchar(15)) || 
            '&qualityControlLevelCode=' || pl.ProcessingLevelCode || '&startDate=&endDate=') AS "GetDataURL", 
            ('http://wpfdbs.uwrl.usu.edu:8086/query?u=web_client&p=password&db=envirodiy&q=SELECT%20%2A%20FROM%20'||
            '%22uuid_' || replace( cast(r.resultuuid AS TEXT), '-', '_' ) || '%22') AS "GetDataInflux", 
              'uuid_' || replace( cast(r.resultuuid AS text), '-', '_' ) AS "InfluxIdentifier"
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
        JOIN odm2.Organizations as o 
        ON aff.OrganizationID = o.OrganizationID
        LEFT JOIN odm2.Units AS ut
        ON tsr.IntendedTimeSpacingUnitsID = ut.UnitsID
        WHERE r.ValueCount >0;
    """

    connection_string = 'postgresql://{username}:{password}@{host}:{port}/{database}'
    source_url = connection_string.format(**APP_SETTINGS.tsa_catalog_source)
    destination_url = connection_string.format(**APP_SETTINGS.tsa_catalog_destination)

    from_conn = sqlalchemy.create_engine(source_url)
    values = pd.read_sql(sqlalchemy.text(script), from_conn)

    if APP_SETTINGS.VERBOSE:
        print(values)
    to_conn = sqlalchemy.create_engine(destination_url)

    # empty table and reset sequence counter
    conn = to_conn.connect()
    delete_result = conn.execute('DELETE FROM public."DataSeries"')
    sequence_result = conn.execute('ALTER SEQUENCE "Catalog".series_increment RESTART WITH 1')

    if APP_SETTINGS.VERBOSE:
        print(delete_result)
        print(sequence_result)

    # fill table
    values.to_sql(name="DataSeries", con=to_conn, if_exists="append", index=False)
    return True


if __name__ == "__main__":
    if update_catalog():
        print('Catalog updated without error')
    # import timeit
    # time = timeit.Timer(timer=update_catalog())
    # print(str(time))
