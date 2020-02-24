from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession,SQLContext,HiveContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
sc = SparkContext(appName="pysp_tools")
sqlContext = HiveContext(sc)

def get_tables(database):
    sqlContext.sql("use "+database)
    tablesrows=sqlContext.sql("show tables").collect()
    tables=[str(row.tableName) for row in tablesrows]
    return tables

def get_cols(tablename):
    colrows=sqlContext.sql("desc "+tablename).collect()
    cols=[str(row.col_name) for row in colrows]
    return cols

def check_field(database,fieldlike,tables=None,outtables=False):
    #sqlContext.sql("use "+database)
    foundin=[]
    if tables is None:
        alltables=get_tables(database)
        #- check tmp tables and remove from the list
        tables= [s for s in alltables if "tmp_" !=s[:4]]
    else:
        #- load the database here
        sqlContext.sql("use "+database)
    for table in tables:
        columns=get_cols(table)
        matching = [s for s in columns if fieldlike in s]
        if len(matching)>0:
            foundin.append(table)
            print("Matching columns found in Table: "+table+":  ",matching)
        #else:
        #    print("No matchin column found in Table: "+table)
    print("Finish checking "+database+" for "+fieldlike)
    if outtables:
        return foundin
    return

def parsePandas(csvfile):
    #- to parse a csv/tsv wtiten using hive e.g:
    
    df=pd.read_csv(csvfile,delimiter="\t",names=['load_ts','id','ts','properties','data_dt'],header=0)

    propVals=df.properties.values
    ndf=df.drop("properties",axis=1)
    propParsed=[]
    for i, val in enumerate(propVals):
        try:
            propParsed.append(util.get_strings(json.loads((val.lower()))))
        except json.JSONDecodeError as je:
            propParsed.append({})
   
    propDF=pd.io.json.json_normalize(propParsed)
    parsedDF=pd.concat([ndf,propDF],axis=1)
    return parsedDF

def sortDFby(datafr,key):
    """
    datafr: Pandas dataframe
    """
    from datetime import datetime
    if key == "load_ts":
        load_ts=[datetime.strptime(ind,"%Y/%m/%d %H:%M:%S") for ind in datafr[key]]
        tseries=pd.to_datetime(load_ts)

    if key == "ts":
        tseries=pd.DatetimeIndex(pd.to_datetime(datafr[key],unit='ms'))

    ts,kk=tseries.sortlevel()
    sortedDF=datafr.iloc[kk]
    return sortedDF

