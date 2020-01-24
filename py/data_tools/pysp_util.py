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


