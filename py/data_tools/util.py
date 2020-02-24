rom pyspark.sql import SparkSession,SQLContext 
from pyspark.sql import Row
import platform
import json
python_ver = platform.python_version()[0]
print("PYTHON VERSION: ", python_ver)
def run_session(appname='test'):
    sparkSession = (SparkSession
                .builder
                .appName(appname)
                .enableHiveSupport()
                .getOrCreate())
    return sparkSession


