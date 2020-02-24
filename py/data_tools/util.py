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

def get_strings(obj):

    if isinstance(obj,bool):
        return str(obj)
    #- unicode instance for python 2
    if isinstance(obj,unicode):
        if python_ver=='2':
            return str(obj)
    if isinstance(obj,dict):
        #if python_ver=='2':
        return dict([(get_strings(key),get_strings(val)) for key,val in obj.items()])
        #elif python_ver=='3': #- python 3
        #    return {get_strings(key):get_strings(value) for key,value in obj.items()}
    return obj

def json_loads(listofstrings):
    """
    listofstrings: list of strings, e.g datafr.select("properties").collect() for ecocl data
    """
    flat_val=[]
    for i, val in enumerate(listofstrings):
        if python_ver == '3':
            try: 
                flat_val.append(get_strings(json.loads((val[0]))))
            except json.JSONDecodeError as je:
                flat_val.append({})
        else: #- for python 2 
            try:
                flat_val.append(get_strings(json.loads((val[0]))))
            except: #- this should be valueError
                flat_val.append({})
                
    return flat_val




