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


def dictstoDF(sparksession,listofdicts):
    sc = sparksession.sparkContext
    sqlContext = SQLContext(sc)
    parRdd = sc.parallelize(listofdicts)
    thisdFrame = sqlContext.read.json(parRdd)
    return thisdFrame

def flatten_row(r):
    r_ = r.features.asDict()
    r_.update({'row_num': r.row_num})
    return Row(**r_)

def add_row_num(df,ratio=None):
    
    if ratio is not None:
        df_row_num = df.rdd.zipWithIndex().toDF(['features', 'row_num'],sampleRatio=ratio)
        df_out = df_row_num.rdd.map(lambda x : flatten_row(x)).toDF(sampleRatio=ratio)
    else: #- default 100 rows to infer schema
        df_row_num = df.rdd.zipWithIndex().toDF(['features', 'row_num'])
        df_out = df_row_num.rdd.map(lambda x : flatten_row(x)).toDF()

    return df_out

def mergeDFs(df1,df2,ratio=None):
    """
    merge two pyspark databases
    """
    #- first check for the common columns
    comcols = list(set(df1.columns) & set(df2.columns))
    if len(comcols) > 0:
        combDF=df1.join(df2,tt,"outer")
 
    else: #- no common columns
        df1_row = add_row_num(df1,ratio = ratio)
        df2_row = add_row_num(df2,ratio = ratio)
        combDF = df1_row.join(df2_row, on = 'row_num').drop('row_num')
    return combDF

def extract_key(key,listofdicts):
    newlist=[]
    for ii in range(len(listofdicts)):
        for k,v in listofdicts[ii].items():
            if k == key:
                newlist.append(v)
            else:
                newlist.append(None)
    return newlist 


 
