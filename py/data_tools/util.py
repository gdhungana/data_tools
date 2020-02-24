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


#- create partitioned hive table (agnostic)
def createDtPartitionTable(hiveContext,path,from_table,to_table,dt,bucketing=False,bucketing_key='',bucketing_size=64,external='external'):
    test_rows=hiveContext.sql("select * from "+from_table+" limit 1").collect()
    #assert (len(test_rows) > 0),"ERROR: no records in "+from_table +' for to_table: '+to_table+ ' dt=' +dt; 
    if (len(test_rows) < 0): print("+++ERROR: no records in "+from_table +' for to_table: '+to_table+ ' dt=' +dt); 
    print("from_table is:  "+from_table)
    #hiveContext.sql('DROP TABLE if exists '+table);
    new_rows=hiveContext.sql("desc "+from_table).collect()

    new_schema = ','.join(['`'+row.col_name+ '` '+row.data_type for row in new_rows if (row[0]!='dt' and 'Partition' not in row[0] and 'col_name' not in row[0]) ])
    new_col_names = ','.join(['`'+row.col_name+ '`' for row in new_rows if (row[0]!='dt' and 'Partition' not in row[0] and 'col_name' not in row[0])])

    table_exists = False
    tables=hiveContext.sql("show tables").collect()
    for row in tables:
        if to_table==row[0]:
            table_exists = True
            break

    if table_exists:
        existing_schema=''
        existing_rows=hiveContext.sql("desc "+to_table).collect()
        existing_col_names = ','.join([row.col_name for row in existing_rows if (row[0]!='dt' and 'Partition' not in row[0] and 'col_name' not in row[0]) ])
        ###### see if existsing table has the same schema if not drop and recreate 
        old_name=existing_col_names.lower().replace('`','')
        new_name=new_col_names.lower().replace('`','')
        assert (old_name == new_name),"ERROR: schema changed!! to_table:" + to_table+ "; old: "+old_name+'; new: '+new_name

    if path =='':
        location_clause=''
    else:
        location_clause="LOCATION '"+path +"'"
    print("create "+ external+" table IF NOT EXISTS "+to_table+ " ("+new_schema+") " \
        +"PARTITIONED BY (dt int) " \
        +"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' "\
        +location_clause)
    hiveContext.sql("create "+ external+" table IF NOT EXISTS "+to_table+ " ("+new_schema+") " \
        +"PARTITIONED BY (dt int) " \
        +"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' "\
        +location_clause) 
    #new_col_names+= ','+str(dt)
    if bucketing: 
        hiveContext.sql("SET hive.enforce.bucketing=true")
        hiveContext.sql( "ALTER TABLE "+ to_table+ " CLUSTERED BY ("+bucketing_key+") into "+str(bucketing_size)+" BUCKETS ")
    #- load data
    s="insert overwrite table "+to_table+" partition (dt="+str(dt)+") select "+new_col_names + " from "+from_table
    print(s)
    hiveContext.sql(s)   
