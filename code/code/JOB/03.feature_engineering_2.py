##############
#### 指引 ####
##############
# 請依下方TODO修改路徑
# Step1: 修改腳本directory (TODO_0)
# Step2: 確認自己富邦utils的functions要放在哪裡 (TODO_1)
# Step3: 在TODO_1的directory下新增Log資料夾 (TODO_2)
# Step4: 建議自行設定spark session (TODO_3)
# Step5: 因為之後要使用sql，所以須將目錄改到相應的sql/JOB_SQL資料夾位置 (TODO_4)
# Step6: 請修改config_tbl.py中VIEW及表名 (TODO_5) 
# Step7: 需修改Feature Engineering後Parquet_v1存放位置 (TODO_6)
# Step8: 修改Parquet存放位置 (TODO_7)


#######################################
#### 更改當下路徑到運行腳本的資料夾路徑 ####
#######################################
import os
# 將工作路徑改到此 script 的路徑
abspath = os.getcwd()
print(f'Original absolute path: {abspath}')
print('Change working directory...')
os.chdir(r'./P&B_Development/Training')                       # TODO_0: 因為之後要使用富邦utils的functions，所以一定要改目錄到相應的位置
abspath = os.getcwd()
print(f'Current absolute path: {abspath}')
            

#####################
## Import packages ##
#####################
import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
import json
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score,f1_score,confusion_matrix,make_scorer, accuracy_score, precision_score, recall_score
import pickle
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType,StringType, IntegerType, FloatType, BooleanType, LongType, DateType
from sklearn.model_selection import GridSearchCV, KFold, RandomizedSearchCV, train_test_split
import time
from datetime import date, datetime, timedelta
from pyspark.sql.functions import col, countDistinct, when, round, ceil, trim, concat, coalesce, monotonically_increasing_id, datediff, date_format, row_number, to_timestamp, concat, unix_timestamp, lit, collect_list
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import random
from datetime import date, datetime
import dateutil
import logging
from logging.handlers import RotatingFileHandler
import utils.feature_engineering_2023 as fe                        # TODO_1: 確認自己富邦utils的functions要放在哪裡，決定TODO_1位置
import utils.config_tbl as config                                  # TODO_1: 確認自己富邦utils的functions要放在哪裡，決定TODO_1位置
from IPython.display import display


################################
#### 設定一般腳本logger的規則 ####
################################
def build_logger(log_path: str):
    # 定義一個RotatingFileHandler，最多備份5個日志文件，每個日志文件最大10M
    Rthandler = RotatingFileHandler(log_path, maxBytes=10*1024*1024, backupCount=5, encoding = 'utf-8')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    Rthandler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    Rthandler.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(Rthandler)
    logging.getLogger().setLevel(logging.INFO)
    # initial function
    logger=logging.getLogger()
    return logger

################################
#### 設定一般腳本logger的規則 ####
################################
logger = build_logger('./Log/P&B_validation.log')           # TODO_2: 在TODO_1的directory下新增Log資料夾
logger.info(f"{'*' * 24} Job of P&B Feature Engineering 2 Started {'*' * 24}")
logger.info(f'Original absolute path: {abspath}')
logger.info(f'Current absolute path: {abspath}')
logger.info(f"{'='*70}")

#########################
###Start Spark Session###
#########################
spark = build_spark_session(method = 'normal_gpu')          # TODO_3: 建議自行設定spark session


#################################
########## 設定時間區間 ##########
################################
today = datetime.today().date()
max_date = today.strftime('%Y-%m-%d')
print(max_date)
min_date = (today - dateutil.relativedelta.relativedelta(months = 1)).strftime('%Y-%m-%d')
print(min_date)

################################
#### Initiate impala logger ####
################################
# Initiate logger
impala_logger = get_logger('test')

#######################################
#### 更改當下路徑到運行腳本的資料夾路徑 ##
#######################################
import os
# 將工作路徑改到此 script 的路徑
abspath = os.getcwd()
print(f'Original absolute path: {abspath}')
print('Change working directory...')
os.chdir(r'/home/cdsw/sql/JOB_SQL')                         # TODO_4: 因為之後要使用sql，所以須將目錄改到相應的sql/JOB_SQL資料夾位置
abspath = os.getcwd()
print(f'Current absolute path: {abspath}')

#################
####讀取參數檔####
#################
# 建立參數                                                   # TODO_5: 請修改config_tbl.py中VIEW及表名
HIVE_VIEW = config.HIVE_VIEW
# 行銀資料表
MB_LOG_VIEW = config.MB_LOG_VIEW                                             # 行銀資料VIEW
MB_ACCESS_LOG = config.MB_ACCESS_LOG                                         # 行銀資料檔名
B2C_LOG_VIEW = config.B2C_LOG_VIEW                                           # 網銀資料VIEW
B2C_ACCESS_LOG = config.B2C_ACCESS_LOG                                       # 網銀資料檔名
print(f'HIVE_VIEW: {HIVE_VIEW}')
print(f'ORACLE_VIEW: {ORACLE_VIEW}')
            
#########################################
#### 更改當下路徑到運行腳本的資料夾路徑 ####
#########################################
import os
# 將工作路徑改到此 script 的路徑
abspath = os.getcwd()
print(f'Original absolute path: {abspath}')
print('Change working directory...')
os.chdir(r'/home/cdsw/P&B_Development/Training/P&B_Tech_Transfer_python/Data')  # 因為之後要用到自己寫的 function，所以一定要改目錄到相應的位置
abspath = os.getcwd()
print(f'Current absolute path: {abspath}')

##########################
###建立帳號、cust_id資料###
##########################
nonwarning = pd.read_csv(f'nonwarning_feature_engineering.csv')
#建立最晚交易日表
nonwarning_1 = nonwarning
nonwarning_cust = nonwarning_1[['acct_nbr_ori', 'cust_id']].drop_duplicates()
res_max = nonwarning_1.groupby('cust_id')['tx_date'].max().reset_index()
res_max = res_max.rename(columns = {'tx_date': 'max_date'})
df_max = res_max[['cust_id', 'max_date']]
#建立最早交易日表
res_min = nonwarning_1.groupby('cust_id')['tx_date'].min().reset_index()
res_min = res_min.rename(columns = {'tx_date': 'min_date'})
df_min = res_min[['cust_id', 'min_date']]
#合併最早交易日表
nonwarning_cust = nonwarning_cust.merge(df_max, on = 'cust_id', how = 'left')
nonwarning_cust = nonwarning_cust.merge(df_min, on = 'cust_id', how = 'left')

#轉換為時間戳記格式
#nonwarning_cust['max_date'] = pd.to_datetime(nonwarning_cust['max_date'], format='%Y-%m-%d %H:%M:%S')
#nonwarning_cust['min_date'] = pd.to_datetime(nonwarning_cust['min_date'], format='%Y-%m-%d %H:%M:%S')
nonwarning_cust['max_date'] = nonwarning_cust['max_date'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
nonwarning_cust['min_date'] = nonwarning_cust['min_date'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))

#轉為spark dataframe
from pyspark.sql.types import *
caseSchema = StructType([
    StructField('acct_nbr_ori', StringType(), True),
    StructField('cust_id', StringType(), True),
    StructField('max_date', TimestampType(), True),
    StructField('min_date', TimestampType(), True)
])
nonwarning_cust = spark.createDataFrame(nonwarning_cust, schema = caseSchema)
nonwarning_cust.coalesce(1).write.mode('overwrite').saveAsTable(f'{HIVE_VIEW}.job_cust_id_pred') 

#######################
## 客戶資料 to impala ##
#######################
# invalidate_metadata
impala_sql('', impala_logger, mode = 'invalidate_metadata', table_name = f'{HIVE_VIEW}.job_cust_id_pred') 

#########################################
#### 更改當下路徑到運行腳本的資料夾路徑 ####
#########################################
import os
# 將工作路徑改到此 script 的路徑
abspath = os.getcwd()
print(f'Original absolute path: {abspath}')
print('Change working directory...')
os.chdir(r'/home/cdsw/P&B_Development/Training/P&B_Tech_Transfer_python/JOB_SQL')  # 因為之後要用到自己寫的 function，所以一定要改目錄到相應的位置
abspath = os.getcwd()
print(f'Current absolute path: {abspath}')
            

#######################################################
########## 修改網行銀查詢紀錄表VIEW、TABLE NAME ##########
#######################################################
tbl_name = 'job_tracking_log_pred'
# open file
with open(f'POLICE_DASB_{tbl_name}.sql',"r") as f:
    string = f.read().replace('HIVE_VIEW',str(HIVE_VIEW))          
# write file
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"w") as f:
    f.write(string)

with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"r") as f:
    string = f.read().replace('MB_LOG_VIEW',str(MB_LOG_VIEW))          
# write file
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"w") as f:
    f.write(string)

with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"r") as f:
    string = f.read().replace('MB_ACCESS_LOG',str(MB_ACCESS_LOG))          
# write file
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"w") as f:
    f.write(string)

with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"r") as f:
    string = f.read().replace('B2C_LOG_VIEW',str(B2C_LOG_VIEW))          
# write file
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"w") as f:
    f.write(string)

with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"r") as f:
    string = f.read().replace('B2C_ACCESS_LOG',str(B2C_ACCESS_LOG))          
# write file
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"w") as f:
    f.write(string)
    
#################################
#### 由impala計算網行銀查詢次數 ####
#################################
try:
    #建立IP變更次數資料
    tbl_name = 'job_tracking_log_pred'
    # Create impala table
    impala_sql(f'POLICE_DASB_{tbl_name}_tmp_file.sql', impala_logger, mode='overwrite', table_name = f'{HIVE_VIEW}.POLICE_DASB_{tbl_name}')
    print('Tracking_log_alarm Table Creation done!')
except Exception:
    logging.exception('message')
    print('Tracking_log_alarm Creation failed.')

#############################
#### 由Hadoop撈取查餘額次數 ##
#############################
try:
    print('Merging Table started...')
    # Hadoop撈取查餘額次數
    tbl_name = 'job_tracking_log_pred'
    # Create Hive table
    query = f'''
    SELECT *
    FROM usr_julian_liu.POLICE_DASB_{tbl_name}
    '''
    # impala to Hive
    sdf_tracking_log = spark.sql(query)
    sdf_tracking_log.show(2)
    df_tracking_log= sdf_tracking_log.toPandas()
    print(type(df_tracking_log['access_date']))
    df_tracking_log['access_date'] = pd.to_datetime(df_tracking_log['access_date'])
    nonwarning['tx_date'] = pd.to_datetime(nonwarning['tx_date'])
    nonwarning = nonwarning.merge(df_tracking_log, left_on = ['cust_id', 'tx_date'], right_on = ['company_uid', 'access_date'], how = 'left')
    nonwarning['mb_limit'] = nonwarning['mb_limit'].fillna(0)
    nonwarning['mb_check'] = nonwarning['mb_check'].fillna(0)
    nonwarning['eb_check'] = nonwarning['eb_check'].fillna(0)
    nonwarning = nonwarning.drop('access_date', axis = 1)
    nonwarning = nonwarning.drop('company_uid', axis = 1)
    nonwarning[nonwarning['mb_limit']!= 0].head(2)
    print('Merging Table done!')
except Exception:
    logging.exception('message')
    print('Tracking_log_alarm Creation failed.')
            
#########################################
#### 更改當下路徑到運行腳本的資料夾路徑 ####
#########################################
import os
# 將工作路徑改到此 script 的路徑
abspath = os.getcwd()
print(f'Original absolute path: {abspath}')
print('Change working directory...')
os.chdir(r'/home/cdsw/P&B_Development/Training/P&B_Tech_Transfer_python/Data')  # 因為之後要用到自己寫的 function，所以一定要改目錄到相應的位置
abspath = os.getcwd()
print(f'Current absolute path: {abspath}')            
            
#####################
#### 儲存預測資料 ####
#####################
print('Saving file...')
nonwarning.to_csv(f'nonwarning_feature_engineering_{min_date}_{max_date}_v2.csv', index = False)
print('Saving Table done!')

################################
#### 設定一般腳本logger的規則 ####
################################
logger.info(f"{'*' * 24} Job of P&B Feature Engineering 2 Done {'*' * 24}")
            
############################
## Turn off Spark Session ##
############################
spark.stop()