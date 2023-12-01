##############
#### 指引 ####
##############
# 請依下方TODO修改路徑
# Step1: 修改腳本directory (TODO_0)
# Step2: 確認自己富邦utils的functions要放在哪裡 (TODO_1)
# Step3: 在TODO_1的directory下新增Log資料夾 (TODO_2)
# Step4: 建議自行設定spark session (TODO_3)
# Step5: 需修改Data cleansing後Parquet存放位置(TODO_4)
# Step6: 需修改Feature Engineering後存放位置(TODO_5)

#######################################
#### 更改當下路徑到運行腳本的資料夾路徑 ####
#######################################
import os
# 將工作路徑改到此 script 的路徑
abspath = os.getcwd()
print(f'Original absolute path: {abspath}')
print('Change working directory...')
os.chdir(r'./P&B_Development/Training')                # TODO_0: 因為之後要使用富邦utils的functions，所以一定要改目錄到相應的位置
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
import utils.feature_engineering_2023 as fe             # TODO_1: 確認自己富邦utils的functions要放在哪裡，決定TODO_1位置
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
logger = build_logger('./Log/P&B_validation.log')       # TODO_2: 在TODO_1的directory下新增Log資料夾
logger.info(f"{'*' * 24} Job of P&B Feature Engineering 1 Started {'*' * 24}")
logger.info(f'Original absolute path: {abspath}')
logger.info(f'Current absolute path: {abspath}')
logger.info(f"{'='*70}")


#########################
###Start Spark Session###
#########################
#請開最大運算資源、normal spark session
spark = build_spark_session(method = 'normal_gpu')      # TODO_3: 建議自行設定spark session


#################################
########## 設定時間區間 ##########
################################
today = datetime.today().date()
max_date = today.strftime('%Y-%m-%d')
print(max_date)
min_date = (today - dateutil.relativedelta.relativedelta(months = 1)).strftime('%Y-%m-%d')
print(min_date)

#########################################
#### 更改當下路徑到運行腳本的資料夾路徑 ####
#########################################
import os
# 將工作路徑改到此 script 的路徑
abspath = os.getcwd()
print(f'Original absolute path: {abspath}')
print('Change working directory...')
os.chdir(r'/home/cdsw/P&B_Development/Training/P&B_Tech_Transfer_python/Data')  # 請修改:因為之後要用到自己寫的 function，所以一定要改目錄到相應的位置
abspath = os.getcwd()
print(f'Current absolute path: {abspath}')
            
#############################
######## 排程特徵工程 ########
#############################
#請開最大運算資源、normal spark session
try:
    # add logging
    logger.info(f'{"=" * 20} Normal Account Feature Engineering {"=" * 20}')
    logger.info('Normal Account Feature Engineering started')
    start = time.time()
    # 讀取預測交易資料
    nonwarning_spark = pd.read_csv(f'nonwarning.csv')                      # TODO_4:需修改Data cleansing後Parquet存放位置
    # 建立id_list
    nonwarning_id_list = nonwarning_spark['cust_id'].unique().tolist()
    # 取得min、max date
    normal_min_date = nonwarning_spark['tx_date'].min()
    normal_max_date = nonwarning_spark['tx_date'].max()
    #撈取INVM資料
    #====
    date = datetime.now()
    date = date - dateutil.relativedelta.relativedelta(days=1)
    date = date.strftime('%Y-%m-%d')
    query = f""" select ACCT_NO_14, ACCT_OPEN_DT from ODS_T_VIEW.INVM where snap_date = '{date}' """
    invm = spark.sql(query).toPandas()
    #===
    #撈取約轉資料
    #====
    query = f''' select * from usr_julian_liu.cfpebtrfin '''
    cfpebtrfin = spark.sql(query).toPandas()
    #===
    # 正常戶特徵工程
    nonwarning_spark = fe.preprocessing_source(nonwarning_spark, spark, invm = invm, cfpebtrfin = cfpebtrfin, mode = 'train', id_list = nonwarning_id_list, min_date = normal_min_date, max_date = normal_max_date, date_list = None, source = 'alarm', label = 0)
    logger.info('Normal Account Feature Engineering done.')
    print('Saving file...')
    nonwarning_spark.to_csv('nonwarning_feature_engineering.csv', index = False)    #TODO_5: 需修改Feature Engineering後存放位置
    print('Saving Done.')
    #add logging
    end = time.time()
    logger.info(f'time consumed: {(end-start)/60} minutes')
    logger.info('Normal Account Feature Engineering and data exporting done.')
    logger.info(f'{"="*70}')
except Exception:
    logging.exception('message')
    print('Normal Account Feature Engineering failed.')
    logging.error('Normal Account Feature Engineering failed.')
    logger.info(f'{"="*70}')

################################
#### 設定一般腳本logger的規則 ####
################################
logger.info(f"{'*' * 24} Job of P&B Feature Engineering 1 Done {'*' * 24}")
    
############################
## Turn off Spark Session ##
############################
spark.stop()