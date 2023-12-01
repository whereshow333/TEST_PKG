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
# Step7: 如資料在ORACLE，需修改ORACLE與Hadoop連線方式(視各家銀行系統而訂) (TODO_6)
# Step8: 修改核心客戶檔存放位置名稱[HIVE_VIEW]，將資料從Hive拉到Impala (TODO_7)
# Step9: 修改證券客戶XX: 證券商及保代號 (TODO_8)
# Step10: 需修改Parquet存放位置 (TODO_9)


#######################################
#### 更改當下路徑到運行腳本的資料夾路徑 ####
#######################################
import os
# 將工作路徑改到此 script 的路徑
abspath = os.getcwd()
print(f'Original absolute path: {abspath}')
print('Change working directory...')
os.chdir(r'./P&B_Development/Training')  # TODO_0: 因為之後要使用富邦utils的functions，所以一定要改目錄到相應的位置
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
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType,StringType, IntegerType, FloatType, BooleanType, LongType, DateType
import time
from datetime import date, datetime, timedelta
from pyspark.sql.functions import col, countDistinct, when, round, ceil, trim, concat, coalesce, monotonically_increasing_id, datediff, date_format, row_number, to_timestamp, concat, unix_timestamp, lit, collect_list
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from datetime import date, datetime
import dateutil
import logging
from logging.handlers import RotatingFileHandler
import utils.data_cleansing as dc                                            # TODO_1: 確認自己富邦utils的functions要放在哪裡，決定TODO_1位置
import config_tbl as config                                                  # TODO_1: 確認自己富邦utils的functions要放在哪裡，決定TODO_1位置
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
logger = build_logger('./Log/P&B_validation.log')                            # TODO_2: 在TODO_1的directory下新增Log資料夾
logger.info(f"{'*' * 24} Job of P&B Data Cleansing Started {'*' * 24}")
logger.info(f'Original absolute path: {abspath}')
logger.info(f'Current absolute path: {abspath}')
logger.info(f"{'='*70}")

###########################
#### 啟用Impala logger ####
###########################
# Initiate logger
logger = get_logger('test')
            
#########################
###Start Spark Session###
#########################
spark = build_spark_session(method = 'normal_gpu')                           # TODO_3: 建議自行設定spark session

######################
####指定OS路徑至sql####
######################
import os
# 將工作路徑改到此 script 的路徑
abspath = os.getcwd()
print(f'Original absolute path: {abspath}')
print('Change working directory...')
os.chdir(r'/home/cdsw/sql/JOB_SQL')                                          # TODO_4: 因為之後要使用sql，所以須將目錄改到相應的sql/JOB_SQL資料夾位置
abspath = os.getcwd()
print(f'Current absolute path: {abspath}')

#################
####讀取參數檔####
#################
# 建立參數                                                                    # TODO_5: 請修改config_tbl.py中VIEW及表名
HIVE_VIEW = config.HIVE_VIEW
ORACLE_VIEW = config.ORACLE_VIEW
BANCS_CUST_ACCT_EXP = config.BANCS_CUST_ACCT_EXP                             # 核心客戶檔名
SECURITY = config.SECURITY                                                   # 證券戶檔名

# 薪轉戶來源表
SAV_TXN_VIEW = config.SAV_TXN_VIEW
SAV_TXN = config.SAV_TXN                                                     # 活存交易檔名
WMG_CUST_VIEW = config.WMG_CUST_VIEW
WMG_CUST = config.WMG_CUST                                                   # 活存客戶基本資料檔名
print(f'HIVE_VIEW: {HIVE_VIEW}')
print(f'ORACLE_VIEW: {ORACLE_VIEW}')


##############################################
########## 修改薪轉戶VIEW、TABLE NAME ##########
##############################################
tbl_name = 'job_salary_acct'
# open file
with open(f'POLICE_DASB_{tbl_name}.sql',"r") as f:
    string = f.read().replace('SAV_TXN_VIEW', str(SAV_TXN_VIEW))         
# write file
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"w") as f:
    f.write(string)

with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"r") as f:
    string = f.read().replace('WMG_CUST_VIEW',str(WMG_CUST_VIEW))          
# write file
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"w") as f:
    f.write(string)

with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"r") as f:
    string = f.read().replace('SAV_TXN',str(SAV_TXN))          
# write file
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"w") as f:
    f.write(string)

with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"r") as f:
    string = f.read().replace('WMG_CUST',str(WMG_CUST))          
# write file
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"w") as f:
    f.write(string)
            
#############################
#### 由impala撈取薪轉戶帳號 ##
#############################
try:
    #建立薪轉帳號資料
    tbl_name = 'job_salary_acct'
    #Create impala table
    impala_sql(f'POLICE_DASB_{tbl_name}_tmp_file.sql', logger, mode='overwrite', table_name = f'{HIVE_VIEW}.POLICE_DASB_{tbl_name}') 
    time.sleep(10)
    print('Salary Acct Table Creation done!')
except Exception:
    logging.exception('message')
    print('Salary Acct Table Creation failed.')

            
#################
###取得psid帳密###
#################
# 為了下方ReadFromPSID函式input所需
psid_info = spark.sql('select * from USR_XXX_XXX.psid_info').toPandas()
usr = psid_info['usr'][0]
pwd = psid_info['pwd'][0]


########################
###建立PSID撈資料之函式###
########################        
def ReadFromPSID(sQueryString, user, pwd, spark):
    # TODO_6: 如資料在ORACLE，需修改ORACLE與Hadoop連線方式(視各家銀行系統而訂)
    return aDataFrame


#################
### 核心客戶檔 ###
################
query1 = f'''
(
SELECT ACCT_NO_SAV, DERIVATIVE_DATE, DERIVATIVE_FLG, SWINDLE_MORATORIUM_DATE, SWINDLE_MORATORIUM_FLG, SWINDLE_FLG, ALERT_DATE, ALERT_FLG
FROM {ORACLE_VIEW}.{BANCS_CUST_ACCT_EXP}                                   
WHERE DERIVATIVE_FLG is not null
OR SWINDLE_MORATORIUM_FLG is not null
OR SWINDLE_FLG is not null
OR ALERT_FLG is not null
) cctlog
'''
  
spark_df = ReadFromPSID(query1, usr, pwd, spark)
  
# 檢查資料是否有誤
spark_df.show(2)

# 寫進hadoop hive
spark_df.write.mode('overwrite').saveAsTable(f'{HIVE_VIEW}.BANCS_CUST_ACCT_EXP')


#########################
## 核心客戶檔 to impala ##
#########################
# invalidate_metadata
impala_sql('', logger, mode = 'invalidate_metadata', table_name = f'{HIVE_VIEW}.BANCS_CUST_ACCT_EXP')     # TODO_7: 修改核心客戶檔存放位置名稱[HIVE_VIEW]，將資料從Hive拉到Impala
           

#################
### 撈取證券戶 ###
#################
# TODO_8: 修改證券客戶XX: 證券商及保代號

query2 = f'''
(select distinct DEP_ACCT_NO 
from {ORACLE_VIEW}.{SECURITY}
where (substr(STCK_CSTDY_CD,1,2) = 'XX' 
OR STCK_CSTDY_CD is null 
OR STCK_CSTDY_CD = 'XX' 
OR length(STCK_CSTDY_CD) < 4))cctlog
'''
ashf = ReadFromPSID(query2, usr, pwd, spark)
ashf.write.format('Hive').mode('overwrite').saveAsTable(f'{HIVE_VIEW}.POLICE_DASB_ashf_list')


#######################
## 證券戶檔 to impala ##
#######################
# invalidate_metadata
impala_sql('', logger, mode = 'invalidate_metadata', table_name = f'{HIVE_VIEW}.POLICE_DASB_ashf_list') 

#################################
########## 設定時間區間 ##########
#################################
today = datetime.today().date()
max_date = today.strftime('%Y-%m-%d')
print(max_date)
min_date = (today - dateutil.relativedelta.relativedelta(months = 1)).strftime('%Y-%m-%d')
print(min_date)

###################################
########## 修改sql時間區間 ##########
###################################
tbl_name = 'job_cust_predict'
# open file
with open(f'POLICE_DASB_{tbl_name}.sql',"r") as f:
    string = f.read().replace('SNAP_DATE',str(max_date))
            
# write as tmp.file
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"w") as f:
    f.write(string)
  
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"r") as f:
    string = f.read().replace('HIVE_VIEW',str(HIVE_VIEW))
            
# write as tmp.file
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"w") as f:
    f.write(string)

with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"r") as f:
    string = f.read().replace('WMG_CUST',str(WMG_CUST))
            
# write as tmp.file
with open(f'POLICE_DASB_{tbl_name}_tmp_file.sql',"w") as f:
    f.write(string)
            
#####################################
#### 由impala撈取排除條件之預測資料 ####
#####################################
try:
    #建立預測帳號資料
    tbl_name = 'job_cust_predict'
    # Create impala table
    impala_sql(f'POLICE_DASB_{tbl_name}_tmp_file.sql', logger, mode='overwrite', table_name = f'{HIVE_VIEW}.POLICE_DASB_{tbl_name}_tmp')
    time.sleep(10)
    print('Normal_acct Table Creation done!')
except Exception:
    logging.exception('message')
    print('Normal_acct Table Creation failed.')
            
#########################
######## 交易清理 ########
#########################
try:
    #add logging
    logger.info(f"{'=' * 20} Normal Acct Transaction Cleansing started {'=' * 20}")
    start = time.time()
    nonwarning = spark.sql(query02)
    nonwarning = nonwarning.toPandas()
    data = nonwarning
    data = fe.acct_nbr_ori_digits(data, 'acct_nbr_ori')
    data['drcr'] = data['drcr'].astype(int)
    data['pb_bal'] = data['pb_bal'].astype(float)
    data['tx_amt'] = data['tx_amt'].astype(float)
    data['tx_date'] = data['tx_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    data['tx_time'] = data['tx_time'].apply(lambda x: datetime.utcfromtimestamp(int(x)).strftime('%H:%M:%S'))
    data['tx_time'] = data['tx_date'].astype(str) + " " + data['tx_time'].astype(str)
    data = data[data['tx_mode'] == '1']
    data = dc.channel_memo_cleansing_func(data)
    print('Memo Cleansing Done!')
    data = dc.exchange_cleansing_func(data) #排除換匯交易
    print('Exchange Cleansing Done!')
    data = dc.filter_cur(data) #排除其他純外幣交易
    #====================================
    nonwarning = data
    nonwarning.to_csv('nonwarning.csv', index = False)         # TODO_9: 需修改Parquet存放位置
    print('Normal Acct Transaction Cleansing Done!')
    end = time.time()
    print(f'time consumed: {(end-start)/60} minutes')
    logger.info(f'time consumed: {(end-start)/60} minutes')
    logger.info('Nonwarning Acct Transaction cleansing done.')
    logger.info(f'{"="*70}')
except Exception:
    logging.exception('message')
    print('Nonwarning Acct Transaction cleansing failed.')
    logging.error('Nonwarning Acct Transaction cleansing failed.')
    logger.info(f'{"="*70}')

################################
#### 設定一般腳本logger的規則 ####
################################
logger.info(f"{'*' * 24} Job of P&B Data Cleansing Done {'*' * 24}")

############################
## Turn off Spark Session ##
############################
spark.stop()
  
  
