import os
# 新增環境變數(一定要加的)
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

# load data from Hive or PSID
from pyspark.sql import SparkSession, HiveContext
from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.sql.types import StringType, StructField, IntegerType, FloatType, BooleanType, LongType, DateType
from Job.config import psid_info
from pyspark.sql.functions import col, countDistinct, when, round, coalesce, monotonically_increasing_id, date_format, row_number, to_timestamp, concat, unix_timestamp, lit,collect_list, to_date

# relative to spark
import pyspark.sql.functions as pys
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# time utils
import time
from datetime import date, datetime, timedelta
import dateutil.relativedelta

# Dataframe
import pandas as pd
import numpy as np
import pickle

# relative to training
import sklearn
import xgboost as xgb
from sklearn.model_selection import cross_val_score, cross_validate, GridSearchCV, KFold, RandomizedSearchCV, train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score,f1_score,confusion_matrix,make_scorer, accuracy_score, precision_score, recall_score

# other packages
import json
import pprint

# 透過 pprint 設定 print 的格式
pp = pprint.PrettyPrinter(width=120, compact=True)

################################
######## 警示戶交易清理 ########
###############################
def salary_list_cleansing(data: F.DataFrame, spark: object):
    """
    功能: 清除薪轉戶
    input: 警示戶交易資料Spark Dataframe、Spark Session Object
    output: 警示戶交易資料Spark Dataframe
    """
    print('salary_list_cleansing')
    print('='*70)
#     querySalary = '''
#     SELECT distinct acct_no_14, payroll_code
#     FROM ods_t_view.invm 
#     WHERE NULLIF(TRIM(payroll_code), '') IS NOT NULL
#     '''
    querySalary = '''
    SELECT *
    FROM usr_julian_liu.POLICE_DASB_salary_acct
    '''
    salary_list = spark.sql(querySalary)
    data = data.join(salary_list, data.acct_nbr_ori == salary_list.acct_no_14, how='left')
    data = data.filter(data.payroll_code.isNull()).drop('acct_no_14', 'payroll_code') 
    return data

def channel_memo_cleansing_func(data: F.DataFrame):
    """
    功能: 限定交易通路/memo/排除字串備註，請自行刪減/補充
    input: 警示戶交易資料Spark Dataframe
    output: 警示戶交易資料Spark Dataframe
    """
    print('channel_memo cleansing')
    print('='*70)
    channel_desc = ['銀行資訊交換平台FEP跨行ATM','網路銀行 行銀','銀行資訊交換平台FEP匯款',
                '銀行資訊交換平台FEP自行ATM','新端末','網路銀行 網銀','一卡通']
    tx_list = \
    ['ＣＤ轉收','ＣＤ提款','行動跨轉','匯入款','行動自轉','網路跨轉','ＣＤ轉支','跨行存款',
    'ＣＤ存現','領現','轉支','提領','主動儲值','轉收','ＣＤ轉帳','儲值',
    '跨國提款','網路自轉','行動換匯','無卡提款','付款','國外匯入','自動儲值','國外匯出']
    
    remk_remove=['信用卡','基金','學貸','房','房貸']                 #請自行刪減/補充
    data = data[data['channel_desc'].isin(channel_desc)]
    data = data[data['memo'].isin(tx_list)]
    data = data[~data['remk'].isin(remk_remove)]
    return data

def exchange_cleansing_func(data):
    """
    功能: 排除換匯交易
    input: 警示戶交易資料Dataframe
    output: 警示戶交易資料
    Dataframe
    """
    print('exchange_cleansing')
    print('='*70)
    data['exchange_remove_index'] = range(1, len(data) + 1)
    tx_time_dupli = data.groupby(['acct_nbr_ori','tx_time']).size().reset_index(name = 'count')
    tx_time_dupli = tx_time_dupli[tx_time_dupli['count']>1]
    tx_count = data.merge(tx_time_dupli, on = ['acct_nbr_ori','tx_time'])
    cur_exchange = tx_count.groupby(['acct_nbr_ori','tx_time','jrnl_no']).agg({'cur': 'unique'}).rename(columns={'cur':'count_cur'}).reset_index()
    cur_exchange = cur_exchange[cur_exchange['count_cur'].apply(lambda x: len(x)>1)]
    drcr_exchange = tx_count.groupby(['acct_nbr_ori','tx_time','jrnl_no']).agg({'drcr': 'unique'}).rename(columns={'drcr':'count_drcr'}).reset_index()
    drcr_exchange = drcr_exchange[drcr_exchange['count_drcr'].apply(lambda x: len(x)>1)]
    exchange = cur_exchange.merge(drcr_exchange, on=['acct_nbr_ori','tx_time','jrnl_no'])
    tx_count = tx_count.merge(exchange, on=['acct_nbr_ori','tx_time','jrnl_no'])
    tx_count = tx_count['exchange_remove_index'].reset_index()
    tx_count['idx'] = 1
    data = data.merge(tx_count, on=['exchange_remove_index'], how='left')
    data = data[data['idx'].isnull()].drop(['exchange_remove_index','idx', 'index'], axis=1)  
    return data

def filter_cur(data: F.DataFrame):
    """
    功能: 排除外幣純轉入或轉出
    input: 警示戶交易資料Dataframe
    output: 警示戶交易資料Dataframe
    """
    print('foreign currency cleansing')
    print('='*70)
    data = data[data['cur']=='TWD']
    return data