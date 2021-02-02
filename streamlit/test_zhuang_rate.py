# coding:utf-8
#指定日期，个股前45天均值与后60天中极大值比值，大于x倍数数量占比
import mpl_finance
# import tushare as ts
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pymysql
from matplotlib import ticker
from matplotlib.pylab import date2num
import numpy as np
import datetime
import logging
import re
from multiprocessing import Pool
import json

logging.basicConfig(level=logging.DEBUG, filename='comp_zhaung_210114.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
all_count=up_12=up_13=up_15=0
def get_df_from_db(sql, db):
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()
    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    # df = df.set_index(keys=['trade_date'])
    df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
    df.reset_index(inplace=True)
    df = df.fillna(0)
    # df = df.dropna(axis=0, how='any')
    # df.reset_index(inplace=True)
    df['trade_date2'] = df['trade_date'].copy()
    # #print('trade_date2:',type(df['trade_date2'][0]))
    df['trade_date2'] = pd.to_datetime(df['trade_date2']).map(date2num)
    df['dates'] = np.arange(0, len(df))
    df['arv_10'] = df['close_price'].rolling(10).mean()
    df['arv_5'] = df['close_price'].rolling(5).mean()
    # df['increase'] = df['increase'].astype('float')
    # df.loc[-1.5<float(df['increase'])<1.5,'increase_flag'] = 1 #increase 是str
    df['increase_flag'] = 0
    df['increase_abs'] = 0
    for i in range(1,len(df)-1):
        #涨幅绝对值
        df.loc[i, 'increase_abs'] = abs(float(df.loc[i, 'increase']))
        #DB中历史老数据缺失increase
        df.loc[i, 'increase'] = (df.loc[i,'close_price']-df.loc[i-1,'close_price']) / df.loc[i-1,'close_price']*100
        if -2 <= float(df.loc[i,'increase']) <=2:
            df.loc[i, 'increase_flag'] = 1
    cursor.close()
    # #print(df)
    # df['trade_date'] = date2num(df['trade_date'])
    # #print('df:', df[['increase','increase_flag']])
    return df
def com_core(df,date,piece = 45):
    global all_count,up_12,up_13,up_15
    index_list = df.query("trade_date == '{}'".format(date))
    if len(index_list) == 0:
        return
    index = index_list.index[0]
    print('index:',index)
    if index - piece >0 and index + 60 < len(df):
        mean_value = df['close_price'][(index - piece) : index].mean()
        max_value = df['close_price'][index:(index + 60)].max()
        rate = max_value*10//mean_value
        #print('比值：',max_value,mean_value,rate)
        all_count += 1
        if rate >=15:
            up_15 += 1
        elif rate >= 13:
            up_13 += 1
        elif rate >= 12:
            up_12 +=1

def main(h_tab, start_t, end_t,date):
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    sql = "select distinct  stock_id,stock_name from stock_history_trade{0}".format(h_tab)
    #临时补漏
    # sql = "select distinct  h.stock_id,h.stock_name from stock_history_trade{0} h " \
    #       "right join com_zhuang c " \
    #       "on h.stock_id = c.stock_id " \
    #       "where c.zhuang_grade / 10000000 < 10 and c.zhuang_grade / 10000000 >= 1".format(h_tab)
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    # stock_id_list = [('600121','郑州煤电'),] #测试数据 h_tab = 3
    # stock_id_list = [('600165', '新日恒力'), ] #h_tab = 1
    # stock_id_list = [('603967', '中创物流'), ] #h_tab = 2
    # stock_id_list = [('002889', '东方嘉盛'), ] #h_tab = 6
    # stock_id_list = [('002958', '青农商行'), ]  # h_tab = 6
    # stock_id_list = [('002221', '东华能源'), ]  # h_tab = 8
    # stock_id_list = [('603331', '百达精工'), ]  # h_tab = 3
    # stock_id_list = [('000937', '冀中能源'), ]  # h_tab = 9
    for ids_tuple in stock_id_list:
        # zhuang_grade = 1
        # zhuang_json = {}
        ids = ids_tuple[0]
        if start_t != None and end_t != None:
            sql = "SELECT stock_id,trade_date,open_price,close_price,high_price,low_price,increase  FROM stock_history_trade{0} \
                    where trade_date >= '{1}' and trade_date <= '{2}' and  stock_id  = '{3}'".format(h_tab, start_t, end_t,ids)
        else:
            sql = "SELECT stock_id,trade_date,open_price,close_price,high_price,low_price,increase  FROM stock_history_trade{0} \
                    where stock_id  = '{1}'".format(h_tab,ids)
        df = get_df_from_db(sql, db)
        # #print('flag1')
        com_core(df,date)
def main_run( start_t, end_t,date):
    global all_count, up_12, up_13, up_15
    for i in range(1,11):
        main(str(i), start_t, end_t, date)
    print('15:',up_15,all_count,up_15/all_count)
    print('13:', up_13,all_count,up_13 / all_count)
    print('12:', up_12,all_count,up_12 / all_count)
def run(start_t, end_t,date_list):
    p = Pool(8)
    for date in date_list:
        p.apply_async(main_run, args=( start_t,end_t,date,))
    #    p.apply_async(main, args=('1',date,))
    #print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    #print('All subprocesses done.')
if __name__ == '__main__':
    start_t = None#'2020-01-01'
    end_t = None#'2021-01-14'

    # h_tab = 3
    # date = '2020-03-16'
    # main_run( start_t, end_t,date)
    print('start')
    date_list = ['2018-04-14','2018-08-14','2018-12-12','2019-04-15','2019-08-16','2020-04-16','2020-08-18','2020-11-01']
    run(start_t, end_t,date_list)
    print('end')