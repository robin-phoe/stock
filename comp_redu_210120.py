# coding:utf-8
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
import openpyxl

logging.basicConfig(level=logging.DEBUG, filename='comp_redu_210120.py.log', filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

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
    # df = df.dropna(axis=0, how='any')
    # df.reset_index(inplace=True)
    df['trade_date2'] = df['trade_date'].copy()
    # print('trade_date2:',type(df['trade_date2'][0]))
    df['trade_date2'] = pd.to_datetime(df['trade_date2']).map(date2num)
    df['dates'] = np.arange(0, len(df))
    df['avg_10'] = df['close_price'].rolling(10).mean()
    df['avg_5'] = df['close_price'].rolling(5).mean()
    cursor.close()
    # print(df)
    # df['trade_date'] = date2num(df['trade_date'])
    print('df:', df[['avg_10', 'close_price']])
    return df
#计算龙虎榜热度
def com_redu1(db,date,delta = 30):
    cursor = db.cursor()
    end_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    start_date = (end_date - datetime.timedelta(days=delta)).strftime("%Y-%m-%d")
    # sql = "select sum(30+jmrate)+count(jmrate)*1000 from longhu_info where stock_code = '{0}' and " \
    #       "trade_date >= '{1}' and trade_date <= '{2}'".format(s_code,start_date,date)
    sql = "select stock_code,sum(30+jmrate)+count(jmrate)*10000 from longhu_info where  " \
          "trade_date >= '{0}' and trade_date <= '{1}' group by stock_code".format(start_date,date)
    # print('sql:',sql)
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()
    cursor.close()
    #print('data:',data)
    return dict(data)
#计算涨停热度
def com_redu2(db,ids,h_tab,date,delta = 30):
    cursor = db.cursor()
    end_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    start_date = (end_date - datetime.timedelta(days=delta)).strftime("%Y-%m-%d")
    # sql = "select sum(30+jmrate)+count(jmrate)*1000 from longhu_info where stock_code = '{0}' and " \
    #       "trade_date >= '{1}' and trade_date <= '{2}'".format(s_code,start_date,date)
    sql = "SELECT count(increase)  FROM stock_history_trade{0} " \
          "where trade_date >= '{1}' and trade_date <= '{2}' " \
          "and  stock_id  = '{3}' and increase >= 9.75".format(h_tab, start_date, date,ids)
    # print('sql:',sql)
    cursor.execute(sql)  # 执行SQL语句
    zhangting_res = cursor.fetchall()
    if len(zhangting_res) !=0:
        zhangting_count = zhangting_res[0][0]*10000
    else:
        zhangting_count = 0
    cursor.close()
    print('zhangting_count:',zhangting_count)
    return zhangting_count
def com_redu_init(redu,df):
    if redu<10000:
        return 0
    max_value = df['close_price'][len(df)-11 : len(df) - 1].max()
    print('max_value:',max_value)
    index = df.query("close_price == {}".format(max_value)).index[-1]
    for i in range(index,len(df)):
        if df.loc[i,'close_price'] <=  df.loc[i,'avg_5']:
            return 0
    return 1
#判断前溯极大值是10日内最大值
def find_max(df):
    len_df = len(df)
    # increase 未统一，暂不使用
    if df.loc[len_df-1,'increase'] <= -7:
        return False
    val = 0
    for i in range(len_df-1,len_df-11,-1):
        val = df.loc[i,'close_price']
        print('val:',val,df.loc[i-1,'close_price'])
        if val > df.loc[i-1,'close_price']:
            break
    print('max_val:',df['close_price'][len_df-11:len_df-1].max(),val)
    if df['close_price'][len_df-11:len_df-1].max() > val:
        return False
    else:
        return True
def main(h_tab,date):
    if date == None:
        date = datetime.datetime.now().strftime('%Y-%m-%d')
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    sql = "select distinct  stock_id,stock_name from stock_history_trade{0}".format(h_tab)
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    date_time = datetime.datetime.strptime(date, '%Y-%m-%d')
    start_t = (date_time - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
    for ids_tuple in stock_id_list:
        ids = ids_tuple[0]
        stock_name = ids_tuple[1]
        trade_code = re.sub('-', '', date[0:10]) + ids
        redu_dict = com_redu1(db, date, delta=10)
        redu_grade = com_redu2(db, ids, h_tab, date, delta=10)
        sql = "SELECT stock_id,trade_date,open_price,close_price,high_price,low_price,increase  FROM stock_history_trade{0} \
                where trade_date >= '{1}' and trade_date <= '{2}' and  stock_id  = '{3}'".format(h_tab, start_t, date,
                                                                                                 ids)
        df = get_df_from_db(sql, db)
        if len(df) < 20:
            continue
        avg_5 = df.loc[len(df) - 1, 'avg_5']
        if ids in redu_dict:
            redu_grade += redu_dict[ids]
        if redu_grade == 0:
            continue
        if df.loc[len(df) - 3, 'avg_5'] > df.loc[len(df) - 3, 'close_price'] and df.loc[len(df) - 2, 'avg_5'] > df.loc[len(df) - 2, 'close_price'] and df.loc[len(df) - 1, 'avg_5'] > df.loc[len(df) - 1, 'close_price']:
            redu_grade = redu_grade /10000
        redu_5 = 0
        if redu_grade >=10000 and df.loc[len(df) - 1, 'avg_5'] >= df.loc[len(df) - 1,'close_price']:
            if find_max(df):#return bool
                redu_5 = redu_grade
        print('redu_grade:',redu_grade,redu_5)
        redu_init = com_redu_init(redu_grade, df)
        sql = "insert into com_redu(trade_code,trade_date,stock_id,stock_name,redu,redu_5,avg_5,h_table,redu_init) \
            values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}') " \
              "ON DUPLICATE KEY UPDATE trade_code='{0}',trade_date='{1}',stock_id='{2}',stock_name='{3}'," \
              "redu ='{4}',redu_5 ='{5}',avg_5='{6}',h_table = '{7}',redu_init = '{8}'\
            ".format(trade_code, date, ids, stock_name, redu_grade, redu_5, float(avg_5), h_tab, redu_init)
        print('sql:', sql)
        cursor.execute(sql)
    try:
        db.commit()
        print('存储完成')
        logging.info('存储完成:id:{},name:{}'.format(ids, stock_name))
    except Exception as err:
        db.rollback()
        print('存储失败:', err)
        logging.error('存储失败:id:{},name:{}\n{}'.format(ids, stock_name, err))
    cursor.close()
def run(date):
    p = Pool(8)
    for i in range(1, 11):
        p.apply_async(main, args=(str(i),date,))
    #    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
def history_com(h_tab,start_date,end_date):
    db_h = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    cursor = db_h.cursor()
    sql = "select distinct(trade_date) from stock_history_trade1 where trade_date >= '{0}' and trade_date <= '{1}'".format(start_date,end_date)
    cursor.execute(sql)
    date_list = cursor.fetchall()
    print('date_list:',date_list)
    for date_t in date_list:
        date = date_t[0].strftime('%Y-%m-%d')
        print(date)
        main(h_tab,date)
def run_h(start_date,end_date):
    p = Pool(8)
    for i in range(1, 11):
        p.apply_async(history_com, args=(str(i),start_date,end_date))
    #    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
if __name__ == '__main__':
    date ='2021-02-10' #'2021-01-20'
    run(date)

    # h_tab = '1'
    # main(h_tab, date)

    # history_com(h_tab,start_date='2020-01-01', end_date='2021-01-19')
    # run_h(start_date='2020-07-31', end_date='2021-01-19')