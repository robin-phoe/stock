#coding:utf-8
import mpl_finance
#import tushare as ts
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


logging.basicConfig(level=logging.DEBUG,filename='find_zhuang.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
logging.info('test')
with open('find_zhuang.log') as f:
    f.read()
    print(f)

pd.set_option('display.max_rows',1000)
sns.set()
#pro = ts.pro_api()

def select_info(ids,db):
    cursor = db.cursor()
    sql="select h_table from stock_informations where stock_id={}".format(ids)
    cursor.execute(sql)
    h_table_tuple = cursor.fetchall()
    # print('h_table_tuple:',h_table_tuple)
    #print(stock_id_list)
    cursor.close()
    return h_table_tuple[0][0]
def get_df_from_db(sql, db):
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()
    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    #df = df.set_index(keys=['trade_date'])
    df = df.sort_values(axis=0, ascending=True, by='trade_date', na_position='last')
    df.reset_index(inplace=True)
    #df = df.dropna(axis=0, how='any')
    #df.reset_index(inplace=True)
    df['trade_date2'] = df['trade_date'].copy()
    # print('trade_date2:',type(df['trade_date2'][0]))
    df['trade_date'] = pd.to_datetime(df['trade_date']).map(date2num)
    df['dates'] = np.arange(0, len(df))
    cursor.close()
    #print(df)
    # df['trade_date'] = date2num(df['trade_date'])
    return df

def compute_junxiancha(df):
    df_cha = df
    df_cha['5'] = df_cha['close_price'].rolling(5).mean()
    # df_cha['var_30'] = df_cha['close_price'].rolling(30).var()
    df_cha['30_m'] = df_cha['close_price'].rolling(30).mean()
    df_cha['up_down'] = abs(df_cha['close_price'] / df_cha['30_m'] - 1)
    df_cha['ratio_30'] = df_cha['up_down'].rolling(30).sum()
    df_cha['5_x'] = df_cha['5'].shift(1)
    df_cha['cha'] = (df_cha['5'] / df_cha['5_x'])**2 * 10
    df_cha.fillna(0,inplace = True)
    # df_cha.to_excel('df_cha.xlsx')
    # df_cha['trade_date2'] = df_cha.trade_date2.transform(parse).copy()
    # print("trade_date type:",df_cha['trade_date'][0])
    # df_cha = df.set_index(keys=['trade_date2'])
    # df_cha = df_cha[['5','cha']]
    
    # print('df_cha:',df_cha)
    return df_cha
def compute_junxiancha2(df,ids,db):
    df_cha = df
    df_cha['5'] = df_cha['close_price'].rolling(5).mean()
    # df_cha['var_30'] = df_cha['close_price'].rolling(30).var()
    df_cha['30_m'] = df_cha['close_price'].rolling(30).mean()
    df_cha['up_down'] = abs(df_cha['close_price'] / df_cha['30_m'] - 1)
    df_cha['ratio_30'] = df_cha['up_down'].rolling(30).sum()
    df_cha['5_x'] = df_cha['5'].shift(1)
    df_cha['cha'] = (df_cha['5'] / df_cha['5_x'])**2 * 10
    df_cha.fillna(0,inplace = True)
    cursor = db.cursor()
    df_cha.to_excel('df_cha.xlsx')
    # val = []
    # for i  in range(len(df_cha)):
    #     trade_code = df_cha.loc[i,'trade_date2'].strftime('%Y%m%d') + ids
    #     # print('df_cha:',df_cha.loc[i,'trade_code'])
    #     ratio_30 = df_cha.loc[i,'ratio_30']
    #     val.append((trade_code,float(ratio_30)))
    # # try:
    # #     sql = "replace into compute_result(trade_code,ratio_30) \
    # #         values(%s,%s)\
    # #         "
    # #         # .format(tuple(df_cha['trade_code']), df_cha['ratio_30'])
    # #     # print('sql:', sql)
    # #     cursor.executemany(sql,val)
    # #     db.commit()
    # #     print('存储完成')
    # #     logging.info('存储完成:id:{}'.format(ids))
    # # except Exception as err:
    # #     db.rollback()
    # #     print('存储失败:', err)
    # #     logging.error
    return df_cha
def comput_zero(num):
    if num >= 10:
        return 1
    else:
        return -1
def save(db,ids,stock_name,zhuang_grade,trade_code,end_t,zhuang_json_s,ratio_30):
    cursor = db.cursor()
    try:
        print('zhuang_grade:',zhuang_grade)

        sql="insert into compute_result(trade_code,trade_date,stock_id,stock_name,zhuang_grade,zhuang_json,ratio_30) \
            values('{0}','{1}','{2}','{3}','{4}','{5}','{6}') " \
            "ON DUPLICATE KEY UPDATE trade_code='{0}',trade_date='{1}',stock_id='{2}',stock_name='{3}'," \
            "zhuang_grade='{4}',zhuang_json='{5}',ratio_30 = '{6}'\
            ".format(trade_code,end_t,ids,stock_name,zhuang_grade,zhuang_json_s,ratio_30)
        print('sql:',sql)
        cursor.execute(sql)
        db.commit()
        print('存储完成')
        logging.info('存储完成:id:{},name:{}'.format(ids,stock_name))
    except Exception as err:
        db.rollback()
        print('存储失败:',err)
        logging.error('存储失败:id:{},name:{}\n{}'.format(ids,stock_name,err))
    cursor.close()
def compute_zhuang(df_cha,ids):
    df_t_list = df_cha['trade_date2']
    # def comp_z_t(num):
    #     zero_time = datetime.datetime.strptime('2020-01-01','%Y-%m-%d')
    #     print('num:',num,"df_cha.loc[num,'trade_date2']:",df_cha.loc[num,'trade_date2'])
    #     return (df_cha.loc[num,'trade_date2'] - zero_time).days
    df_cha = df_cha.reset_index()
    zero_i = len(df_cha)-1
    # print('zero_i:',zero_i)
    flat_tuple = []
    flat_list = []
    not_flat = 0
    up_list = []
    zhuang_json = {}
    # print("df_cha['cha']", df_cha[['cha', 'trade_date2']])
    for i in range(len(df_cha)-1,0 ,-1):
        # print('flag 2', df_cha.loc[i, 'trade_date2'])
        #计算1涨区间
        if comput_zero(df_cha.loc[i,'cha'])+comput_zero(df_cha.loc[i-1,'cha']) == 0:
            # print('flag 1',i,df_cha.loc[i,'trade_date2'])
            if  zero_i - i >= 7 and 0.011 <= (df_cha.loc[zero_i,'close_price'] - df_cha.loc[i,'close_price'])/(zero_i - i) <= 0.02:
                # print('flag 1', i, df_cha.loc[i, 'trade_date2'])
                up_list.append((zero_i,i))
            zero_i = i
        #计算平缓区间
        if 9.85 <= df_cha.loc[i,'cha'] <= 10.15:
            flat_tuple.append(i)
            not_flat = 0
        elif len(flat_tuple) != 0 :
            if not_flat < 2:
                flat_tuple.append(i)
                not_flat += 1
            else:
                if len(flat_tuple) >=5:
                    flat_list.append(tuple(flat_tuple))
                flat_tuple = []
                not_flat =0
    # print('flat_list:',flat_list,'\nup_list:',up_list)
    zhuang_grade = 0
    if len(up_list) > 0:
        flat_list.sort(key=None, reverse=True)
        up_list.sort(key=None, reverse=True)
        zhuang_json["up_list"] = [str(df_t_list[up_list[0][-1]])[0:10], str(df_t_list[up_list[0][0]])[0:10]]
        logging.info('ids:{},flat_list:{},\n up_list:{}'.format(ids,flat_list,up_list))
        #过滤过时结果
        # if flat_list[0] > up_list[0]:
            # logging.info('len_cha:{0},flat_list[0][0]:{1}'.format(len(df_cha),flat_list[0][0]))
            # if len(df_cha) - flat_list[0][0] > 3:
            #     zhuang_grade = 0
            #     return zhuang_grade
            # else:
            #     if len(df_cha) - up_list[0][0] > 5:
            #         zhuang_grade = 0
            #         return zhuang_grade
        flat_two = 0
        flat_one = 0
        flat_two_count = 0
        for tup in flat_list:
            if tup > up_list[0]:
                flat_two_count += 1
                zhuang_json["two_flat"] = [str(df_t_list[tup[-1]])[0:10],str(df_t_list[tup[0]])[0:10]]
            elif tup < up_list[0] and len(tup) > 20:
                df_cha_clo = df_cha["close_price"][tup[0]: up_list[0][0]]
                ratio = df_cha_clo.max() / df_cha_clo.min()
                if ratio <= 1.2:
                    flat_one = 4000
                    zhuang_json["one_flat"] = [str(df_t_list[tup[-1]])[0:10],str(df_t_list[tup[0]])[0:10]]
                    if len(tup) >= 30:
                        pass
                    break
        if flat_two_count <= 2:
            flat_two = 1000
        zhuang_grade = flat_one + 2000 + flat_two
        #达标超值
        if zhuang_grade >= 7000:
            # up_two = df_cha.loc[len(df_cha)-1,'close_price'] / df_cha.loc[up_list[0][-1],'close_price']
            df_cha_clo = df_cha['close_price'][len(df_cha)-1 : flat_list[0][0]]
            up_two = df_cha_clo.max() / df_cha_clo.min()
            if up_two >= 1.7:
                zhuang_grade += 4000
            elif up_two >= 1.5:
                zhuang_grade += 2000
            elif up_two >= 1.3:
                zhuang_grade += 1000
        # print('zhuang_grade:',zhuang_grade)
    zhuang_json_s = json.dumps(zhuang_json)
    # print('ratio_30:',df_cha['ratio_30'])
    ratio_30 = df_cha['ratio_30'][len(df_cha)-1]
    return zhuang_grade,zhuang_json_s,ratio_30


def get_his_time(db):
    cursor = db.cursor()
    sql = "select distinct(trade_date) from stock_history_trade1"
    cursor.execute(sql)
    date_list = cursor.fetchall()
    cursor.close()
    return date_list
def main(h_tab,start_t,end_t):
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    cursor = db.cursor()#使用cursor()方法获取用于执行SQL语句的游标
    sql = "select distinct  stock_id,stock_name from stock_history_trade{0}".format(h_tab)
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    # stock_id_list = [('600165','新日恒力'),]
    for ids_tuple in stock_id_list:
        ids = ids_tuple[0]
        sql = "SELECT trade_date,open_price,close_price,high_price,low_price  FROM stock_history_trade{0} \
                where trade_date >= '{1}' and trade_date <= '{2}' and  stock_id  = '{3}'".format(h_tab,start_t,end_t,ids)
        df = get_df_from_db(sql, db)
        df_cha = compute_junxiancha(df)
        zhuang_grade,zhuang_json_s,ratio_30 = compute_zhuang(df_cha,ids)
        date_str = re.sub('-', '', end_t[0:10])
        trade_code = date_str + ids
        save(db, ids, ids_tuple[1], zhuang_grade, trade_code,end_t,zhuang_json_s,ratio_30)
        # compute_junxiancha2(df, ids, db)
    cursor.close()


if __name__ == '__main__':

##########
    start_t = '2020-02-01'
    end_t = '2020-10-22 17:00:00'
    # db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    # h_tab = '1'
    # main(h_tab,start_t,end_t)

    p = Pool(8)
    for i in range(1,11):
        p.apply_async(main, args=(str(i),start_t,end_t,))
##    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
##########################
    # h_tab = '1'
    # db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    # date_list = list(get_his_time(db))
    # date_list.sort(reverse=True)
    # # print(date_list)
    # p = Pool(6)
    # for i in range(len(date_list) - 60):
    #     start_t = date_list[i+60][0].strftime("%Y-%m-%d %H:%M:%S")#'2020-06-01'
    #     end_t = date_list[i][0].strftime("%Y-%m-%d %H:%M:%S")#'2020-09-08 17:00:00'
    #     # start_t = '2020-06-01'
    #     # end_t = '2020-09-08 17:00:00'
    #     print(start_t,end_t)
    #     # db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    #     # main(h_tab,start_t,end_t)
    #
    #
    #     for i in range(1, 11):
    #         h_tab = str(i)
    #         p.apply_async(main, args=(h_tab, start_t, end_t,))
    #     ##    p.apply_async(main, args=('1',date,))
    # print('Waiting for all subprocesses done...')
    # p.close()
    # p.join()
    # print('All subprocesses done.')
