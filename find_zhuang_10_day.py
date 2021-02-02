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

logging.basicConfig(level=logging.DEBUG,filename='find_zhuang_10_day.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
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
    df['trade_date2'] = pd.to_datetime(df['trade_date2']).map(date2num)
    df['dates'] = np.arange(0, len(df))
    df['arv_10'] = df['close_price'].rolling(10).mean()
    df['arv_5'] = df['close_price'].rolling(5).mean()
    cursor.close()
    #print(df)
    # df['trade_date'] = date2num(df['trade_date'])
    print('df:',df[['arv_10','close_price']])
    return df
def com_fantan(df,trade_code):
    fantan = 1
    open_price = df.loc[len(df)-1,'open_price']
    close_price = df.loc[len(df)-1,'close_price']
    arv_5 = df.loc[len(df)-1, 'arv_5']
    print('fantan1:',open_price,close_price,arv_5)
    arv_5_flag = 0
    if open_price <= arv_5 and close_price <= arv_5:
        arv_5_flag = 7000
        #print('flag1:',arv_5_flag)
    elif close_price <= arv_5:
        arv_5_flag = 4000
        #print('flag2:', arv_5_flag)
    if arv_5_flag > 0:
        max_index = 0
        for i in range(len(df) - 1, -2, -1):
            if df.loc[i, 'close_price'] > df.loc[i-1, 'close_price']:
                max_index = i
                break
        for i in range(max_index, -2, -1):
            if df.loc[i, 'close_price'] < df.loc[i-1, 'close_price']:
                min_index = i
                break
        zhangfu = (df.loc[max_index, 'close_price'] - df.loc[min_index, 'close_price'])/df.loc[min_index, 'close_price']
        diefu = (df.loc[max_index, 'close_price'] - df.loc[len(df) - 1, 'close_price']) / df.loc[
            len(df) - 1, 'close_price']
        logging.info('fantan : zhangfu:{0}, trade_code:{1}, max_date:{2}, min_date:{3}'.format(zhangfu,trade_code,
                                                                                               df.loc[max_index, 'trade_date'],
                                                                                               df.loc[min_index, 'trade_date']))
        print('fantan : zhangfu:{0}, trade_code:{1}, max_date:{2}, min_date:{3}'.format(zhangfu,trade_code,
                                                                                               df.loc[max_index, 'trade_date'],
                                                                                               df.loc[min_index, 'trade_date']))
        if (zhangfu + diefu) >= 0.15:
            fantan = arv_5_flag + (zhangfu + diefu)*100
        else:
            fantan = 1000
    print('fantan_grade:',fantan)
    return fantan
def compute_sujiang(df,canshu1,xielv):
    max_val = 0
    min_val = 100000
    count = 0
    day_count = 1
    sujiang_end = 0
    sujiang_start_time = sujiang_end_time = ''
    sujiang_flag = 0
    sujiang_df_end = 0
    for i in range(len(df) - 1, -1, -1):
        new_val = df.loc[i, 'close_price']
##        print("new_val:",new_val)
##        print("min_val:",min_val)
##        print("max_val:",max_val)
##        print("date:",df.loc[i, 'trade_date'])
        if new_val < max_val:
            count += 1
        elif new_val <= min_val:
            max_val = min_val = new_val
            day_count = 1
            sujiang_start_time = sujiang_end_time = df.loc[i, 'trade_date']
            print("sujiang_end_time:",sujiang_end_time)
            df_r = i
            count = 0
        elif new_val >= max_val:
            max_val = new_val
            count = 0
            day_count += 1
            sujiang_start_time = df.loc[i, 'trade_date']
            print("sujiang_start_time:",sujiang_start_time)
        if count == 10 or new_val < min_val or i == 1 :
            print('flag1')
            print((max_val - min_val)/max_val,(max_val - min_val) / day_count)
            if (max_val - min_val)/max_val >= canshu1 and (max_val - min_val) / day_count >= xielv:
                sujiang_flag = 1
                sujiang_df_end = df_r
                break
            else:
                max_val = min_val = new_val
                sujiang_start_time = sujiang_end_time = df.loc[i, 'trade_date']
                print("sujiang_end_time:",sujiang_end_time)
    return sujiang_flag,sujiang_start_time,sujiang_end_time,sujiang_df_end

def compute_10day(df,canshu_df1,canshu2,canshu3,canshu4,canshu_count,step = 10):
    count = 0
    start_time = end_time = ''
    ten_day_flag = 0
    df_end = canshu_df1
    # for i in range(len(df) - 1, canshu_df1 + 10, -10):
    #     arv = df.loc[i, 'arv_10']
    #     #print("arv:",arv)
    #     sum = 0
    #     for j in range(0, 10):
    #         sum += abs(df.loc[i - j, 'close_price'] - arv) / arv
    #     print('sum / 10:',sum / 10, df.loc[i, 'trade_date'])
    #     if canshu2 <=(sum / 10) <= canshu3:
    #         if count == 0:
    #             end_time = df.loc[i, 'trade_date']
    #             df_r = i
    #         count += 1
    #     elif count >= canshu_count:
    #         start_time = df.loc[i - j, 'trade_date']
    #         ten_day_flag = 1
    #         df_end = df_r
    #         break
    #     else:
    #         count = 0
    df_r_s = 0
    for i in range(canshu_df1 + step, len(df) - 1, step):
        arv = df.loc[i, 'arv_10']
        #print("arv:",arv)
        sum = 0
        for j in range(0, step):
            sum += abs(df.loc[i - j, 'close_price'] - arv) / arv
        print('sum / step:',sum / step, df.loc[i, 'trade_date'])
        if canshu2 <=(sum / step) <= canshu3:
            if count == 0:
                start_time = df.loc[i-step, 'trade_date']
                df_r_s = i-step
            df_r = i
            count += 1
        elif count >= canshu_count and df.loc[i-step, 'close_price']/df.loc[df_r_s, 'close_price'] >= canshu4:
            end_time = df.loc[i-step, 'trade_date']
            ten_day_flag = 1
            df_end = df_r
            break
        else:
            count = 0
        print("canshu4:",df.loc[i-step, 'close_price'],df.loc[df_r_s, 'close_price'],df.loc[i-step, 'close_price']/df.loc[df_r_s, 'close_price'])
    if  ten_day_flag ==0 and count >= canshu_count and df.loc[i-step, 'close_price']/df.loc[df_r_s, 'close_price'] >= canshu4:
        end_time = df.loc[len(df)-1, 'trade_date']
        ten_day_flag = 1
        df_end = df_r
    return ten_day_flag,start_time, end_time,df_end


def save(db,ids,stock_name,zhuang_grade,trade_code,end_t,zhuang_json_s,fantan):
    cursor = db.cursor()
    try:
        print('zhuang_grade:',zhuang_grade)

        sql="insert into compute_result(trade_code,trade_date,stock_id,stock_name,zhuang_grade,zhuang_json,fantan_grade) \
            values('{0}','{1}','{2}','{3}','{4}','{5}','{6}') " \
            "ON DUPLICATE KEY UPDATE trade_code='{0}',trade_date='{1}',stock_id='{2}',stock_name='{3}'," \
            "zhuang_grade='{4}',zhuang_json='{5}',fantan_grade = '{6}'\
            ".format(trade_code,end_t,ids,stock_name,zhuang_grade,zhuang_json_s,fantan)
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
def only_com_fantan():
    pass
def main(h_tab,start_t,end_t):
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    cursor = db.cursor()#使用cursor()方法获取用于执行SQL语句的游标
    sql = "select distinct  stock_id,stock_name from stock_history_trade{0}".format(h_tab)
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    #stock_id_list = [('002752','昇兴股份'),]
    #stock_id_list = [('600295','鄂尔多斯'),]
    #stock_id_list = [('603969','银龙股份'),]
    #stock_id_list = [('600760','中航沈飞'),]
    #stock_id_list = [('603323','苏农银行'),]
    for ids_tuple in stock_id_list:
        zhuang_grade = 1
        zhuang_json = {}
        ids = ids_tuple[0]
        sql = "SELECT trade_date,open_price,close_price,high_price,low_price  FROM stock_history_trade{0} \
                where trade_date >= '{1}' and trade_date <= '{2}' and  stock_id  = '{3}'".format(h_tab,start_t,end_t,ids)
        df = get_df_from_db(sql, db)
        #速降区间
        sujiang_flag,sujiang_start_time,sujiang_end_time,sujiang_df_end = compute_sujiang(df,0.25,0.010)
        print('sujiang:',sujiang_flag,sujiang_start_time,sujiang_end_time)
        #台地区间
        taidi_flag,taidi_start, taidi_end,taidi_df_end = compute_10day(df, sujiang_df_end, 0, 0.015, 0.97, 10, 3)
        print("taidi:",taidi_flag,'taidi_start:',taidi_start, 'taidi_end:',taidi_end)
        if taidi_flag == 1:
            zhuang_json["one_flat"] = [str(taidi_start),str(taidi_end)]
            zhuang_grade += 4000
            #预拉升区间
            lasheng_flag, lasheng_start, lasheng_end, lasheng_df_end = compute_10day(df, taidi_df_end, 0.02, 0.09, 1.05, 3, 3)
            print("lasheng:",lasheng_flag, str(lasheng_start), str(lasheng_end))
            if lasheng_flag == 1:
                zhuang_json["up_list"] = [str(lasheng_start), str(lasheng_end)]
                zhuang_grade += 2000
                #平台区间
                pingtai_flag, pingtai_start, pingtai_end, pingtai_df_end = compute_10day(df, lasheng_df_end, 0, 0.02, 0.96, 3,3)
                print("pingtai:",pingtai_flag, pingtai_start, pingtai_end)
                if pingtai_flag == 1:
                    zhuang_json["two_flat"] = [str(pingtai_start),str(pingtai_end)]
                    zhuang_grade += 1000
        zhuang_json_s = json.dumps(zhuang_json)
        date_str = re.sub('-', '', end_t[0:10])
        trade_code = date_str + ids
        fantan = com_fantan(df,trade_code)
        save(db, ids, ids_tuple[1], zhuang_grade, trade_code, end_t, zhuang_json_s,fantan)
        # compute_junxiancha2(df, ids, db)
    cursor.close()

if __name__ == "__main__":
    #h_tab = 10
    start_t ='20200101'
    end_t =  '20201211'
    #main(h_tab, start_t, end_t)
    
    p = Pool(8)
    for i in range(1,11):
        p.apply_async(main, args=(str(i),start_t,end_t,))
##    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
