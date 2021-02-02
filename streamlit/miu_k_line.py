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
import streamlit as st
import datetime
import json

sns.set()
#pro = ts.pro_api()
def select_comput_res(db,date,option,grade):
    cursor = db.cursor()
    sql = "select C.{0},I.stock_id,I.stock_name,I.h_table from stock_informations I " \
          "LEFT JOIN (select stock_id,{0} from compute_result where trade_date like '{1}') C " \
          "ON I.stock_id = C.stock_id " \
          "where C.{0} >={2} ".format(option,date,grade)
    grade2 = grade - 1000
    sql1 = "select C.zhuang_grade,I.stock_id,I.stock_name,I.h_table from stock_informations I " \
          "LEFT JOIN (select stock_id,zhuang_grade from compute_result where trade_date like '{0}') C " \
          "ON I.stock_id = C.stock_id " \
          "where C.zhuang_grade >={1} and C.zhuang_grade < ({1} + 1000)".format(date,grade)
    print('grade:',grade)
    # sql1 = "select C.ratio_30,I.stock_id,I.stock_name,I.h_table from stock_informations I " \
    #       "LEFT JOIN (select stock_id,ratio_30 from compute_result where trade_date like '{0}%') C " \
    #       "ON I.stock_id = C.stock_id " \
    #       "where 0 < C.ratio_30 and C.ratio_30 <={1} ".format(date,grade)
    sql2 = "select C.fantan_grade,I.stock_id,I.stock_name,I.h_table from stock_informations I " \
          "LEFT JOIN (select stock_id,fantan_grade from compute_result where trade_date like '{0}') C " \
          "ON I.stock_id = C.stock_id " \
          "where C.fantan_grade >={1} ".format(date,grade)
    if option == 'fantan_grade':
        sql = sql2
    elif option == 'zhuang_grade':
        sql = sql1
    cursor.execute(sql)
    info_list = cursor.fetchall()
    info_list = sorted(info_list, reverse=True )
    print(info_list)
    cursor.close()
    return info_list

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
    df['trade_date'] = [x.strftime('%Y-%m-%d') for x in df['trade_date']]
    # df['trade_date'] = pd.to_datetime(df['trade_date']).map(date2num)
    df['dates'] = np.arange(0, len(df))
    cursor.close()
    print("df:",df)
    # df['trade_date'] = date2num(df['trade_date'])
    return df

def comput_ind(df,time):
    time = time[0:10]
    ind = df.query("trade_date == '{}'".format(time))
    print("comput_ind:",df['trade_date'][0],type(df['trade_date'][0]),time,type(time))
    if len(ind)>0:
        return ind.index[0]
    else:
        return 0
def draw_k_line(df,chart_title,zhuang_json):
    #fig, ax = plt.subplots(figsize=(10, 5))

    # print(len(df))
    df['5'] = df['close_price'].rolling(5).mean()

    # df['20'] = df.rolling(20).mean()
    # df['30'] = df.rolling(30).mean()
    # df['60'] = df.rolling(60).mean()
    # df['120'] = df.rolling(120).mean()
    # df['250'] = df.rolling(250).mean()

    # fig, ax = plt.subplots(figsize=(10,5))
    # mpl_finance.candlestick_ochl(
    #     ax=ax,
    #     quotes=df[['trade_date', 'open_price', 'close_price', 'high_price', 'low_price']].values,
    #     width=0.7,
    #     colorup='r',
    #     colordown='g',
    #     alpha=0.7)
    # ax.xaxis_date()
    # # 绘制均线
    # # for ma in ['5']:
    # #     plt.plot(df['dates'], df[ma])
    # # plt.legend()
    # # ax.set_title('上证综指K线图(2017.1-)', fontsize=20)
    # plt.xticks(rotation=30)
    def format_date(x,pos):
        if x<0 or x>len(date_tickers)-1:
            return ''
        return date_tickers[int(x)]

    date_tickers = df.trade_date2.values
    plt.rcParams['font.sans-serif'] = ['KaiTi']
    plt.rcParams['axes.unicode_minus'] = False
    fig, ax = plt.subplots(figsize=(23,5))
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    ax.set_title(chart_title, fontsize=20)
    # 绘制K线图
    mpl_finance.candlestick_ochl(
        ax=ax,
        quotes=df[['dates', 'open_price', 'close_price', 'high_price', 'low_price']].values,
        width=0.7,
        colorup='r',
        colordown='g',
        alpha=0.7)
    # 绘制均线
    # for ma in ['5',]:
    #     plt.plot(df['dates'], df[ma])
    plt.plot(df['dates'], df['5'])
    print('zhuang_json:',zhuang_json)
    if 'one_flat' in zhuang_json:
        sta = comput_ind(df, zhuang_json['one_flat'][0])
        end = comput_ind(df, zhuang_json['one_flat'][-1])
        print('one_flat:',sta,end )
        plt.plot(df['dates'][sta:end], df['5'][sta:end] ,color='green')
    if 'up_list' in zhuang_json:
        sta = comput_ind(df, zhuang_json['up_list'][0])
        end = comput_ind(df, zhuang_json['up_list'][-1])
        plt.plot(df['dates'][sta:end], df['5'][sta:end] ,color='yellow')
    if 'two_flat' in zhuang_json:
        sta = comput_ind(df, zhuang_json['two_flat'][0])
        end = comput_ind(df, zhuang_json['two_flat'][-1])
        plt.plot(df['dates'][sta:end], df['5'][sta:end] ,color='red')

    plt.legend();

    plt.show()  # 显示
    st.pyplot()
def sel_zhuang_json(db,date,ids):
    cursor = db.cursor()
    sql = "select zhuang_json from compute_result where trade_date like '{}%' and stock_id = '{}'".format( date, ids)
    cursor.execute(sql)
    zhuang_json_s = cursor.fetchall()
    print('zhuang_json_s:',zhuang_json_s[0][0])
    cursor.close()
    zhuang_json = json.loads(zhuang_json_s[0][0])
    return zhuang_json
def select_h_table(db,id):
    cursor = db.cursor()
    sql = "select h_table from stock_informations where stock_id ='{}'".format(id)
    cursor.execute(sql)
    h_table = cursor.fetchall()[0][0]
    print('h_table:',h_table)
    cursor.close()
    return h_table
def select_trade_code(db,id,start_date,end_date,h_table):
    cursor = db.cursor()
    sql = "select trade_code from stock_miu_trade{0} where stock_id ='{1}' and trade_date >= '{2}' and trade_date <= '{3}'".format(h_table,id,start_date,end_date)
    cursor.execute(sql)
    trade_code_list = cursor.fetchall()
    print('trade_code_list:',trade_code_list)
    cursor.close()
    return trade_code_list
def db_to_df(db,h_table,id,trade_code):
    cursor = db.cursor()
    sql = "SELECT data  FROM stock_miu_trade{0} \
            where trade_code = '{1}'".format(h_table, trade_code)
    cursor.execute(sql)
    data = cursor.fetchall()[0][0]
    sql = "SELECT 20_miu_json  FROM stock_miu_trade{0} \
            where trade_code = '{1}'".format(h_table, trade_code)
    print('h_table, trade_code:',h_table, trade_code)
    cursor.execute(sql)
    mean_data = cursor.fetchall()[0][0]
    print('mean_data:',mean_data)
    cursor.close()
    print('len(data):',len(data))

    print('db_data:',data)
    #data_json = data
    data_json = json.loads(data)
    df = pd.DataFrame.from_dict(data_json, orient='columns')
    print('df1:',df)
    if len(data_json) != 0:
        df['time'] = ''
        for i in range(len(df)):
            df.loc[i,'time'] = str(df.loc[i, 't'])[:-2]+'00'
        print('df_f1',df)
        df = df.groupby(['time'])['v'].sum()
        print('df_f2', df)
        df = df.reset_index()

        # for i in range(len(df)):
        #     df.loc[i,'time'] = datetime.datetime.strptime(str(df.loc[i,'t']), "%H%M%S")
        #     #print('time:',df.loc[i,'time'])
        # df = df.set_index(keys=['time'])
        # df = df[['v']]
    mean_data_json = json.loads(mean_data)
    mean_df = pd.DataFrame.from_dict(mean_data_json, orient='columns')
    mean_df = mean_df.set_index('time')
    df = df.set_index('time')
    print('mean_df:',mean_df)
    #df = df.set_index(keys=['time'])
    df_sum = df.join(mean_df)
    print('df_f3:', df_sum)
    df_sum = df_sum[['v','pv']]
    print('df2:', df_sum)
    return df_sum
    # df['tm'] = ''
    # for i in range(len(df)):
    #     df.loc[i,'tm'] = str(df.loc[i,'t'])[:-2]
    # print('df2:', df)
def draw_history(db,id,start_date,end_date):
    h_table = select_h_table(db,id)
    trade_code_list = select_trade_code(db,id,start_date,end_date,h_table)
    print('trade_code_list:',trade_code_list)
    for trade_codes in [trade_code_list[-1],]:
        trade_code = trade_codes[0]
        #chart_title = id_tup[1] + '_' + id_tup[2] + '_' + str(id_tup[0]) + '_' + date
        # ids = '600018'

        df = db_to_df(db,h_table,id,trade_code)
        print('df:',df)
        # zhuang_json = sel_zhuang_json(db, date, ids)
        #draw_k_line(df, chart_title,delta_t,zhuang_json)
        if len(df) == 0:
            continue
        st.bar_chart(df)

if __name__ == '__main__':
    # st.sidebar.title(u'菜单侧边栏')
    print('start.')
    date_str = datetime.datetime.now().strftime('%Y-%m-%d')
    id = st.text_input('id', value='', key=None)
    date = st.text_input('时间', value=date_str, key=None)
    grade = st.text_input('分数', value='7001', key=None)
    start_date = st.text_input('开始时间', value='2020-01-01', key=None)
    end_date = st.text_input('结束时间', value='2021-01-01', key=None)
    if grade != 'None':
        grade = float(grade)
    options = ['个股历史','分时分数']
    opt_res = st.selectbox( label='选择',options = options)
    # print('opt_res:',opt_res)
    run1 = st.button(u'执行')
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    #查看反弹
    if run1 and opt_res == '分时分数':
        #date = '2020-08-28'
        if grade == 'None':
            grade = 100
        info_list = select_comput_res(db,date,option = 'fantan_grade',grade = grade)
        # draw_main(db, info_list,date,start_date,end_date)
        '''========END========'''
    #查看庄线
    elif run1 and opt_res == '个股历史':
        #date = '2020-08-28'
        info_list = select_comput_res(db,date,option = 'zhuang_grade',grade = grade)
        draw_history(db, id, start_date, end_date)
        # draw_main(db, info_list,date,start_date,end_date)

        '''========END========'''