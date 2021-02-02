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
def show_bk(db):
    cursor = db.cursor()
    sql = "select distinct  bk_name from stock_informations"
    cursor.execute(sql)
    bk_tup = cursor.fetchall()
    bk_txt = ''
    for bk in bk_tup:
        bk_txt += bk[0]+','
    print('bk_txt:',bk_txt)
    cursor.close()
    return bk_txt
def select_comput_res(db,date,bk,option):
    cursor = db.cursor()
    delta = 30
    end_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    start_date = (end_date - datetime.timedelta(days=delta)).strftime("%Y-%m-%d")
    # sql3 = "select C.redu_grade,I.stock_id,I.stock_name,I.h_table from stock_informations I " \
    #       "inner JOIN (select stock_code,(sum(30+jmrate)+count(jmrate)*10000) as redu_grade from longhu_info where  " \
    #       "trade_date >= '{0}' and trade_date <= '{1}' group by stock_code) C " \
    #       "ON I.stock_id = C.stock_code " .format(start_date,date) #旧版本查询热度
    sql1 = "select case  when C.redu is null then 0 else C.redu end,I.stock_id,I.stock_name,I.h_table from stock_informations I " \
          "LEFT JOIN (select stock_id,redu from compute_result where trade_date like '{0}') C " \
          "ON I.stock_id = C.stock_id where bk_name = '{1}' ".format(date,bk)
    if option == 'bk0':
        sql = sql1
    cursor.execute(sql)
    info_list = cursor.fetchall()
    print('info_list:',info_list)
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
def draw_k_line(df,chart_title,delta_t,zhuang_json):
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
    cursor.close()
    if len(zhuang_json_s)!=0:
        print('zhuang_json_s:',zhuang_json_s[0][0])
        zhuang_json = json.loads(zhuang_json_s[0][0])
    else:
        zhuang_json = []
    return zhuang_json
def draw_main(db,info_list,date,start_date,end_date):
    zero_time = datetime.datetime.strptime('2020-01-01','%Y-%m-%d')
    origin = '2020-01-01'
    delta_t = (zero_time - datetime.datetime.strptime(origin,'%Y-%m-%d')).days
    for id_tup in info_list:
        print('id_tup:',id_tup)
        ids = id_tup[1]
        chart_title = id_tup[1] + '_' + id_tup[2] + '_' + str(id_tup[0]) + '_' + date
        h_tab = id_tup[3]
        # ids = '600018'
        sql = "SELECT trade_date,open_price,close_price,high_price,low_price  FROM stockdb.stock_history_trade{0} \
                where trade_date >= '{1}' and trade_date <= '{2}' and  stock_id = '{3}'".format(h_tab, start_date, end_date, ids)
        df = get_df_from_db(sql, db)
        zhuang_json = sel_zhuang_json(db, date, ids)
        draw_k_line(df, chart_title,delta_t,zhuang_json)
def draw_main_bk(db,date,start_date,end_date,bk_name):
    zero_time = datetime.datetime.strptime('2020-01-01','%Y-%m-%d')
    origin = '2020-01-01'
    delta_t = (zero_time - datetime.datetime.strptime(origin,'%Y-%m-%d')).days
    cursor = db.cursor()
    sql = "select  bankuai_name,bankuai_code,amount_money,zhenfu,increase,turnover_rate" \
          " from bankuai_day_data where trade_date like '{}%' and bankuai_name = '{}'".format(date,bk_name)
    cursor.execute(sql)
    bk_info = cursor.fetchall()[0]
    print('bk_info:',bk_info)
    cursor.close()
    #ids = one_bk[1]
    #chart_title = one_bk[1] + '_' + one_bk[0]+'_' + date + one_bk[4] + '_' + str(int(one_bk[2])/1000000000)+'亿' + '_'+one_bk[5]+'_' + date + one_bk[4]
    chart_title ="{0}_{1}_{2}_振幅：{3}_{4}亿_换手:{5}_涨幅：{6}".format(bk_info[1],bk_info[0],date,str(bk_info[4]),str(float(bk_info[2])/1000000000),str(bk_info[5]),str(bk_info[4]))
    sql = "SELECT trade_date,open_price,close_price,high_price,low_price  FROM bankuai_day_data \
            where trade_date >= '{0}' and trade_date <= '{1}' and  bankuai_code = '{2}'".format(start_date, end_date, bk_info[1])
    df = get_df_from_db(sql, db)
    zhuang_json = []#sel_zhuang_json(db, date, ids)
    draw_k_line(df, chart_title,delta_t,zhuang_json)
if __name__ == '__main__':
    # st.sidebar.title(u'菜单侧边栏')
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    bk_txt = show_bk(db)
    st.markdown(bk_txt, unsafe_allow_html=False)
    date_str = datetime.datetime.now().strftime('%Y-%m-%d')
    date = st.text_input('时间', value=date_str, key=None)
    # grade = st.text_input('分数', value='7001', key=None)
    bk_name = st.text_input('bk', value='银行', key=None)
    start_date = st.text_input('开始时间', value='2020-01-01', key=None)
    end_date = st.text_input('结束时间', value='2021-01-01', key=None)
    # if grade != 'None':
    #     grade = float(grade)
    options = ['板块','板块5','板块10','板块15','板块20']
    opt_res = st.selectbox( label='选择',options = options)
    # print('opt_res:',opt_res)
    run1 = st.button(u'执行')



    # 查看板块
    if run1 and opt_res == '板块':
        # date = '2020-08-28'
        draw_main_bk(db,date,start_date,end_date,bk_name)
        info_list = select_comput_res(db,date,bk_name,option = 'bk0')
        draw_main(db, info_list,date,start_date,end_date)
    '''========END========'''