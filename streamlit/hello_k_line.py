#coding:utf-8
import mpl_finance
import mplfinance as mpf
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
import copy

sns.set()
#pro = ts.pro_api()
def select_comput_res(db,date,option,grade_start = 0,grade_end = 0,stock_id = None):
    cursor = db.cursor()
    sql = "select C.{0},I.stock_id,I.stock_name,I.h_table from stock_informations I " \
          "LEFT JOIN (select stock_id,{0} from compute_result where trade_date like '{1}') C " \
          "ON I.stock_id = C.stock_id " \
          "where C.{0} >={2} ".format(option,date,grade)
    grade2 = grade - 1000
    # sql1 = "select C.zhuang_grade,I.stock_id,I.stock_name,I.h_table from stock_informations I " \
    #       "LEFT JOIN (select stock_id,zhuang_grade from compute_result where trade_date like '{0}') C " \
    #       "ON I.stock_id = C.stock_id " \
    #       "where C.zhuang_grade >={1} and C.zhuang_grade < ({1} + 1000)".format(date,grade)
    sql1 = "select C.zhuang_grade,I.stock_id,I.stock_name,I.h_table from stock_informations I " \
          "LEFT JOIN (select stock_id,zhuang_grade from com_zhuang ) C " \
          "ON I.stock_id = C.stock_id " \
          "where C.zhuang_grade >={0} and C.zhuang_grade < {1}".format(grade_start,grade_end)
    print('grade:',grade)
    # sql1 = "select C.ratio_30,I.stock_id,I.stock_name,I.h_table from stock_informations I " \
    #       "LEFT JOIN (select stock_id,ratio_30 from compute_result where trade_date like '{0}%') C " \
    #       "ON I.stock_id = C.stock_id " \
    #       "where 0 < C.ratio_30 and C.ratio_30 <={1} ".format(date,grade)
    sql2 = "select C.fantan_grade,I.stock_id,I.stock_name,I.h_table from stock_informations I " \
          "LEFT JOIN (select stock_id,fantan_grade from compute_result where trade_date like '{0}') C " \
          "ON I.stock_id = C.stock_id " \
          "where C.fantan_grade >={1} and C.fantan_grade <({1} + 1000)".format(date,grade)
    delta = 30
    end_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    start_date = (end_date - datetime.timedelta(days=delta)).strftime("%Y-%m-%d")
    # sql3 = "select C.redu_grade,I.stock_id,I.stock_name,I.h_table from stock_informations I " \
    #       "inner JOIN (select stock_code,(sum(30+jmrate)+count(jmrate)*10000) as redu_grade from longhu_info where  " \
    #       "trade_date >= '{0}' and trade_date <= '{1}' group by stock_code) C " \
    #       "ON I.stock_id = C.stock_code " .format(start_date,date) #旧版本查询热度
    sql3 = "select redu_5,stock_id,stock_name,h_table from com_redu where redu_5 >='{1}' and redu_5 <='{2}' " \
           "and trade_date = '{0}'".format(date,grade_start,grade_end)
    sql4 = "select C.zhuang_grade,I.stock_id,I.stock_name,I.h_table from stock_informations I " \
           "LEFT JOIN (select stock_id,zhuang_grade from com_zhuang ) C " \
           "ON I.stock_id = C.stock_id " \
           "where I.stock_id = '{0}'".format(stock_id)
    print('sql4',sql4)
    # sql5 = "select V.redu_5,V.stock_id,V.stock_name,I.h_table,V.trade_date,I.bk_name from verify_redu_5 V " \
    #        "left join stock_informations I " \
    #        "on V.stock_id = I.stock_id " \
    #        " where V.redu_5 >= 10000 and V.days = 4 "

    #自定义量庄测试
    sql5 = "select C.zhuang_grade,C.stock_id,C.stock_name,I.h_table,C.dibu_date,I.bk_name from compute_zhuang_test C " \
           "left join stock_informations I " \
           "on C.stock_id = I.stock_id order by C.stock_id"
    if option == 'fantan_grade':
        sql = sql2
    elif option == 'zhuang_grade':
        sql = sql1
    elif option == 'remen_grade':
        sql = sql3
    elif option == 'gegu':
        sql = sql4
    elif option == 'zidingyi':
        sql = sql5
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
    df = df.fillna(0)
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
def draw_k_line(df,chart_title,zhuang_section=[],yidong =[]):
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
    plt.plot(df['dates'], df['5'])
    print('zhuang_section:',zhuang_section)
    for zhaung_tup in zhuang_section:
        sta = comput_ind(df, zhaung_tup[1])
        end = comput_ind(df, zhaung_tup[0])
        print('indexs:',sta,end )
        plt.plot(df['dates'][sta:end], df['5'][sta:end] ,color='green')
    plt.legend();

    plt.show()  # 显示
    st.pyplot()
    #绘制成交量图
    # turnover_df = df[['dates','turnover_rate']]
    # st.bar_chart(turnover_df)
#查询庄线集合
def new_draw_k_line(df,chart_title,zhuang_section=[],yidong =[]):
    old_df = copy.deepcopy(df)
    old_df['5'] = old_df['close_price'].rolling(5).mean()
    # old_df.set_index(["dates"], inplace=True)
    # 对数据进行改名，mplfinance有要求
    df.rename(
        columns={'trade_date': 'Date', 'open_price': 'Open', 'high_price': 'High', 'low_price': 'Low', 'close_price': 'Close', 'trade_amount': 'Volume'},
        inplace=True)
    # 将Date设置为索引，并转换为 datetime 格式
    df.set_index(["Date"], inplace=True)
    df.index = pd.to_datetime(df.index)
    plt.rcParams['font.sans-serif'] = ['KaiTi']
    plt.rcParams['axes.unicode_minus'] = False
    s = mpf.make_mpf_style(base_mpf_style='yahoo', rc={'font.family': 'SimHei'})
    fig, ax = plt.subplots(figsize=(23,3))
    mpf.plot(df, type='candle', mav=(5,125), volume=True,title=chart_title,style=s)
    for zhaung_tup in zhuang_section:
        sta = comput_ind(old_df, zhaung_tup[1])
        end = comput_ind(old_df, zhaung_tup[0])
        print('indexs:',sta,end )
        plt.plot(old_df['dates'][sta:end], old_df['5'][sta:end], color='yellow')
    plt.legend()
    st.pyplot()
def sel_zhuang_json(db,date,ids):
    cursor = db.cursor()
    sql = "select zhuang_section,yidong from com_zhuang where stock_id = '{}'".format(ids)
    cursor.execute(sql)
    zhuang_res_s = cursor.fetchall()
    cursor.close()
    if len(zhuang_res_s)!=0:
        print('zhuang_res_s:',zhuang_res_s)
        # zhuang_section = list(zhuang_res_s[0][0][0])
        # yidong = list(zhuang_res_s[0][0][1])
        zhuang_section = eval(zhuang_res_s[0][0])
        yidong =eval(zhuang_res_s[0][1])
    else:
        zhuang_section = []
        yidong = []
    return zhuang_section,yidong
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
        # sql = "SELECT trade_date,open_price,close_price,high_price,low_price,turnover_rate as trade_amount  FROM stockdb.stock_history_trade{0} \
        #         where trade_date >= '{1}' and trade_date <= '{2}' and  stock_id = '{3}'".format(h_tab, start_date, end_date, ids)
        sql = "SELECT trade_date,open_price,close_price,high_price,low_price,turnover_rate as trade_amount  FROM stockdb.stock_history_trade{0} \
                where  trade_date>='{1}' and trade_date<='{2}' and stock_id = '{3}'".format(h_tab,start_date,end_date,ids)
        df = get_df_from_db(sql, db)
        zhuang_section,yidong = sel_zhuang_json(db, date, ids)
        # draw_k_line(df, chart_title,zhuang_section,yidong)
        new_draw_k_line(df, chart_title,zhuang_section,yidong)
def draw_main_user_define(db,info_list):
    for id_tup in info_list:
        print('id_tup:',id_tup)
        ids = id_tup[1]
        trade_date = id_tup[4]
        bk_name = id_tup[5]
        # trade_date_datetime = datetime.datetime.strptime(trade_date, '%Y-%m-%d')
        trade_date_datetime = trade_date
        start_date = (trade_date_datetime - datetime.timedelta(days=150)).strftime('%Y-%m-%d')
        # end_date = (trade_date_datetime + datetime.timedelta(days=3)).strftime('%Y-%m-%d')
        date_str = trade_date_datetime.strftime('%Y-%m-%d')
        # print('se_date:',start_date,end_date)
        chart_title = id_tup[1] + '_' + id_tup[2] + '_' + str(id_tup[0]) + '_' + date_str + '_' + bk_name
        h_tab = id_tup[3]
        # ids = '600018'
        sql = "SELECT trade_date,open_price,close_price,high_price,low_price,trade_amount  FROM stockdb.stock_history_trade{0} \
                where trade_date >= '{1}' and  stock_id = '{2}'".format(h_tab, start_date, ids)
        df = get_df_from_db(sql, db)
        index_list = df.query("trade_date == '{}'".format(date_str))
        if len(index_list) == 0:
            continue
        print('index_list',type(df['trade_date'][0]))
        index = index_list.index[-1]
        # df =df[0:index+4]
        zhuang_section,yidong = sel_zhuang_json(db, date, ids)
        # draw_k_line(df, chart_title,zhuang_section,yidong)
        new_draw_k_line(df, chart_title, zhuang_section=[], yidong=[])
def draw_main_bk(db,date,start_date,end_date,len_day):
    end_time = datetime.datetime.strptime(date,'%Y-%m-%d')
    start_t = (end_time - datetime.timedelta(days = len_day)).strftime('%Y-%m-%d')
    cursor = db.cursor()
    sql = "select  bankuai_name,bankuai_code,sum(amount_money) amount_money,sum(redu) redu,sum(increase) increase,sum(turnover_rate) turnover_rate " \
          " from bankuai_day_data where trade_date  >= '{0}' and trade_date  <= '{1}' group by bankuai_code order by redu DESC".format(start_t,date)
    cursor.execute(sql)
    bk_info = cursor.fetchall()
    print('bk_info:',bk_info)
    cursor.close()
    for one_bk in bk_info:
        print('one_bk:',one_bk)
        #ids = one_bk[1]
        #chart_title = one_bk[1] + '_' + one_bk[0]+'_' + date + one_bk[4] + '_' + str(int(one_bk[2])/1000000000)+'亿' + '_'+one_bk[5]+'_' + date + one_bk[4]
        chart_title ="{0}_{1}_{2}_热度：{3}_{4}亿_换手:{5}_涨幅：{6}".format(one_bk[1],one_bk[0],date,str(one_bk[3]),str(float(one_bk[2])/1000000000),str(one_bk[5]),str(one_bk[4]))
        sql = "SELECT trade_date,open_price,close_price,high_price,low_price  FROM bankuai_day_data \
                where trade_date >= '{0}' and trade_date <= '{1}' and  bankuai_code = '{2}'".format(start_date, end_date, one_bk[1])
        df = get_df_from_db(sql, db)
        # zhuang_json = []#sel_zhuang_json(db, date, ids)
        draw_k_line(df, chart_title)
if __name__ == '__main__':
    # st.sidebar.title(u'菜单侧边栏')
    date_str = datetime.datetime.now().strftime('%Y-%m-%d')
    date = st.text_input('时间', value=date_str, key=None)
    grade_start = st.text_input('分数起 / 股票编码', value='1000', key=None)
    grade_end = st.text_input('分数止', value='2000', key=None)
    start_date = st.text_input('开始时间', value='2019-01-01', key=None)
    end_date = st.text_input('结束时间', value='2021-03-01', key=None)
    # if grade_start != 'None' and grade_end != None:
    #     grade_start = float(grade_start)
    #     grade_end = float(grade_end)
    #临时变量
    grade = 0
    options = ['庄线','个股','反弹','热门','板块0','板块3','板块7','板块14','板块25','自定义']
    opt_res = st.selectbox( label='选择',options = options)
    # print('opt_res:',opt_res)
    run1 = st.button(u'执行')
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    #查看反弹
    if run1 and opt_res == '反弹':
        #date = '2020-08-28'
        if grade == 'None':
            grade = 7000
        info_list = select_comput_res(db,date,option = 'fantan_grade',grade = grade)
        draw_main(db, info_list,date,start_date,end_date)
    #查看庄线
    elif run1 and opt_res == '庄线':
        #date = '2020-08-28'
        # if grade == 'None':
        #     grade = 7000
        info_list = select_comput_res(db,date,option = 'zhuang_grade',grade_start = float(grade_start),grade_end =  float(grade_end))
        draw_main(db, info_list,date,start_date,end_date)

    # 查看板块
    elif run1 and opt_res == '板块0':
        # date = '2020-08-28'
        len_day = 0
        draw_main_bk(db,date,start_date,end_date,len_day)
    elif run1 and opt_res == '板块3':
        # date = '2020-08-28'
        len_day = 3
        draw_main_bk(db, date, start_date, end_date, len_day)
    elif run1 and opt_res == '板块7':
        # date = '2020-08-28'
        len_day = 7
        draw_main_bk(db,date,start_date,end_date,len_day)
    elif run1 and opt_res == '板块14':
        # date = '2020-08-28'
        len_day = 14
        draw_main_bk(db, date, start_date, end_date, len_day)
    elif run1 and opt_res == '板块25':
        # date = '2020-08-28'
        len_day = 25
        draw_main_bk(db,date,start_date,end_date,len_day)
    elif run1 and opt_res == '热门':
        #date = '2020-08-28'
        # if grade == 'None':
        #     grade = 7000
        info_list = select_comput_res(db,date,option = 'remen_grade',grade_start = float(grade_start),grade_end =  float(grade_end))
        draw_main(db, info_list,date,start_date,end_date)
    elif run1 and opt_res == '个股':
        #date = '2020-08-28'
        # if grade == 'None':
        #     grade = 7000
        info_list = select_comput_res(db,date,option = 'gegu',stock_id = grade_start)
        draw_main(db, info_list,date,start_date,end_date)
    elif run1 and opt_res == '自定义':
        #date = '2020-08-28'
        # if grade == 'None':
        #     grade = 7000
        info_list = select_comput_res(db,date,option = 'zidingyi')
        draw_main_user_define(db, info_list)
    '''========END========'''
