#coding=utf-8
import streamlit as st
import pandas as pd
import numpy as np
import pymysql
import datetime
import time
#from cpu_memory import read_disk
st.sidebar.title(u'菜单侧边栏')
def get_df_from_db(sql, db):
    cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)  # 执行SQL语句
    data = cursor.fetchall()

    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df
    df = df.set_index(keys=['trade_date'])
    cursor.close()
    print(df)
    return df
def show_chart(df):
    chart_data = df#pd.DataFrame(np.random.randn(20, 3),columns = ['a', 'b', 'c'])
    st.line_chart(chart_data)
def main():
    db = pymysql.connect("localhost", "root", "Zzl08382020", "stockdb")
    ids = '600018'
    sql = "SELECT trade_date,open_price,close_price,high_price,low_price  FROM stockdb.stock_history_trade1 limit 10 where stock_id = '{}'".format(ids)
    df = get_df_from_db(sql, db)
    show_chart(df)
main()
