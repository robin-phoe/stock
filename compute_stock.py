#coding=utf-8
import requests
import re
import pymysql
import pandas as pd
import logging
import math
import datetime
from multiprocessing import Pool
pd.set_option('display.max_rows',1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth',1000)

logging.basicConfig(level=logging.DEBUG,filename='stock_compute.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')




def get_df_from_db(sql,db):
    cursor = db.cursor()#使用cursor()方法获取用于执行SQL语句的游标
    cursor.execute(sql)# 执行SQL语句
    """
    使用fetchall函数以元组形式返回所有查询结果并打印出来
    fetchone()返回第一行，fetchmany(n)返回前n行
    游标执行一次后则定位在当前操作行，下一次操作从当前操作行开始
    """
    data = cursor.fetchall()

    #下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description #获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))] #获取列名
    df = pd.DataFrame([list(i) for i in data],columns=columnNames) #得到的data为二维元组，逐行取出，转化为列表，再转化为df
    
    """
    使用完成之后需关闭游标和数据库连接，减少资源占用,cursor.close(),db.close()
    db.commit()若对数据库进行了修改，需进行提交之后再关闭
    """
    cursor.close()
    #db.close()

    #print("cursor.description中的内容：",columnDes)
    return df
def compute(stock_df,db,trade_code,date,fantan_grade):
    stock_df = stock_df.fillna(999999)
    #print(stock_df['trade_date'])
    #print('stock_df:',stock_df)
    stock_df=stock_df.sort_values(axis=0, ascending=False, by='trade_date',na_position='last')
    stock_df.reset_index(inplace=True)
    #print('stock_df_sort:',stock_df)
    #print("stock_df['P_E']",stock_df['P_E'])
    #print(stock_df['trade_date'])
    stock_df[['close_price','increase','turnover_rate','trade_amount']] = stock_df[['close_price','increase','turnover_rate','trade_amount']].astype(float)
    price_list=stock_df['close_price']
    last_price=float(price_list[0])
    #print("last_price:",last_price)
    his_mean = price_list.mean()
    #print('his_mean',his_mean)
    #市值
    #print("stock_df.loc[0,'fxl']",stock_df.loc[0,'fxl'])
    if stock_df.loc[0,'fxl'] != None:
        market_value = stock_df.loc[0,'fxl'] * last_price /1000000 #亿
    else:
        market_value = 999999
    #历史均值比
    his_compare_now=his_mean/last_price
    #
    mean_300 = price_list[0:300].mean()
    #print('mean_300',mean_300)
    #300日均值比
    now_compare_300=mean_300/last_price
    mean_60 = price_list[0:60].mean()
    #print('mean_60',mean_60)
    #计算30日价比
    #print('len(price_list)',len(price_list))
    if len(price_list)>30:
        diff_pri_30=price_list[0]/price_list[30]
        #print('diff_pri_30',diff_pri_30)
    else:
        diff_pri_30 = 0
    #计算历史除极交量均值
    his_trade_amount=[]
    his_turnover_amount=[]
    #his_turnover_mer_amount=[]
    for i in  range(len(stock_df)):
        if math.fabs(stock_df.loc[i,'increase'])<0.03:
            his_trade_amount.append(stock_df.loc[i,'trade_amount'])
            his_turnover_amount.append(stock_df.loc[i,'turnover_rate'])
            #his_turnover_mer_amount.append(stock_df.loc[i,'turnover_ratio'])
    if len(his_trade_amount)>0:
        his_trade_amount_mean=sum(his_trade_amount)/(len(his_trade_amount))
        his_turnover_mean=sum(his_turnover_amount)/(len(his_turnover_amount))
        #his_turnover_mer_mean=sum(his_turnover_mer_amount)/(len(his_turnover_mer_amount))
        his_turnover_mer_mean = 0
    else:
        his_trade_amount_mean=his_turnover_mean=his_turnover_mer_mean=0
##    print('his_trade_amount_mean:',his_trade_amount_mean)
##    print('his_turnover_mean:',his_turnover_mean)
##    print('his_turnover_mer_mean:',his_turnover_mer_mean)
    #计算60日价格振幅小于2.5%统计；60日均值振幅合格数统计
    count_price_60=0
    count_mean_60=0
    if len(stock_df)>=60:
        for i in range(60):
            if math.fabs(stock_df.loc[i,'increase'])<0.025:
                count_price_60+=1
            if math.fabs(stock_df.loc[i,'close_price']-mean_60)/mean_60 < 0.03:
                count_mean_60+=1
        print('count_price_60:',count_price_60,'count_mean_60:',count_mean_60,stock_df.loc[0,'increase'])
    #计算换手率合格天数
    if len(stock_df)>=100:
        hsts=stock_df['达标换手率'][0:100].sum()
    else:
        hsts=stock_df['达标换手率'].sum()
    #print('hsts:',hsts)
    #历史极值
    his_high = price_list.max()
    his_low = price_list.min()
    #市盈
    #print("stock_df['P_E'][0]",stock_df['P_E'][0])
    logging.info("name:{},stock_df['P_E'][0]{}".format(stock_df.loc[1,'stock_name'],stock_df['P_E'][0]))
    if stock_df['P_E'][0]!='-'and stock_df['P_E'][0]!=None and float(stock_df['P_E'][0]) > 0:
        P_E_grade=28/(float(stock_df['P_E'][0]))*5-5
    else:
        P_E_grade=-10
    #50日极值差
    #
    #计算分数
    #计算现价
    if float(last_price)<=50:
        last_price_grade=10
    else:
        last_price_grade=1/(float(last_price)-49)*10
    #历史均值比分数
    if his_compare_now >0 and his_compare_now<3:
        his_compare_now_grade=-1/his_compare_now
    elif his_compare_now>3:
        his_compare_now_grade=(1-2/his_compare_now)*20
    else:
        his_compare_now_grade=0
    #300日均值比分数
    if now_compare_300 >0 and now_compare_300<3:
        now_compare_300_grade=-1/now_compare_300
    elif now_compare_300>3:
        now_compare_300_grade=(1-2/now_compare_300)*20
    else:
        now_compare_300_grade=0
    #60日振幅合格计数分数
    if count_price_60 >40:
        count_price_60_grade=((count_price_60-39)/2)**2
    else:
        count_price_60_grade=0
    #60日均值振幅合格计数分数
    if count_mean_60 >30:
        count_mean_60_grade=((count_mean_60-29)/3)**2
    else:
        count_mean_60_grade=0
    #70 日方差分数
    mean_70 = 0
    if len(price_list)>=70:
        mean_70 = price_list[0:70].mean()
        mean_70_grade=0
        for num in price_list[0:70]:
            mean_70_grade=mean_70_grade+math.fabs(num-mean_70)/mean_70
        mean_70_grade=100/mean_70_grade
        logging.info('name:{},mean_70_grade:{}'.format(stock_df.loc[1,'stock_name'],mean_70_grade))
    else:
        mean_70_grade = 0
    #现价/70日均价
    now_compare_70_grade = 0
    now_compare_70 = 0
    if mean_70!=0:
        now_compare_70 = last_price/mean_70
        if now_compare_70 > 2:
            now_compare_70_grade = -30
        elif now_compare_70 > 1.5:
            now_compare_70_grade = -15
        elif now_compare_70 > 1.3:
            now_compare_70_grade = -5
        else:
            now_compare_70_grade = 10
    #换手grade
    if hsts >=10:
        hsts_grade = 10
    else:
        hsts_grade = 0
    #市值grade
    if market_value <= 70:
        market_grade = 10
    else:
        market_grade = 0
    #grade=last_price_grade+his_compare_now_grade+now_compare_300_grade+count_price_60_grade+count_mean_60_grade+P_E_grade
    
    grade = mean_70_grade + now_compare_70_grade + P_E_grade + hsts_grade + market_grade
    logging.info('name:{},grade:{},P_E_grade:{}'.format(stock_df.loc[1,'stock_name'],grade,P_E_grade))
    #print('market_value:',market_value)
    #print('grade:',grade,'last_price_grade:',last_price_grade,'his_compare_now_grade:',his_compare_now_grade,'now_compare_300_grade:',\
    #      now_compare_300_grade,'count_price_60_grade:',count_price_60_grade,'count_mean_60_grade:',count_mean_60_grade,'P_E_grade:',P_E_grade)
    try:
        cursor = db.cursor()
        sql="replace into compute_result(trade_code,trade_date,stock_id,stock_name,现价,grade,平稳率_70,P_E,P_B,现价70日价比,30日价位比,历史均值,历史均值比,300日均值,\
            300日均值比,60日振幅合格计数,60日均值,60日均值振幅合格计数,历史除极交量均值,历史除极换手均值,历史最高,历史最低,100日达标换手天数,市值流通,hsts_grade,P_E_grade,now_compare_70_grade,market_grade,fantan_grade) \
            values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}','{18}','{19}','{20}','{21}','{22}','{23}','{24}','{25}','{26}','{27}','{28}')\
            ".format(trade_code,date,stock_df.loc[1,'stock_id'],stock_df.loc[1,'stock_name'],last_price,grade,mean_70_grade,stock_df['P_E'][0],
                     stock_df['P_B'][0],now_compare_70,diff_pri_30,his_mean,his_compare_now,mean_300,now_compare_300,count_price_60,
                     mean_60,count_mean_60,his_trade_amount_mean,his_turnover_mean,his_high,his_low,hsts,market_value,hsts_grade,P_E_grade,now_compare_70_grade,market_grade,fantan_grade)
        cursor.execute(sql)
        db.commit()
        print('存储完成')
        logging.info('存储完成:id:{},name:{}'.format(stock_df.loc[1,'stock_id'],stock_df.loc[1,'stock_name']))
    except Exception as err:
        db.rollback()
        print('存储失败:',err)
        logging.error('存储失败:id:{},name:{}\n{}'.format(stock_df.loc[1,'stock_id'],stock_df.loc[1,'stock_name'],err))
    cursor.close()

def delete():
    db = pymysql.connect("localhost","root","Zzl08382020","stockdb" )
    cursor = db.cursor()#使用cursor()方法获取用于执行SQL语句的游标
    try:
        sql="delete from compute_result"
        cursor.execute(sql)
        db.commit()
        print('删除成功。')
        return 1
    except Exception as err:
        db.rollback()
        print('删除失败：',err)
        return 0
def date_list(datestart,dateend):
    datestart = datetime.datetime.strptime(datestart,'%Y-%m-%d')  
    dateend = datetime.datetime.strptime(dateend,'%Y-%m-%d')  
    riqi_list = [] 
    while datestart < dateend:
        datestart += datetime.timedelta(days=1)
        qu = datestart.strftime('%Y-%m-%d')
        riqi_list.append(qu)
    return riqi_list
def select_info(table):
    db = pymysql.connect("localhost","root","Zzl08382020","stockdb" )
    cursor = db.cursor()
    sql="select stock_id from stock_informations where h_table={}".format(table)
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    #print(stock_id_list)
    cursor.close()
    return stock_id_list

def main(page,date):
    db = pymysql.connect("localhost","root","Zzl08382020","stockdb" )
    cursor = db.cursor()
##    flag=1
##    flag=delete(db)
##    if flag==0:
##        return 0
    #print('flag1')
    if not date:
        date=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sql="select stock_id from stock_history_trade{}".format(page)
    id_df=get_df_from_db(sql,db)
    id_set=set(id_df['stock_id'])
    #print(id_set)
    #print(len(id_set))
    #id_set={'002108',}
    for ids in id_set:
        #ids='603105'
        #print('ids:',ids)
        sql="select  * from stock_history_trade{} where stock_id='{}' and trade_date <= '{}' and close_price != '-'".format(page,ids,date)
        stock_df=get_df_from_db(sql,db)
        date_str=re.sub('-','',date[0:10])
        #print('stock_df',stock_df)
        if (len(stock_df))>0:
            trade_code=date_str+ids
            fantan_grade = compute_fantan(db,cursor,page,ids,date,trade_code)
            compute(stock_df,db,trade_code,date,fantan_grade)
            
    db.close()
def main2(date_list,ids):
    db = pymysql.connect("localhost","root","Zzl08382020","stockdb" )
    cursor = db.cursor()
    sql = "select h_table from stock_informations where stock_id = '{}'".format(ids)
    cursor.execute(sql)
    table_list = cursor.fetchall()
    if len(table_list) == 1:
        page=table_list[0][0]
        #print('table:',table_list)
    else:
        print('{} not fund table.'.format(ids))
        return 0
    for date in date_list:
        #ids='603105'
        #print('ids:',ids)
        sql="select  * from stock_history_trade{} where stock_id='{}' and trade_date <= '{}' and close_price != '-'".format(page,ids,date)
        stock_df=get_df_from_db(sql,db)
        date_str=re.sub('-','',date[0:10])
        #print('stock_df',stock_df)
        if (len(stock_df))>0:
            trade_code=date_str+ids
            fantan_grade = compute_fantan(db,cursor,page,ids,date,trade_code)
            #compute(stock_df,db,trade_code,date,fantan_grade)
    db.close()
def compute_fantan(db,cursor,table,ids,date,trade_code):
    sql="select  * from stock_history_trade{} where stock_id='{}' and trade_date <= '{}' and close_price != '-'".format(table,ids,date)
    stock_df=get_df_from_db(sql,db)
    if (len(stock_df))>30:
        stock_df=stock_df.sort_values(axis=0, ascending=False, by='trade_date',na_position='last')
        stock_df.reset_index(inplace=True)
        #print(stock_df)
        s_name = stock_df.loc[1,'stock_name']
        price_list = stock_df['close_price'][0:30]
        last_5 = stock_df['close_price'][0:5]
        mean_5 = last_5.mean()
        last_close_price = stock_df['close_price'][0]
        last_open_price = stock_df['open_price'][0]
        fantan_grade = 0
        if price_list.max()  / price_list.min() >= 1.4:
            fantan_grade += 1000
            print('check_100:',last_open_price,last_close_price,mean_5)
            if last_open_price <= mean_5 and last_close_price <= mean_5 :#and stock_df['close_price'][0:5].max() / price_list.max() >0.98:
                fantan_grade += 100
                fantan_grade += (1 - last_close_price/mean_5) * 20 #5日均线穿深
                #fantan_grade += (0.88- last_5.min()/last_5.max()) * 20 #涨幅强劲度
            if stock_df['close_price'][0:5].max() / stock_df['close_price'][0:30].max() >0.98:
                #print("stock_df['high_price'][0:5].max():",stock_df['high_price'][0:5])
                #print("stock_df['high_price'][0:30].max():",stock_df['high_price'][0:30])
                fantan_grade += 10
        try:
            cursor = db.cursor()
            sql="replace into compute_result(trade_code,trade_date,stock_id,stock_name,fantan_grade) \
                values('{0}','{1}','{2}','{3}','{4}')\
                ".format(trade_code,date,ids,s_name,fantan_grade)
            cursor.execute(sql)
            db.commit()
            print('存储完成:trade_code:{},name:{}'.format(trade_code,s_name))
            logging.info('存储完成:id:{},name:{}'.format(ids,s_name))
        except Exception as err:
            db.rollback()
            print('存储失败:',err)
            logging.error('存储失败:id:{},name:{}\n{}'.format(ids,s_name,err))
        cursor.close()
        return fantan_grade
    else:
        return 0
def compute_fantan_history(table,star_day,end_day):
    db = pymysql.connect("localhost","root","Zzl08382020","stockdb" )
    cursor = db.cursor()
    sql="select distinct stock_id from stock_history_trade{}".format(table)
    cursor.execute(sql)
    id_list = cursor.fetchall()
    #id_list = [('603877','')]
    #print('id_list:',id_list)
    for ids in id_list:
        ids = ids[0]
        sql="select  * from stock_history_trade{} where stock_id='{}' and trade_date >= '{}' and trade_date <= '{}' and close_price != '-'".format(table,ids,star_day,end_day)
        stock_df=get_df_from_db(sql,db)
        #print('stock_df:',stock_df)
        len_df = len(stock_df)
        if (len_df)>30:
            stock_df['5'] = stock_df['close_price'].rolling(5).mean()
            stock_df=stock_df.sort_values(axis=0, ascending=False, by='trade_date',na_position='last')
            stock_df.reset_index(inplace=True)
            print('stock_df:',stock_df)
            s_name = stock_df.loc[1,'stock_name']
            for i in range(len_df):
                date = stock_df.loc[i,'trade_date']
                date_str=re.sub('-','',str(date)[0:10])
                trade_code = date_str + ids
                mean_5 = stock_df.loc[i,'5']
                last_close_price = stock_df['close_price'][i]
                last_open_price = stock_df['open_price'][i]
                fantan_grade = 0
                if last_close_price < mean_5 and last_open_price < mean_5:
                    fantan_grade += 1000
                    price_list = stock_df['close_price'][i:i+30]
                    last_5 = stock_df['close_price'][i:i+5]
                    print('trade_code:',trade_code,'price_list.max():',price_list.max(),'price_list.min():',price_list.min(),'result:',price_list.max()  / price_list.min())
                    if price_list.max()  / price_list.min() >= 1.2:
                        fantan_grade += 100
                        fantan_grade += (1 - last_close_price/mean_5) * 20 #5日均线穿深
                        #fantan_grade += (0.88- last_5.min()/last_5.max()) * 20 #涨幅强劲度
                    if stock_df['close_price'][i:i+5].max() / stock_df['close_price'][i:i+30].max() >0.98:
                            fantan_grade += 10
                print('fantan_grade:',fantan_grade)
                try:
                    cursor = db.cursor()
                    sql="replace into compute_result(trade_code,trade_date,stock_id,stock_name,fantan_grade) \
                        values('{0}','{1}','{2}','{3}','{4}')\
                        ".format(trade_code,date,ids,s_name,fantan_grade)
                    cursor.execute(sql)
                    db.commit()
                    print('存储完成:trade_code:{},name:{}'.format(trade_code,s_name))
                    logging.info('存储完成:id:{},name:{}'.format(ids,s_name))
                except Exception as err:
                    db.rollback()
                    print('存储失败:',err)
                    logging.error('存储失败:id:{},name:{}\n{}'.format(ids,s_name,err))
    cursor.close()
    
if __name__ == "__main__":
    #flag=delete()
    date=None#'2020-08-20'
    p = Pool(8)
    for i in range(1,11):
        p.apply_async(main, args=(str(i),date,))
##    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
    
##    main('8',date)
    
##    for i in range(1,32):
##        date = '2019-5-'+str(i)
##        main2('1',date)
    
##    date_list = date_list('2020-05-01','2020-08-18')#['2020-08-19','2020-08-18']
##    ids_list = select_info('5')#['603105','600295','600166','600749']
##    #ids = '603105'
##    p = Pool(8)
##    for ids in ids_list:
##        p.apply_async(main2, args=(date_list,ids[0],))
####        main2(date_list,ids[0])
##    print('Waiting for all subprocesses done...')
##    p.close()
##    p.join()
##    print('All subprocesses done.')


##    #ids = '603105'
##    p = Pool(8)
##    for tab in range(1,11):
##        p.apply_async(compute_fantan_history, args=(str(tab),'2020-01-01','2020-08-28',))
##    print('Waiting for all subprocesses done...')
##    p.close()
##    p.join()
##    print('All subprocesses done.')
##
##    compute_fantan_history(4,'2020-07-01','2020-08-24')
    
##"localhost","root","Zzl08382020","stockdb" 
