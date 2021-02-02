# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 00:55:04 2019
 
@author: Administrator
"""
#from appium import webdriver 
#from selenium.webdriver.support.ui import WebDriverWait 
#from selenium .webdriver.common.by import By  
#from selenium.webdriver.support import expected_conditions as EC
import uiautomator2 as u2
import time
from time import sleep
import datetime
import os
import pandas as pd
import xlwt
import pymysql
import re

workbook=xlwt.Workbook(encoding='utf-8')
worksheet= workbook.add_sheet('sheet1')
db = pymysql.connect("localhost","root","Zzl08382020","stockdb" )
cursor = db.cursor()

def init_u2():
    d=u2.connect_usb()
    d.wait_activity(".ApiDemos", timeout=2)
    return d
def load_all_stock(d):

    print(d.info)
    d.app_start("cn.com.sina.finance")
    d(text="行情").click()
    sleep(2)
    d.swipe(360, 1694, 360,96)
    sleep(3)
    d.swipe(360, 1645, 360,800)
    sleep(3)
    d(text="查看更多").click()
    sleep(4)
def read_information(amount,cursor,d):
    cup='init'
    for a in range(amount):
        print(a)
        view_count=d(className="android.widget.TextView").count
        print(view_count)
    ##    for i in range(14,18):
    ##        if d(className="android.widget.TextView")[i].wait(timeout=1):
        #name_el=d(className="android.widget.TextView")[14]
        name_el=d(resourceId="cn.com.sina.finance:id/tv_cn_rank_name")[0]
        name=name_el.get_text()
        print(name)
        sql="select stock_name from stock_informations where stock_name='{}'".format(name)
        cursor.execute(sql)
        results = cursor.fetchall()
        print('sql results:',results)
        if len(results)==0:
            if name!=cup:
                cup=name
                #worksheet.write(row,0,name)
                id_num=d(className="android.widget.TextView")[15].get_text()
                print(id_num)
                #worksheet.write(row,1,id_num)
                #d(className="android.widget.TextView")[14].click()
                d(text=name).click()
                sleep(3)
                head_name_el=d(resourceId="cn.com.sina.finance:id/StockDetail_P_Title_Name")
                if head_name_el.count!=0:
                    head_name=head_name_el.get_text()
                else:
                    continue
                print('head_name:',head_name,'name:',name)
                if head_name!=name:
                    print('名字不匹配')
                    d.xpath('//*[@resource-id="cn.com.sina.finance:id/rl_iv_back"]').click()
                    continue
                if name[0]=='N':
                    name=name[1:]
                d.swipe(0, 1488, 0,226)
                sleep(2.5)
                d(text="F10").click()
                sleep(2 )
                if d(text="个股诊断综合评价").count>0:
                    d.xpath('//*[@resource-id="cn.com.sina.finance:id/TitleBar1_Text_Close"]').click()
                    d.xpath('//*[@resource-id="cn.com.sina.finance:id/rl_iv_back"]').click()
                    continue
                d.swipe(0, 800, 0,280)
                sleep(2)
                tv_na=d(resourceId="cn.com.sina.finance:id/tv_name").count
                related=''
                for i in range(tv_na):
                    related+=d(resourceId="cn.com.sina.finance:id/tv_name")[i].get_text()+'。'
                try:
                    business=d.xpath('//*[@resource-id="cn.com.sina.finance:id/tv_industry_name"]').get_text()
                except:
                    business='null'
                
                main_business_el=d(resourceId="cn.com.sina.finance:id/tv_fare_area")
                if main_business_el.count!=0:
                    main_business=d.xpath('//*[@resource-id="cn.com.sina.finance:id/tv_fare_area"]').get_text()
                else:
                    d.swipe(0, 956, 0,650)
                    main_business_el=d(resourceId="cn.com.sina.finance:id/tv_fare_area")
                    if main_business_el.count!=0:
                        main_business=main_business_el.get_text()
                    else:
                        main_business='null'
                print(business,main_business,related)
                sql="insert into stock_informations(stock_name,stock_id,business,main_business,related) \
                values('{}','{}','{}','{}','{}')".format(name,d_num,business,main_business,related)
                try:
                    cursor.execute(sql)
                    db.commit()
                except:
                    db.rollback()
                d.xpath('//*[@resource-id="cn.com.sina.finance:id/rl_iv_back"]').click()
                #d.press.back()
                d.swipe(0, 956, 0,761)
                #workbook.save('stock_id.xls')
                #row+=1
                
            else:
                d.swipe(0, 956, 0,761)
                sleep(2)
        else:
            print('已存在')
            d.swipe(0, 756, 0,761)
def read_day_data(cursor,d,id_num):
##    print(d(resourceId="cn.com.sina.finance:id/tv_optional_capital_col_data").count)
##    data_el=d(resourceId="cn.com.sina.finance:id/tv_optional_capital_col_data")
##    counts=data_el.count
##    for i in range(counts):
##        print(data_el[i].get_text())
##    for i in range(4):
##        counts=data_el.count
##        print('counts',counts)
##        print('===')
##        counts=data_el.count
##        for i in range(counts):
##            print(data_el[i].get_text())
##        d.swipe(1000, 627, 360,627)
##    d.swipe(0, 1502.4, 0,558)
##    id_num=d(resourceId="cn.com.sina.finance:id/tv_cn_rank_symbol")
##    print('id_num.count:',id_num.count)
##    for i in range(id_num.count):
##        print('ids',id_num[i].get_text())
##    for i in range(20):
##        sleep(1)
##        d.swipe(0, 956.9, 0,761)
    date=datetime.datetime.now().strftime("%Y%m%d")
    date='20200730'
    trade_code=date+id_num.get_text()
    sql="select trade_code from stock_day_trade where trade_code='{}'".format(trade_code)
    cursor.execute(sql)
    results = cursor.fetchall()
    if len(results)!=0:
        print(id_num.get_text(),'已存在')
        return 0
    else:
        id_num.click()
        sleep(2)
    if d(resourceId="cn.com.sina.finance:id/StockDetail_P_Title_Name").count==0:
        return 0
    name=d(resourceId="cn.com.sina.finance:id/StockDetail_P_Title_Name").get_text()
    if name[0]=='N':
        name=name[1:]
    print(name)
    id_num=d(resourceId="cn.com.sina.finance:id/StockDetail_P_Title_Code").get_text()
    print(id_num)
    close_price=d(resourceId="cn.com.sina.finance:id/dp_price").get_text()
    print(close_price)
    increase=d(resourceId="cn.com.sina.finance:id/dp_diff_percent").get_text()
    increase=float(re.findall('\+.*?\+(.*?)%',str(increase))[0])
    print(increase)
    open_price=d(resourceId="cn.com.sina.finance:id/dp_v_1").get_text()
    print(open_price)
    
    close_yesterday=d(resourceId="cn.com.sina.finance:id/dp_v_4").get_text()
    print(close_yesterday)
    turnover_rate=d(resourceId="cn.com.sina.finance:id/dp_v_7").get_text()
    turnover_rate=float(turnover_rate[0:-1])
    print(turnover_rate)
    high_price=d(resourceId="cn.com.sina.finance:id/dp_v_2").get_text()
    print('high_price:',high_price)
    low_price=d(resourceId="cn.com.sina.finance:id/dp_v_5").get_text()
    print(low_price)
    P_E=d(resourceId="cn.com.sina.finance:id/dp_v_8").get_text()
    print(P_E)
    trade_amount=d(resourceId="cn.com.sina.finance:id/dp_v_3").get_text()
    if trade_amount[-2:]=='万股' :
        trade_amount=float(trade_amount[0:-2])*10000
    elif trade_amount[-1:]=='万':
        trade_amount=float(trade_amount[0:-1])*10000
    print(trade_amount)
    trade_money=d(resourceId="cn.com.sina.finance:id/dp_v_6").get_text()
##    res=re.findall('(.*?)(\D)',trade_money)
    if trade_money[-1:]=='亿':
        trade_money=float(trade_money[:-1])*10000
    elif trade_money[-1:]=='万':
        trade_money=float(trade_money[:-1])
    print(trade_money)
    market_value=d(resourceId="cn.com.sina.finance:id/dp_v_9").get_text()
    #res=re.findall('(.*?)(\D)',market_value)
    if market_value[-1:]=='亿':
        market_value=float(market_value[:-1])*10000
    print(market_value)
    sql="insert into stock_day_trade(trade_code,stock_name,stock_id,trade_date,close_price,increase,open_price,close_yesterday,turnover_rate,P_E,high_price,low_price,trade_amount,trade_money,market_value) \
        values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}')\
        ".format(trade_code,name,id_num,date,close_price,increase,open_price,close_yesterday,turnover_rate,P_E,high_price,low_price,trade_amount,trade_money,market_value)
    try:
        cursor.execute(sql)
        db.commit()
        print('存储完成')
    except Exception as err:
        print('SAVE ERROR:',err)
        db.rollback()
    d.xpath('//*[@resource-id="cn.com.sina.finance:id/rl_iv_back"]').click()
    return 1
    #d.press.back()
    
    
def to_info_page(amount,cursor,d):
    counts=1
    for a in range(amount):
        name_el=d(resourceId="cn.com.sina.finance:id/tv_cn_rank_name")[0]
        id_num=d(resourceId="cn.com.sina.finance:id/tv_cn_rank_symbol")[0]
        print('第：',counts,a)
        try:
            ret=read_day_data(cursor,d,id_num)
            if ret==1:
                counts+=1
        except Exception as err:
            print('ERROR:',err)
            d.xpath('//*[@resource-id="cn.com.sina.finance:id/rl_iv_back"]').click()
        d.swipe(0, 956, 0,761)
def db_update(cursor):
    sql="select trade_code from stock_day_trade"
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    for res in results:
        new_key='20200729'+res[0][8:]
        print('new_key:',new_key)
        print('res[0]:',res[0])
        sql="UPDATE stock_day_trade SET trade_code='{}',trade_date='20200729' where trade_code='{}'".format(new_key,res[0])
        try:
            cursor.execute(sql)
            db.commit()
        except Exception as err:
            db.rollback()
            print('ERR:',err)

def read_history_info(cursor):
    #d.drag(30,800 , 100, 800)
    #d.long_click(30,800, 5)
    sourceid={'trade_name':'cn.com.sina.finance:id/StockDetail_P_Title_Name','trade_id':'cn.com.sina.finance:id/StockDetail_P_Title_Code',
     'open_price':'cn.com.sina.finance:id/tv_dayk_hsl_portrait_open','close_price':'cn.com.sina.finance:id/tv_dayk_hsl_portrait_close',
     'high_price':'cn.com.sina.finance:id/tv_dayk_hsl_portrait_high','low_price':'cn.com.sina.finance:id/tv_dayk_hsl_portrait_low',
     'increase':'cn.com.sina.finance:id/tv_dayk_hsl_portrait_change','turnover_rate':'cn.com.sina.finance:id/tv_dayk_hsl_portrait_change',
     'trade_amount':'cn.com.sina.finance:id/tv_dayk_hsl_portrait_volume','trade_date':'cn.com.sina.finance:id/tv_dayk_hsl_portrait_date'}
    
    for i in range(100):
        value_dic={}
        d.touch.down(1000+12.5*i,800 )
        #sleep(1)
        print(i)
        for s_id in sourceid:
            el=d(resourceId=sourceid[s_id])
            print('el_count:',el.count)
            if el.count==0:
                break
            value_dic[s_id]=el.get_text()
            print(value_dic)
            
##        if len(value_dic)==10:
##            value_dic['increase']=float(value_dic['increase'][0:-1])
##            value_dic['turnover_rate']=float(value_dic['turnover_rate'][0:-1])
##            if value_dic['trade_amount'][-2]=='万':
##                value_dic['trade_amount']=float(value_dic['trade_amount'][0:-2])
##            elif value_dic['trade_amount'][-2]=='亿':
##                value_dic['trade_amount']=float(value_dic['trade_amount'][0:-2])*10000
##            else:
##                value_dic['trade_amount']=float(value_dic['trade_amount'][0:-1])/10000
##            value_dic['trade_date']=re.sub('/','',value_dic['trade_date'])
##            value_dic['trade_code']=value_dic['trade_date']+value_dic['trade_id']
##        print('value_dic',value_dic)
        
    #d.touch.down(1045,800 )
    #d.touch.move(100, 800)
if __name__=="__main__":
    print('start')
    d=init_u2()
    #load_all_stock(d)
    #read_information(4000,cursor,d)
    #read_day_data(cursor,d)
    to_info_page(4000,cursor,d)
    #db_update(cursor)
    #read_history_info(cursor)

##    sql="select * from stock_day_trade where trade_date='20200730'"
##    cursor.execute(sql)
##    results = cursor.fetchall()
##    print(results)
    db.close()








##d(text="基金排行").click()
##count=0
####leixing_l=[('混合型','股票型'),('股票型','混合型'),('混合型','指数型')]
####for num in range(3):
####    sleep(1)
####    print(leixing_l[num][0],leixing_l[num][1])
####    for i in range(30):
####        print(i,d(className="android.view.View")[i].get_text())
####    d(text="{}".format(leixing_l[num][0])).click()
####    d(className="android.view.View")[12].click()
####    d.click(45, 380)
####    sleep(1)
####    d(text="{}".format(leixing_l[num][1])).click()
##    ##big_view=d.xpath('//*[@resource-id="root"]/android.view.View[1]/android.view.View[1]/android.view.View[1]/android.view.View[1]/android.view.View[1]/android.view.View[1]/android.view.View[1]')
##    ##big_view.right(className="android.view.View").click()
##
##d(text="混合型").click()
##d(text="股票型").click()
##a=d(className="android.view.View").count
##print(a)
####print(d(className="android.view.View")[21].get_text())
##step=0
##jijin_name={'基金':[],'规模':[],'股票id':[],'价格':[],'涨幅':[]}
##j_df=pd.DataFrame(jijin_name)
##gupiao_name={'基金':[],'股票':[],'股票id':[],'涨幅':[],'比例':[],'规模':[]}
##g_df=pd.DataFrame(gupiao_name)
##
##for i in range(22,1000,6):
##    j_name=d(className="android.view.View")[i].get_text()
##    j_id=d(className="android.view.View")[i+1].get_text()
##    j_price=d(className="android.view.View")[i+2].get_text()
##    j_zhangfu=d(className="android.view.View")[i+3].get_text()
##    print(j_name,j_id,j_price,j_zhangfu)
####    sleep(0.5)
##    d(className="android.view.View")[i].click()
##    
##    sleep(2.5)
##    d.swipe(0, 1497, 0, 589)
##    d(text="基金档案").click()
##    sleep(1.5)
##    guimo=d(className="android.view.View")[16].get_text()
##    print(guimo)
##    j_df.loc[len(j_df)]=[j_name,guimo,j_id,j_price,j_zhangfu]
##    d(text="持仓").click()
##    sleep(1.5)
##    d.swipe(0, 1697, 0, 950)
##    sleep(1.5)
##    a=d(className="android.view.View").count
##    print(a)
##    for r in range(20,a,6):
##        name=d(className="android.view.View")[r].get_text()
##        n_id=d(className="android.view.View")[r+1].get_text()
##        zhangfu=d(className="android.view.View")[r+2].get_text()
##        bili=d(className="android.view.View")[r+3].get_text()
##        g_df.loc[len(g_df)]=[j_name,name,n_id,zhangfu,bili,guimo]
##        print(name,n_id,zhangfu,bili)
##        
##    
##    d(resourceId="com.alipay.mobile.nebula:id/h5_nav_back").click()
##    sleep(1)
##    d(resourceId="com.alipay.mobile.nebula:id/h5_nav_back").click()
####    d.swipe(0, 672, 0, 483)
####    step+=1
####    if step==8:
####    d.swipe(0, 1810, 0, 483)
##    d.swipe(0, 672, 0, 475)
##    print(j_df)
##    print(g_df)
##
##    j_df.to_excel('jijin_data.xls')
##    g_df.to_excel('gupiao_data.xls')
##    count+=1
##    print('count',count)
####big_view.down(className="android.view.View").click()
####sleep(2)
##d.app_stop("com.eg.android.AlipayGphone")
