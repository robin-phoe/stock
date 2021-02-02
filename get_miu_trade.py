
'''
数据来源：东方财富网-行情中心
http://quote.eastmoney.com/center
'''
#coding=utf-8
import requests
import re
import pymysql
#import pandas as pd
import logging
import datetime
import json
from multiprocessing import Pool

logging.basicConfig(level=logging.DEBUG,filename='stock_minu_trade.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')

db = pymysql.connect("localhost","root","Zzl08382020","stockdb" )
cursor = db.cursor()
count=0

def get_page_contant(page,stock_id):
    if stock_id[0:2] == '60':
        url = "http://push2ex.eastmoney.com/getStockFenShi?pagesize=144&ut=7eea3edcaed734bea9cbfc24409ed989&dpt=wzfscj&cb=jQuery112303757051884710463_1597157301810&pageindex={0}&id={1}1&sort=1&ft=1&code={1}&market=1&_=1597157301811".format(page,stock_id)
    elif stock_id[0:2] == '00':
        url = "http://push2ex.eastmoney.com/getStockFenShi?pagesize=144&ut=7eea3edcaed734bea9cbfc24409ed989&dpt=wzfscj&cb=jQuery112303757051884710463_1597157301810&pageindex={0}&id={1}2&sort=1&ft=1&code={1}&market=0&_=1597157301811".format(page, stock_id)
    #print('url:',url)
    header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
    response = requests.get(url,headers=header)
    text=response.text
    #print('text1:',text)
    result=re.findall('\[.*?\]',text)
    # print('result:',result)
    if len(result)==1:
        text=result[0]
    else:
        text = ''
    #print('text2:',text)
    return text
def get_stock_contant(stock_id):
    last_page=''
    stock_contant=[]
    page=0
    while True:
        text=get_page_contant(page,stock_id)
        if text==last_page or text == '':
            break
        last_page=text
        js_list=json.loads(text)
        #print('js_list:',js_list)
        stock_contant.extend(js_list)
        #print('stock_contant:',stock_contant)
        page+=1
        #print('page',page)
    return stock_contant
def select_stock_id(table):
    sql="select stock_id from stock_informations where h_table={}".format(table)
    cursor.execute(sql)
    stock_id_list = cursor.fetchall()
    #print(stock_id_list)
    return stock_id_list
def main(table,date):
    count=0
    if not date:
        date=datetime.datetime.now().strftime('%Y%m%d')
    #for table in range(1,10):
    stock_id_list=select_stock_id(table)
    # stock_id_list=[('000088',)]
    #print('stock_id_list',stock_id_list)
    table_contant=[]
    for stock_id in stock_id_list:
        stock_contant = get_stock_contant(stock_id[0])
        stock_contant = json.dumps(stock_contant)
        print('stock_contant:',type(stock_contant))
        trade_code=date+stock_id[0]
        try:
        #if 1:
            sql="replace into stock_miu_trade{}(trade_code,stock_id,data,trade_date) \
                values('{}','{}','{}','{}')".format(str(table),trade_code,stock_id[0],str(stock_contant),date)
            #print('tuple(val):',val)
            #print('tuple(sql):',sql)
            cursor.execute(sql)
            db.commit()
            print('存储完成')
            logging.info('存储完成:id:{}'.format(stock_id[0]))
        except Exception as err:
        #else:
            db.rollback()
            print('存储失败:',err)
            logging.error('存储失败:id:{}\n{}'.format(stock_id[0],err))
        count+=1
        print('count:',count)
if __name__ == "__main__":
    #flag=delete()
    date=None#'20200824'#None#'20200717'
    p = Pool(8)
    for i in range(1,11):
        print('i:',i)
        p.apply_async(main, args=(str(i),date,))
##    p.apply_async(main, args=('1',date,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
    # main('5',date)
#to_history()
