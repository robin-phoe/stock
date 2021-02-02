



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
logging.basicConfig(level=logging.DEBUG,filename='stock_day_trade1.log',filemode='w',
                    format='%(asctime)s-%(levelname)5s: %(message)s')
with open('stock_day_trade1.log','r') as f:
    f.read()
    print(f)
db = pymysql.connect("localhost","root","Zzl08382020","stockdb" )
cursor = db.cursor()
count=0
#用get方法访问服务器并提取页面数据
def getHtml(cmd,page):
    url = "http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?cb=jQuery112406115645482397511_1542356447436&type=CT&token=4f1862fc3b5e77c150a2b985b12db0fd&sty=FCOIATC&js=(%7Bdata%3A%5B(x)%5D%2CrecordsFiltered%3A(tot)%7D)&cmd="+cmd+"&st=(ChangePercent)&sr=-1&p="+str(page)+"&ps=20"
    #url= "http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?cb=jQuery112404462275420342996_1542343049719&type=CT&token=4f1862fc3b5e77c150a2b985b12db0fd&sty=FCOIATC&js=(%7Bdata%3A%5B(x)%5D%2CrecordsFiltered%3A(tot)%7D)&cmd=C.2&st=(ChangePercent)&sr=-1&p=1&ps=20&_=1542343050897"
    #url="http://www.zhihu.com/explore"
    #print(url)
    header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36'}
    response = requests.get(url,headers=header)
    text=response.text
    print('text:',text)
    pat = "data:\[(.*?)\]"
    page_list = re.compile(pat,re.S).findall(text)
##    data_l=page_list[0].split('","')
##    for data in data_l:
##        data=data.replace('"',"")
##        #print('page_list\n',page_list[0])
##        data_list=data.split(",")
##        print('data_list\n',data_list)
##        if len(data_list)>1:
##            try:
##                sql="insert into stock_day_trade(trade_code,stock_name,stock_id,trade_date,close_price,increase,open_price,close_yesterday,turnover_rate,P_B,high_price,low_price,trade_amount,trade_money) \
##                    values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}')\
##                    ".format(data_list[1],data_list[2],data_list[1],data_list[24],data_list[3],data_list[8],data_list[11],data_list[12],data_list[15],data_list[17],data_list[9],data_list[10],data_list[6],data_list[7])
##                cursor.execute(sql)
##                db.commit()
##                print('存储完成')
##                logging.info('存储完成:cmd:{},page:{},id:{},name:{}'.format(cmd,page,data_list[1],data_list[2]))
##            except Exception as err:
##                db.rollback()
##                print('存储失败:',err)
##                logging.error('存储失败:cmd:{},page:{},id:{},name:{}\n{}\n{}'.format(cmd,page,data_list[1],data_list[2],data_list,err))
    #print('data:',data)
    return page_list

#获取单个页面股票数据
def getOnePageStock(cmd,page,i):
    global count
    page_list = getHtml(cmd,page)
    
##    datas = data[0].split('","')
##    stocks = []
##    for i in range(len(datas)):
##        stock = datas[i].replace('"',"").split(",")
##        stocks.append(stock)
##    #print('stock:',stock)
    print('page_list:',page_list)
    data_l=page_list[0].split('","')
    for data in data_l:
        data=data.replace('"',"")
        #print('page_list\n',page_list[0])
        data_list=data.split(",")
        #print('data_list\n',data_list)
        data_str=re.sub('-','',data_list[24][0:10])
        trade_code=data_str+data_list[1]
        print(trade_code)
        if len(data_list)>1 and data_list[3] != '-':
            try:
                sql="insert into stock_day_trade1(trade_code,stock_name,stock_id,trade_date,close_price,increase,open_price,close_yesterday,turnover_rate,P_E,P_B,high_price,low_price,trade_amount,trade_money,exchange) \
                    values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}')\
                    ".format(trade_code,data_list[2],data_list[1],data_list[24],data_list[3],data_list[8],data_list[11],data_list[12],data_list[15],data_list[16],data_list[17],data_list[9],data_list[10],data_list[6],data_list[7],i)
                cursor.execute(sql)
                db.commit()
                print('存储完成')
                print('存储完成:cmd:{},page:{},id:{},name:{}'.format(cmd,page,data_list[1],data_list[2]))
                logging.info('存储完成:cmd:{},page:{},id:{},name:{}'.format(cmd,page,data_list[1],data_list[2]))
                count += 1
                print('count:',count)
            except Exception as err:
                db.rollback()
                print('存储失败:',err)
                logging.error('存储失败:cmd:{},page:{},id:{},name:{}\n{}\n{}'.format(cmd,page,data_list[1],data_list[2],data_list,err))
                print('存储失败:cmd:{},page:{},id:{},name:{}\n{}\n{}'.format(cmd,page,data_list[1],data_list[2],data_list,err))
##            try:
##                h_table=count//400+1
##                print('h_table:',h_table,count)
##                sql="insert into stock_informations(stock_name,stock_id,exchange,h_table) \
##                    values('{0}','{1}','{2}','{3}')\
##                    ".format(data_list[2],data_list[1],i,h_table)
##                cursor.execute(sql)
##                db.commit()
##                print('存储完成')
##                count+=1
##            except Exception as err:
##                db.rollback()
##                print('存储失败:',err)
    return data_list
def to_history():
    for num in range(1,11):
        try:
            sql="REPLACE into stock_history_trade{0}(trade_code,stock_name,\
                stock_id,trade_date,close_price,increase,open_price,turnover_rate,P_E,P_B,high_price,low_price,trade_amount,trade_money)\
                select trade_code,stock_name,\
                stock_id,trade_date,close_price,increase,open_price,turnover_rate,P_E,P_B,high_price,low_price,trade_amount,trade_money \
                from stock_day_trade1 \
                where stock_id in (select stock_id from stock_history_trade{0})".format(str(num))
            cursor.execute(sql)
            db.commit()
            print('转存成功！',num)
        except Exception as err:
            db.rollback()
            print('转存失败：',err)
            logging.info('转存失败：{}'.format(err))
            return 0
def to_info():
    try:
        sql="UPDATE stock_informations I left join stock_day_trade1 D ON I.stock_id = D.stock_id  \
            set I.发行量 = (D.trade_amount / (D.turnover_rate/100))"
        cursor.execute(sql)
        db.commit()
        print('转存fxl成功！')
    except Exception as err:
        db.rollback()
        print('转存fxl失败：',err)
        logging.info('转存fxl失败：{}'.format(err))
        return 0  
def main():
    try:
        sql="delete from stock_day_trade1"
        cursor.execute(sql)
        db.commit()
    except Exception as err:
        db.rollback()
        print('删除失败：',err)
        return 0
    
##    cmd = {
##        "上证A股":"C.2",
##        "上证指数":"C.1",
##        "深圳指数":"C.5",
##        "沪深A股":"C._A",
##        "深圳A股":"C._SZAME",
##        "新股":"C.BK05011",
##        "中小板":"C.13",
##        "创业板":"C.80"
##    }
    cmd = {
        "上证A股":"C.2",
        "深圳A股":"C._SZAME"
    }
    
    for i in cmd.keys():
        page = 1
        stocks = getOnePageStock(cmd[i],page,i)
        #自动爬取多页，并在结束时停止
        while True: 
            page +=1
            try:
                if getHtml(cmd[i],page)!= getHtml(cmd[i],page-1):
                    #stocks.extend(getOnePageStock(cmd[i],page))
                    getOnePageStock(cmd[i],page,i)
                    #print('stocks:',stocks)
                    print(i+"已加载第"+str(page)+"页")
                else:
                    break
            except Exception as err:
                print('get page ERROR:{}\n{}\n{}',format(cmd[i],page,err))
                logging.error('get page ERROR:{}\n{}\n{}',format(cmd[i],page,err))
        #df = pd.DataFrame(stocks)
        #提取主要数据/提取全部数据
        #df.drop([0,14,15,16,17,18,19,20,21,22,23,25],axis=1,inplace=True)
        #columns = {1:"代码",2:"名称",3:"最新价格",4:"涨跌额",5:"涨跌幅",6:"成交量",7:"成交额",8:"振幅",9:"最高",10:"最低",11:"今开",12:"昨收",13:"量比",24:"时间"}
        #df.rename(columns = columns,inplace=True)
        #df.to_excel("股票{}.xls".format(i))
        #print("已保存"+i+".xls")
    to_history()
    to_info()
main()
#to_info()
#to_history()
