import baostock as bs
import pandas as pd

from datetime import date,datetime,timedelta
import mysql.connector

#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

cnx = mysql.connector.connect(user='root',password='',host='192.168.1.85',database='tdx')
cur_code = cnx.cursor(buffered=True)
cur_insert = cnx.cursor(buffered=True)
query_code = ("SELECT code,market FROM tdx.index")
cur_code.execute(query_code)
code_lists = cur_code.fetchall()

q_insert =(("INSERT INTO kdata (date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST)"
            " values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"))

for code in code_lists:
    index = code[1]+'.'+code[0]

    #### 获取历史K线数据 ####
    # 详细指标参数，参见“历史行情指标参数”章节
    rs = bs.query_history_k_data_plus(index,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
        start_date='2020-03-30', end_date='2020-03-30',
        frequency="d", adjustflag="3") #frequency="d"取日k线，adjustflag="3"默认不复权

    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        value_row = tuple(rs.get_row_data())
        if(int(value_row[7]) == 0):
            continue
        cur_insert.execute(q_insert,value_row)
        cnx.commit()
        
cnx.disconnect()

#### 登出系统 ####
bs.logout()

print("执行完成")

