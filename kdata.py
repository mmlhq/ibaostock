### K线数据

import baostock as bs
import pandas as pd
import time

from datetime import date,datetime,timedelta
import pymysql

#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

# Rock64的连接
cnx = pymysql.connect(host='192.168.1.85',user='lihq',password='lihq8087*)*&',database='tdx',charset='utf8')
# 写入本机
# cnx = pymysql.connect(user='root',password='lihq8087*)*&',host='127.0.0.1',database='tdx')

cur_code = cnx.cursor()
cur_insert = cnx.cursor()
cur_date = cnx.cursor()

# 选择股票代码
query_code = ("SELECT code,market FROM tdx.index")
cur_code.execute(query_code)
code_lists = cur_code.fetchall()

query_date = "SELECT Date from tdx.recordtime WHERE ID = 'KDATE'"
cur_date.execute(query_date)
s_date = cur_date.fetchone()
str_date = str(s_date)
e_date = time.strftime("%Y-%m-%d")

q_insert =(("INSERT INTO kdata (date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST)"
            " values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"))

for code in code_lists:
    index = code[1]+'.'+code[0]

    #### 获取历史K线数据 ####
    rs = bs.query_history_k_data_plus(index,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
        start_date='%s'%s_date, end_date='%s'%e_date,
        frequency="d", adjustflag="3") #frequency="d"取日k线，adjustflag="3"默认不复权

    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        value_row = rs.get_row_data()
        if value_row[1] == 'sh.000001':
            value_row[1] = '999999'
        value_row[1] = value_row[1][-6:]
        value_row = tuple(value_row)
        if value_row[7] == '0' or value_row[7] == '':
            continue
        cur_insert.execute(q_insert,value_row)
        cnx.commit()
        q_updatedate = "UPDATE tdx.index SET klatest_date = '%s' WHERE code = '%s'" % (value_row[0],value_row[1])
        cur_date.execute(q_updatedate)
        cnx.commit()

s_date = e_date
q_updatedate = "UPDATE recordtime SET Date = '%s' WHERE ID = 'KDATE'"%s_date
cur_date.execute(q_updatedate)
cnx.commit()

cur_code.close()
cur_insert.close()
cur_date.close()
cnx.close()

#### 登出系统 ####
bs.logout()

print("执行完成")

