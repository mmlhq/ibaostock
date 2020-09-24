# 成長能力

import baostock as bs
import pandas as pd
import pymysql
import time
from datetime import datetime

# 登陆网站系统
lg = bs.login()

# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

# 连续本地数据库
# cnx = pymysql.connect("192.168.1.85","lihq","lihq8087*)*&","tdx")
cnx = pymysql.connect(host='192.168.1.85',user='lihq',password='lihq8087*)*&',database='tdx',charset='utf8')

cur_index = cnx.cursor()
cur_index_sql = "select code,market from tdx.index;"
cur_index.execute(cur_index_sql)
code_item_list = cur_index.fetchall() # 财务更新日期元组项的元组（（code,market），....）

cur_forcast = cnx.cursor()
cur_date = cnx.cursor()

current_time = time.localtime(time.time())


query_date = "SELECT Date from tdx.recordtime WHERE ID = 'fcast'"
cur_date.execute(query_date)

s_date = cur_date.fetchone()
start_date = str(s_date[0])
end_date = time.strftime("%Y-%m-%d")

forcast_head = ['code','profitForcastExpPubDate','profitForcastExpStatDate','profitForcastType','profitForcastAbstract','profitForcastChgPctUp','profitForcastChgPctDwn']

count = 0  # 親數據
for code_item in code_item_list:
    code = code_item[1]+"."+code_item[0]

    rs_forcast = bs.query_forecast_report(code=code_item[1] + "." + code_item[0], start_date=start_date,end_date=end_date)

    while (rs_forcast.error_code == '0') & rs_forcast.next():
        growth_list = rs_forcast.get_row_data()

        insert_sql = "INSERT INTO forcast("
        value_sql = "VALUES("
        value_list = []

        for index in range(len(growth_list)):
            if growth_list[index] != '':
                insert_sql += forcast_head[index]+','
                value_sql += "'%s',"
                value_list.append(growth_list[index])

        insert_sql = insert_sql[:-1]
        value_sql = value_sql[:-1]

        insert_sql += ') '
        value_sql += ")"

        tuple_value = tuple(value_list)

        cur_forcast_insert = insert_sql + value_sql

        cur_forcast.execute(cur_forcast_insert%tuple_value)
        cnx.commit()

        count += 1
        growth_list.clear()
    else:
        continue

query_date = "update tdx.recordtime set Date='%s' WHERE ID = 'fcast'"
cur_date.execute(query_date%end_date)
cnx.commit()

print("新增%d條數據"%count)
cnx.close()

# 登出系统
bs.logout()