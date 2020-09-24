# 盈利能力

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
cnx = pymysql.connect("192.168.1.85","lihq","lihq8087*)*&","tdx")
cur_index = cnx.cursor()
cur_index_sql = "select code,market,flatest_date from tdx.index;"
cur_index.execute(cur_index_sql)
code_item_list = cur_index.fetchall() # 财务更新日期元组项的元组（（code,market,flatest_date），....）

cur_profit = cnx.cursor()

current_time = time.localtime(time.time())
current_quarter = (current_time.tm_mon - 1) // 3 + 1  #当前日期的季度

# 偿债能力

profit_head = ['code','pubDate','statDate','roeAvg','npMargin','gpMargin','netProfit','epsTTM','MBRevenue','totalShare','liqaShare','year','quarter']

count = 0  # 親數據
for code_item in code_item_list:
    for year in range(2020,2021):
        for quarter in range(1,5):
            code = code_item[1]+"."+code_item[0]
            cur_profit_sql = "select code,year,quarter from tdx.profit where code='%s' and year='%d' and quarter='%d';"%(code,year, quarter)
            # 查看数据库是否已有该数据
            cur_profit.execute(cur_profit_sql)
            findinfo = cur_profit.fetchone()

            if findinfo is None:  # 數據庫中還沒有該數據，寫入數據庫
                rs_profit = bs.query_profit_data(code=code_item[1]+"."+code_item[0], year=year, quarter= quarter)
                while (rs_profit.error_code == '0') & rs_profit.next():
                    profit_list = rs_profit.get_row_data()

                    insert_sql = "INSERT INTO profit("
                    value_sql = "VALUES("
                    value_list = []

                    for index in range(len(profit_list)):
                        if profit_list[index] != '':
                            insert_sql += profit_head[index]+','
                            value_sql += "'%s',"
                            value_list.append(profit_list[index])

                    insert_sql += 'year,quarter) '
                    value_sql += "'%s','%s')"

                    value_list.append(year)
                    value_list.append(quarter)
                    tuple_value = tuple(value_list)

                    cur_profit_insert = insert_sql + value_sql

                    cur_profit.execute(cur_profit_insert%tuple_value)
                    cnx.commit()

                    count += 1
                    profit_list.clear()
            else:
                continue

print("新增%d條數據"%count)
cnx.close()

# 登出系统
bs.logout()
