#!/usr/bin/python3
# -*-coding:utf-8-*-

# author:天空城
# date:2019年6月29日
# 将time列按固定格式更新

import datetime
import traceback
from common import mysql_operation

# 数据库连接常量
DB_URL = "localhost"
DB_USERNAME = "root"
DB_PASSWORD = "root"
DB_NAME = "testdb"


# 日期处理逻辑
def get_date(diff_day, curr_date=datetime.datetime.now()):
    """
        function_name: get_date
        function_params:
        diff_day 与curr_date相差时间，往前推算取负值
        curr_date 传入的当前时间，默认为当前时间
        return:返回一个与curr_date相差diff_day的日期
    """
    # date = datetime.datetime(1971, 1, 1, 0, 0, 40)
    result_date = None
    try:
        time_diff_day = datetime.timedelta(days=-diff_day)
        # %Y-%m-%d %H:%M:%S
        result_date = (curr_date + time_diff_day).strftime("%Y-%m-%d 06:15:00")
    except:
        traceback.print_exc()
    return result_date


# 具体情况具体编写
# 存储为{"kpi_name":[],"kpi_name2":[]...}，然后遍历存time_spot
def create_new_dict(result_set):
    data_dict = {}
    for row in result_set:
        key = row[0]
        kpi_name = row[1]
        if kpi_name not in data_dict:
            # 不包含，则创建存储list
            temp_list = [key, kpi_name]
            data_list = [temp_list]
            data_dict[kpi_name] = data_list
        else:
            data_list = data_dict[kpi_name]
            temp_list = [key, kpi_name]
            data_list.append(temp_list)
    return data_dict


def update_time(data_dict):
    execute_many_list = []
    for kpi_name in data_dict:
        data_list = data_dict[kpi_name]
        # 遍历处理data_list中所有数据的time_spot
        for i in range(len(data_list)):
            time_spot = get_date(i)
            data_list[i].append(time_spot)
            # 适配批量，更换0,2，去除1
            temp_value = data_list[i][0]
            data_list[i][0] = data_list[i][2]
            data_list[i][2] = temp_value
            data_list[i].remove(data_list[i][1])
            # transfer tuple
            temp_tuple = tuple(data_list[i])
            data_list[i] = temp_tuple
        execute_many_list.extend(data_list)
    return execute_many_list


if __name__ == "__main__":
    mysql_operation = mysql_operation.MysqlOperation("localhost", "root", "root", "testdb")
    sql = "select id,kpi_name,time_spot from t_rtm_kpi_value order by id desc"
    result = mysql_operation.select(sql)
    data_dict = create_new_dict(result)
    execute_many_list = update_time(data_dict)
    update_sql = "update t_rtm_kpi_value set time_spot = %s where id = %s"
    mysql_operation.execute_sql_args(update_sql, execute_many_list, True)
    mysql_operation.close()
