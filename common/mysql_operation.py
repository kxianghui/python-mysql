#!/usr/bin/python3
# -*-coding:utf-8-*-

# author:天空城
# date:2019年6月29日

import pymysql


class MysqlOperation:
    """
    mysql具体操作类 use pymysql
    """
    def __init__(self, url, username, password, database_name):
        self.url = url
        self.username = username
        self.password = password
        self.database_name = database_name
        self.__connect()

    def __connect(self):
        """
        私有连接方法，
        :return:
        """
        self.__connection = pymysql.connect(self.url, self.username, self.password, self.database_name)
        self.__cursor = self.__connection.cursor()

    def close(self):
        """
        关闭连接
        :return:
        """
        self.__cursor.close()
        self.__connection.close()

    def execute_sql(self, sql):
        """
        insert,update,delete
        无参
        :param sql: 待执行的sql
        :return: None
        """
        try:
            self.__cursor.execute(sql)
            self.__connection.commit()
        except Exception as e:
            self.__connection.rollback()
            print("执行MySQL: %s 时出错：%s" % (sql, e))

    def execute_sql_args(self, sql, args, many_flag=False):
        """
        insert,update,delete 有参数，使用占位符形式
        [insert|update|delete]
        where param1 = %s and param2 = %s
        :param sql: 占位符sql
        :param args: 参数，形式为一个tuple或list
        :param many_flag: 是否批量执行的
        :return: None
        """
        try:
            if not args:
                return
            if many_flag:
                self.__cursor.executemany(sql, args)
            else:
                self.__cursor.execute(sql, args)
            self.__connection.commit()
        except Exception as e:
            self.__connection.rollback()
            print("执行MySQL: %s 时出错：%s" % (sql, e))

    def select(self, sql, args=None, all_flag=True):
        """
        查询方法，
        :param sql: 待执行的sql
        :param args: 参数列表 [tuple|list]
        :param all_flag: 返回一条或全部
        :return: list 形如[{},{},{}]每一个字典是一行数据
        """
        if not args:
            self.__cursor.execute(sql, args)
        else:
            self.__cursor.execute(sql)
        return self.__cursor.fetchall() if all_flag else self.__cursor.fetchone()
