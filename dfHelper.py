# -*- coding: utf-8 -*-
__author__ = 'yunyang'
import MySQLdb
import pandas as pd
import environment as env


def queryDataFrame(db_config, table):
    """
    查询DataFrame
    :param db_config:数据库配置
    :param sql: sql查询语句
    :return:DataFrame TODO:数据类型全部为字符串
    """

    conn = MySQLdb.connect(host=db_config['host'], user=db_config['user'],
                           passwd=db_config['password'], db=db_config['dbName'],
                           port=db_config['port'])
    cur = conn.cursor()
    cur.execute(table["sql"])
    ret = cur.fetchall()
    colNames = ret[0]
    if len(ret) == 1:
        data = pd.DataFrame(columns=colNames)
    else:
        data = pd.DataFrame(list(ret[1:]), columns=colNames,dtype=str)
        orders = []
        if table["order"] is not None and table["order"] != "":
            orders = table["order"]
        else:
            orders = list(data.columns)
        data.sort(orders, ascending=map(lambda x:0, orders), inplace=True)
        data.reset_index(drop=True, inplace=True)


    cur.close()
    conn.close()

    return data


def compareDataFrameColumns(df1, df2):
    """
    对比DataFrame 列对比
    :param df1: DataFrame
    :param df2: DataFrame
    :return:两个DataFrame相同返回True，否则False
    """
    flag = True
    if len(df1) != len(df2):
        env.BaseLogging.getAppLog().error("查询结果行数不一致")
        return False

    if list(df1.columns) != list(df2.columns):
        env.BaseLogging.getAppLog().error("dataframe column 列不一致")
        return False

    for field in df1.columns:
        li1 = list(df1[field])
        li2 = list(df2[field])
        if li1 != li2:
            env.BaseLogging.getAppLog().error("字段{0}不一致".format(field))
            flag = False

    return flag

def compereDataTableWithRows(df1,df2):
    """
    对比DataFrame 行对比
    :param df1: DataFrame
    :param df2: DataFrame
    :return:两个DataFrame相同返回True，否则False
    """
    flag = True
    if len(df1) != len(df2):
        env.BaseLogging.getAppLog().error("查询结果行数不一致")
        return False

    if list(df1.columns) != list(df2.columns):
        env.BaseLogging.getAppLog().error("dataframe column 列不一致")
        return False

    length = len(df1)
    diff_count = 0
    for i in xrange(length):
        row1 = list(df1.iloc[i])
        row2 = list(df2.iloc[i])
        if row1 != row2:
            flag = False
            env.BaseLogging.getAppLog().error("行{0}数据不一致".format(str(i)))
            env.BaseLogging.getAppLog().error(row1)
            env.BaseLogging.getAppLog().error(row2)
            diff_count += 1

    if diff_count > 0:
        env.BaseLogging.getAppLog().error("不一致百分比{0}".format(diff_count/(length * 1.0)))

    return flag