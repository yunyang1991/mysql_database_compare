# -*- coding: utf-8 -*-
__author__ = 'yunyang'

import environment as env
import dfHelper

#表示含义 name 表示表名称 sql 查询DataFrame字符串 field 表示排序的字段

def compare_with_column(db_config1,db_config2,tables):
    """
    列对比函数，效率高但是数据结果不够详细
    :param db_config1:数据库配置
    :param db_config2:另外一个数据库配置
    :param tables: 表
    
    :return:成功返回True，否则False
    """
    try:
        flag = True
        for table in tables:
            env.BaseLogging.getAppLog().info("开始对比表{0}".format(table["name"]))
            df1 = dfHelper.queryDataFrame(db_config1,table)
            df2 = dfHelper.queryDataFrame(db_config2,table)
            if dfHelper.compareDataFrameColumns(df1, df2):
                env.BaseLogging.getAppLog().info("表{0}相同".format(table["name"]))
            else:
                env.BaseLogging.getAppLog().error("表{0}不相同".format(table["name"]))
                flag = False

        if flag:
            env.BaseLogging.getAppLog().info("数据库一致")
        else:
            env.BaseLogging.getAppLog().error("数据库不一致")
    except Exception as ex:
        env.BaseLogging.getAppLog().error(ex)


def compare_with_row(db_config1,db_config2,tables):
    """
    对比函数 数据结果比较详细，精确到行 但是效率低
    :param db_config1:数据库配置
    :param db_config2:另外一个数据库配置
    :param tables: 表
    
    :return:成功返回True，否则False
    """
    try:
        flag = True
        for table in tables:
            env.BaseLogging.getAppLog().info("开始对比表{0}".format(table["name"]))
            df1 = dfHelper.queryDataFrame(db_config1,table)
            df2 = dfHelper.queryDataFrame(db_config2,table)
            if dfHelper.compereDataTableWithRows(df1, df2):
                env.BaseLogging.getAppLog().info("表{0}相同".format(table["name"]))
            else:
                env.BaseLogging.getAppLog().error("表{0}不相同".format(table["name"]))
                flag = False

        if flag:
            env.BaseLogging.getAppLog().info("数据库一致")
        else:
            env.BaseLogging.getAppLog().error("数据库不一致")
    except Exception as ex:
        env.BaseLogging.getAppLog().error(ex)

if __name__ == "__main__":
    db_config1 = {'host':'ip1','user':'your_db_user','password':'pwd','port':3306,'dbName':'your_db_name'}
    db_config2 = {'host':'ip1','user':'your_db_user','password':'pwd','port':3306,'dbName':'your_db_name'}
    import compare_tables
    compare(db_config1, db_config2, compare_tables.tables)