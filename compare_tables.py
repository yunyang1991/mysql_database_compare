# -*- coding: utf-8 -*-
__author__ = 'yunyang'
tables = [
    #字段说明
    {"name": "base_area",#name 表示对比数据库名称
    "sql": """
            SELECT 
             'area_name',
             'source_id' 
             UNION 
              SELECT 
               area_name,
               source_id 
              FROM 
               base_area""",#对比的SQL，注意要用union查询
    "order": ["source_id"]}, #查询结果对比的字段，如果order:None 表示按照所有字段排序
]