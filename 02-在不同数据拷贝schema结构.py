# -*- coding: utf-8 -*-
"""
Created on Mon Jul 28 15:02:03 2014

@author: duhan
"""

#%%
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

#%%
srcConnectString = 'oracle://srcUser:password@scrdb'
desConnectString = 'oracle://desUser:password@desdb'

#%% 连接数据仓库 
srcEngine = create_engine(srcConnectString, echo=True, convert_unicode=True)
srcSession = sessionmaker(bind=srcEngine)()
srcMetaData = MetaData(bind=srcEngine)

#%% 连接ODS
desEngine = create_engine(desConnectString, echo=True, convert_unicode=True)
desSession = sessionmaker(bind=desEngine)()
desMetaData = MetaData(bind=desEngine)

#%% 读取源数据表的结构
Table('duh0725_0', srcMetaData,autoload=True)
Table('duh0725_1', srcMetaData,autoload=True)

#%% 拷贝数据结构到目标schema
desMetaData.tables = srcMetaData.tables.copy()

#%% 创建目标数据结构
desMetaData.create_all(desEngine)
