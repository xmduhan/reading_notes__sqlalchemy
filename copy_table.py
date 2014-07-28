# -*- coding: utf-8 -*-
"""
    在两个数据库间拷贝已将数据表
"""

#%%
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

#%%
srcConnectString = 'oracle://khfw:khfw123@new_sjck.world'
desConnectString = 'oracle://khfw:khfw123@odsdb'


#%% 连接源数据库 
srcEngine = create_engine(srcConnectString, echo=True, convert_unicode=True)
srcSession = sessionmaker(bind=srcEngine)()
srcMetaData = MetaData(bind=srcEngine)

#%% 连接目标数据库
desEngine = create_engine(desConnectString, echo=True, convert_unicode=True)
desSession = sessionmaker(bind=desEngine)()
desMetaData = MetaData(bind=desEngine)

#%% 读取源数据表的结构
srcTable = Table('duh0725_1', srcMetaData,autoload=True)

#%% 拷贝源数据表到目标schema|
desTable = srcTable.tometadata(desMetaData)

#%% 检查数据表是否存在
# 如果不存在，创建表结构并将数据插入
if not desTable.exists():
    # 这里要小心如果表已经存在，系统默认情况下是不会给提示的 
    desMetaData.create_all(desEngine)    
    # 读取数据并插入目标表
    data = srcSession.execute(srcTable.select()).fetchall()
    if data :
        desSession.execute(desTable.insert(),data)
        desSession.commit()    
else: 
    print 'The table already exist!'

