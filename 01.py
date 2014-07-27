# -*- coding: utf-8 -*-
"""
Created on Sun Jul 27 10:15:30 2014

@author: Administrator
"""

#%% 载入必要的模块
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import Sequence
from sqlalchemy.sql import select
from sqlalchemy.sql import and_, or_, not_
#%% 创建连接引擎
engine = create_engine('sqlite:///:memory:', echo=True)

#%% 内存schema
metadata = MetaData()

#%% 创建表结构
users = Table(
    'users',metadata,
    Column('id', Integer, Sequence('user_id_seq'), primary_key=True),
    Column('name', String),
    Column('fullname', String),
)

addresses = Table(
    'addresses', metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', None, ForeignKey('users.id')),
    Column('email_address', String, nullable=False)
)   

#%% 通过内存schema创建数据结构到数据库
metadata.create_all(engine)


#%%
ins = users.insert()
str(ins)

#%% 插入时指定列
ins = users.insert().values(name='jack', fullname='Jack Jones')  
str(ins)

#%% 查看参数
ins.compile().params

#%% 创建连接
conn = engine.connect()

#%% 执行插入语句
ins = users.insert().values(name='test01', fullname='test01')  
result = conn.execute(ins)
ins = users.insert().values(name='test02', fullname='test02')  
result = conn.execute(ins)

#%%
result.inserted_primary_key

#%% 一次插入多条记录
conn.execute(addresses.insert(), [
     {'user_id': 1, 'email_address' : 'jack@yahoo.com'},
     {'user_id': 1, 'email_address' : 'jack@msn.com'},
     {'user_id': 2, 'email_address' : 'www@www.org'},
     {'user_id': 2, 'email_address' : 'wendy@aol.com'},
])

#%% 执行查询
s = select([addresses])
result = conn.execute(s)
for row in result:
    print row
result.close()

#%% 查询时指定列
s = select([addresses.c.email_address]) # 同 addresses.columns.email_address
result = conn.execute(s)
for row in result:
    print row
result.close()

#%% 遍历一个表的所有列
for col in addresses.columns:
    print col.name


#%% 关联两张表查询
exp1 = users.c.id == addresses.c.user_id
exp2 = users.c.id == 1
exp = and_(exp1, exp2)
s = select([users, addresses]).where(exp)
for row in conn.execute(s):
    print row

#%% 使用like表达式
exp = addresses.c.email_address.like('j%')
s = select([addresses]).where(exp)
for row in conn.execute(s):
    print row