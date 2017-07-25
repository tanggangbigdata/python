#!/usr/bin/env python
# _*_ coding:utf-8 _*_
__create_time__="2017-03-09"
__Author__ = "Tanggang"
__note__ = ""

from .conf import conf
import MySQLdb
class connmysql:
  def __init__(self,config,database=""):
    c=conf().getConf(config)
    if database !="":
      c['database']=database
    self.conn = MySQLdb.connect(c['host'],c['user'],c['password'],c['database'])

  def query(self,table_name,dt,*columns):
    self.cursor=self.conn.cursor()
    if len(columns)==1:
      sql="""select {column1} from {table_name} where dt={dt} """.format(column1=columns[0],table_name=(table_name),dt=dt)
    elif len(columns)==2:
      sql="""select {column1},{column2} from {table_name} where dt={dt} """.format(column1=columns[0],colimn2=columns[1],table_name=table_name,dt=dt)
    else:
      sql="""select {column1},{column2},{column3} from {table_name} where dt={dt} """.format(column1=columns[0],column2=columns[1],column3=columns[2],table_name=table_name,dt=dt)
    self.cursor.execute(sql)
    result = self.cursor.fetchall()
    return result
    self.conn.close()

  def query2(self,table_name,dt,dt_group,*columns):
    self.cursor=self.conn.cursor()
    if len(columns)==1:
      sql="""select {column1} from {table_name} where dt={dt} and dt_group={dt_group} """.format(column1=columns[0],table_name=(table_name),dt=dt,dt_group=dt_group)
      print sql
    elif len(columns)==2:
      sql="""select {column1},{column2} from {table_name} where dt={dt} and dt_grup={dt_group}""".format(column1=columns[0],colimn2=columns[1],table_name=table_name,dt=dt,dt_group=dt_group)
    else:
      sql="""select {column1},{column2},{column3} from {table_name} where dt={dt} and dt_group={dt_droup} """.format(column1=columns[0],column2=columns[1],column3=columns[2],table_name=table_name,dt=dt,dt_group=dt_group)
    self.cursor.execute(sql)
    result = self.cursor.fetchall()
    return result
    self.conn.close()

  def insert(self,table_name,*columns):
    self.cursor=self.conn.cursor()
    sql="""INSERT INTO {table_name} VALUES({column1},{column2},{column3})""".format(table_name=table_name,column1=columns[0],column2=columns[1],column3=columns[2])
    print sql
    self.cursor.execute(sql)
    self.conn.commit()
    self.conn.close()
  def update(self,table_name,dt,*columns):
    self.cursor=self.conn.cursor()
    if len(columns)==2:
      sql="""UPDATE {table_name} set {column1}={result1} where dt={dt} """.format(table_name=table_name,column1=colmns[0],result1=columns[1])
    else:
      sql="""UPDATE {table_name} set {column1}={result1},{column2}={result2} where dt={dt} """.format(table_name=table_name,column1=colmns[0],result1=columns[1],column2=coumns[2],result2=columns[3])
    self.cursor.execute(sql)
    self.conn.commit()
    self.conn.close()
  def delete(self,table_name,dt):
    self.cursor=self.conn.cursor()
    sql="""DELETE from {table_name} where dt={dt}""".fromat(table_name=table_name,dt=dt)
    self.cursor.execute(sql)
    self.conn.commit()
    self.conn.close()
