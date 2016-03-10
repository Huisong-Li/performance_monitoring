#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: github.com/Huisong-Li


import cx_Oracle
import sqlalchemy
from sqlalchemy import *
import os
import json



def connect_db():
	abspath_str =  os.path.abspath(".") + '\\performance_monitoring'
	with open(abspath_str+'\\configuration\\administrator.json', 'r') as file:
		administrator_dict = json.load(file)
	conn_str = administrator_dict['oracle']['conn_str']
	db = cx_Oracle.connect(conn_str)
	return db

def engine_db():
	abspath_str =  os.path.abspath(".") + '\\performance_monitoring'
	with open(abspath_str+'\\configuration\\administrator.json', 'r') as file:
		administrator_dict = json.load(file)
	conn_str = administrator_dict['oracle']['engine']
	engine = create_engine(conn_str)
	return engine

def sql_dml(sql, db):
	cursor=db.cursor()
	cursor.execute(sql)
	cursor.close()
	db.commit()

def sql_select(sql, db):
	cursor = db.cursor()
	cursor.execute(sql)
	result_str = cursor.fetchall()
	cursor.close()
	return result_str