#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: github.com/Huisong-Li
# Date  : 03/12/2016

import os
import re
import sys
import math
import json
from datetime import *
import xml.dom.minidom
from xml.dom.minidom import parse

import matplotlib
from matplotlib import font_manager
matplotlib.use('Agg')
matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['axes.unicode_minus'] = False
import matplotlib.pyplot as plt
from matplotlib.pyplot import plot,savefig
import numpy as np
import numpy.linalg as la
import scipy as sp
import statsmodels.api as sm
import pandas as pd
import WindPy as wind
from ffn import *
import xlsxwriter

from ..utils import oracle

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'



class assets_engine():
	def __init__(self):
		self.prices = pd.DataFrame()
		self.portDF = pd.DataFrame()
		self.perf   = None
		self.abspath_str =  os.path.abspath(".") + '\\performance_monitoring'
		with open(self.abspath_str+'\\configuration\\user_io.json', 'r') as file:
			user_io_dict = json.load(file)
		self.dest_file_directory_str = user_io_dict['destination']['file_directory']
		self.assets_file_name_str = user_io_dict['destination']['assets_file_name']
		with open(self.abspath_str+'\\configuration\\user_select.json', 'r') as file:
			user_select_dict = json.load(file)
		self.start_date_str = user_select_dict['basic']['start_date']
		self.end_date_str   = user_select_dict['basic']['end_date']
		# self.fund_names = user_select_dict['basic']['fund_names']
		# self.port_names = user_select_dict['basic']['port_names']
		self.select_port_list = None
		self.benchmark_code_str = user_select_dict['basic']['benchmark_code']
		# self.select_list = [self.fund_names, self.port_names, self.start_date_str, self.end_date_str]
		self.engine = oracle.engine_db()

	# 装载数据
	# data 输入格式：
	#               	aapl	msft
	# Date
	# 2010-01-04	28.466830	26.415914
	# 2010-01-05	28.516046	26.424448
	# 2010-01-06	28.062460	26.262284
	#
	def setup_close_data(self,benchmark_df,port_df):
		self.prices = pd.merge(benchmark_df,port_df,left_index=True,right_index=True,how='inner')
		self.prices.fillna(method='ffill',inplace=True)

	def setup_return_data(self,benchmark_df,port_df):
		self.prices_return = pd.merge(benchmark_df,port_df,left_index=True,right_index=True,how='inner')
		self.prices_return.fillna(method='ffill',inplace=True)

	def calc_performance_indexs(self):
		perf = self.prices.calc_stats()
		self.perf=perf

	def calculate_beta(self, column_name):
		# it doesn't make much sense to calculate beta for less than two days,
		# so return nan.
		algorithm_returns = self.prices[column_name]
		algorithm_returns = (algorithm_returns-algorithm_returns.shift(1))/algorithm_returns.shift(1)
		if (column_name==self.benchmark_code_str):
			self.prices_return[column_name]=self.prices_return[column_name]/100
		else:
			self.prices_return[column_name]=algorithm_returns
		algorithm_returns = algorithm_returns[1:]
		benchmark_returns = self.prices_return[self.benchmark_code_str]
		benchmark_returns = benchmark_returns[1:]
		if len(algorithm_returns) < 2:
			return np.nan

		returns_matrix = np.vstack([algorithm_returns,benchmark_returns])
		C = np.cov(returns_matrix, ddof=1)

		# If there are missing benchmark values, then we can't calculate the
		# beta.
		if not np.isfinite(C).all():
			return np.nan

		eigen_values = la.eigvals(C)
		condition_number = max(eigen_values) / min(eigen_values)
		algorithm_covariance = C[0][1]
		benchmark_variance = C[1][1]
		beta = algorithm_covariance / benchmark_variance
		
		#print beta
		return beta

	def information_ratio(self, column_name):
		algorithm_returns = self.prices_return[column_name]
		#algorithm_returns = (algorithm_returns-algorithm_returns.shift(1))/algorithm_returns.shift(1)
		algorithm_returns = algorithm_returns[1:]
		if (column_name!=self.benchmark_code_str):
			benchmark_returns = self.prices_return[self.benchmark_code_str]
			benchmark_returns = benchmark_returns[1:]
			relative_returns = algorithm_returns - benchmark_returns
			relative_deviation = relative_returns.std(ddof=1)
			return np.mean(relative_returns) / relative_deviation
		else:
			return 0

	# 取基准 收盘价
	def get_benchmark_close(self):
		benchmark_df=pd.DataFrame()
		wind.w.start()
		tmp=wind.w.wsd(self.benchmark_code_str, "close", self.start_date_str, self.end_date_str, "Fill=Previous;PriceAdj=F")
		benchmark_df[self.benchmark_code_str] = tmp.Data[0]
		benchmark_df['Date'] = [datetime.strptime(x.strftime("%Y-%m-%d"),"%Y-%m-%d") for x in tmp.Times]
		benchmark_df.index=benchmark_df['Date']
		benchmark_df.drop('Date',axis=1,inplace=True)
		return benchmark_df

	# 取基准 收益率
	def get_benchmark_return(self):
		benchmark_df=pd.DataFrame()
		wind.w.start()
		tmp=wind.w.wsd(self.benchmark_code_str, "pct_chg", self.start_date_str, self.end_date_str, "Fill=Previous;PriceAdj=F")
		benchmark_df[self.benchmark_code_str] = tmp.Data[0]
		benchmark_df['Date'] = [datetime.strptime(x.strftime("%Y-%m-%d"),"%Y-%m-%d") for x in tmp.Times]
		benchmark_df.index=benchmark_df['Date']
		benchmark_df.drop('Date',axis=1,inplace=True)
		return benchmark_df

	#从Oracle中获取指定时间范围的当日单位净值，组合成一个port
	def get_ports_return(self):
		ports_df = pd.DataFrame()
		with open(self.abspath_str+'\\configuration\\manager_fof_construction.json', 'r') as file:
			manager_fof_construction_dict = json.load(file)
		for select_port in self.select_port_list:
			fund_id = select_port[0]
			port_id = select_port[1]
			if port_id == 'P00000':
				port_name_str = manager_fof_construction_dict[fund_id]['fund_property']['fund_name']
			else:
				port_name_str = manager_fof_construction_dict[fund_id]['portfolios'][port_id]['port_property']['port_name']
			select_list = [fund_id,port_id,self.start_date_str,self.end_date_str]
			select_sql = '''
                         select l_date, unit_value as {0[4]} from portAsset 
                         where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date BETWEEN '{0[2]}' AND '{0[3]}' AND DATA_STATUS = '1' AND VALUES_COSTS = 'values'
                         '''.format(select_list+[port_name_str])
			df = pd.read_sql(select_sql,self.engine)
			df.index=df['l_date']
			df.drop('l_date',axis=1,inplace=True)
			if ports_df.empty:
				ports_df = df.copy()
			else:
				ports_df = pd.merge(ports_df,df,left_index=True,right_index=True,how='outer')
		return ports_df

	def draw(self):
		file_path = self.dest_file_directory_str
		self.prices.rebase().plot(figsize=(12,5))
		file_name='timeSeriesPic.png'
		full_name=file_path+file_name
		savefig(full_name)
		self.prices.to_drawdown_series().plot()
		file_name='drawdown.png'
		full_name=file_path+file_name
		savefig(full_name)

	#更新stats_df 的格式，例如 %, 等等
	def chg_fmt(self,stats_df):
		dom_tree = xml.dom.minidom.parse(self.abspath_str+'\\configuration\\port_assets_view_config.xml')
		performance = dom_tree.documentElement
		indexs = performance.getElementsByTagName("index")
		index_list = []
		type_list = []
		display_list = []
		chinese_list = []
		for index in indexs:
			type = index.getElementsByTagName('type')[0]
			display = index.getElementsByTagName('display')[0]
			language = index.getElementsByTagName('language')[0]
			chinese = language.getElementsByTagName('chinese')[0]
			type_list.append(type.childNodes[0].data)
			display_list.append(display.childNodes[0].data)
			chinese_list.append(chinese.childNodes[0].data)
			index_list.append(index.getAttribute("content"))
		data = {
               'display' : display_list,
               'type' : type_list,
               'chinese' : chinese_list
               }
		chg_fmt_df = pd.DataFrame(data, index = index_list)
		#stats_df.index.name = ''
		#print stats_df
		#stats_chg_fmt_df = pd.merge(stats_df,chg_fmt_df,left_index=True,right_index=True, how = 'left')
		#print stats_chg_fmt_df
		stats_df['chinese'] = 0
		stats_df['type'] = 0
		stats_df['display'] = 0
		#print stats_df
		#print chg_fmt_df
		for index in list(stats_df.index):
			stats_df.loc[index,'chinese'] = chg_fmt_df.loc[index,'chinese']
			stats_df.loc[index,'type'] = chg_fmt_df.loc[index,'type']
			stats_df.loc[index,'display'] = chg_fmt_df.loc[index,'display']
		
		#print stats_df
		stats_df.drop_duplicates(inplace = True)
		#print stats_df
		return stats_df

	# 将所有绩效数据 文件中
	def save_to_file(self):
		'''
		xl_stats_df=pd.DataFrame()
		for column_name in self.perf:
			xl_stats_df[column_name]=self.perf[column_name].stats
		'''
		stats_dict = {
                 'Start':'start',
                 'End':'end',
                 'Risk-free rate':'_yearly_rf',
                 'Total Return':'total_return',
                 'Daily Sharpe':'daily_sharpe',
                 'CAGR':'cagr',
                 'Max Drawdown':'max_drawdown',
                 'MTD':'mtd',
                 '3m':'three_month',
                 '6m':'six_month',
                 'YTD':'ytd',
                 '1Y':'one_year',
                 '3Y (ann.)':'three_year',
                 '5Y (ann.)':'five_year',
                 '10Y (ann.)':'ten_year',
                 'Since Incep. (ann.)':'incep',
                 'Daily Sharpe':'daily_sharpe',
                 'Daily Mean (ann.)':'daily_mean',
                 'Daily Vol (ann.)':'daily_vol',
                 'Daily Skew':'daily_skew',
                 'Daily Kurt':'daily_kurt',
                 'Best Day':'best_day',
                 'Worst Day':'worst_day',
                 'Monthly Sharpe':'monthly_sharpe',
                 'Monthly Mean (ann.)':'monthly_mean',
                 'Monthly Vol (ann.)':'monthly_vol',
                 'Monthly Skew':'monthly_skew',
                 'Monthly Kurt':'monthly_kurt',
                 'Best Month':'best_month',
                 'Worst Month':'worst_month',
                 'Yearly Sharpe':'yearly_sharpe',
                 'Yearly Mean':'yearly_mean',
                 'Yearly Vol':'yearly_vol',
                 'Yearly Skew':'yearly_skew',
                 'Yearly Kurt':'yearly_kurt',
                 'Best Year':'best_year',
                 'Worst Year':'worst_year',
                 'Avg. Drawdown':'avg_drawdown',
                 'Avg. Drawdown Days':'avg_drawdown_days',
                 'Avg. Up Month':'avg_up_month',
                 'Avg. Down Month':'avg_down_month',
                 'Win Year %':'win_year_perc',
                 'Win 12m %':'twelve_month_win_perc'}
		csv_directory_str = self.dest_file_directory_str+'assets_performance.csv'
		self.perf.to_csv(path = csv_directory_str)
		xl_stats_df = pd.read_csv(csv_directory_str,skiprows= [4,9,19,27,35,43])
		#print xl_stats_df
		
		drop_list = []
		for index in list(xl_stats_df.index):
			if xl_stats_df.iloc[int(index),1] == '-':
				drop_list.append(index)
		xl_stats_df.drop(drop_list,axis = 0, inplace=True)
		
		for index in list(xl_stats_df.index):
			dict_key = xl_stats_df.loc[index,'Stat']
			xl_stats_df.loc[index,'Stat'] = stats_dict[dict_key]
		xl_stats_df.index = list(xl_stats_df['Stat'])
		xl_stats_df.drop('Stat', axis=1, inplace=True)
		index_list = list(xl_stats_df.index)
		index_list.remove('start')
		index_list.remove('end')
		for index in index_list:
			for columns in list(xl_stats_df.columns):
				value = xl_stats_df.loc[index,columns]
				if isinstance(value,str):
					if re.findall('%',value) != []:
						value = value.replace('%','')
						value = float(value)/100
					else:
						value = float(value)
				xl_stats_df.loc[index,columns]=value
		#print xl_stats_df
		xl_stats_df.columns=[unicode(x) for x in xl_stats_df.columns]
		#print xl_stats_df
		if 'daily_mean' in list(xl_stats_df.index):
			daily_df = pd.DataFrame(index = ['beta','treynor_ratio','jensens_alpha','information_ratio'], columns = list(xl_stats_df.columns))
			for column_name in daily_df.columns:
				beta = self.calculate_beta(column_name)
				daily_df.loc['beta',column_name] = beta
				daily_df.loc['treynor_ratio',column_name] =  xl_stats_df.loc['daily_mean',column_name]/beta
				daily_df.loc['jensens_alpha',column_name] = xl_stats_df.loc['daily_mean',column_name] - (xl_stats_df.loc['daily_mean',self.benchmark_code_str])*beta
				daily_df.loc['information_ratio',column_name] = self.information_ratio(column_name)
		xl_stats_df = xl_stats_df.append(daily_df)
		stats_chg_fmt_df = self.chg_fmt(xl_stats_df)
		xl_stats_df = stats_chg_fmt_df.drop(['type','display'],axis =1)
		xl_stats_df.index = list(xl_stats_df['chinese'])
		xl_stats_df.drop('chinese',axis = 1, inplace = True)
		i = 0
		delete_num_list = []
		delete_list = []
		for state in stats_chg_fmt_df['display']:
			if state == 'invisible':
				delete_num_list.append(i)
			i = i + 1
		for num in delete_num_list:
			delete_list.append(list(xl_stats_df.index)[num])
		xl_stats_df.drop(delete_list, inplace=True)
		xl_stats_df.dropna(axis = 0, how = 'all',inplace = True)

		writer = pd.ExcelWriter(self.dest_file_directory_str+self.assets_file_name_str, engine='xlsxwriter',datetime_format='yyyy/mm/dd', date_format='yyyy/mm/dd')
		self.prices.to_excel(writer, sheet_name='Sheet1')
		worksheet = writer.sheets['Sheet1']
		worksheet.set_column(0,0,20)
		i = 1
		for columns_name in list(self.prices.columns):
			worksheet.set_column(i,i,len(columns_name)*2)
			i = i + 1

		colnum=0
		rownum=35
		xl_stats_df.to_excel(writer,sheet_name='Sheet2',startrow=0,startcol=0)
		xl_stats_df_len=len([x  for x in xl_stats_df.columns])+1

		workbook  = writer.book
		worksheet = writer.sheets['Sheet2']

		startrow = rownum+5
		worksheet.insert_image('A'+str(startrow), self.dest_file_directory_str+u'timeSeriesPic.png')
		worksheet.insert_image('A'+str(startrow+25), self.dest_file_directory_str+u'drawdown.png')

		format_percent = workbook.add_format({'num_format': '0.00%'})
		format_float = workbook.add_format({'num_format': '0.000'})
		worksheet.set_column(0,(2+xl_stats_df_len),20,format_percent)
		i = 1
		for state in stats_chg_fmt_df[stats_chg_fmt_df['display']=='visible']['type']:
			if state == 'float':
				worksheet.set_row(i,None,format_float)
			i = i + 1

		worksheet.set_column('A:A', 18)
		i = 1
		for column_name in list(xl_stats_df.columns):
			worksheet.set_column(i,i,len(column_name)*2+5)
			i = i + 1

		writer.save()
		writer.close()


	#删除指定的绩效结果
	def deleteReload(self,db,deleteConstraint):
		sql = '''insert into performance_statics_bak
		                select * from performance_statics
						        where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND start_date = {0[2]} AND end_date = {0[3]}'''.format(deleteConstraint)
		tool.sqlDML(sql,db)
		sql='''delete from performance_statics
			        where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND start_date = {0[2]} AND end_date = {0[3]}'''.format(deleteConstraint)
		tool.sqlDML(sql,db)
		print('Delete Finish')

	#检查绩效是否重载，对于重载的数据DATA_STATUS置为'0'，LCU和LCD与新插入的相应数据中的FCU和FCD相同
	def checkReload(self,db,selectConstraint):
		sql='''select * from performance_statics
			            where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND start_date = {0[2]} AND end_date = {0[3]} AND indicator_name = '{0[4]}' '''.format(selectConstraint)
		rs = tool.sqlSelect(sql,db)
		#print(rs)
		if rs != []:
			sql='''update performance_statics set LCD = sysdate, LCU = ora_login_user, DATA_STATUS = \'0\'
				        where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND start_date = {0[2]} AND end_date = {0[3]} AND indicator_name = '{0[4]}' '''.format(selectConstraint)
			tool.sqlDML(sql,db)
			print('Exist')
		else:
			print('Not Exist')
		print('Check Finish')

	#将所有绩效存入oracle 中
	def saveToDb(self):
		db = tool.connectDB()
		statsDF=pd.DataFrame()
		for x in self.perf:
			statsDF[x]=self.perf[x].stats
		#print statsDF

		DOMTree = xml.dom.minidom.parse("fund_port_rel.xml")
		mapping = DOMTree.documentElement
		funds = mapping.getElementsByTagName("fund")
		for col in statsDF.columns:

			for index in statsDF.index:
				if index != 'start' and index != 'end' :
					insertList = []
					FundID_PortID = tool.getFundID_PortID(funds,col)
					insertList.append(FundID_PortID[0])
					insertList.append(FundID_PortID[1])
					insertList.append(tool.getDate(str(statsDF.loc['start'][col]),'(\d{4}-\d{2}-\d{2}).*'))
					insertList.append(tool.getDate(str(statsDF.loc['end'][col]),'(\d{4}-\d{2}-\d{2}).*'))
					insertList.append(index)
					selectConstraint = [insertList[0],insertList[1],insertList[2],insertList[3],insertList[4]]
					#self.checkReload(db,selectConstraint)
					DOMTree = xml.dom.minidom.parse("viewConfig.xml")
					performance = DOMTree.documentElement
					indexs = performance.getElementsByTagName("index")
					for i in indexs:
						if index == i.getAttribute("content"):
							language = i.getElementsByTagName('language')[0]
							chinese = language.getElementsByTagName('chinese')[0]
							insertList.append((chinese.childNodes[0].data).encode("utf-8"))
							#print((chinese.childNodes[0].data).decode('utf-8'))
					statics_value = statsDF.loc[index][col]
					if type(statics_value) == type(np.float64(0)):
						statics_value = statics_value.item()
					if math.isnan(statics_value):
						statics_value = 'null'

					insertList.append(statics_value)
					sql='''insert into performance_statics
							(id,fund_id,port_id,start_date,end_date,indicator_name,chinese_name,statics_value,FCU,LCU)
							VALUES(S_performance_statics.Nextval,'{0[0]}','{0[1]}','{0[2]}','{0[3]}','{0[4]}','{0[5]}',{0[6]},ora_login_user,ora_login_user)'''.format(insertList)
					tool.sqlDML(sql,db)
		db.close()

	def debug(self):
		benchmark_close_df = self.get_benchmark_close()
		benchmark_return_df = self.get_benchmark_return()
		port_df = self.get_ports_return()
		self.setup_close_data(benchmark_close_df,port_df.sort())
		self.setup_return_data(benchmark_return_df,port_df.sort())
		self.calc_performance_indexs()
		self.draw()
		self.save_to_file()
		# self.saveToDb()
