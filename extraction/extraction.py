#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: github.com/Huisong-Li
# Date  : 03/09/2016

import sys
import json
import re
import os
import math

import cx_Oracle
import numpy as np
import pandas as pd
from pandas import Series, DataFrame

from ..utils import oracle
from ..utils import select_translate

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'



class extraction():
	def __init__(self):
		self.abspath_str =  os.path.abspath(".") + '\\performance_monitoring'
		with open(self.abspath_str+'\\configuration\\administrator.json', 'r') as file:
			administrator_dict = json.load(file)
		self.source_file_directory_str = self.abspath_str + administrator_dict['source']['file_directory']
		with open(self.abspath_str+'\\configuration\\manager_estimation_table_construction.json', 'r') as file:
			estimation_table_construction_dict = json.load(file)
		self.metc_dict = estimation_table_construction_dict
		self.db = oracle.connect_db()
		self.engine = oracle.engine_db()

	# get file names list from source
	def get_files_list(self):
		with open(self.abspath_str+'\\configuration\\user_io.json', 'r') as file:
			user_io_dict = json.load(file)
		source_file_name_list = user_io_dict['source']['file_names'].decode('UTF-8').split(";")
		source_file_name_list = [x for x in source_file_name_list if x != u'']
		if source_file_name_list == []:
			file_names_list = os.listdir(self.source_file_directory_str)
		if file_names_list != []:
			return file_names_list
		else:
			sys.exit('empty folder')

	# get file date string
	def get_file_date(self,file_name):
		regexp_str = '.*(\d{4}-\d{2}-\d{2}).*'
		date_list =  (re.findall(regexp_str,file_name.encode('UTF-8')))
		if date_list != []: 
			date_str = date_list[0].replace('-','')
			return date_str
		else:
			sys.exit(('wrong file name (date format): {0}'.format(file_name.encode('UTF-8'))).decode('UTF-8'))

	# get file fund id and portfolio id dictionary
	def get_file_id(self,file_name):
		with open(self.abspath_str+'\\configuration\\manager_fof_construction.json', 'r') as file:
			fof_construction_dict = json.load(file)
		find_boolean = False
		for fund_id in fof_construction_dict:
			# fund_id is string type 
			if 'portfolios' in fof_construction_dict[fund_id]:
				portfolio_dict = fof_construction_dict[fund_id]['portfolios']
				for port_id in portfolio_dict:
					# port_id is string type
					port_name_str = portfolio_dict[port_id]['port_property']['port_name']
					regexp_str = '.*({0}).*'.format(port_name_str.encode('UTF-8'))
					if  re.findall(regexp_str,file_name.encode('UTF-8')) != []:
						return {'fund_id':fund_id, 'port_id':port_id}
		if find_boolean == False:
			sys.exit(('wrong file name (portfolio name): {0}'.format(file_name.encode('UTF-8'))).decode('UTF-8'))

	# get file construction dictionary
	def get_file_construction(self,file_name):
		find_boolean = False
		for estimation_table_name in self.metc_dict:
			# estimation_table_name is string type
			port_name_str = (self.metc_dict[estimation_table_name]['port_name']).encode('UTF-8')
			regexp_str = '.*({0}).*'.format(port_name_str)
			if  re.findall(regexp_str,file_name.encode('UTF-8')) != []:
				return self.metc_dict[estimation_table_name]['construction']
		if find_boolean == False:
			sys.exit(('wrong file name (portfolio name): {0}'.format(file_name.encode('UTF-8'))).decode('UTF-8'))

	# get holdings dateframe
	def get_holdings_return(self):
		holdings_df = pd.DataFrame()
		file_names_list = self.get_files_list()
		for file_name in file_names_list:
			# file_name is string type 
			
			# analysis file name
			file_date_str = self.get_file_date(file_name)
			file_id_dict = self.get_file_id(file_name)
			file_construction_dict = self.get_file_construction(file_name)
			file_holdings_construction_dict = file_construction_dict['holdings']
			# analysis assets construction
			sheet_name_str = file_construction_dict['sheet_name']
			header_int = file_construction_dict['header']
			vertical_dict = file_construction_dict['vertical_axis']
			vertical_key_list = []
			for vertical_key in vertical_dict:
				# vertical_key is string
				vertical_key_list.append(vertical_key)
			# analysis vertical axis key 
			account_id_str   = vertical_dict['account_id']
			account_name_str = vertical_dict['account_name']
			amount_str       = vertical_dict['amount']
			unit_cost_str    = vertical_dict['unit_cost']
			total_cost_str   = vertical_dict['total_cost']
			market_price_str = vertical_dict['market_price']
			market_value_str = vertical_dict['market_value']
			pandl_str        = vertical_dict['pandl']
			# analysis horizontal axis key
			logical_dict     = file_holdings_construction_dict['logical_axis']
			calculation_dict = file_holdings_construction_dict['calculation_axis']
			security_type_dict = logical_dict['security_type']
			logical_key_list = []
			for logical_key in logical_dict:
				# logical_key is string
				logical_key_list.append(logical_key)
			calculation_key_list = []
			for calculation_key in calculation_dict:
				# calculation_key is string
				calculation_key_list.append(calculation_key)
			dataframe_key_list =  vertical_key_list + logical_key_list + calculation_key_list + ['l_date','fund_id','port_id']
			dataframe_key_list.sort()
			# get excel DataFrame
			to_process_df = pd.read_excel(self.source_file_directory_str+file_name, sheetname = sheet_name_str, header = header_int)
			to_process_df.index = to_process_df[account_id_str]
			to_process_list =  list(to_process_df.index)
			# filter accout id
			account_id_list =  filter(self.account_id_filter, to_process_list)
			for account_id in account_id_list:
				# account_id is string
				stock_regexp  =  '^({}.*)'.format(security_type_dict['stock'])
				fund_regexp   =  '^({}.*)'.format(security_type_dict['fund'])
				repo_regexp   =  '^({}.*)'.format(security_type_dict['repo'])
				future_regexp =  '^({}.*)'.format(security_type_dict['future'])
				if re.findall(stock_regexp, account_id) != [] and len(account_id) < 12:
					account_id_list.remove(account_id)
				if re.findall(fund_regexp, account_id)  != [] and len(account_id) < 12:
					account_id_list.remove(account_id)
				if re.findall(repo_regexp, account_id) != [] and len(account_id) < 12:
					account_id_list.remove(account_id)
				if re.findall(future_regexp, account_id) != [] and len(account_id) < 12:
					account_id_list.remove(account_id)
			holdings_tmp_df = pd.DataFrame(columns = dataframe_key_list)
			# extracte holdings from DataFrame
			for account_id in account_id_list:
				# account_id is string
				index_tmp_df = pd.DataFrame(columns = dataframe_key_list)
				index_tmp_df['l_date']  = [file_date_str]
				index_tmp_df['fund_id'] = [str(file_id_dict['fund_id'])]
				index_tmp_df['port_id'] = [str(file_id_dict['port_id'])]
				for vertical_key in vertical_key_list:
					vertical_value_str = vertical_dict[vertical_key]
					vertical_value = to_process_df.loc[account_id][vertical_value_str]
					# vertical_value contain string and float
					index_tmp_df[vertical_key] = [vertical_value]
				for logical_key in logical_dict:
					index_tmp_df[logical_key] = None
					child_node = logical_dict[logical_key]
					# child node may contain empty dictionary, dictionary, list or string
					if logical_key == 'security_type':
						for type_key in child_node:
							value_len_num = len(child_node[type_key])
							to_compare_str = re.findall('^(\d{%d}).*'%value_len_num, account_id)[0]
							if to_compare_str == child_node[type_key]:
								index_tmp_df[logical_key] = [type_key]
					if logical_key == 'sub_market_no': 
						for sub_no_key in child_node:
							value_len_num = len(child_node[sub_no_key])
							to_compare_str = re.findall('^(\d{%d}).*'%value_len_num, account_id)[0]
							if to_compare_str == child_node[sub_no_key]:
								index_tmp_df[logical_key] = [sub_no_key]
					if logical_key == 'position_flag':
						index_tmp_df[logical_key] = ['long']
						if isinstance(child_node, list):
							for list_item in child_node:
								# list item is string
								value_len_num = len(list_item)
								to_compare_str = re.findall('^(\d{%d}).*'%value_len_num, account_id)[0]
								if to_compare_str == str(list_item):
									index_tmp_df[logical_key] = ['short']
						if isinstance(child_node, unicode):
							value_len_num = len(child_node)
							to_compare_str = re.findall('^(\d{%d}).*'%value_len_num, account_id)[0]
							if to_compare_str == str(child_node):
								index_tmp_df[logical_key] = ['short']
					if logical_key == 'market_no':
						for no_key in child_node:
							grand_child_node = child_node[no_key]
							if isinstance(grand_child_node, list):
								for list_item in grand_child_node:
									# list item is string
									value_len_num = len(list_item)
									to_compare_str = re.findall('^(\d{%d}).*'%value_len_num, account_id)[0]
									if to_compare_str == str(list_item):
										index_tmp_df[logical_key] = [no_key]
							if isinstance(grand_child_node, unicode):
								value_len_num = len(grand_child_node)
								to_compare_str = re.findall('^(\d{%d}).*'%value_len_num, account_id)[0]
								if to_compare_str == str(grand_child_node):
									index_tmp_df[logical_key] = [no_key]
					if logical_key == 'security_code':
						if len(account_id) == 6:
							index_tmp_df[logical_key] = account_id
						else:
							security_code_str = re.findall('.*(.{6})$', account_id)
							index_tmp_df[logical_key] = security_code_str
				index_tmp_df['wind_security_code'] = None
				market_no_str = (index_tmp_df['market_no'])[0] 
				if market_no_str == u'SH':
					index_tmp_df['wind_security_code'] = [(index_tmp_df['security_code'])[0] + '.SH']
				if market_no_str == u'SZ':
					index_tmp_df['wind_security_code'] = [(index_tmp_df['security_code'])[0] + '.SZ']
				if market_no_str == u'ZJS':
					index_tmp_df['wind_security_code'] = [(index_tmp_df['security_code'])[0] + '.CFE']
				# joint DataFrames
				if holdings_tmp_df.empty:
					holdings_tmp_df = index_tmp_df
				else:
					holdings_tmp_df = holdings_tmp_df.append(index_tmp_df)
			if holdings_df.empty:
				holdings_df = holdings_tmp_df
			else:
				holdings_df = holdings_df.append(holdings_tmp_df)
		return holdings_df

	# get assets dateframe
	def get_assets_return(self):
		assets_df = pd.DataFrame()
		file_names_list = self.get_files_list()
		for file_name in file_names_list:
			# file_name is string type 
			
			# analysis file name
			file_date_str = self.get_file_date(file_name)
			file_id_dict = self.get_file_id(file_name)
			file_construction_dict = self.get_file_construction(file_name)
			file_assets_construction_dict = file_construction_dict['assets']
			# analysis assets construction
			sheet_name_str = file_construction_dict['sheet_name']
			header_int = file_construction_dict['header']
			vertical_dict = file_construction_dict['vertical_axis']
			# analysis vertical axis key 
			account_id_str   = vertical_dict['account_id']
			account_name_str = vertical_dict['account_name']
			total_cost_str   = vertical_dict['total_cost']
			market_value_str = vertical_dict['market_value']
			# analysis horizontal axis key
			balance_dict = file_assets_construction_dict['balance_axis']
			balance_key_list = []
			for balance_key in balance_dict:
				# balance_key is string
				balance_key_list.append(balance_key)
			balance_key_list.sort()
			summary_dict = file_assets_construction_dict['summary_axis']
			summary_key_list = []
			for summary_key in summary_dict:
				# summary_key is string
				summary_key_list.append(summary_key)
			summary_key_list.sort()
			# get excel DataFrame
			to_process_df = pd.read_excel(self.source_file_directory_str+file_name, sheetname = sheet_name_str, header = header_int)
			to_process_df.index = to_process_df[account_id_str]
			to_process_df.drop(account_id_str, axis=1, inplace=True)
			# extracte assets from DataFrame
			values_tmp_df = pd.DataFrame(columns = balance_key_list)
			costs_tmp_df = pd.DataFrame(columns = balance_key_list)
			for balance_key in balance_key_list:
				# balance_key is string
				balance_value_str = balance_dict[balance_key]
				if balance_value_str in to_process_df.index:
					account_market_value_float = to_process_df.loc[balance_value_str][market_value_str]
					account_total_cost_float   = to_process_df.loc[balance_value_str][total_cost_str]
					values_tmp_df[balance_key] = [account_market_value_float]
					costs_tmp_df[balance_key]  = [account_total_cost_float]
				else:
					values_tmp_df[balance_key] = None
					costs_tmp_df[balance_key]  = None
			summary_tmp_df = pd.DataFrame(columns = summary_key_list)
			for summary_key in summary_key_list:
				# summary_key is string
				summary_value_str = summary_dict[summary_key]
				if summary_value_str in to_process_df.index:
					summary_value_float = to_process_df.loc[summary_value_str][account_name_str]
					summary_tmp_df[summary_key] = [summary_value_float]
				else:
					summary_tmp_df[balance_key] = None
			# add summary, date, fund id, portfolio id  
			summary_tmp_df['l_date'] = [file_date_str]
			summary_tmp_df.index = summary_tmp_df['l_date']
			summary_tmp_df.drop('l_date', axis=1, inplace=True)
			values_tmp_df['l_date']  = [file_date_str]
			values_tmp_df['fund_id'] = [str(file_id_dict['fund_id'])]
			values_tmp_df['port_id'] = [str(file_id_dict['port_id'])]
			values_tmp_df.index = values_tmp_df['l_date']
			values_tmp_df.drop('l_date', axis=1, inplace=True)
			values_tmp_df['net_assets'] = values_tmp_df['total_assets'] - values_tmp_df['credit_value']
			values_tmp_df = pd.DataFrame.merge(values_tmp_df, summary_tmp_df, left_index = True, right_index = True)
			values_tmp_df['values_costs'] = ['values']
			costs_tmp_df['l_date']  = [file_date_str]
			costs_tmp_df['fund_id'] = [str(file_id_dict['fund_id'])]
			costs_tmp_df['port_id'] = [str(file_id_dict['port_id'])]
			costs_tmp_df.index = costs_tmp_df['l_date']
			costs_tmp_df.drop('l_date', axis=1, inplace=True)
			costs_tmp_df['net_assets'] = costs_tmp_df['total_assets'] - costs_tmp_df['credit_value']
			costs_tmp_df = pd.DataFrame.merge(costs_tmp_df, summary_tmp_df, left_index = True, right_index = True)
			costs_tmp_df['values_costs'] = ['costs']
			# joint DataFrames
			assets_tmp_df = values_tmp_df.append(costs_tmp_df)
			if assets_df.empty:
				assets_df = assets_tmp_df
			else:
				assets_df = assets_df.append(assets_tmp_df)
		return assets_df

	# delete assets data from certain table and backup
	def delete_from_oracle(self, delete_sql_str):
		delete_columns_str = (re.findall('delete(.*)from',delete_sql_str))[0].replace(' ','')
		if delete_columns_str == '':
			delete_sql_chg_str =  delete_sql_str[0:6] + ' *'  + delete_sql_str[6:]
		else:
			delete_sql_chg_str = delete_sql_str
		delete_sql_chg_str = delete_sql_chg_str.replace('delete','select')
		if (re.findall('.*(portAsset).*',delete_sql_str)) != []:
			insert_sql = '''insert into portAsset_bak ''' + delete_sql_chg_str
		if (re.findall('.*(portHolding).*',delete_sql_str)) != []:
			insert_sql = '''insert into portHolding_bak ''' + delete_sql_chg_str
		print insert_sql
		oracle.sql_dml(insert_sql, self.db)
		delete_sql = delete_sql_str
		oracle.sql_dml(delete_sql, self.db)
		print('delete and backup finish')

	# check whether the assets data is reload
	def assets_check_reload(self,select_constraint_list):
		select_sql = '''
              select * from portAsset
              where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]} AND values_costs = '{0[3]}'
              '''.format(select_constraint_list)
		rs = oracle.sql_select(select_sql, self.db)
		if rs != []:
			update_sql = '''
                  update portAsset set LCD = sysdate, LCU = ora_login_user, DATA_STATUS = \'0\'
                  where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]} AND values_costs = '{0[3]}'
                  '''.format(select_constraint_list)
			oracle.sql_dml(update_sql, self.db)

	# insert assets into oracle
	def assets_insert_into_oracle(self,assets_df):
		assets_df['l_date'] = assets_df.index
		df_index_len_int = len(assets_df.index)
		df_columns_list = list(assets_df.columns)
		df_columns_list_str = ','.join(df_columns_list)
		i = 0
		while i < df_index_len_int:
			# check reload
			insert_list = [df_columns_list_str]
			fund_id_str = assets_df.iloc[i]['fund_id']
			port_id_str = assets_df.iloc[i]['port_id']
			l_date_str = assets_df.iloc[i]['l_date']
			values_costs_str = assets_df.iloc[i]['values_costs']
			select_constraint_list = [fund_id_str, port_id_str, l_date_str, values_costs_str]
			self.assets_check_reload(select_constraint_list)
			# transfor format
			for item in assets_df.iloc[i]:
				if isinstance(item, np.float64):
					item = item.item()
				if item is None:
					item ='null'
				if isinstance(item, float):
					if math.isnan(item):
						item = 'null'
				if isinstance(item, unicode):
					item = float(item.replace(',', ''))
				insert_list.append(item)
			i = i + 1
			insert_sql = '''
                  insert into portAsset
                  (id,{0[0]},FCU,LCU)
                  VALUES(S_portAsset.Nextval,{0[1]},{0[2]},{0[3]},{0[4]},{0[5]},{0[6]},{0[7]},
                         {0[8]},{0[9]},{0[10]},{0[11]},{0[12]},{0[13]},{0[14]},{0[15]},{0[16]},
                         {0[17]},{0[18]},'{0[19]}','{0[20]}',{0[21]},{0[22]},{0[23]},{0[24]},
                         {0[25]},{0[26]},'{0[27]}','{0[28]}',ora_login_user,ora_login_user)
                  '''.format(insert_list)
			oracle.sql_dml(insert_sql, self.db)

	# check whether the holdings data is reload
	def holdings_check_reload(self,select_constraint_list):
		select_sql = '''
              select * from portHolding
              where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]} AND account_id = '{0[3]}'
              '''.format(select_constraint_list)
		rs = oracle.sql_select(select_sql, self.db)
		if rs != []:
			update_sql = '''
                  update portHolding set LCD = sysdate, LCU = ora_login_user, DATA_STATUS = \'0\'
                  where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date = {0[2]} AND account_id = '{0[3]}'
                  '''.format(select_constraint_list)
			oracle.sql_dml(update_sql, self.db)

	# insert holdings into oracle
	def holdings_insert_into_oracle(self, holdings_df):
		df_index_len_int = len(holdings_df.index)
		df_columns_list = list(holdings_df.columns)
		df_columns_list_str = ','.join(df_columns_list)
		i = 0
		while i < df_index_len_int:
			# check reload
			insert_list = [df_columns_list_str]
			fund_id_str = holdings_df.iloc[i]['fund_id']
			port_id_str = holdings_df.iloc[i]['port_id']
			l_date_str  = holdings_df.iloc[i]['l_date']
			account_id_str = holdings_df.iloc[i]['account_id']
			select_constraint_list = [fund_id_str, port_id_str, l_date_str, account_id_str]
			self.holdings_check_reload(select_constraint_list)
			# transfor format
			j = 0
			for item in holdings_df.iloc[i]:
				columns_name_str =  holdings_df.columns[j]
				if isinstance(item, np.float64):
					item = item.item()
				if item is None:
					if columns_name_str == 'market_no':
						item = ''
					elif columns_name_str == 'sub_market_no':
						item = ''
					elif columns_name_str == 'security_type':
						item = ''
					elif columns_name_str == 'wind_security_code':
						item = ''
					else:
						item ='null'
				if isinstance(item, float):
					if math.isnan(item):
						item = 'null'
				if isinstance(item, unicode):
					item = item.encode('UTF-8')
					if item.find(',') != -1:
						item = item.replace(',', '')
				insert_list.append(item)
				j = j + 1
			i = i + 1
			insert_sql = '''
                  insert into portHolding
                  (id,{0[0]},FCU,LCU)
                  VALUES(S_portHolding.Nextval,'{0[1]}','{0[2]}',{0[3]},{0[4]},{0[5]},{0[6]},{0[7]},
                         '{0[8]}',{0[9]},'{0[10]}',{0[11]},{0[12]},{0[13]},'{0[14]}','{0[15]}',{0[16]},
                         {0[17]},{0[18]},'{0[19]}','{0[20]}','{0[21]}',{0[22]},{0[23]},'{0[24]}',ora_login_user,ora_login_user)
                  '''.format(insert_list)
			oracle.sql_dml(insert_sql, self.db)

	# filter account id
	def account_id_filter(self, to_process_str):
		if isinstance(to_process_str, unicode):
			if re.findall('^(\d+).*', to_process_str) != []:
				if len(to_process_str) > 10 or len(to_process_str) == 6:
					return True
				else:
					return False
			else:
				return False
		else:
			return False

	# calculate buy amount, sale amount, buy cash, sale cash in holdings dataframe
	def calc_holdings_return(self,holdings_df):
		holdings_tmp_df = pd.DataFrame()
		# group by port_id
		grouped_port_id = holdings_df.groupby('port_id')
		for grouped_tumple in grouped_port_id:
			grouped_df = pd.DataFrame()
			grouped_df = grouped_tumple[1].copy()
			# make history dataFrame for shift
			fund_id_str = ((grouped_df['fund_id']).drop_duplicates('first'))[0]
			port_id_str = ((grouped_df['port_id']).drop_duplicates('first'))[0]
			start_date_str = '19700101'
			end_date_str   = str(int(list(grouped_df['l_date'])[0])-1)
			select_list = [fund_id_str, port_id_str, start_date_str, end_date_str]
			select_sql = '''
                         select L_DATE,ACCOUNT_ID,AMOUNT,BEGIN_AMOUNT,BUY_AMOUNT,SALE_AMOUNT,BUY_CASH,SALE_CASH,TOTAL_COST from portHolding 
                         where fund_id = '{0[0]}' AND port_id = '{0[1]}' AND l_date BETWEEN {0[2]} AND {0[3]} AND DATA_STATUS = '1' 
                         '''.format(select_list)
			history_df = pd.read_sql(select_sql, self.engine)
			if history_df.empty:
				empty_df = (grouped_df.drop_duplicates('account_id')).copy()
				empty_df.loc[:,'amount'] = 0
				empty_df.loc[:,'total_cost'] = 0
				empty_df.loc[:,'l_date'] = str(int(list(grouped_df['l_date'])[0])-1)
				history_df = empty_df.copy()
			grouped_tmp_df = grouped_df.copy()
			grouped_tmp_df = history_df.append(grouped_tmp_df) 
			grouped_tmp_df = grouped_tmp_df[['l_date','account_id','amount','begin_amount','buy_amount','sale_amount','buy_cash','sale_cash','total_cost']].copy()
			holding_df = pd.DataFrame()
			# group by account id
			grouped_account_id = grouped_tmp_df.groupby('account_id')
			for grouped_tumple in grouped_account_id:
				grouped_df = pd.DataFrame()
				grouped_df = grouped_tumple[1].copy()
				grouped_df.index = grouped_df['l_date']
				grouped_tmp_df = grouped_df.copy()
				grouped_tmp_df.loc[:,'begin_amount'] = grouped_tmp_df['amount'].shift(1)
				grouped_tmp_df.loc[:,'begin_cost'] = grouped_tmp_df['total_cost'].shift(1)
				# calculate amount and cash
				for date in grouped_tmp_df.index:
					amount_dif = grouped_tmp_df.loc[date,'amount'] - grouped_tmp_df.loc[date,'begin_amount']
					if amount_dif == 0:
						grouped_tmp_df.loc[date,'sale_amount'] = 0
						grouped_tmp_df.loc[date,'buy_amount']  = 0
					elif amount_dif > 0:
						grouped_tmp_df.loc[date,'sale_amount'] = 0
						grouped_tmp_df.loc[date,'buy_amount']  = amount_dif
					elif amount_dif < 0:
						grouped_tmp_df.loc[date,'sale_amount'] = -amount_dif
						grouped_tmp_df.loc[date,'buy_amount']  = 0
					if math.isnan(grouped_tmp_df.loc[date,'sale_amount']) and math.isnan(grouped_tmp_df.loc[date,'buy_amount']):
						grouped_tmp_df.loc[date,'begin_amount'] = None
					cash_dif =  grouped_tmp_df.loc[date,'total_cost'] - grouped_tmp_df.loc[date,'begin_cost']
					if cash_dif == 0:
						grouped_tmp_df.loc[date,'sale_cash'] = 0
						grouped_tmp_df.loc[date,'buy_cash']  = 0
					elif cash_dif > 0:
						grouped_tmp_df.loc[date,'sale_cash'] = 0
						grouped_tmp_df.loc[date,'buy_cash']  = cash_dif
					elif cash_dif < 0:
						grouped_tmp_df.loc[date,'sale_cash'] = -cash_dif
						grouped_tmp_df.loc[date,'buy_cash']  = 0
				holding_df = holding_df.append(grouped_tmp_df)
			# drop history data
			holding_df.sort_index(inplace = True)
			def drop_list(date):
				if date <= select_list[3]:
					return True
				else:
					return False
			droped_list =  filter(drop_list, holding_df.index)
			holding_df.drop(droped_list, inplace = True)
			# joint dataFrame
			holdings_tmp_df = holdings_tmp_df.append(holding_df)
		holdings_tmp_df.rename(columns={'account_id':'account_id_x','amount':'amount_x','begin_amount':'begin_amount_x',\
                                        'buy_amount':'buy_amount_x','sale_amount':'sale_amount_x','buy_cash':'buy_cash_x',\
                                        'sale_cash':'sale_cash_x','total_cost':'total_cost_x','l_date':'l_date_x'}, inplace=True)
		holdings_df = pd.merge(holdings_df,holdings_tmp_df,left_on=['account_id','l_date'] , right_on=['account_id_x','l_date_x'])
		# assign and drop
		holdings_df.loc[:,'begin_amount'] = holdings_df['begin_amount_x']
		holdings_df.loc[:,'buy_amount']   = holdings_df['buy_amount_x']
		holdings_df.loc[:,'sale_amount']  = holdings_df['sale_amount_x']
		holdings_df.loc[:,'buy_cash']     = holdings_df['buy_cash_x']
		holdings_df.loc[:,'sale_cash']    = holdings_df['sale_cash_x']
		holdings_df.drop(['account_id_x','amount_x','begin_amount_x','buy_amount_x','sale_amount_x','buy_cash_x',\
                          'sale_cash_x','total_cost_x','begin_cost','l_date_x'],axis=1,inplace = True)
		return holdings_df

	# function for debug
	def debug(self):
		## holdings
		holdings_df = pd.DataFrame()
		holdings_df = self.get_holdings_return()
		## holdings with calculation
		holdings_df = self.calc_holdings_return(holdings_df)
		self.holdings_insert_into_oracle(holdings_df)
		## assets 
		assets_df = pd.DataFrame()
		assets_df = self.get_assets_return()
		self.assets_insert_into_oracle(assets_df)
		## extracte into excel
		# assets_df.to_excel('C:\\Users\\M.Me_Too\\Desktop\\'+'debug.xls')
		## delete sql language
		# delete_sql_str = raw_input('please enter the sql statement(enable table portAsset, portHolding):\n')
		# self.delete_from_oracle(delete_sql_str)
		# select_translate.translate()
