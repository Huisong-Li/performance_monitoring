#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: github.com/Huisong-Li
# Date  : 03/11/2016

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

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class assemble():
	def __init__(self):
		self.abspath_str =  os.path.abspath(".") + '\\performance_monitoring'
		with open(self.abspath_str+'\\configuration\\administrator.json', 'r') as file:
			administrator_dict = json.load(file)
		self.db = oracle.connect_db()
		self.engine = oracle.engine_db()

	def fund_holdings(self, fund_id):
		sql = '''select * from portholding where fund_id = '{}' and port_id != 'P00000' and data_status = '1' '''.format(fund_id)
		df = pd.read_sql(sql, self.engine)
		fund_df = pd.DataFrame()
		grouped_l_date = df.groupby('l_date')
		for grouped_tumple in grouped_l_date:
			grouped_grouped_l_date_df = pd.DataFrame()
			grouped_grouped_l_date_df = grouped_tumple[1].copy()
			grouped_account_id = grouped_grouped_l_date_df.groupby('account_id')
			fund_date_tmp_df = pd.DataFrame()
			for grouped_tumple in grouped_account_id:
				fund_id_tmp_df = pd.DataFrame()
				grouped_account_id_df = pd.DataFrame()
				grouped_account_id_df = grouped_tumple[1].copy()
				fund_id_tmp_df = grouped_account_id_df.iloc[0,:].copy()
				to_sum_df = grouped_account_id_df[['amount','total_cost','market_value','pandl','begin_amount','buy_amount','sale_amount','buy_cash','sale_cash','buy_fee','sale_fee']]
				to_weight_df = grouped_account_id_df[['amount','unit_cost']]
				sum_numbers  = to_sum_df.sum(axis = 0, numeric_only = True)
				fund_id_tmp_df['port_id'] = 'P00000'
				for index_name in sum_numbers.index:
					fund_id_tmp_df[index_name] = sum_numbers[index_name]
				fund_id_tmp_df['unit_cost'] = np.average(to_weight_df['unit_cost'], weights=to_weight_df['amount'])
				fund_date_tmp_df = fund_date_tmp_df.append(fund_id_tmp_df)
			fund_df = fund_df.append(fund_date_tmp_df)
		fund_df.drop(['id','fcd','fcu','lcd','lcu','data_status'],axis = 1,inplace = True)
		return fund_df

	def fund_assets(self, fund_id):
		sql = '''select * from portasset where fund_id = '{}' and port_id != 'P00000' and data_status = '1' '''.format(fund_id)
		df = pd.read_sql(sql, self.engine)
		fund_df = pd.DataFrame()
		grouped_l_date = df.groupby('l_date')
		for grouped_tumple in grouped_l_date:
			grouped_grouped_l_date_df = pd.DataFrame()
			grouped_grouped_l_date_df = grouped_tumple[1].copy()
			grouped_values_costs = grouped_grouped_l_date_df.groupby('values_costs')
			fund_date_tmp_df = pd.DataFrame()
			for grouped_tumple in grouped_values_costs:
				fund_id_tmp_df = pd.DataFrame()
				grouped_values_costs_df = pd.DataFrame()
				grouped_values_costs_df = grouped_tumple[1].copy()
				fund_id_tmp_df = grouped_values_costs_df.iloc[0,:].copy()
				to_sum_df = grouped_values_costs_df[['deposit_asset', 'deposit_reservation', 'deposit_recognizance', 'stock_asset', 'bond_asset', 'assets_backed_security','fund_asset', 'financial_products', 'repo_asset', 'dividend_receivable', 'interest_receivable', 'subscription_receivable','other_receivables', 'bad_debt_reserves', 'liquidation_security', 'futures_asset', 'accumulate_profit', 'allocatble_profit','net_assets', 'total_assets', 'credit_value']]
				to_weight_df = grouped_values_costs_df[['net_assets','unit_value','accumulate_unit_value']]
				sum_numbers  = to_sum_df.sum(axis = 0, numeric_only = True)
				fund_id_tmp_df['port_id'] = 'P00000'
				for index_name in sum_numbers.index:
					fund_id_tmp_df[index_name] = sum_numbers[index_name]
				fund_id_tmp_df['unit_value'] = np.average(to_weight_df['unit_value'], weights=to_weight_df['net_assets'])
				fund_id_tmp_df['accumulate_unit_value'] = np.average(to_weight_df['accumulate_unit_value'], weights=to_weight_df['net_assets'])
				fund_date_tmp_df = fund_date_tmp_df.append(fund_id_tmp_df)
			fund_date_tmp_df.loc[:,'unit_value_yesterday'] = fund_date_tmp_df.loc[:,'unit_value'].shift(1)
			fund_df = fund_df.append(fund_date_tmp_df)
		fund_df.drop(['id','fcd','fcu','lcd','lcu','data_status'],axis = 1,inplace = True)
		#fund_df.to_excel('C:\\Users\\M.Me_Too\\Desktop\\'+'debug.xls')
		return fund_df

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
		#assets_df.to_excel('C:\\Users\\M.Me_Too\\Desktop\\'+'debug.xls')
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
				if item == 0:
					item = 'null'
				insert_list.append(item)
			i = i + 1
			insert_sql = '''
                  insert into portAsset
                  (id,{0[0]},FCU,LCU)
                  VALUES(S_portAsset.Nextval,{0[1]},{0[2]},{0[3]},{0[4]},{0[5]},{0[6]},{0[7]},{0[8]},
                        {0[9]},{0[10]},{0[11]},{0[12]},{0[13]},'{0[14]}',{0[15]},{0[16]},'{0[17]}',
                        {0[18]},{0[19]},{0[20]},'{0[21]}',{0[22]},{0[23]},{0[24]},{0[25]},{0[26]},
                        {0[27]},'{0[28]}',ora_login_user,ora_login_user)
                  '''.format(insert_list)
			#print insert_sql
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
				if columns_name_str == 'citics_industry_code' and item == 'null':
						item = ''
				if columns_name_str == 'citics_industry_name' and item == 'null':
						item = ''
				insert_list.append(item)
				j = j + 1
			i = i + 1
			insert_sql = '''
                  insert into portHolding
                  (id,{0[0]},FCU,LCU)
                  VALUES(S_portHolding.Nextval,'{0[1]}','{0[2]}',{0[3]},{0[4]},{0[5]},{0[6]},{0[7]},
                         '{0[8]}','{0[9]}','{0[10]}','{0[11]}','{0[12]}',{0[13]},{0[14]},{0[15]},{0[16]},
                         {0[17]},'{0[18]}','{0[19]}',{0[20]},{0[21]},{0[22]},'{0[23]}','{0[24]}','{0[25]}',{0[26]},{0[27]},'{0[28]}',ora_login_user,ora_login_user)
                  '''.format(insert_list)
			oracle.sql_dml(insert_sql, self.db)

	def debug(self):
		# pass
		fund_df = self.fund_assets('F00002')
		self.assets_insert_into_oracle(fund_df)
		fund_df = self.fund_holdings('F00002')
		self.holdings_insert_into_oracle(fund_df)