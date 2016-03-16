#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: github.com/Huisong-Li
# Date  : 01/28/2016


import os
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from datetime import *

import WindPy as wind
import pandas as pd
from pandas import Series, DataFrame
import xlsxwriter

from ..utils import oracle

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'



class holdings_engine():
	def __init__(self):
		self.abspath_str =  os.path.abspath(".") + '\\performance_monitoring'
		with open(self.abspath_str+'\\configuration\\user_io.json', 'r') as file:
			user_io_dict = json.load(file)
		self.dest_file_directory_str = user_io_dict['destination']['file_directory']
		self.holdings_file_name_str = user_io_dict['destination']['holdings_file_name']
		with open(self.abspath_str+'\\configuration\\user_select.json', 'r') as file:
			user_select_dict = json.load(file)
		self.start_date_str = user_select_dict['basic']['start_date']
		self.end_date_str   = user_select_dict['basic']['end_date']
		#self.fund_names = user_select_dict['basic']['fund_names']
		#self.port_names = user_select_dict['basic']['port_names']
		self.fund_names = None
		self.port_names = None
		self.benchmark_code_str = user_select_dict['basic']['benchmark_code']
		self.ranking_table_num = user_select_dict['holdings']['ranking_table_num']
		self.weight_table_num = user_select_dict['holdings']['ranking_table_num']
		#self.select_list = [self.fund_names, self.port_names, self.start_date_str, self.end_date_str, self.ranking_table_num]
		self.select_list = [self.start_date_str, self.end_date_str, self.ranking_table_num]
		self.title_list = [self.weight_table_num, self.start_date_str, self.end_date_str]
		self.engine = oracle.engine_db()

	# 组合权重股表
	def top_stocks(self):
		sql = '''
              select 股票代码, 股票名称, 期间平均持仓占比, 期间最大持仓占比, 期间最小持仓占比 from
              (select a.SECURITY_CODE as 股票代码, a.ACCOUNT_NAME as 股票名称, SUM(a.MARKET_VALUE/b.TOTAL_ASSETS) as 期间累计持仓占比, AVG(a.MARKET_VALUE/b.TOTAL_ASSETS) as 期间平均持仓占比, MAX(a.MARKET_VALUE/b.TOTAL_ASSETS) as 期间最大持仓占比, MIN(a.MARKET_VALUE/b.TOTAL_ASSETS) as 期间最小持仓占比 from
              ((SELECT L_DATE, SECURITY_CODE, ACCOUNT_NAME, MARKET_VALUE FROM Portholding
              WHERE FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}'  AND SECURITY_TYPE = 'stock' AND DATA_STATUS = '1') a
              left join
              (select L_DATE, TOTAL_ASSETS from portasset
              WHERE FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' AND VALUES_COSTS = 'values' AND DATA_STATUS = '1') b
              on a.L_DATE = b.L_DATE)
              GROUP BY SECURITY_CODE,ACCOUNT_NAME
              ORDER BY 期间累计持仓占比 DESC)
              WHERE ROWNUM <= {0[4]}
              '''.format(self.select_list)
		df = pd.read_sql(sql, self.engine)
		df.index=df[u'股票代码']
		df.drop(u'股票代码',axis=1,inplace=True)
		return df

	# 组合行业市值与权重表 
	def industry_value_weight(self):
		sql = '''
              select * from Portholding 
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and security_type = 'stock' and citics_industry_code is not null and data_status = '1'
              '''.format(self.select_list)
		df = pd.read_sql(sql, self.engine)
		industry_name_list = list(set(list(df.loc[:,'citics_industry_name'])))
		industry_value_list = []
		for industry_name in industry_name_list:
			industry_value_list.append(industry_name + '_市值')
		industry_pct_list = []
		for industry_name in industry_name_list:
			industry_pct_list.append(industry_name + '_占比')
		df_columns_list = industry_value_list + industry_pct_list
		industry_value_weight_df = pd.DataFrame()
		grouped_l_date = df.groupby('l_date')
		for grouped_tumple in grouped_l_date:
			grouped_l_date_df = pd.DataFrame()
			l_date_str = grouped_tumple[0]
			grouped_l_date_df = grouped_tumple[1].copy()
			stock_assets_num = (grouped_l_date_df['market_value']).sum()
			grouped_industry_name = grouped_l_date_df.groupby('citics_industry_name')
			industry_value_weight_tmp_df = pd.DataFrame(index = [l_date_str],columns = df_columns_list )
			for grouped_tumple in grouped_industry_name:
				grouped_industry_name_df = pd.DataFrame()
				industry_name_str =  grouped_tumple[0].decode('UTF-8')
				grouped_industry_name_df = grouped_tumple[1].copy()
				industry_value_num = (grouped_industry_name_df['market_value']).sum()
				industry_pct_num = industry_value_num/stock_assets_num
				industry_value_weight_tmp_df['{}_市值'.format(industry_name_str)] = [industry_value_num]
				industry_value_weight_tmp_df['{}_占比'.format(industry_name_str)] = [industry_pct_num]
			if industry_value_weight_df.empty:
				industry_value_weight_df = industry_value_weight_tmp_df
			else:
				industry_value_weight_df = industry_value_weight_df.append(industry_value_weight_tmp_df)
		return industry_value_weight_df

	# 组合日收益率与净值表 
	## TODO 添加其他持仓数
	def daily_return_net_value(self):
		sql = '''
              select a.L_DATE,a.TOTAL_ASSETS as 组合总市值,a.UNIT_VALUE as 组合日净值, a.return as 组合日收益率, a.STOCK_ASSET as 股票持仓市值, a.stock_percent as 股票仓位占比, a.cash as 现金金额, a.cash_percent as 现金仓位占比, a.dres as 清算备付金, a.dresp as 清算备付金占比, a.drec as 存出保证金, a.drecp as 存出保证金占比, a.bond as 债券持仓, a.bondp as 债券持仓占比, a.abse as 资产支持证券投资,a.absep as 资产支持证券投资占比,a.fund as 基金持仓, a.fundp as 基金持仓占比,a.fp as 理财产品投资,a.fpp as 理财产品投资占比,a.repo as 买入返售金额持仓,a.repop as 买入返售金额持仓占比,a.direc as 应收股利,a.direcp as 应收股利占比, a.ir as 应收利息 , a.irp as 应收利息占比 ,a.sr as 应收申购款,a.srp as 应收申购款占比, a.ore as 其他应收款, a.orep as 其他应收款占比, a.bdr as 待摊费用, a.bdrp as 待摊费用占比, a.ls as 证券清算款, a.lsp as 证券清算款占比, a.future as 其他衍生工具持仓, a.futurep as 其他衍生工具持仓占比,b.position/a.TOTAL_ASSETS as 股票当日加减仓比例 from
              ((select L_DATE, FUND_ID, PORT_ID,UNIT_VALUE, TOTAL_ASSETS, ((UNIT_VALUE-UNIT_VALUE_YESTERDAY)/UNIT_VALUE_YESTERDAY)*100 as return, STOCK_ASSET, (STOCK_ASSET/TOTAL_ASSETS) as stock_percent, 
                       DEPOSIT_ASSET as cash, (DEPOSIT_ASSET/TOTAL_ASSETS) as cash_percent, DEPOSIT_RESERVATION  as dres, DEPOSIT_RESERVATION/TOTAL_ASSETS dresp, DEPOSIT_RECOGNIZANCE as drec, DEPOSIT_RECOGNIZANCE/TOTAL_ASSETS as drecp,
                       BOND_ASSET as bond, BOND_ASSET/TOTAL_ASSETS as bondp, ASSETS_BACKED_SECURITY as abse, ASSETS_BACKED_SECURITY/TOTAL_ASSETS as absep, FUND_ASSET as fund, FUND_ASSET/TOTAL_ASSETS as fundp,FINANCIAL_PRODUCTS as fp, FINANCIAL_PRODUCTS/TOTAL_ASSETS as fpp,
                       REPO_ASSET as repo, REPO_ASSET/TOTAL_ASSETS as repop, DIVIDEND_RECEIVABLE as direc, DIVIDEND_RECEIVABLE/TOTAL_ASSETS as direcp, INTEREST_RECEIVABLE as ir, INTEREST_RECEIVABLE/TOTAL_ASSETS as irp,SUBSCRIPTION_RECEIVABLE as sr, SUBSCRIPTION_RECEIVABLE/TOTAL_ASSETS as srp, 
                       OTHER_RECEIVABLES as ore,OTHER_RECEIVABLES/TOTAL_ASSETS as orep, BAD_DEBT_RESERVES as bdr, BAD_DEBT_RESERVES/TOTAL_ASSETS as bdrp, LIQUIDATION_SECURITY as ls, LIQUIDATION_SECURITY/TOTAL_ASSETS as lsp,FUTURES_ASSET as future, FUTURES_ASSET/TOTAL_ASSETS as futurep
              from portasset
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and values_costs = 'values' )a
              left join
              (select L_DATE, FUND_ID, PORT_ID, SUM(BUY_CASH-SALE_CASH) as position
              from Portholding
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and security_type = 'stock'
              group by L_DATE, FUND_ID, PORT_ID)b
              on a.L_DATE = b.L_DATE and a.FUND_ID  = b.FUND_ID and a.PORT_ID = b.PORT_ID)
			  order by L_DATE
              '''.format(self.select_list)
		df_tmp = pd.read_sql(sql, self.engine)
		df_tmp.index=df_tmp['l_date']
		df_tmp.drop('l_date',axis=1,inplace=True)
		benchmark_df=self.get_benchmark_return()
		df = pd.merge(df_tmp, benchmark_df, left_index=True,right_index=True,how='inner')
		df.loc[:,u'日超额收益率'] = df[u'组合日收益率']-df[u'日基准收益率']
		df.loc[:,u'组合日收益率'] = df[u'组合日收益率']/100
		df.loc[:,u'日基准收益率'] = df[u'日基准收益率']/100
		df.loc[:,u'日超额收益率'] = df[u'日超额收益率']/100
		df.dropna(axis = 1,how = 'all', inplace = True)
		return df

	# 组合持仓查询
	def select_holding(self):
		sql = '''
		      select  a.L_DATE, a.SECURITY_CODE as 股票代码, a.ACCOUNT_NAME as 股票名称,a.AMOUNT as 持仓数量, a.MARKET_VALUE as 持仓金额, a.MARKET_VALUE/b.TOTAL_ASSETS as 持仓占比 from
              ((SELECT L_DATE, SECURITY_CODE, ACCOUNT_NAME, AMOUNT, MARKET_VALUE FROM Portholding
              WHERE FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and'{0[3]}'  AND SECURITY_TYPE = 'stock' AND DATA_STATUS = '1')a
              left join
              (select L_DATE, TOTAL_ASSETS from portasset
              WHERE FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and'{0[3]}' AND VALUES_COSTS = 'values' AND DATA_STATUS = '1')b
              on a.L_DATE = b.L_DATE)
			  '''.format(self.select_list)
		df = pd.read_sql(sql, self.engine)
		df.index = df['l_date']
		df.drop('l_date',axis=1,inplace=True)
		df.sort_index(inplace = True)
		return df

	# 组合期间排名查询
	def select_rank(self):
		# 涨跌幅
		sql = '''
              select L_DATE, SECURITY_CODE, ACCOUNT_NAME, MARKET_PRICE, UNIT_COST  from portholding
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and'{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock' 
              '''.format(self.select_list)
		df = pd.read_sql(sql, self.engine)
		security_code_grouped = df.groupby(df['security_code'])
		df = pd.DataFrame()
		code_list = []
		dft_list = []
		for grouped_temple in security_code_grouped:
			security_code_str = grouped_temple[0]
			grouped_df = grouped_temple[1]
			grouped_df.index = grouped_df['l_date']
			grouped_df.drop('l_date',axis=1,inplace=True)
			code_list.append(security_code_str)
			price_tmp = grouped_df.drop(['security_code','account_name', 'unit_cost'],axis = 1)
			cost_tmp = grouped_df.sort_index()['unit_cost']
			df_temp = pd.DataFrame()
			df_temp['l_date'] = [str(int(sorted(list(grouped_df.index))[0])-1)]
			df_temp['market_price'] = [cost_tmp[0]]
			df_temp.index=df_temp['l_date']
			df_temp.drop('l_date',axis=1,inplace=True)
			df_temp = df_temp.append(price_tmp)
			dft_change_tmp=df_temp.pct_change()
			period_chg=(dft_change_tmp+1).cumprod()-1
			dft_list.append(period_chg['market_price'][-1])
		df[u'股票代码'] = code_list
		df[u'幅度'] =  dft_list
		df.index=df[u'股票代码']
		df.drop(u'股票代码',axis=1,inplace=True)

		sql = '''
              select DISTINCT SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称  from portholding
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and'{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock' 
              '''.format(self.select_list)
		df_tmp = pd.read_sql(sql, self.engine)
		df_tmp.index=df_tmp[u'股票代码']
		df_tmp.drop(u'股票代码',axis=1,inplace=True)
		df = pd.merge(df_tmp, df, left_index=True,right_index=True,how='inner')
		# 涨幅
		df_increase = df.sort([u'幅度'],ascending=False)
		df_increase = df_increase.dropna().head(self.select_list[4])
		# 跌幅
		df_decrease = df.sort([u'幅度'],ascending=True)
		df_decrease = df_decrease.dropna().head(self.select_list[4])
		
		# PANDL 跌
		sql_pandl_de = '''
                      select * FROM
                      (select SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, sum(PANDL) as 金额 from
                      (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, PANDL  from portholding
                      where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                      group by SECURITY_CODE, ACCOUNT_NAME
                      order by 金额)
                      WHERE ROWNUM <= {0[4]}
                      '''.format(self.select_list)
		# PANDL 涨
		sql_pandl_in = '''
                      select * FROM
                      (select SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, sum(PANDL) as 金额 from
                      (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, PANDL  from portholding
                      where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                      group by SECURITY_CODE, ACCOUNT_NAME
                      order by 金额 DESC)
                      WHERE ROWNUM <= {0[4]}
                      '''.format(self.select_list) 
		
		# 减仓金额
		sql_position_de = '''
		                 select * FROM
		                 (select SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, sum(BUY_CASH)-sum(SALE_CASH) as 金额 from
                         (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, BUY_CASH, SALE_CASH  from portholding
                         where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}'  and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                         group by SECURITY_CODE, ACCOUNT_NAME
                         order by 金额 DESC)
                         WHERE ROWNUM <= {0[4]}
                         '''.format(self.select_list)
		
		# 增仓金额
		sql_position_in = '''
		                 select * FROM
		                 (select SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, sum(BUY_CASH)-sum(SALE_CASH) as 金额 from
                         (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, BUY_CASH, SALE_CASH  from portholding
                         where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}'  and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                         group by SECURITY_CODE, ACCOUNT_NAME
                         order by 金额)
                         WHERE ROWNUM <= {0[4]}
                         '''.format(self.select_list)
		
		select_rank_dic = {}
		select_rank_dic['increase'] = df_increase
		select_rank_dic['decrease'] = df_decrease
		select_rank_dic['pandl_de'] = pd.read_sql(sql_pandl_de, self.engine)
		select_rank_dic['pandl_de'].index=select_rank_dic['pandl_de'][u'股票代码']
		select_rank_dic['pandl_de'].drop(u'股票代码',axis=1,inplace=True)
		select_rank_dic['pandl_in'] = pd.read_sql(sql_pandl_in, self.engine)
		select_rank_dic['pandl_in'].index=select_rank_dic['pandl_in'][u'股票代码']
		select_rank_dic['pandl_in'].drop(u'股票代码',axis=1,inplace=True)
		select_rank_dic['position_de'] = pd.read_sql(sql_position_de, self.engine)
		select_rank_dic['position_de'].index=select_rank_dic['position_de'][u'股票代码']
		select_rank_dic['position_de'].drop(u'股票代码',axis=1,inplace=True)
		select_rank_dic['position_in'] = pd.read_sql(sql_position_in, self.engine)
		select_rank_dic['position_in'].index=select_rank_dic['position_in'][u'股票代码']
		select_rank_dic['position_in'].drop(u'股票代码',axis=1,inplace=True)
		return select_rank_dic

	# 对冲损益查询(损益)
	def hedge_profit(self):
		sql_test = '''
		           select FUND_ID, PORT_ID, L_DATE, SUM(MARKET_VALUE) as future from portholding
                   where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and SECURITY_TYPE = 'future' and POSITION_FLAG = 'short' and DATA_STATUS = '1'
                   group by FUND_ID, PORT_ID ,L_DATE
                   '''.format(self.select_list)
		df = pd.read_sql(sql_test, self.engine)
		if list(df.index) == []:
			sql = '''
                  select L_DATE, STOCK_ASSET as 对冲前组合市值 , STOCK_ASSET as 对冲后组合市值,  (STOCK_ASSET- STOCK_ASSET) as 期货头寸, STOCK_ASSET as 风险敞口 from
                  (select STOCK_ASSET, FUND_ID, PORT_ID ,L_DATE from Portasset
                  where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and VALUES_COSTS = 'values' )                  
                  '''.format(self.select_list)
			df = pd.read_sql(sql, self.engine)
			df.index=df['l_date']
			df.drop('l_date',axis=1,inplace=True)
		else:
			sql = '''
                  select a.L_DATE, b.STOCK_ASSET as 对冲前组合市值 , (b.STOCK_ASSET-a.future) as 对冲后组合市值, a.future as 期货头寸, (b.STOCK_ASSET-a.future) as 风险敞口 from
                  ((select FUND_ID, PORT_ID, L_DATE, SUM(MARKET_VALUE) as future from portholding
                  where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and SECURITY_TYPE = 'future' and POSITION_FLAG = 'short' and DATA_STATUS = '1'
                  group by FUND_ID, PORT_ID ,L_DATE)a
                  left join
                  (select STOCK_ASSET, FUND_ID, PORT_ID ,L_DATE from Portasset
                  where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and VALUES_COSTS = 'values' )b
                  on a.FUND_ID = b.FUND_ID and a.PORT_ID = b.PORT_ID and a.L_DATE = b.L_DATE)
                  '''.format(self.select_list)
			df = pd.read_sql(sql, self.engine)
			df.index=df['l_date']
			df.drop('l_date',axis=1,inplace=True)
		return df

	# 对冲损益查询(标准差)
	def hedge_profit_stddev(self):
		sql_test = '''
		           select FUND_ID, PORT_ID, L_DATE, SUM(MARKET_VALUE) as future from portholding
                   where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and SECURITY_TYPE = 'future' and POSITION_FLAG = 'short' and DATA_STATUS = '1'
                   group by FUND_ID, PORT_ID ,L_DATE
                   '''.format(self.select_list)
		df = pd.read_sql(sql_test, self.engine)
		if list(df.index) == []:
			sql = '''
                  select stddev(before) as 对冲前组合 ,stddev(after) as 对冲后组合 from
                  (select L_DATE, FUND_ID, PORT_ID ,STOCK_ASSET as after , STOCK_ASSET as before from Portasset
                  where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and VALUES_COSTS = 'values' )b
                  '''.format(self.select_list)
			df = pd.read_sql(sql, self.engine)
			df.index=df[u'对冲前组合']
			df.drop(u'对冲前组合',axis=1,inplace=True)
		else:
			sql = '''
                  select stddev(a.before) as 对冲前组合 ,stddev(a.after) as 对冲后组合 from
                  (select a.L_DATE,a.FUND_ID, a.PORT_ID, b.STOCK_ASSET as before , (b.STOCK_ASSET-a.future) as after, a.future as future, (b.STOCK_ASSET-a.future) as window from
                  ((select FUND_ID, PORT_ID, L_DATE, SUM(MARKET_VALUE) as future from portholding
                  where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and SECURITY_TYPE = 'future' and POSITION_FLAG = 'short' and DATA_STATUS = '1'
                  group by FUND_ID, PORT_ID ,L_DATE)a
                  left join
                  (select STOCK_ASSET, FUND_ID, PORT_ID ,L_DATE from Portasset
                  where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and VALUES_COSTS = 'values' )b
                  on a.FUND_ID = b.FUND_ID and a.PORT_ID = b.PORT_ID and a.L_DATE = b.L_DATE))a
                  '''.format(self.select_list)
			df = pd.read_sql(sql, self.engine)
			df.index=df[u'对冲前组合']
			df.drop(u'对冲前组合',axis=1,inplace=True)
		return df

	# 组合行业换手率
	def industry_tr(self):
		sql = '''
              select a.* , b.total_assets from
              (select * from Portholding 
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and security_type = 'stock' and citics_industry_code is not null and data_status = '1')a
              left join
              (select l_date,sum(market_value) as total_assets from Portholding
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and data_status = '1'
              group by l_date)b
              on a.l_date = b.l_date
              '''.format(self.select_list)
		df = pd.read_sql(sql, self.engine)
		industry_name_list = list(set(list(df.loc[:,'citics_industry_name'])))
		industry_tr_df = pd.DataFrame()
		grouped_l_date = df.groupby('l_date')
		for grouped_tumple in grouped_l_date:
			grouped_l_date_df = pd.DataFrame()
			l_date_str = grouped_tumple[0]
			grouped_l_date_df = grouped_tumple[1].copy()
			value_num = list(grouped_l_date_df['total_assets'])[0]
			grouped_industry_name = grouped_l_date_df.groupby('citics_industry_name')
			industry_tr_tmp_df = pd.DataFrame(index = [l_date_str],columns = industry_name_list )
			for grouped_tumple in grouped_industry_name:
				grouped_industry_name_df = pd.DataFrame()
				industry_name_str =  grouped_tumple[0].decode('UTF-8')
				grouped_industry_name_df = grouped_tumple[1].copy()
				industry_turnover_num = (grouped_industry_name_df['buy_cash']).sum() + (grouped_industry_name_df['sale_cash']).sum()
				industry_tr = industry_turnover_num/value_num
				industry_tr_tmp_df['{}'.format(industry_name_str)] = [industry_tr]
			if industry_tr_df.empty:
				industry_tr_df = industry_tr_tmp_df
			else:
				industry_tr_df = industry_tr_df.append(industry_tr_tmp_df)
		return industry_tr_df

	# 取 基准 收益率
	def get_benchmark_return(self):
		benchmark_df = pd.DataFrame()
		start_date = self.start_date_str
		end_date = self.end_date_str
		start_date = start_date[0:4] + '-' + start_date[4:6] + '-' +start_date[6:8]
		end_date = end_date[0:4] + '-' + end_date[4:6] + '-' + end_date[6:8]
		wind.w.start()
		tmp=wind.w.wsd(self.benchmark_code_str, "pct_chg", start_date, end_date, "Fill=Previous;PriceAdj=F")
		benchmark_df[u'日基准收益率']=tmp.Data[0]
		benchmark_df['Date']=[str(datetime.strptime(x.strftime("%Y-%m-%d"),"%Y-%m-%d"))[0:10].replace('-','') for x in tmp.Times]
		benchmark_df.index=benchmark_df['Date']
		benchmark_df.drop('Date',axis=1,inplace=True)
		return benchmark_df

	# 将要写入EXCEL文件的dataframe组成一个列表
	def combine_performance(self):
		insert_dic = {}
		insert_dic['top_stocks'] = self.top_stocks()
		insert_dic['industry_value_weight'] = self.industry_value_weight()
		insert_dic['daily_return_net_value'] = self.daily_return_net_value()
		insert_dic['select_holding'] = self.select_holding()
		insert_dic['select_rank_decrease'] = self.select_rank()['decrease']
		insert_dic['select_rank_increase'] = self.select_rank()['increase']
		insert_dic['select_rank_pandl_de'] = self.select_rank()['pandl_de']
		insert_dic['select_rank_pandl_in'] = self.select_rank()['pandl_in']
		insert_dic['select_rank_position_de'] = self.select_rank()['position_de']
		insert_dic['select_rank_position_in'] = self.select_rank()['position_in']
		insert_dic['hedge_profit'] = self.hedge_profit()
		insert_dic['hedge_profit_stddev'] = self.hedge_profit_stddev()
		insert_dic['industry_tr'] = self.industry_tr()
		return insert_dic

	# save hodings performance to excel
	def save_file(self, insert_dic):
		# try:
			writer = pd.ExcelWriter(self.dest_file_directory_str + self.fund_names+'_'+self.port_names+'_'+self.holdings_file_name_str, engine='xlsxwriter')
			workbook = writer.book
			bold_format = workbook.add_format({'bold': True})
			percent_format = workbook.add_format({'num_format': '0.00%'})
			float_format = workbook.add_format({'num_format': '0.00'})
			border_format=workbook.add_format()
			border_format.set_border(1)
			merge_format = workbook.add_format({'bold': True,'align':'center','valign':'vcenter'})
			# 组合十大权重股表
			insert_dic['top_stocks'].to_excel(writer, sheet_name=u'组合{}大权重股表'.format(self.weight_table_num), startrow=4, startcol=1)
			worksheet = writer.sheets[u'组合{}大权重股表'.format(self.weight_table_num)]
			worksheet.write(2,1, u'组合{0[0]}大权重股表({0[1]},{0[2]}到{0[3]})'.format(self.title_list), bold_format)
			worksheet.conditional_format(4,1,4+self.weight_table_num,5, {'type': 'blanks','format':border_format})
			worksheet.conditional_format(4,1,4+self.weight_table_num,5, {'type': 'no_blanks','format':border_format})
			worksheet.set_column(1, 5, 16, float_format)
			worksheet.set_column(3, 5, 16, percent_format)
			# 组合行业市值与权重表
			date_dif = len(insert_dic['industry_value_weight'].index)
			columns_num = len(insert_dic['industry_value_weight'].columns)
			insert_dic['industry_value_weight'].to_excel(writer, sheet_name=u'组合行业市值与权重表', startrow=4, startcol=1)
			worksheet = writer.sheets[u'组合行业市值与权重表']
			worksheet.write(2,1, u'组合行业市值与权重表({0[1]},{0[2]}到{0[3]})'.format(self.title_list), bold_format)
			worksheet.conditional_format(4,1,4+date_dif,1+columns_num, {'type': 'blanks','format':border_format})
			worksheet.conditional_format(4,1,4+date_dif,1+columns_num, {'type': 'no_blanks','format':border_format})
			worksheet.set_column(1, 1+columns_num/2, 20, float_format)
			worksheet.set_column(1 + columns_num/2 + 1, 1+columns_num, 20, percent_format)
			# 组合日收益率_资产_净值表
			date_dif = len(insert_dic['daily_return_net_value'].index)
			columns_num = len(insert_dic['daily_return_net_value'].columns)
			insert_dic['daily_return_net_value'].to_excel(writer, sheet_name=u'组合日收益率_资产_净值表', startrow=4, startcol=1)
			worksheet = writer.sheets[u'组合日收益率_资产_净值表']
			worksheet.write(2,1, u'组合日收益率_资产_净值表({0[1]},{0[2]}到{0[3]})'.format(self.title_list), bold_format)
			worksheet.conditional_format(4,1,4+date_dif,1+columns_num, {'type': 'blanks','format':border_format})
			worksheet.conditional_format(4,1,4+date_dif,1+columns_num, {'type': 'no_blanks','format':border_format})
			worksheet.set_column(1, 1+columns_num, 18, float_format)
			worksheet.set_column(4, 4, 14, percent_format)
			worksheet.set_column(6, 6, 14, percent_format)
			worksheet.set_column(8, 8, 14, percent_format)
			worksheet.set_column(10, 10, 18, percent_format)
			worksheet.set_column(12, 12, 18, percent_format)
			worksheet.set_column(14, 14, 18, percent_format)
			worksheet.set_column(16, 16, 18, percent_format)
			worksheet.set_column(18, 18, 18, percent_format)
			worksheet.set_column(20, 20, 18, percent_format)
			worksheet.set_column(22, 22, 18, percent_format)
			worksheet.set_column(24, 24, 18, percent_format)
			worksheet.set_column(26, 26, 18, percent_format)
			worksheet.set_column(28, 28, 18, percent_format)
			worksheet.set_column(30, 30, 18, percent_format)
			worksheet.set_column(32, 32, 18, percent_format)
			worksheet.set_column(34, 34, 18, percent_format)
			worksheet.set_column(36, 36, 18, percent_format)
			worksheet.set_column(columns_num-1, columns_num-1, 18, percent_format)
			worksheet.set_column(columns_num, columns_num, 18, percent_format)
			worksheet.set_column(1+columns_num, 1+columns_num, 18, percent_format)
			# 组合持仓查询
			insert_dic['select_holding'].to_excel(writer, sheet_name=u'组合持仓查询', startrow=4, startcol=1)
			index_int = len(insert_dic['select_holding'].index)
			worksheet = writer.sheets[u'组合持仓查询']
			worksheet.write(2,1, u'组合持仓查询({0[1]},{0[2]}到{0[3]})'.format(self.title_list), bold_format)
			worksheet.conditional_format(4,1,4 + index_int,6, {'type': 'blanks','format':border_format})
			worksheet.conditional_format(4,1,4 + index_int,6, {'type': 'no_blanks','format':border_format})
			worksheet.set_column(1, 6, 12)
			worksheet.set_column(6, 6, 12, percent_format)
			# 组合期间排名查询
			insert_dic['select_rank_increase'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=1)
			worksheet = writer.sheets[u'组合期间排名查询']
			worksheet.write(2,1, u'组合期间排名查询({0[1]},{0[2]}到{0[3]})'.format(self.title_list), bold_format)
			worksheet.merge_range(4,1,4,3,u'涨幅前{0[4]}名'.format(self.select_list),merge_format)
			insert_dic['select_rank_decrease'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=4)
			worksheet.merge_range(4,4,4,6,u'跌幅前{0[4]}名'.format(self.select_list),merge_format)
			insert_dic['select_rank_pandl_de'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=7)
			worksheet.merge_range(4,7,4,9,u'亏损额最大前{0[4]}名'.format(self.select_list),merge_format)
			insert_dic['select_rank_pandl_in'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=10)
			worksheet.merge_range(4,10,4,12,u'亏损额最小前{0[4]}名'.format(self.select_list),merge_format)
			insert_dic['select_rank_position_de'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=13)
			worksheet.merge_range(4,13,4,15,u'本期增仓前{0[4]}名'.format(self.select_list),merge_format)
			insert_dic['select_rank_position_in'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=16)
			worksheet.merge_range(4,16,4,18,u'本期增仓后{0[4]}名'.format(self.select_list),merge_format)
			worksheet.conditional_format(5,1,5+self.select_list[4],18, {'type': 'blanks','format':border_format})
			worksheet.conditional_format(5,1,5+self.select_list[4],18, {'type': 'no_blanks','format':border_format})
			worksheet.set_column(1, 18, 12, float_format)
			worksheet.set_column(3, 3, 12, percent_format)
			worksheet.set_column(6, 6, 12, percent_format)
			# 对冲损益查询
			date_dif = len(insert_dic['hedge_profit'].index)
			insert_dic['hedge_profit'].to_excel(writer, sheet_name=u'对冲损益查询', startrow=4, startcol=1)
			insert_dic['hedge_profit_stddev'].to_excel(writer, sheet_name=u'对冲损益查询', startrow=4+date_dif+1+4, startcol=1)
			worksheet = writer.sheets[u'对冲损益查询']
			worksheet.write(2,1, u'对冲损益查询({0[1]},{0[2]}到{0[3]})'.format(self.title_list), bold_format)
			worksheet.write(4+date_dif+1+2,1, u'对冲损益查询(标准差)', bold_format)
			worksheet.conditional_format(4,1,4+date_dif,5, {'type': 'blanks','format':border_format})
			worksheet.conditional_format(4,1,4+date_dif,5, {'type': 'no_blanks','format':border_format})
			worksheet.conditional_format(4+date_dif+1+4,1,4+date_dif+1+5,2, {'type': 'blanks','format':border_format})
			worksheet.conditional_format(4+date_dif+1+4,1,4+date_dif+1+5,2, {'type': 'no_blanks','format':border_format})
			worksheet.set_column(1, 5, 15, float_format)
			# 组合行业换手率
			date_dif = len(insert_dic['industry_tr'].index)
			columns_num = len(insert_dic['industry_tr'].columns)
			insert_dic['industry_tr'].to_excel(writer, sheet_name=u'组合行业换手率', startrow=4, startcol=1)
			worksheet = writer.sheets[u'组合行业换手率']
			worksheet.write(2,1, u'组合行业换手率({0[1]},{0[2]}到{0[3]})'.format(self.title_list), bold_format)
			worksheet.conditional_format(4,1,4+date_dif,1+columns_num, {'type': 'blanks','format':border_format})
			worksheet.conditional_format(4,1,4+date_dif,1+columns_num, {'type': 'no_blanks','format':border_format})
			worksheet.set_column(1, 1+columns_num, 20, percent_format)
			
			writer.save()
			writer.close()
		# except:
			# sys.exit('Oops,Close Current EXCEL File and Retry,please!')

	# fuction for debug
	def debug(self):
		performance_dic = self.combine_performance()
		self.save_file(performance_dic)
