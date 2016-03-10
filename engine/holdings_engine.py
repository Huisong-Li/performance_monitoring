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
		self.dest_file_directory_str = self.abspath_str + user_io_dict['destination']['file_directory']
		self.holdings_file_name_str = user_io_dict['destination']['holdings_file_name']
		with open(self.abspath_str+'\\configuration\\user_select.json', 'r') as file:
			user_select_dict = json.load(file)
		self.start_date_str = user_select_dict['basic']['start_date']
		self.end_date_str   = user_select_dict['basic']['end_date']
		self.fund_names = user_select_dict['basic']['fund_names']
		self.port_names = user_select_dict['basic']['port_names']
		self.ranking_table_num = user_select_dict['holdings']['ranking_table_num']
		self.weight_table_num = user_select_dict['holdings']['ranking_table_num']
		self.select_list = [self.fund_names, self.port_names, self.start_date_str, self.end_date_str, self.ranking_table_num]
		self.title_list = [self.weight_table_num, u'盈润七号', self.start_date_str, self.end_date_str]
		self.engine = oracle.engine_db()

	# 组合权重股表
	def top_stocks(self):
		select_list = [self.fund_names, self.port_names, self.start_date_str, self.end_date_str, self.weight_table_num]
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
		pass

	# 组合日收益率与净值表 
	def daily_return_net_value(self):
		sql = '''
              select a.L_DATE,a.TOTAL_ASSETS as 组合总市值,a.UNIT_VALUE as 组合日净值, a.return as 组合日收益率, a.STOCK_ASSET as 股票持仓市值, a.stock_percent as 股票仓位占比, a.cash as 现金金额, a.cash_percent as 现金仓位占比, b.position/a.TOTAL_ASSETS as 当日加减仓比例 from
              ((select L_DATE, FUND_ID, PORT_ID,UNIT_VALUE, TOTAL_ASSETS, ((UNIT_VALUE-UNIT_VALUE_YESTERDAY)/UNIT_VALUE_YESTERDAY)*100 as return, STOCK_ASSET, (STOCK_ASSET/TOTAL_ASSETS) as stock_percent, CURRENT_CASH_EOD as cash, (CURRENT_CASH_EOD/TOTAL_ASSETS) as cash_percent
              from portasset
              where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1')a
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
		benchmark_df=self.get_benchmark_return('000300.SH', self.start_date_str, self.end_date_str)
		df = pd.merge(df_tmp, benchmark_df, left_index=True,right_index=True,how='inner')
		df.loc[:,u'日超额收益率'] = df[u'组合日收益率']-df[u'日基准收益率']
		df.loc[:,u'组合日收益率'] = df[u'组合日收益率']/100
		df.loc[:,u'日基准收益率'] = df[u'日基准收益率']/100
		df.loc[:,u'日超额收益率'] = df[u'日超额收益率']/100
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
	def selectRank(self):
		pass
		# # 涨跌幅
		# ## 第一天
		# sql = '''
              # select L_DATE, SECURITY_CODE, ACCOUNT_NAME, MARKET_PRICE, UNIT_COST  from portholding
              # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and'{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock' 
              # '''.format(self.selectList)
		# df = pd.read_sql(sql, self.db)
		# grouped = df.groupby(df['security_code'])

		# df = pd.DataFrame()
		# codeList = []
		# dftList = []
		# for x in grouped:
			# x[1].index=x[1]['l_date']
			# x[1].drop('l_date',axis=1,inplace=True)
			# codeList.append(x[0])
			# temp = x[1].sort_index()['market_price']
			# temp1 = x[1].drop(['security_code','account_name', 'unit_cost'],axis = 1)
			# costTMP = x[1].sort_index()['unit_cost']
			# df_temp = pd.DataFrame()
			# df_temp['l_date'] = [str(int(sorted(list(x[1].index))[0])-1)]
			# df_temp['market_price'] = [costTMP[0]]
			# df_temp.index=df_temp['l_date']
			# df_temp.drop('l_date',axis=1,inplace=True)
			# df_temp = df_temp.append(temp1)
			# dftChangeTmp=df_temp.pct_change()
			# periodChg=(dftChangeTmp+1).cumprod()-1
			# dftList.append(periodChg['market_price'][-1])
		# df[u'股票代码'] = codeList
		# df[u'幅度'] =  dftList
		# df.index=df[u'股票代码']
		# df.drop(u'股票代码',axis=1,inplace=True)

		# sql = '''
              # select DISTINCT SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称  from portholding
              # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and'{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock' 
              # '''.format(self.selectList)
		# dftmp = pd.read_sql(sql, self.db)
		# dftmp.index=dftmp[u'股票代码']
		# dftmp.drop(u'股票代码',axis=1,inplace=True)
		# df = pd.merge(dftmp, df, left_index=True,right_index=True,how='inner')
		# # 涨幅
		# df_increase = df.sort([u'幅度'],ascending=False)
		# df_increase = df_increase.dropna().head(self.selectList[4])
		# # 跌幅
		# df_decrease = df.sort([u'幅度'],ascending=True)
		# df_decrease = df_decrease.dropna().head(self.selectList[4])
		
		# # PANDL 跌
		# sql_pandlDe = '''
		              # select * FROM
		              # (select SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, sum(PANDL) as 金额 from
                      # (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, PANDL  from portholding
                      # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                      # group by SECURITY_CODE, ACCOUNT_NAME
                      # order by 金额)
					  # WHERE ROWNUM <= {0[4]}
		              # '''.format(self.selectList)
		# # PANDL 涨
		# sql_pandlIn = '''
		              # select * FROM
		              # (select SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, sum(PANDL) as 金额 from
                      # (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, PANDL  from portholding
                      # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                      # group by SECURITY_CODE, ACCOUNT_NAME
                      # order by 金额 DESC)
					  # WHERE ROWNUM <= {0[4]}
		              # '''.format(self.selectList)	  
		
		# # 减仓金额
		# sql_positionDe = '''
		                 # select * FROM
		                 # (select SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, sum(BUY_CASH)-sum(SALE_CASH) as 金额 from
                         # (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, BUY_CASH, SALE_CASH  from portholding
                         # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}'  and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                         # group by SECURITY_CODE, ACCOUNT_NAME
                         # order by 金额 DESC)
						 # WHERE ROWNUM <= {0[4]}
			             # '''.format(self.selectList)
		
		# # 增仓金额
		# sql_positionIn = '''
		                 # select * FROM
		                 # (select SECURITY_CODE as 股票代码, ACCOUNT_NAME as 股票名称, sum(BUY_CASH)-sum(SALE_CASH) as 金额 from
                         # (select L_DATE, SECURITY_CODE, ACCOUNT_NAME, BUY_CASH, SALE_CASH  from portholding
                         # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}'  and DATA_STATUS = '1' and SECURITY_TYPE = 'stock')
                         # group by SECURITY_CODE, ACCOUNT_NAME
                         # order by 金额)
						 # WHERE ROWNUM <= {0[4]}
		                 # '''.format(self.selectList)
		
		# selectRankDic = {}
		# selectRankDic['increase'] = df_increase
		# selectRankDic['decrease'] = df_decrease
		# selectRankDic['pandlDe'] = pd.read_sql(sql_pandlDe, self.db)
		# selectRankDic['pandlDe'].index=selectRankDic['pandlDe'][u'股票代码']
		# selectRankDic['pandlDe'].drop(u'股票代码',axis=1,inplace=True)
		# selectRankDic['pandlIn'] = pd.read_sql(sql_pandlIn, self.db)
		# selectRankDic['pandlIn'].index=selectRankDic['pandlIn'][u'股票代码']
		# selectRankDic['pandlIn'].drop(u'股票代码',axis=1,inplace=True)
		# selectRankDic['positionDe'] = pd.read_sql(sql_positionDe, self.db)
		# selectRankDic['positionDe'].index=selectRankDic['positionDe'][u'股票代码']
		# selectRankDic['positionDe'].drop(u'股票代码',axis=1,inplace=True)
		# selectRankDic['positionIn'] = pd.read_sql(sql_positionIn, self.db)
		# selectRankDic['positionIn'].index=selectRankDic['positionIn'][u'股票代码']
		# selectRankDic['positionIn'].drop(u'股票代码',axis=1,inplace=True)
		# return selectRankDic

	# 对冲损益查询(损益)
	def hedgeProfit(self):
		pass
		# sql_test = '''
		           # select FUND_ID, PORT_ID, L_DATE, SUM(MARKET_VALUE) as future from portholding
                   # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and SECURITY_TYPE = 'future' and POSITION_FLAG = 'short' and DATA_STATUS = '1'
                   # group by FUND_ID, PORT_ID ,L_DATE
				   # '''.format(self.selectList)
		# df = pd.read_sql(sql_test, self.db)
		# if list(df.index) == []:
			# sql = '''
                  # select L_DATE, STOCK_ASSET as 对冲前组合市值 , STOCK_ASSET as 对冲后组合市值,  (STOCK_ASSET- STOCK_ASSET) as 期货头寸, STOCK_ASSET as 风险敞口 from
                  # (select STOCK_ASSET, FUND_ID, PORT_ID ,L_DATE from Portasset
                  # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1')                  
                  # '''.format(self.selectList)
			# df = pd.read_sql(sql, self.db)
			# df.index=df['l_date']
			# df.drop('l_date',axis=1,inplace=True)
		# else:
			# sql = '''
                  # select a.L_DATE, b.STOCK_ASSET as 对冲前组合市值 , (a.future+b.STOCK_ASSET) as 对冲后组合市值, a.future as 期货头寸, (a.future+b.STOCK_ASSET) as 风险敞口 from
                  # ((select FUND_ID, PORT_ID, L_DATE, SUM(MARKET_VALUE) as future from portholding
                  # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and SECURITY_TYPE = 'future' and POSITION_FLAG = 'short' and DATA_STATUS = '1'
                  # group by FUND_ID, PORT_ID ,L_DATE)a
                  # left join
                  # (select STOCK_ASSET, FUND_ID, PORT_ID ,L_DATE from Portasset
                  # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1')b
                  # on a.FUND_ID = b.FUND_ID and a.PORT_ID = b.PORT_ID and a.L_DATE = b.L_DATE)
                  # '''.format(self.selectList)
			# df = pd.read_sql(sql, self.db)
			# df.index=df['l_date']
			# df.drop('l_date',axis=1,inplace=True)
		# return df

	# 对冲损益查询(标准差)
	def hedgeProfit_stddev(self):
		pass
		# sql_test = '''
		           # select FUND_ID, PORT_ID, L_DATE, SUM(MARKET_VALUE) as future from portholding
                   # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and SECURITY_TYPE = 'future' and POSITION_FLAG = 'short' and DATA_STATUS = '1'
                   # group by FUND_ID, PORT_ID ,L_DATE
				   # '''.format(self.selectList)
		# df = pd.read_sql(sql_test, self.db)
		# if list(df.index) == []:
			# sql = '''
                  # select stddev(before) as 对冲前组合 ,stddev(after) as 对冲后组合 from
                  # (select L_DATE, FUND_ID, PORT_ID ,STOCK_ASSET as after , STOCK_ASSET as before from Portasset
                  # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1')b
                  # '''.format(self.selectList)
			# df = pd.read_sql(sql, self.db)
			# df.index=df[u'对冲前组合']
			# df.drop(u'对冲前组合',axis=1,inplace=True)
		# else:
			# sql = '''
                  # select stddev(a.before) as 对冲前组合 ,stddev(a.after) as 对冲后组合 from
                  # (select a.L_DATE,a.FUND_ID, a.PORT_ID, b.STOCK_ASSET as before , (a.future+b.STOCK_ASSET) as after, a.future as future, (a.future+b.STOCK_ASSET) as window from
                  # ((select FUND_ID, PORT_ID, L_DATE, SUM(MARKET_VALUE) as future from portholding
                  # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and SECURITY_TYPE = 'future' and POSITION_FLAG = 'short' and DATA_STATUS = '1'
                  # group by FUND_ID, PORT_ID ,L_DATE)a
                  # left join
                  # (select STOCK_ASSET, FUND_ID, PORT_ID ,L_DATE from Portasset
                  # where FUND_ID = '{0[0]}' and PORT_ID = '{0[1]}' and L_DATE between '{0[2]}' and '{0[3]}' and DATA_STATUS = '1')b
                  # on a.FUND_ID = b.FUND_ID and a.PORT_ID = b.PORT_ID and a.L_DATE = b.L_DATE))a
                  # '''.format(self.selectList)
			# df = pd.read_sql(sql, self.db)
			# df.index=df[u'对冲前组合']
			# df.drop(u'对冲前组合',axis=1,inplace=True)
		# return df

	# 组合行业换手率
	def industry_tr(self):
		pass	

	# 取基准 收益率
	# 返回 基准的 收益率 DataFrame
	def get_benchmark_return(self,benchmark_code,startDate,endDate):
		benchmarkDF=pd.DataFrame()
		wind.w.start()
		tmp=wind.w.wsd(benchmark_code, "pct_chg", start_date, end_date, "Fill=Previous;PriceAdj=F")
		benchmarkDF[u'日基准收益率']=tmp.Data[0]
		benchmarkDF['Date']=[str(datetime.strptime(x.strftime("%Y-%m-%d"),"%Y-%m-%d"))[0:10].replace('-','') for x in tmp.Times]
		#print str(benchmarkDF['Date'][0])[0:10].replace('-','')
		benchmarkDF.index=benchmarkDF['Date']
		benchmarkDF.drop('Date',axis=1,inplace=True)
		self.benchmarkDF=benchmarkDF
		return benchmarkDF

	# 将要写入EXCEL文件的dataframe组成一个列表
	def combine_performance(self):
		insert_dic = {}
		insert_dic['top_stocks'] = self.top_stocks()
		insert_dic['industry_value_weight'] = self.industry_value_weight()
		insert_dic['daily_return_net_value'] = self.daily_return_net_value()
		insert_dic['select_holding'] = self.select_holding()
		# insert_dic['selectRank_decrease'] = self.selectRank()['decrease']
		# insert_dic['selectRank_increase'] = self.selectRank()['increase']
		# insert_dic['selectRank_pandlDe'] = self.selectRank()['pandlDe']
		# insert_dic['selectRank_pandlIn'] = self.selectRank()['pandlIn']
		# insert_dic['selectRank_positionDe'] = self.selectRank()['positionDe']
		# insert_dic['selectRank_positionIn'] = self.selectRank()['positionIn']
		# insert_dic['hedgeProfit'] = self.hedgeProfit()
		# insert_dic['hedgeProfit_stddev'] = self.hedgeProfit_stddev()
		insert_dic['industry_tr'] = self.industry_tr()
		return insert_dic

	# save hodings performance to excel
	def save_file(self, insert_dic):
		# try:
			writer = pd.ExcelWriter(self.dest_file_directory_str + self.holdings_file_name_str, engine='xlsxwriter')
			workbook = writer.book
			bold_format = workbook.add_format({'bold': True})
			percent_format = workbook.add_format({'num_format': '0.00%'})
			float_format_ = workbook.add_format({'num_format': '0.00'})
			border_format=workbook.add_format()
			border_format.set_border(1)
			merge_format = workbook.add_format({'bold': True,'align':'center','valign':'vcenter'})
			# 组合十大权重股表
			insert_dic['top_stocks'].to_excel(writer, sheet_name=u'组合{}大权重股表'.format(self.weight_table_num), startrow=4, startcol=1)
			worksheet = writer.sheets[u'组合{}大权重股表'.format(self.weight_table_num)]
			worksheet.write(2,1, u'组合{0[0]}大权重股表({0[1]},{0[2]}到{0[3]})'.format(self.title_list), bold_format)
			worksheet.conditional_format(4,1,4+self.weight_table_num,5, {'type': 'blanks','format':border_format})
			worksheet.conditional_format(4,1,4+self.weight_table_num,5, {'type': 'no_blanks','format':border_format})
			worksheet.set_column(1, 5, 16, float_format_)
			worksheet.set_column(3, 5, 16, percent_format)
			# 组合日收益率与净值表
			date_dif = len(insert_dic['daily_return_net_value'].index)
			insert_dic['daily_return_net_value'].to_excel(writer, sheet_name=u'组合日收益率与净值表', startrow=4, startcol=1)
			worksheet = writer.sheets[u'组合日收益率与净值表']
			worksheet.write(2,1, u'组合日收益率与净值表({0[1]},{0[2]}到{0[3]})'.format(self.title_list), bold_format)
			worksheet.conditional_format(4,1,4+date_dif,11, {'type': 'blanks','format':border_format})
			worksheet.conditional_format(4,1,4+date_dif,11, {'type': 'no_blanks','format':border_format})
			worksheet.set_column(1, 11, 14, float_format_)
			worksheet.set_column(4, 4, 14, percent_format)
			worksheet.set_column(6, 6, 14, percent_format)
			worksheet.set_column(8, 8, 14, percent_format)
			worksheet.set_column(9, 9, 14, percent_format)
			worksheet.set_column(10, 10, 14, percent_format)
			worksheet.set_column(11, 11, 14, percent_format)
			# 组合持仓查询
			insert_dic['select_holding'].to_excel(writer, sheet_name=u'组合持仓查询', startrow=4, startcol=1)
			index_int = len(insert_dic['select_holding'].index)
			worksheet = writer.sheets[u'组合持仓查询']
			worksheet.write(2,1, u'组合持仓查询({0[1]},{0[2]}到{0[3]})'.format(self.title_list), bold_format)
			worksheet.conditional_format(4,1,4 + index_int,6, {'type': 'blanks','format':border_format})
			worksheet.conditional_format(4,1,4 + index_int,6, {'type': 'no_blanks','format':border_format})
			worksheet.set_column(1, 6, 12)
			worksheet.set_column(6, 6, 12, percent_format)
			# # 组合期间排名查询
			# insertDic['selectRank_increase'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=1)
			# worksheet = writer.sheets[u'组合期间排名查询']
			# worksheet.write(2,1, u'组合期间排名查询({0[0]},{0[1]}到{0[2]})'.format(self.titleList), bold)
			# worksheet.merge_range(4,1,4,3,u'涨幅前{0[4]}名'.format(self.selectList),merge_format)
			# insertDic['selectRank_decrease'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=4)
			# worksheet.merge_range(4,4,4,6,u'跌幅前{0[4]}名'.format(self.selectList),merge_format)
			# insertDic['selectRank_pandlDe'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=7)
			# worksheet.merge_range(4,7,4,9,u'亏损额最大前{0[4]}名'.format(self.selectList),merge_format)
			# insertDic['selectRank_pandlIn'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=10)
			# worksheet.merge_range(4,10,4,12,u'亏损额最小前{0[4]}名'.format(self.selectList),merge_format)
			# insertDic['selectRank_positionDe'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=13)
			# worksheet.merge_range(4,13,4,15,u'本期增仓前{0[4]}名'.format(self.selectList),merge_format)
			# insertDic['selectRank_positionIn'].to_excel(writer, sheet_name=u'组合期间排名查询', startrow=5, startcol=16)
			# worksheet.merge_range(4,16,4,18,u'本期增仓后{0[4]}名'.format(self.selectList),merge_format)
			# worksheet.conditional_format(5,1,5+self.selectList[4],18, {'type': 'blanks','format':format})
			# worksheet.conditional_format(5,1,5+self.selectList[4],18, {'type': 'no_blanks','format':format})
			# worksheet.set_column(1, 18, 12, format_float)
			# worksheet.set_column(3, 3, 12, format_percent)
			# worksheet.set_column(6, 6, 12, format_percent)
			# # 对冲损益查询
			# date_dif = len(insertDic['hedgeProfit'].index)
			# insertDic['hedgeProfit'].to_excel(writer, sheet_name=u'对冲损益查询', startrow=4, startcol=1)
			# insertDic['hedgeProfit_stddev'].to_excel(writer, sheet_name=u'对冲损益查询', startrow=4+date_dif+1+4, startcol=1)
			# worksheet = writer.sheets[u'对冲损益查询']
			# worksheet.write(2,1, u'对冲损益查询({0[0]},{0[1]}到{0[2]})'.format(self.titleList), bold)
			# worksheet.write(4+date_dif+1+2,1, u'对冲损益查询(标准差)', bold)
			# worksheet.conditional_format(4,1,4+date_dif,5, {'type': 'blanks','format':format})
			# worksheet.conditional_format(4,1,4+date_dif,5, {'type': 'no_blanks','format':format})
			# worksheet.conditional_format(4+date_dif+1+4,1,4+date_dif+1+5,2, {'type': 'blanks','format':format})
			# worksheet.conditional_format(4+date_dif+1+4,1,4+date_dif+1+5,2, {'type': 'no_blanks','format':format})
			# worksheet.set_column(1, 5, 15, format_float)
			
			writer.save()
			writer.close()
		# except:
			# sys.exit('Oops,Close Current EXCEL File and Retry,please!')

	# fuction for debug
	def debug(self):
		performance_dic = self.combine_performance()
		self.save_file(performance_dic)
