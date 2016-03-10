#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: github.com/Huisong-Li

from extraction.extraction import *
from engine.holdings_engine import *
from engine.assets_engine import *


extraction = extraction()
extraction.debug()
# holdings_engine = holdings_engine()
# holdings_engine.debug()
# assets_engine = assets_engine()
# assets_engine.debug()

# print('\n')
# print('...processing assets information...')
# print('-----------------------------------')
# assets_df =self.get_assets_return()
# print('please enter operation:')
# def assets_input():
	# op_str = raw_input("[i]nsert or [d]elete\n")
	# if op_str == 'i':
		# self.insert_into_oracle(assets_df)
	# elif op_str == 'd':
		# def delete_sql():
			# delete_sql_str = raw_input('please enter the sql statement:\n')
			# regexp_str = '^(delete).*'
			# result_list = re.findall(regexp_str,delete_sql_str)
			# if result_list == []:
				# delete_sql()
			# else:
				# self.delete_from_oracle(delete_sql_str)
		# delete_sql()
	# else:
		# print('oops, wrong input, enter I or D to select the operation')
		# assets_input()
# assets_input()
# print('-----------------------------------')
# print('.........ahhh, all is done.........')

