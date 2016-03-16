#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: github.com/Huisong-Li

import json
from extraction.extraction import *
from assemble.assemble import *
from engine.holdings_engine import *
from engine.assets_engine import *
from utils.oracle import *
from utils.select_translate import *
from utils.move_back_up import *

print('\n')
print('-----------------------------------')
print('...extracte from excel...')
extraction = extraction()
extraction.debug()
print('...add industry information...')
oracle.procedure('do_industry')
print('...assemble to fof...')
assemble = assemble()
assemble.debug()
print('...calculate assets performance...')
select_list = translate()
assets = assets_engine()
assets.select_port_list = select_list
assets.debug()
print('...calculate holdings performance...')
for select_item in select_list:
	fund_id = select_item[0]
	port_id = select_item[1]
	holdings = holdings_engine()
	holdings.fund_names = fund_id
	holdings.port_names = port_id
	abspath_str =  os.path.abspath(".") + '\\performance_monitoring'
	with open(abspath_str+'\\configuration\\manager_fof_construction.json', 'r') as file:
		manager_fof_construction_dict = json.load(file)
	if port_id == 'P00000':
		port_name_str = manager_fof_construction_dict[fund_id]['fund_property']['fund_name']
	else:
		port_name_str = manager_fof_construction_dict[fund_id]['portfolios'][port_id]['port_property']['port_name']
	holdings.title_list = [holdings.title_list[0],port_name_str,holdings.title_list[1],holdings.title_list[2]]
	holdings.select_list = [fund_id]+[port_id]+holdings.select_list
	holdings.debug()
print('...move files to backup folder...')
move_back_up()
print('-----------------------------------')
print('.........ahhh, all is done.........')
 