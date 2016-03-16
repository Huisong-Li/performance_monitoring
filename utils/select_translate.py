#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: github.com/Huisong-Li

import os
import json

def translate():
	abspath_str =  os.path.abspath(".") + '\\performance_monitoring'
	with open(abspath_str+'\\configuration\\user_select.json', 'r') as file:
		user_select_dict = json.load(file)
	fund_names = user_select_dict['basic']['fund_names']
	port_names = user_select_dict['basic']['port_names']
	with open(abspath_str+'\\configuration\\manager_fof_construction.json', 'r') as file:
		manager_fof_construction_dict = json.load(file)
	select_list = []
	if isinstance(fund_names, list):
		if fund_names != []:
			for fund_name in fund_names:
				select_list.append([fund_name,'P00000'])
	if isinstance(fund_names, unicode):
		if fund_names != u'':
			fund_name = fund_names
			select_list.append([fund_name,'P00000'])
	if isinstance(port_names, list):
		if port_names != []:
			for port_name in port_names:
				for fund_id in manager_fof_construction_dict:
					if 'portfolios' in manager_fof_construction_dict[fund_id]:
						for port_id in manager_fof_construction_dict[fund_id]['portfolios']:
							if port_name == port_id:
								select_list.append([fund_id,port_name])
	if isinstance(port_names, unicode):
		if fund_names != u'':
			port_name = port_names
			for fund_id in manager_fof_construction_dict:
				if 'portfolios' in manager_fof_construction_dict[fund_id]:
					for port_id in manager_fof_construction_dict[fund_id]['portfolios']:
						if port_name == port_id:
							select_list.append([fund_id,port_names])
	return select_list
		