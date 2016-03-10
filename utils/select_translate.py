#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: github.com/Huisong-Li

import os
import json

def translate():
	## TODO
	abspath_str =  os.path.abspath(".") + '\\performance_monitoring'
	with open(abspath_str+'\\configuration\\user_select.json', 'r') as file:
		user_select_dict = json.load(file)
	fund_names = user_select_dict['basic']['fund_names']
	port_names = user_select_dict['basic']['port_names']
	select_list = []
	if isinstance(fund_names, list):
		for fund_name in fund_names:
		# fund name is unicode
			select_tmp_list = []
			if isinstance(port_names, list):
				for port_name in port_names:
					# port name is unicode
					select_tmp_list = []
					select_tmp_list = [fund_name,port_name]
					select_list.append(select_tmp_list)
			if isinstance(port_names, unicode):
				select_tmp_list = [fund_name,port_name]
				select_list.append(select_tmp_list)
	if isinstance(fund_names, unicode):
		fund_name = fund_names
		select_tmp_list = []
		if isinstance(port_names, list):
			for port_name in port_names:
			# port name is unicode
				select_tmp_list = []
				select_tmp_list = [fund_name,port_name]
				select_list.append(select_tmp_list)
		if isinstance(port_names, unicode):
			port_name = port_names
			select_tmp_list = [fund_name,port_name]
			select_list.append(select_tmp_list)
	print select_list
		