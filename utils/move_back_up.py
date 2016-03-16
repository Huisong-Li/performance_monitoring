#!/usr/bin/python
# -*- coding: UTF-8 -*-
# author: github.com/Huisong-Li

import os
import shutil
import json


def move_back_up():
	abspath_str =  os.path.abspath(".") + '\\performance_monitoring'
	with open(abspath_str+'\\configuration\\administrator.json', 'r') as file:
		administrator_dict = json.load(file)
	source_file_directory_str = abspath_str + administrator_dict['source']['file_directory']
	back_file_directory_str = abspath_str + administrator_dict['back']['file_directory']
	with open(abspath_str+'\\configuration\\user_io.json', 'r') as file:
		user_io_dict = json.load(file)
	source_file_name = user_io_dict['source']['file_names']
	source_file_name = [x for x in source_file_name if x != u'']
	list_files = source_file_name
	if source_file_name == []:
		list_files = os.listdir(source_file_directory_str)
	if os.path.exists(back_file_directory_str) == False:
		os.mkdir((back_file_directory_str))
	file_bak_path = back_file_directory_str
	for file in list_files:
		shutil.move(source_file_directory_str + file, file_bak_path + file)
