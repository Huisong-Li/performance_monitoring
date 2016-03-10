import json
import os
from pprint import pprint

class json_to_object:
	def __init__(self, d):
		self.__dict__ = d

# before read or write file
abspath =  os.path.abspath(".")

# with open(abspath+'\\configuration\\user_select.json', 'r') as file:
	# data = json.load(file, object_hook=json_to_object)
# print(data.holdings.ranking_table_num)
# print type(data.holdings.ranking_table_num)
#pprint(data)


with open(abspath+'\\configuration\\manager_estimation_table_construction.json', 'r') as file:
	data = json.load(file)
# for x in data:
	# print data[x]['portfolios']['P00001']['port_property']['port_name']