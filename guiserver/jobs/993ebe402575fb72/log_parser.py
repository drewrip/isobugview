#!/usr/bin/env python3

from itertools import filterfalse
import sys
import pprint
import yaml
import json
import re
import time
import random
from pathlib import Path

"""
for sql in log:
	if str(sql[0][0]).lower().startswith("begin"):
		# print("Begin")
		txn_key_list = []
		txn_v_list = []
	elif str(sql[0][0]).lower().startswith("commit"):
		txn_key = tuple(txn_key_list)
		if txn_key not in txn:
			txn[txn_key] = [txn_v_list]
		else:
			txn[txn_key].append(txn_v_list)
		# print("Commit")
	else:       #processing transactions
		# print("Processing transaction",end=": ")
		# print(entry)
		sql_key_list = []
		sql_value = []
		for op in sql:
			op_key = (op[3], op[1], op[2])
			op_v = op[4]
			sql_key_list.append(op_key)
			sql_value.append(op_v)
		txn_key_list.append(tuple(sql_key_list))
		txn_v_list.append(sql_value)
"""

order_dic = {"Type_1": 2, "Type_2": 1, "Type_3": 3, "Type_4": 4, "Type_5": 5}

# order of group: JOIN, WHERE, TARGET


def group_op_by_anchor(anchor, sql_list):
	dic = {}
	dic["WHERE"] = []
	dic["JOIN"] = []
	dic["OTHER"] = []

	for elem in sql_list:
		if elem[0] != anchor:
			continue
		if elem[1] == "JOIN":
			dic["JOIN"].append(elem)
		elif elem[1] == "WHERE":
			dic["WHERE"].append(elem)
		else:
			dic["OTHER"].append(elem)
	# pprint.pprint(dic)

	dic["JOIN"] = sorted(dic["JOIN"], key=lambda x: (
		order_dic[x[4][0]], x[2], x[3]))
	dic["WHERE"] = sorted(dic["WHERE"], key=lambda x: (
		order_dic[x[4][0]], x[2], x[3]))
	dic["OTHER"] = sorted(dic["OTHER"], key=lambda x: (
		order_dic[x[4][0]], x[2], x[3]))

	return [dic["JOIN"], dic["WHERE"], dic["OTHER"]]


def process_sql_recur(anchor, sql_list, sql_key_list, sql_value, frm="SELECT"):
	group = group_op_by_anchor(anchor, sql_list)
	join_list = group[0]
	where_list = group[1]
	other_list = group[2]

	# process order: 1)JOIN, 2)WHERE, 3)OTHER clause
	if len(join_list) != 0:
		for elem in join_list:
			op_key = (elem[1], elem[2], elem[3])  # (OP, TABLE, COLUMN)
			v_type = elem[4][0]
			if v_type == "Type_1":
				if op_key in sql_key_list:
					index = sql_key_list.index(op_key)
					if sql_value[index] == elem[4][1]:
						continue
				sql_key_list.append(op_key)
				sql_value.append(elem[4][1])
			elif v_type == "Type_3":
				if op_key in sql_key_list:
					continue
				sql_key_list.append(op_key)
				sql_value.append(None)
			else:
				print("Error: wrong type {0} in JOIN clause".format(v_type))
				exit(-1)

	if len(where_list) != 0:
		for elem in where_list:
			op_key = (elem[1], elem[2], elem[3])
			v_type = elem[4][0]
			if v_type == "Type_1":
				if op_key in sql_key_list:
					index = sql_key_list.index(op_key)
					if sql_value[index] == elem[4][1]:
						continue
				sql_key_list.append(op_key)
				sql_value.append(elem[4][1])
			elif v_type == "Type_2":
				# process all predicate reads in subquery
				process_sql_recur(elem[4][1], sql_list, sql_key_list, sql_value, "WHERE")
				# TODO: add read where.
				# print("Type_2 in WHERE clause: op_key is {0}".format(op_key))
				if elem[3].startswith("?"):
					# it's virtual anchor, just skip it.
					continue
				else:
					if op_key in sql_key_list:
						continue
					sql_key_list.append(op_key)
					sql_value.append(None)
			elif v_type == "Type_3":
				if op_key in sql_key_list:
					continue
				sql_key_list.append(op_key)
				sql_value.append(None)
			elif v_type == "Type_4":  # where colA = colB
				if op_key not in sql_key_list:
					sql_key_list.append(op_key)
					sql_value.append(None)

				ll = elem[4][1].split(".")
				op_key2 = (elem[1], ll[0], ll[1])
				if op_key2 not in sql_key_list:
					sql_key_list.append(op_key2)
					sql_value.append(None)
			else:
				print("Error: wrong type {0} in WHERE clause".format(v_type))
				exit(-1)

	if anchor == 0:
		for elem in other_list:
			op_key = (elem[1], elem[2], elem[3])
			v_type = elem[4][0]
			if v_type == "Type_1":
				if op_key in sql_key_list:
					index = sql_key_list.index(op_key)
					if sql_value[index] == None:
						continue
				sql_key_list.append(op_key)
				sql_value.append(None)
			elif v_type == "Type_2":
				# process all predicate reads in subquery
				# print("pass anchor {0}".format(elem[4][1]))
				process_sql_recur(elem[4][1], sql_list, sql_key_list, sql_value, op_key[0])
				# print("Type_2 in TARGET clause: op_key is {0}".format(op_key))
				if elem[3].startswith("?"):
					# it's virtual anchor, just skip it.
					continue
				else:
					if op_key in sql_key_list:
						continue
					sql_key_list.append(op_key)
					sql_value.append(None)
			elif v_type == "Type_3":
				if op_key in sql_key_list:
					continue
				sql_key_list.append(op_key)
				sql_value.append(None)
			elif v_type == "Type_5":
				# processing special case: select 1 from tb where id = ...
				op_key = (elem[1], elem[2], "@1")
				sql_key_list.append(op_key)
				sql_value.append(None)
			else:
				print("Error: wrong type {0} in {1} clause".format(v_type, elem[1]))
				exit(-1)
	else:  # subquery
		for elem in other_list:
			# print(elem)
			# print(frm)
			if frm == "WHERE" or frm == "UPDATE" or frm == "INSERT":
				op_key = ("WHERE", elem[2], elem[3])
			else:
				op_key = (elem[1], elem[2], elem[3])
			v_type = elem[4][0]
			if v_type == "Type_3":
				if op_key in sql_key_list:
					continue
				# print(op_key)
				sql_key_list.append(op_key)
				sql_value.append(None)
			elif v_type == "Type_5":
				continue
			else:
				print("Error: wrong type {0} in {1} clause".format(v_type, elem[1]))
				print("Param: anchor:{0}, sql_list:{1}, sql_key_list:{2}, sql_value:{3}, frm:{4}".format(
					anchor, sql_list, sql_key_list, sql_value, frm))
				print("op_key is {0}".format(op_key))
				exit(-1)
	return


def process_sql(sql_list):
	sql_key_list = []
	sql_value = []
	process_sql_recur(0, sql_list, sql_key_list, sql_value)
	return tuple(sql_key_list), sql_value


def process_line(line, txn, txn_key_list, txn_v_list, txn_sql_list, last_ret):
	# print("{1}, line is: {0}".format(line, last_ret))
	if last_ret == 3:
		sql = line.rstrip("\n")
	else:
		sql = eval(line)
	ret_val = -1
	# print(sql)
	if type(sql) == str:
		print(sql)
		txn_sql_list.append(sql)
		ret_val = 4
	elif str(sql[0][0]).lower().startswith("begin"):
		# print("Begin")
		ret_val = 1
	elif str(sql[0][0]).lower().startswith("commit"):
		# print("Commit")
		ret_val = 2
	else:
		# print("Processing transaction",end=": ")
		# print(sql)
		sql_key, sql_value = process_sql(sql)
		txn_key_list.append(sql_key)
		txn_v_list.append(sql_value)
		if len(sql_key) == 0:
			ret_val = 5
		else:
			ret_val = 3
		assert len(sql_key) != 0, "Let's check {0}".format(pprint.pformat(sql))
	return ret_val


def process_log(txn, sql_trace, log_lines):
	txn_key_list = []
	txn_v_list = []
	txn_sql_list = []
	last_ret = -1
	for line in log_lines:
		if re.match(r'^\s*$', line):
			continue
		proc_ret = process_line(line, txn, txn_key_list,
								txn_v_list, txn_sql_list, last_ret)
		if proc_ret == 1:
			txn_key_list = []
			txn_v_list = []
			txn_sql_list = []
		elif proc_ret == 2:
			txn_key = tuple(txn_key_list)
			if txn_key not in txn:
				txn[txn_key] = [txn_v_list]
				sql_trace[txn_key] = [txn_sql_list]
			else:
				txn[txn_key].append(txn_v_list)
				sql_trace[txn_key].append(txn_sql_list)
		elif proc_ret == 3:
			pass
		elif proc_ret == 4:
			pass
		else:
			print("Error: unknown return value {0} from process_line".format(proc_ret))
		last_ret = proc_ret
	return


def read_log(log_name):
	txn = {}
	txn_sql_trace = {}
	with open(log_name, 'r') as f:
		process_log(txn, txn_sql_trace, f.readlines())
	return txn, txn_sql_trace


"""
def read_log(log_name):
	txn = {}
	# with open("sqlOpsList_tpcc_small_column.txt",'r') as f:
	with open(log_name,'r') as f:
		for line in f.readlines():
			sql = eval(line)
			if str(sql[0][0]).lower().startswith("begin"):
				print("Begin")
				txn_key_list = []
				txn_v_list = []
			elif str(sql[0][0]).lower().startswith("commit"):
				txn_key = tuple(txn_key_list)
				if txn_key not in txn:
					txn[txn_key] = [txn_v_list]
				else:
					txn[txn_key].append(txn_v_list)
				print("Commit")
			else:       #processing transactions
				print("Processing transaction",end=": ")
				print(sql)

				# Old code, to be deleted
				id_equal = False
				sql_key_list = []
				sql_value = []
				for op in sql:
					op_key = (op[3], op[1], op[2])
					op_v = op[4]
					if isinstance(op_v, list):
						if len(op_v) == 2 and op_v[0] == 'i':
							id_equal = True
						# else:
						#    print("Unhandled case on op value: "+ pprint.pformat(op))

					sql_key_list.append(op_key)
					if op_key[0] == "WHERE" or op_key[0] == "INSERT":
						sql_value.append(op_v)
					else:
						sql_value.append(None)

				if id_equal == True:
					sql_value = fill_id_helper(sql_key_list, sql_value)
				txn_key_list.append(tuple(sql_key_list))
				txn_v_list.append(sql_value)

	return txn
"""


def index_builder(txn_dic, txn_sql_trace):
	dic = {}
	trace = {}
	sql_index = 0
	for txn in txn_dic:
		# transaction should have at least one operation, otherwise ignore it
		if len(txn) == 0:
			continue
		op_index = 0
		t_lis = []
		for sql in txn:
			s_lis = []
			for op in sql:
				s_lis.append(op + (sql_index, op_index))
				op_index = op_index + 1
			sql_index = sql_index + 1
			t_lis.append(tuple(s_lis))
		dic[tuple(t_lis)] = txn_dic[txn]
		trace[tuple(t_lis)] = txn_sql_trace[txn]
	return dic, trace

"""
def fill_id_helper(sql_key, sql_value):
	ret = []
	for v in sql_value:
		if isinstance(v, list) and len(v) == 2 and v[0] == 'i':
			id = v[1]
			# print("Find "+id)
			for op_it in range(len(sql_key)):
			# for op in sql_key:
				op = sql_key[op_it]
				if op[2] == id and op[0] == "WHERE":
					index = sql_key.index(op)
					ret.append(sql_value[index])
		else:
			ret.append(v)
	return ret
"""

# discover correlated operations in transaction
def find_associate(txn_symbol, txn_value):
	pair_dic = {}
	check_first = False
	count = 0
	sql_count = 0
	for sql in txn_symbol:
		count = count + len(sql)
		sql_count += 1
	# print("#OP = {0}, #SQL = {1}".format(count, sql_count))
	i = 0
	for inst in txn_value:
		if not check_first:
			# do check on all pairs
			# print(len(inst))
			# k = 0
			for s_it1 in range(0, len(inst), 1):
				for s_it2 in range(s_it1 + 1, len(inst), 1):
					for op_it1 in range(len(inst[s_it1])):
						for op_it2 in range(len(inst[s_it2])):
							op1_idx = (s_it1, op_it1)
							op2_idx = (s_it2, op_it2)
							v1 = inst[s_it1][op_it1]
							v2 = inst[s_it2][op_it2]
							if v1 != None and v2 != None and type(v1) == type(
									v2):
								if v1 == v2 and op1_idx != op2_idx:
									pair_dic[(op1_idx, op2_idx)] = True
								else:
									pair_dic[(op1_idx, op2_idx)] = False
							else:
								pair_dic[(op1_idx, op2_idx)] = False
			check_first = True
			"""
			for s1 in inst[:-1]:
				for s2 in inst[inst.index(s1)+1:]:
					print(s2)
					for v1 in s1:
						for v2 in s2:
							op1_idx = (inst.index(s1), s1.index(v1))
							op2_idx = (inst.index(s2), s2.index(v2))
							if v1 != None and v2 != None :    
								if v1 == v2 and op1_idx != op2_idx:
									pair_dic[(op1_idx, op2_idx)] = True
									print("Check <{0}/{2}, {1}/{3}>".format(op1_idx, op2_idx, v1, v2))
								else:
									pair_dic[(op1_idx, op2_idx)] = False
							else:
								pair_dic[(op1_idx, op2_idx)] = False
							idx1 = t_symbol[op1_idx[0]][op1_idx[1]][4]
							idx2 = t_symbol[op2_idx[0]][op2_idx[1]][4]
							print("Check <{0}, {1}>".format(idx1, idx2))
				print("-----------")
			print ("K= "+str(k))
			"""
			# print("{0}: {1}".format(i, sum(value == True for value in pair_dic.values())))
		else:
			for pair in pair_dic:
				if pair_dic[pair] == False:
					continue
				else:
					op1_idx = pair[0]
					op2_idx = pair[1]
					v1 = inst[op1_idx[0]][op1_idx[1]]
					v2 = inst[op2_idx[0]][op2_idx[1]]
					if v1 != v2:
						pair_dic[pair] = False
			# print("{0}: {1}".format(i, sum(value == True for value in pair_dic.values())))
		i = i + 1
	ret = []
	for pair in pair_dic:
		if pair_dic[pair] == False:
			# idx1 = t_symbol[op1_idx[0]][op1_idx[1]][4]
			# idx2 = t_symbol[op2_idx[0]][op2_idx[1]][4]
			# print("({0}, {1}): False".format(idx1, idx2))
			continue
		else:
			op1_idx = pair[0]
			op2_idx = pair[1]
			idx1 = txn_symbol[op1_idx[0]][op1_idx[1]][4]
			idx2 = txn_symbol[op2_idx[0]][op2_idx[1]][4]
			# print("({0}, {1}): True".format(idx1, idx2))
			ret.append((idx1, idx2))

	# pprint.pprint(pair_dic)
	# sys.exit(1)
	return ret


def build_correlation(txn, corr_pair):
	correlation = {}
	for sql in txn:
		if len(sql) == 0:
			print("empty sql in transaction {0}".format(txn))
			exit(-1)
		correlation[sql[0][3]] = []
	if len(txn) <= 1:
		return correlation
	pprint.pprint(txn)
	pprint.pprint(corr_pair)

	# run union find to group correlated edges
	parent = {}

	def find(op):
		ret = op
		while parent[ret] != ret:
			ret = parent[ret]
		return ret

	def isConnected(op1, op2):
		return find(op1) == find(op2)

	def connect(op1, op2):
		parent[find(op1)] = find(op2)
		return

	for sql in txn:
		for op in sql:
			parent[op[4]] = op[4]

	for pair in corr_pair:
		op1 = pair[0]
		op2 = pair[1]
		if isConnected(op1, op2) == False:
			connect(op1, op2)

	def add_correlation(key, value):
		if key not in correlation:
			correlation[key] = []
		correlation[key].append(value)
		return

	def extractWhereClause(txn_list):
		sql_where_list = []
		for sql in txn_list:
			sql_where = []
			for op in sql:
				if op[0] == "WHERE":
					sql_where.append(op)
			sql_where_list.append(sql_where)
		return sql_where_list

	def isSqlCorrelated(sql_where_list1, sql_where_list2):
		len1 = len(sql_where_list1)
		len2 = len(sql_where_list2)
		if len1 == 0 or len2 == 0:
			return False
		set1 = set()
		set2 = set()
		for op in sql_where_list1:
			# pprint.pprint(op)
			root = find(op[4])
			set1.add(root)
		for op in sql_where_list2:
			# pprint.pprint(op)
			root = find(op[4])
			set2.add(root)
		# pprint.pprint(set1)
		# pprint.pprint(set2)
		if set1 == set2:
			return True
		else:
			return False

	where_list = extractWhereClause(txn)
	n = len(txn)
	for i in range(0, n, 1):
		for j in range(i + 1, n, 1):
			if isSqlCorrelated(where_list[i], where_list[j]):
				sql_idx1 = where_list[i][0][3]
				sql_idx2 = where_list[j][0][3]
				add_correlation(sql_idx1, sql_idx2)
				add_correlation(sql_idx2, sql_idx1)
	return correlation


def gen_final_output(result, sql_examples):
	l = []
	cnt = 0
	for el in result:
		trans = {}
		trans['name'] = str(cnt)
		trans['id'] = cnt
		cnt += 1
		ops = []
		for sql in el[0]:
			for op in sql:
				tmp_dic = {}
				if op[0] == "WHERE":
					tmp_dic['op'] = 'r'
				elif op[0] == "SELECT":
					tmp_dic['op'] = 'r'
				elif op[0] == "UPDATE":
					tmp_dic['op'] = 'w'
				elif op[0] == "INSERT":
					tmp_dic['op'] = 'w'
				elif op[0] == "DELETE":
					tmp_dic['op'] = 'w'
				elif op[0] == "JOIN":
					tmp_dic['op'] = 'r'
				else:
					print("Error: unknown op {0}".format(op[0]))
					exit(-1)
				tmp_dic['obj'] = op[1] + "-" + op[2]
				tmp_dic['offset'] = op[4]
				tmp_dic['sql_idx'] = op[3]
				tmp_dic['sql_op'] = op[0]
				ops.append(tmp_dic)
		trans['ops'] = ops

		# correlation = []
		# for pair in el[1]:
		#	correlation.append([pair[0], pair[1]])
		corr = el[1]
		trans['correlation'] = corr
		l.append(trans)
	final_dump = {}
	# final_dump.append({"transactions": l})
	# final_dump.append({"sqls": sql_examples})
	final_dump["transactions"] = l
	final_dump["sqls"] = sql_examples
	return final_dump


def to_json(result, sql_examples, schema_file, output_base):
	final_dump = gen_final_output(result, sql_examples)
	ff = open("conf/txn/" + output_base + ".json", "w")
	json.dump(final_dump, ff, indent=2)
	ff.close()

	schema_name = Path(schema_file).name
	new_schema_folder = "conf/schema/"
	Path(new_schema_folder).mkdir(parents=True, exist_ok=True)
	new_schema_file = new_schema_folder + schema_name

	with open(schema_file, "r") as r, open(new_schema_file, "w") as w:
		for line in r:
			w.write(line)

	file_line = "file = " + "conf/txn/" + output_base + ".json"
	txn_id_line = "txn_id = "
	schema_line = "schema_file = " + new_schema_file
	for i in range(len(result)):
		if i != 0:
			txn_id_line += ","
		txn_id_line += str(i)
	with open("conf/" + output_base + ".conf", "w") as conf_f:
		print(file_line, file=conf_f)
		print(txn_id_line, file=conf_f)
		print(schema_line, file=conf_f)
	print("Save file to conf/txn/" + output_base + ".json")
	return


def to_yaml(result, sql_examples, output_base):
	final_dump = gen_final_output(result, sql_examples)
	noalias_dumper = yaml.dumper.SafeDumper
	noalias_dumper.ignore_aliases = lambda self, data: True
	ff = open("conf/txn/" + output_base + ".yml", "w")
	yaml.dump(final_dump, ff, default_flow_style=False, Dumper=noalias_dumper)
	ff.close()

	# write conf file
	file_line = "file = " + "conf/txn/" + output_base + ".yml"
	txn_id_line = "txn_id = "
	for i in range(len(result)):
		if i != 0:
			txn_id_line += ","
		txn_id_line += str(i)
	with open("conf/" + output_base + ".conf", "w") as conf_f:
		print(file_line, file=conf_f)
		print(txn_id_line, file=conf_f)
	print("Save file to conf/txn/" + output_base + ".yml")
	return

def print_txn(txn_list):
	i = 0
	for txn in txn_list:
		print ("Transaction {0}:".format(i))
		i = i + 1
		print("BEGIN")
		for sql_str in txn:
			print(sql_str)	
		print("END")
	print("")

def save_map(sql_map, filename):
	with open(filename, 'w') as f:
		print(sql_map, file=f)
	return

def print_map_to_file(sql_map, txn_list, filename):
	i = 0
	with open(filename, 'w') as f:
		for txn in txn_list:
			if len(txn) == 0:
				continue
			print("Transaction {0}:".format(i), file=f)
			for sql in txn:
				print("<{0}> {1}".format(sql[0][3], sql_map[sql[0][3]]), file=f)
			print("END", file=f)
			i = i+1
	return

def print_txn_to_file(txn_list, filename):
	i = 0
	with open(filename, 'w') as f:
		for txn in txn_list:
			print ("Transaction {0}:".format(i), file=f)
			i = i + 1
			print("BEGIN", file=f)
			for sql_str in txn:
				print(sql_str, file=f)
			print("END", file=f)
	return

def to_sql_from_trace(txn, txn_sql_trace, sql_map):
	rand_list = random.choice(txn_sql_trace[txn])
	i = 0
	for e in txn:
		sql_map[e[0][3]] = rand_list[i]
		# ret.append("<{0}> {1}".format(e[0][3], rand_list[i]))
		i = i+1
	return

def to_sql(txn):
	sql_str_list = []
	print(txn)
	for sql in txn:
		str_sql = ""
		for op in sql:
			# if op[0] == 
			continue
		"""
		sql_index = -1
		cr_list = []
		jn_list = []
		op_list = []
		op_name = "Unknown"
	
		for op in sql:
			if sql_index == -1:
				sql_index = op[3]
			if op[0] == 'WHERE':
				cr_list.append(op[1]+"-"+op[2]+"<"+str(op[4])+">")
			elif op[0] == 'JOIN':
				jn_list.append(op[1]+"-"+op[2]+"<"+str(op[4])+">")
			else:
				op_name = op[0]
				op_list.append(op[1]+"-"+op[2]+"<"+str(op[4])+">")
		str_sql = ""

		# print("sql is ",end="")
		# pprint.pprint(sql)

		assert op_name != 'Unknown'
		if cr_list == []:
			str_sql = "[{0}] {1} {2}".format(
				sql_index,
				op_name,
				", ".join(x for x in op_list)
			)
		elif jn_list == []:
			str_sql = "[{0}] {1} {2} WHERE {3}".format(
				sql_index,
				op_name,
				", ".join(x for x in op_list),
				" AND ".join(x+" = ?" for x in cr_list)
			)
		else:
			# assert len(jn_list)==2, "object in join is greater than 2, {0}".format(len(jn_list)) 
			str_sql = "[{0}] {1} {2} JOIN {3} WHERE {4}".format(
				sql_index,
				op_name,
				", ".join(x for x in op_list),
				",".join(x for x in jn_list),
				" AND ".join(x+" = ?" for x in cr_list)
			)
			"""
		sql_str_list.append(str_sql)
	return sql_str_list


def main():
	if len(sys.argv) != 3:
		print("Error: need input file and schema file.")
		sys.exit(1)
	else:
		file_name = sys.argv[1]
		file_base = str.split(Path(file_name).name, '.')[0]

		schema_file = sys.argv[2]

		print("Read raw data")
		txn_dic, txn_sql_trace = read_log(file_name)
		txn_dic, txn_sql_trace = index_builder(txn_dic, txn_sql_trace)

		print("Start analysis")
		final_result = []
		i = 1

		# txn_str_list = []
		sql_map = {}
		for txn in txn_dic:
			if len(txn_dic[txn]) == 0:
				continue
			to_sql_from_trace(txn, txn_sql_trace, sql_map)
			# txn_str_list.append()

			associate = find_associate(txn, txn_dic[txn])
			correlation = build_correlation(txn, associate)
			# print("Correlation is: ")
			# pprint.pprint(correlation)
			# print("-------------------")
			final_result.append((txn, correlation))
			# final_result.append((txn, associate))
			print("finish " + str(i))
			i = i + 1
		# print("{0}: {1}\n --- \n{2}\n###########\n{3}".format(i, pprint.pformat(txn), pprint.pformat(txn_dic[txn]), pprint.pformat(associate)))
		# for txn in txn_dic:
		# print_txn(txn_str_list)
		# print(sql_map)

		save_map(sql_map, "map/" + file_base + ".dic")
		print_map_to_file(sql_map, txn_dic, "sql/" + file_base + "_sql.txt")

		# print_txn_to_file(txn_str_list, "sql/"+file_base+"_sql.txt")
		# to_yaml(final_result, sql_map, file_base)
		to_json(final_result, sql_map, schema_file, file_base)

def test():
	print("Start test.")
	lis = []
	"""
	lis.append("[('Begin Transaction',)]")
	lis.append("[(0, 'WHERE', 'customer', 'c_d_id', ('Type_1', 4)), (0, 'WHERE', 'customer', 'c_id', ('Type_1', 998)), (0, 'WHERE', 'customer', 'c_w_id', ('Type_4', 'warehouse.w_id')), (0, 'WHERE', 'warehouse', 'w_id', ('Type_1', 3)), (0, 'SELECT', 'customer', 'c_credit', ('Type_3', '?')), (0, 'SELECT', 'customer', 'c_discount', ('Type_3', '?')), (0, 'SELECT', 'customer', 'c_last', ('Type_3', '?')), (0, 'SELECT', 'warehouse', 'w_tax', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'district', 'd_id', ('Type_1', 4)), (0, 'WHERE', 'district', 'd_w_id', ('Type_1', 3)), (0, 'SELECT', 'district', 'd_next_o_id', ('Type_3', '?')), (0, 'SELECT', 'district', 'd_tax', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'district', 'd_id', ('Type_1', 4)), (0, 'WHERE', 'district', 'd_w_id', ('Type_1', 3)), (0, 'UPDATE', 'district', 'd_next_o_id', ('Type_1', '3002+1'))]")
	lis.append("[(0, 'INSERT', 'orders', 'o_all_local', ('Type_1', 1)), (0, 'INSERT', 'orders', 'o_c_id', ('Type_1', 998)), (0, 'INSERT', 'orders', 'o_d_id', ('Type_1', 4)), (0, 'INSERT', 'orders', 'o_entry_d', ('Type_1', '2019-07-02 15:48:07')), (0, 'INSERT', 'orders', 'o_id', ('Type_1', 3002)), (0, 'INSERT', 'orders', 'o_ol_cnt', ('Type_1', 13)), (0, 'INSERT', 'orders', 'o_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'INSERT', 'new_orders', 'no_d_id', ('Type_1', 4)), (0, 'INSERT', 'new_orders', 'no_o_id', ('Type_1', 3002)), (0, 'INSERT', 'new_orders', 'no_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 7355)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 7355)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 7355)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 34))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '200.75686645507812')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 4)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', 'k2RgMQPMJs5L0C3LJ06kl4xY')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 7355)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 1)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 5)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 7843)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 7843)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 7843)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 21))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '72.65155029296875')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 4)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', '14Q063AGcwBfkH8pBVbykezN')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 7843)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 2)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[('Commit',)]")
"""

	lis.append("[('Begin Transaction',)]")
	lis.append("[(0, 'WHERE', 'customer', 'c_d_id', ('Type_1', 3)), (0, 'WHERE', 'customer', 'c_id', ('Type_1', 502)), (0, 'WHERE', 'customer', 'c_w_id', ('Type_4', 'warehouse.w_id')), (0, 'WHERE', 'warehouse', 'w_id', ('Type_1', 3)), (0, 'SELECT', 'customer', 'c_credit', ('Type_3', '?')), (0, 'SELECT', 'customer', 'c_discount', ('Type_3', '?')), (0, 'SELECT', 'customer', 'c_last', ('Type_3', '?')), (0, 'SELECT', 'warehouse', 'w_tax', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'district', 'd_id', ('Type_1', 3)), (0, 'WHERE', 'district', 'd_w_id', ('Type_1', 3)), (0, 'SELECT', 'district', 'd_next_o_id', ('Type_3', '?')), (0, 'SELECT', 'district', 'd_tax', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'district', 'd_id', ('Type_1', 3)), (0, 'WHERE', 'district', 'd_w_id', ('Type_1', 3)), (0, 'UPDATE', 'district', 'd_next_o_id', ('Type_1', '3002+1'))]")
	lis.append("[(0, 'INSERT', 'orders', 'o_all_local', ('Type_1', 1)), (0, 'INSERT', 'orders', 'o_c_id', ('Type_1', 502)), (0, 'INSERT', 'orders', 'o_d_id', ('Type_1', 3)), (0, 'INSERT', 'orders', 'o_entry_d', ('Type_1', '2019-07-02 15:48:22')), (0, 'INSERT', 'orders', 'o_id', ('Type_1', 3002)), (0, 'INSERT', 'orders', 'o_ol_cnt', ('Type_1', 13)), (0, 'INSERT', 'orders', 'o_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'INSERT', 'new_orders', 'no_d_id', ('Type_1', 3)), (0, 'INSERT', 'new_orders', 'no_o_id', ('Type_1', 3002)), (0, 'INSERT', 'new_orders', 'no_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 1027)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 1027)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 1027)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 34))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '483.44244384765625')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', '1vfLs9GtMQdKbWnKau0JXclr')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 1027)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 1)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 5)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 7860)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 7860)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 7860)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 7))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '433.400390625')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', 'JLOHu7DlYm3eufJCoqcgQ8KZ')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 7860)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 2)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 5)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 20395)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 20395)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 20395)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 89))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '509.139404296875')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', 'JFbKOADHFS6gnFiNiWRQaSiU')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 20395)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 6)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 24483)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 24483)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 24483)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 49))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '897.2976684570312')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', 'Br4tORXygdsKXuvsfumTXlNa')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 24483)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 4)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 10)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 31489)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 31489)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 31489)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 41))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '41.98288345336914')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', 'm5IhHVtjS2TDfBTRJJj3JVOA')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 31489)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 5)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 2)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 33779)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 33779)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 33779)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 41))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '32.750003814697266')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', 'pxCHerJdHRcxNfhNeut20YMI')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 33779)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 6)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 1)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 57347)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 57347)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 57347)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 29))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '518.3408203125')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', 'y6gIxmk4hCpVB05XKohsnafk')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 57347)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 7)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 10)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 66563)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 66563)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 66563)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 33))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '187.73873901367188')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', '0SLywe2PYIiYJNRYOQby01zz')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 66563)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 8)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 2)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 72063)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 72063)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 72063)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 41))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '622.6378173828125')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', 'yI77Ac6LAx2e40XDBGlCpRJZ')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 72063)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 9)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 6)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 73729)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 73729)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 73729)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 23))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '467.1984558105469')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', 'HCVxbmlgVKPDiTIvmdAUAKSM')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 73729)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 10)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 5)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 81847)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 81847)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 81847)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 75))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '20.876161575317383')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', 'x9m4GNW0Ud842kEQeYR3kg6x')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 81847)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 11)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 4)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 85857)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 85857)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 85857)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 93))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '153.0289764404297')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', 'hbmE8uxYclQZgsV1EG3bhpfC')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 85857)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 12)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 2)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[(0, 'WHERE', 'item', 'i_id', ('Type_1', 88035)), (0, 'SELECT', 'item', 'i_data', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_name', ('Type_3', '?')), (0, 'SELECT', 'item', 'i_price', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 88035)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'SELECT', 'stock', 's_data', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_01', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_02', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_03', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_04', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_05', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_06', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_07', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_08', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_09', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_dist_10', ('Type_3', '?')), (0, 'SELECT', 'stock', 's_quantity', ('Type_3', '?'))]")
	lis.append("[(0, 'WHERE', 'stock', 's_i_id', ('Type_1', 88035)), (0, 'WHERE', 'stock', 's_w_id', ('Type_1', 3)), (0, 'UPDATE', 'stock', 's_quantity', ('Type_1', 61))]")
	lis.append("[(0, 'INSERT', 'order_line', 'ol_amount', ('Type_1', '630.5816040039062')), (0, 'INSERT', 'order_line', 'ol_d_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_dist_info', ('Type_1', '2uGZfQ0OZV1QJ2Hmlo63dO7o')), (0, 'INSERT', 'order_line', 'ol_i_id', ('Type_1', 88035)), (0, 'INSERT', 'order_line', 'ol_number', ('Type_1', 13)), (0, 'INSERT', 'order_line', 'ol_o_id', ('Type_1', 3002)), (0, 'INSERT', 'order_line', 'ol_quantity', ('Type_1', 10)), (0, 'INSERT', 'order_line', 'ol_supply_w_id', ('Type_1', 3)), (0, 'INSERT', 'order_line', 'ol_w_id', ('Type_1', 3))]")
	lis.append("[('Commit',)]")

	txn = {}
	txn_sql_list = {}
	process_log(txn, txn_sql_list, lis)
	
	txn = index_builder(txn)
	count = 0
	for key in txn:
		for sql in key:
			count += len(sql)
			pprint.pprint(sql)
			index = key.index(sql)
			for inst in txn[key]:
				print(inst[index])
	# pprint.pprint(txn)

	print(len(txn))
	print(count)
	return

if __name__ == "__main__":
	before = int(round(time.time() * 1000))
	main()
	after = int(round(time.time() * 1000))
	print("Spend {0} ms.".format(after - before))
	# test()

"""
# txn_dic = read_log("sqlOpsList_tpcc_manual_column.txt")
txn_dic = read_log("sqlOpsList_tpcc_small_column.txt")
txn_dic = index_builder(txn_dic)

final_result = []
i = 1
for txn in txn_dic:
	print("process "+str(i))
	final_result.append((txn, find_associate(txn, txn_dic[txn])))
	i = i+1

# pprint.pprint(final_result[0])

# sys.exit(1)
# pprint(len(log))
# i = 1
# for key in txn:
	# print("{0}: {1}\n --- \n{2}".format(i, pprint.pformat(key), pprint.pformat(txn[key])))
	# break
	# i = i+1
# pprint.pprint(len(txn))
# pprint(log)
"""
