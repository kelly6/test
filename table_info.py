#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import json
import redis
import ujson
import config
import datetime
import Queue
try:
    import MySQLdb
except:
    pass
import traceback
import threading
import logging.config
import sqlite3
import pickle
try:
    import sqlite_fulltext as wp
except:
    pass
from itertools import product

class out_of_depth_limit(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr("out_of_depth_limit: depth == " + str(self.value))

#解析words dic
#最早设想 暂时没有用
class term_parser:
    def __init__(self):
        self.depth = 0
        self.max_depth = 1000
        self.valid_flag = 1
    
    def parse_near(self, item_pair):
        if len(item_pair[1]) < 2:
            return ""
        if isinstance(item_pair[1][0], unicode) and isinstance(item_pair[1][1], unicode):
            return u"(" + item_pair[1][0] + " NEAR " + item_pair[1][1] + u")"
        if isinstance(item_pair[1][0], unicode):
            s = item_pair[1][0]
            s2_pair = item_pair[1][1].popitem()
            s2_dic = {s2_pair[0]:[]}
            for s2 in s2_pair[1]:
                item = {u"NEAR":[s, s2]}
                s2_dic[s2_pair[0]].append(item)
            return self.parse_with_depth(s2_dic)
        elif isinstance(item_pair[1][1], unicode):
            s_pair = item_pair[1][0].popitem()
            s2 = item_pair[1][1]
            s_dic = {s_pair[0]:[]}
            for s in s_pair[1]:
                item = {u"NEAR":[s, s2]}
                s_dic[s_pair[0]].append(item)
            return self.parse_with_depth(s_dic)
        else:
            s_pair = item_pair[1][0].popitem()
            s2_pair = item_pair[1][1].popitem()

            s_dic = {s_pair[0]:[]}
            for s in s_pair[1]:
                s2_dic = {s2_pair[0]:[]}
                for s2 in s2_pair[1]:
                    item = {"NEAR":[s, s2]}
                    s2_dic[s2_pair[0]].append(item)
                s_dic[s_pair[0]].append(s2_dic)
            return self.parse_with_depth(s_dic)

    #如果出错返回None
    #否则返回字符串
    def parse(self, term):
        self.depth = 0
        self.valid_flag = 1
        try:
            ret = self.parse_with_depth(term)
        except out_of_depth_limit, reason:
            print reason
            return None
        if not self.valid_flag:
            return None;
        if isinstance(ret, unicode):
            ret = ret.encode("utf-8")
        return ret

    def val_check(self, item_pair):
        if len(item_pair[1]) < 2:
            #todo:如果需要显示出错逻辑, 可在此处赋值item_pair
            self.valid_flag = 0
            return 0
        return 1
        if item_pair[0].upper() == "NOT":
            pass

    def parse_with_depth(self, term):
        if not self.valid_flag:
            return ""
        if self.depth > self.max_depth:
            raise out_of_depth_limit(self.depth)
        self.depth += 1
        if isinstance(term, str) or isinstance(term, unicode):
            return term
        try:
            item_pair = term.popitem()
        except KeyError, reason:
            return ""
        if not self.val_check(item_pair):
            return ""
        if item_pair[0].upper() == "NEAR":
            return self.parse_near(item_pair)
        if len(item_pair[1]) < 2:
            return ""
        key = item_pair[0]
        space_key = u" " + key.upper() + u" "
        return u"(" + space_key.join([self.parse_with_depth(t) for t in item_pair[1]]) + u")"

class raw_info_parser:
    def __init__(self):
        raw_info_parser.table_info_hkey = config.table_info_hkey
        self.table_info = {}
        self.raw_info_parser_config()
        if not hasattr(raw_info_parser, "rhd"):
            raw_info_parser.rhd = redis.from_url(config.redis_url)
        hkeys = raw_info_parser.rhd.hkeys(raw_info_parser.table_info_hkey)
        for config_id in hkeys:
            self.refresh_table_info(config_id)

    def raw_info_parser_config(self):
        raw_info_parser.one2one = "one2one"
        raw_info_parser.one2multi = "one2multi"
        raw_info_parser.multi2multi = "multi2multi"
        raw_info_parser.data_idx = 0
        raw_info_parser.table_idx = 1
        raw_info_parser.type_idx = 2
        raw_info_parser.data_col_idx = 0
        raw_info_parser.table_col_idx = 1
        raw_info_parser.map_type_idx = 2

    def exists(self, config_id):
        #self.refresh_table_info(config_id)
        return raw_info_parser.rhd.hexists(raw_info_parser.table_info_hkey, config_id)

    def get_config_id(self, config_id):
        #self.refresh_table_info(config_id)
        v = raw_info_parser.rhd.hget(config.table_info_hkey, config_id)
        return ujson.loads(v)
    
    def get_create_sql(self, config_id):
        #self.refresh_table_info(config_id)
        return self.table_info.get(config_id, {}).get("create_sql", [])

    def gen_create_sql(self, config_id, table_v):
        td = table_v
        create_sql = []
        for table in td["tables"]:
            name = table["table_name"]
            content_list = table["content"]
            index_list = table.get("index", [])
            create_sql.extend(self.get_create_sql_by_items(name, content_list, index_list))
        return create_sql

    def get_create_sql_by_items(self, name, content_list, index_list):
        sqls = []
        cols = []
        for content in content_list:
            s = " %s %s %s " % (content[0], content[1], content[3])
            if content[4]:
                s += " default %s " % content[4]
            cols.append(s)
        create_sql = """ create table %s (%s); """ % (name, ",\n".join(cols))
        sqls.append(create_sql)
        for index in index_list:
            if not index[0]:
                continue
            sqls.append("""create %s INDEX %s_%s_index on %s (%s)""" % (" UNIQUE " if index[2] else "", name, index[0], name, index[1]))
        return sqls

    def get_table_level_dic(self, config_id):
        #self.refresh_table_info(config_id)
        return self.table_info[config_id]["table_level"]

    def get_col_list(self, config_id, table_name):
        #self.refresh_table_info(config_id)
        #print "config_id check:", config_id in self.table_info
        #print "table_info check:", "table_info" in self.table_info[config_id]
        #print "table_name:", table_name, type(table_name)
        #print "table_name dic:", self.table_info[config_id]["table_info"]
        #print "table_name check:", table_name in self.table_info[config_id]["table_info"]
        return self.table_info.get(config_id, {}).get("table_info", {}).get("col_list", {}).get(table_name, [])

    def get_table_list(self, config_id):
        #self.refresh_table_info(config_id)
        return self.table_info.get(config_id, {}).get("table_name_list", [])

    def get_col_data_path_list(self, config_id, table_name, col):
        #todo
        #这里需要更新, 暂不考虑更新的情况
        #self.refresh_table_info(config_id)
        data_path_list = self.table_info[config_id]["data_path_list"]
        return (data_path_list.get(table_name, {}).get(col, []), self.get_table_type(config_id, table_name))

    def get_data_path_deepth(self, config_id, table_name):
        #self.refresh_table_info(config_id)
        return self.table_info[config_id]["list_deepth_dic"][table_name]

    def gen_col_list(self, config_id, table):
        table_type_dic = self.table_info[config_id]["table_type_dic"]
        col_list = []
        table_name = table[u"table_name"].encode("utf8")
        map_type = table_type_dic[table_name]
        for col_obj in table[u"content"]:
            col_list.append(col_obj[0].encode("utf8"))
        if map_type == raw_info_parser.multi2multi:
            col_list = [col_list]
        return col_list

    def is_parent(self, config_id, table_name):
        return table_name in self.table_info[config_id]["parent_set"]

    def get_table_type(self, config_id, table_name):
        #self.refresh_table_info(config_id)
        return self.table_info[config_id]["table_type_dic"][table_name]

    def get_main_table(self, config_id):
        #self.refresh_table_info(config_id)
        table_list = self.table_info.get(config_id, {}).get("table_name_list", [])
        parent_dic = self.table_info.get(config_id, {}).get("parent_dic", {})
        for table in table_list:
            if table not in parent_dic:
                return table
        return None

    def get_dump_col_set(self, config_id):
        return self.table_info[config_id]["dump_col_set"]

    def get_list_col_set(self, config_id):
        return self.table_info[config_id]["list_col_set"]

    def get_table_index(self, config_id, table_name):
        return self.table_info[config_id]["table_index"][table_name]

    def get_table_index_dic(self, config_id):
        return self.table_info[config_id]["table_index"]

    def get_table_parent_dic(self, config_id):
        #self.refresh_table_info(config_id)
        return self.table_info[config_id]["parent_dic"]

    def get_table_children_dic(self, config_id):
        #self.refresh_table_info(config_id)
        return self.table_info[config_id]["children_dic"]

    def gen_table_parent_dic(self, config_id, table_v):
        mapping = table_v[u"mapping"]
        ret = {}
        for item in mapping:
            parent = item[0].split(u".")[0].encode("utf8")
            child = item[1].split(u".")[0].encode("utf8")
            ret[child] = parent
        return ret

    def gen_table_children_dic(self, config_id, table_v):
        mapping = table_v[u"mapping"]
        ret = {}
        for item in mapping:
            parent = item[0].split(u".")[0].encode("utf8")
            child = item[1].split(u".")[0].encode("utf8")
            if parent not in ret:
                ret[parent] = []
            ret[parent].append(child)
        return ret

    def gen_table_parent_set(self, config_id, table_v):
        mapping = table_v[u"mapping"]
        ret = set()
        for item in mapping:
            parent = item[0].split(".")[0].encode("utf8")
            ret.add(parent)
        return ret

    def gen_table_index(self, config_id, table_v):
        mapping = table_v[u"mapping"]
        list_table_name = set()
        for item in mapping:
            sp = item[1].split(".")
            list_table_name.add(sp[0].encode("utf8"))

        table_list = self.table_info.get(config_id, {}).get("table_name_list", [])
        ret = {}
        for table_name in table_list:
            col_list = self.table_info.get(config_id, {}).get("table_info", {}).get("col_list", {}).get(table_name, [])
            __url_idx = -1
            url_idx = -1
            gtime_idx = -1
            for idx, col in enumerate(col_list):
                if col == "__url":
                    __url_idx = idx
                elif col == "url":
                    url_idx = idx
                elif col == "__gtime":
                    gtime_idx = idx
            ret[table_name] = ("list" if table_name in list_table_name else "", (("__url", __url_idx) if table_name in list_table_name else ("", -1), ("url", url_idx), ("__gtime", gtime_idx)))
        return ret

    def gen_dump_col_set(self, config_id, table_v):
        storage = table_v[u"storage"]
        ret = {}
        for item in storage:
            sp = item[1].split(u".")
            table_name = sp[0].encode("utf8")
            col_name = sp[1].encode("utf8")
            if table_name not in ret:
                ret[table_name] = set()
            if item[3] == u"dump":
                ret[table_name].add(col_name)
        return ret

    def gen_list_col_set(self, config_id, table_v):
        storage = table_v[u"storage"]
        ret = {}
        for item in storage:
            sp = item[1].split(u".")
            table_name = sp[0].encode("utf8")
            col_name = sp[1].encode("utf8")
            if table_name not in ret:
                ret[table_name] = set()
            if item[3] == u"list":
                ret[table_name].add(col_name)
        return ret

    def gen_table_level(self, config_id, table_v):
        storage = table_v[u"storage"]
        table_level_dic = {}
        for item in storage:
            sp = item[1].split(u".")
            table_name = sp[0].encode("utf8")
            level = 0
            sp = item[0].split(u".")
            for i in sp:
                if i == u"__list__":
                    level += 1
            if table_name not in table_level_dic:
                table_level_dic[table_name] = 0
            if table_level_dic[table_name] < level:
                table_level_dic[table_name] = level
        return table_level_dic

    def gen_data_path_list(self, config_id, table_v):
        storage_list = table_v[u"storage"]
        data_path_list = {}
        table_type_dic = {}
        list_deepth_dic = {}
        for storage in storage_list:
            data_col = storage[raw_info_parser.data_col_idx].encode("utf8")
            table_col = storage[raw_info_parser.table_col_idx].encode("utf8")
            map_type = storage[raw_info_parser.map_type_idx].encode("utf8")
            table_sp = table_col.split(".")
            data_sp = data_col.split(".")
            table_name = table_sp[0]
            if table_name not in data_path_list:
                data_path_list[table_sp[0]] = {}
            data_path = [list if i == "__list__" else i for i in data_sp[1:]]
            data_path_list[table_sp[0]][table_sp[1]] = data_path
            list_deepth = 0
            for i in data_path:
                if i is list:
                    list_deepth += 1
            tmp_v = list_deepth_dic.get(table_sp[0], 0)
            list_deepth_dic[table_sp[0]] = list_deepth if tmp_v < list_deepth else tmp_v
            table_type_dic[table_sp[0]] = map_type
        #for table_name in data_path_list:
        #    if table_type_dic[table_name] == raw_info_parser.multi2multi:
        #        old_table_dic = data_path_list[table_name].items()
        #        col_list = ["__url"]
        #        #path_list = old_table_dic[0][1][:-1]
        #        path_list = [["url"]]
        #        for col, pathl in old_table_dic:
        #            col_list.append(col)
        #            #print "pathl:", pathl
        #            path_list.append(pathl)
        #        data_path_list[table_name] = {tuple(col_list):path_list}

        return (data_path_list, table_type_dic, list_deepth_dic)

    def update_table_info_s(self, config_id, table_s):
        table_v = ujson.loads(table_s)
        self.table_info[config_id] = {}
        self.table_info[config_id]["raw_s"] = table_s
        self.table_info[config_id]["table_data"] = table_v
        self.table_info[config_id]["create_sql"] = self.gen_create_sql(config_id, table_v)
        self.table_info[config_id]["table_level"] = self.gen_table_level(config_id, table_v)
        self.table_info[config_id]["list_col_set"] = self.gen_list_col_set(config_id, table_v)
        self.table_info[config_id]["dump_col_set"] = self.gen_dump_col_set(config_id, table_v)
        data_path_list, table_type_dic, list_deepth_dic = self.gen_data_path_list(config_id, table_v)
        self.table_info[config_id]["data_path_list"] = data_path_list
        self.table_info[config_id]["table_type_dic"] = table_type_dic
        self.table_info[config_id]["list_deepth_dic"] = list_deepth_dic
        self.table_info[config_id]["parent_set"] = self.gen_table_parent_set(config_id, table_v)
        self.table_info[config_id]["parent_dic"] = self.gen_table_parent_dic(config_id, table_v)
        self.table_info[config_id]["children_dic"] = self.gen_table_children_dic(config_id, table_v)
        table_info = {}
        table_info["col_list"] = {}
        for table_name in data_path_list:
            table_info["col_list"][table_name] = data_path_list[table_name].keys()
        self.table_info[config_id]["table_info"] = table_info
        self.table_info[config_id]["table_name_list"] = data_path_list.keys()
        self.table_info[config_id]["table_index"] = self.gen_table_index(config_id, table_v)
        #for table in table_v[u"tables"]:
        #    table_name = table[u"table_name"].encode("utf8")
        #    table_name_list.append(table_name)
        #    #print "config_id:%s, table:%s" % (config_id, table)
        #    table_info["col_list"][table_name] = self.gen_col_list(config_id, table)

    def refresh_table_info(self, config_id):
        #print "config.table_info_hkey:%s, config_id:%s" % (config.table_info_hkey, config_id)
        s = raw_info_parser.rhd.hget(config.table_info_hkey, config_id)
        if not s:
            s = "{}"
        if config_id in self.table_info and self.table_info[config_id]["raw_s"] != s:
            return
        else:
            self.update_table_info_s(config_id, s)

class table_gen:
    def __init__(self, config_id):
        #self.td_list = td_list
        self.info_parser = raw_info_parser()
        self.config_id = config_id
        self.multi2multi = raw_info_parser.multi2multi

    def verify(self):
        #check for proper
        return 1

    def get_create_sql(self):
        td = self.info_parser.get_config_id(self.config_id)
        return self.info_parser.get_create_sql(self.config_id)

    def get_col_list(self, table_name):
        return self.info_parser.get_col_list(self.config_id, table_name)
        #[(col_name, "col type")...]
        tmp = [ ("url", str), ("title", str), ("content", str), ("config_id", str), ("siteName", str), ("source", str), ("keywords", str), ("gtime", int), ("ctime", int)]
        return ([col[0] for col in tmp], [col[1] for col in tmp])
    
    def get_table_list(self):
        return self.info_parser.get_table_list(self.config_id)

    def get_table_name(self, info_flag):
        #todo
        if info_flag == "10":
            return "comment"
        else:
            return "info"

    def get_table_type(self, table_name):
        return self.info_parser.get_table_type(self.config_id, table_name)

    def parse_data(self, data):
        table_list = self.info_parser.get_table_list(self.config_id)

    def get_list_flag(self, table_name):
        #todo 弃用此接口
        return 0

    def __get_col_data(self, v, path_list, idx):
        #return (value, value_type)
        if idx == len(path_list) - 1:
            ret = v[path_list[idx]]
            if isinstance(ret, list) and not ret:
                ret = None
            #print "ret:", v[path_list[idx]]
            return (ret, 1 if isinstance(ret, list) else 0)
        if path_list[idx] is list:
            ret = []
            list_flag = 0
            for item in v:
                v, list_flag_tmp = self.__get_col_data(item, path_list, idx + 1)
                list_flag |= list_flag_tmp
                ret.append(v)
            return (ret, list_flag)
        else:
            return self.__get_col_data(v[path_list[idx]], path_list, idx + 1)

    def get_col_list_data(self, table_name, data):
        #return (col_list, transed_data_list, list_flag, list_len)
        #print "going in get_col_list_data", table_name
        if table_name == "info":
            debug = 1
        else:
            debug = 0
        col_list = self.get_col_list(table_name)
        insert_data = []
        list_flag = 0
        deepth = self.info_parser.get_data_path_deepth(self.config_id, table_name)
        dump_col_set = self.info_parser.get_dump_col_set(self.config_id)
        #print " in get col list data table_name:", table_name
        for col in col_list:
            path_list, map_type = self.info_parser.get_col_data_path_list(self.config_id, table_name, col)
            v, list_flag_tmp = self.__get_col_data(data, path_list, 0)
            if col in dump_col_set[table_name]:
                if v:
                    v = ujson.dumps(v)
                else:
                    v = ujson.dumps([])
            if isinstance(v, list) and not v:
                return ([], [], 0)
            list_flag |= list_flag_tmp
            #print "v:", v
            #print "list:", type(v)
            #if deepth > 1 and not isinstance(v, list):
            #    v = [v]
            insert_data.append(v)
            #list_len = 0
            #if isinstance(v, list):
            #    list_len = len(v)
        if deepth > 0:
            insert_data = [tuple(insert_data)]
        tmp_insert = []
        for i in range(deepth):
            for item in insert_data:
                max_len = 0
                for col_item in item:
                    if isinstance(col_item, list):
                        if len(col_item) > max_len:
                            max_len = len(col_item)
                if max_len:
                    tmpl = [d if isinstance(d, list) else [d] * max_len for d in item]
                    tmp_insert.extend(zip(*tmpl))
                else:
                    tmp_insert.append(item)
            insert_data = tmp_insert
            tmp_insert = []
        if deepth < 1:
            insert_data = [insert_data]
        return col_list, insert_data, list_flag

class storage_info:
    def __init__(self):
        self.info_parser = raw_info_parser()

    def get_create_sql(self, config_id):
        self.table_gen = table_gen(config_id)
        return self.table_gen.get_create_sql()

    def gen_result_list(self, dlist, list_flag):
        #while list_len:
        #    dlist = zip(*[d if isinstance(d, list) else [d] * list_len for d in dlist])
        #    for d in dlist:
        #        if isinstance(d, list):
        #            list_len = len(d)
        #    #print "list_len:", list_len
        #    list_len = 0
        if list_flag:
            #print "in list_flag before dlist:", dlist
            ret_list = []
            for item in dlist:
                #print "before item:", item
                tmp = list(product(*[d if isinstance(d, list) else [d] for d in item]))
                #print "after tmp:", tmp
                #with open("/home/kelly/temp/temp.json", "w") as fd:
                #    ujson.dump(tmp, fd)
                #exit()
                ret_list.extend(tmp)
            #return list(product(*dlist))
            #with open("/home/kelly/temp/temp.json", "w") as fd:
            #    ujson.dump(ret_list, fd)
            #exit()
            return ret_list
        else:
            #return [dlist]
            return dlist
            #if list_len:
            #    return dlist
            #else:
            #    return [dlist]

    def get_config_id_by_fname(self, fname):
        sp = fname.split("_")
        sp = sp[:-1]
        return "_".join(sp)

    def get_rebuild_info_by_fname(self, fname):
        config_id = self.get_config_id_by_fname(fname)
        return self.get_rebuild_info(config_id)

    def get_rebuild_info(self, config_id):
        #([table1, table2, ...], [col_list1, col_list2, ...])
        #todo
        return ["info", "comment"], [["url", "title", "content", "config_id", "siteName", "source", "keywords", "gtime", "ctime"], ["url", "title", "content", "ctime"]]

    def get_insert_sqls(self, data):
        #返回(sql_template, data_list)
        #单条也是list
        config_id = str(data.get(u"config_id", None))
        if not config_id:
            return []
        self.table_gen = table_gen(config_id)
        #table_name = self.table_gen.get_table_name(info_flag)
        result_list = []
        table_name_list = self.table_gen.get_table_list()
        #print "table_name_list:", table_name_list
        for table_name in table_name_list:
            if table_name == "info":
                debug = 1
            else:
                debug = 0
            col_list, data_list, list_flag = self.table_gen.get_col_list_data(table_name, data)
            if not data_list:
                continue
            data_list = self.gen_result_list(data_list, list_flag)
            col_str = ",".join(col_list)
            sql = "insert into %s (%s) values (%s)" % (table_name, col_str, ",".join(["?"] * len(data_list[0])))
            result_list.append((sql, data_list))
        return result_list

    def is_special_config_id(self, config_id):
        return 1

class dedup_tables():
    def __init__(self, config_id):
        self.config_id = config_id
        self.info_parser = raw_info_parser()
        self.table_path = config.table_path
        self.day_back = config.day_back

    def gen_dbpath_list(self):
        dbpath_list = ["/home/kelly/temp/-2_real.db"]
        #dbpath_list = ["/home/kelly/temp/test.db"]
        #print "dbpath_list:", dbpath_list
        return dbpath_list
        dbpath_list = []
        today = datetime.datetime.now().strftime("%Y%m%d")
        dbpath = os.path.join(self.table_path, today, str(self.config_id) + today + ".db")
        if os.path.exists(dbpath):
            dbpath_list.append(dbpath)
        for i in range(self.day_back):
            dt = (datetime.datetime.now() - datetime.timedelta(days = i)).strftime("%Y%m%d")
            dbpath = os.path.join(self.table_path, dt, str(self.config_id) + "_" +dt + ".db")
            if os.path.exists(dbpath):
                dbpath_list.append(dbpath)
        #dbpath_list = ["/home/kelly/temp/test.db"]
        #print "dbpath_list:", dbpath_list
        return dbpath_list

    def gen_dedup_dic(self):
        main_table = self.info_parser.get_main_table(self.config_id)
        if not main_table:
            return {}
        dbpath_list = self.gen_dbpath_list()
        dbpath_url_dic = dict.fromkeys(dbpath_list, {})
        g_url_dic = {}
        url_dbpath_dic = {}
        self.dbpath_conn_dic = {}
        for dbpath in dbpath_list:
            conn = sqlite3.connect(dbpath)
            self.dbpath_conn_dic[dbpath] = conn
            cur = conn.cursor()
            print "dbpath:", dbpath
            sql = "select url, __gtime from %s" % (main_table)
            cur.execute(sql)
            l = cur.fetchall()
            for i in l:
                url = i[0]
                __gtime = i[1]
                if url not in g_url_dic:
                    g_url_dic[url] = __gtime
                    url_dbpath_dic[url] = dbpath
                    dbpath_url_dic[dbpath][url] = __gtime
                elif __gtime > g_url_dic[url]:
                    g_url_dic[url] = __gtime
                    url_dbpath_dic[url] = dbpath
                    if url in url_dbpath_dic:
                        del dbpath_url_dic[url_dbpath_dic[url]][url]
                    dbpath_url_dic[dbpath][url] = __gtime
        return dbpath_url_dic

    def gen_raw_data(self):
        dbpath_url_dic = self.gen_dedup_dic()
        #with open("/home/kelly/temp/test.json", "w") as fd:
        #    ujson.dump(dbpath_url_dic, fd)
        #print "dbpath_url_dic dump done."
        #exit()
        table_list = self.info_parser.get_table_list(self.config_id)
        children_dic = self.info_parser.get_table_children_dic(self.config_id)
        main_table = self.info_parser.get_main_table(self.config_id)
        table_index_dic = self.info_parser.get_table_index_dic(self.config_id)
        col_type_dic = self.info_parser.get_list_col_set(self.config_id)
        for dbpath in dbpath_url_dic:
            for url in dbpath_url_dic[dbpath]:
                task = (dbpath, url, dbpath_url_dic[dbpath][url])
                work_queue = Queue.Queue()
                work_queue.put((main_table, "url", task))
                data = {}
                url_data = {}
                __url_data = {}
                index_gtime = {}
                while 1:
                    if work_queue.empty():
                        break
                    task_tuple = work_queue.get()
                    table_name, url_colname, task = task_tuple
                    ldbpath, url, __gtime = task

                    col_list = self.info_parser.get_col_list(self.config_id, table_name)

                    data[table_name] = {}
                    url_data[table_name] = {}
                    __url_data[table_name] = {}
                    index_gtime[table_name] = {}
                    l_col_type_dic = col_type_dic[table_name]
                    __url_idx = table_index_dic[table_name][1][0][1]
                    url_idx = table_index_dic[table_name][1][1][1]
                    gtime_idx = table_index_dic[table_name][1][2][1]
                    list_flag = 1 if table_index_dic[table_name][0] == "list" else 0

                    conn = self.dbpath_conn_dic[dbpath]
                    cur = conn.cursor()
                    cur.execute("select %s from %s where %s = ? and __gtime = ?" % (",".join(col_list), table_name, url_colname), (url, __gtime))
                    l = cur.fetchall()
                    if not l:
                        continue
                    i = l[0]
                    is_parent = self.info_parser.is_parent(self.config_id, table_name)
                    if is_parent:
                        children = children_dic[table_name]
                        for child in children:
                            task = (dbpath, i[url_idx], i[gtime_idx])
                            work_queue.put((child, "__url", task))
                    for i in l:
                        if list_flag:
                            __url = i[__url_idx]
                        else:
                            __url = ""
                        if url_idx >= 0:
                            url = i[url_idx]
                        else:
                            url = ""
                        gtime = i[gtime_idx]
                        new_gtime = gtime
                        index = str(__url) + str(url) + str(gtime)
                        if index in data[table_name]:
                            if gtime < index_gtime[table_name][index]:
                                continue
                            elif gtime == index_gtime[table_name][index]:
                                if is_parent:
                                    item = data[table_name][index][url]
                                else:
                                    item = {}
                                for idx, col in enumerate(col_list):
                                    if col in l_col_type_dic and i[idx]:
                                        if i[idx]:
                                            if col not in item:
                                                item[col] = set()
                                            item[col].add(i[idx])
                                        else:
                                            item[col] = set()
                                if is_parent:
                                    if url not in data[table_name][index]:
                                        data[table_name][index][url] = item
                                else:
                                    if index not in data[table_name]:
                                        data[table_name][index] = []
                                    data[table_name][index].append(item)
                            else:
                                index_gtime[table_name][index] = gtime
                                for idx, col in enumerate(col_list):
                                    if col in l_col_type_dic:
                                        if i[idx]:
                                            data[table_name][index][col] = set([i[idx]])
                                        else:
                                            data[table_name][index][col] = set()
                                    else:
                                        data[table_name][index][col] = i[idx]
                        else:
                            data[table_name][index] = {}
                            index_gtime[table_name][index] = gtime
                            item = {}
                            for idx, col in enumerate(col_list):
                                if col in l_col_type_dic:
                                    if i[idx]:
                                        item[col] = set([i[idx]])
                                    else:
                                        item[col] = set()
                                else:
                                    item[col] = i[idx]
                            if is_parent:
                                data[table_name][index][url] = item
                            else:
                                data[table_name][index] = [item]
                        if url:
                            lurl_index = url
                        elif __url:
                            lurl_index = __url
                        if not is_parent:
                            url_data[table_name][lurl_index] = data[table_name][index]
                        else:
                            if lurl_index not in url_data[table_name]:
                                url_data[table_name][lurl_index] = type(data[table_name][index])()
                            if isinstance(data[table_name][index], list):
                                url_data[table_name][lurl_index].extend(data[table_name][index])
                            elif isinstance(data[table_name][index], dict):
                                url_data[table_name][lurl_index].update(data[table_name][index])
                        if __url:
                            lurl_index = __url
                        elif url:
                            lurl_index = url
                            #if url not in __url_data[table_name]:
                            #    __url_data[table_name][url] = {}
                            #__url_data[table_name][url].update(data[table_name][index])

                        #todo
                        if not is_parent:
                            __url_data[table_name][lurl_index] = data[table_name][index]
                        else:
                            if lurl_index not in __url_data[table_name]:
                                __url_data[table_name][lurl_index] = type(data[table_name][index])()
                            if isinstance(data[table_name][index], list):
                                __url_data[table_name][lurl_index].extend(data[table_name][index])
                            elif isinstance(data[table_name][index], dict):
                                __url_data[table_name][lurl_index].update(data[table_name][index])
                yield url_data, __url_data

class restore_info:
    def __init__(self):
        begin = datetime.datetime.now()
        self.info_parser = raw_info_parser()
        end = datetime.datetime.now()
        self.time_info_parser_init = end - begin
        #self.table_path = config.table_path
        #self.day_back = config.day_back

    def gen_raw_data(self, config_id, table_name_list):
        #{"table_name":set(["col1", "col2"])}
        col_type_dic = self.info_parser.get_list_col_set(config_id)

        #print "col_type_dic:", col_type_dic
        data = {}
        url_data = {}
        __url_data = {}
        index_gtime = {}
        #如果table is_parent 则类型为{__url:{ url:{} }} 子表合并后再转成list
        #否则 类型为{__url:[]}
        for table_name_list_idx, table_name in enumerate(table_name_list):
            if table_name == "info":
                debug = 1
            else:
                debug = 0
            is_parent = self.info_parser.is_parent(config_id, table_name)
            col_list = self.info_parser.get_col_list(config_id, table_name)
            dbpath_list = self.gen_dbpath_list(config_id)
            data[table_name] = {}
            url_data[table_name] = {}
            __url_data[table_name] = {}
            index_gtime[table_name] = {}
            #print "col_list:", col_list, table_name
            l_col_type_dic = col_type_dic[table_name]
            #table_index:("list", (("__url", 3), ("url", 5), ("__gtime", 4)))
            table_index = self.info_parser.get_table_index(config_id, table_name)
            #print "table_index:", table_index
            #如果不是list table 则__url为空 index = -1 下面不使用此字段
            #print "table_index:", table_index
            __url_idx = table_index[1][0][1]
            url_idx = table_index[1][1][1]
            gtime_idx = table_index[1][2][1]
            list_flag = 1 if table_index[0] == "list" else 0
            #for idx, col in enumerate(col_list):
            #    #if col in l_col_type_dic:
            #    #    data[table_name][col] = []
            #    #todo 如果有需要改为动态生成idx
            #    if col == "url":
            #        url_idx = idx
            #    elif col == "__gtime":
            #        gtime_idx = idx
            for dbpath in dbpath_list:
                conn = sqlite3.connect(dbpath)
                cur = conn.cursor()
                #print "table_name:%s, col_list:%s" % (table_name, ",".join(col_list))
                sql = "select %s from %s" % (",".join(col_list), table_name)
                #print "sql:", sql
                #print "dbpath:", dbpath
                cur.execute(sql)
                l = cur.fetchall()
                for i in l:
                    if list_flag:
                        __url = i[__url_idx]
                    else:
                        __url = ""
                    if url_idx >= 0:
                        url = i[url_idx]
                    else:
                        url = ""
                    gtime = i[gtime_idx]
                    new_gtime = gtime
                    #print "__url:", __url, type(__url)
                    #print "url:", url, type(url)
                    #print "gtime:", gtime, type(gtime)
                    #print "gtime index:", gtime_idx
                    #print "table:", table_name
                    index = str(__url) + str(url) + str(gtime)
                    if index in data[table_name]:
                        #print "data[%s]:%s" % (table_name, data[table_name])
                        #print "__gtime:", "__gtime" in data[table_name]
                        if gtime < index_gtime[table_name][index]:
                            continue
                        elif gtime == index_gtime[table_name][index]:
                            if is_parent:
                                item = data[table_name][index][url]
                            else:
                                item = {}
                            for idx, col in enumerate(col_list):
                                if col in l_col_type_dic and i[idx]:
                                    if i[idx]:
                                        if col not in item:
                                            item[col] = set()
                                        item[col].add(i[idx])
                                    else:
                                        item[col] = set()
                                #else:
                                #    item[col] = i[idx]
                            if is_parent:
                                if url not in data[table_name][index]:
                                    data[table_name][index][url] = item
                            else:
                                if index not in data[table_name]:
                                    data[table_name][index] = []
                                data[table_name][index].append(item)
                        else:
                            index_gtime[table_name][index] = gtime
                            for idx, col in enumerate(col_list):
                                if col in l_col_type_dic:
                                    if i[idx]:
                                        data[table_name][index][col] = set([i[idx]])
                                    else:
                                        data[table_name][index][col] = set()
                                else:
                                    data[table_name][index][col] = i[idx]
                    else:
                        data[table_name][index] = {}
                        index_gtime[table_name][index] = gtime
                        item = {}
                        for idx, col in enumerate(col_list):
                            if col in l_col_type_dic:
                                if i[idx]:
                                    item[col] = set([i[idx]])
                                else:
                                    item[col] = set([])
                            else:
                                item[col] = i[idx]
                        if is_parent:
                            data[table_name][index][url] = item
                        else:
                            data[table_name][index] = [item]

                    if url:
                        lurl_index = url
                    elif __url:
                        lurl_index = __url
                        #url_data[table_name][__url] = data[table_name][index]

                    #todo
                    if not is_parent:
                        url_data[table_name][lurl_index] = data[table_name][index]
                    else:
                        if lurl_index not in url_data[table_name]:
                            url_data[table_name][lurl_index] = type(data[table_name][index])()
                        if isinstance(data[table_name][index], list):
                            url_data[table_name][lurl_index].extend(data[table_name][index])
                        elif isinstance(data[table_name][index], dict):
                            url_data[table_name][lurl_index].update(data[table_name][index])

                    #url_data[table_name][lurl_index] = data[table_name][index]

                    if __url:
                        lurl_index = __url
                    elif url:
                        lurl_index = url
                        #if url not in __url_data[table_name]:
                        #    __url_data[table_name][url] = {}
                        #__url_data[table_name][url].update(data[table_name][index])

                    #todo
                    if not is_parent:
                        __url_data[table_name][lurl_index] = data[table_name][index]
                    else:
                        if lurl_index not in __url_data[table_name]:
                            __url_data[table_name][lurl_index] = type(data[table_name][index])()
                        if isinstance(data[table_name][index], list):
                            __url_data[table_name][lurl_index].extend(data[table_name][index])
                        elif isinstance(data[table_name][index], dict):
                            __url_data[table_name][lurl_index].update(data[table_name][index])

        return url_data, __url_data

    def gen_data(self, config_id):
        dedup = dedup_tables(config_id)
        data_list = []
        for url_data, __url_data in dedup.gen_raw_data():
            #table_name_list = self.info_parser.get_table_list(config_id)
            ##print "table_name_list:", table_name_list
            #begin = datetime.datetime.now()
            #url_data, __url_data = self.gen_raw_data(config_id, table_name_list)
            #end = datetime.datetime.now()
            #self.time_gen_raw_data = end - begin
            #print "time_gen_raw_data:", self.time_gen_raw_data
            #raw_input(">>")
            #v = {"url_data":url_data, "__url_data":__url_data}
            #with open("/home/kelly/temp/test.json", "w") as fd:
            #    ujson.dump(v, fd)
            #print "dump url_data done."
            #exit()
            begin = datetime.datetime.now()
            data = self.merge_raw_data(config_id, url_data, __url_data)
            end = datetime.datetime.now()
            self.time_merge_raw_data = end - begin
            data_list.extend(data)
        #with open("/home/kelly/temp/test.json", "w") as fd:
        #    ujson.dump(data, fd)
        #print "gen_data done."
        return data_list

    def gen_dbpath_list(self, config_id):
        dbpath_list = ["/home/kelly/temp/test.db"]
        #print "dbpath_list:", dbpath_list
        return dbpath_list
        dbpath_list = []
        today = datetime.datetime.now().strftime("%Y%m%d")
        dbpath = os.path.join(self.table_path, today, str(config_id) + today + ".db")
        if os.path.exists(dbpath):
            dbpath_list.append(dbpath)
        for i in range(self.day_back):
            dt = (datetime.datetime.now() - datetime.timedelta(days = i)).strftime("%Y%m%d")
            dbpath = os.path.join(self.table_path, dt, str(config_id) + "_" +dt + ".db")
            if os.path.exists(dbpath):
                dbpath_list.append(dbpath)
        #dbpath_list = ["/home/kelly/temp/test.db"]
        #print "dbpath_list:", dbpath_list
        return dbpath_list

    #@profile
    def insert_data(self, data, result, table_set, config_id, table, data_index = []):
        if isinstance(data, list):
            data_index.append(-1)
            for idx, d in enumerate(data):
                data_index[-1] = idx
                self.insert_data(d, result, table_set, config_id, table, data_index)
            data_index.pop()
        else:
            for k in data:
                if k in table_set:
                    if data[k]:
                        self.insert_data(data[k], result, table_set, config_id, k)
                    else:
                        #这个暂时决定放到verify的时候更新
                        #临时先加上顶层的空表
                        result[k] = []
                else:
                    data_path = self.info_parser.get_col_data_path_list(config_id, table, k)
                    dump_col_set = self.info_parser.get_dump_col_set(config_id)
                    data_path = data_path[0]
                    tmp = result
                    list_index = 0
                    for idx, tpath in enumerate(data_path):
                        path = tpath
                        if path is list:
                            ldindex = data_index[list_index] 
                            list_index += 1
                            if ldindex >= 0:
                                if len(tmp) <= ldindex:
                                    tmp.append({})
                                tmp = tmp[ldindex]
                            continue
                        else:
                            if path in tmp:
                                tmp = tmp[path]
                            else:
                                if idx > len(data_path) - 2:
                                    if k in dump_col_set[table]:
                                        tmp[path] = ujson.loads(data[k])
                                    else:
                                        tmp[path] = data[k]
                                elif idx <= len(data_path) - 2:
                                    if data_path[idx + 1] is list:
                                        if path not in tmp:
                                            tmp[path] = []
                                            tmp = tmp[path]
                                    else:
                                        tmp[path] = {}
                                        tmp = tmp[path]

    def merge_raw_data(self, config_id, data, __url_data):
        begin = datetime.datetime.now()
        table_parent_dic = self.info_parser.get_table_parent_dic(config_id)
        table_level_dic = self.info_parser.get_table_level_dic(config_id)
        #[(1, 'suggestedAnswer'), (1, 'author'), (1, 'acceptedAnswer'), (0, 'info')]
        sorted_table_list = sorted([(table_level_dic[table_name], table_name) for table_name in table_level_dic], reverse = True)
        table_set = set(table_level_dic.keys())
        main_table = sorted_table_list[-1][1]
        end = datetime.datetime.now()
        self.time_merge_prepare = end - begin
        #v = {"url_data:":data, "__url_data":__url_data}
        #with open("/home/kelly/temp/test.json", "w") as fd:
        #    ujson.dump(v, fd)
        #print "before combined dump."
        #exit()
        begin = datetime.datetime.now()
        for ttable in sorted_table_list:
            level = ttable[0]
            table = ttable[1]
            if table in table_parent_dic:
                parent_table = table_parent_dic[table]
            else:
                parent_table = ""
            if table == "author":
                debug = 1
            else:
                debug = 0
            __url_d = __url_data.pop(table)
            d = data.pop(table)
            table_data = __url_d
            if not table_data:
                if parent_table:
                    for url in data[parent_table]:
                        #with open("/home/kelly/temp/test.json", "w") as fd:
                        #    ujson.dump(data, fd)
                        data[parent_table][url][url][table] = []
            if parent_table:
                for __url in table_data:
                    if isinstance(table_data[__url], list):
                        append_data = table_data[__url]
                    elif isinstance(table_data[__url], dict):
                        append_data = []
                        #这里合并__url
                        for lurl in table_data[__url]:
                            append_data.append(table_data[__url][lurl])
                    data[parent_table][__url][__url][table] = append_data
            else:
                data[table] = {}
                for __url in table_data:
                    append_data = []
                    for lurl in table_data[__url]:
                        append_data.append(table_data[__url][lurl])
                    data[table][__url] = append_data[0]
        end = datetime.datetime.now()
        self.time_combin_table = end - begin
        #print "time_combin_table:", self.time_combin_table
        #raw_input(">>")
        #with open("/home/kelly/temp/test.json", "w") as fd:
        #    ujson.dump(data, fd)
        #print "dump combined table done."
        #exit()
        url_dic = data[main_table]
        #with open("/home/kelly/temp/test.json", "w") as fd:
        #    ujson.dump(url_dic, fd)
        #print "before url_dic dump."
        #exit()
        result_list = []
        begin = datetime.datetime.now()
        for url in url_dic:
            url_data = url_dic[url]
            result = {}
            #with open("/home/kelly/temp/test.db", "w") as fd:
            #    ujson.dump(url_data, fd)
            #print "url_data dump done."
            #exit()
            self.insert_data(url_data, result, table_set, config_id, main_table)
            result_list.append(result)
        end = datetime.datetime.now()
        self.time_insert_data = end - begin
        #print "time_insert_data:", self.time_insert_data
        #raw_input(">>")
        #with open("/home/kelly/temp/test.json", "w") as fd:
        #    ujson.dump(result_list, fd)
        #print "after url_dic result_list dump."
        #exit()
        return result_list

class table_info:
    def __init__(self):
        self.info_parser = raw_info_parser()
        test_conf = {
                "tables": [
                    {
                        "table_name": "info",
                        "content": [
                            [
                                "url",
                                "text"
                                ],
                            [
                                "title",
                                "text"
                                ],
                            [
                                "content",
                                "text"
                                ],
                            [
                                "ctime",
                                "interger"
                                ]
                            ],
                        "index": [
                            ["url_index", "url", 1, 1],
                            ["gtime_index", "gtime", 1, 1]
                            ]
                        },
                    {
                        "table_name": "comment",
                        "content": [
                            [
                                "url",
                                "text"
                                ],
                            [
                                "title",
                                "text"
                                ],
                            [
                                "content",
                                "text"
                                ],
                            [
                                "ctime",
                                "interger"
                                ]
                            ],
                        "index": [
                            ["url_index", "url", 1, 1],
                            ["gtime_index", "gtime", 1, 1]
                            ]
                        }
                    ],
                "storage":[
                    ["data.url", "info.url", "one2one"],
                    ["data.title", "info.title", "one2one"],
                    ["data.comment.user", "comment.user", "multi2multi"],
                    ["data.comment.content", "comment.content", "multi2multi"],
                ]
        }

    def escape_wordstr(self, sql_str):
        result_str = sql_str.replace("'", '"')
        result_str = result_str.replace('"', '\\"')
        return result_str

    def parse_word_item(self, term_str):
        #暂时先使用客户端传过来的原始字符串
        ret_str = self.escape_wordstr(term_str)
        return ret_str
        try:
            term = ujson.loads(term_str)
            return self.parser.parse(term)
        except:
            ret_str = self.escape_wordstr(term_str)
            return ret_str
    
    def parse_item(self, term, colname, sqlcolname):
        #print "colname:", colname
        item = term[colname]
        if isinstance(item, unicode):
            item = item.encode("utf-8")
        if colname == "words":
            item = self.parse_word_item(item)
            item_str = """info_vtb match '%s'""" % item
        elif colname == "negative":
            if item != "all":
                item_str = """negative %s""" % item
            else:
                item_str = ""
        elif colname == "source":
            #print "source:", item
            if isinstance(item, list):
                item = [u"'" + unicode(i) + u"'" for i in item]
            else:
                item = [u"'" + unicode(item) + u"'"]
            item_str = "%s in (%s)" % (sqlcolname[colname], (u",".join(item)).encode("utf-8"))
        elif colname == "site_domain":
            #print "site_domain:", item
            if isinstance(item, list):
                item = [u"'" + unicode(i) + u"'" for i in item]
            else:
                item = [u"'" + unicode(item) + u"'"]
            item_str = "%s in (%s)" % (sqlcolname[colname], (u','.join(item)).encode("utf8"))
        elif colname == "sitename":
            if isinstance(item, list):
                item = [u"'" + unicode(i) + u"'" for i in item]
            else:
                item = [u"'" + unicode(item) + u"'"]
            item_str = "%s in (%s)" % (sqlcolname[colname], (u','.join(item)).encode("utf8"))
        else:
            item_str = sqlcolname[colname] + "='" + item + "'"
        return item_str
    
    def gen_location_sql(self, location_list):
        city_list = []
        district_list = []
        term_list = []
        for location in location_list:
            if u'city' in location:
                city_list.append("'" + location[u'city'] + "'")
            if u'district' in location:
                district_list.append("'" + location[u'district'] + "'")

        if city_list:
            term_list.append("city in (%s)" % ",".join(city_list))
        if district_list:
            term_list.append("district in (%s)" % ",".join(district_list))
        sql_str = " and ".join(term_list)
        if isinstance(sql_str, unicode):
            sql_str = sql_str.encode("utf-8")
        return sql_str

    def parse_term(self, term):
        #return select_sql
        sqlcolname = {u"url":"url", u"sitename":"siteName", u"source":"info_flag", u"site_domain":"site_domain"}
        special_col = set([u"negative", u"words"])
        #sql_list = ["select * from info_vtb where 1=1"]
        sql_list = ["select rowid from info_vtb where 1=1"]
        if u"location" in term:
            l = term['location']
            location_term = self.gen_location_sql(l)
            if location_term:
                sql_list = ["select * from info_vtb where 1=1 and url in (select url from location where %s)" % location_term]
        elif u"subject_id" in term:
            if isinstance(term['subject_id'], list):
                subject_id_str_list = [str(subject_id) for subject_id in term['subject_id']]
            else:
                subject_id_str_list = [str(term['subject_id'])]
            sql_list = ["select * from info_vtb where 1=1 and url in (select url from subject where subject_id in (%s))" % ",".join(subject_id_str_list)]
        idx = 1
        for key in term:
            if key in sqlcolname or key in special_col:
                sql = self.parse_item(term, key, sqlcolname)
                if sql:
                    sql_list.append(sql)
            else:
                #print "key:", key, "not in sqlcolname, spcial_col"
                pass
        sql_str = " and ".join(sql_list)

        if isinstance(sql_str, unicode):
            sql_str = sql_str.encode("utf-8")
    
        media_col = ""
        if "media" in term:
            media = term["media"]
            if media == "pic":
                media_col = "pic_urls"
            elif media == "video":
                media_col = "video_urls"
        media_str = "and pic_urls is not null and pic_urls != '' "
        sql_str = "select * from info where rowid in (%s) where 1=1 " % sql_str
        return sql_str
    
    def gen_dbpath_query_sql(self, terms):
        today = datetime.datetime.now().strftime("%Y%m%d")
        ftype = ["config_id"]
        #使用整个path list
        root_path = config.ftype_path_dic["config_id"]
        reg_str = ""
        today_sql_term = ""
        min_date = terms.get('min_date', "")
        max_date = terms.get('max_date', "")
        if "config_id" in terms:
            #print "country in terms:", "country" in terms
            if "country" in terms:
                if terms["country"] == "all":
                    #print "appendding oversea_config_id"
                    ftype.append("oversea_config_id")
                elif terms["country"] == "oversea":
                    #print "replace config_id to oversea_config_id"
                    ftype = ["oversea_config_id"]
            if isinstance(terms['config_id'], list):
                if terms["config_id"] and terms["config_id"][0] == 0 and (not max_date or max_date >= today):
                    today_sql_term = "or date = '%s'" % today
                config_id_list = ["^" + str(config_id) + "_" for config_id in terms['config_id']]
            else:
                config_id_list = ["^" + str(terms['config_id']) + "_"]
            reg_str = "%s.*" % "|".join(config_id_list)

        if 'uid' in terms:
            ftype = ["data"]
            root_path = config.ftype_path_dic["data"]
            if isinstance(terms['uid'], list):
                uid_list = ["^" + str(uid) + "_" for uid in terms['uid']]
            else:
                uid_list = ["^" + str(terms['uid']) + "_"]
            reg_str = "%s.*" % "|".join(uid_list)
        if 'location' in terms:
            ftype = ["location"]
            root_path = config.ftype_path_dic["location"]
            location_list = ["^" + location['province'].encode("utf-8") + "_" for location in terms['location']]
            reg_str = "%s.*" % "|".join(location_list)
            #确保location与uid字段不共存
            terms.pop('uid', 0)
        #print "ftype:", ftype
        sql_str = \
                "select type, date, fname, root_path_idx from dbpath where %s %s and (1=1 %s %s %s %s) order by date desc" \
                % (" ( " + " or ".join(["type = '" + (t.encode("utf-8") if isinstance(t, unicode) else t) + "'" for t in ftype ]) + " ) ", "and worker_id = '%s'" % str(config.self_ip) , "and fname rlike '%s'" % reg_str if reg_str else "", "and date >= '%s'" % min_date.encode("utf-8") if min_date else "", "and date <= '%s' " % max_date.encode("utf-8") if max_date else "", today_sql_term)
        return sql_str, root_path
    
    def gen_dbpath_list(self, terms):
        db = MySQLdb.connect(host=config.mysql_host, port=config.mysql_port, user=config.mysql_user, passwd=config.mysql_password, db=config.mysql_dbname, charset='utf8')
        cur = db.cursor()
        sql_str, root_dir = self.gen_dbpath_query_sql(terms)
        #print "!!!!!!!!!!!sql_str:", sql_str
        cur.execute(sql_str)
        l = cur.fetchall()
        today_root_dir = config.today_root_dir
        today = datetime.datetime.now().strftime("%Y%m%d")
        dbpath_list = [os.path.join(root_dir[i[3]], i[0], i[1], i[2]) for i in l if i[1] != today]
        today_dbpath_list = [os.path.join(today_root_dir, i[0], i[1], i[2]) for i in l if i[1] == today ]
        return dbpath_list, today_dbpath_list
    
    def get_table_info(self, terms):
        if "config_id" not in terms and "location" not in terms and "uid" not in "terms":
            terms["config_id"] = [0]
        sql = self.parse_term(terms)
        dbpath_list = self.gen_dbpath_list(terms)
        return (sql, dbpath_list)

    def get_create_vtb_sql(self, config_id):
        #[("table_name", create_sql), ("table_name", create_sql)]
        pass

    def get_columns(self, config_id):
        #[("table_name", [cols]), ("table_name", [cols]), ...]
        pass

try:
    from jsonschema import Draft4Validator
except:
    pass

def test(schema_file, data_file):
    if not data_file:
        print u"数据文件名为空"
        return 
    with open(schema_file, "r") as f:
        schema_json = json.load(f)
    schema_obj = Draft4Validator(schema_json)

    with open(data_file) as fd:
        data_list = json.load(fd)
    return schema_obj.is_valid(data_list[0])

    with open(data_file) as fd:
        data_list = json.load(fd)
        for idx, data in enumerate(data_list):
            #data[u"attributes"] = []
            for error in schema_obj.iter_errors(data):
                print "error message:", error.message
                print "idx:", idx
                exit()
    return 

def test_data_check(schema_json, data):
    schema_obj = Draft4Validator(schema_json)
    return schema_obj.is_valid(data)

if __name__ == "__main__":
    config_id = "-2"
    if 0:
        s = {"author":{"test":[]}}
        dbpath = ["author", "test"]
        tmp = s
        for path in dbpath:
            tmp = tmp[path]
        tmp.append(1)
        print s
        exit()

    if 0:
        #os.system("cp /mnt/DATA/user_def/20150910/%s_20150910.db /home/kelly/temp/test.db" % config_id)
        r = restore_info()
        result_list = r.gen_data(config_id)
        with open("/home/kelly/temp/test.json", "w") as fd:
            json.dump(ujson.loads(ujson.dumps(result_list)), fd, indent = True)
        print "final dump one done."
        #print "time_info_parser_init:", r.time_info_parser_init
        #print "time_gen_raw_data:", r.time_gen_raw_data
        #print "time_merge_raw_data:", r.time_merge_raw_data
        #print "time_merge_prepare:", r.time_merge_prepare
        #print "time_combin_table:", r.time_combin_table
        #print "time_insert_data:", r.time_insert_data
        exit()

    if 0:
        #schema test
        schema_path_dic = {
                "-2":"/home/kelly/文档/抓取格式/Question/scrape/scrape-schema-v1.json",
                "-3":"/home/kelly/文档/抓取格式/MusicRecording/scrape/scrape-schema-v1.json",
                "-4":"/home/kelly/文档/抓取格式/MusicAlbum/scrape/scrape-schema-v1.json",
                "-5":"/home/kelly/文档/抓取格式/Movie/scrape/scrape-schema-v1.json",
                "-6":"/home/kelly/文档/抓取格式/TVSeries/scrape/scrape-schema-v1.json",
                "-7":"/home/kelly/文档/抓取格式/TVShow/scrape/scrape-schema-v1.json",
                "-8":"/home/kelly/文档/抓取格式/Book/scrape/scrape-schema-v1.json",
                "-9":"/home/kelly/文档/抓取格式/Recipe/scrape/scrape-schema-v1.json",
                }
        schema_path = schema_path_dic[config_id]
        data_file = "/home/kelly/temp/test.json"
        with open(schema_path) as fd:
            schema_json = ujson.load(fd)
        data = {}
        print test_data_check(schema_json, data)
        exit()

    if 0:
        #schema test
        schema_path_dic = {
                "-2":"/home/kelly/文档/抓取格式/Question/scrape/scrape-schema-v1.json",
                "-3":"/home/kelly/文档/抓取格式/MusicRecording/scrape/scrape-schema-v1.json",
                "-4":"/home/kelly/文档/抓取格式/MusicAlbum/scrape/scrape-schema-v1.json",
                "-5":"/home/kelly/文档/抓取格式/Movie/scrape/scrape-schema-v1.json",
                "-6":"/home/kelly/文档/抓取格式/TVSeries/scrape/scrape-schema-v1.json",
                "-7":"/home/kelly/文档/抓取格式/TVShow/scrape/scrape-schema-v1.json",
                "-8":"/home/kelly/文档/抓取格式/Book/scrape/scrape-schema-v1.json",
                "-9":"/home/kelly/文档/抓取格式/Recipe/scrape/scrape-schema-v1.json",
                }
        schema_path = schema_path_dic[config_id]
        data_file = "/home/kelly/temp/test.json"
        test(schema_path, data_file)
        exit()

    temp_db_path = "/home/kelly/temp/test.db"
    os.system("rm -rf %s" % temp_db_path)
    storage = storage_info()
    #rhd = redis.from_url("redis://192.168.120.214/2")
    #s = rhd.lindex("custom_data", -1)
    #data = ujson.loads(s)
    #with open("/home/kelly/temp/test.json", "w") as fd:
    #    ujson.dump(data, fd)
    #exit()
    #config_id = str(data[u"config_id"])
    data_path = "/home/kelly/temp/custom_data/%s.dat" % config_id
    with open(data_path) as fd:
        data = ujson.load(fd)
    data[u"__gtime"] = unicode(datetime.datetime.now())
    #for k in data:
    #    print k, data[k]
    #exit()
    sqls = storage.get_create_sql(config_id)
    #print sqls
    conn = sqlite3.connect(temp_db_path)
    conn.text_factory = str
    cur = conn.cursor()
    for sql in sqls:
        cur.execute(sql)
    conn.commit()
    result_list = storage.get_insert_sqls(data)
    #print "result_list ret:", len(result_list)
    #print ujson.dumps(result_list)
    #with open("/tmp/test.json", "w") as fd:
    #    ujson.dump(result_list, fd)
    #print "before result_list print"
    #print result_list[0][0]
    #print result_list[0][1]
    for result in result_list:
        #print result[0]
        #print result[1]
        #break
        try:
            #if result[0].find("track_author") > 0:
            #    print result[0]
            #    print result[1]
            #    exit()
            cur.executemany(result[0], result[1])
        except Exception, reason:
            print "~~~~~~~~~~~", result[0]
            print "~~~~~~~~~~~", result[1]
            print "got executemany error", reason
    conn.commit()
    print "executemany done."
    exit()
    os.system("rm -rf test.db")
    conn = sqlite3.connect("test.db")
    conn.text_factory = str
    cur = conn.cursor()
    for sql in sqls:
        cur.execute(sql)
    while 1:
        pv = rhd.lpop("zhiku_data")
        data = pickle.loads(pv)
        data = {
                "config_id":"14867",
                "ctime":1434470400,
                "title" : "test title",
                "content" : ["test content", "test content2"],
                "url" : "test url",
                "siteName" : "test siteName",
                "source" : "test source",
                #"info_flag" : "01",
                "info_flag" : "10",
                "keywords" : "test keywords",
                "gtime" : 1437628652
                }
        result_list = s.get_insert_sqls(data)
        for result in result_list:
            print "result[0]:", result[0]
            print "result[1]:", result[1]
            cur.executemany(result[0], result[1])
        conn.commit()
        cur.close()
        conn.close()
        break
    exit()
    terms = ujson.loads(ujson.dumps({
        #"config_id":["67_41", "test", "1868"],
        "location":[{"province":"北京", "district":"海淀区", "city":"北京"}, {"province":"河南省", "district":"项城市", "city":"周口市"}],
        "negative":1,
        "words":"河北 AND 腐败"}))
    t = table_info()
    print t.get_table_info(terms)[0]
