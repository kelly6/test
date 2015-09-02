#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import redis
import ujson
import config
import datetime
import MySQLdb
import traceback
import threading
import logging.config
import sqlite3
import pickle
try:
    import sqlite_fulltext as wp
except:
    pass
from user_def_data import table_data
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
        return raw_info_parser.rhd.hexists(raw_info_parser.table_info_hkey, config_id)

    def get_config_id(self, config_id):
        v = raw_info_parser.rhd.hget(config.table_info_hkey, config_id)
        return ujson.loads(v)
    
    def get_create_sql(self, config_id):
        self.refresh_table_info(config_id)
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

    #def gen_col_list(self, config_id, table_name):
    #    if isinstance(table_name, str):
    #        table_name = table_name.decode("utf8")
    #    v = self.get_config_id(config_id)
    #    for table in v[u"tables"]:
    #        if table[u"table_name"] == table_name:
    #            break
    #    else:
    #        return []
    #    col_list = []
    #    for col_obj in table[u"content"]:
    #        col_list.append(col_obj[0].encode("utf8"))
    #    return col_list

    def get_col_list(self, config_id, table_name):
        self.refresh_table_info(config_id)
        #print "config_id check:", config_id in self.table_info
        #print "table_info check:", "table_info" in self.table_info[config_id]
        #print "table_name:", table_name, type(table_name)
        #print "table_name dic:", self.table_info[config_id]["table_info"]
        #print "table_name check:", table_name in self.table_info[config_id]["table_info"]
        return self.table_info.get(config_id, {}).get("table_info", {}).get("col_list").get(table_name, [])

    def get_table_list(self, config_id):
        self.refresh_table_info(config_id)
        return self.table_info.get(config_id, {}).get("table_name_list", [])

    def get_col_data_path_list(self, config_id, table_name, col):
        #todo
        #这里需要更新, 暂不考虑更新的情况
        data_path_list = self.table_info[config_id]["data_path_list"]
        return (data_path_list.get(table_name, {}).get(col, []), self.get_table_type(config_id, table_name))

    def get_data_path_deepth(self, config_id, table_name):
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

    def get_table_type(self, config_id, table_name):
        return self.table_info[config_id]["table_type_dic"][table_name]

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
        data_path_list, table_type_dic, list_deepth_dic = self.gen_data_path_list(config_id, table_v)
        self.table_info[config_id]["data_path_list"] = data_path_list
        self.table_info[config_id]["table_type_dic"] = table_type_dic
        self.table_info[config_id]["list_deepth_dic"] = list_deepth_dic
        table_info = {}
        table_info["col_list"] = {}
        for table_name in data_path_list:
            table_info["col_list"][table_name] = data_path_list[table_name].keys()
        self.table_info[config_id]["table_info"] = table_info
        self.table_info[config_id]["table_name_list"] = data_path_list.keys()
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
    #    #print "td:", td
    #    create_sql = []
    #    for table in td["tables"]:
    #        name = table["table_name"]
    #        content_list = table["content"]
    #        index_list = table["index"]
    #        create_sql.extend(self.get_create_sql_by_items(name, content_list, index_list))
    #    return create_sql

    #def get_create_sql_by_items(self, name, content_list, index_list):
    #    sqls = []
    #    cols = []
    #    for content in content_list:
    #        s = " %s %s %s " % (content[0], content[1], content[3])
    #        if content[4]:
    #            s += " default %s " % content[4]
    #        cols.append(s)
    #    create_sql = """ create table %s (%s); """ % (name, ",\n".join(cols))
    #    sqls.append(create_sql)
    #    for index in index_list:
    #        if not index[0]:
    #            continue
    #        sqls.append("""create %s INDEX %s_%s_index on %s (%s)""" % (" UNIQUE " if index[2] else "", name, index[0], name, index[1]))
    #    return sqls

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
            #print "v:", v
            #print "path_list:", path_list
            #print "idx:", idx
            ##debug
            #with open("/home/kelly/temp/temp.json", "w") as fd:
            #    ujson.dump(v, fd)
            ret = v[path_list[idx]]
            if isinstance(ret, list) and not ret:
                ret = None
            #print "ret:", v[path_list[idx]]
            return (ret, 1 if isinstance(ret, list) else 0)
        if path_list[idx] is list:
            #print "v:", v
            #print "path_list:", path_list
            #print "idx:", idx
            #print "path_list[idx]:", path_list[idx]
            #print "path_list[idx + 1]:", path_list[idx + 1]
            ret = []
            #with open("/home/kelly/temp/temp.json", "w") as fd:
            #    ujson.dump(v, fd)
            list_flag = 0
            for item in v:
                #print "item:", item
                #print "path_list:", path_list
                #print "idx:", idx
                #print "path_list[idx]:", path_list[idx]
                #print "path_list[idx + 1]:", path_list[idx + 1]
                #with open("/home/kelly/temp/temp.json", "w") as fd:
                #    ujson.dump(item, fd)
                v, list_flag_tmp = self.__get_col_data(item, path_list, idx + 1)
                list_flag |= list_flag_tmp
                ret.append(v)
            return (ret, list_flag)
        else:
            print "v:", v
            print "path_list:", path_list
            print "idx:", idx
            print "path_list[idx]:", path_list[idx]
            return self.__get_col_data(v[path_list[idx]], path_list, idx + 1)

    def get_col_list_data(self, table_name, data):
        #return (col_list, transed_data_list, list_flag, list_len)
        col_list = self.get_col_list(table_name)
        insert_data = []
        list_flag = 0
        print " in get col list data table_name:", table_name
        if table_name == "info":
            debug = 1
        else:
            debug = 0
        debug = 0
        debug_v = {"col_list":col_list, "col_path_list":{}, "data":data}
        for col in col_list:
            path_list, map_type = self.info_parser.get_col_data_path_list(self.config_id, table_name, col)
            debug_v["col_path_list"][col] = path_list
            v, list_flag_tmp = self.__get_col_data(data, path_list, 0)
            if debug:
                print "in col table_name:", table_name
                print "col:", col, v
            if isinstance(v, list) and not v:
                return ([], [], 0)
            list_flag |= list_flag_tmp
            insert_data.append(v)
            #list_len = 0
            #if isinstance(v, list):
            #    list_len = len(v)
        deepth = self.info_parser.get_data_path_deepth(self.config_id, table_name)
        if debug:
            print "%s raw_insert_data:" % table_name, insert_data
            print "deepth:", deepth
        if deepth > 1:
            insert_data = zip(*insert_data)
        for i in range(deepth - 2):
            ret_list = []
            for i in insert_data:
                new_list = zip(*insert_data)
                ret_list.append(new_list)
            insert_data = ret_list

        list_len = 0
        for item in insert_data:
            if deepth > 1:
                for i in item:
                    if debug:
                        print "i:", i
                    if isinstance(i, list):
                        list_len = len(i)
                        print "in deep list_len:", list_len
            else:
                if isinstance(item, list):
                    #if debug:
                    #    print "item:", item
                    list_len = len(item)
        if debug:
            pass
            #print "insert_data:", insert_data
            #print "list_len:", list_len
        if deepth > 1:
            tmp_list = []
            for item in insert_data:
                #print "zip :", zip(*[d if isinstance(d, list) else [d] * list_len for d in item])
                tmp_list.extend(zip(*[d if isinstance(d, list) else [d] * list_len for d in item]))
            insert_data = tmp_list
        elif deepth == 1:
            insert_data = zip(*[d if isinstance(d, list) else [d] * list_len for d in insert_data])
        else:
            insert_data = [insert_data]
        #if debug:
        #    print "insert_data:", insert_data
        #    print "deepth:", deepth
        #    exit()
        #print "ret:", insert_data, list_flag
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
            print "in list_flag before dlist:", dlist
            ret_list = []
            for item in dlist:
                print "before item:", item
                tmp = list(product(*[d if isinstance(d, list) else [d] for d in item]))
                print "after tmp:", tmp
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
            #print "table_name:", table_name
            if table_name == "info":
                debug = 1
            else:
                debug = 0
            #debug = 0
            col_list, data_list, list_flag = self.table_gen.get_col_list_data(table_name, data)
            if not data_list:
                continue
            if debug:
                print "raw data_list:", ujson.dumps(data_list)
                print "list_flag:", list_flag
                print "table_name:", table_name
                with open("/home/kelly/temp/temp.pickle", "w") as fd:
                    pickle.dump(data_list, fd)
            data_list = self.gen_result_list(data_list, list_flag)
            #print "data_list:%d, table_name:%s, list_flag:%d, list_len:%d" % (len(data_list), table_name, list_flag, list_len)
            if debug:
                print "after gen data_list:", ujson.dumps(data_list)
                with open("/home/kelly/temp/temp.json", "w") as fd:
                    ujson.dump(data_list, fd)
                print "table_name:", table_name
                #exit()
                #print "list_len:%d, list_flag:%d" % (list_len, list_flag)
            col_str = ",".join(col_list)
            #print "data_list:", data_list
            sql = "insert into %s (%s) values (%s)" % (table_name, col_str, ",".join(["?"] * len(data_list[0])))
            result_list.append((sql, data_list))
            continue

            #if table_name != "info":
            #    continue
            #print "table_name:", table_name
            col_list = self.table_gen.get_col_list(table_name)
            table_type = self.table_gen.get_table_type(table_name)
            dlist = []
            list_flag = 0
            #print "col_list:", col_list
            for col_idx, col in enumerate(col_list):
                d, dtype = self.table_gen.get_col_data(config_id, table_name, col, data)
                if debug:
                    #print "col:", col
                    #print "get_col_data config_id:", config_id
                    #print "get_col_data table_name:", table_name
                    #print "get_col_data col:", col
                    #print "get_col_data d:", d
                    #print "get_col_data dtype:", dtype
                    #print "get_col_data: dlist:", dlist
                    #print "get_col_data bool:", isinstance(d, list), dtype is list
                    pass
                if dtype is list:
                    dlist.extend(d)
                else:
                    if isinstance(d, list):
                        if not d:
                            d = None
                        list_flag = 1
                    dlist.append(d)
                    if debug:
                        #print "d:", d
                        #print "before dlist:", ujson.dumps(dlist)
                        #print "list_flag:", list_flag
                        #raw_input(">>")
                        pass
            #if debug:
            #    print "after dlist:", dlist
            #print "list_flag:", list_flag
            if table_type == self.table_gen.multi2multi:
                col_str = ",".join(col_list[0])
            else:
                dlist = self.gen_result_list(dlist, list_flag)
                col_str = ",".join(col_list)
            sql = "insert into %s (%s) values (%s)" % (table_name, col_str, ",".join(["?"] * len(dlist[0])))
            result_list.append((sql, dlist))
        return result_list

    def is_special_config_id(self, config_id):
        return 1

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

if __name__ == "__main__":
    #with open("/home/kelly/temp/zhihu_datatype.json") as fd:
    #    s = ujson.load(fd)
    #rhd = redis.from_url("redis://192.168.120.213/5")
    #rhd = redis.from_url("redis://192.168.2.97/0")
    #rhd.hset("table_info", "-2", ujson.dumps(s))
    #exit()

    temp_db_path = "/home/kelly/temp/test.db"
    os.system("rm -rf %s" % temp_db_path)
    storage = storage_info()
    config_id = "-9"
    sqls = storage.get_create_sql(config_id)
    print sqls
    conn = sqlite3.connect(temp_db_path)
    conn.text_factory = str
    cur = conn.cursor()
    for sql in sqls:
        cur.execute(sql)
    conn.commit()
    #rhd = redis.from_url("redis://192.168.120.214/2")
    #s = rhd.lindex("custom_data", -1)
    #data = ujson.loads(s)
    data_path = "/home/kelly/temp/custom_data/%s.dat" % config_id
    with open(data_path) as fd:
        data = ujson.load(fd)
    #for k in data:
    #    print k, data[k]
    #exit()
    result_list = storage.get_insert_sqls(data)
    print "result_list ret:", len(result_list)
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
            cur.executemany(result[0], result[1])
        except:
            print "~~~~~~~~~~~", result[0]
            print "~~~~~~~~~~~", result[1]
            print "got executemany error"
    conn.commit()
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
