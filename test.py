#!/usr/bin/python
# -*- coding: UTF-8 -*-
import warning
import time
import re
import datetime
from negative import *
import os
import shutil
import pickle
import random
from bitarray import bitarray
from fast_bitarray import *
import fast_search
from classify import Classify
import ujson
import json
from xlwt import *
import jieba
import exit_notify

def read_file(filename):
    fd = file(filename, "r")
    title = fd.readline()
    title = title.strip()
    content = fd.read()
    fd.close()
    return (title, content)

def get_content(fpath):
    with open(fpath) as fd:
        return fd.read()

def gen_stopwords():
    stopword_path = "/home/kelly/code/git/cpp_bayes/stopwords.txt"
    stopwords_set = set()
    with open(stopword_path) as fd:
        for l in fd:
            stopwords_set.add(l.strip().decode("utf-8"))
    stopwords_set.add(u" ")
    stopwords_set.add(u"  ")
    stopwords_set.add(u"\n")
    stopwords_set.add(u"\r\n")
    stopwords_set.add(u"\r")
    stopwords_set.add(u"\t")
    return stopwords_set

stopwords_set = gen_stopwords()
neg_pos_fs_hd = fast_search.load("/home/kelly/temp/neg_pos_word.txt")

def gen_content_dic_negative(fpath):
    title, content = read_file(fpath)
    info_flag = "01"
    if fpath.find("04") > 0:
        info_flag = "0401"
    json_data = {'info_flag':info_flag, 'title':title, 'content':content}

    negative.judge(json_data)

    word_list = []
    word_list += json_data['negative']['words']['1']
    word_list += json_data['negative']['words']['2']
    word_list += json_data['negative']['words']['3']
    #word_list += json_data['negative']['words']['4']
    word_list += json_data['negative']['words']['-1']
    word_list += json_data['negative']['words']['-2']
    word_list += json_data['negative']['words']['-3']
    #word_list += json_data['negative']['words']['-4']
    ret_dic = {}
    for i in word_list:
        word = i.decode("utf-8")
        ret_dic[word] = ret_dic.get(word, 0)
        ret_dic[word] = 1
    return ret_dic

def gen_content_dic_jieba(fpath):
    global stopwords_set
    #stopwords_set = set()
    
    l = list(jieba.cut(get_content(fpath)))
    ret_dic = {}
    for i in l:
        if i in stopwords_set:
            continue
        ret_dic[i] = ret_dic.get(i, 0)
        ret_dic[i] += 1
    return ret_dic

def gen_content_dic_posseg(res0):
    ret_dic = {}
    #排除的类型
    type_set = set([u'v'])
    for w in res0[1]:
        if w[1] not in type_set:
            continue
        ret_dic[w[0]] = ret_dic.get(w[0], 0)
        ret_dic[w[0]] = 1
    return ret_dic

def gen_content_dic_fast_search(fpath):
    s = get_content(fpath)
    l = fast_search.jump_findall(neg_pos_fs_hd, s)
    ret_dic = {}
    for i in l:
        word = i[0].decode("utf-8")
        ret_dic[word] = ret_dic.get(word, 0)
        ret_dic[word] += 1
    return ret_dic

#返回值格式{"word":w_value}
def gen_content_dic(fpath):
    #return gen_content_dic_posseg(fpath)
    #return gen_content_dic_negative(fpath)
    #return gen_content_dic_jieba(fpath)
    return gen_content_dic_fast_search(fpath)

exit_notify.init()

if 0:
    negative.init()
    root_path = "/home/kelly/idf_article"
    root_path = "/dev/shm/idf_article"
    result_dic = {}
    counter = 0
    fpath_list = []
    for fname in os.listdir(root_path):
        fpath = os.path.join(root_path, fname)
        fpath_list.append(fpath)
        print counter
        counter += 1
    counter = 0
    for fpath in fpath_list:
        print counter
        counter += 1
        title, content = read_file(fpath)
        json_data = {'info_flag':"01", 'title':title, 'content':content}
        negative.judge(json_data)
        result = json_data['negative']['result']
        result_dic[result] = result_dic.get(result, 0)
        result_dic[result] += 1
    print result_dic

if 0:
    #jieba词性标注测试
    import jieba.posseg as pseg
    jieba.initialize()

    def cut_res_to_ps_dic(res, new_res_list):
        title, content = read_file(res[0])
        stopwords_set = gen_stopwords()
        words = list(pseg.cut(title + content))
        word_list = []
        for w in words:
            if w.word in stopwords_set:
                continue
            word_list.append((w.word, w.flag))
        res[0] = (res[0], word_list)
        new_res_list.append(res)

    with open("/home/kelly/temp/neg_res_list.json") as fd:
        neg_res_list = json.load(fd)
    with open("/home/kelly/temp/pos_res_list.json") as fd:
        pos_res_list = json.load(fd)
    with open("/home/kelly/temp/neu_res_list.json") as fd:
        neu_res_list = json.load(fd)
    with open("/home/kelly/temp/notneg_res_list.json") as fd:
        notneg_res_list = json.load(fd)

    ps_neg_res_list = [[], []]
    for res in neg_res_list[0]:
        cut_res_to_ps_dic(res, ps_neg_res_list[0])
    for res in neg_res_list[1]:
        cut_res_to_ps_dic(res, ps_neg_res_list[1])

    ps_neu_res_list = [[], []]
    for res in neu_res_list[0]:
        cut_res_to_ps_dic(res, ps_neu_res_list[0])
    for res in neu_res_list[1]:
        cut_res_to_ps_dic(res, ps_neu_res_list[1])

    ps_pos_res_list = [[], []]
    for res in pos_res_list[0]:
        cut_res_to_ps_dic(res, ps_pos_res_list[0])
    for res in pos_res_list[1]:
        cut_res_to_ps_dic(res, ps_pos_res_list[1])
    
    with open("/home/kelly/temp/ps_neg_res_list.json", "w") as fd:
        ujson.dump(ps_neg_res_list, fd)
    with open("/home/kelly/temp/ps_neu_res_list.json", "w") as fd:
        ujson.dump(ps_neu_res_list, fd)
    with open("/home/kelly/temp/ps_pos_res_list.json", "w") as fd:
        ujson.dump(ps_pos_res_list, fd)

if 0:
    import heapq
    with open("/home/kelly/temp/ps_neg_res_list.json") as fd:
        ps_neg_res_list = ujson.load(fd)
    #with open("/home/kelly/temp/ps_neu_res_list.json") as fd:
    #    ps_neu_res_list = ujson.load(fd)
    #with open("/home/kelly/temp/ps_pos_res_list.json") as fd:
    #    ps_pos_res_list = ujson.load(fd)
    #ps_neg_res_list[0]: [res0, res1]
    #ps_neg_res_list[0][0]: [[fname, [(word, word_flag)]], 0, 1, 2...]
    #ps_neg_res_list[0][0][0]: [fname, [(word, word_flag)]]
    type_dic = {}
    for res in ps_neg_res_list[0]:
        for w in res[0][1]:
            type_dic[w[1]] = type_dic.get(w[1], 0)
            type_dic[w[1]] += 1
    print type_dic
    type_list = []
    for t in type_dic:
        type_list.append((type_dic[t], t))
    print heapq.nlargest(len(type_list), type_list)

if 0:
    res_list_path = "/home/kelly/result/no_jump_no_repeat_result_list.json"
    pos_res_list_path = "/home/kelly/temp/pos_res_list.json"
    neg_res_list_path = "/home/kelly/temp/neg_res_list.json"
    neu_res_list_path = "/home/kelly/temp/neu_res_list.json"
    notneg_res_list_path = "/home/kelly/temp/notneg_res_list.json"

    pos_res_list = [[], []]
    neg_res_list = [[], []]
    neu_res_list = [[], []]
    notneg_res_list = [[], []]

    pos_idx = 0
    neg_idx = 0
    neu_idx = 0
    notneg_idx = 0

    with open(res_list_path) as fd:
        res_list = ujson.load(fd)
    for res in res_list:
        rdm = random.random()
        if rdm > 0.33:
            neg_idx = 0
            neu_idx = 0
            pos_idx = 0
            notneg_idx = 0
        else:
            neg_idx = 1
            neu_idx = 1
            pos_idx = 1
            notneg_idx = 1
        if res[0].find("04") > 0:
            continue
        if res[5] < 0:
            neg_res_list[neg_idx].append(res)
            #neg_idx = (neg_idx + 1) % 2
        else:
            notneg_res_list[notneg_idx].append(res)
            #notneg_idx = (notneg_idx + 1) % 2
            if res[5] > 0:
                pos_res_list[pos_idx].append(res)
                #pos_idx = (pos_idx + 1) % 2
            else:
                neu_res_list[neu_idx].append(res)
                #neu_idx = (neu_idx + 1) % 2
    
    with open(pos_res_list_path, "w") as fd:
        ujson.dump(pos_res_list, fd)
    with open(neg_res_list_path, "w") as fd:
        ujson.dump(neg_res_list, fd)
    with open(neu_res_list_path, "w") as fd:
        ujson.dump(neu_res_list, fd)
    with open(notneg_res_list_path, "w") as fd:
        ujson.dump(notneg_res_list, fd)

    print len(neg_res_list[0]), len(neg_res_list[1])
    print len(neu_res_list[0]), len(neu_res_list[1])
    print len(pos_res_list[0]), len(pos_res_list[1])
    exit()
    p1_pos_path = "/home/kelly/temp/p1_pos.txt"
    p2_pos_path = "/home/kelly/temp/p2_pos.txt"
    p1_neg_path = "/home/kelly/temp/p1_neg.txt"
    p2_neg_path = "/home/kelly/temp/p2_neg.txt"
    p1_neu_path = "/home/kelly/temp/p1_neu.txt"
    p2_neu_path = "/home/kelly/temp/p2_neu.txt"
    p1_notneg_path = "/home/kelly/temp/p1_notneg.txt"
    p2_notneg_path = "/home/kelly/temp/p2_notneg.txt"

    os.remove(p1_pos_path)
    os.remove(p2_pos_path)
    os.remove(p1_neg_path)
    os.remove(p2_neg_path)
    os.remove(p1_neu_path)
    os.remove(p2_neu_path)
    os.remove(p1_notneg_path )
    os.remove(p2_notneg_path )

    for res in pos_res_list[0]:
        append_article(p1_pos_path, res[0])
    for res in pos_res_list[1]:
        append_article(p2_pos_path, res[0])

    for res in neg_res_list[0]:
        append_article(p1_neg_path, res[0])
    for res in neg_res_list[1]:
        append_article(p2_neg_path, res[0])

    for res in neu_res_list[0]:
        append_article(p1_neu_path, res[0])
    for res in neu_res_list[1]:
        append_article(p2_neu_path, res[0])

    for res in notneg_res_list[0]:
        append_article(p1_notneg_path, res[0])
    for res in notneg_res_list[1]:
        append_article(p2_notneg_path, res[0])

if 1:
    #生成nltk训练集
    jieba.initialize()
    negative.init()

    #stopwords_set = set()
    #with open("/home/kelly/code/git/cpp_bayes/stopwords.txt") as fd:
    #    for l in fd:
    #        stopwords_set.add(l.strip())

    with open("/home/kelly/temp/neg_res_list.json") as fd:
        neg_res_list = json.load(fd)
    with open("/home/kelly/temp/pos_res_list.json") as fd:
        pos_res_list = json.load(fd)
    with open("/home/kelly/temp/neu_res_list.json") as fd:
        neu_res_list = json.load(fd)
    with open("/home/kelly/temp/notneg_res_list.json") as fd:
        notneg_res_list = json.load(fd)
    result = []
    for res in neg_res_list[0]:
        #if res[0].find("04") > 0:
        #    continue
        item = gen_content_dic(res[0])
        result.append((item, 'neg'))
    
    for res in neu_res_list[0]:
        #if res[0].find("04") > 0:
        #    continue
        item = gen_content_dic(res[0])
        result.append((item, 'neu'))
    
    for res in pos_res_list[0]:
        #if res[0].find("04") > 0:
        #    continue
        item = gen_content_dic(res[0])
        result.append((item, 'pos'))
    
    #for res in notneg_res_list[0]:
    #    #if res[0].find("04") > 0:
    #    #    continue
    #    item = gen_content_dic(res[0])
    #    result.append((item, 'notneg'))

    print len(result)
    with open("/home/kelly/temp/nltk.train_list.json", "w") as fd:
        json.dump(result, fd)

if 1:
    #训练+保存classify结果
    def trans_nltk_classify_result(result):
        #return result.prob("notneg")
        l = [(result.prob("neg"), "neg"), (result.prob("neu"), "neu"), (result.prob("pos"), "pos")]
        m = max(l)
        if m[1] == "neg":
            return m[0]/3.0
        elif m[1] == "neu":
            return m[0] / 3.0 + 0.33
        elif m[1] == "pos":
            return m[0] / 3.0 + 0.66
        else:
            return 2.0

    negative.init()
    from nltk.classify.naivebayes import NaiveBayesClassifier

    with open("/home/kelly/temp/nltk.train_list.json") as fd:
        train_list = json.load(fd)
    classifier = NaiveBayesClassifier.train(train_list)
    with open("/home/kelly/temp/neg_res_list.json") as fd:
        neg_res_list = json.load(fd)
    with open("/home/kelly/temp/neu_res_list.json") as fd:
        neu_res_list = json.load(fd)
    with open("/home/kelly/temp/pos_res_list.json") as fd:
        pos_res_list = json.load(fd)
    with open("/home/kelly/temp/notneg_res_list.json") as fd:
        notneg_res_list = json.load(fd)
    #test_list = [item[0] for item in train_list]
    classifier.show_most_informative_features()
    neg_p2_res_compare_list = [res for res in neg_res_list[1]]
    neu_p2_res_compare_list = [res for res in neu_res_list[1]]
    pos_p2_res_compare_list = [res for res in pos_res_list[1]]
    notneg_p2_res_compare_list = [res for res in notneg_res_list[1]]
    test_list = []
    for res in neg_p2_res_compare_list:
        dic = gen_content_dic(res[0])
        test_list.append(dic)
    for res in neu_p2_res_compare_list:
        dic = gen_content_dic(res[0])
        test_list.append(dic)
    for res in pos_p2_res_compare_list:
        dic = gen_content_dic(res[0])
        test_list.append(dic)
    #for res in notneg_p2_res_compare_list:
    #    dic = gen_content_dic(res[0])
    #    test_list.append(dic)

    l = classifier.prob_classify_many(test_list)
    print len(l)
    idx = 0
    neg_len = len(neg_p2_res_compare_list)
    neu_len = len(neu_p2_res_compare_list)
    pos_len = len(pos_p2_res_compare_list)
    notneg_len = len(notneg_p2_res_compare_list)
    for i in range(len(l)):
        if i < neg_len:
            neg_p2_res_compare_list[i].append(trans_nltk_classify_result(l[i]))
        elif i < (neg_len + neu_len):
            neu_p2_res_compare_list[i - neg_len].append(trans_nltk_classify_result(l[i]))
        elif i < (neg_len + neu_len + pos_len):
            pos_p2_res_compare_list[i - (neg_len + neu_len)].append(trans_nltk_classify_result(l[i]))
        else:
            notneg_p2_res_compare_list[i - neg_len].append(trans_nltk_classify_result(l[i]))
    with open("/home/kelly/temp/neg_p2_res_compare_list.nltk.json", "w") as fd:
        json.dump(neg_p2_res_compare_list, fd)
    with open("/home/kelly/temp/neu_p2_res_compare_list.nltk.json", "w") as fd:
        json.dump(neu_p2_res_compare_list, fd)
    with open("/home/kelly/temp/pos_p2_res_compare_list.nltk.json", "w") as fd:
        json.dump(pos_p2_res_compare_list, fd)
    #with open("/home/kelly/temp/notneg_p2_res_compare_list.nltk.json", "w") as fd:
    #    json.dump(notneg_p2_res_compare_list, fd)

if 0:
    #使用posseg生成nltk训练集
    jieba.initialize()
    negative.init()

    stopwords_set = set()
    with open("/home/kelly/code/git/cpp_bayes/stopwords.txt") as fd:
        for l in fd:
            stopwords_set.add(l.strip())

    with open("/home/kelly/temp/ps_neg_res_list.json") as fd:
        ps_neg_res_list = ujson.load(fd)
    with open("/home/kelly/temp/ps_pos_res_list.json") as fd:
        ps_pos_res_list = ujson.load(fd)
    with open("/home/kelly/temp/ps_neu_res_list.json") as fd:
        ps_neu_res_list = ujson.load(fd)
    result = []
    for res in ps_neg_res_list[0]:
        item = gen_content_dic(res[0])
        result.append((item, 'neg'))
    
    for res in ps_neu_res_list[0]:
        item = gen_content_dic(res[0])
        result.append((item, 'neu'))
    
    for res in ps_pos_res_list[0]:
        item = gen_content_dic(res[0])
        result.append((item, 'pos'))
    
    print len(result)
    with open("/home/kelly/temp/nltk.train_list.json", "w") as fd:
        json.dump(result, fd)

if 0:
    #使用posseg训练+保存classify结果
    def trans_nltk_classify_result(result):
        #return result.prob("notneg")
        l = [(result.prob("neg"), "neg"), (result.prob("neu"), "neu"), (result.prob("pos"), "pos")]
        m = max(l)
        if m[1] == "neg":
            return m[0]/3.0
        elif m[1] == "neu":
            return m[0] / 3.0 + 0.33
        elif m[1] == "pos":
            return m[0] / 3.0 + 0.66
        else:
            return 2.0

    negative.init()
    from nltk.classify.naivebayes import NaiveBayesClassifier

    with open("/home/kelly/temp/nltk.train_list.json") as fd:
        train_list = json.load(fd)
    classifier = NaiveBayesClassifier.train(train_list)
    with open("/home/kelly/temp/ps_neg_res_list.json") as fd:
        ps_neg_res_list = json.load(fd)
    with open("/home/kelly/temp/ps_neu_res_list.json") as fd:
        ps_neu_res_list = json.load(fd)
    with open("/home/kelly/temp/ps_pos_res_list.json") as fd:
        ps_pos_res_list = json.load(fd)
    #test_list = [item[0] for item in train_list]
    classifier.show_most_informative_features()
    neg_p2_res_compare_list = [res for res in ps_neg_res_list[1]]
    neu_p2_res_compare_list = [res for res in ps_neu_res_list[1]]
    pos_p2_res_compare_list = [res for res in ps_pos_res_list[1]]
    test_list = []
    for res in neg_p2_res_compare_list:
        dic = gen_content_dic(res[0])
        test_list.append(dic)
    for res in neu_p2_res_compare_list:
        dic = gen_content_dic(res[0])
        test_list.append(dic)
    for res in pos_p2_res_compare_list:
        dic = gen_content_dic(res[0])
        test_list.append(dic)

    l = classifier.prob_classify_many(test_list)
    print len(l)
    idx = 0
    neg_len = len(neg_p2_res_compare_list)
    neu_len = len(neu_p2_res_compare_list)
    pos_len = len(pos_p2_res_compare_list)
    for i in range(len(l)):
        if i < neg_len:
            neg_p2_res_compare_list[i].append(trans_nltk_classify_result(l[i]))
        elif i < (neg_len + neu_len):
            neu_p2_res_compare_list[i - neg_len].append(trans_nltk_classify_result(l[i]))
        elif i < (neg_len + neu_len + pos_len):
            pos_p2_res_compare_list[i - (neg_len + neu_len)].append(trans_nltk_classify_result(l[i]))
    with open("/home/kelly/temp/neg_p2_res_compare_list.nltk.json", "w") as fd:
        json.dump(neg_p2_res_compare_list, fd)
    with open("/home/kelly/temp/neu_p2_res_compare_list.nltk.json", "w") as fd:
        json.dump(neu_p2_res_compare_list, fd)
    with open("/home/kelly/temp/pos_p2_res_compare_list.nltk.json", "w") as fd:
        json.dump(pos_p2_res_compare_list, fd)

if 0:
    #合并正负面词文件
    neg_path = "/home/kelly/temp/neg_word.txt"
    pos_path = "/home/kelly/temp/pos_word.txt"
    target_path = "/home/kelly/temp/neg_pos_word.txt"
    with open(target_path, "w") as target_fd:
        with open(neg_path) as fd:
            for l in fd:
                s = l.strip()
                target_fd.write(s + "\t" + "neg\n")
        with open(pos_path) as fd:
            for l in fd:
                s = l.strip()
                target_fd.write(s + "\t" + "pos\n")

if 0:
    s = [[1, 2, 3], [2, 2, 3]]
    for i in s:
        i[2] = 4
    for i in s:
        print i

if 0:
    def save_file(res, fpath):
        with open(fpath, "w") as fd:
            fd.write((res[9]['title'] + u"\n" + res[9]['content'] + u"\n").encode("utf-8"))

    def handle_by_type(result_dic, head_type, table):
        row_counter = 1
        fname_counter = 1
        root_dir = "/home/kelly/temp/lsh_result/6063"
        for htype in result_dic:
            if head_type == "neu":
                if htype[:1] == "p" or (htype[:1] == "n" and htype[:3] != "neu"):
                    continue
            else:
                if htype[:1] != head_type:
                    continue
                if htype[:3] == "neu":
                    continue
            for res in result_dic[htype][:167]:
                fname = str(fname_counter) + ".txt"
                save_file(res, os.path.join(root_dir, head_type, fname))
                table.write(row_counter, 0, str(row_counter).decode("utf-8"))
                table.write(row_counter, 1, Formula(('HYPERLINK("%s/%s"; "%s")' % (head_type, fname, fname)).decode("utf-8")))
                table.write(row_counter, 2, ("/".join(res[9]['negative']['words']['4'])).decode("utf-8"))
                table.write(row_counter, 3, ("/".join(res[9]['negative']['words']['3'])).decode("utf-8"))
                table.write(row_counter, 4, ("/".join(res[9]['negative']['words']['2'])).decode("utf-8"))
                table.write(row_counter, 5, ("/".join(res[9]['negative']['words']['1'])).decode("utf-8"))

                table.write(row_counter, 6, ("/".join(res[9]['negative']['words']['-4'])).decode("utf-8"))
                table.write(row_counter, 7, ("/".join(res[9]['negative']['words']['-3'])).decode("utf-8"))
                table.write(row_counter, 8, ("/".join(res[9]['negative']['words']['-2'])).decode("utf-8"))
                table.write(row_counter, 9, ("/".join(res[9]['negative']['words']['-1'])).decode("utf-8"))

                table.write(row_counter, 10, res[0])
                table.write(row_counter, 11, res[1])
                table.write(row_counter, 12, res[2])
                table.write(row_counter, 13, res[3])
                                                    
                table.write(row_counter, 14, res[4])
                table.write(row_counter, 15, res[5])
                table.write(row_counter, 16, res[6])
                table.write(row_counter, 17, res[7])

                table.write(row_counter, 18, res[8])

                table.write(row_counter, 19, res[0] + res[1] + res[2] + res[3])
                table.write(row_counter, 20, res[4] + res[5] + res[6] + res[7])
                table.write(row_counter, 21, res[9]['title'].decode("utf-8"))

                row_counter += 1
                fname_counter += 1
                print fname_counter

    def init_table(wb, sheet_name):
        table = f.add_sheet(sheet_name)
        table.write(0, 0, u"编号")
        table.write(0, 1, u"文件名")

        table.write(0, 2, u"正面句式")
        table.write(0, 3, u"正面短语")
        table.write(0, 4, u"正面词组")
        table.write(0, 5, u"正面词")

        table.write(0, 6, u"负面句式")
        table.write(0, 7, u"负面短语")
        table.write(0, 8, u"负面词组")
        table.write(0, 9, u"负面词")

        table.write(0, 10, u"正面句式数")
        table.write(0, 11, u"正面短语数")
        table.write(0, 12, u"正面词组数")
        table.write(0, 13, u"正面词数")

        table.write(0, 14, u"负面句式数")
        table.write(0, 15, u"负面短语数")
        table.write(0, 16, u"负面词组数")
        table.write(0, 17, u"负面词数")

        table.write(0, 18, u"判分")
        table.write(0, 19, u"正面总数")
        table.write(0, 20, u"负面总数")
        table.write(0, 21, u"标题")
        return table

    #生成梁少华需要的excel
    f = Workbook()

    with open("/home/kelly/result/lsh_result_n_p_6063.json") as fd:
        result_dic = ujson.load(fd)

    table = init_table(f, u"偏正")
    handle_by_type(result_dic, "p", table)

    table = init_table(f, u"偏负")
    handle_by_type(result_dic, "n", table)

    table = init_table(f, u"中性")
    handle_by_type(result_dic, "neu", table)
    #table.write(idx, 0, Formula(('HYPERLINK("%s"; "%s")' % (fname, str(idx) + ".txt")).decode("utf8")))
    f.save("simple.xls")

if 0:
    import math
    def sigmoid(x):
        return 1.0/(1.0 + pow(math.e, -0.5*x))
    def judge(res_phr, res_wordcom, res_word):
        ret = 0.0
        res_phr_weight = 2.0
        res_wordcom_weight = 2.0
        res_word_weight = 1.0
        ret += res_phr_weight * sigmoid(res_phr)
        ret += res_wordcom_weight * sigmoid(res_wordcom)
        ret += res_word_weight * sigmoid(res_word)
        return ret
    
    print judge(0, 0, 0)

if 0:
    #从oracle导出指定类型的文章
    import os
    os.environ["NLS_LANG"] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    
    import cx_Oracle
    from negative import *
    negative.init()
    idx_map = {}
    idx_map["pos_sen_ret"] = 0
    idx_map["pos_phrase_ret"] = 1
    idx_map["pos_wordcom_ret"] = 2
    idx_map["pos_word_ret"] = 3

    idx_map["neg_sen_ret"] = 4
    idx_map["neg_phrase_ret"] = 5
    idx_map["neg_wordcom_ret"] = 6
    idx_map["neg_word_ret"] = 7

    idx_map["ret_score"] = 8

    result_dic = {}

    result_dic['n1'] = []
    result_dic['n2'] = []
    result_dic['n3'] = []

    result_dic['p1'] = []
    result_dic['p2'] = []
    result_dic['p3'] = []

    result_dic['neu1'] = []
    result_dic['neu2'] = []
    result_dic['neu3'] = []

    eff_counter = 0

    def done_check():
        global result_dic
        single_len = 167
        #if len(result_dic['n1']) < single_len or len(result_dic['n2']) < single_len or len(result_dic['n3']) < single_len or len(result_dic['p1']) < single_len or len(result_dic['p2']) < single_len or len(result_dic['p3']) < single_len or len(result_dic['neu1']) < single_len or len(result_dic['neu2']) < single_len or len(result_dic['neu3']) < single_len:
        if len(result_dic['n1']) < single_len or len(result_dic['n2']) < single_len or len(result_dic['n3']) < single_len or len(result_dic['p1']) < single_len or len(result_dic['p2']) < single_len or len(result_dic['p3']) < single_len:
        #if len(result_dic['neu1']) < single_len or len(result_dic['neu2']) < single_len or len(result_dic['neu3']) < single_len:
            return 0
        return 1

    def sum_ret(ret, sum_type):
        sum_v = 0
        if sum_type == "pos":
            sum_v += ret[idx_map['pos_sen_ret']]
            sum_v += ret[idx_map['pos_phrase_ret']]
            sum_v += ret[idx_map['pos_wordcom_ret']]
            sum_v += ret[idx_map['pos_word_ret']]
        elif sum_type == "neg":
            sum_v += ret[idx_map['neg_sen_ret']]
            sum_v += ret[idx_map['neg_phrase_ret']]
            sum_v += ret[idx_map['neg_wordcom_ret']]
            sum_v += ret[idx_map['neg_word_ret']]
        else:
            sum_v += ret[idx_map['neg_sen_ret']]
            sum_v += ret[idx_map['neg_phrase_ret']]
            sum_v += ret[idx_map['neg_wordcom_ret']]
            sum_v += ret[idx_map['neg_word_ret']]
        return sum_v

    def insert_item(ret):
        global result_dic
        global eff_counter
        if ret[idx_map['ret_score']] < 2.5:
            sum_v = sum_ret(ret, "pos")
            eff_counter += 1
            if sum_v == 1:
                result_dic['p1'].append(ret)
            elif sum_v == 2:
                result_dic['p2'].append(ret)
            elif sum_v >= 3:
                result_dic['p3'].append(ret)
        elif ret[idx_map['ret_score']] > 2.5:
            sum_v = sum_ret(ret, "neg")
            eff_counter += 1
            if sum_v == 1:
                result_dic['n1'].append(ret)
            elif sum_v == 2:
                result_dic['n2'].append(ret)
            elif sum_v >= 3:
                result_dic['n3'].append(ret)
        else:
            sum_v = sum_ret(ret, "neu")
            sent_flag = (ret[idx_map['pos_sen_ret']] == ret[idx_map['neg_sen_ret']])
            phr_flag = (ret[idx_map['pos_phrase_ret']] == ret[idx_map['neg_phrase_ret']])
            wordcom_flag = (ret[idx_map['pos_wordcom_ret']] == ret[idx_map['neg_wordcom_ret']])
            word_flag = (ret[idx_map['pos_word_ret']] == ret[idx_map['neg_word_ret']])
            if sent_flag and phr_flag and wordcom_flag and word_flag:
                eff_counter += 1
                if sum_v == 1:
                    result_dic['neu1'].append(ret)
                elif sum_v == 2:
                    result_dic['neu2'].append(ret)
                elif sum_v >= 3:
                    result_dic['neu3'].append(ret)

    def get_negative_res(title, content):
        json_data = {'info_flag':"03", 'title':title, 'content':content}
        negative.judge(json_data)
        ret = export_res.get_lsh_res()
        ret += (json_data, )
        return ret
    
    def oracle_init():
        #conn = cx_Oracle.connect('SPIDER/SPIDER@192.168.1.22/ORCL')
        conn = cx_Oracle.connect('SPIDER/SPIDER@192.168.1.9/ORCL')
        cursor = conn.cursor()
        return cursor
    
    g_article_type = "03"
    g_full_size = 80000
    g_need_size = 2000
    g_neg_type = 3
    g_dir_name = "有争议"
    uid = 6063
    ref_suffix = "63"
    
    simhash_set = set()
    
    #返回0: 重复
    #返回1: 不重复
    def sim_filter(s):
        import simhash
        global simhash_set
        l_sh = simhash.Simhash(s.decode("utf-8"))
        for sh in simhash_set:
            dis = sh.distance(l_sh)
            if dis < 17:
                return 0
        else:
            simhash_set.add(l_sh)
            return 1
    
    def main():
        global eff_counter
        orien_level_dict = {2:1,1:2,4:-1,5:-2,3:0}
        
        oracle_cursor = oracle_init()
        print "oracle init done."
        
        sql_str = "select WK_T_VALIDATION_INFO.KV_TITLE,WK_T_VALIDATION_INFOCNT.KV_CONTENT, WK_T_VALIDATION_INFO.KV_ORIEN_LEVEL, WK_T_VALIDATION_REF%s.KR_UID from WK_T_VALIDATION_INFO,WK_T_VALIDATION_INFOCNT, WK_T_VALIDATION_REF%s where WK_T_VALIDATION_REF%s.KR_UID = %d and WK_T_VALIDATION_REF%s.KV_UUID = WK_T_VALIDATION_INFO.KV_UUID and WK_T_VALIDATION_INFO.KV_SOURCETYPE = '%s' and (WK_T_VALIDATION_INFO.KV_ORIEN_LEVEL = %d) and WK_T_VALIDATION_INFO.KV_CTIME > '20141116000000' and WK_T_VALIDATION_INFO.KV_UUID = WK_T_VALIDATION_INFOCNT.KV_UUID and rownum<%d " % (ref_suffix, ref_suffix, ref_suffix, uid, ref_suffix, g_article_type, g_neg_type, g_full_size)
        print sql_str
        oracle_cursor.execute(sql_str)
        print "oracle execute done."
        counter = 0
        while 1:
            result = oracle_cursor.fetchone()
            if (not result):
                break
            s = (result[0] or "") + "\n" + str(result[1]) + "\n"
            if not sim_filter(s):
                continue
            title = (result[0] or "")
            content = str(result[1])
            res = get_negative_res(title, content)
            insert_item(res)
            if done_check():
                break
            counter += 1
            print "append counter:", counter, "eff counter:", eff_counter, " neg:", len(result_dic['n1']), len(result_dic['n2']), len(result_dic['n3']) , " pos:", len(result_dic['p1']), len(result_dic['p2']), len(result_dic['p3']), " neu:", len(result_dic['neu1']), len(result_dic['neu2']), len(result_dic['neu3'])
        print "append done."
        #result = oracle_cursor.fetchall()
        #result = oracle_cursor.fetchmany(100)
    
        return 0
    
    main()
    with open("/home/kelly/result/lsh_result_n_p_%d.json" % uid, "w") as fd:
        ujson.dump(result_dic, fd)
    print "done."

if 0:
    bayes_dump_path = "/home/kelly/temp/bayes_dump.json"
    with open(bayes_dump_path) as fd:
        d = json.load(fd)
    print d['total']
    print len(d['d']['neg']['d'])
    print len(d['d']['pos']['d'])

def append_article(dest_path, src_path):
    with open(src_path) as fd:
        s = fd.read()
    with open(dest_path, "a") as fd:
        fd.write(s)

if 0:
    res_list_path = "/home/kelly/result/no_jump_no_repeat_result_list.json"
    neg_path = "/home/kelly/temp/neg.txt"
    pos_path = "/home/kelly/temp/pos.txt"

    with open(res_list_path) as fd:
        res_list = ujson.load(fd)
    neg_counter = 0
    for res in res_list:
        if res[5] < 0:
            neg_counter += 1
    print neg_counter, len(res_list), (neg_counter * 1.0)/len(res_list)

if 0:
    hd = fast_search.load("/home/kelly/code/classify/key/sexlist.txt")
    s = '这些都，看上去似乎是随机出现又很难重现, 三大症状, 百度推荐'

    r = fast_search.findall(hd, s)

    print r

if 0:
    def main():
        t = Classify()
        import time
        d = {"siteName":"新浪", "url":"http://www.sina.com/p=1", 'title':'这些都，看上去似乎是随机出现又很难重现, 三大症状','info_flag':"02"}
        while 1:
            temp = d.copy()
            t.match(temp)
            if temp['classify']['result']:
                print temp
            else:
                print "no find"
            break
        
        t.close()
    
    main()
    exit()

if 0:
    import jieba
    jieba.initialize()

    s = "abc*"

    l = jieba.cut(s)

if 0:
    hd = fast_search.load("/home/kelly/tempfile")

    s = "我测试词测试词做成好事"
    l = fast_search.findall(hd, s)
    for i in l:
        print i[0]
    print "-" * 80
    l = fast_search.optimal_findall(hd, s)
    for i in l:
        print i[0]

    print "-" * 80
    l = fast_search.jump_findall(hd, s)
    for i in l:
        print i[0]

    fast_search.close(hd)

if 0:
    #mysql test
    import MySQLdb
     
    try:
        conn=MySQLdb.connect(host='localhost',user='root',passwd='woshijay',port=3306, db='result_db')
        cur=conn.cursor()

        #cur.execute('create database if not exists result_db')
        #conn.select_db('result_db')
        #cur.execute('create table if not exists negative_result (sigmoid_coe int, phrase_weight int, wordcom_weight int, word_weight int, match_count int, wrong int, neg_to_pos int, pos_to_neg int, total int, wlength int, result_type, int);')
        cur.execute("drop table if exists negative_result")
        cur.execute('create table if not exists negative_result (sigmoid_coe int, phrase_weight int, wordcom_weight int, word_weight int, match_count int, wrong int, neg_to_pos int, pos_to_neg int, total int, wlength int, result_type int);')

        print "after exec"
        values = [[1, 2], [3, 4]]
        cur.executemany("insert into negative_result (sigmoid_coe, phrase_weight) values (%s, %s)", values)
        #cur.execute('insert into test values(%s,%s)',value)
         
        #values=[]
        #for i in range(20):
        #    values.append((i,'hi rollen'+str(i)))
        #     
        #cur.executemany('insert into test values(%s,%s)',values)
     
        #cur.execute('update test set info="I am rollen" where id=3')
     
        conn.commit()
        cur.close()
        conn.close()
        exit()
     
    except MySQLdb.Error,e:
         print "Mysql Error %d: %s" % (e.args[0], e.args[1])
         exit()

def write_file(filename, title, content):
    fd = file(filename, "w")
    fd.write(title + "\n")
    fd.write(content + "\n")
    fd.close()

root_dir = "/home/kelly/temp/04_negative"
root_dir = "/home/kelly/temp/negative_article/00"
back_root_dir = "/home/kelly/temp/04_negative_bak"
result_dir = "/home/kelly/result/"

if 0:
    #where2性能测试
    import where2
    root_path = "/home/kelly/negative_article_old/result/"

    article_list = []
    for fname in os.listdir(root_path):
        fpath = os.path.join(root_path, fname)
        title, content = read_file(fpath)
        json_data = {}
        json_data["info_flag"] = "0401"
        json_data["title"] = title
        json_data["content"] = content
        json_data["url"] = "www.baidu.com"
        article_list.append(json_data)
    
    where2.init()
    begin = datetime.datetime.now()
    for json_data in article_list:
        where2.find_address(json_data)
    end = datetime.datetime.now()
    print end - begin
    where2.py_close()

if 0:

    bitsize = 50000
    b1 = fast_bitarray.fast_bitarray(bitsize)
    b2 = fast_bitarray.fast_bitarray(bitsize)

    begin = datetime.datetime.now()
    for i in range(0, 5):
        b1[int(random.random() * bitsize)] = 1

    for i in range(0, 5):
        b2[int(random.random() * bitsize)] = 1
    end = datetime.datetime.now()

    print "fast bitarray:", end - begin
    
    #for i in range(0, bitsize):
    #    b1[i] = 1
    #    b2[i] = 1

    begin = datetime.datetime.now()
    for i in range(0, 300000):
        b3 = b1 & b2
    end = datetime.datetime.now()
    
    print "fast_bitarray:", end - begin

if 0:
    s = []
    for lists in os.listdir(root_dir):
        fd = open(os.path.join(root_dir, lists), "r")
        tx = fd.read()
        fd.close()
        s.append(tx)
    fd = open("/home/kelly/temp/negative_article/01_00.pickle", "w")
    pickle.dump(s, fd)
    fd.close()

#boson result pickle dump
if 0:
    t1_n2 = []
    t1_n1 = []
    t1_00 = []
    t1_p1 = []
    t1_p2 = []

    t2_n2 = []
    t2_n1 = []
    t2_00 = []
    t2_p1 = []
    t2_p2 = []

    t3_n2 = []
    t3_n1 = []
    t3_00 = []
    t3_p1 = []
    t3_p2 = []

    article_list = []
    fd = open("/home/kelly/temp/negative_article/01_00.pickle", "r")
    t1_00 = pickle.load(fd)
    article_list.append(t1_00)
    fd.close()

    fd = open("/home/kelly/temp/negative_article/01_n2.pickle", "r")
    t1_n2 = pickle.load(fd)
    article_list.append(t1_n2)
    fd.close()

    fd = open("/home/kelly/temp/negative_article/01_n1.pickle", "r")
    t1_n1 = pickle.load(fd)
    article_list.append(t1_n1)
    fd.close()

    fd = open("/home/kelly/temp/negative_article/01_p1.pickle", "r")
    t1_p1 = pickle.load(fd)
    article_list.append(t1_p1)
    fd.close()

    fd = open("/home/kelly/temp/negative_article/01_p2.pickle", "r")
    t1_p2 = pickle.load(fd)
    article_list.append(t1_p2)
    fd.close()
    
    fd = open("/home/kelly/temp/negative_article/02_00.pickle", "r")
    t2_00 = pickle.load(fd)
    article_list.append(t1_00)
    fd.close()

    fd = open("/home/kelly/temp/negative_article/02_n2.pickle", "r")
    t2_n2 = pickle.load(fd)
    article_list.append(t2_n2)
    fd.close()

    fd = open("/home/kelly/temp/negative_article/02_n1.pickle", "r")
    t2_n1 = pickle.load(fd)
    article_list.append(t2_n1)
    fd.close()

    fd = open("/home/kelly/temp/negative_article/02_p1.pickle", "r")
    t2_p1 = pickle.load(fd)
    article_list.append(t2_p1)
    fd.close()

    fd = open("/home/kelly/temp/negative_article/02_p2.pickle", "r")
    t2_p2 = pickle.load(fd)
    article_list.append(t2_p2)
    fd.close()
    
    fd = open("/home/kelly/temp/negative_article/03_00.pickle", "r")
    t3_00 = pickle.load(fd)
    article_list.append(t3_00)
    fd.close()

    fd = open("/home/kelly/temp/negative_article/03_n2.pickle", "r")
    t3_n2 = pickle.load(fd)
    article_list.append(t3_n2)
    fd.close()

    fd = open("/home/kelly/temp/negative_article/03_n1.pickle", "r")
    t3_n1 = pickle.load(fd)
    article_list.append(t3_n1)
    fd.close()

    fd = open("/home/kelly/temp/negative_article/03_p1.pickle", "r")
    t3_p1 = pickle.load(fd)
    article_list.append(t3_p1)
    fd.close()

    fd = open("/home/kelly/temp/negative_article/03_p2.pickle", "r")
    t3_p2 = pickle.load(fd)
    article_list.append(t3_p2)
    fd.close()

    fd = open("/home/kelly/result/boson_article.pickle", "w")
    pickle.dump(article_list, fd)
    fd.close()
    import json
    import requests

    SENTIMENT_URL = 'http://api.bosondata.net/sentiment/analysis'
    # 注意:在测试时请更换为您的 API token 。
    headers = {'X-Token': '79Gz4V4a.30.pGyP15oARMb_'}
    #s = [""" """, """ """]
    #data = json.dumps(s)
    #resp = requests.post(SENTIMENT_URL, headers=headers, data=data)
    #print resp.text, type(resp.text)

    boson_result = []
    try:
        for i in article_list:
            single_result = []
            for idx in range(0, len(i), 100):
                cut = i[idx:idx + 100]
                if len(cut) > 0:
                    data = json.dumps(cut)
                    resp = requests.post(SENTIMENT_URL, headers=headers, data=data)
                    ret = json.loads(resp.text)
                    single_result += ret
                else:
                    break
            #print single_result
            print len(single_result)
            boson_result.append(single_result)
    except Exception, reason:
        print reason
    fd = open("/home/kelly/result/boson_result.pickle", "w")
    pickle.dump(boson_result, fd)
    fd.close()

#boson result pickle load
if 0:
    result_name = ["01_00", "01_n2", "01_n1", "01_p1", "01_p2", "02_00", "02_n2", "02_n1", "02_p1", "02_p2", "03_00", "03_n2", "03_n1", "03_p1", "03_p2"]
    result_dic = {}
    for i in result_name:
        result_dic[i] = result_name.index(i)
    result_path = "/home/kelly/result/boson_result.pickle"
    article_path = "/home/kelly/result/boson_article.pickle"
    fd = open(result_path, "r")
    boson_result = pickle.load(fd)
    fd.close()
    fd = open(article_path, "r")
    boson_article = pickle.load(fd)
    fd.close()
    print boson_result[result_dic["03_n2"]][0]
    counter = 0
    for i in boson_result[result_dic["03_n2"]]:
        if i[1] < 0.6:
            counter += 1
            print i
            print boson_article[result_dic["03_n2"]][boson_result[result_dic["03_n2"]].index(i)]
            print ("-" * 80)
    print "diff:", counter
    exit()

#human boson result pickle dump
if 0:
    #article顺序
    t1_n2 = []
    t1_n1 = []
    t1_00 = []
    t1_p1 = []
    t1_p2 = []

    t2_n2 = []
    t2_n1 = []
    t2_00 = []
    t2_p1 = []
    t2_p2 = []

    t3_n2 = []
    t3_n1 = []
    t3_00 = []
    t3_p1 = []
    t3_p2 = []

    neg_des_dic = {
            2:"确定负面", 
            1:"疑似负面", 
            0:"有争议",
            1:"疑似正面",
            2:"确定正面"
            }
    def gen_article_list(root_dir):
        neg_dic = {
                "确定负面":"n2", 
                "疑似负面":"n1", 
                "有争议":"00",
                "疑似正面":"p1",
                "确定正面":"p2",
                "负面":"-1",
                "非负面":"1",
                }
        ret_dic = {}
        #article path
        for lists in os.listdir(root_dir):
            type_path = os.path.join(root_dir, lists)
            #01 02 03
            for neg_type_path in os.listdir(type_path):
                neg_desc_path = os.path.join(type_path, neg_type_path)
                #确定负面...
                for neg_desc in os.listdir(neg_desc_path):
                    neg_desc_file = os.path.join(neg_desc_path, neg_desc)
                    title, content = read_file(neg_desc_file)
                    if not ret_dic.has_key(lists):
                        ret_dic[lists] = {}
                    if not ret_dic[lists].has_key(neg_dic[neg_type_path]):
                        ret_dic[lists][neg_dic[neg_type_path]] = []
                    #print lists, neg_dic[neg_type_path]
                    #print neg_desc_file
                    #ret_set.add(str(lists) + "_" + str(neg_dic[neg_type_path]))
                    #title, content = read_file()
                    tmp_list = [title, content]
                    ret_dic[lists][neg_dic[neg_type_path]].append(tmp_list)
        return ret_dic

    human_classify_article = gen_article_list("/home/kelly/temp/negative_article_human_classify")
    #fd = open("/home/kelly/result/human_classify_article.pickle", "w")
    #pickle.dump(human_classify_article, fd)
    #fd.close()

    #print "counter:", counter
    #exit()
    article_list = human_classify_article

    import json
    import requests

    SENTIMENT_URL = 'http://api.bosondata.net/sentiment/analysis'
    # 注意:在测试时请更换为您的 API token 。
    headers = {'X-Token': '79Gz4V4a.30.pGyP15oARMb_'}
    #s = [""" """, """ """]
    #data = json.dumps(s)
    #resp = requests.post(SENTIMENT_URL, headers=headers, data=data)
    #print resp.text, type(resp.text)

    counter = 0
    try:
        for i in article_list:
            for j in article_list[i]:
                for idx in range(0, len(article_list[i][j]), 100):
                    cut = article_list[i][j][idx:idx + 100]
                    s_list = []
                    for article in cut:
                        s = article[0] + article[1]
                        s_list.append(s)
                    if len(s_list) > 0:
                        data = json.dumps(s_list)
                        resp = requests.post(SENTIMENT_URL, headers = headers, data = data)
                        ret = json.loads(resp.text)
                        print "counter:", counter
                        counter += 1
                        for single_ret in ret:
                            article_list[i][j][idx + ret.index(single_ret)].append(single_ret)
                    else:
                        break
    except Exception, reason:
        print reason
    fd = open("/home/kelly/result/human_classify_boson_result.pickle", "w")
    pickle.dump(article_list, fd)
    fd.close()
    exit()
    try:
        for i in article_list:
            single_result = []
            for idx in range(0, len(i), 100):
                cut = i[idx:idx + 100]
                if len(cut) > 0:
                    data = json.dumps(cut)
                    resp = requests.post(SENTIMENT_URL, headers=headers, data=data)
                    ret = json.loads(resp.text)
                    single_result += ret
                else:
                    break
            #print single_result
            print len(single_result)
    except Exception, reason:
        print reason
    #fd = open("/home/kelly/result/boson_result.pickle", "w")
    #pickle.dump(boson_result, fd)
    #fd.close()

#human boson result pickle load
if 0:
    result_name = ["01_00", "01_n2", "01_n1", "01_p1", "01_p2", "02_00", "02_n2", "02_n1", "02_p1", "02_p2", "03_00", "03_n2", "03_n1", "03_p1", "03_p2"]
    result_dic = {}
    for i in result_name:
        result_dic[i] = result_name.index(i)
    result_path = "/home/kelly/result/boson_result.pickle"
    article_path = "/home/kelly/result/boson_article.pickle"
    fd = open(result_path, "r")
    boson_result = pickle.load(fd)
    fd.close()
    fd = open(article_path, "r")
    boson_article = pickle.load(fd)
    fd.close()
    print boson_result[result_dic["03_n2"]][0]
    counter = 0
    for i in boson_result[result_dic["03_n2"]]:
        if i[1] < 0.6:
            counter += 1
            print i
            print boson_article[result_dic["03_n2"]][boson_result[result_dic["03_n2"]].index(i)]
            print ("-" * 80)
    print "diff:", counter
    exit()

if 0:
    l = [1, {"2":2}, 3]
    fd = file("/home/kelly/tempfile", "w")
    pickle.dump(l, fd)

    fd.close()

    fd = file("/home/kelly/tempfile", "r")
    l = pickle.load(fd)
    print l
    fd.close()
    exit()

if 0:
    for lists in os.listdir(root_dir):
        path = os.path.join(root_dir, lists)
        print path
    exit()

if 0:
    #单条测试
    negative.init()
    title, content = read_file("/home/kelly/tempfile")
    json_data = {"info_flag":"02", "title":title, "content":content, "sigmoid_coe":-0.5}

    negative.judge(json_data)
    #res = export_res.get_res()
    #print "res:", res

    print json_data["negative"]
    for words in json_data["negative"]["words"]:
        print words, ":"
        for word in json_data["negative"]["words"][words]:
            print word

    print export_res.get_lsh_res()
    #print json_data["title"]
    #print json_data["content"]
    negative.close()
    exit()
    tmp_v = negative.update_keys(-1)

    negative.judge(json_data)

    print json_data["negative"]
    for words in json_data["negative"]["words"]:
        print words, ":"
        for word in json_data["negative"]["words"][words]:
            print word

    exit()

from xlrd import *
def gen_fpath_result_from_xls(root_path, xls_path):
    ret_dic = {}
    wb_hd = open_workbook(xls_path)
    l = wb_hd.sheet_names()
    for sheet in l:
        if sheet.find(u"兼容性") >= 0:
            continue
        table = wb_hd.sheet_by_name(sheet)
        dirname = sheet.encode("utf-8").split("_")
        full_dir = os.path.join(root_path, dirname[0], dirname[1])
        for row_idx in range(1, table.nrows):
            row_v = table.row_values(row_idx)
            fname = row_v[0].encode("utf-8")
            v = int(row_v[2])
            src_name = os.path.join(full_dir, fname)
            ret_dic[src_name] = v
    return ret_dic

if 0:
    d = gen_fpath_result_from_xls("/home/kelly/negative_article", "/home/kelly/code/train_negative/simple.xls")
    zero = 0
    not_zero = 0
    for i in d:
        if d[i]:
            not_zero += 1
        else:
            zero += 1
    print zero, not_zero

if 0:
    #allover test
    negative.init()
    result_path = "/home/kelly/temp/jump_result.json"
    result = {}
    result_list = []
    result_list_path = "/home/kelly/result/jump_no_repeat_result_list.json"
    article_path = "/home/kelly/negative_article"
    fpath_res_dic = {}
    res_path = "/home/kelly/result/no_jump_find_res.json"
    human_judge_dic = gen_fpath_result_from_xls(article_path, "/home/kelly/code/train_negative/simple.xls")
    for article_type in os.listdir(article_path):
        article_type_path = os.path.join(article_path, article_type)
        if not os.path.isdir(article_type_path):
            continue
        for neg_type in os.listdir(article_type_path):
            neg_type_path = os.path.join(article_type_path, neg_type)
            if neg_type == "04":
                info_flag = "0401"
            else:
                info_flag = "01"
            if not os.path.isdir(neg_type_path):
                continue
            for article in os.listdir(neg_type_path):
                fpath = os.path.join(neg_type_path, article)
                title, content = read_file(fpath)
                json_data = {'info_flag':info_flag, 'title':title, 'content':content}
                negative.judge(json_data)
                result[fpath] = json_data['negative']
                fpath_res_dic[fpath] = export_res.get_res()
                try:
                    result_list.append((fpath, ) + export_res.get_res() + (human_judge_dic[fpath],))
                except:
                    pass
    negative.close()
    with open(result_path, 'w') as fd:
        ujson.dump(result, fd)
    with open(res_path, "w") as fd:
        ujson.dump(fpath_res_dic, fd)
    with open(result_list_path, "w") as fd:
        ujson.dump(result_list, fd)

if 0:
    result_list_path = "/home/kelly/result/jump_no_repeat_result_list.json"
    with open(result_list_path) as fd:
        result_list = ujson.load(fd)
    arg_list = [result[1:] for result in result_list]
    l = export_res.train_run(arg_list)
    with open("/home/kelly/result/jump_find_no_repeat_merge_result.json", "w") as fd:
        ujson.dump(l, fd)

def insert_items(values):
    import MySQLdb
    conn = MySQLdb.connect(host='localhost',user='root',passwd='woshijay',port=3306, db='result_db')
    cur=conn.cursor()
    cur.execute('create table if not exists negative_result (sigmoid_coe int, phrase_weight int, wordcom_weight int, word_weight int, match_count int, wrong int, neg_to_pos int, pos_to_neg int, total int, wlength int, result_type int);')
    cur.executemany("insert into negative_result values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", values)
    conn.commit()
    cur.close()
    conn.close()

if 0:
    result_path = "/home/kelly/result/jump_find_no_repeat_merge_result.json"
    lv1_v_list = []
    lv2_v_list = []
    lv3_v_list = []
    result_type = 10
    with open(result_path) as fd:
        result_list = ujson.load(fd)
    for i in result_list:
        lv1_v_list.append((int(i[0] * 100), int(i[1] * 10), int(i[2] * 10), int(i[3] * 10), i[6][0], i[5][0], i[7][0], i[8][0], i[4][0], 100, result_type))
        lv2_v_list.append((int(i[0] * 100), int(i[1] * 10), int(i[2] * 10), int(i[3] * 10), i[6][1], i[5][1], i[7][1], i[8][1], i[4][1], 165, result_type))
        lv3_v_list.append((int(i[0] * 100), int(i[1] * 10), int(i[2] * 10), int(i[3] * 10), i[6][2], i[5][2], i[7][2], i[8][2], i[4][2], 0, result_type))
    insert_items(lv1_v_list)
    insert_items(lv2_v_list)
    insert_items(lv3_v_list)

if 0:
    #查找neg_to_pos判定的文章
    result_list_path = "/home/kelly/result/no_jump_no_repeat_result_list.json"
    with open(result_list_path) as fd:
        result_list = ujson.load(fd)
    print result_list[0]
    print result_list[0][0]
    phrase_weight = [0.75, 1, 2]
    wordcom_weight = [0.75, 1, 2]
    word_weight = [3.5, 3, 1]
    wlen_idx = 4 
    human_judge_idx = 5
    for result in result_list:
        if result[0].find(u"04") >= 0:
            continue
        idx = 0
        if result[wlen_idx] >= 165:
            idx = 2
        elif result[wlen_idx] >= 100 and result[wlen_idx] < 165:
            idx = 1

        fake_judge = export_res.fake_judge(-0.5, phrase_weight[idx], wordcom_weight[idx], word_weight[idx], result[1], result[2], result[3])
        if fake_judge >= 0 and result[human_judge_idx] < 0:
            print result[0], fake_judge, result[human_judge_idx], result[1:5]
    exit()
    arg_list = [result[1:] for result in result_list]
    l = export_res.train_run(arg_list)
    with open("/home/kelly/result/jump_find_no_repeat_merge_result.json", "w") as fd:
        ujson.dump(l, fd)

if 0:
    #测试fake_judge
    res = export_res.fake_judge(-0.5, 2, 2, 1, 0, 0, 0)
    print res

if 0:
    #统计jump_find与no_jump_find不同的个数
    jump_find_res_path = "/home/kelly/result/jump_find_res.json"
    no_jump_find_res_path = "/home/kelly/result/no_jump_find_res.json"
    with open(jump_find_res_path) as fd:
        jump_find_res_dic = ujson.load(fd)
    with open(no_jump_find_res_path) as fd:
        no_jump_find_res_dic = ujson.load(fd)
    for fpath in jump_find_res_dic:
        jump_res = jump_find_res_dic[fpath]
        no_jump_res = no_jump_find_res_dic[fpath]
        if jump_res != no_jump_res:
            print jump_res, no_jump_res

if 0:
    #回归测试
    fd = open("/home/kelly/result/human_classify_article_list_for_boson.pickle", "r")
    article_list = pickle.load(fd)
    fd.close()

    negative.init()

    json_data = {"info_flag":"02", "sigmoid_coe":-0.5}
    for i in article_list:
        info_flag = i[2]
        if i[2] == "04":
            info_flag = "0401"
        json_data["info_flag"] = info_flag
        json_data["title"] = i[0]
        json_data["content"] = i[1]
        negative.judge(json_data)
        #print json_data["negative"]
    negative.close()
    exit()
    counter = 0
    for lists in os.listdir(root_dir):
        print counter
        filename = os.path.join(root_dir, lists)
        title, content = read_file(filename)
        json_data = {}
        json_data["info_flag"] = "0401"
        json_data["title"] = title
        json_data["content"] = content
    
        negative.judge(json_data)
    
        if json_data["negative"]["result"] >= 0:
            os.remove(filename)
            print filename
            #write_file(result_dir + str(counter), title, content)
            #counter += 1

    negative.close()
    exit()

if 0:
    #更新测试
    negative.init()
    while 1:
        raw_input(">>")
    pass

if 0:
    import argparse
    import sys
    
    r, w = os.pipe()
    pid = os.fork()
    if pid:
        #os.close(w)
        r = os.fdopen(r)
        ret = pickle.load(r)
        print "in parent:", ret
    else:
        #os.close(r)
        w = os.fdopen(w, "w")
        l = [1, 2]
        time.sleep(3)
        pickle.dump(l, w)
        exit()
    exit()

if 0:
    w = warning.warning()

    root_path = "/home/kelly/backup/warning_article"
    counter = 0
    miss_counter = 0
    title_flag_counter = 0
    for fname in os.listdir(root_path):
        counter += 1
        #print counter, fname
        fpath = os.path.join(root_path, fname)
        with open(fpath, "r") as fd:
            title, content = read_file(fpath)
            json_data = {}
            json_data["title"] = title
            json_data["content"] = content
            json_data["user_words"] = {"result":[{"user_id":1868, "subject_id":100}]}
            if fname.find("04_") > 0:
                json_data["info_flag"] = "0401"
            else:
                json_data["info_flag"] = "01"
            json_data["negative"] = {"result":-2}
            json_data["url"] = "www.baidu.com"
            json_data["ctime"] = datetime.datetime.utcnow()
            w.judge(json_data)
            if not json_data["warning"] or not json_data["warning"]["result"]:
                print fname
                print json_data["warning"]
                miss_counter += 1

    print counter
    print miss_counter
    exit()

    with open("warning/json_list.pickle", "r") as fd:
        json_list = pickle.load(fd)

    print len(json_list)
    ctime = datetime.datetime.utcnow()
    begin = datetime.datetime.now()
    for json_data in json_list:
        json_data["ctime"] = ctime
        w.judge(json_data)
    end = datetime.datetime.now()
    print end - begin
    print "done."
    
if 0:
    w = warning.warning()

    fpath = "/home/kelly/tempfile"
    fname = "tempfile"
    title, content = read_file(fpath)
    json_data = {}
    json_data["title"] = title
    json_data["content"] = content
    json_data["user_words"] = {"result":[{"user_id":1868, "subject_id":100}]}
    json_data["info_flag"] = "01"
    json_data["negative"] = {"result":-2}
    json_data["url"] = "www.baidu.com"
    json_data["location"] = {"result":[{"words":"河北省"}, {"words":"测试"}]}
    json_data["ctime"] = datetime.datetime.utcnow()
    w.judge(json_data)
    print json_data["warning"]

if 0:
    import jieba
    fpath = "/home/kelly/code/warning/key/stopwords.txt"
    rubbish_set = set()
    with open(fpath, "r") as fd:
        for l in fd:
            rubbish_set.add(l.strip())

    s = "测试/测试/测试>"
    word_list = jieba.cut(s)
    for word in word_list:
        word = word.encode("utf-8")
        if word in rubbish_set:
            print word
    pass

if 0:
    s = "<a测试>"
    r = re.compile('</?\w+[^>]*>')
    
    start = datetime.datetime.utcnow()
    print r.sub("", s)
    
    for i in range(10000):
        subs = r.sub("", s)
    end = datetime.datetime.utcnow()
    
    print end - start

if 0:
    import where2
    import jieba

    def get_location_set(fpath):
        location_set = set()
        with open(fpath, "r") as fd:
            for l in fd:
                loc = l.split("\t")[0]
                location_set.add(loc.decode("utf-8"))
        return location_set

    def jieba_where(location_set, s):
        ws = set(jieba.cut(s))
        return location_set & ws

    fpath = "/home/kelly/code/trunk/filters/where2/key/location.txt"
    location_set = get_location_set(fpath)
    
    where2.init()
    jieba.initialize()
    
    article_root_path = "/home/kelly/negative_article/01/确定负面/"
    fpath = os.path.join(article_root_path, "540.txt")
    print fpath
    title, content = read_file(fpath)
    json_data = {}
    json_data["title"] = title
    json_data["content"] = content
    where2.find_address(json_data)
    where2_set = set()
    jieba_result = jieba_where(location_set, title + content)
    print jieba_result
    for w in json_data["location"]["result"]:
        if w["words"].decode("utf-8") in jieba_result:
            jieba_result.remove(w["words"].decode("utf-8"))
        else:
            print "find_address特有:", w["words"]
    for i in jieba_result:
        print "jieba特有:", i
    raw_input("continue>>")

if 0:
    import where2
    where2.init()
    json_data = {}
    title, content = read_file("/home/kelly/tempfile")
    print "test content:", content
    json_data["info_flag"] = "01"
    json_data["title"] = title
    json_data["content"] = content
    where2.find_address(json_data)
    #print json_data["location"]
    #for w in json_data["location"]["result"]:
    #    print w["words"]
    #for w in json_data["location"]["common_shield"]:
    #    print w
    #for d in json_data["location"]["result"]:
    #    print "province:", d.get('province', "")
    #    print "city:", d.get('city', "")
    #    print "district:", d.get('district', "")
    #    print "-" * 50
    print json_data.get('classify', {})
    where2.py_close()

if 0:
    import where2
    import ujson
    where2.init()
    root_path = "/home/kelly/negative_article"
    article_list = []
    counter = 0
    for article_type in os.listdir(root_path):
        article_path = os.path.join(root_path, article_type)
        if not os.path.isdir(article_path):
            continue
        for neg_type in os.listdir(article_path):
            neg_path = os.path.join(article_path, neg_type)
            for fname in os.listdir(neg_path):
                fpath = os.path.join(neg_path, fname)
                title, content = read_file(fpath)
                article_list.append((title, content))
                counter += 1
    s = ujson.dumps(article_list)
    with open("/home/kelly/code/tempfile", "w") as fd:
        fd.write(s)
    exit()

if 0:
    import where2
    import ujson
    where2.init()
    with open("/home/kelly/code/tempfile", "r") as fd:
        s = fd.read()
    article_list = ujson.loads(s)
    json_data = {}
    print "begin judge"
    begin = datetime.datetime.now()
    counter = 0
    for article in article_list:
        json_data["title"] = article[0].encode("utf-8")
        json_data["content"] = article[1].encode("utf-8")
        where2.find_address(json_data)
        counter += 1
        print counter
        if counter > 1000:
            break
    end = datetime.datetime.now()
    print end - begin

if 0:
    import jieba.posseg as pseg

    s = "我要回家吃饭了我要回家吃饭了我要回家吃饭了我要回家吃饭了我要回家吃饭了我要回家吃饭了我要回家吃饭了我要回家吃饭了我要回家吃饭了我要回家吃饭了我要回家吃饭了"
    print "before pseg cut"
    pseg.cut(s)

    print "second pseg cut"
    pseg.cut(s)

    print "pseg cut done."
    pass

if 0:
    def gen_dict_set(fpath):
        ret_set = set()
        with open(fpath, "r") as fd:
            for l in fd:
                ret_set.add(l.strip())
        return ret_set

    dict_path = "/home/kelly/test_dict.txt"
    dict_set = gen_dict_set(dict_path)

    location_fpath = "/home/kelly/code/where2/key/location.txt"
    ret_list = []
    with open(location_fpath, "r") as fd:
        for l in fd:
            idx = l.find("\t")
            if len(l[:idx].decode("utf-8")) == 2:
                ret_list.append(l[:idx])

    dict_list = list(dict_set)
    for location in ret_list:
        for w in dict_list:
            if w.find(location) >= 0 and w != location:
                print w, location

if 0:
    import jieba
    s = "江津区石蛤镇婚姻登记"

    l = list(jieba.cut(s))

    print "/".join(l)

if 0:
    import jieba

    s = "发展中国家"
    l = list(jieba.cut(s))

    print "/".join(l)

if 0:
    import simhash

    s1 = u"""
    利比亚11架商用飞机失踪 美方担忧再现911式恐袭
    【环球军事报道】据英国《每日邮报》3日报道，利比亚政府8月31日宣布丧失对首都的黎波里的控制权后，以伊斯兰民兵武装为主的“利比亚黎明”部队占领的黎波里国际机场后，共有11架商用飞机失踪。美国官员担心被伊斯兰武装分子偷走，可能借此对该地区发动另一场“9·11”恐怖袭击。

    　　摩洛哥军事专家麦考伊称，11架飞机被另一个伊斯兰组织“蒙面旅”接管，该组织就是两年前袭击美国驻班加西领事馆的主谋。有可靠情报显示，“蒙面旅”正密谋9月11日使用飞机对马格里布国家发动袭击。反恐专家戈尔卡告诉“华盛顿自由灯塔”网，恐怖分子可能使用两种方式袭击北非地区甚至是沙特油田：一是像2001年9月11日那样，让飞机从客运模式转换成具有巨大威力的制导导弹，另一种是利用飞机的民航标志运载恐怖分子进入袭击区域。美国正寻找失踪飞机的下落。
        """
    s2 = u"""
    [转载]利比亚11架商用飞机失踪
    【环球军事报道】据英国《每日邮报》3日报道，利比亚政府8月31日宣布丧失对首都的黎波里的控制权后，以伊斯兰民兵武装为主的“利比亚黎明”部队占领的黎波里国际机场后，共有11架商用飞机失踪。美国官员担心被伊斯兰武装分子偷走，可能借此对该地区发动另一场“9·11”恐怖袭击。

    　　摩洛哥军事专家麦考伊称，11架飞机被另一个伊斯兰组织“蒙面旅”接管，该组织就是两年前袭击美国驻班加西领事馆的主谋。有可靠情报显示，“蒙面旅”正密谋9月11日使用飞机对马格里布国家发动袭击。反恐专家戈尔卡告诉“华盛顿自由灯塔”网，恐怖分子可能使用两种方式袭击北非地区甚至是沙特油田：一是像2001年9月11日那样，让飞机从客运模式转换成具有巨大威力的制导导弹，另一种是利用飞机的民航标志运载恐怖分子进入袭击区域。美国正寻找失踪飞机的下落。

    """

    sh1 = simhash.Simhash(s1)
    sh2 = simhash.Simhash(s2)

    print sh1.distance(sh2)
    print "%x" % sh1.value
    print "%x" % sh2.value

    print len(s1)

if 0:
    import json
    json_str = '{"province":"北京市", "city":"北京市"}'
    json_str = u'{"title":"\u6c5f\u897f\u7701\u8d63\u5dde\u5e02\u5b89\u8fdc\u53bf\u957f\u6c99\u4e61\u6d3e\u51fa\u6240\u4e0d\u4e3a\u8001\u767e\u59d3\u529e\u4e8b\u53ea\u4f1a\u675f"}'

    begin = datetime.datetime.now()
    for i in range(1):
        j = json.loads(json_str)
        print j["title"]
    end = datetime.datetime.now()

    print end - begin

if 0:
    def fibonacci(n):
        x, y = 0, 1
        while n:
            x, y, n = y, x + y, n - 1
        return x
        if n <= 0:
            return -1

        if n == 1:
            return 1

        if n == 2:
            return 1

        before_ret1 = 1
        before_ret2 = 1
        ret = 2
        n -= 2

        while n:
            ret = before_ret1 + before_ret2
            before_ret1 = before_ret2
            before_ret2 = ret
            n -= 1
        return ret
    
    begin = datetime.datetime.now()
    ret = fibonacci(1000000)
    end = datetime.datetime.now()

    print end - begin

if 0:
    import where2
    where2.init()

    raw_input(">>")
