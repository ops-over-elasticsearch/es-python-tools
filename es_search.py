#!/usr/bin/python
# coding:utf-8
"""
es request wrapper tool for python
"""
from es_wrapper_client import ElasticSearchWrapperClient
import json

if __name__ == '__main__':
    es_client = ElasticSearchWrapperClient("http://xxx.com:9222")
    strsss = ',\"from\":0, \"size\":10000'
    search_dsl = '{\"filter\":{\"bool\":{\"must\":[{\"terms\":{\"businessChanceId\":[%s]}},{\"term\":{\"nature\":4}},{\"term\":{\"channel\":\"2dj\"}},{\"terms\":{\"taskStatus\":[0,1]}}]}},\"size\":300,"_source":[\"businessChanceId\"]}'
    id_file = open('/home/zshell/Desktop/bid.txt', 'r')
    id_list = id_file.readlines()
    tmp_id_list = []
    result_files = open('/home/zshell/Desktop/result', 'w')
    condition = ''
    count = 0
    try:
        for bid in id_list:
            if bid.endswith('\r\n'):
                bid = bid.strip('\r\n')
            if bid.endswith('\n'):
                bid = bid.strip('\n')
            bid = '\"' + bid + '\"'
            tmp_id_list.append(bid)
            if tmp_id_list.__len__() < 300:
                continue
            query = ','.join(tmp_id_list)
            dsl = search_dsl % query
            result = es_client.search('task_info_idx', 'task_info', dsl)
            for item in result:
                result_files.write(str(item) + '\n')
            tmp_id_list = []
    finally:
        result_files.close()
