#!/usr/bin/python
# coding:utf-8
"""
es request wrapper tool for python
"""
from es_wrapper_client import ElasticSearchWrapperClient

if __name__ == '__main__':
    es_client = ElasticSearchWrapperClient("http://l-es2.data.p1.11bee.com:9273/")
    id_file = open('/home/zshell/Desktop/id-list.txt', 'r')
    id_list = id_file.readlines()
    count = 0
    for doc_id in id_list:
        if doc_id.endswith('\r\n'):
            doc_id = doc_id.strip('\r\n')
        if doc_id.endswith('\n'):
            doc_id = doc_id.strip('\n')
        es_client.delete('vehicle_new_merge_info_idx_v2', 'vehicle_new_merge_info', doc_id)
        print('delete ' + doc_id)
        count = count + 1

    print('count = ' + count.__str__())
