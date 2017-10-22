#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
es request command
"""
from es_wrapper_client import ElasticSearchWrapperClient

if __name__ == '__main__':
    es_client = ElasticSearchWrapperClient('http://10.90.185.175:9200/')

    for i in xrange(0, 9):
        content_file = open('/home/zshell/Desktop/20170925/part-0000' + i.__str__(), 'r')
        content_list = content_file.readlines()
        for content in content_list:
            res = es_client.index(content, 'product_label_idx_v2', 'product_label')
            if res == False:
                print (content)
        content_file.close()
