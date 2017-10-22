#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
es request wrapper tool for python
"""
from elasticsearch import Elasticsearch
from elasticsearch import NotFoundError
import json
import time
import sys


class ElasticSearchWrapperClient:
    """
    a wrapper client to the official elasticsearch client
    """

    def __init__(self, request_url):
        """
        :param request_url: url of target es node
        """
        self.es_client = Elasticsearch(request_url)

    def search(self, index, doc_type=None, dsl_command='{\"query\": {\"match_all\": {}}, \"size\": 10}'):
        """
        normal search from es
        :return: list of json result
        """
        try:
            res = self.es_client.search(index, doc_type, dsl_command)
            print ("got %d hits" % res['hits']['total'])
            search_result = []
            for hit in res['hits']['hits']:
                search_result.append(hit)
            return search_result
        except Exception, e:
            print ('es query request error: %s' % e)

    def index(self, content, index, doc_type=None, doc_id=None):
        """
        normal index single doc into es
        :param content: the json content of the target document
        :param index: builtin
        :param doc_type: builtin
        :param doc_id: can be none and es will gen id itself
        :return: if successfully index the doc
        """
        try:
            res = self.es_client.index(index, doc_type, content, doc_id)
            return res
        except Exception, e:
            print ('es query request error: %s' % e)

    def get(self, index, doc_id, doc_type='_all'):
        """        
        :return: 
        """
        try:
            res = self.es_client.get(index, doc_id, doc_type)
            res_dict = dict(res)
            if res_dict.get('found'):
                return res_dict.get('_source')
        except NotFoundError:
            print ('not found %s, get fail' % doc_id)
            return None

    def multi_get(self, ids, index, doc_type=None):
        """
        multi get by a list of ids        
        :return: list of json result
        """
        try:
            ids_body = {"ids": ids}
            res = self.es_client.mget(ids_body, index, doc_type)
            res_dict = dict(res)
            docs = res_dict['docs']
            doc_list = []
            doc_id_list = []
            for doc in docs:
                if not doc['found']:
                    continue
                doc_list.append(doc['_source'])
                doc_id_list.append(doc['_id'])
            return doc_list, doc_id_list
        except Exception, e:
            print ('es query request error: %s' % e)

    def delete(self, index, doc_type, doc_id):
        """
        delete a given document        
        :return: True or False to delete the document
        """
        try:
            res = self.es_client.delete(index, doc_type, doc_id)
            res_dict = dict(res)
            if res_dict.get('found'):
                return res_dict.get('found')
        except NotFoundError:
            print ("not found %s, delete fail" % doc_id)
            return False


if __name__ == '__main__':
    es_request_url = sys.argv[1]
    if es_request_url:
        es_request_tool = ElasticSearchWrapperClient(es_request_url)
        while True:
            begin_time = int(round(time.time() * 1000))
            result = es_request_tool.search('test_idx', 'test_idx')
            end_time = int(round(time.time() * 1000))
            print ('latency: %d \n' % (end_time - begin_time))
            print (json.dumps(result))
            time.sleep(1)
