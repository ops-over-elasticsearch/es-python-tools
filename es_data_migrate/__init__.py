#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
es data migrate tool for python
"""
from es_wrapper_client import ElasticSearchWrapperClient
import hashlib


class ElasticSearchDataMigrate:
    """
    es data migrate from one index to another index
    """

    def __init__(self, *request_url):
        """
        :param from_request_url: url of target es node which migrate from
        :param to_request_url: url of target es node which migrate to, if not exists, equals to from_request_url
        """
        target_request_urls_count = request_url.__len__()
        if target_request_urls_count == 1:
            self.es_from_client = ElasticSearchWrapperClient(request_url[0])
            self.es_to_client = ElasticSearchWrapperClient(request_url[0])
        elif target_request_urls_count == 2:
            self.es_from_client = ElasticSearchWrapperClient(request_url[0])
            self.es_to_client = ElasticSearchWrapperClient(request_url[1])
        else:
            raise Exception('not supported constructor argument num')

    def tiny_migrate(self, from_index, to_index, from_type=None, to_type=None,
                     dsl_command='{\"query\": {\"match_all\": {}}, \"size\": 10}'):
        """
        applicable for a little number of data which can be fetched once by search api
        :return: if successfully migrate
        """
        origin_data_list = self.es_from_client.search(from_index, from_type, dsl_command)
        for data in origin_data_list:
            result = self.es_to_client.index(data['_source'], to_index, to_type)
            if not result:
                print ('index data error: %s' % data)
                return False

        return True

    def tiny_migrate_by_id(self, ids, from_index, to_index, from_type=None, to_type=None):
        """
        applicable for a number of certain ids which can be fetched by get api        
        :return: if successfully migrate
        """
        success_count = 0
        migrate_data_list, migrate_data_id_list = self.es_from_client.multi_get(ids, from_index, from_type)

        migrate_data_list, migrate_data_id_list = self.transform(migrate_data_list)

        id_data_dict = zip(migrate_data_id_list, migrate_data_list)

        print ('total count = %d' % id_data_dict.__len__())

        for (doc_id, data) in id_data_dict:
            result = self.es_to_client.index(data, to_index, to_type, doc_id)
            result_dict = dict(result)
            if result_dict['created'] or result_dict['_version'] > 1:
                success_count += 1
                continue
            else:
                print ('index data error: %s' % data)
                print ('%s docs has been successfully migrated' % success_count)
                return False, success_count

        print ('all docs ( %s ) has been successfully migrated' % success_count)
        return True, success_count

    def huge_migrate(self, from_index, to_index, from_type=None, to_type=None):
        """
        
        :param from_index: 
        :param to_index: 
        :param from_type: 
        :param to_type: 
        :return: 
        """
        return True

    def transform(self, migrate_data_list):
        trans_data_id_list = []
        trans_data_list = []
        for data in migrate_data_list:
            task_id = data.get('taskId')
            chance_id = 'chance-' + hashlib.md5(task_id.encode('utf-8')).hexdigest()
            trans_data_id_list.append(chance_id)
            data.__setitem__("taskStatus", 2)
            data.__setitem__('chanceId', chance_id)
            trans_data_list.append(data)
        return trans_data_list, trans_data_id_list


if __name__ == '__main__':
    es_migrate_tool = ElasticSearchDataMigrate('http://l-es1.data.p1.11bee.com:9273/',
                                               'http://l-nbdata5.f.dev.cn0.qunar.com:9273/')
    es_migrate_tool.tiny_migrate('nbdata_user_resume_info_idx', 'nbdata_user_resume_info_idx', 'user_resume_info',
                                 'user_resume_info')

    es_request_tool = ElasticSearchWrapperClient('http://l-nbdata5.f.dev.cn0.qunar.com:9273/')
    file_ids = open('/home/zshell/Documents/task_no/test', 'r')
    id_list = file_ids.readlines()
    es_migrate_tool.tiny_migrate_by_id(id_list, 'history_task_info', 'task_info_idx', 'task_info', 'task_info')
