#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
es request command
"""
from es_data_migrate import ElasticSearchDataMigrate
from es_wrapper_client import ElasticSearchWrapperClient

if __name__ == '__main__':
    es_migrate_tool = ElasticSearchDataMigrate('http://xxx.com:9273/',
                                               'http://xxx.com:9273/')

    es_migrate_tool.tiny_migrate('recording_analysis_idx', 'recording_analysis_idx_v1', 'recording_analysis',
                                 'recording_analysis', '{\"query\": {\"match_all\": {}}, \"size\": 10000}')

    # file_ids_1 = open('/home/zshell/Desktop/task_no/no_taskout.txt', 'r')
    # file_ids_2 = open('/home/zshell/Desktop/task_no/no_secondtest.txt', 'r')
    # file_ids_3 = open('/home/zshell/Desktop/task_no/no_out_2.txt', 'r')
    # file_ids_4 = open('/home/zshell/Desktop/task_no/no_out.txt', 'r')
    # file_ids_5 = open('/home/zshell/Desktop/task_no/no_firsttext.txt', 'r')
    # # file_ids_6 = open('/home/zshell/Desktop/task_no/firsttesttwo.txt', 'r')
    # file_ids_7 = open('/home/zshell/Desktop/task_no/no_all_out.txt', 'r')
    #
    # no_file_ids = open('/home/zshell/Desktop/task_no/no_2.txt', 'w')
    #
    # try:
    #     es_migrate_tool = ElasticSearchDataMigrate('http://xxx.com:9273/')
    #     es_request_tool = ElasticSearchWrapperClient('http://xxx.com:9222/')
    #     pure_id_list = []
    #     id_list = file_ids_1.readlines()
    #     for doc_id in id_list:
    #         if doc_id.endswith('\r\n'):
    #             doc_id = doc_id.strip('\r\n')
    #         if doc_id.endswith('\n'):
    #             doc_id = doc_id.strip('\n')
    #         pure_id_list.append(doc_id)
    #
    #     id_list = file_ids_2.readlines()
    #     for doc_id in id_list:
    #         if doc_id.endswith('\r\n'):
    #             doc_id = doc_id.strip('\r\n')
    #         if doc_id.endswith('\n'):
    #             doc_id = doc_id.strip('\n')
    #         pure_id_list.append(doc_id)
    #
    #     id_list = file_ids_3.readlines()
    #     for doc_id in id_list:
    #         if doc_id.endswith('\r\n'):
    #             doc_id = doc_id.strip('\r\n')
    #         if doc_id.endswith('\n'):
    #             doc_id = doc_id.strip('\n')
    #         pure_id_list.append(doc_id)
    #
    #     id_list = file_ids_4.readlines()
    #     for doc_id in id_list:
    #         if doc_id.endswith('\r\n'):
    #             doc_id = doc_id.strip('\r\n')
    #         if doc_id.endswith('\n'):
    #             doc_id = doc_id.strip('\n')
    #         pure_id_list.append(doc_id)
    #
    #     id_list = file_ids_5.readlines()
    #     for doc_id in id_list:
    #         if doc_id.endswith('\r\n'):
    #             doc_id = doc_id.strip('\r\n')
    #         if doc_id.endswith('\n'):
    #             doc_id = doc_id.strip('\n')
    #         pure_id_list.append(doc_id)
    #
    #     # id_list = file_ids_6.readlines()
    #     # for doc_id in id_list:
    #     #     if doc_id.endswith('\r\n'):
    #     #         doc_id = doc_id.strip('\r\n')
    #     #     if doc_id.endswith('\n'):
    #     #         doc_id = doc_id.strip('\n')
    #     #     pure_id_list.append(doc_id)
    #
    #     id_list = file_ids_7.readlines()
    #     for doc_id in id_list:
    #         if doc_id.endswith('\r\n'):
    #             doc_id = doc_id.strip('\r\n')
    #         if doc_id.endswith('\n'):
    #             doc_id = doc_id.strip('\n')
    #         pure_id_list.append(doc_id)
    #
    #     # id_dict = {}
    #     # for doc_id in pure_id_list:
    #     #     if doc_id in id_dict:
    #     #         count = id_dict.get(doc_id)
    #     #         count += 1
    #     #         id_dict.__setitem__(doc_id, count)
    #     #     else:
    #     #         id_dict.__setitem__(doc_id, 1)
    #     #
    #     # dup = 0
    #     # for item in id_dict.iterkeys():
    #     #     if id_dict[item] > 2:
    #     #         dup += 1
    #     #
    #     # print (dup)
    #
    #     # for doc_id in pure_id_list:
    #     #     query_dsl = '{\"query\": {\"bool\": {\"must\": [{\"term\": {\"taskId\": \"%s\"}}]}}}' % doc_id
    #     #     res = es_request_tool.search('task_info_idx', 'task_info', query_dsl)
    #     #     if res:
    #     #         print res
    #     #         if res.__len__() == 1:
    #     #             chanceId = res[0].get('_id')
    #     #             result = es_request_tool.delete('task_info_idx', 'task_info', chanceId)
    #     #             if not result:
    #     #                 print ('delete fail %s' % chanceId)
    #
    #     no_task_ids = []
    #
    #     for doc_id in pure_id_list:
    #         res = es_request_tool.get('task_info_idx_backup', doc_id, 'task_info')
    #         if not res:
    #             no_task_ids.append(doc_id)
    #
    #     for doc_id in no_task_ids:
    #         no_file_ids.write(doc_id + '\n')
    #         # task_ids.append(res.get('chanceId'))
    #
    # # for doc_id in task_ids:
    # #     res = es_request_tool.get('task_info_idx', doc_id, 'task_info')
    # #     if not res:
    # #         print (doc_id + '\n')
    #
    # # tmp_list = []
    # # all_count = 0
    # # for i in xrange(0, pure_id_list.__len__(), 1):
    # #     if tmp_list.__len__() >= 500 or i == pure_id_list.__len__() - 1:
    # #         result, count = es_migrate_tool.tiny_migrate_by_id(tmp_list, 'history_task_info', 'task_info_idx',
    # #                                                            'task_info', 'task_info')
    # #         all_count += count
    # #         tmp_list = []
    # #
    # #     tmp_list.append(pure_id_list[i])
    # # print ('all all count = %d' % all_count)
    #
    # finally:
    #     file_ids_1.close()
    #     no_file_ids.close()
    #     file_ids_2.close()
    #     file_ids_3.close()
    #     file_ids_4.close()
    #     file_ids_5.close()
    #     # file_ids_6.close()
    #     file_ids_7.close()
