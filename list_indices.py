#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
list indices by availability_set conditions
"""
import requests
import json


def list_indices(es_url):
    if not es_url.endswith('/'):
        es_url = es_url + '/'
    response = requests.get(es_url + '_cat/indices/*?h=index')
    result = response.text
    indices_list_unicode = result.split('\n')
    indices_list = []
    for index in indices_list_unicode:
        index_str = index.encode('unicode-escape').decode('string_escape')
        indices_list.append(index_str.strip())
    return indices_list


def fetch_index_as(es_url, index_name):
    """
    fetch availability set of the given index    
    """
    if not es_url.endswith('/'):
        es_url = es_url + '/'
    response = requests.get(es_url + index_name)
    index_settings = json.loads(response.text)
    result = index_settings[index_name]['settings']['index']['routing']['allocation']['require']['AvailabilitySet']
    return result


def list_indices_of_given_as(es_url, availability_set):
    indices_list = list_indices(es_url)
    result = []
    for index in indices_list:
        try:
            as_property = fetch_index_as(es_url, index)
            if cmp(str(as_property), availability_set) == 0:
                result.append(index)
        except Exception, e:
            print ('error index is: %s, %s', index, e)
    return result


if __name__ == '__main__':
    indices_list = list_indices_of_given_as('http://xxx.com:9222/', 'offline')
    for index in indices_list:
        print (index + '\n')
