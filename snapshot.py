#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
es snapshot settings helper
"""
import argparse

CURATOR_CONTENT = '''---
actions:
  1:
    action: snapshot
    description: snapshot per day
    options:
      repository: backup
      name: %s-%%Y%%m%%d
      ignore_unavailable: False
      include_global_state: True
      partial: False
      wait_for_completion: True
      skip_repo_fs_check: False
      timeout_override:
      continue_if_exception: False
      disable_action: False
    filters:
    - filtertype: pattern
      kind: prefix
      value: %s
      exclude:

'''


def snapshot_per_day(index):
    print ('snapshot per day, index = %s' % index)
    target_file = args.base_dir + 'snapshot-per-day/' + index + '.yml'
    snapshot_file = open(target_file, 'w')
    try:
        data = CURATOR_CONTENT % (index, index)
        snapshot_file.write(data)
    finally:
        snapshot_file.close()


def snapshot_per_week(index):
    target_file = args.base_dir + 'snapshot-per-week/' + index + '.yml'
    snapshot_file = open(target_file, 'w')
    try:
        data = CURATOR_CONTENT % (index, index)
        snapshot_file.write(data)
    finally:
        snapshot_file.close()


def snapshot_at_once(index):
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='es snapshot settings helper')
    parser.add_argument('-i', '--index', type=str, required=True, help='index to backup')
    parser.add_argument('-l', '--backup-level', type=str, default='day', help='backup level, once, day or week')
    parser.add_argument('-d', '--base-dir', type=str, default='/home/q/elasticsearch/config/curator/actions/',
                        help='base snapshot config dir')

    args = parser.parse_args()

    switcher = {
        'once': snapshot_at_once,
        'day': snapshot_per_day,
        'week': snapshot_per_week
    }

    func = switcher[args.backup_level]
    func(args.index)
