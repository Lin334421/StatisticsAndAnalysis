# -*-coding:utf-8-*-
import csv
import json

csv.field_size_limit(500 * 1024 * 1024)
import os

import clickhouse_connect

privileged_events_list = ["added_to_project", "converted_note_to_issue",
                          "deployed", "deployment_environment_changed",
                          "locked", "merged", "moved_columns_in_project",
                          "pinned", "removed_from_project",
                          "review_dismissed", "transferred",
                          "unlocked", "unpinned", "user_blocked"]


def get_github_issues(repo_name):
    client = clickhouse_connect.get_client(host='123.57.177.158',
                                           port=28800,
                                           username='querydata',
                                           password='querydata@huawei123!'
                                           )

    event_filename = "../repo_events/" + repo_name + ".csv"
    if os.path.exists(event_filename):
        print(repo_name + " existed")
    query_str = f"SELECT * FROM github_issues_timeline WHERE search_key__repo = '{repo_name}'"
    results = client.query(query_str).result_set

    with open(event_filename, 'w', encoding='utf-8') as f1:
        csv_writer = csv.writer(f1)
        for row in results:
            csv_writer.writerow(row)
        f1.close()


def identify_key_dev(repo_name):
    key_dev_list = []
    event_filename = "../repo_events/" + repo_name + ".csv"
    with open(event_filename, 'r', encoding='utf-8') as f1:
        csv_reader = csv.reader((line.replace('\0', '') for line in f1))
        for event in csv_reader:
            event_type = event[3]
            if event_type in privileged_events_list:
                timeline_raw = event[6]
                raw_data = json.loads(timeline_raw)
                dev = raw_data['actor']
                dev_name = dev['login']
                key_dev_list.append(dev_name)
    key_dev_list = set(key_dev_list)
    dev_filename = repo_name + '_key_dev_by_events.csv'
    with open(dev_filename, 'w', encoding='utf-8', newline='') as f2:
        csv_writer = csv.writer(f2)
        for key_dev in key_dev_list:
            csv_writer.writerow([key_dev])


if __name__ == '__main__':
    repo_name = 'node'
    identify_key_dev(repo_name)
