# -*-coding:utf-8-*-
import io

from clickhouse_driver import Client, connect
from flask import send_file


class CKClient:
    def __init__(self, host, port, user, password, database, settings={}, kwargs={}):
        self.client = Client(host=host, port=port, user=user, password=password, database=database, settings=settings,
                             **kwargs)
        self.connect = connect(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.connect.cursor()

    def execute(self, sql: object, params: list) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params)
        return result

    def execute_use_setting(self, sql: object, params: list, settings) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params, settings=settings)
        return result

    def execute_no_params(self, sql: object):
        result = self.client.execute(sql)
        return result

    def fetchall(self, sql):
        result = self.client.execute(sql)
        return result

    def close(self):
        self.client.disconnect()


def get_ck_client(conn_info):
    # ck_client = CKClient(host='',
    #                      port=0,
    #                      database='',
    #                      password='',
    #                      user='')

    ck_client = CKClient(host=conn_info['HOST'],
                         port=conn_info['PORT'],
                         database=conn_info['DATABASE'],
                         password=conn_info['PASSWD'],
                         user=conn_info['USER'])

    return ck_client


conn_infos = {
    "jump_10090": {
        "HOST": "123.57.177.158",
        "PORT": 10090,
        "USER": "default",
        "PASSWD": "default",
        "DATABASE": "default",
        "DESCRIPTION": "rust"
    },
    "local_9000": {
        "HOST": "192.168.8.150",
        "PORT": 9000,
        "USER": "default",
        "PASSWD": "default@HUAWEI!@#123",
        "DATABASE": "default",
        "DESCRIPTION": "本地集群"
    },

    "jump_29900": {
        "HOST": "123.57.177.158",
        "PORT": 29900,
        "USER": "default",
        "PASSWD": "default@HUAWEI!@#123",
        "DATABASE": "default",
        "DESCRIPTION": "本地集群"
    }
}


# 通过参数端口推断数据库的连接信息
def get_ck_conn_info(port_info):
    return conn_infos.get(port_info)


def mk_file_buffer(df, file_name):
    buffer = io.BytesIO()
    df.to_csv(buffer, index=False, encoding='utf-8')
    buffer.seek(0)
    # 发送CSV文件给客户端
    return send_file(
        buffer,
        as_attachment=True,
        download_name=file_name,
        mimetype='text/csv'
    )


def execute_sql(conn_info, sql_query):
    ck_cli = get_ck_client(conn_info)

    df = ck_cli.client.query_dataframe(sql_query)

    ck_cli.close()
    return df


def write_to_csv_buffer(df, new_row):
    # print(new_row)
    df.loc[len(df.index)] = new_row
    # df.append(new_row, ignore_index=True)


def parse_params(request_args):
    top_n = request_args.get('top_n')
    start_month = request_args.get('start_month')
    end_month = request_args.get('end_month')
    owner = request_args.get('owner')
    modified_proportion = request_args.get('modified_proportion')
    if start_month > end_month:
        return {"Error": "start_month>end_month"}
    if not top_n:
        top_n = 9999999
    if int(top_n) <= 0:
        return {"Error": "top_n<=0"}
    return top_n, start_month, end_month, owner, modified_proportion


def parse_conn_info(request_args):
    dev_info = request_args.get('dev_info')
    conn_info = get_ck_conn_info(dev_info)
    return conn_info
