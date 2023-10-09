# -*-coding:utf-8-*-
from flask import Blueprint, request

from commit_dir import do_optimize_table
from util import get_ck_conn_info

ck_bp = Blueprint('clickhouse', __name__)


@ck_bp.route('/optimize_tables')
def optimize_tables():
    table_names = request.args.get("table_names").split(',')
    dev_info = request.args.get('dev_info')
    conn_info = get_ck_conn_info(dev_info)
    try:
        do_optimize_table(table_names=table_names, conn_info=conn_info)
        return {"state": "successful"}
    except Exception as e:
        return {"state": "failed"}