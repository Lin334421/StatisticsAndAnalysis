# -*-coding:utf-8-*-
from flask import Blueprint, request

from commit_dir import get_chinese_repo_contributed_file_modified, get_topn_active_repo_recent_years, \
    get_top_n_chinese_commit_histroy, get_topn_active_repo_chinese_recent_years_commit, \
    get_top_n_modified_code_file_developer_commit, get_topn_active_repo_chinese_recent_years_commit_strict
from util import mk_file_buffer, parse_params, parse_conn_info

repo_bp = Blueprint('repo_contributions', __name__)


# 中国开发者在项目目录中的贡献情况
# 需要指定项目和目录


@repo_bp.route("/chinese_contributed_repo_files")
def chinese_commit_in_repo_dir():
    owner = request.args.get('owner')
    repo = request.args.get('repo')
    conn_info = parse_conn_info(request_args=request.args)
    df, file_name = get_chinese_repo_contributed_file_modified(owner_repo=(owner, repo), conn_info=conn_info)
    return mk_file_buffer(df=df, file_name=file_name)


@repo_bp.route("/topn_repo_list_recent_years_in_database")
def topn_repos():
    top_n = request.args.get('top_n')
    start_month = request.args.get('start_month')
    end_month = request.args.get('end_month')
    if start_month > end_month:
        return {"Error": "start_month>end_month"}
    if int(top_n) <= 0:
        return {"Error": "top_n<=0"}
    conn_info = parse_conn_info(request_args=request.args)
    df, file_name = get_topn_active_repo_recent_years(top_n, start_month, end_month, conn_info)
    return mk_file_buffer(df=df, file_name=file_name)


@repo_bp.route("/topn_chinese_commit_history")
def topn_chinese_commits_list():
    top_n, start_month, end_month, owner, modified_proportion = parse_params(request.args)
    conn_info = parse_conn_info(request_args=request.args)
    df, file_name = get_top_n_chinese_commit_histroy(top_n, start_month, end_month, conn_info, owner)
    return mk_file_buffer(df=df, file_name=file_name)


@repo_bp.route("/topn_chinese_active_commit_repos")
def topn_chinese_active_commit_repos():
    top_n, start_month, end_month, owner, modified_proportion = parse_params(request.args)
    conn_info = parse_conn_info(request_args=request.args)
    df, file_name = get_topn_active_repo_chinese_recent_years_commit(top_n, start_month, end_month, conn_info, owner)
    return mk_file_buffer(df=df, file_name=file_name)


@repo_bp.route("/modified_code_file_developer")
def get_modified_code_file_developer():
    _, start_month, end_month, owner, modified_proportion = parse_params(request.args)
    conn_info = parse_conn_info(request_args=request.args)
    df, file_name = get_top_n_modified_code_file_developer_commit(start_month, end_month, conn_info, owner,
                                                                  modified_proportion)
    return mk_file_buffer(df=df, file_name=file_name)


@repo_bp.route("/topn_chinese_active_commit_repos_strict")
def topn_chinese_active_commit_repos_strict():
    top_n, start_month, end_month, owner, modified_proportion = parse_params(request.args)
    conn_info = parse_conn_info(request_args=request.args)
    df, file_name = get_topn_active_repo_chinese_recent_years_commit_strict(top_n, start_month, end_month, conn_info, owner)
    return mk_file_buffer(df=df, file_name=file_name)
