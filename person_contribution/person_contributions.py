# -*-coding:utf-8-*-
import io

from flask import Blueprint, request, send_file

from commit_dir import get_contributor_modified_files, get_dir_contributed_percentage, \
    get_dir_company_contributed_percentage, get_dir_email_domain_contributed_percentage, \
    repo_top_n_modify_files_sql_execute, get_code_reviewed_events
from util import get_ck_conn_info, mk_file_buffer

person_bp = Blueprint('person_contributions', __name__)


# 指定数据库地址，用户和密码 端口 数据库


# 指定 owner repo login 数据库设备信息
@person_bp.route("/contributor_git_modified_files")
def get_contributor_git_modified_files():
    owner = request.args.get('owner')
    repo = request.args.get('repo')
    login = request.args.get('login')
    dev_info = request.args.get('dev_info')
    conn_info = get_ck_conn_info(dev_info)
    df, file_name = get_contributor_modified_files(owner_repo=(owner, repo), login=login, conn_info=conn_info)
    return mk_file_buffer(df=df, file_name=file_name)

    # 执行sql 返回csv


@person_bp.route("/contributor_git_dir_contributed_percentage")
def get_contributor_git_dir_contributed_percentage():
    owner = request.args.get('owner')
    repo = request.args.get('repo')
    login = request.args.get('login')
    dev_info = request.args.get('dev_info')
    dir_level = request.args.get('dir_level')
    conn_info = get_ck_conn_info(dev_info)
    df, file_name = get_dir_contributed_percentage(owner_repo=(owner, repo),
                                                   login=login,
                                                   conn_info=conn_info,
                                                   dir_levels=dir_level)
    return mk_file_buffer(df=df, file_name=file_name)


@person_bp.route("/company_git_dir_contributed_percentage")
def get_company_git_dir_contributed_percentage():
    owner = request.args.get('owner')
    repo = request.args.get('repo')
    company = request.args.get('company')
    dev_info = request.args.get('dev_info')
    dir_level = request.args.get('dir_level')
    conn_info = get_ck_conn_info(dev_info)
    df, file_name = get_dir_company_contributed_percentage(owner_repo=(owner, repo),
                                                           company=company,
                                                           conn_info=conn_info,
                                                           dir_levels=dir_level)

    return mk_file_buffer(df=df, file_name=file_name)


@person_bp.route("/email_domain_gits_dir_contributed_percentage")
def get_email_domain_gits_dir_contributed_percentage():
    owner = request.args.get('owner')
    repo = request.args.get('repo')
    email_domain = request.args.get('email_domain')
    dev_info = request.args.get('dev_info')
    dir_level = request.args.get('dir_level')
    conn_info = get_ck_conn_info(dev_info)
    df, file_name = get_dir_email_domain_contributed_percentage(owner_repo=(owner, repo),
                                                                email_domain=email_domain,
                                                                conn_info=conn_info,
                                                                dir_levels=dir_level)

    return mk_file_buffer(df=df, file_name=file_name)


# 筛选出了修改程序文件的行数
@person_bp.route("/repo_top_n_modified_files_lines_count")
def get_repo_top_n_modify_files():
    dev_info = request.args.get('dev_info')
    conn_info = get_ck_conn_info(dev_info)
    df, file_name = repo_top_n_modify_files_sql_execute(conn_info=conn_info)
    return mk_file_buffer(df=df, file_name=file_name)




@person_bp.route('/code_reviewed_events', methods=['GET'])
def code_reviewed_events():
    # 暂时无需提供owner repo
    dev_info = request.args.get('dev_info')
    conn_info = get_ck_conn_info(dev_info)
    df, file_name = get_code_reviewed_events(conn_info=conn_info)