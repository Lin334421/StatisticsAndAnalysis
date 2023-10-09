# -*-coding:utf-8-*-
import pandas as pd
from clickhouse_driver import Client, connect

from util import write_to_csv_buffer, execute_sql, get_ck_client

"""pip install clickhouse_driver"""








def get_contributor_modified_files(owner_repo, login, conn_info):
    owner, repo = owner_repo
    sql_ = f"""
with '{owner}' as owner, '{repo}' as repo, '{login}' as login
select b.new_login, a.*
from (select search_key__owner,search_key__repo,id,
             file_name,
             sum(insertions) as sum_insertions,
             sum(deletions)  as sum_deletions,
             sum(lines)      as sum_lines,
             count()         as modified_file_count
      from (select a.*, b.id
            from (select search_key__owner, search_key__repo, author_email, file_name, insertions, deletions, lines
                  from gits
                      array join
                       `files.file_name` as file_name,
                       `files.insertions` as insertions,
                       `files.deletions` as deletions,
                       `files.lines` as lines
                  where search_key__owner = owner
                    and search_key__repo = repo
                    and length(parents) = 1) as a global
                     join (select author__id as id, commit__author__email
                           from github_commits
                           where search_key__owner = owner
                             and search_key__repo = repo
                             and id != 0
                           group by author__id, commit__author__email) as b on a.author_email = b.commit__author__email)
      group by search_key__owner, search_key__repo, id, file_name
      order by search_key__owner,search_key__repo,id, file_name) as a global
         join (select author__id, argMax(author__login, commit__author__date) as new_login
               from github_commits
               where author__login = login
               group by author__id
    ) as b on a.id = b.author__id
"""

    ck_cli = get_ck_client(conn_info)

    df = ck_cli.client.query_dataframe(sql_)

    ck_cli.close()
    return df, f'./{login}_{owner}_{repo}_contributed_dir_info.csv'


def get_chinese_repo_contributed_file_modified(owner_repo, conn_info):
    owner, repo = owner_repo
    if repo:
        sql_ = f"""
        with '{owner}' as owner, '{repo}' as repo
    select b.new_login, a.*
    from (select search_key__owner,search_key__repo,id,
                 file_name,
                 sum(insertions) as sum_insertions,
                 sum(deletions)  as sum_deletions,
                 sum(lines)      as sum_lines,
                 count()         as modified_file_count
          from (select a.*, b.id
                from (select search_key__owner, search_key__repo, author_email, file_name, insertions, deletions, lines
                      from gits
                          array join
                           `files.file_name` as file_name,
                           `files.insertions` as insertions,
                           `files.deletions` as deletions,
                           `files.lines` as lines
                      where search_key__owner = owner
                        and search_key__repo = repo
                        and length(parents) = 1) as a global
                         join (select author__id as id, commit__author__email
                               from github_commits
                               where search_key__owner = owner
                                 and search_key__repo = repo
                                 and id != 0
                               group by author__id, commit__author__email) as b on a.author_email = b.commit__author__email)
          group by search_key__owner, search_key__repo, id, file_name
          order by search_key__owner,search_key__repo,id, file_name) as a global
             join (select author__id, argMax(author__login, commit__author__date) as new_login
                   from github_commits where author__id !=0 and author__id global in (-- 与中国沾边的id
                                                          select author__id
                                                          from (select a.*, b.inferred_from_location__country
                                                                from (select a.*, b.author__id
                                                                      from (select *, splitByChar('@', author_email)[2] as email_domain
                                                                            from gits
                                                                            where length(parents) == 1) as a global
                                                                               join (select commit__author__email, author__id
                                                                                     from github_commits
                                                                                     where author__id != 0
                                                                                     group by commit__author__email, author__id) as b
                                                                                    on
                                                                                        a.author_email = b.commit__author__email) as a global
                                                                         left join (select id, inferred_from_location__country, location
                                                                                    from github_profile
                                                                                    where github_profile.inferred_from_location__country = 'China') as b
                                                                                   on a.author__id = b.id)
                                                          where inferred_from_location__country != ''
                                                             or author_tz = 8
                                                             or email_domain global in ('baidu.com',
                                                                                        '163.com',
                                                                                        '126.com',
                                                                                        'qq.com',
                                                                                        'bytedance.com',
                                                                                        'huawei.com',
                                                                                        'xiaomi.com',
                                                                                        'alibaba-inc.com',
                                                                                        'linux.alibaba.com',
                                                                                        'alibaba.com',
                                                                                        'bytedance.com',
                                                                                        'streamcomputing.com',
                                                                                        'loongson.cn',
                                                                                        'iscas.ac.cn',
                                                                                        'tencent.com')
                                                             or email_domain like '%.cn'
    
                                                          group by author__id)
                   group by author__id
        ) as b on a.id = b.author__id
        
        """
    else:
        sql_ = f"""
with '{owner}' as owner
    select b.new_login, a.*
    from (select search_key__owner,search_key__repo,id,
                 file_name,
                 sum(insertions) as sum_insertions,
                 sum(deletions)  as sum_deletions,
                 sum(lines)      as sum_lines,
                 count()         as modified_file_count
          from (select a.*, b.id
                from (select search_key__owner,search_key__repo,author_email, file_name, insertions, deletions, lines
                      from gits
                          array join
                           `files.file_name` as file_name,
                           `files.insertions` as insertions,
                           `files.deletions` as deletions,
                           `files.lines` as lines
                      where search_key__owner = owner
                        and length(parents) = 1) as a global
                         join (select author__id as id, commit__author__email
                               from github_commits
                               where search_key__owner = owner
                                 and id != 0
                               group by author__id, commit__author__email) as b on a.author_email = b.commit__author__email)
          group by search_key__owner,search_key__repo, id, file_name
          order by search_key__owner,search_key__repo,id, file_name) as a global
             join (select author__id, argMax(author__login, commit__author__date) as new_login
                   from github_commits where author__id !=0 and search_key__owner = owner and author__id global in (-- 与中国沾边的id
                                                          select author__id
                                                          from (select a.*, b.inferred_from_location__country
                                                                from (select a.*, b.author__id
                                                                      from (select *, splitByChar('@', author_email)[2] as email_domain
                                                                            from gits
                                                                            where length(parents) == 1) as a global
                                                                               join (select commit__author__email, author__id
                                                                                     from github_commits
                                                                                     where author__id != 0
                                                                                     group by commit__author__email, author__id) as b
                                                                                    on
                                                                                        a.author_email = b.commit__author__email) as a global
                                                                         left join (select id, inferred_from_location__country, location
                                                                                    from github_profile
                                                                                    where github_profile.inferred_from_location__country = 'China') as b
                                                                                   on a.author__id = b.id)
                                                          where inferred_from_location__country != ''
                                                             or author_tz = 8
                                                             or email_domain global in ('baidu.com',
                                                                                        '163.com',
                                                                                        '126.com',
                                                                                        'qq.com',
                                                                                        'bytedance.com',
                                                                                        'huawei.com',
                                                                                        'xiaomi.com',
                                                                                        'alibaba-inc.com',
                                                                                        'linux.alibaba.com',
                                                                                        'alibaba.com',
                                                                                        'bytedance.com',
                                                                                        'streamcomputing.com',
                                                                                        'loongson.cn',
                                                                                        'iscas.ac.cn',
                                                                                        'tencent.com')
                                                             or email_domain like '%.cn'

                                                          group by author__id)
                   group by author__id
        ) as b on a.id = b.author__id

"""
    ck_cli = get_ck_client(conn_info)

    df = ck_cli.client.query_dataframe(sql_)

    ck_cli.close()
    return (df, f'./{owner}_{repo}_chinese_contributed_files_info.csv') if repo else (
        df, f'./{owner}_chinese_contributed_files_info.csv')


def get_dir_contributed_percentage(owner_repo, login, dir_levels, conn_info):
    # print(owner_repo,login,dir_levels,conn_info)
    owner, repo = owner_repo

    """ compare_with_self_lines_percentage(%) 是占自己所有目录贡献的百分比，compare_with_all_lines_percentage 是占当前目录中所有人的贡献百分比"""
    sql_ = f"""

with '{owner}' as owner, '{repo}' as repo, '{login}' as login, {dir_levels} as dir_levels
select a.search_key__owner,
       a.search_key__repo,
       a.author__logins,
       a.dir,
       file_lines_in_dir,
       `compare_with_self_lines_percentage(%)`,
       round(file_lines_in_dir / total_file_lines_in_dir * 100, 3)      as `compare_with_all_lines_percentage(%)`,
       file_insertions_in_dir,
       `compare_with_self_insertions_percentage(%)`,
       round(file_insertions_in_dir / total_file_insertions_in_dir * 100, 3) as `compare_with_all_insertions_percentage(%)`,
       file_deletions_in_dir,
       `compare_with_self_deletions_percentage(%)`,
       round(file_deletions_in_dir / total_file_deletions_in_dir * 100, 3)  as `compare_with_all_deletions_percentage(%)`,
       dir_level
from (select a.search_key__owner,
             a.search_key__repo,
             a.author__logins,
             dir,
             file_lines_in_dir,
             round(file_lines_in_dir / total_commit_lines * 100, 2)           as `compare_with_self_lines_percentage(%)`,
             file_insertions_in_dir,
             round(file_insertions_in_dir / total_commit_insertions * 100, 2) as `compare_with_self_insertions_percentage(%)`,
             file_deletions_in_dir,
             round(file_deletions_in_dir / total_commit_deletions * 100, 2)   as `compare_with_self_deletions_percentage(%)`,
             length(splitByChar('/', dir)) - 1                                as dir_level
      from (select search_key__owner,
                   search_key__repo,
                   author__logins,
                   dir,
                   sum(file_lines) as   file_lines_in_dir,
                   sum(file_insertions) file_insertions_in_dir,
                   sum(file_deletions)  file_deletions_in_dir,
                   count()         as   commit_count_in_dir
            from dir_label_new_test_v2
            where search_key__owner = owner
              and search_key__repo = repo
              and author__logins = login
            group by search_key__owner, search_key__repo, dir, author__logins
            order by file_lines_in_dir desc) as a global
               join (select search_key__owner,
                            search_key__repo,
                            author__logins,
                            sum(commit_lines)      as total_commit_lines,
                            sum(commit_insertions) as total_commit_insertions,
                            sum(commit_deletions)  as total_commit_deletions,
                            count()                as total_commit_count
                     from (select search_key__owner,
                                  search_key__repo,
                                  author__logins,
                                  hexsha,
                                  commit_lines,
                                  commit_insertions,
                                  commit_deletions,
                                  commit_modified_file_count
                           from dir_label_new_test_v2
                           where search_key__owner = owner
                             and search_key__repo = repo
                             and author__logins = login
                           group by search_key__owner, search_key__repo, author__logins, hexsha, commit_lines,
                                    commit_insertions,
                                    commit_deletions,
                                    commit_modified_file_count
                              )
                     group by search_key__owner, search_key__repo, author__logins) as b
                    on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and
                       a.author__logins = b.author__logins) as a global
         join (select search_key__owner,
                      search_key__repo,
                      dir,
                      sum(file_lines) as   total_file_lines_in_dir,
                      sum(file_insertions) total_file_insertions_in_dir,
                      sum(file_deletions)  total_file_deletions_in_dir
               from dir_label_new_test_v2
               where search_key__owner = owner
                 and search_key__repo = repo
               group by search_key__owner, search_key__repo, dir) as b
              on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and a.dir = b.dir
where dir_level in dir_levels

"""
    # print(sql_)
    ck_cli = get_ck_client(conn_info)
    df = ck_cli.client.query_dataframe(sql_)
    # print(df)
    # df.to_csv(f'./{login}_{owner}_{repo}_contributed_dir_percentage.csv')
    ck_cli.close()
    return df, f'./{login}_{owner}_{repo}_contributed_dir_percentage.csv'


def get_dir_company_contributed_percentage(owner_repo, company, dir_levels, conn_info):
    owner, repo = owner_repo
    sql_ = f"""
       with '{owner}' as owner, '{repo}' as repo, '{company}' as company, {dir_levels} as dir_levels
        select a.search_key__owner,
       a.search_key__repo,
       a.author_company,
       a.dir,
       file_lines_in_dir,
       `compare_with_self_lines_percentage(%)`,
       round(file_lines_in_dir / total_file_lines_in_dir * 100, 3)           as `compare_with_all_lines_percentage(%)`,
       file_insertions_in_dir,
       `compare_with_self_insertions_percentage(%)`,
       round(file_insertions_in_dir / total_file_insertions_in_dir * 100, 3) as `compare_with_all_insertions_percentage(%)`,
       file_deletions_in_dir,
       `compare_with_self_deletions_percentage(%)`,
       round(file_deletions_in_dir / total_file_deletions_in_dir * 100, 3)   as `compare_with_all_deletions_percentage(%)`,
       dir_level
from (select a.search_key__owner,
             a.search_key__repo,
             a.author_company,
             dir,
             file_lines_in_dir,
             round(file_lines_in_dir / total_commit_lines * 100, 2)           as `compare_with_self_lines_percentage(%)`,
             file_insertions_in_dir,
             round(file_insertions_in_dir / total_commit_insertions * 100, 2) as `compare_with_self_insertions_percentage(%)`,
             file_deletions_in_dir,
             round(file_deletions_in_dir / total_commit_deletions * 100, 2)   as `compare_with_self_deletions_percentage(%)`,
             length(splitByChar('/', dir)) - 1                                as dir_level
      from (select search_key__owner,
                   search_key__repo,
                   author_company,
                   dir,
                   sum(file_lines) as   file_lines_in_dir,
                   sum(file_insertions) file_insertions_in_dir,
                   sum(file_deletions)  file_deletions_in_dir,
                   count()         as   commit_count_in_dir
            from dir_label_new_test_v2
            where search_key__owner = owner
              and search_key__repo = repo
              and author_company = company
            group by search_key__owner, search_key__repo, dir, author_company
            order by file_lines_in_dir desc) as a global
               join (select search_key__owner,
                            search_key__repo,
                            author_company,
                            sum(commit_lines)      as total_commit_lines,
                            sum(commit_insertions) as total_commit_insertions,
                            sum(commit_deletions)  as total_commit_deletions,
                            count()                as total_commit_count
                     from (select search_key__owner,
                                  search_key__repo,
                                  author_company,
                                  hexsha,
                                  commit_lines,
                                  commit_insertions,
                                  commit_deletions,
                                  commit_modified_file_count
                           from dir_label_new_test_v2
                           where search_key__owner = owner
                             and search_key__repo = repo
                             and author_company = company
                           group by search_key__owner, search_key__repo, author_company, hexsha, commit_lines,
                                    commit_insertions,
                                    commit_deletions,
                                    commit_modified_file_count
                              )
                     group by search_key__owner, search_key__repo, author_company) as b
                    on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and
                       a.author_company = b.author_company) as a global
         join (select search_key__owner,
                      search_key__repo,
                      dir,
                      sum(file_lines) as   total_file_lines_in_dir,
                      sum(file_insertions) total_file_insertions_in_dir,
                      sum(file_deletions)  total_file_deletions_in_dir
               from dir_label_new_test_v2
               where search_key__owner = owner
                 and search_key__repo = repo
               group by search_key__owner, search_key__repo, dir) as b
              on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and a.dir = b.dir
where dir_level in dir_levels

    """
    ck_cli = get_ck_client(conn_info=conn_info)

    df = ck_cli.client.query_dataframe(sql_)
    # df.to_csv(f'./{company}_{owner}_{repo}_company_contributed_dir_info.csv')
    ck_cli.close()
    return df, f'./{company}_{owner}_{repo}_company_contributed_dir_info.csv'


def get_dir_email_domain_contributed_percentage(owner_repo, email_domain, dir_levels, conn_info):
    owner, repo = owner_repo
    sql_ = f"""
with '{owner}' as owner, '{repo}' as repo, '{email_domain}' as email_domain, {dir_levels} as dir_levels
select a.search_key__owner,
       a.search_key__repo,
       a.author_email_domain,
       a.dir,
       file_lines_in_dir,
       `compare_with_self_lines_percentage(%)`,
       round(file_lines_in_dir / total_file_lines_in_dir * 100, 3)           as `compare_with_all_lines_percentage(%)`,
       file_insertions_in_dir,
       `compare_with_self_insertions_percentage(%)`,
       round(file_insertions_in_dir / total_file_insertions_in_dir * 100, 3) as `compare_with_all_insertions_percentage(%)`,
       file_deletions_in_dir,
       `compare_with_self_deletions_percentage(%)`,
       round(file_deletions_in_dir / total_file_deletions_in_dir * 100, 3)   as `compare_with_all_deletions_percentage(%)`,
       dir_level
from (select a.search_key__owner,
             a.search_key__repo,
             a.author_email_domain,
             dir,
             file_lines_in_dir,
             round(file_lines_in_dir / total_commit_lines * 100, 2)           as `compare_with_self_lines_percentage(%)`,
             file_insertions_in_dir,
             round(file_insertions_in_dir / total_commit_insertions * 100, 2) as `compare_with_self_insertions_percentage(%)`,
             file_deletions_in_dir,
             round(file_deletions_in_dir / total_commit_deletions * 100, 2)   as `compare_with_self_deletions_percentage(%)`,
             length(splitByChar('/', dir)) - 1                                as dir_level
      from (select search_key__owner,
                   search_key__repo,
                   author_email_domain,
                   dir,
                   sum(file_lines) as   file_lines_in_dir,
                   sum(file_insertions) file_insertions_in_dir,
                   sum(file_deletions)  file_deletions_in_dir,
                   count()         as   commit_count_in_dir
            from dir_label_new_test_v2
            where search_key__owner = owner
              and search_key__repo = repo
              and author_email_domain = email_domain
            group by search_key__owner, search_key__repo, dir, author_email_domain
            order by file_lines_in_dir desc) as a global
               join (select search_key__owner,
                            search_key__repo,
                            author_email_domain,
                            sum(commit_lines)      as total_commit_lines,
                            sum(commit_insertions) as total_commit_insertions,
                            sum(commit_deletions)  as total_commit_deletions,
                            count()                as total_commit_count
                     from (select search_key__owner,
                                  search_key__repo,
                                  author_email_domain,
                                  hexsha,
                                  commit_lines,
                                  commit_insertions,
                                  commit_deletions,
                                  commit_modified_file_count
                           from dir_label_new_test_v2
                           where search_key__owner = owner
                             and search_key__repo = repo
                             and author_email_domain = email_domain
                           group by search_key__owner, search_key__repo, author_email_domain, hexsha, commit_lines,
                                    commit_insertions,
                                    commit_deletions,
                                    commit_modified_file_count
                              )
                     group by search_key__owner, search_key__repo, author_email_domain) as b
                    on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and
                       a.author_email_domain = b.author_email_domain) as a global
         join (select search_key__owner,
                      search_key__repo,
                      dir,
                      sum(file_lines) as   total_file_lines_in_dir,
                      sum(file_insertions) total_file_insertions_in_dir,
                      sum(file_deletions)  total_file_deletions_in_dir
               from dir_label_new_test_v2
               where search_key__owner = owner
                             and search_key__repo = repo
               group by search_key__owner, search_key__repo, dir) as b
              on a.search_key__owner = b.search_key__owner and a.search_key__repo = b.search_key__repo and a.dir = b.dir
where dir_level in dir_levels

    """
    # print(sql_)
    ck_cli = get_ck_client(conn_info)

    df = ck_cli.client.query_dataframe(sql_)
    # print(df)
    # df.to_csv(f'./{email_domain}_{owner}_{repo}_email_domain_contributed_dir_info.csv')
    ck_cli.close()
    return df, f'./{email_domain}_{owner}_{repo}_email_domain_contributed_dir_info.csv'


"""
全量获取不指定repo
"""


def repo_top_n_modify_files_sql_execute(conn_info):
    ck_cli = get_ck_client(conn_info)

    sql_ = """
    select a.*, b.new_author_login
from (select a.*, b.repo_code_insertions, b.repo_code_deletions, b.repo_code_lines
      from (select search_key__owner,
                   search_key__repo,
                   author__id,
                   sum(code_insertions)   total_code_insertion,
                   sum(code_deletions) as total_code_deletions,
                   sum(code_lines)     as total_code_lines
            from (select search_key__owner, search_key__repo, code_insertions, code_deletions, code_lines, b.*
                  from (-- 每个邮箱真正参与代码的修改部分行数
                           select search_key__owner,
                                  search_key__repo,
                                  author_email,
                                  sum(insertions) as code_insertions,
                                  sum(deletions)  as code_deletions,
                                  sum(lines)      as code_lines
                           from (select search_key__owner,
                                        search_key__repo,
                                        author_email,
                                        file_name,
                                        insertions,
                                        deletions,
                                        lines,
                                        splitByChar('.', file_name)[-1] file_extension
                                 from gits array join
                                      `files.file_name` as file_name,
                                      `files.insertions` as insertions,
                                      `files.deletions` as deletions,
                                      `files.lines` as lines
                                 where length(parents) = 1
                                   and file_extension global in
                                       ['py', 'kt', 'yml', 'rpg', 'pl',
                                        'vb', 'pm', 'abap', 'sb', 'c', 'fs', 'yaml', 'dart',
                                        'tcl', 'vhd', 'cs', 'go', 'html',
                                         'd', 'java', 'f', 'lua', 'php', 'rb', 'sh', 'NET', 'ads', 'vbnet', 
                                         'cob', 'jl', 'as', 'js', 'logo', 'sql', 'rpgle', 'adb', 'htm', 'asm', 
                                         'm', 'rs', 'cls', 'Rmd', 'pli', 'swift', 'cbl', 'css', 'f90', 'ts',
                                          'scala', 'groovy', 'fsi', 'hpp', 'vbs', 'h', 'cpp', 'xml', 'sas', 'R']
                                    )
                           group by search_key__owner, search_key__repo, author_email) as a global
                           join (select author__id, commit__author__email
                                 from github_commits
                                 where author__id != 0
                                 group by author__id, commit__author__email) as b
                                on a.author_email = b.commit__author__email)
            group by search_key__owner, search_key__repo, author__id
            order by search_key__owner, search_key__repo, total_code_lines desc) as a global
               left join (select search_key__owner,
                                 search_key__repo,
                                 sum(code_insertions) as repo_code_insertions,
                                 sum(code_deletions)  as repo_code_deletions,
                                 sum(code_lines)      as repo_code_lines
                          from (select search_key__owner,
                                       search_key__repo,
                                       code_insertions,
                                       code_deletions,
                                       code_lines,
                                       b.*
                                from (-- 每个邮箱真正参与代码的修改部分行数
                                         select search_key__owner,
                                                search_key__repo,
                                                author_email,
                                                sum(insertions) as code_insertions,
                                                sum(deletions)  as code_deletions,
                                                sum(lines)      as code_lines
                                         from (select search_key__owner,
                                                      search_key__repo,
                                                      author_email,
                                                      file_name,
                                                      insertions,
                                                      deletions,
                                                      lines,
                                                      splitByChar('.', file_name)[-1] file_extension
                                               from gits array join
                                                    `files.file_name` as file_name,
                                                    `files.insertions` as insertions,
                                                    `files.deletions` as deletions,
                                                    `files.lines` as lines
                                               where length(parents) = 1
                                                 and file_extension global in
                                                     ['py', 'kt', 'yml', 'rpg', 'pl', 'vb', 'pm', 'abap', 'sb',
                                                      'c', 'fs', 'yaml', 'dart', 'tcl', 'vhd', 'cs', 'go', 'html',
                                                       'd', 'java', 'f', 'lua', 'php', 'rb', 'sh', 'NET', 'ads',
                                                        'vbnet', 'cob', 'jl', 'as', 'js', 'logo', 'sql', 'rpgle',
                                                         'adb', 'htm', 'asm', 'm', 'rs', 'cls', 'Rmd', 'pli', 'swift',
                                                          'cbl', 'css', 'f90', 'ts', 'scala', 'groovy', 'fsi', 'hpp', 
                                                          'vbs', 'h', 'cpp', 'xml', 'sas', 'R']
                                                  )
                                         group by search_key__owner, search_key__repo, author_email) as a global
                                         join (select author__id, commit__author__email
                                               from github_commits
                                               where author__id != 0
                                               group by author__id, commit__author__email) as b
                                              on a.author_email = b.commit__author__email)
                          group by search_key__owner, search_key__repo
                          order by repo_code_lines desc) as b
                         on a.search_key__owner = b.search_key__owner and
                            a.search_key__repo = b.search_key__repo) as a global
         left join (select author__id, argMax(author__login, commit__author__date) as new_author_login
                    from (select author__id, author__login, commit__author__date
                          from github_commits
                          where author__id != 0
                            and author__login != ''
                          group by author__id, author__login, commit__author__date
                             )
                    group by author__id) as b on a.author__id = b.author__id
order by search_key__owner, search_key__repo, total_code_lines desc 
    
    """
    results = ck_cli.execute_no_params(sql_)
    repo_map = {}
    # repo:(total,now)
    df = pd.DataFrame(columns=["search_key__owner", "search_key__repo",
                               "author__id",
                               "total_code_insertion",
                               "total_code_deletions",
                               "total_code_lines",
                               "repo_code_insertions",
                               "repo_code_deletions",
                               "repo_code_lines",
                               "new_author_login"])
    for result in results:
        owner = result[0]
        repo = result[1]
        developer_modified_total_lines = result[5]
        repo_value = repo_map.get(owner + '__' + repo)
        if repo_value:
            if developer_modified_total_lines + repo_value[1] >= repo_value[1] * 0.8:
                write_to_csv_buffer(df, result)
                continue
        else:
            repo_map[owner + '__' + repo] = (result[8], developer_modified_total_lines)
        write_to_csv_buffer(df, result)
    return df, "repo_top_n_modified_files_lines_count.csv"


def get_topn_active_repo_recent_years(top_n, start_month, end_month, conn_info):
    sql_ = f"""

with {start_month} as start_month,{end_month} as end_month, {top_n} as top_n
select search_key__owner,
       search_key__repo,
       sum(total__lines)      as total_lines,
       sum(total__insertions) as total_insertions,
       sum(total__deletions)  as total_deletions,
       sum(total__files)      as total_files,
       count()                as total_commit_count,
       sumIf(total__lines,toYYYYMM(authored_date)>=start_month and toYYYYMM(authored_date)<=end_month)      as recent_years_modified_lines,
       sumIf(total__insertions,toYYYYMM(authored_date)>=start_month and toYYYYMM(authored_date)<=end_month) as recent_years_modified_insertions,
       sumIf(total__deletions,toYYYYMM(authored_date)>=start_month and toYYYYMM(authored_date)<=end_month)  as recent_years_modified_deletions,
       sumIf(total__files,toYYYYMM(authored_date)>=start_month and toYYYYMM(authored_date)<=end_month)      as recent_years_modified_files_count,
       countIf(toYYYYMM(authored_date)>=start_month and toYYYYMM(authored_date)<=end_month)                as recent_years_commit_count
from gits
where length(parents) = 1
group by search_key__owner, search_key__repo
order by recent_years_commit_count desc limit top_n

"""

    df = execute_sql(conn_info=conn_info,sql_query=sql_)
    return df, f'top_{top_n}_active_repo_recent_years_start_{start_month}_end_{end_month}.csv'


def get_top_n_chinese_commit_histroy(top_n, start_month, end_month, conn_info, owner):

    if owner:
        sql_ = f"""
        with {start_month} as start_month,{end_month} as end_month, {top_n} as top_n,'{owner}' as owner
select b.new_login,a.* from (select search_key__owner,
       search_key__repo,
       author__id,
       sum(total__lines)                                                                                         as total_lines,
       sum(total__insertions)                                                                                    as total_insertions,
       sum(total__deletions)                                                                                     as total_deletions,
       sum(total__files)                                                                                         as total_files,
       count()                                                                                                   as total_commit_count,
       sumIf(total__lines, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                      end_month)                                 as recent_years_modified_lines,
       sumIf(total__insertions, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                           end_month)                            as recent_years_modified_insertions,
       sumIf(total__deletions, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                          end_month)                             as recent_years_modified_deletions,
       sumIf(total__files, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                      end_month)                                 as recent_years_modified_files_count,
       countIf(toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                          end_month)                                             as recent_years_commit_count
from (select a.*, b.author__id
      from (select * from gits where length(parents) = 1 and search_key__owner = owner) as a global
               join (

                   -- 与中国沾边的author id 和邮箱 指定owner
                   select commit__author__email, author__id
                     from github_commits
                     where author__id != 0
                       and search_key__owner = owner
                       and  author__login not like '%[bot]%'
                       and author__id global in (-- 与中国沾边的id
                         select author__id
                         from (select a.*, b.inferred_from_location__country
                               from (select a.*, b.author__id
                                     from (select *, splitByChar('@', author_email)[2] as email_domain
                                           from gits
                                           where length(parents) == 1) as a global
                                              join (select commit__author__email, author__id
                                                    from github_commits
                                                    where author__id != 0
                                                     and author__login not like '%[bot]%'
                                                    group by commit__author__email, author__id) as b
                                                   on
                                                       a.author_email = b.commit__author__email) as a global
                                        left join (select id, inferred_from_location__country, location
                                                   from github_profile
                                                   where github_profile.inferred_from_location__country = 'China') as b
                                                  on a.author__id = b.id)
                         where inferred_from_location__country != ''
                            or author_tz = 8
                            or email_domain global in ('baidu.com',
                                                       '163.com',
                                                       '126.com',
                                                       'qq.com',
                                                       'bytedance.com',
                                                       'huawei.com',
                                                       'xiaomi.com',
                                                       'alibaba-inc.com',
                                                       'linux.alibaba.com',
                                                       'alibaba.com',
                                                       'bytedance.com',
                                                       'streamcomputing.com',
                                                       'loongson.cn',
                                                       'iscas.ac.cn',
                                                       'tencent.com')
                            or email_domain like '%.cn'

                         group by author__id)
                     group by commit__author__email, author__id) as b on a.author_email = b.commit__author__email)
group by search_key__owner, search_key__repo, author__id order by recent_years_commit_count desc limit top_n) as a global left join (

    -- 与中国沾边的 author id 和 最新的login 这个不要指定 owner或者 repo 多个项目中找最新的login
    select author__id, argMax(author__login, commit__author__date) as new_login
                   from github_commits where author__id !=0 and author__login not like '%[bot]%' and author__id global in (-- 与中国沾边的id
                                                          select author__id
                                                          from (select a.*, b.inferred_from_location__country
                                                                from (select a.*, b.author__id
                                                                      from (select *, splitByChar('@', author_email)[2] as email_domain
                                                                            from gits
                                                                            where length(parents) == 1) as a global
                                                                               join (select commit__author__email, author__id
                                                                                     from github_commits
                                                                                     where author__id != 0 and author__login not like '%[bot]%'
                                                                                     group by commit__author__email, author__id) as b
                                                                                    on
                                                                                        a.author_email = b.commit__author__email) as a global
                                                                         left join (select id, inferred_from_location__country, location
                                                                                    from github_profile
                                                                                    where github_profile.inferred_from_location__country = 'China') as b
                                                                                   on a.author__id = b.id)
                                                          where inferred_from_location__country != ''
                                                             or author_tz = 8
                                                             or email_domain global in ('baidu.com',
                                                                                        '163.com',
                                                                                        '126.com',
                                                                                        'qq.com',
                                                                                        'bytedance.com',
                                                                                        'huawei.com',
                                                                                        'xiaomi.com',
                                                                                        'alibaba-inc.com',
                                                                                        'linux.alibaba.com',
                                                                                        'alibaba.com',
                                                                                        'bytedance.com',
                                                                                        'streamcomputing.com',
                                                                                        'loongson.cn',
                                                                                        'iscas.ac.cn',
                                                                                        'tencent.com')
                                                             or email_domain like '%.cn'

                                                          group by author__id)
                   group by author__id
        ) as b on a.author__id = b.author__id
        """
    else:
        sql_ = f"""
with {start_month} as start_month,{end_month} as end_month, {top_n} as top_n
select b.new_login,a.* from (select search_key__owner,
       search_key__repo,
       author__id,
       sum(total__lines)                                                                                         as total_lines,
       sum(total__insertions)                                                                                    as total_insertions,
       sum(total__deletions)                                                                                     as total_deletions,
       sum(total__files)                                                                                         as total_files,
       count()                                                                                                   as total_commit_count,
       sumIf(total__lines, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                      end_month)                                 as recent_years_modified_lines,
       sumIf(total__insertions, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                           end_month)                            as recent_years_modified_insertions,
       sumIf(total__deletions, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                          end_month)                             as recent_years_modified_deletions,
       sumIf(total__files, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                      end_month)                                 as recent_years_modified_files_count,
       countIf(toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                          end_month)                                             as recent_years_commit_count
from (select a.*, b.author__id
      from (select * from gits where length(parents) = 1) as a global
               join (select commit__author__email, author__id
                     from github_commits
                     where author__id != 0
                       and author__login not like '%[bot]%'
                       and author__id global in (-- 与中国沾边的id
                         select author__id
                         from (select a.*, b.inferred_from_location__country
                               from (select a.*, b.author__id
                                     from (select *, splitByChar('@', author_email)[2] as email_domain
                                           from gits
                                           where length(parents) == 1) as a global
                                              join (select commit__author__email, author__id
                                                    from github_commits
                                                    where author__id != 0
                                                     and author__login not like '%[bot]%'
                                                    group by commit__author__email, author__id) as b
                                                   on
                                                       a.author_email = b.commit__author__email) as a global
                                        left join (select id, inferred_from_location__country, location
                                                   from github_profile
                                                   where github_profile.inferred_from_location__country = 'China') as b
                                                  on a.author__id = b.id)
                         where inferred_from_location__country != ''
                            or author_tz = 8
                            or email_domain global in ('baidu.com',
                                                       '163.com',
                                                       '126.com',
                                                       'qq.com',
                                                       'bytedance.com',
                                                       'huawei.com',
                                                       'xiaomi.com',
                                                       'alibaba-inc.com',
                                                       'linux.alibaba.com',
                                                       'alibaba.com',
                                                       'bytedance.com',
                                                       'streamcomputing.com',
                                                       'loongson.cn',
                                                       'iscas.ac.cn',
                                                       'tencent.com')
                            or email_domain like '%.cn'

                         group by author__id)
                     group by commit__author__email, author__id) as b on a.author_email = b.commit__author__email)
group by search_key__owner, search_key__repo, author__id order by recent_years_commit_count desc limit top_n) as a global left join (

    -- 与中国沾边的 author id 和 最新的login
    select author__id, argMax(author__login, commit__author__date) as new_login
                   from github_commits where author__id !=0 and author__login not like '%[bot]%' and author__id global in (-- 与中国沾边的id
                                                          select author__id
                                                          from (select a.*, b.inferred_from_location__country
                                                                from (select a.*, b.author__id
                                                                      from (select *, splitByChar('@', author_email)[2] as email_domain
                                                                            from gits
                                                                            where length(parents) == 1) as a global
                                                                               join (select commit__author__email, author__id
                                                                                     from github_commits
                                                                                     where author__id != 0 and author__login not like '%[bot]%'
                                                                                     group by commit__author__email, author__id) as b
                                                                                    on
                                                                                        a.author_email = b.commit__author__email) as a global
                                                                         left join (select id, inferred_from_location__country, location
                                                                                    from github_profile
                                                                                    where github_profile.inferred_from_location__country = 'China') as b
                                                                                   on a.author__id = b.id)
                                                          where inferred_from_location__country != ''
                                                             or author_tz = 8
                                                             or email_domain global in ('baidu.com',
                                                                                        '163.com',
                                                                                        '126.com',
                                                                                        'qq.com',
                                                                                        'bytedance.com',
                                                                                        'huawei.com',
                                                                                        'xiaomi.com',
                                                                                        'alibaba-inc.com',
                                                                                        'linux.alibaba.com',
                                                                                        'alibaba.com',
                                                                                        'bytedance.com',
                                                                                        'streamcomputing.com',
                                                                                        'loongson.cn',
                                                                                        'iscas.ac.cn',
                                                                                        'tencent.com')
                                                             or email_domain like '%.cn'

                                                          group by author__id)
                   group by author__id
        ) as b on a.author__id = b.author__id

"""
    df = execute_sql(conn_info=conn_info, sql_query=sql_)
    return df, f'top_{top_n}_active_chinese_recent_years_start_{start_month}_end_{end_month}.csv' if not owner else f'{owner}_top_{top_n}_active_chinese_active_recent_years_start_{start_month}_end_{end_month}.csv'


def get_topn_active_repo_chinese_recent_years_commit(top_n, start_month, end_month, conn_info, owner):
    if owner:
        sql_ = f"""
        with {start_month} as start_month,{end_month} as end_month, {top_n} as top_n, '{owner}' as owner
select search_key__owner,
       search_key__repo,
       sum(total__lines)                                                              as total_lines,
       sum(total__insertions)                                                         as total_insertions,
       sum(total__deletions)                                                          as total_deletions,
       sum(total__files)                                                              as total_files,
       count()                                                                        as total_commit_count,
       sumIf(total__lines, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                      end_month)      as recent_years_modified_lines,
       sumIf(total__insertions, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                           end_month) as recent_years_modified_insertions,
       sumIf(total__deletions, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                          end_month)  as recent_years_modified_deletions,
       sumIf(total__files, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                      end_month)      as recent_years_modified_files_count,
       countIf(toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                          end_month)                  as recent_years_commit_count
from (select a.*, b.author__id
      from (select * from gits where length(parents) = 1 and search_key__owner = owner) as a global
               join (select commit__author__email, author__id
                     from github_commits
                     where author__id != 0
                       and author__login not like '%[bot]%'
                       and author__id global in (-- 与中国沾边的id
                         select author__id
                         from (select a.*, b.inferred_from_location__country
                               from (select a.*, b.author__id
                                     from (select *, splitByChar('@', author_email)[2] as email_domain
                                           from gits
                                           where length(parents) == 1) as a global
                                              join (select commit__author__email, author__id
                                                    from github_commits
                                                    where author__id != 0

                                                      and author__login not like '%[bot]%'
                                                    group by commit__author__email, author__id) as b
                                                   on
                                                       a.author_email = b.commit__author__email) as a global
                                        left join (select id, inferred_from_location__country, location
                                                   from github_profile
                                                   where github_profile.inferred_from_location__country = 'China') as b
                                                  on a.author__id = b.id)
                         where inferred_from_location__country != ''
                            or author_tz = 8
                            or email_domain global in ('baidu.com',
                                                       '163.com',
                                                       '126.com',
                                                       'qq.com',
                                                       'bytedance.com',
                                                       'huawei.com',
                                                       'xiaomi.com',
                                                       'alibaba-inc.com',
                                                       'linux.alibaba.com',
                                                       'alibaba.com',
                                                       'bytedance.com',
                                                       'streamcomputing.com',
                                                       'loongson.cn',
                                                       'iscas.ac.cn',
                                                       'tencent.com')
                            or email_domain like '%.cn'

                         group by author__id)
                     group by commit__author__email, author__id) as b on a.author_email = b.commit__author__email)
group by search_key__owner, search_key__repo
order by recent_years_commit_count desc
limit top_n
        """
    else:
        sql_ = f"""
        with {start_month} as start_month,{end_month} as end_month, {top_n} as top_n
select search_key__owner,
       search_key__repo,
       sum(total__lines)                                                                                         as total_lines,
       sum(total__insertions)                                                                                    as total_insertions,
       sum(total__deletions)                                                                                     as total_deletions,
       sum(total__files)                                                                                         as total_files,
       count()                                                                                                   as total_commit_count,
       sumIf(total__lines, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                      end_month)                                 as recent_years_modified_lines,
       sumIf(total__insertions, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                           end_month)                            as recent_years_modified_insertions,
       sumIf(total__deletions, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                          end_month)                             as recent_years_modified_deletions,
       sumIf(total__files, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                      end_month)                                 as recent_years_modified_files_count,
       countIf(toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                          end_month)                                             as recent_years_commit_count
from (select a.*, b.author__id
      from (select * from gits where length(parents) = 1) as a global
               join (select commit__author__email, author__id
                     from github_commits
                     where author__id != 0
                       and author__login not like '%[bot]%'
                       and author__id global in (-- 与中国沾边的id
                         select author__id
                         from (select a.*, b.inferred_from_location__country
                               from (select a.*, b.author__id
                                     from (select *, splitByChar('@', author_email)[2] as email_domain
                                           from gits
                                           where length(parents) == 1) as a global
                                              join (select commit__author__email, author__id
                                                    from github_commits
                                                    where author__id != 0
                                                     and author__login not like '%[bot]%'
                                                    group by commit__author__email, author__id) as b
                                                   on
                                                       a.author_email = b.commit__author__email) as a global
                                        left join (select id, inferred_from_location__country, location
                                                   from github_profile
                                                   where github_profile.inferred_from_location__country = 'China') as b
                                                  on a.author__id = b.id)
                         where inferred_from_location__country != ''
                            or author_tz = 8
                            or email_domain global in ('baidu.com',
                                                       '163.com',
                                                       '126.com',
                                                       'qq.com',
                                                       'bytedance.com',
                                                       'huawei.com',
                                                       'xiaomi.com',
                                                       'alibaba-inc.com',
                                                       'linux.alibaba.com',
                                                       'alibaba.com',
                                                       'bytedance.com',
                                                       'streamcomputing.com',
                                                       'loongson.cn',
                                                       'iscas.ac.cn',
                                                       'tencent.com')
                            or email_domain like '%.cn'

                         group by author__id)
                     group by commit__author__email, author__id) as b on a.author_email = b.commit__author__email)
group by search_key__owner, search_key__repo order by recent_years_commit_count desc limit top_n
        """

    df = execute_sql(conn_info=conn_info, sql_query=sql_)
    return df, f'top_{top_n}_active_chinese_commit_repo_recent_years_start_{start_month}_end_{end_month}.csv' if not owner else f'{owner}_top_{top_n}_active_chinese_commit_repo_recent_years_start_{start_month}_end_{end_month}.csv'


def get_topn_active_repo_chinese_recent_years_commit_strict(top_n, start_month, end_month, conn_info, owner):
    if owner:
        sql_ = f"""
        with {start_month} as start_month,{end_month} as end_month, {top_n} as top_n, '{owner}' as owner
select search_key__owner,
       search_key__repo,
       sum(total__lines)                                                              as total_lines,
       sum(total__insertions)                                                         as total_insertions,
       sum(total__deletions)                                                          as total_deletions,
       sum(total__files)                                                              as total_files,
       count()                                                                        as total_commit_count,
       sumIf(total__lines, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                      end_month)      as recent_years_modified_lines,
       sumIf(total__insertions, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                           end_month) as recent_years_modified_insertions,
       sumIf(total__deletions, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                          end_month)  as recent_years_modified_deletions,
       sumIf(total__files, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                      end_month)      as recent_years_modified_files_count,
       countIf(toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                          end_month)                  as recent_years_commit_count
from (
    select a.*, b.author__id
      from (select * from gits where length(parents) = 1 and search_key__owner = owner) as a global
               join (select commit__author__email, author__id
                     from github_commits
                     where author__id != 0
                       and author__login not like '%[bot]%'
                       and author__id global in (select github_id
from (select github_id,
             argMax(main_tz_area, update_at_timestamp) as new_main_tz_area,
             argMax(location, update_at_timestamp)     as new_location
      from github_id_main_tz_map
      group by github_id)
where new_main_tz_area = '中国' and (new_location = 'China' or new_location = '')
   or new_location = 'China')
                     group by commit__author__email, author__id) as b on a.author_email = b.commit__author__email)
group by search_key__owner, search_key__repo
order by recent_years_commit_count desc
limit top_n
        """
    else:
        sql_ = f"""
        with {start_month} as start_month,{end_month} as end_month, {top_n} as top_n
select search_key__owner,
       search_key__repo,
       sum(total__lines)                                                                                         as total_lines,
       sum(total__insertions)                                                                                    as total_insertions,
       sum(total__deletions)                                                                                     as total_deletions,
       sum(total__files)                                                                                         as total_files,
       count()                                                                                                   as total_commit_count,
       sumIf(total__lines, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                      end_month)                                 as recent_years_modified_lines,
       sumIf(total__insertions, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                           end_month)                            as recent_years_modified_insertions,
       sumIf(total__deletions, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                          end_month)                             as recent_years_modified_deletions,
       sumIf(total__files, toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                                      end_month)                                 as recent_years_modified_files_count,
       countIf(toYYYYMM(authored_date) >= start_month and toYYYYMM(authored_date) <=
                                                          end_month)                                             as recent_years_commit_count
from (select a.*, b.author__id
      from (select * from gits where length(parents) = 1) as a global
               join (select commit__author__email, author__id
                     from github_commits
                     where author__id != 0
                       and author__login not like '%[bot]%'
                       and author__id global in (select github_id
from (select github_id,
             argMax(main_tz_area, update_at_timestamp) as new_main_tz_area,
             argMax(location, update_at_timestamp)     as new_location
      from github_id_main_tz_map
      group by github_id)
where new_main_tz_area = '中国' and (new_location = 'China' or new_location = '')
   or new_location = 'China')
                     group by commit__author__email, author__id) as b on a.author_email = b.commit__author__email)
group by search_key__owner, search_key__repo order by recent_years_commit_count desc limit top_n
        """

    df = execute_sql(conn_info=conn_info, sql_query=sql_)
    return df, f'top_{top_n}_active_chinese_commit_repo_recent_years_start_{start_month}_end_{end_month}_strict.csv' if not owner else f'{owner}_top_{top_n}_active_chinese_commit_repo_recent_years_start_{start_month}_end_{end_month}_strict.csv'


# 取修改代码相关文件行数占比默认前80%的贡献，可指定时间范围，可指定owner,贡献占比也是可选
def get_top_n_modified_code_file_developer_commit(start_month, end_month, conn_info, owner, modified_proportion=0.8):
    # start_month end_month 可做成必选
    if owner:
        sql_ = f"""
with {start_month} as start_at,{end_month} as end_at,'{owner}' as owner, {modified_proportion} as modified_proportion
select *
from (select *,
             sum(total_code_lines)
                 over (partition by (search_key__owner, search_key__repo) ORDER BY total_code_lines DESC rows between unbounded preceding and current row) as sum_lines
      from (select a.*, b.new_author_login
            from (select a.*, b.repo_code_insertions, b.repo_code_deletions, b.repo_code_lines
                  from (select search_key__owner,
                               search_key__repo,
                               author__id,
                               sum(code_insertions) as   total_code_insertion,
                               sum(code_deletions) as total_code_deletions,
                               sum(code_lines)     as total_code_lines
                        from (select search_key__owner,
                                     search_key__repo,
                                     code_insertions,
                                     code_deletions,
                                     code_lines,
                                     b.*
                              from (-- 每个邮箱真正参与代码的修改部分行数
                                       select search_key__owner,
                                              search_key__repo,
                                              author_email,
                                              sum(insertions) as code_insertions,
                                              sum(deletions)  as code_deletions,
                                              sum(lines)      as code_lines
                                       from (select search_key__owner,
                                                    search_key__repo,
                                                    author_email,
                                                    file_name,
                                                    insertions,
                                                    deletions,
                                                    lines,
                                                    splitByChar('.', file_name)[-1] file_extension
                                             from gits array join
                                                  `files.file_name` as file_name,
                                                  `files.insertions` as insertions,
                                                  `files.deletions` as deletions,
                                                  `files.lines` as lines
                                             where length(parents) = 1
                                               and toYYYYMM(authored_date)<= end_at
                                               and toYYYYMM(authored_date)>= start_at
                                               and search_key__owner = owner
                                               and file_extension global in
                                                   ['py', 'kt', 'yml', 'rpg', 'pl',
                                                       'vb', 'pm', 'abap', 'sb', 'c', 'fs', 'yaml', 'dart',
                                                       'tcl', 'vhd', 'cs', 'go', 'html',
                                                       'd', 'java', 'f', 'lua', 'php', 'rb', 'sh', 'NET', 'ads', 'vbnet',
                                                       'cob', 'jl', 'as', 'js', 'logo', 'sql', 'rpgle', 'adb', 'htm', 'asm',
                                                       'm', 'rs', 'cls', 'Rmd', 'pli', 'swift', 'cbl', 'css', 'f90', 'ts',
                                                       'scala', 'groovy', 'fsi', 'hpp', 'vbs', 'h', 'cpp', 'xml', 'sas', 'R']
                                                )
                                       group by search_key__owner, search_key__repo, author_email) as a global
                                       join (select author__id, commit__author__email
                                             from github_commits
                                             where author__id != 0
                                               and search_key__owner = owner
                                               and author__login not like '%[bot]%'
                                             group by author__id, commit__author__email) as b
                                            on a.author_email = b.commit__author__email)
                        group by search_key__owner, search_key__repo, author__id
                        order by search_key__owner, search_key__repo, total_code_lines desc) as a global
                           left join (select search_key__owner,
                                             search_key__repo,
                                             sum(code_insertions) as repo_code_insertions,
                                             sum(code_deletions)  as repo_code_deletions,
                                             sum(code_lines)      as repo_code_lines
                                      from (select search_key__owner,
                                                   search_key__repo,
                                                   code_insertions,
                                                   code_deletions,
                                                   code_lines,
                                                   b.*
                                            from (-- 每个邮箱真正参与代码的修改部分行数
                                                     select search_key__owner,
                                                            search_key__repo,
                                                            author_email,
                                                            sum(insertions) as code_insertions,
                                                            sum(deletions)  as code_deletions,
                                                            sum(lines)      as code_lines
                                                     from (select search_key__owner,
                                                                  search_key__repo,
                                                                  author_email,
                                                                  file_name,
                                                                  insertions,
                                                                  deletions,
                                                                  lines,
                                                                  splitByChar('.', file_name)[-1] file_extension
                                                           from gits array join
                                                                `files.file_name` as file_name,
                                                                `files.insertions` as insertions,
                                                                `files.deletions` as deletions,
                                                                `files.lines` as lines
                                                           where length(parents) = 1
                                                           and toYYYYMM(authored_date)<= end_at
                                                           and toYYYYMM(authored_date)>= start_at
                                                           and search_key__owner = owner
                                                             and file_extension global in
                                                                 ['py', 'kt', 'yml', 'rpg', 'pl', 'vb', 'pm', 'abap', 'sb',
                                                                     'c', 'fs', 'yaml', 'dart', 'tcl', 'vhd', 'cs', 'go', 'html',
                                                                     'd', 'java', 'f', 'lua', 'php', 'rb', 'sh', 'NET', 'ads',
                                                                     'vbnet', 'cob', 'jl', 'as', 'js', 'logo', 'sql', 'rpgle',
                                                                     'adb', 'htm', 'asm', 'm', 'rs', 'cls', 'Rmd', 'pli', 'swift',
                                                                     'cbl', 'css', 'f90', 'ts', 'scala', 'groovy', 'fsi', 'hpp',
                                                                     'vbs', 'h', 'cpp', 'xml', 'sas', 'R']
                                                              )
                                                     group by search_key__owner, search_key__repo, author_email) as a global
                                                     join (select author__id, commit__author__email
                                                           from github_commits
                                                           where author__id != 0 and author__login not like '%[bot]%' and search_key__owner = owner
                                                           group by author__id, commit__author__email) as b
                                                          on a.author_email = b.commit__author__email)
                                      group by search_key__owner, search_key__repo
                                      order by repo_code_lines desc) as b
                                     on a.search_key__owner = b.search_key__owner and
                                        a.search_key__repo = b.search_key__repo) as a global
                     left join (select author__id, argMax(author__login, commit__author__date) as new_author_login
                                from (select author__id, author__login, commit__author__date
                                      from github_commits
                                      where author__id != 0
                                        and author__login != ''
                                      group by author__id, author__login, commit__author__date
                                         )
                                group by author__id) as b on a.author__id = b.author__id
            order by search_key__owner, search_key__repo, total_code_lines desc))
where sum_lines <= repo_code_lines * modified_proportion
"""
    else:
        sql_ = f"""
with {start_month} as start_at,{end_month} as end_at,{modified_proportion} as modified_proportion
select *
from (select *,
             sum(total_code_lines)
                 over (partition by (search_key__owner, search_key__repo) ORDER BY total_code_lines DESC rows between unbounded preceding and current row) as sum_lines
      from (select a.*, b.new_author_login
            from (select a.*, b.repo_code_insertions, b.repo_code_deletions, b.repo_code_lines
                  from (select search_key__owner,
                               search_key__repo,
                               author__id,
                               sum(code_insertions)   total_code_insertion,
                               sum(code_deletions) as total_code_deletions,
                               sum(code_lines)     as total_code_lines
                        from (select search_key__owner,
                                     search_key__repo,
                                     code_insertions,
                                     code_deletions,
                                     code_lines,
                                     b.*
                              from (-- 每个邮箱真正参与代码的修改部分行数
                                       select search_key__owner,
                                              search_key__repo,
                                              author_email,
                                              sum(insertions) as code_insertions,
                                              sum(deletions)  as code_deletions,
                                              sum(lines)      as code_lines
                                       from (select search_key__owner,
                                                    search_key__repo,
                                                    author_email,
                                                    file_name,
                                                    insertions,
                                                    deletions,
                                                    lines,
                                                    splitByChar('.', file_name)[-1] file_extension
                                             from gits array join
                                                  `files.file_name` as file_name,
                                                  `files.insertions` as insertions,
                                                  `files.deletions` as deletions,
                                                  `files.lines` as lines
                                             where length(parents) = 1
                                               and toYYYYMM(authored_date)<= end_at
                                               and toYYYYMM(authored_date)>= start_at
                                               and file_extension global in
                                                   ['py', 'kt', 'yml', 'rpg', 'pl',
                                                       'vb', 'pm', 'abap', 'sb', 'c', 'fs', 'yaml', 'dart',
                                                       'tcl', 'vhd', 'cs', 'go', 'html',
                                                       'd', 'java', 'f', 'lua', 'php', 'rb', 'sh', 'NET', 'ads', 'vbnet',
                                                       'cob', 'jl', 'as', 'js', 'logo', 'sql', 'rpgle', 'adb', 'htm', 'asm',
                                                       'm', 'rs', 'cls', 'Rmd', 'pli', 'swift', 'cbl', 'css', 'f90', 'ts',
                                                       'scala', 'groovy', 'fsi', 'hpp', 'vbs', 'h', 'cpp', 'xml', 'sas', 'R']
                                                )
                                       group by search_key__owner, search_key__repo, author_email) as a global
                                       join (select author__id, commit__author__email
                                             from github_commits
                                             where author__id != 0
                                               and author__login not like '%[bot]%'
                                             group by author__id, commit__author__email) as b
                                            on a.author_email = b.commit__author__email)
                        group by search_key__owner, search_key__repo, author__id
                        order by search_key__owner, search_key__repo, total_code_lines desc) as a global
                           left join (select search_key__owner,
                                             search_key__repo,
                                             sum(code_insertions) as repo_code_insertions,
                                             sum(code_deletions)  as repo_code_deletions,
                                             sum(code_lines)      as repo_code_lines
                                      from (select search_key__owner,
                                                   search_key__repo,
                                                   code_insertions,
                                                   code_deletions,
                                                   code_lines,
                                                   b.*
                                            from (-- 每个邮箱真正参与代码的修改部分行数
                                                     select search_key__owner,
                                                            search_key__repo,
                                                            author_email,
                                                            sum(insertions) as code_insertions,
                                                            sum(deletions)  as code_deletions,
                                                            sum(lines)      as code_lines
                                                     from (select search_key__owner,
                                                                  search_key__repo,
                                                                  author_email,
                                                                  file_name,
                                                                  insertions,
                                                                  deletions,
                                                                  lines,
                                                                  splitByChar('.', file_name)[-1] file_extension
                                                           from gits array join
                                                                `files.file_name` as file_name,
                                                                `files.insertions` as insertions,
                                                                `files.deletions` as deletions,
                                                                `files.lines` as lines
                                                           where length(parents) = 1
                                                           and toYYYYMM(authored_date)<= end_at
                                                           and toYYYYMM(authored_date)>= start_at
                                                             and file_extension global in
                                                                 ['py', 'kt', 'yml', 'rpg', 'pl', 'vb', 'pm', 'abap', 'sb',
                                                                     'c', 'fs', 'yaml', 'dart', 'tcl', 'vhd', 'cs', 'go', 'html',
                                                                     'd', 'java', 'f', 'lua', 'php', 'rb', 'sh', 'NET', 'ads',
                                                                     'vbnet', 'cob', 'jl', 'as', 'js', 'logo', 'sql', 'rpgle',
                                                                     'adb', 'htm', 'asm', 'm', 'rs', 'cls', 'Rmd', 'pli', 'swift',
                                                                     'cbl', 'css', 'f90', 'ts', 'scala', 'groovy', 'fsi', 'hpp',
                                                                     'vbs', 'h', 'cpp', 'xml', 'sas', 'R']
                                                              )
                                                     group by search_key__owner, search_key__repo, author_email) as a global
                                                     join (select author__id, commit__author__email
                                                           from github_commits
                                                           where author__id != 0 and author__login not like '%[bot]%'
                                                           group by author__id, commit__author__email) as b
                                                          on a.author_email = b.commit__author__email)
                                      group by search_key__owner, search_key__repo
                                      order by repo_code_lines desc) as b
                                     on a.search_key__owner = b.search_key__owner and
                                        a.search_key__repo = b.search_key__repo) as a global
                     left join (select author__id, argMax(author__login, commit__author__date) as new_author_login
                                from (select author__id, author__login, commit__author__date
                                      from github_commits
                                      where author__id != 0
                                        and author__login != ''
                                      group by author__id, author__login, commit__author__date
                                         )
                                group by author__id) as b on a.author__id = b.author__id
            order by search_key__owner, search_key__repo, total_code_lines desc))
where sum_lines <= repo_code_lines * modified_proportion

                """
    df = execute_sql(conn_info=conn_info, sql_query=sql_)
    return df, f'modified_code_file_commit_{modified_proportion}_proportion_start_{start_month}_end_{end_month}.csv' if not owner else f'{owner}_modified_code_file_commit_{modified_proportion}_proportion_start_{start_month}_end_{end_month}.csv'


# 用于去重表，当发现数据重复的时候请求
def do_optimize_table(table_names, conn_info):
    ck_cli = get_ck_client(conn_info)
    # print(table_names)
    for table_name in table_names:
        sql_ = f"""
                optimize table {table_name}_local on cluster replicated final
                """
        print(sql_)
        ck_cli.execute_no_params(sql_)


def get_code_reviewed_events(conn_info):
    pass


