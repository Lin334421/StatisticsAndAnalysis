# -*-coding:utf-8-*-
import pandas as pd

df = pd.DataFrame(columns=["search_key__owner", "search_key__repo",
                           "author__id",
                           "total_code_insertion",
                           "total_code_deletions",
                           "total_code_lines",
                           "repo_code_insertions",
                           "repo_code_deletions",
                           "repo_code_lines",
                           "new_author_login"])
new_row = ('0-0-1', 'drc', 8138156, 3, 3, 6, 3, 3, 6, '0-0-1')
# df.append(new_row, ignore_index=True)
df.loc[len(df.index)] = new_row
print(df)