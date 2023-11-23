# -*-coding:utf-8-*-
import copy
import json

import numpy
import pandas as pd


def np_type_2_py_type(row, template='None'):
    if isinstance(row, numpy.int64):
        row = int(row)
    elif isinstance(row, dict):
        row = str(row).replace(": ", ":")
    elif isinstance(row, numpy.bool_):
        row = int(bool(row))
    elif row is None:
        # ATTN : Sometime a 'int' variable could be None.
        if isinstance(template, int):
            row = 0
        elif isinstance(template, str):
            row = "null"
        else:
            row = "null"
    elif isinstance(row, bool):
        row = int(row)
    elif isinstance(row, numpy.float64):
        row = float(row)
    return row




def parse_data(df, temp):
    # 这个是最终插入ck的数据字典
    dict_data = copy.deepcopy(temp)
    for index, row in df.iloc[0].items():
        # 去除以raw_data开头的字段
        if index.startswith('raw'):
            index = index[9:]
        index = index.replace('.', '__')
        # 只要是空的就跳过
        if not row:
            continue
        # 第一步的转化
        row = np_type_2_py_type(row)
        # 解决嵌套 array
        if isinstance(row, list):
            # 数组中是字典
            if row and isinstance(row[0], dict):
                default_dict = {}
                for key in dict_data:
                    if key.startswith(f'{index}.'):
                        # default_dict 保存模板中的默认值，0, None, {} 等
                        default_dict[key] = dict_data[key][0]
                        dict_data[key] = []
                for data in row:
                    vis = {}
                    for key in data:
                        # 若不在模板中，之后授予default value
                        if (f'{index}.{key}' not in dict_data):
                            continue
                        vis[f'{index}.{key}'] = 1
                        filter_data = np_type_2_py_type(data.get(key), default_dict.get(f'{index}.{key}'))
                        try:
                            # BUG 如果一个list包含3个dict，其中只有第二个有某个键值对
                            #     那这就会有问题。
                            #     目前解决办法：通过下面的default_dict处理
                            dict_data.get(f'{index}.{key}').append(filter_data)
                        except Exception as e:
                            print(f'{index}.{key}')
                            raise e
                    # chenkx :: 若不存在该key，则添加默认值:"0,'',None,{}等"，保证list的完整性
                    for key in default_dict:
                        if key not in vis:
                            dict_data[key].append(default_dict[key])
            else:
                data_name = f'{index}'
                # if data_name in dict_data:
                dict_data[data_name] = row
        else:
            data_name = f'{index}'
            # if data_name in dict_data:
            dict_data[data_name] = row

    return dict_data

with open('simple_pr_event.json','r') as f:
    source_data = json.load(f)

print(source_data)
normalized_data = pd.json_normalize(source_data)

dict_data = parse_data(normalized_data, {})
print(dict_data)