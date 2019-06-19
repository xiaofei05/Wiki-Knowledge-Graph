'''
处理中、英文维基百科数据。该数据是通过Wiki extractor处理过的Wiki dump数据
处理方式：将中文繁体字转化为简体字、统计实体页面上共现的其他实体并附在实体页面后面
'''
import re
import json
from langconv import *
import os
import multiprocessing
import math
import argparse
from pathlib import Path



ahref_patten = re.compile(r'<a\b[^>]+\bhref="([^"]*)"[^>]*>(.*?)</a>')


def add_mention(data, mode):
    wikipedia_prefix="https://"+mode+".wikipedia.org/wiki/"
    new_data = {}
    new_data['mentions'] = {}
    result = ahref_patten.findall(data['text'])
    for pairs in result:
        new_data['mentions'][pairs[1]] = wikipedia_prefix + pairs[0]
    data.update(new_data)
    return data

def one_process(file_list, mode, output_file):
    """
    :param file_list: 待处理文件
    :param output_file: 输出结果文件路径
    :param mode: 模式
    :return:
    """
    assert mode == "zh" or "en"
    for file_path in file_list:
        #存储在output路径下的同子路径文件中
        output_f = output_file + file_path[file_path.index('/', 2):]
        with open(output_f, 'w+', encoding='utf-8') as fw:
            with open(file_path, 'r', encoding='utf-8') as f:
                count=0
                for line_data in f:
                    if count%3000==0:
                        print(file_path, count)
                    count+=1
                    if mode == "zh":
                        #将line_data转化为一个dict
                        line_data = json.loads(Converter('zh-hans').convert(line_data).strip())
                        # print(line_data)
                    else:
                        line_data = json.loads(line_data.strip())
                    new_data = add_mention(line_data, mode)
                    fw.write(json.dumps(new_data, ensure_ascii=False)+'\n')

def run_task(process_num, mode, file_list, output_file):
    """
    :param process_num: 多线程数
    :param mode: 处理中文维基数据（zh）还是英文数据（en）
    :param file_list: 待处理文件列表
    :return:
    """
    #列出file_list下的文件个数
    num=len(file_list)
    
    # 新建文件夹
    for file_path in file_list:
        #存储在output路径下的同子路径文件中
        output_f = output_file + file_path[file_path.index('/', 2):]
        if not os.path.exists(output_f[:output_f.rindex("/")]):
            os.makedirs(output_f[:output_f.rindex("/")])
    
    pool = multiprocessing.Pool(processes = process_num)
    one_process_num = (int)(math.ceil(num*1.0 / process_num))
    #起始文件位置
    start = 0
    #终止文件位置
    end = start + one_process_num
    print("The number of files is %d. "%num)
    while end<num:
        pool.apply_async(one_process, args=(file_list[start:end], mode, output_file))
        start = end
        end = min(end+one_process_num, num)
    pool.close()
    pool.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    #input_file是指Wikiextractor处理后的文件上级目录，也就是text文件夹
    parser.add_argument("-n","--need_extractor",help="need running WikiExtractor or not",
                        action="store_true")
    parser.add_argument("--input_file", type=str,
                        default="./en_output",
                        help="pure article data upper folder where the text file is")
    parser.add_argument("--output_file", type=str,
                        default="./en_output_analyzed",
                       help="Analyzed data upper folder")
    parser.add_argument("--process_num", type=int, default=8, help="multiprocessnum")
    args = parser.parse_args()

    #mode是指处理的中文维基数据还是英文数据
    if args.input_file.split("/")[-1].startswith("zh"):
        mode="zh"
    else:
        mode="en"
    
    #获取text文件夹下的所有子文件夹的文件
    file_list = []
    for root, dirs, files in os.walk(args.input_file):
        if len(files)==0:
            continue
        else:
            for file in files:
                file_list.append(root+'/'+file)
    run_task(args.process_num, mode, file_list, args.output_file)