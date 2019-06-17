import re
import json
from langconv import *
import os
import multiprocessing
import math

file_prefix = "AA/wiki_"
mention_prefix = "https://zh.wikipedia.org/wiki/"

num = 33
process_num = 8

pattern = re.compile(r'<a\b[^>]+\bhref="([^"]*)"[^>]*>(.*?)</a>')


def add_mention(data):
    new_data = data
    new_data['mentions'] = {}
    result = pattern.findall(data['text'])
    for pairs in result:
        new_data['mentions'][pairs[1]] = mention_prefix + pairs[0]
    return new_data

def one_process(start, end):
    for num in range(start, end):
        file = file_prefix + "%02d"%num
        with open(file+'_zh', 'w', encoding='utf8') as fw:
            with open(file, 'r', encoding='utf8') as f:
                count=0
                for line in f:
                    if count%1000==0:
                        print(file, count)
                    count+=1
                    line_data = Converter('zh-hans').convert(line)
                    data = json.loads(line_data.strip())
                    new_data = add_mention(data)
                    fw.write(json.dumps(new_data, ensure_ascii=False)+'\n')

def run_task():
    pool = multiprocessing.Pool(processes = process_num)
    one_process_num = (int)(math.ceil(num*1.0 / process_num))
    start = 0
    end = start + one_process_num
    while end<=num:
        pool.apply_async(one_process, args=(start, end))
        start = end
        end = min(end+one_process_num, num+1)
    pool.close()
    pool.join()

if __name__ == "__main__":
    run_task()
