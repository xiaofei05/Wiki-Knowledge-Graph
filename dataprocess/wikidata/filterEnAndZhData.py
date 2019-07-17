import json
import os
import argparse

def process_data(input_file, output_dir, num_per_file):
    """
    :param input_file: 输入文件
    :param output_dir: 输出路径
    :param num_per_file: 每个输出文件的数据个数
    :return:
    """
    with open(input_file, 'r') as f:
        process_count = 0
        save_count = 0
        for line in f:
            
            output_file = output_dir + '/data' + str(save_count//num_per_file)
            if process_count%10000==0:
                print(output_file, save_count, process_count)
            process_count += 1
            data = line.strip()
            if data == '[' or data == ']':
                continue
            if data[-1] == ',':
                data = data[:-1]
            data = json.loads(data)
            if "sitelinks" not in data.keys():
                continue
            if "zhwiki" in data['sitelinks'] or "enwiki" in data['sitelinks']:
                save_count += 1
                data['sitelinks'] = {
                    "zhwiki": data['sitelinks'].get("zhwiki", {}),
                    "enwiki": data['sitelinks'].get("enwiki", {}),
                }
                with open(output_file, 'a+') as fw:
                    fw.write(json.dumps(data)+'\n')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str,
                        default="../data/latest-all.json",
                        help="Downloaded pure json data file")

    parser.add_argument("--output", type=str,
                        default="../data/output",
                       help="Analyzed data upper folder")
    
    parser.add_argument("--num_per_file", type=int, default=50000, help="The number of data in each output file")
    args = parser.parse_args()

    if not os.path.exists(args.output):
        os.mkdir(args.output)
    
    process_data(args.input, args.output, 50000)
