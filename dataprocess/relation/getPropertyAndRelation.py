import re
import argparse
import os
import zhconv
import requests
import json
property_pattern = re.compile(r'\"(P[1-9]+[0-9]*)\"')
url = "https://www.wikidata.org/w/api.php?action=wbgetentities&languages=en|zh&props=aliases|labels|descriptions&format=json&ids="

def getOneFile(file_path):
    property_id = set()
    with open(file_path, "r", encoding="utf8") as f:
        for line in f:
            props = property_pattern.findall(line)
            for prop in props:
                property_id.add(prop)
    return property_id

def getPropertyJson(props, percount, output):
    for index in range(0, len(props), percount):
        props_detail = []
        ids = "|".join(props[index:index+percount])
        r = requests.get(url+ids)
        data = json.loads(zhconv.convert(r.text, "zh-cn"))
        for k, v in data['entities'].items():
            if "missing" in v or v.get("labels", -1)==-1:
                continue
            newdata = {}
            newdata['id'] = k
            attrs = ['labels', 'descriptions']
            langs = ['zh', 'en']
            for attr in attrs:
                if v.get(attr, -1)!=-1:
                    for lang in langs:
                        if v[attr].get(lang, -1)!=-1:
                            newdata[lang + '-' + attr[:-1]] = v[attr][lang]["value"]
            if v.get("aliases", -1)!=-1:
                for lang in langs:
                    if v["aliases"].get(lang, -1)!=-1:
                        newdata[lang + '-' + "aliase"] = []
                        for vv in v["aliases"][lang]:
                            newdata[lang + '-' + "aliase"].append(vv["value"])
            props_detail.append(newdata)
            print("Crawled %d properties."%(len(props_detail)))
            with open(output, "a+", encoding="utf8") as f:
                for pp in props_detail:
                    json.dump(pp, f, ensure_ascii=False)
                    f.write("\n")
    print("total crawled properties: %d"%(total))


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str,
                        default="../data/veoutput",
                        help="input dir")

    parser.add_argument("--output", type=str,
                        default="../data/result",
                       help="output file")
    
    parser.add_argument("--percount", type=int,
                        default=40,
                       help="the number of property per requests. (max 50)")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        raise ValueError
    
    file_list = []
    for root, dirs, files in os.walk(args.input):
        if len(files)==0:
            continue
        else:
            for file in files:
                file_list.append(root+'/'+file)

    props = set()
    for file in file_list:
        props = props | getOneFile(file)
        print(file, len(props))
    props = list(props)
    getPropertyJson(props, args.percount, args.output)