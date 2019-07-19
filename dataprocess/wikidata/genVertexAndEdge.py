import json
import os
import argparse
import sys
import zhconv
import multiprocessing
# sys.path.append("../../")

types = ["entity", "property", "concept", "relation"]

def process_file(file_path, output_dir):
    print("\nprocessing %s ..."%(file_path))
    entity_data = []
    concept_data = []
    property_data = []
    relation_data = []
    with open(file_path, "r") as f:
        for line in f:

            data = json.loads(line.strip())
            # id, type
            entity = {
                "id": data["id"],
                "type": data["type"]
            }
            # modified
            if data.get("modified", -1)!=-1:
                entity["modified"] = data["modified"]
            
            # descriptions
            if data.get("descriptions", -1)!=-1:
                for language, value in data["descriptions"].items():
                    if language in ["zh", "en"]:
                        if language == "zh":
                            entity[language + "-description"] = zhconv.convert(value["value"], 'zh-cn')
                        else:
                            entity[language + "-description"] = value["value"]
            # labels
            if data.get("labels", -1)!=-1:
                for language, value in data["labels"].items():
                    if language in ["zh", "en"]:
                        if language == "zh":
                            entity[language + "-label"] = zhconv.convert(value["value"], 'zh-cn')
                        else:
                            entity[language + "-label"] = value["value"]
            # aliases
            if data.get("aliases", -1)!=-1:
                for language, value in data["aliases"].items():
                    if language in ["zh", "en"]:
                        entity[language + "-aliases"] = []
                        for item in data["aliases"][language]:
                            entity[language + "-aliases"].append(zhconv.convert(item["value"], 'zh-cn'))
            # sitelinks
            if data.get("sitelinks", -1)!=-1:
                for language, value in data["sitelinks"].items():
                    if language in ["zhwiki", "enwiki"] and data["sitelinks"][language]!={}:
                        entity[language[:2] + "-link"] = value['title']
            
            concept_edge = []
            entity_edge = []
            
            # 关系和属性
            if data.get("claims", -1)!=-1:
                concept_edge = []
                entity_edge = []
                for pid, values in data['claims'].items():
                    for toEntity in values:
                        flag = "attr"  # attr, concept, relation

                        edge = {}
                        if toEntity["mainsnak"]["snaktype"]!="value":
                            continue
                        
                        if toEntity["mainsnak"]["datavalue"]["type"] != "wikibase-entityid":
                            edge = {
                                "datatype": toEntity["mainsnak"]["datatype"],
                                "datavalue": toEntity["mainsnak"]["datavalue"]
                            }
                        else:
                            if toEntity["mainsnak"]["datatype"]=="wikibase-item":
                                if data["type"]=="property":
                                    flag = "concept"
                                else:
                                    flag = "relation"
                            elif toEntity["mainsnak"]["datatype"]=="wikibase-property":
                                flag = "concept"
                            else:
                                continue
                            
                            edge["to_vertex_id"] = toEntity["mainsnak"]["datavalue"]["value"]["id"]
                            edge["from_vertex_id"] = data["id"]
                            edge["property_id"] = pid

                        # add type rank
                        edge["type"] = toEntity["type"]
                        edge["rank"] = toEntity["rank"] 

                        if toEntity.get("qualifiers", -1) != -1:
                            edge["abouts"] = {}
                            for k, v in toEntity["qualifiers"].items():
                                for obj in v:
                                    if obj["snaktype"]!="value":
                                        continue
                                    if obj["datatype"] in ["wikibase-sense", "wikibase-lexeme", "wikibase-form"]:
                                        continue
                                    edge["abouts"][obj["property"]] = edge["abouts"].get(obj["property"], [])
                                    edge["abouts"][obj["property"]].append({
                                        "datatype": obj["datatype"],
                                        "datavalue": obj["datavalue"]
                                    })
                        
                        if toEntity.get("references", -1) != -1:
                            edge["references"] = {}
                            for v in toEntity["references"]:
                                for pk, lv in v["snaks"].items():
                                    for obj in lv:
                                        if obj["snaktype"]!="value":
                                            continue
                                        if obj["datatype"] in ["wikibase-sense", "wikibase-lexeme", "wikibase-form"]:
                                            continue
                                        edge["references"][obj["property"]] = edge["references"].get(obj["property"], [])
                                        edge["references"][obj["property"]].append({
                                            "datatype": obj["datatype"],
                                            "datavalue": obj["datavalue"]
                                        })
                        
                        # save
                        if flag == "attr":
                            entity["abouts"] = entity.get("abouts", {})
                            entity["abouts"][toEntity["mainsnak"]["property"]] = entity["abouts"].get(toEntity["mainsnak"]["property"], [])
                            entity["abouts"][toEntity["mainsnak"]["property"]].append(edge)
                        elif flag == "relation":
                            entity_edge.append(edge)
                        elif flag == "concept":
                            concept_edge.append(edge)
                        else:
                            raise ValueError

            entity_data.append(entity)
            if entity["type"]=="property":
                property_data.append(entity)
            else:
            relation_data += entity_edge
            concept_data += concept_edge

        lists = [entity_data, property_data, concept_data, relation_data]
        paths = ["%s/%s/%s_%s"%(output_dir, i, file_path.split("/")[-1], i) for i in types]
        
        for i in range(len(types)):
            if len(lists[i])==0:
                continue
            with open(paths[i], "w") as f:
                for one_json in lists[i]:
                    f.write(json.dumps(one_json) + "\n")
        
        print("\nprocessing %s ... done"%(file_path))

def run_task(process_num, file_list, output_file):
    #列出file_list下的文件个数
    num=len(file_list)
    pool = multiprocessing.Pool(processes = process_num)
    print("The number of files is %d. "%num)

    for file_path in file_list:
        pool.apply_async(process_file, args=(file_path, output_file))
    pool.close()
    pool.join()



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str,
                        default="../data/output",
                        help="Downloaded pure json data file")

    parser.add_argument("--output", type=str,
                        default="../data/veoutput",
                       help="Analyzed data upper folder")
    
    parser.add_argument("--process_num", type=int,
                        default=8,
                       help="the number of process")
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

    for i in types:
        if not os.path.exists(os.path.join(args.output, i)):
            os.makedirs(os.path.join(args.output, i))
    

    run_task(args.process_num, file_list, args.output)
    # for file_path in file_list:
    #     process_file(file_path, args.output)

            






