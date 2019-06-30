import json
import os
import argparse


total_count = 0
entity_count = 0
relation_count = 0
concept_count = 0
property_count = 0
types = ["entity", "property", "concept", "relation"]

def process_file(file_path, output_dir):
    print("processing %s ..."%(file_path))
    entity_data = []
    concept_data = []
    property_data = []
    relation_data = []
    global total_count, types, entity_count, relation_count, concept_count, property_count
    with open(file_path, "r") as f:
        for line in f:
            if total_count%30000==0:
                print("\ntotal: %d, ent count: %d, pro count: %d\nrelation count: %d, concept count: %d"%(
                    total_count, entity_count, property_count, relation_count, concept_count))
            total_count+=1
            
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
                entity["descriptions"] = {}
                for language, value in data["descriptions"].items():
                    if language in ["zh", "en"]:
                        entity["descriptions"][language] = value["value"]
            # labels
            if data.get("labels", -1)!=-1:
                entity["labels"] = {}
                for language, value in data["labels"].items():
                    if language in ["zh", "en"]:
                        entity["labels"][language] = value["value"]
            # aliases
            if data.get("aliases", -1)!=-1:
                entity["aliases"] = {}
                for language, value in data["aliases"].items():
                    if language in ["zh", "en"]:
                        entity["aliases"][language] = []
                        for item in data["aliases"][language]:
                            entity["aliases"][language].append(item["value"])
            # sitelinks
            if data.get("sitelinks", -1)!=-1:
                entity["sitelinks"] = {}
                for language, value in data["sitelinks"].items():
                    if language in ["zhwiki", "enwiki"] and data["sitelinks"][language]!={}:
                        entity["sitelinks"][language] = value['title']
            
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
                                "datavalues": toEntity["mainsnak"]["datavalue"]
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
                property_count+=1
            else:
                entity_count+=1
            relation_data += entity_edge
            concept_data += concept_edge
            
            relation_count+=len(entity_edge)
            concept_count+=len(concept_edge)

        lists = [entity_data, property_data, concept_data, relation_data]
        paths = ["%s/%s/%s_%s"%(output_dir, i, file_path.split("/")[-1], i) for i in types]
        
        for i in range(len(types)):
            if len(lists[i])==0:
                continue
            with open(paths[i], "w") as f:
                for one_json in lists[i]:
                    f.write(json.dumps(one_json) + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str,
                        default="./output",
                        help="Downloaded pure json data file")

    parser.add_argument("--output", type=str,
                        default="./veoutput",
                       help="Analyzed data upper folder")
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
    
    for file_path in file_list:
        process_file(file_path, args.output)

            






