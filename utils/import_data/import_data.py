import os
import gremlin_python
import json
import argparse
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import Cardinality
import logging
import logging.config
import multiprocessing
import pprint
import math


def analyze_schema_func(schema_file, index_config):
    with open(schema_file, "r", encoding="utf-8") as f:
        try:
            schema = json.load(f)
        except FileNotFoundError:
            logging.error("schema file located in {} is not found.".format(schema_file))
            raise FileNotFoundError("schema file located in {} is not found.".format(schema_file))
        #schema_commands是最后要提交到server执行的语句
        schema_commands = ["mgmt = {}.openManagement()".format("graph")]
        for schema_key, schema_value in schema.items():
            #解析属性键
            if schema_key == "property_keys":
                for item in schema_value:
                    schema_commands.append("{} = mgmt.makePropertyKey('{}').dataType({}.class).cardinality(Cardinality.{}).make()".format(item["name"], item["name"], item["dataType"],item["cardinality"]))
            elif schema_key == "vertex_labels":
                for item in schema_value:
                    schema_commands.append("{} = mgmt.makeVertexLabel('{}').make()".format(item["name"], item["name"]))
            elif schema_key == "edge_labels":
                for item in schema_value:
                    schema_commands.append("{} = mgmt.makeEdgeLabel('{}').multiplicity({}).make()".format(item["name"], item["name"],item["multiplicity"]))
            #建立索引待完成
            elif schema_key == "vertexIndexes" and index_config["vertexIndexes"] is True:
                #节点索引，为节点
                #先为已经取出来的property
                # property_key_set = set()
                # for item in schema_value:
                #     if item["composite"] is True:
                #         if item["unique"] is True:  #建立一个unique的index
                #             schema_commands.append("")
                #         schema_commands.append("")
                pass
            elif schema_key == "edgeIndexes" and index_config["edgeIndexes"] is True:
                pass
            elif schema_key == "vertexCentricIndexes" and index_config["vertexCentricIndexes"] is True:
                pass
        #依次提交schema_commands中的语句
        logging.info("Analyzing finished, Now submitting gremlin commands")
        commands = ""
        for command in schema_commands:
            print(command)
            commands = commands + command + "\n"
        commands += "mgmt.commit()\n"

        connection.submit(commands)
        logging.info("Schema building finished.")

    # A example for call gremlin language
    # schema_msg = """mgmt = graph.openManagement()
    #                 string_prop = mgmt.makePropertyKey('string_prop').dataType(String.class).cardinality(Cardinality.LIST).make()
    #                 float_prop = mgmt.makePropertyKey('float_prop').dataType(Float.class).cardinality(Cardinality.LIST).make()
    #                 integer_prop = mgmt.makePropertyKey('integer_prop').dataType(Integer.class).cardinality(Cardinality.LIST).make()
    #                 mgmt.commit()"""
    # connection.submit(schema_msg)

def process_vertex(vertex_file_list):
    """

    :param vertex_file_list: entity file list
    :return: no return
    """
    if type(vertex_file_list) is str:
        vertex_file_list = [vertex_file_list]
    for vertex_file in vertex_file_list:
        with open(vertex_file, "r", encoding="utf-8") as v_f:
            for line in v_f:
                entity_info = json.loads(line)
                ent = g.addV("entity")
                for k, v in entity_info.items():
                   if type(v) is str:
                       g.property(k, v)
                   else:
                       g.property(k, str(v))
                ent.next()
                logging.debug("insert node success. node id:{}".format(entity_info["id"]))

def import_vertex_func(process_num):
    """

    :param file: 节点文件放置路径
    :param graph: janusgraph图实例
    :param process_num: 线程数
    :return:
    """

    try:
        assert os.path.exists(args.vertex_file)
    except AssertionError:
        logging.error("vertex_file is not exist!")
        logging.error("Error vertex file path: {}".format(args.vertex_file))
    #遍历file下每个文件，读取这些文件
    file_list = []
    for root, dir ,files in os.walk(args.vertex_file, topdown=False):
        for name in files:
            if name.endswith("entity"):
                file_list.append(os.path.join(root, name))
    num = len(file_list)
    logging.info("The number of files is %d. " % num)
    pool = multiprocessing.Pool(processes=args.worker_num)
    one_process_num = (int)(math.ceil(num * 1.0 / process_num))
    # 起始文件位置
    start = 0
    # 终止文件位置
    end = start + one_process_num
    while True:
        if start >= num:
            break
        pool.apply_async(process_vertex, args=(file_list[start:end]))
        start = end
        end = min(start + one_process_num, num)
    pool.close()
    pool.join()

def process_edges(edge_file_list):
    if type(edge_file_list) is str:
        edge_file_list = [edge_file_list]
    for edge_file in edge_file_list:
        with open(edge_file, "r", encoding="utf-8") as e_f:
            for line in e_f:
                relation_info = json.loads(line)
                try:
                    head_node = g.V().has("id", relation_info["from_vertex_id"]).next()
                except StopIteration:
                    #head节点不存在
                    head_node = g.addV("entity").property("id", relation_info["from_vertex_id"]).next()
                try:
                    tail_node = g.V("id", relation_info["to_vertex_id"]).next()
                except StopIteration:
                    tail_node = g.addV("entity").property("id", relation_info["to_vertex_id"]).next()
                rel = g.V(head_node).addE("relation").to(tail_node)
                rel.property("property_id", relation_info["property_id"])
                rel.iterate()   #execute import edge command
                logging.debug("insert edge success. head node:{}, tail node:{}, relation type:{}".format(relation_info["from_vertex_id"], relation_info["to_vertex_id"], relation_info["property_id"]))

def import_edge_func(process_num):
    """

    :param file:
    :param graph:
    :param process_num:
    :return:
    """
    # 遍历file下每个文件，读取这些文件
    try:
        assert os.path.exists(args.edge_file)
    except AssertionError:
        logging.error("edge_file is not exist!")
        logging.error("Error edge file path: {}".format(args.edge_file))
    file_list = []
    for root, dir, files in os.walk(args.edge_file, topdown=False):
        for name in files:
            if name.endswith("relation"):
                file_list.append(os.path.join(root, name))
    num = len(file_list)
    logging.info("The number of files is %d. " % num)
    pool = multiprocessing.Pool(processes=args.worker_num)
    one_process_num = (int)(math.ceil(num * 1.0 / process_num))
    # 起始文件位置
    start = 0
    # 终止文件位置
    end = start + one_process_num
    while True:
        if start >= num:
            break
        pool.apply_async(process_edges, args=(file_list[start:end]))
        start = end
        end = min(start + one_process_num, num)
    pool.close()
    pool.join()



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s:%(levelname)s:%(message)s",
                        datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser()
    #指定schema file、vertex file、edge file、janusgraph server position
    #janusgraph
    logging.info("WE ASSUME THAT YOU HAVE STARTED JANUSGRAPH SERVER!")
    parser.add_argument("--schema_file", type=str,
                        default="/home/vcp/local/data/import_storage_data/schema.json")
    parser.add_argument("--vertex_file", type=str,
                        default="/home/vcp/local/data/entity_data")
    parser.add_argument("--edge_file", type=str,
                        default="/home/vcp/local/data/relation_data")
    parser.add_argument("--janusgraph_server", type=str,
                        default="localhost:8182")
    parser.add_argument("--worker_num", type=int,
                        default=8)
    args = parser.parse_args()
    try:
        graph = Graph()
        connection = DriverRemoteConnection('ws://' + args.janusgraph_server + '/gremlin', 'g')
        logging.info("Successfully connect janusgraph server!")
        g = graph.traversal().withRemote(connection)
    except ConnectionRefusedError:
        logging.error('JanusGraph configration is not correct! Please check janusgraph server configuration')

    # logging.debug("Reading and analyzing schema file..")
    # index_config = {
    #     "vertexIndexes": False,
    #     "edgeIndexes": False,
    #     "vertexCentricIndexes": False
    # }
    # analyze_schema_func(args.schema_file, index_config)

    # logging.info("Start loading vertex files...")
    # logging.info("There are {} workers reading files..".format(args.worker_num))
    # import_vertex_func(args.worker_num)

    logging.info("Start loading edges files...")
    logging.info("There are {} workers reading files..".format(args.worker_num))
    import_edge_func(args.worker_num)


