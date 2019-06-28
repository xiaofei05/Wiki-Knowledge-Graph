import os
import gremlin_python
import json
import argparse
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import logging
import logging.config
import multiprocessing


def analyze_schema_func(schema_file, index=False):
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
            elif schema_key == "vertexIndexes" and index is True:
                #节点索引，为节点
                #先为已经取出来的property
                property_key_set = set()
                for item in schema_value:
                    if item["composite"] is True:
                        if item["unique"] is True:  #建立一个unique的index
                            schema_commands.append("")
                        schema_commands.append("")

        #call gremlin language
        schema_msg = """mgmt = graph.openManagement()
                        string_prop = mgmt.makePropertyKey('string_prop').dataType(String.class).cardinality(Cardinality.LIST).make()
                        float_prop = mgmt.makePropertyKey('float_prop').dataType(Float.class).cardinality(Cardinality.LIST).make()
                        integer_prop = mgmt.makePropertyKey('integer_prop').dataType(Integer.class).cardinality(Cardinality.LIST).make()
                        mgmt.commit()"""
        connection.submit(schema_msg)

    pass
def import_vertex_func(file, graph, process_num):
    #遍历file下每个文件，读取这些文件
    files = os.listdir(file)
    pool = multiprocessing.Pool(processes=args.worker_num)
    pass
def import_edge_func(file, graph, process_num):
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s:%(levelname)s:%(message)s",
                        datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser()
    #指定schema file、vertex file、edge file、janusgraph server position
    #janusgraph
    logging.info("WE ASSUME THAT YOU HAVE STARTED JANUSGRAPH SERVER!")
    parser.add_argument("--schema_file", type=str,
                        default="../../data/import_storage_data/schema.json")
    parser.add_argument("--vertex_file", type=str,
                        default="../../data/import_storage_data/vertex_data")
    parser.add_argument("--edge_file", type=str,
                        default="../../data/import_storage_data/edge_data")
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
        logging.error('JanusGraph configration is not correct!')
        logging.error("Please check janusgraph server configuration")
    try:
        logging.warning("Graph schema is not defined. Reading graph management(schema)... from file {}".format(args.schema_file))
        mgmt = graph.openManagement()

    except AttributeError:
        logging.debug("Reading and analyzing schema file..")
        analyze_schema_func(args.schema_file)
    logging.info("Start loading vertex files...")
    logging.info("There are {} workers reading files..".format(args.worker_num))
    import_vertex_func(args.vertex_file, args.worker_num)
    logging.info("Start loading edges files...")
    logging.info("There are {} workers reading files..".format(args.worker_num))
    import_edge_func(args.edge_file, args.worker_num)


