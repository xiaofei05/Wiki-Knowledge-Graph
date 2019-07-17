package com.schema;
import org.janusgraph.diskstorage.BackendException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;


public class SchemaMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMain.class);

    public void loadSchema(JanusGraph g, String schemaFile) throws Exception {
        JanusGraphSONSchema importer = new JanusGraphSONSchema(g);
        importer.readFile(schemaFile);
    }

    public static void main(String[] args) {
        if (null == args || args.length != 2) {
            System.err.println(
                    "Usage: SchemaMain <janusgraph-config-file> <com.schema.json>");
            System.exit(1);
        }
        JanusGraph graph = JanusGraphFactory.open(args[0]);
        try {
            LOGGER.info("droping the database!");
            JanusGraphFactory.drop(graph);
            graph = JanusGraphFactory.open(args[0]);
            LOGGER.info("droped the database!");
        } catch (BackendException e) {
            e.printStackTrace();
            LOGGER.error("can not drop the database!");
        }

        try {
            new SchemaMain().loadSchema(graph, args[1]);
        }catch (Exception e){
            LOGGER.error(e.getMessage());
        }finally {
            graph.close();
        }
    }
}
