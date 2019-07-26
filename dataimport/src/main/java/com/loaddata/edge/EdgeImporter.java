package com.loaddata.edge;
import com.loaddata.Importer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.SchemaViolationException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Set;


public class EdgeImporter extends Importer {
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeImporter.class);

    private JanusGraphTransaction graphTransaction;
    private GraphTraversalSource g;
    public String filePath;

    @Override
    public Integer call () throws InterruptedException {
        LOGGER.info(String.format("Process file: %s", filePath));
        Integer total = 0, sucess = 0;
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(filePath)));
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                total += 1;
                Edge edge = addRelation(line);
                if(edge!=null){
                    sucess+=1;
                }
                if(total%COMMIT_COUNT==0){
                    graphTransaction.commit();
                    graphTransaction.close();
                    graphTransaction = getGraph().newTransaction();
                    g = graphTransaction.traversal();
                }
            }
            br.close();
        }catch (IOException e){
            LOGGER.error(e.getMessage());
        }
        graphTransaction.commit();
        graphTransaction.close();
        LOGGER.info("Process file: " + filePath + " done!");
        return sucess;
    }

    public Edge addRelation(String str) {
        JSONObject json = new JSONObject(str);
        Iterator<Vertex> node_1 = g.V().has("entity", "entity-id", json.getString("from_vertex_id"));
        Iterator<Vertex> node_2 = g.V().has("entity", "entity-id", json.getString("to_vertex_id"));
        Edge edge = null;
        try {
            if (node_1.hasNext() && node_2.hasNext()) {
                Vertex v1 = node_1.next();
                Vertex v2 = node_2.next();
                edge = v1.addEdge("relation", v2);
                edge.property("type", json.getString("type"));
                edge.property("rank", json.getString("rank"));
                edge.property("relation-id", json.getString("property_id"));

                if (json.has("abouts")) {
                    edge.property("abouts", json.getJSONObject("abouts").toString());
//                    Set<String> keys = attrs.keySet();
//                    for (String key : keys) {
//                        JSONArray values = attrs.getJSONArray(key);
//                        if(graphTransaction.getPropertyKey(key)==null){
//                            continue;
//                        }
//                        for (int j = 0; j < values.length(); j++) {
//                            edge.property(key, genValue(values.getJSONObject(j)));
//                        }
//                    }
                }
                if(json.has("references")){
                    edge.property("references", json.getJSONObject("references").toString());
                }
            } else {
                return null;
            }
        } catch (SchemaViolationException e) {
            if(edge!=null) {
                edge.remove();
            }
            LOGGER.error(e.getMessage());
        } catch (Exception e) {
            throw e;
        }
        return edge;
    }

    public EdgeImporter(JanusGraph jg, String fp) {
        super(jg);
        graphTransaction = getGraph().newTransaction();
        g = graphTransaction.traversal();
        filePath = fp;
    }
}