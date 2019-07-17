
package com.loaddata.vertex;

import com.loaddata.Importer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;

public class VertexImporter extends Importer {

    private static final Logger LOGGER = LoggerFactory.getLogger(VertexImporter.class);
    private JanusGraphTransaction graphTransaction;
    private GraphTraversalSource g;
    public String filePath;

    @Override
    public Integer call () throws InterruptedException {
        LOGGER.info(String.format("Processing file: %s", filePath));

        Integer total = 0, sucess = 0;

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(filePath)));
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                total+=1;
                Vertex entity = addEntity(line);
                if(entity!=null){
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

    public Vertex addEntity(String str) {
        JSONObject json = new JSONObject(str);
        Vertex entity = graphTransaction.addVertex("entity");
        try {
            entity.property("entity-id", json.getString("id"));
            entity.property("type", json.getString("type"));
            String[] language = {"en", "zh"};
            String[] properties = {"description", "label", "link"};
            for (String lang : language) {
                for (String prop : properties) {
                    String key = lang + "-" + prop;
                    if (json.has(key)) {
                        entity.property(key, json.getString(key));
                    }
                }
                String aliases = lang + "-" + "aliases";
                if (json.has(aliases)) {
                    JSONArray aliasesValue = json.getJSONArray(aliases);
                    for (int i = 0; i < aliasesValue.length(); i++) {
                        entity.property(aliases, aliasesValue.getString(i));
                    }
                }
            }
            if (!json.has("abouts")) {
                return entity;
            }
            // add attributes
            JSONObject attrs = json.getJSONObject("abouts");
            Set<String> attrKeys = attrs.keySet();
            for (String key : attrKeys) {
                JSONArray values = attrs.getJSONArray(key);
                for (int i = 0; i < values.length(); i++) {
                    Vertex prop = graphTransaction.addVertex("feature");
                    try {
                        JSONObject val = values.getJSONObject(i);
                        prop.property("property-id", key);
                        prop.property("value", genValue(val));
                        prop.property("type", val.getString("type"));
                        prop.property("rank", val.getString("rank"));
                        // add sub attributes
                        if (val.has("abouts")) {
                            prop.property("abouts", val.getJSONObject("abouts").toString());
//                            JSONObject subAttrs = val.getJSONObject("abouts");
//                            Set<String> subAttrKeys = subAttrs.keySet();
//                            for (String subKey : subAttrKeys) {
////                            if (graphTransaction.getPropertyKey(subKey) == null) {
////                                continue;
////                            }
//                                JSONArray subValues = subAttrs.getJSONArray(subKey);
//                                for (int j = 0; j < subValues.length(); j++) {
//                                    JSONObject subVal = subValues.getJSONObject(j);
//                                    prop.property(subKey, genValue(subVal));
//                                }
//                            }
                        }

                        // add references
                        if (val.has("references")) {
                            prop.property("references", val.getJSONObject("references").toString());
                        }
                        entity.addEdge("attribute", prop);
                    }catch (Exception e){
                        prop.remove();
                        LOGGER.warn(e.getMessage());
                    }
                }
            }
        }catch (Exception e){
            LOGGER.warn(e.getMessage());
            entity.remove();
            return null;
        }
        return entity;
    }
    public VertexImporter(JanusGraph jg, String fp) {
        super(jg);
        graphTransaction = getGraph().newTransaction();
        g = graphTransaction.traversal();
        filePath = fp;
    }
}