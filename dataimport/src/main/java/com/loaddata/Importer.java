package com.loaddata;
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;
import org.janusgraph.core.JanusGraph;
import org.json.JSONObject;

import java.util.concurrent.Callable;

public abstract class Importer implements Callable {
    private final JanusGraph graph;
    public int COMMIT_COUNT  = 10000;

    public Importer(JanusGraph jg){
        graph = jg;
    }
    public JanusGraph getGraph(){
        return graph;
    }

    public static String genValue(JSONObject val) throws ValueException {
        if(val.has("datatype") && val.has("datavalue")) {
            JSONObject datavalue = val.getJSONObject("datavalue");
            return String.format("{\"datatype\":%s, \"datavalue\": %s}", val.getString("datatype"), datavalue.toString());
        }else{
            throw new ValueException("No key named datatype or datavalue!");
        }
    }
}
