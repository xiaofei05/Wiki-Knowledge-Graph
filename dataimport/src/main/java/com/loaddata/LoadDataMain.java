package com.loaddata;

import com.loaddata.edge.EdgeImporter;
import com.loaddata.vertex.VertexImporter;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class LoadDataMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadDataMain.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        if (null == args || args.length != 4) {
            System.err.println(
                    "Usage: LoadDataMain <janusgraph-config-file> <data-files-directory> <edge/vertex> <Thread>");
            System.exit(1);
        }
        String dirPath = args[1];
        JanusGraph graph = JanusGraphFactory.open(args[0]);
        ExecutorService executorService = Executors.newFixedThreadPool((Integer.parseInt(args[3])));

        File folder = new File(dirPath);
        File[] listOfFiles = folder.listFiles();
        List<String> filePaths = new ArrayList<String>();
        for (File file : listOfFiles) {
            if (file.isFile()) {
                filePaths.add(dirPath + "/"+ file.getName());
            }
        }
        LOGGER.info(String.format("Total files is %d", filePaths.size()));
        List<FutureTask<Integer>> futureTasks = new ArrayList<FutureTask<Integer>>();

        if(args[2].toLowerCase()=="vertex"){
            for(String filePath:filePaths){
                FutureTask<Integer> future = new FutureTask<Integer>(new VertexImporter(graph, filePath));
                futureTasks.add(future);
                executorService.submit(future);
            }
        }else if(args[2].toLowerCase()=="edge"){
            for(String filePath:filePaths){
                FutureTask<Integer> future = new FutureTask<Integer>(new EdgeImporter(graph, filePath));
                futureTasks.add(future);
                executorService.submit(future);
            }
        }

        int total = 0;
        for (FutureTask<Integer> futureTask : futureTasks) {
            total+= futureTask.get();
        }
        LOGGER.info(String.format("Total data : %d", total));
        graph.tx().commit();
        graph.close();
    }
}
