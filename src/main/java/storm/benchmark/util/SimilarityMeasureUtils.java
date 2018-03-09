package storm.benchmark.util;

import org.apache.storm.generated.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimilarityMeasureUtils {
    public static final double EPSILON = 1E-4;

    // from https://wadsashika.wordpress.com/2014/09/19/measuring-graph-similarity-using-neighbor-matching/
    public static class Graph {
        private Integer[][] graph;
        private int graphSize;
        private List<Integer> nodeList;
        private List<List<Integer>> inDegreeNodeList;
        private List<List<Integer>> outDegreeNodeList;

        public Graph(StormTopology stormTopology) {
            int counter = 0;
            this.nodeList = new ArrayList<>();
            Map<String, List<Integer>> componentToIds = new HashMap<>();
            // put in spouts
            if(stormTopology.get_spouts() != null) {
                for (Map.Entry<String, SpoutSpec> entry : stormTopology.get_spouts().entrySet()) {
                    String key = entry.getKey();
                    List<Integer> ids = new ArrayList<>();
                    for (int i = 0; i < entry.getValue().get_common().get_parallelism_hint(); i++) {
                        ids.add(counter);
                        this.nodeList.add(counter);
                        counter++;
                    }
                    componentToIds.put(key, ids);
                }
            }

            // put in state spouts
            if(stormTopology.get_state_spouts() != null) {
                for (Map.Entry<String, StateSpoutSpec> entry: stormTopology.get_state_spouts().entrySet()) {
                    String key = entry.getKey();
                    List<Integer> ids = new ArrayList<>();
                    for (int i = 0; i < entry.getValue().get_common().get_parallelism_hint(); i++) {
                        ids.add(counter);
                        this.nodeList.add(counter);
                        counter++;
                    }
                    componentToIds.put(key, ids);
                }
            }

            //put in bolts
            if(stormTopology.get_bolts() != null) {
                for (Map.Entry<String, Bolt> entry: stormTopology.get_bolts().entrySet()) {
                    String key = entry.getKey();
                    List<Integer> ids = new ArrayList<>();
                    for(int i = 0; i < entry.getValue().get_common().get_parallelism_hint(); i++) {
                        ids.add(counter);
                        this.nodeList.add(counter);
                        counter++;
                    }
                    componentToIds.put(key, ids);
                }
            }

            // set up graph size
            this.graphSize = counter;
            this.graph = new Integer[this.graphSize][this.graphSize];
            // init graph to 0's
            for(int i = 0; i < this.graph.length; i++) {
                for(int j = 0; j < this.graph.length; j++) {
                    this.graph[i][j] = 0;
                }
            }

            // build graph
            if(stormTopology.get_bolts() != null) {
                for(Map.Entry<String, Bolt> entry: stormTopology.get_bolts().entrySet()) {
                    String destKey = entry.getKey();
                    for(Map.Entry<GlobalStreamId, Grouping> streamIdGroupingEntry: entry.getValue().get_common().get_inputs().entrySet()) {
                        String srcKey = streamIdGroupingEntry.getKey().get_componentId();
                        for(Integer srcId: componentToIds.get(srcKey)) {
                            for(Integer destId: componentToIds.get(destKey)) {
                                this.graph[srcId][destId] = 1;
                            }
                        }
                    }
                }
            }

            // build degree lists
            this.inDegreeNodeList = new ArrayList<>();
            this.outDegreeNodeList = new ArrayList<>();
            for(int i = 0; i < this.graphSize; i++) {
                inDegreeNodeList.add(new ArrayList<>());
                outDegreeNodeList.add(new ArrayList<>());
            }
            for(int i = 0; i < graphSize; i++) {
                for(int j = 0; j < graphSize; j++) {
                    if(graph[i][j] != 0) {
                        inDegreeNodeList.get(j).add(i);
                        outDegreeNodeList.get(i).add(j);
                    }
                }
            }
        }
    }

    public static class SimilarityMatrices {
        double[][] inNodeSimilarity;
        double[][] outNodeSimilarity;
        double[][] similarity;

        public SimilarityMatrices() {
        }

        public SimilarityMatrices(double[][] inNodeSimilarity, double[][] outNodeSimilarity, double[][] similarity) {
            this.inNodeSimilarity = inNodeSimilarity;
            this.outNodeSimilarity = outNodeSimilarity;
            this.similarity = similarity;
        }

        public double[][] getInNodeSimilarity() {
            return inNodeSimilarity;
        }

        public void setInNodeSimilarity(double[][] inNodeSimilarity) {
            this.inNodeSimilarity = inNodeSimilarity;
        }

        public double[][] getOutNodeSimilarity() {
            return outNodeSimilarity;
        }

        public void setOutNodeSimilarity(double[][] outNodeSimilarity) {
            this.outNodeSimilarity = outNodeSimilarity;
        }

        public double[][] getSimilarity() {
            return similarity;
        }

        public void setSimilarity(double[][] similarity) {
            this.similarity = similarity;
        }
    }

    public static double scoreSimilarity(StormTopology a, StormTopology b) {
        Graph graphA = new Graph(a);
        Graph graphB = new Graph(b);
        SimilarityMatrices similarityMatrices = buildSimilarityMatrices(graphA, graphB);
        calculateSimilarity(similarityMatrices, graphA, graphB, EPSILON);
        return calcFinalSimilarity(similarityMatrices.similarity, graphA, graphB);
    }

    private static SimilarityMatrices buildSimilarityMatrices(Graph graphA, Graph graphB) {
        double[][] inNodeSimilarity = new double[graphA.graphSize][graphB.graphSize];
        double[][] outNodeSimilarity = new double[graphA.graphSize][graphB.graphSize];
        double[][] similarity = new double[graphA.graphSize][graphB.graphSize];

        for(int i = 0; i < graphA.graphSize; i++) {
            for(int j = 0; j < graphB.graphSize; j++) {
                int maxDegree = Math.max(graphA.inDegreeNodeList.get(i).size(), graphB.inDegreeNodeList.get(j).size());
                if(maxDegree > 0) {
                    inNodeSimilarity[i][j] = Math.min(graphA.inDegreeNodeList.get(i).size(), graphB.inDegreeNodeList.get(j).size()) / ((double) maxDegree);
                }
                else {
                    inNodeSimilarity[i][j] = 0.0;
                }

                maxDegree = Math.max(graphA.outDegreeNodeList.get(i).size(), graphB.outDegreeNodeList.get(j).size());
                if(maxDegree > 0) {
                    outNodeSimilarity[i][j] = Math.min(graphA.outDegreeNodeList.get(i).size(), graphB.outDegreeNodeList.get(j).size()) / ((double) maxDegree);
                }
                else {
                    outNodeSimilarity[i][j] = 0.0;
                }
            }
        }
        for(int i = 0; i < graphA.graphSize; i++) {
            for(int j = 0; j < graphB.graphSize; j++) {
                similarity[i][j] = (inNodeSimilarity[i][j] + outNodeSimilarity[i][j]) / 2.0;
            }
        }

        return new SimilarityMatrices(inNodeSimilarity, outNodeSimilarity, similarity);
    }

    private static void calculateSimilarity(SimilarityMatrices similarityMatrices, Graph graphA, Graph graphB, double epsilon) {
        double maxDifference = 0.0;
        boolean terminate = false;

        while(!terminate) {
            maxDifference = 0.0;
            for(int i = 0; i < graphA.graphSize; i++) {
                for(int j = 0; j < graphB.graphSize; j++) {
                    // calculate in-degree similarities
                    double simularitySum;
                    int maxDegree = Math.max(graphA.inDegreeNodeList.get(i).size(), graphB.inDegreeNodeList.get(j).size());
                    int minDegree = Math.min(graphA.inDegreeNodeList.get(i).size(), graphB.inDegreeNodeList.get(j).size());
                    if (minDegree == graphA.inDegreeNodeList.get(i).size()) {
                        simularitySum = calcSimilaritySum(similarityMatrices.similarity, graphA.inDegreeNodeList.get(i), graphB.inDegreeNodeList.get(j), true);
                    }
                    else {
                        simularitySum = calcSimilaritySum(similarityMatrices.similarity, graphB.inDegreeNodeList.get(j), graphA.inDegreeNodeList.get(i), false);
                    }

                    if(maxDegree <= 0 && simularitySum <= 0.0) {
                        similarityMatrices.inNodeSimilarity[i][j] = 1.0;
                    }
                    else if(maxDegree <= 0) {
                        similarityMatrices.inNodeSimilarity[i][j] = 0.0;
                    }
                    else {
                        similarityMatrices.inNodeSimilarity[i][j] = simularitySum / ((double) maxDegree);
                    }

                    // calculate out-degree similarities
                    simularitySum = 0.0;
                    maxDegree = Math.max(graphA.outDegreeNodeList.get(i).size(), graphB.outDegreeNodeList.get(j).size());
                    minDegree = Math.min(graphA.outDegreeNodeList.get(i).size(), graphB.outDegreeNodeList.get(j).size());
                    if(minDegree == graphA.outDegreeNodeList.get(i).size()) {
                        simularitySum = calcSimilaritySum(similarityMatrices.similarity, graphA.outDegreeNodeList.get(i), graphB.outDegreeNodeList.get(j), true);
                    }
                    else {
                        simularitySum = calcSimilaritySum(similarityMatrices.similarity, graphB.outDegreeNodeList.get(j), graphA.outDegreeNodeList.get(i), false);
                    }

                    if(maxDegree <= 0 && simularitySum <= 0.0) {
                        similarityMatrices.outNodeSimilarity[i][j] = 1.0;
                    }
                    else if(maxDegree <= 0) {
                        similarityMatrices.outNodeSimilarity[i][j] = 0.0;
                    }
                    else {
                        similarityMatrices.outNodeSimilarity[i][j] = simularitySum / ((double)maxDegree);
                    }
                }
            }

            for(int i = 0; i < graphA.graphSize; i++) {
                for(int j = 0; j < graphB.graphSize; j++) {
                    double temp = (similarityMatrices.inNodeSimilarity[i][j] + similarityMatrices.outNodeSimilarity[i][j]) / 2.0;
                    double diff = Math.abs(similarityMatrices.similarity[i][j] - temp);
                    if(diff > maxDifference) {
                        maxDifference = diff;
                    }
                    similarityMatrices.similarity[i][j] = temp;
                }
            }
            terminate = maxDifference < epsilon;
        }
    }

    private static double calcSimilaritySum(double[][] similarity, List<Integer> neighborListMin, List<Integer> neighborListMax, boolean useNodeToKey) {
        Map<Integer, Double> valueMap = new HashMap<>();
        for(Integer node: neighborListMin) {
            double max = 0.0;
            int maxIndex = -1;
            for(Integer key: neighborListMax) {
                if(!valueMap.containsKey(key)) {
                    if(useNodeToKey) {
                        if (max < similarity[node][key]) {
                            max = similarity[node][key];
                            maxIndex = key;
                        }
                    }
                    else {
                        if(max < similarity[key][node]) {
                            max = similarity[key][node];
                            maxIndex = key;
                        }
                    }
                }
            }
            valueMap.put(maxIndex, max);
        }

        return valueMap.values().stream().reduce(0.0, (a, b) -> a + b);
    }

    private static double calcFinalSimilarity(double[][] similarity, Graph graphA, Graph graphB) {
        if(graphA.graphSize < graphB.graphSize) {
            return calcSimilaritySum(similarity, graphA.nodeList, graphB.nodeList, true);
        }
        else {
            return calcSimilaritySum(similarity, graphA.nodeList, graphB.nodeList, false);
        }
    }
}
