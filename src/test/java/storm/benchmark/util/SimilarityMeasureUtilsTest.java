package storm.benchmark.util;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.junit.Test;
import storm.benchmark.benchmarks.FileReadWordCount;
import storm.benchmark.benchmarks.RollingCount;
import storm.benchmark.benchmarks.SOL;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.benchmarks.common.WordCount;
import storm.benchmark.benchmarks.perf.ConstSpoutIdBoltNullBoltTopo;
import storm.benchmark.benchmarks.perf.ConstSpoutNullBoltTopo;
import storm.benchmark.benchmarks.starter.ResourceAwareWordCount;
import storm.benchmark.benchmarks.starter.SlidingTupleTimestamps;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.fest.assertions.api.Assertions.assertThat;

public class SimilarityMeasureUtilsTest {
    private StormTopology sol() {
        StormBenchmark benchmark = new SOL();
        Config config = new Config();
        config.put(SOL.SPOUT_NUM, SOL.DEFAULT_SPOUT_NUM);
        config.put(SOL.BOLT_NUM, SOL.DEFAULT_BOLT_NUM);
        config.put(SOL.TOPOLOGY_LEVEL, SOL.DEFAULT_NUM_LEVELS);
        StormTopology topology = benchmark.getTopology(config);
        assertThat(topology).isNotNull();

        return topology;
    }

    private SimilarityMeasureUtils.Graph solGraph() {
        StormTopology topology = sol();

        SimilarityMeasureUtils.Graph solGraph = new SimilarityMeasureUtils.Graph(topology);
        assertThat(solGraph).isNotNull();
        return solGraph;
    }

    private StormTopology rollingCount() {
        StormBenchmark benchmark = new RollingCount();
        Config config = new Config();
        config.put(RollingCount.SPOUT_NUM, RollingCount.DEFAULT_SPOUT_NUM);
        config.put(RollingCount.SPLIT_NUM, RollingCount.DEFAULT_SP_BOLT_NUM);
        config.put(RollingCount.COUNTER_NUM, RollingCount.DEFAULT_RC_BOLT_NUM);

        StormTopology topology = benchmark.getTopology(config);
        assertThat(topology).isNotNull();
        return topology;
    }

    private SimilarityMeasureUtils.Graph rollingCountGraph() {
        StormTopology topology = rollingCount();

        SimilarityMeasureUtils.Graph rollingCount = new SimilarityMeasureUtils.Graph(topology);
        assertThat(rollingCount).isNotNull();

        return rollingCount;
    }

    private StormTopology fileWordCount() {
        StormBenchmark benchmark = new FileReadWordCount();
        Config config = new Config();
        config.put(WordCount.SPOUT_NUM, WordCount.DEFAULT_SPOUT_NUM);
        config.put(WordCount.SPLIT_NUM, WordCount.DEFAULT_SPLIT_BOLT_NUM);
        config.put(WordCount.COUNT_NUM, WordCount.DEFAULT_COUNT_BOLT_NUM);
        StormTopology topology = benchmark.getTopology(config);
        assertThat(topology).isNotNull();

        return topology;
    }

    private SimilarityMeasureUtils.Graph fileWordCountGraph() {
        StormTopology topology = fileWordCount();

        SimilarityMeasureUtils.Graph wordCount = new SimilarityMeasureUtils.Graph(topology);
        assertThat(wordCount).isNotNull();

        return wordCount;
    }

    private StormTopology constSpoutIdBoltNullBoltTopo() {
        StormBenchmark benchmark = new ConstSpoutIdBoltNullBoltTopo();
        Config config = new Config();
        config.put(ConstSpoutIdBoltNullBoltTopo.SPOUT_NUM, ConstSpoutIdBoltNullBoltTopo.DEFAULT_SPOUT_NUM);
        config.put(ConstSpoutIdBoltNullBoltTopo.ID_NUM, ConstSpoutIdBoltNullBoltTopo.DEFAULT_ID_BOLT_NUM);
        config.put(ConstSpoutIdBoltNullBoltTopo.NULL_NUM, ConstSpoutIdBoltNullBoltTopo.DEFAULT_NULL_BOLT_NUM);
        StormTopology stormTopology = benchmark.getTopology(config);
        assertThat(stormTopology).isNotNull();

        return stormTopology;
    }

    private SimilarityMeasureUtils.Graph constSpoutIdBoltNullBoltTopoGraph() {
        StormTopology stormTopology = constSpoutIdBoltNullBoltTopo();

        SimilarityMeasureUtils.Graph graph = new SimilarityMeasureUtils.Graph(stormTopology);
        assertThat(graph).isNotNull();

        return graph;
    }

    private StormTopology constSpoutNullBoltTopo() {
        StormBenchmark benchmark = new ConstSpoutNullBoltTopo();
        Config config = new Config();
        config.put(ConstSpoutNullBoltTopo.SPOUT_NUM, ConstSpoutNullBoltTopo.DEFAULT_SPOUT_NUM);
        config.put(ConstSpoutNullBoltTopo.BOLT_NUM, ConstSpoutNullBoltTopo.DEFAULT_BOLT_NUM);
        config.put(ConstSpoutNullBoltTopo.GROUPING, ConstSpoutNullBoltTopo.DEFAULT_GROUPING);
        StormTopology stormTopology = benchmark.getTopology(config);
        assertThat(stormTopology).isNotNull();

        return stormTopology;
    }

    private SimilarityMeasureUtils.Graph constSpoutNullBoltTopoGraph() {
        StormTopology stormTopology = constSpoutNullBoltTopo();

        SimilarityMeasureUtils.Graph graph = new SimilarityMeasureUtils.Graph(stormTopology);
        assertThat(graph).isNotNull();

        return graph;
    }

    private StormTopology resourceAwareWordCount() {
        StormBenchmark benchmark = new ResourceAwareWordCount();
        Config config = new Config();
        config.put(ResourceAwareWordCount.SPOUT_NUM, ResourceAwareWordCount.DEFAULT_SPOUT_NUM);
        config.put(ResourceAwareWordCount.SPLIT_NUM, ResourceAwareWordCount.DEFAULT_SPLIT_BOLT_NUM);
        config.put(ResourceAwareWordCount.COUNT_NUM, ResourceAwareWordCount.DEFAULT_SPLIT_BOLT_NUM);
        StormTopology stormTopology = benchmark.getTopology(config);
        assertThat(stormTopology).isNotNull();

        return stormTopology;
    }

    private SimilarityMeasureUtils.Graph resourceAwareWordCountGraph() {
        StormTopology stormTopology = resourceAwareWordCount();

        SimilarityMeasureUtils.Graph graph = new SimilarityMeasureUtils.Graph(stormTopology);
        assertThat(graph).isNotNull();

        return graph;
    }

    private StormTopology slideTupleTimestamps() {
        StormBenchmark benchmark = new SlidingTupleTimestamps();
        Config config = new Config();
        config.put(SlidingTupleTimestamps.SPOUT_NUM, SlidingTupleTimestamps.DEFAULT_SPOUT_NUM);
        config.put(SlidingTupleTimestamps.SLIDING_SUM_NUM, SlidingTupleTimestamps.DEFAULT_SLIDING_SUM_NUM);
        config.put(SlidingTupleTimestamps.PRINTER_NUM, SlidingTupleTimestamps.DEFAULT_PRINTER_NUM);
        StormTopology stormTopology = benchmark.getTopology(config);
        assertThat(stormTopology).isNotNull();

        return stormTopology;
    }

    private SimilarityMeasureUtils.Graph slideTupleTimestampsGraph() {
        StormTopology stormTopology = slideTupleTimestamps();

        SimilarityMeasureUtils.Graph graph = new SimilarityMeasureUtils.Graph(stormTopology);
        assertThat(graph);

        return graph;
    }

    @Test
    public void testBuildSOLGraph() {
        solGraph();
    }

    @Test
    public void testBuildRollingCountGraph() {
        rollingCountGraph();
    }

    @Test
    public void testBuildFileWordCount() {
        fileWordCountGraph();
    }

    @Test
    public void testBuildConstIdBoltNullBoltTopo() {
        constSpoutIdBoltNullBoltTopoGraph();
    }

    @Test
    public void testBuildConstSpoutNullBoltTopo() {
        constSpoutNullBoltTopoGraph();
    }

    @Test
    public void testResourceAwareWordCount() {
        resourceAwareWordCountGraph();
    }

    @Test
    public void testSlidingTupleTimestamps() {
        slideTupleTimestampsGraph();
    }

    @Test
    public void testSimilarities() throws IOException {
        StormTopology[] trainedTopologies = {sol(), rollingCount(), fileWordCount()};
        String[] trainedNames = {"SOL", "RollingCount", "WordCount"};

        StormTopology[] testTopologies = {constSpoutIdBoltNullBoltTopo(), constSpoutNullBoltTopo(), resourceAwareWordCount(), slideTupleTimestamps()};
        String[] testNames = {"ConstSpoutIdBoltNullBoltTopo", "ConstSpoutNullBoltTopo", "ResourceAwareWordCount", "SlidingTupleTimestamps"};

        File outCsv = new File("similarities.csv");
        PrintWriter writer = new PrintWriter(outCsv);

        // write header
        writer.println("TrainTopology,TestTopology,Simple Similarity,Aggressive Similarity");
        System.out.println("TrainTopology,TestTopology,Simple Similarity,Aggressive Similarity");

        for(int i = 0; i < trainedTopologies.length; i++) {
            StormTopology trainedTopology = trainedTopologies[i];
            String trainedName = trainedNames[i];

            for(int j = 0; j < testTopologies.length; j++) {
                StormTopology testTopology = testTopologies[j];
                String testName = testNames[j];

                double simpleSimilariy = SimilarityMeasureUtils.scoreSimilarity(trainedTopology, testTopology);
                double aggressiveSimilarity = SimilarityMeasureUtils.scoreSimilarityAggressive(trainedTopology, testTopology);

                writer.println(String.format("%s,%s,%f,%f", trainedName, testName, simpleSimilariy, aggressiveSimilarity));
                System.out.println(String.format("%s,%s,%f,%f", trainedName, testName, simpleSimilariy, aggressiveSimilarity));
            }
        }

        writer.close();
    }
}
