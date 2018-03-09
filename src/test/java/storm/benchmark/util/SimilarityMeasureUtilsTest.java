package storm.benchmark.util;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.junit.Test;
import storm.benchmark.benchmarks.FileReadWordCount;
import storm.benchmark.benchmarks.RollingCount;
import storm.benchmark.benchmarks.SOL;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.benchmarks.common.WordCount;

import static org.fest.assertions.api.Assertions.assertThat;

public class SimilarityMeasureUtilsTest {
    private SimilarityMeasureUtils.Graph solGraph() {
        StormBenchmark benchmark = new SOL();
        Config config = new Config();
        config.put(SOL.SPOUT_NUM, 4);
        config.put(SOL.BOLT_NUM, 3);
        config.put(SOL.TOPOLOGY_LEVEL, 3);
        StormTopology topology = benchmark.getTopology(config);
        assertThat(topology).isNotNull();

        SimilarityMeasureUtils.Graph solGraph = new SimilarityMeasureUtils.Graph(topology);
        assertThat(solGraph).isNotNull();
        return solGraph;
    }

    private SimilarityMeasureUtils.Graph rollingCount() {
        StormBenchmark benchmark = new RollingCount();
        Config config = new Config();
        config.put(RollingCount.SPOUT_NUM, 4);
        config.put(RollingCount.SPLIT_NUM, 5);
        config.put(RollingCount.COUNTER_NUM, 3);

        StormTopology topology = benchmark.getTopology(config);
        assertThat(topology).isNotNull();

        SimilarityMeasureUtils.Graph rollingCount = new SimilarityMeasureUtils.Graph(topology);
        assertThat(rollingCount).isNotNull();

        return rollingCount;
    }

    private SimilarityMeasureUtils.Graph fileWordCount() {
        StormBenchmark benchmark = new FileReadWordCount();
        Config config = new Config();
        config.put(WordCount.SPOUT_NUM, 3);
        config.put(WordCount.SPLIT_NUM, 4);
        config.put(WordCount.COUNT_NUM, 5);
        StormTopology topology = benchmark.getTopology(config);
        assertThat(topology).isNotNull();

        SimilarityMeasureUtils.Graph wordCount = new SimilarityMeasureUtils.Graph(topology);
        assertThat(wordCount).isNotNull();

        return wordCount;
    }

    @Test
    public void testBuildSOLGraph() {
        solGraph();
    }

    @Test
    public void testBuildRollingCountGraph() {
        rollingCount();
    }
}
