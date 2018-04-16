package storm.benchmark.benchmarks.configurable;

import com.google.common.collect.Sets;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.bolt.configurable.ConfigurableConstBolt;
import storm.benchmark.lib.bolt.perf.ConstBolt;
import storm.benchmark.lib.spout.RandomMessageSpout;
import storm.benchmark.lib.spout.configurable.ConfigurableRandomMessageSpout;
import storm.benchmark.metrics.BasicMetricsCollector;
import storm.benchmark.metrics.IMetricsCollector;
import storm.benchmark.util.BenchmarkUtils;

public class ConfigurableSOL extends StormBenchmark {
    public static final String TOPOLOGY_LEVEL = "topology.level";
    public static final String SPOUT_ID = "spout";
    public static final String SPOUT_NUM = "component.spout_num";
    public static final String SPOUT_RATE = "component.spout_rate";
    public static final String BOLT_ID = "bolt";
    public static final String BOLT_NUM = "component.bolt_num";
    public static final String BOLT_RATE = "component.bolt_rate";

    public static final int DEFAULT_NUM_LEVELS = 2;
    public static final int DEFAULT_SPOUT_NUM = 4;
    public static final int DEFAULT_BOLT_NUM = 4;
    public static final int DEFAULT_SPOUT_RATE = 1;
    public static final int DEFAULT_BOLT_RATE = 1;

    private IRichSpout spout;

    @Override
    public StormTopology getTopology(Config config) {
        final int numLevels = BenchmarkUtils.getInt(config, TOPOLOGY_LEVEL, DEFAULT_NUM_LEVELS);
        final int msgSize = BenchmarkUtils.getInt(config, RandomMessageSpout.MESSAGE_SIZE,
                RandomMessageSpout.DEFAULT_MESSAGE_SIZE);
        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int spoutRate = BenchmarkUtils.getInt(config, SPOUT_RATE, DEFAULT_SPOUT_RATE);
        final int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);
        final int boltRate = BenchmarkUtils.getInt(config, BOLT_RATE, DEFAULT_BOLT_RATE);

        spout = new ConfigurableRandomMessageSpout(spoutRate, msgSize, BenchmarkUtils.ifAckEnabled(config));

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(BOLT_ID + 1, new ConfigurableConstBolt(boltRate), boltNum)
                .shuffleGrouping(SPOUT_ID);
        for (int levelNum = 2; levelNum <= numLevels - 1; levelNum++) {
            builder.setBolt(BOLT_ID + levelNum, new ConstBolt(), boltNum)
                    .shuffleGrouping(BOLT_ID + (levelNum - 1));
        }
        return builder.createTopology();
    }

    @Override
    public IMetricsCollector getMetricsCollector(Config config, StormTopology topology) {
        return new BasicMetricsCollector(config, topology,
                Sets.newHashSet(IMetricsCollector.MetricsItem.ALL));
    }
}
