package storm.benchmark.benchmarks.configurable;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import storm.benchmark.benchmarks.common.ConfigurableWordCount;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.benchmarks.common.WordCount;
import storm.benchmark.lib.bolt.RollingBolt;
import storm.benchmark.lib.bolt.configurable.ConfigurableRollingCountBolt;
import storm.benchmark.lib.spout.configurable.ConfigurableFileReadSpout;
import storm.benchmark.util.BenchmarkUtils;

public class ConfigurableRollingCount extends StormBenchmark {
    private static final String WINDOW_LENGTH = "window.length";
    private static final String EMIT_FREQ = "emit.frequency";

    public static final String SPOUT_ID = "spout";
    public static final String SPOUT_NUM = "component.spout_num";
    public static final String SPOUT_RATE = "component.spout_rate";
    public static final String SPLIT_ID = "split";
    public static final String SPLIT_NUM = "component.split_bolt_num";
    public static final String SPLIT_RATE = "component.split_rate";
    public static final String COUNTER_ID = "rolling_count";
    public static final String COUNTER_NUM = "component.rolling_count_bolt_num";
    public static final String COUNTER_RATE = "component.rolling_count_bolt_rate";

    public static final int DEFAULT_SPOUT_NUM = 4;
    public static final int DEFAULT_SP_BOLT_NUM = 8;
    public static final int DEFAULT_RC_BOLT_NUM = 8;

    public static final int DEFAULT_SPOUT_RATE = 1;
    public static final int DEFAULT_SP_BOLT_RATE = 1;
    public static final int DEFAULT_RC_BOLT_RATE = 1;

    private IRichSpout spout;

    @Override
    public StormTopology getTopology(Config config) {

        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int spBoltNum = BenchmarkUtils.getInt(config, SPLIT_NUM, DEFAULT_SP_BOLT_NUM);
        final int rcBoltNum = BenchmarkUtils.getInt(config, COUNTER_NUM, DEFAULT_RC_BOLT_NUM);

        final int spoutRate = BenchmarkUtils.getInt(config, SPOUT_RATE, DEFAULT_SPOUT_RATE);
        final int spBoltRate = BenchmarkUtils.getInt(config, SPLIT_RATE, DEFAULT_SP_BOLT_RATE);
        final int rcBoltRate = BenchmarkUtils.getInt(config, COUNTER_RATE, DEFAULT_RC_BOLT_RATE);

        final int windowLength = BenchmarkUtils.getInt(config, WINDOW_LENGTH,
                RollingBolt.DEFAULT_SLIDING_WINDOW_IN_SECONDS);
        final int emitFreq = BenchmarkUtils.getInt(config, EMIT_FREQ,
                RollingBolt.DEFAULT_EMIT_FREQUENCY_IN_SECONDS);

        spout = new ConfigurableFileReadSpout(spoutRate, BenchmarkUtils.ifAckEnabled(config));

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(SPLIT_ID, new ConfigurableWordCount.SplitSentence(spBoltRate), spBoltNum)
                .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(COUNTER_ID, new ConfigurableRollingCountBolt(rcBoltRate, windowLength, emitFreq), rcBoltNum)
                .fieldsGrouping(SPLIT_ID, new Fields(WordCount.SplitSentence.FIELDS));
        return builder.createTopology();
    }
}
