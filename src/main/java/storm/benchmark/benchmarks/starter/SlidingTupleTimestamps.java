package storm.benchmark.benchmarks.starter;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.bolt.perf.DevNullBolt;
import storm.benchmark.lib.bolt.starter.SlidingWindowSumBolt;
import storm.benchmark.lib.spout.RandomIntegerSpout;
import storm.benchmark.util.BenchmarkUtils;

import java.util.concurrent.TimeUnit;

public class SlidingTupleTimestamps extends StormBenchmark {
    public static final String SPOUT_ID = "spout";
    public static final String SPOUT_NUM = "component.spout_num";

    public static final String SLIDING_SUM_ID = "sliding_sum";
    public static final String SLIDING_SUM_NUM = "component.sliding_sum_num";

    public static final String PRINTER_ID = "printer";
    public static final String PRINTER_NUM = "component.printer_num";

    public static final String SLIDING_WINDOW_SIZE_SECONDS_NUM = "sliding_window_size_seconds_num";

    public static final int DEFAULT_SPOUT_NUM = 1;
    public static final int DEFAULT_SLIDING_SUM_NUM = 8;
    public static final int DEFAULT_PRINTER_NUM = 8;
    public static final int DEFAULT_SLIDING_WINDOW_SIZE_SECONDS_NUM = 5;

    private IRichSpout spout;

    @Override
    public StormTopology getTopology(Config config) {
        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int slidingSumNum = BenchmarkUtils.getInt(config, SLIDING_SUM_NUM, DEFAULT_SLIDING_SUM_NUM);
        final int printerNum = BenchmarkUtils.getInt(config, PRINTER_NUM, DEFAULT_PRINTER_NUM);
        final int slidingWindowSizeSeconds = BenchmarkUtils.getInt(config, SLIDING_WINDOW_SIZE_SECONDS_NUM, DEFAULT_SLIDING_WINDOW_SIZE_SECONDS_NUM);

        spout = new RandomIntegerSpout();

        BaseWindowedBolt slidingSumBolt = new SlidingWindowSumBolt()
                .withWindow(new BaseWindowedBolt.Duration(slidingWindowSizeSeconds, TimeUnit.SECONDS))
                .withTimestampField("ts")
                .withLag(new BaseWindowedBolt.Duration(slidingWindowSizeSeconds, TimeUnit.SECONDS));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(SLIDING_SUM_ID, slidingSumBolt, slidingSumNum)
               .shuffleGrouping(SPOUT_ID);
        builder.setBolt(PRINTER_ID, new DevNullBolt(), printerNum)
                .shuffleGrouping(SLIDING_SUM_ID);
        return builder.createTopology();
    }
}
