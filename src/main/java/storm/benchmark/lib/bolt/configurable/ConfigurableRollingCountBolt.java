package storm.benchmark.lib.bolt.configurable;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.benchmark.lib.bolt.RollingBolt;
import storm.benchmark.lib.reducer.LongSummer;
import storm.benchmark.tools.SlidingWindow;

import java.util.Map;

public class ConfigurableRollingCountBolt extends RollingBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurableRollingCountBolt.class);
    public static final String FIELDS_OBJ = "obj";
    public static final String FIELDS_CNT = "count";
    public static final int DEFAULT_RATE = 1;

    private int rate;

    private final SlidingWindow<Object, Long> window;

    public ConfigurableRollingCountBolt(int rate, int winLen, int emitFreq) {
        super(winLen, emitFreq);
        assert rate >= 1;
        this.rate = rate;
        this.window = new SlidingWindow<>(new LongSummer(), getWindowChunks());
    }

    public ConfigurableRollingCountBolt(int rate) {
        this(rate, DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public ConfigurableRollingCountBolt() {
        this(DEFAULT_RATE);
    }

    @Override
    public void emitCurrentWindow(BasicOutputCollector collector) {
        emitCurrentWindowCounts(collector);
    }

    @Override
    public void updateCurrentWindow(Tuple tuple) {
        countObj(tuple);
    }

    private void emitCurrentWindowCounts(BasicOutputCollector collector) {
        Map<Object, Long> counts = window.reduceThenAdvanceWindow();
        for (Map.Entry<Object, Long> entry : counts.entrySet()) {
            Object obj = entry.getKey();
            Long count = entry.getValue();
            LOG.info(String.format("get %d %s in last %d seconds", count, obj, windowLengthInSeconds));
            for(int x = 0; x < this.rate; x++) {
                collector.emit(new Values(obj, count));
            }
        }
    }

    private void countObj(Tuple tuple) {
        Object obj = tuple.getValue(0);
        window.add(obj, (long) 1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELDS_OBJ, FIELDS_CNT));
    }
}
