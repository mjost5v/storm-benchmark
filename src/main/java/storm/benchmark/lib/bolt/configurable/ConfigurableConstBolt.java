package storm.benchmark.lib.bolt.configurable;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class ConfigurableConstBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1L;
    public static final String FIELDS = "message";
    public static final int DEFAULT_RATE = 1;

    private int rate;

    public ConfigurableConstBolt(int rate) {
        this.rate = rate;
    }

    public ConfigurableConstBolt() {
        this(DEFAULT_RATE);
    }

    @Override
    public void prepare(Map conf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        for(int i = 0; i < this.rate; i++) {
            collector.emit(new Values(tuple.getValue(0)));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELDS));
    }
}
