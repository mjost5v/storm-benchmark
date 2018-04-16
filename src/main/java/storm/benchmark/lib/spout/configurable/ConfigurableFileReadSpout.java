package storm.benchmark.lib.spout.configurable;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.benchmark.tools.FileReader;

import java.util.Map;

public class ConfigurableFileReadSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurableFileReadSpout.class);
    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_FILE = "/resources/A_Tale_of_Two_City.txt";
    public static final boolean DEFAULT_ACK = false;
    public static final int DEFAULT_RATE = 1;
    public static final String FIELDS = "sentence";

    public final boolean ackEnabled;
    public final FileReader reader;
    private SpoutOutputCollector collector;

    private long count = 0;

    private int rate = DEFAULT_RATE;

    public ConfigurableFileReadSpout(int rate, boolean ackEnabled, FileReader reader) {
        assert rate >= 1L;
        this.rate = rate;
        this.ackEnabled = ackEnabled;
        this.reader = reader;
    }

    public ConfigurableFileReadSpout(int rate, boolean ackEnabled, String fileName) {
        this(rate, ackEnabled, new FileReader(fileName));
    }

    public ConfigurableFileReadSpout(int rate, boolean ackEnabled) {
        this(rate, ackEnabled, DEFAULT_FILE);
    }

    public ConfigurableFileReadSpout(int rate) {
        this(rate, DEFAULT_ACK, DEFAULT_FILE);
    }

    public ConfigurableFileReadSpout(boolean ackEnabled) {
        this(DEFAULT_RATE, ackEnabled, DEFAULT_FILE);
    }

    public ConfigurableFileReadSpout() {
        this(DEFAULT_RATE, DEFAULT_ACK, DEFAULT_FILE);
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        for(int x = 0; x < this.rate; x++) {
            if (ackEnabled) {
                collector.emit(new Values(reader.nextLine()), count);
                count++;

            } else {
                collector.emit(new Values(reader.nextLine()));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELDS));
    }
}
