package storm.benchmark.lib.spout.configurable;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class ConfigurableRandomMessageSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    public static final String FIELDS = "message";
    public static final String MESSAGE_SIZE = "message.size";
    public static final int DEFAULT_MESSAGE_SIZE = 100;
    public static final int DEFAULT_RATE = 1;
    private static final int MESSAGE_BUFFER_SIZE = 100;

    private final int sizeInBytes;
    private long messageCount = 0;
    private SpoutOutputCollector collector;
    private String[] messages = null;
    private final boolean ackEnabled;
    private Random rand = null;
    private int rate;

    public ConfigurableRandomMessageSpout(int rate, int sizeInBytes, boolean ackEnabled) {
        assert rate >= 1;
        this.rate = rate;
        this.sizeInBytes = sizeInBytes;
        this.ackEnabled = ackEnabled;
    }

    public ConfigurableRandomMessageSpout(int rate, boolean ackEnabled) {
        this(rate, DEFAULT_MESSAGE_SIZE, ackEnabled);
    }

    public ConfigurableRandomMessageSpout(boolean ackEnabled) {
        this(DEFAULT_RATE, DEFAULT_MESSAGE_SIZE, ackEnabled);
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.rand = new Random();
        this.collector = collector;
        this.messages = new String[MESSAGE_BUFFER_SIZE];
        for(int i = 0; i < MESSAGE_BUFFER_SIZE; i++) {
            StringBuilder sb = new StringBuilder(this.sizeInBytes);
            for(int j = 0; j < sizeInBytes; j++) {
                sb.append(rand.nextInt(9));
            }
            messages[i] = sb.toString();
        }
    }

    @Override
    public void nextTuple() {
        final String message = messages[rand.nextInt(messages.length)];
        for(int i = 0; i < this.rate; i++) {
            if(ackEnabled) {
                collector.emit(new Values(message), messageCount);
                messageCount++;
            }
            else {
                collector.emit(new Values(message));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELDS));
    }
}
