package storm.benchmark.benchmarks.common;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.benchmark.lib.operation.WordSplit;
import storm.benchmark.util.BenchmarkUtils;

import java.util.HashMap;
import java.util.Map;

public abstract class ConfigurableWordCount extends StormBenchmark {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurableWordCount.class);

    public static final String SPOUT_ID = "spout";
    public static final String SPOUT_NUM = "component.spout_num";
    public static final String SPOUT_RATE = "component.spout_rate";
    public static final String SPLIT_ID = "split";
    public static final String SPLIT_NUM = "component.split_bolt_num";
    public static final String SPLIT_RATE = "component.split_rate";
    public static final String COUNT_ID = "count";
    public static final String COUNT_NUM = "component.count_bolt_num";
    public static final String COUNT_RATE = "component.count_rate";
    public static final int DEFAULT_SPOUT_NUM = 8;
    public static final int DEFAULT_SPLIT_BOLT_NUM = 4;
    public static final int DEFAULT_COUNT_BOLT_NUM = 4;
    public static final int DEFAULT_SPOUT_RATE = 1;
    public static final int DEFAULT_SPLIT_BOLT_RATE = 1;
    public static final int DEFAULT_COUNT_BOLT_RATE = 1;

    protected IRichSpout spout;

    @Override
    public StormTopology getTopology(Config config) {

        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int spBoltNum = BenchmarkUtils.getInt(config, SPLIT_NUM, DEFAULT_SPLIT_BOLT_NUM);
        final int cntBoltNum = BenchmarkUtils.getInt(config, COUNT_NUM, DEFAULT_COUNT_BOLT_NUM);

        final int spoutRate = BenchmarkUtils.getInt(config, SPOUT_RATE, DEFAULT_SPOUT_RATE);
        final int splitRate = BenchmarkUtils.getInt(config, SPLIT_RATE, DEFAULT_SPLIT_BOLT_RATE);
        final int countRate = BenchmarkUtils.getInt(config, COUNT_RATE, DEFAULT_COUNT_BOLT_RATE);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(SPLIT_ID, new SplitSentence(splitRate), spBoltNum).localOrShuffleGrouping(
                SPOUT_ID);
        builder.setBolt(COUNT_ID, new WordCount.Count(), cntBoltNum).fieldsGrouping(SPLIT_ID,
                new Fields(WordCount.SplitSentence.FIELDS));

        return builder.createTopology();
    }

    public static class SplitSentence extends BaseBasicBolt {
        public static final String FIELDS = "word";
        private int rate;

        public SplitSentence() {
            this.rate = DEFAULT_SPLIT_BOLT_RATE;
        }

        public SplitSentence(int rate) {
            assert rate >= 1;
            this.rate = rate;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            for (String word : WordSplit.splitSentence(input.getString(0))) {
                for(int i = 0; i < this.rate; i++) {
                    collector.emit(new Values(word));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

    public static class Count extends BaseBasicBolt {
        public static final String FIELDS_WORD = "word";
        public static final String FIELDS_COUNT = "count";
        private int rate;

        Map<String, Integer> counts = new HashMap<>();

        public Count() {
            this.rate = DEFAULT_SPOUT_RATE;
        }

        public Count(int rate) {
            this.rate = rate;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            for(int i = 0; i < this.rate; i++) {
                String word = tuple.getString(0);
                Integer count = counts.getOrDefault(word, 0);
                count++;
                counts.put(word, count);
                collector.emit(new Values(word, count));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS_WORD, FIELDS_COUNT));
        }
    }
}
