package storm.benchmark.benchmarks.configurable;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import storm.benchmark.benchmarks.common.ConfigurableWordCount;
import storm.benchmark.lib.spout.configurable.ConfigurableFileReadSpout;
import storm.benchmark.util.BenchmarkUtils;

public class ConfigurableFileWordCount extends ConfigurableWordCount {
    @Override
    public StormTopology getTopology(Config config) {
        spout = new ConfigurableFileReadSpout(BenchmarkUtils.getInt(config, SPOUT_RATE, DEFAULT_SPOUT_RATE),
                BenchmarkUtils.ifAckEnabled(config));
        return super.getTopology(config);
    }
}
