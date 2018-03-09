
/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License
*/
package storm.benchmark.benchmarks.perf;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.bolt.perf.DevNullBolt;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;

/***
 * This topo helps measure speed of reading from Kafka
 *   Spout Reads from Kafka.
 *   Bolt acks and discards tuples
 */
public class KafkaSpoutNullBoltTopo extends StormBenchmark {
    public static final String SPOUT_ID = "spout";
    public static final String SPOUT_NUM = "component.spout_num";
    public static final String BOLT_ID = "bolt";
    public static final String BOLT_NUM = "component.bolt_num";

    public static final int DEFAULT_SPOUT_NUM = 4;
    public static final int DEFAULT_BOLT_NUM = 4;

    private IRichSpout spout;

    @Override
    public StormTopology getTopology(Config config) {
        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

        spout = new KafkaSpout(KafkaUtils.getSpoutConfig(config, new SchemeAsMultiScheme(new StringScheme())));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(BOLT_ID, new DevNullBolt(), boltNum)
                .localOrShuffleGrouping(SPOUT_ID);
        return builder.createTopology();
    }
}
