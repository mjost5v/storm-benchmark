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
package storm.benchmark.benchmarks;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.bolt.DevNullBolt;
import storm.benchmark.lib.spout.ConstSpout;
import storm.benchmark.util.BenchmarkUtils;

/***
 * This topo helps measure the messaging speed between a spout and a bolt.
 *  Spout generates a stream of a fixed string.
 *  Bolt will simply ack and discard the tuple received
 */
public class ConstSpoutNullBoltTopo extends StormBenchmark {
    public static final String SPOUT_ID = "spout";
    public static final String SPOUT_NUM = "component.spout_num";
    public static final String BOLT_ID = "bolt";
    public static final String BOLT_NUM = "component.bolt_num";
    public static final String GROUPING = "grouping";

    public static final String LOCAL_GROUPING = "local";
    public static final String SHUFFLE_GROUPING = "shuffle";
    public static final String DEFAULT_GROUPING = LOCAL_GROUPING;
    public static final int DEFAULT_SPOUT_NUM = 4;
    public static final int DEFAULT_BOLT_NUM = 4;

    public static final String SPOUT_MESSAGE = "some data";
    public static final String SPOUT_FIELD = "str";

    private IRichSpout spout;

    @Override
    public StormTopology getTopology(Config config) {

        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);
        final String groupingType = (String) Utils.get(config, GROUPING, DEFAULT_GROUPING);

        spout = new ConstSpout(SPOUT_MESSAGE).withOutputFields(SPOUT_FIELD);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);

        BoltDeclarer bd = builder.setBolt(BOLT_ID, new DevNullBolt(), DEFAULT_BOLT_NUM);
        if(groupingType.equalsIgnoreCase(LOCAL_GROUPING)){
            bd.localOrShuffleGrouping(SPOUT_ID);
        }
        else if(groupingType.equalsIgnoreCase(SHUFFLE_GROUPING)){
            bd.shuffleGrouping(SPOUT_ID);
        }
        else {
            throw new IllegalArgumentException(String.format("grouping must be either %s or %s", LOCAL_GROUPING, SHUFFLE_GROUPING));
        }
        return builder.createTopology();
    }
}
