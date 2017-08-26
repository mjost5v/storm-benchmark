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
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.bolt.DevNullBolt;
import storm.benchmark.lib.bolt.IdBolt;
import storm.benchmark.lib.spout.ConstSpout;
import storm.benchmark.util.BenchmarkUtils;

/**
 * ConstSpout -> IdBolt -> DevNullBolt
 * This topology measures speed of messaging between spouts->bolt  and  bolt->bolt
 *   ConstSpout : Continuously emits a constant string
 *   IdBolt : clones and emits input tuples
 *   DevNullBolt : discards incoming tuples
 */
public class ConstSpoutIdBoltNullBoltTopo extends StormBenchmark {

    public final static String SPOUT_ID = "spout";
    public final static String SPOUT_NUM = "component.spout_num";
    public final static String ID_ID = "id";
    public final static String ID_NUM = "component.id_bolt_num";
    public final static String NULL_ID = "null";
    public final static String NULL_NUM = "component.null_bolt_num";

    public static final int DEFAULT_SPOUT_NUM = 4;
    public static final int DEFAULT_ID_BOLT_NUM = 4;
    public static final int DEFAULT_NULL_BOLT_NUM = 4;

    public static final String SPOUT_OUTPUT_MESSAGE = "some data";
    public static final String SPOUT_OUTPUT_FIELD = "str";

    private IRichSpout spout;

    @Override
    public StormTopology getTopology(Config config) {
        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int idBoltNum = BenchmarkUtils.getInt(config, ID_NUM, DEFAULT_ID_BOLT_NUM);
        final int nullBoltNum = BenchmarkUtils.getInt(config, NULL_NUM, DEFAULT_NULL_BOLT_NUM);
        spout = new ConstSpout(SPOUT_OUTPUT_MESSAGE).withOutputFields(SPOUT_OUTPUT_FIELD);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(ID_ID, new IdBolt(), idBoltNum)
                .localOrShuffleGrouping(SPOUT_ID);
        builder.setBolt(NULL_ID, new DevNullBolt(), nullBoltNum)
                .localOrShuffleGrouping(ID_ID);
        return builder.createTopology();
    }
}
