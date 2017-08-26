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
import storm.benchmark.lib.spout.ConstSpout;
import storm.benchmark.util.BenchmarkUtils;

/***
 * This topo helps measure how fast a spout can produce data (so no bolts are attached)
 *  Spout generates a stream of a fixed string.
 */
public class ConstSpoutOnlyTopo extends StormBenchmark {
    public static final String SPOUT_ID = "spout";
    public static final String SPOUT_NUM = "component.spout_num";

    public static final int DEFAULT_SPOUT_NUM = 4;

    public static final String SPOUT_MESSAGE = "some data";
    public static final String SPOUT_FIELD = "str";

    private IRichSpout spout;

    @Override
    public StormTopology getTopology(Config config) {
        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);

        spout = new ConstSpout(SPOUT_MESSAGE).withOutputFields(SPOUT_FIELD);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        return builder.createTopology();
    }
}
