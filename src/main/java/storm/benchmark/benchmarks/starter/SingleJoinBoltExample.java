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
package storm.benchmark.benchmarks.starter;

import org.apache.storm.Config;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.bolt.perf.DevNullBolt;
import storm.benchmark.lib.bolt.starter.SingleJoinBolt;
import storm.benchmark.lib.spout.starter.AgeSpout;
import storm.benchmark.lib.spout.starter.GenderSpout;
import storm.benchmark.util.BenchmarkUtils;

import java.util.concurrent.TimeUnit;

public class SingleJoinBoltExample extends StormBenchmark {
    public static final String GENDER_ID = "gender";
    public static final String GENDER_NUM = "component.gender_num";
    public static final String AGE_ID = "age";
    public static final String AGE_NUM = "component.age_num";
    public static final String JOINER_ID = "joiner";
    public static final String JOINER_NUM = "component.joiner_num";

    public static final int DEFAULT_GENDER_NUM = 1;
    public static final int DEFAULT_AGE_NUM = 1;
    public static final int DEFAULT_JOINER_NUM = 1;

    private IRichSpout genderSpout;
    private IRichSpout ageSpout;

    @Override
    public StormTopology getTopology(Config config) {
        final int genderNum = BenchmarkUtils.getInt(config, GENDER_NUM, DEFAULT_GENDER_NUM);
        final int ageNum = BenchmarkUtils.getInt(config, AGE_NUM, DEFAULT_AGE_NUM);
        final int joinerNum = BenchmarkUtils.getInt(config, JOINER_NUM, DEFAULT_JOINER_NUM);

        genderSpout = new GenderSpout();
        ageSpout = new AgeSpout();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(GENDER_ID, genderSpout, genderNum);
        builder.setSpout(AGE_ID, ageSpout, ageNum);

        // inner join of 'age' and 'gender' records on 'id' field
        SingleJoinBolt joiner = new SingleJoinBolt(new Fields("gender", "age"));
        builder.setBolt(JOINER_ID, joiner, joinerNum)
                .fieldsGrouping(GENDER_ID, new Fields("id"))
                .fieldsGrouping(AGE_ID, new Fields("id"));

        return builder.createTopology();
    }
}
