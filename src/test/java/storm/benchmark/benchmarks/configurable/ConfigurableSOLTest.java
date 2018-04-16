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

package storm.benchmark.benchmarks.configurable;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;
import org.testng.annotations.Test;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.util.TestUtils;

import static org.fest.assertions.api.Assertions.assertThat;

public class ConfigurableSOLTest {

  @Test
  public void componentParallelismCouldBeSetThroughConfig() {
    StormBenchmark benchmark = new ConfigurableSOL();
    Config config = new Config();
    config.put(ConfigurableSOL.SPOUT_NUM, 4);
    config.put(ConfigurableSOL.BOLT_NUM, 3);
    config.put(ConfigurableSOL.TOPOLOGY_LEVEL, 3);
    config.put(ConfigurableSOL.SPOUT_RATE, 2);
    config.put(ConfigurableSOL.BOLT_RATE, 1);
    StormTopology topology = benchmark.getTopology(config);
    assertThat(topology).isNotNull();
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, ConfigurableSOL.SPOUT_ID), 4);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, ConfigurableSOL.BOLT_ID + "1"), 3);
    TestUtils.verifyParallelism(Utils.getComponentCommon(topology, ConfigurableSOL.BOLT_ID + "2"), 3);
  }
}
