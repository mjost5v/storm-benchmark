/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.benchmark.benchmarks.starter;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import storm.benchmark.benchmarks.FileReadWordCount;
import storm.benchmark.lib.spout.FileReadSpout;
import storm.benchmark.util.BenchmarkUtils;

public class ResourceAwareWordCount extends FileReadWordCount {
    public static final String CPU_LOAD_SPOUT_NUM = "cpu_load.spout_num";
    public static final String CPU_LOAD_SPLIT_BOLT_NUM = "cpu_load.split_bolt_num";
    public static final String CPU_LOAD_COUNT_BOLT_NUM = "cpu_load.count_bolt_num";

    public static final String MEMLOAD_ON_HEAP_SPOUT_NUM = "memload_on_heap.spout_num";
    public static final String MEMLOAD_ON_HEAP_SPLIT_BOLT_NUM = "memload_on_heap.split_bolt_num";
    public static final String MEMLOAD_ON_HEAP_COUNT_BOLT_NUM = "memload_on_heap.count_bolt_num";

    public static final String MEMLOAD_OFF_HEAP_SPOUT_NUM = "memload_off_heap.spout_num";
    public static final String MEMLOAD_OFF_HEAP_SPLIT_BOLT_NUM = "memload_off_heap.split_bolt_num";
    public static final String MEMLOAD_OFF_HEAP_COUNT_BOLT_NUM = "memload_off_heap.count_bolt_num";

    public static final int DEFAULT_CPU_LOAD_SPOUT = 20;
    public static final int DEFAULT_CPU_LOAD_SPLIT_BOLT = 100;
    public static final int DEFAULT_CPU_LOAD_COUNT_BOLT = 100;

    public static final int DEFAULT_MEMLOAD_ON_HEAP_SPOUT = 64;
    public static final int DEFAULT_MEMLOAD_ON_HEAP_SPLIT_BOLT = 100;
    public static final int DEFAULT_MEMLOAD_ON_HEAP_COUNT_BOLT = 100;

    public static final int DEFAULT_MEMLOAD_OFF_HEAP_SPOUT = 16;
    public static final int DEFAULT_MEMLOAD_OFF_HEAP_SPLIT_BOLT = 16;
    public static final int DEFAULT_MEMLOAD_OFF_HEAP_COUNT_BOLT = 16;

    @Override
    public StormTopology getTopology(Config config) {
        spout = new FileReadSpout(BenchmarkUtils.ifAckEnabled(config));

        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int spBoltNum = BenchmarkUtils.getInt(config, SPLIT_NUM, DEFAULT_SPLIT_BOLT_NUM);
        final int cntBoltNum = BenchmarkUtils.getInt(config, COUNT_NUM, DEFAULT_COUNT_BOLT_NUM);

        final int spoutCpuLoad = BenchmarkUtils.getInt(config, CPU_LOAD_SPOUT_NUM, DEFAULT_CPU_LOAD_SPOUT);
        final int spCpuLoad = BenchmarkUtils.getInt(config, CPU_LOAD_SPLIT_BOLT_NUM, DEFAULT_CPU_LOAD_SPLIT_BOLT);
        final int cntCpuLoad = BenchmarkUtils.getInt(config, CPU_LOAD_COUNT_BOLT_NUM, DEFAULT_CPU_LOAD_COUNT_BOLT);

        final int spoutMemLoadOnHeap = BenchmarkUtils.getInt(config, MEMLOAD_ON_HEAP_SPOUT_NUM, DEFAULT_MEMLOAD_ON_HEAP_SPOUT);
        final int spMemLoadOnHeap = BenchmarkUtils.getInt(config, MEMLOAD_ON_HEAP_SPLIT_BOLT_NUM, DEFAULT_MEMLOAD_ON_HEAP_SPLIT_BOLT);
        final int cntMemLoadOnHeap = BenchmarkUtils.getInt(config, MEMLOAD_ON_HEAP_COUNT_BOLT_NUM, DEFAULT_MEMLOAD_ON_HEAP_COUNT_BOLT);

        final int spoutMemLoadOffHeap = BenchmarkUtils.getInt(config, MEMLOAD_OFF_HEAP_SPOUT_NUM, DEFAULT_MEMLOAD_OFF_HEAP_SPOUT);
        final int spMemLoadOffHeap = BenchmarkUtils.getInt(config, MEMLOAD_OFF_HEAP_SPLIT_BOLT_NUM, DEFAULT_MEMLOAD_OFF_HEAP_SPLIT_BOLT);
        final int cntMemLoadOffHeap = BenchmarkUtils.getInt(config, MEMLOAD_OFF_HEAP_COUNT_BOLT_NUM, DEFAULT_MEMLOAD_OFF_HEAP_COUNT_BOLT);

        TopologyBuilder builder = new TopologyBuilder();

        SpoutDeclarer spoutDeclarer = builder.setSpout(SPOUT_ID, spout, spoutNum);
        spoutDeclarer.setCPULoad(spoutCpuLoad);
        spoutDeclarer.setMemoryLoad(spoutMemLoadOnHeap, spoutMemLoadOffHeap);

        BoltDeclarer splitDeclarer = builder.setBolt(SPLIT_ID, new SplitSentence(), spBoltNum).localOrShuffleGrouping(
                SPOUT_ID);
        splitDeclarer.setCPULoad(spCpuLoad);
        splitDeclarer.setMemoryLoad(spMemLoadOnHeap, spMemLoadOffHeap);

        BoltDeclarer countDeclarer = builder.setBolt(COUNT_ID, new Count(), cntBoltNum).fieldsGrouping(SPLIT_ID,
                new Fields(SplitSentence.FIELDS));
        countDeclarer.setCPULoad(cntCpuLoad);
        countDeclarer.setMemoryLoad(cntMemLoadOnHeap, cntMemLoadOffHeap);

        return builder.createTopology();
    }
}
