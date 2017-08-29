
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
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import storm.benchmark.benchmarks.common.LineWriter;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.util.BenchmarkUtils;
import storm.benchmark.util.KafkaUtils;

/***
 * This topo helps measure speed of reading from Kafka and writing to Hdfs.
 *  Spout Reads from Kafka.
 *  Bolt writes to Hdfs
 */
public class KafkaHdfsTopo extends StormBenchmark{
    public static final String SPOUT_ID = "spout";
    public static final String SPOUT_NUM = "component.spout_num";
    public static final String BOLT_ID = "bolt";
    public static final String BOLT_NUM = "component.bolt_num";

    public static final int DEFAULT_SPOUT_NUM = 4;
    public static final int DEFAULT_BOLT_NUM = 4;
    public static final String DEFAULT_HDFS_URI = "hdfs://hdfs.namenode:8020";
    public static final String DEFAULT_HDFS_PATH = "/tmp/storm";
    public static final Integer DEFAULT_HDFS_BATCH = 1000;

    public static final String HDFS_URI = "hdfs.uri";
    public static final String HDFS_PATH = "hdfs.dir";
    public static final String HDFS_BATCH = "hdfs.batch";

    public static final String FIELD = "str";

    private IRichSpout spout;

    @Override
    public StormTopology getTopology(Config config) {
        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = BenchmarkUtils.getInt(config, BOLT_NUM, DEFAULT_BOLT_NUM);

        final String hdfsUrl = (String)Utils.get(config, HDFS_URI, DEFAULT_HDFS_URI);
        final String hdfsPath = (String)Utils.get(config, HDFS_PATH, DEFAULT_HDFS_PATH);
        final Integer hdfsBatch = (Integer)Utils.get(config, HDFS_BATCH, DEFAULT_HDFS_BATCH);

        spout = new KafkaSpout(KafkaUtils.getSpoutConfig(config, new SchemeAsMultiScheme(new StringScheme())));


        RecordFormat format = new LineWriter(FIELD);
        SyncPolicy syncPolicy = new CountSyncPolicy(hdfsBatch);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.GB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(hdfsPath);
        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl(hdfsUrl)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(BOLT_ID, bolt, boltNum)
                .localOrShuffleGrouping(SPOUT_ID);
        return builder.createTopology();
    }
}
