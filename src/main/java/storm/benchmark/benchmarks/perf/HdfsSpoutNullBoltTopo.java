/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package storm.benchmark.benchmarks.perf;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.hdfs.spout.TextFileReader;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import storm.benchmark.benchmarks.common.StormBenchmark;
import storm.benchmark.lib.bolt.perf.DevNullBolt;
import storm.benchmark.util.BenchmarkUtils;

/***
 * This topo helps measure speed of reading from Hdfs.
 *  Spout Reads from Hdfs.
 *  Bolt acks and discards tuples
 */
public class HdfsSpoutNullBoltTopo extends StormBenchmark {
    public static final String SPOUT_ID = "spout";
    public static final String SPOUT_NUM = "component.spout_num";
    public static final String BOLT_ID = "bolt";
    public static final String BOLT_NUM = "component.bolt_num";

    public static final int DEFAULT_SPOUT_NUM = 4;
    public static final int DEFAULT_BOLT_NUM = 4;
    public static final String DEFAULT_FILE_FORMAT = "text";
    public static final String DEFAULT_HDFS_URI = "hdfs://hdfs.namenode:8020";
    public static final String DEFAULT_HDFS_SOURCE_DIR = "/tmp/storm/in";
    public static final String DEFAULT_HDFS_ARCHIVE_DIR = "/tmp/storm/in";
    public static final String DEFAULT_HDFS_BAD_DIR = "/tmp/storm/bad";

    public static final String HDFS_URI    = "hdfs.uri";
    public static final String SOURCE_DIR  = "hdfs.source.dir";
    public static final String ARCHIVE_DIR = "hdfs.archive.dir";
    public static final String BAD_DIR = "hdfs.bad.dir";
    public static final String FILE_FORMAT = "hdfs.file.format";

    private IRichSpout spout;

    @Override
    public StormTopology getTopology(Config config) {
        final int spoutNum = BenchmarkUtils.getInt(config, SPOUT_NUM, DEFAULT_SPOUT_NUM);
        final int boltNum = BenchmarkUtils.getInt(config, BOLT_ID, DEFAULT_BOLT_NUM);

        final String fileFormat = (String) Utils.get(config, FILE_FORMAT, DEFAULT_FILE_FORMAT);
        final String hdfsUri = (String)Utils.get(config, HDFS_URI, DEFAULT_HDFS_URI);
        final String sourceDir = (String)Utils.get(config, SOURCE_DIR, DEFAULT_HDFS_SOURCE_DIR);
        final String archiveDir = (String)Utils.get(config, ARCHIVE_DIR, DEFAULT_HDFS_ARCHIVE_DIR);
        final String badDir = (String)Utils.get(config, BAD_DIR, DEFAULT_HDFS_ARCHIVE_DIR);

        spout = new HdfsSpout()
                .setReaderType(fileFormat)
                .setHdfsUri(hdfsUri)
                .setSourceDir(sourceDir)
                .setArchiveDir(archiveDir)
                .setBadFilesDir(badDir)
                .withOutputFields(TextFileReader.defaultFields);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT_ID, spout, spoutNum);
        builder.setBolt(BOLT_ID, new DevNullBolt(), boltNum)
                .localOrShuffleGrouping(SPOUT_ID);
        return builder.createTopology();
    }
}
