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

package storm.benchmark.benchmarks.common;

import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.tuple.Tuple;

public class LineWriter implements RecordFormat {
    private static final long serialVersionUID = 7524288317405514146L;
    private String lineDelimiter = System.lineSeparator();
    private String fieldName;

    public LineWriter(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * Overrides the default record delimiter.
     *
     * @param delimiter
     * @return
     */
    public LineWriter withLineDelimiter(String delimiter){
        this.lineDelimiter = delimiter;
        return this;
    }

    public byte[] format(Tuple tuple) {
        return (tuple.getValueByField(fieldName).toString() +  this.lineDelimiter).getBytes();
    }
}
