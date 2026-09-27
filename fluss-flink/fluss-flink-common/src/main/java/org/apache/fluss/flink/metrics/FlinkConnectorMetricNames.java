/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.metrics;

/** Metric names for connector data flow. */
public class FlinkConnectorMetricNames {

    public static final String IN_DATA_COUNT = "inDataCount";
    public static final String IN_DATA_RATE = "inDataRate";
    public static final String IN_DATA_BYTES = "inDataBytes";
    public static final String IN_DATA_BYTES_RATE = "inDataBytesRate";
    public static final String OUT_DATA_COUNT = "outDataCount";
    public static final String OUT_DATA_RATE = "outDataRate";
    public static final String OUT_DATA_BYTES = "outDataBytes";
    public static final String OUT_DATA_BYTES_RATE = "outDataBytesRate";

    private FlinkConnectorMetricNames() {}
}
