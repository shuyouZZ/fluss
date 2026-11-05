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

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/** Test for {@link TableMetricGroup}. */
class TableMetricGroupTest {

    @Test
    void testArrowBatchDistributionsAndWeightedTotals() {
        Map<String, Metric> registered = new HashMap<>();
        Map<String, String> variables = new HashMap<>();
        MetricRegistry registry = mock(MetricRegistry.class);
        doAnswer(
                        invocation -> {
                            String name = invocation.getArgument(1);
                            if (name.startsWith("arrowLog")) {
                                registered.put(name, invocation.getArgument(0));
                                AbstractMetricGroup group = invocation.getArgument(2);
                                variables.putAll(group.getAllVariables());
                            }
                            return null;
                        })
                .when(registry)
                .register(any(Metric.class), anyString(), any(AbstractMetricGroup.class));
        TableMetricGroup group =
                new TableMetricGroup(
                        registry,
                        TablePath.of("db", "pk_table"),
                        true,
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        try {
            Gauge<?> ratio =
                    (Gauge<?>) registered.get(MetricNames.ARROW_LOG_ACTUAL_COMPRESSION_RATIO);
            assertThat(ratio.getValue()).isEqualTo(Double.NaN);
            group.recordArrowLogBatch(3, 600, 400, 130, 260);
            assertThat(ratio.getValue()).isEqualTo(0.5);
            group.recordArrowLogBatch(0, 67, 0, 0, 0);
            assertThat(ratio.getValue()).isEqualTo(0.5);
            assertThat(((Histogram) registered.get(MetricNames.ARROW_LOG_BATCH_RECORDS)).getCount())
                    .isEqualTo(2);
            assertThat(
                            ((Histogram) registered.get(MetricNames.ARROW_LOG_BATCH_RECORDS))
                                    .getStatistics()
                                    .getMean())
                    .isEqualTo(1.5);
            assertThat(
                            ((Histogram) registered.get(MetricNames.ARROW_LOG_BATCH_METADATA_BYTES))
                                    .getStatistics()
                                    .getMean())
                    .isEqualTo(200);
            assertThat(((Counter) registered.get(MetricNames.ARROW_LOG_BATCHES_TOTAL)).getCount())
                    .isEqualTo(2);
            assertThat(((Counter) registered.get(MetricNames.ARROW_LOG_RECORDS_TOTAL)).getCount())
                    .isEqualTo(3);
            assertThat(((Counter) registered.get(MetricNames.ARROW_LOG_BYTES_TOTAL)).getCount())
                    .isEqualTo(667);
            assertThat(
                            ((Counter) registered.get(MetricNames.ARROW_LOG_METADATA_BYTES_TOTAL))
                                    .getCount())
                    .isEqualTo(400);
            assertThat(
                            ((Counter) registered.get(MetricNames.ARROW_LOG_BODY_BYTES_TOTAL))
                                    .getCount())
                    .isEqualTo(130);
            assertThat(
                            ((Counter)
                                            registered.get(
                                                    MetricNames
                                                            .ARROW_LOG_UNCOMPRESSED_BODY_BYTES_TOTAL))
                                    .getCount())
                    .isEqualTo(260);
            assertThat(
                            ((Histogram)
                                            registered.get(
                                                    MetricNames
                                                            .ARROW_LOG_BATCH_UNCOMPRESSED_BODY_BYTES))
                                    .getStatistics()
                                    .getMean())
                    .isEqualTo(130);
            group.recordArrowLogBatch(1, 600, 400, 150, 100);
            assertThat(ratio.getValue()).isEqualTo(1.5);
            assertThat(variables)
                    .containsEntry("tablet_type", "cdc_log")
                    .containsEntry("database", "db")
                    .containsEntry("table", "pk_table");
        } finally {
            group.close();
        }
    }

    @Test
    void testArrowBatchMetricsAreNotRegisteredForLogTables() {
        MetricRegistry registry = mock(MetricRegistry.class);
        Map<String, Metric> registered = new HashMap<>();
        doAnswer(
                        invocation -> {
                            registered.put(invocation.getArgument(1), invocation.getArgument(0));
                            return null;
                        })
                .when(registry)
                .register(any(Metric.class), anyString(), any(AbstractMetricGroup.class));
        TableMetricGroup group =
                new TableMetricGroup(
                        registry,
                        TablePath.of("db", "log_table"),
                        false,
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        try {
            group.recordArrowLogBatch(3, 600, 400, 130, 260);
            assertThat(registered.keySet()).noneMatch(name -> name.startsWith("arrowLog"));
        } finally {
            group.close();
        }
    }

    @Test
    void testKvBackpressureMetricsExposeOnlyCoreSignals() {
        TableMetricGroup metricGroup =
                new TableMetricGroup(
                        NOPMetricRegistry.INSTANCE,
                        TablePath.of("db", "pk_table"),
                        true,
                        TestingMetricGroups.TABLET_SERVER_METRICS);

        assertThat(metricGroup.getMetrics())
                .containsKeys(
                        MetricNames.KV_BACKPRESSURE_MAX_PRESSURE,
                        MetricNames.KV_BACKPRESSURE_REJECTIONS_TOTAL)
                .doesNotContainKey("kvBackpressureAffectedBuckets");
    }
}
